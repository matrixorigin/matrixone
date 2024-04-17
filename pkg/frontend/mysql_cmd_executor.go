// Copyright 2021 Matrix Origin
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//      http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package frontend

import (
	"bytes"
	"context"
	"encoding/binary"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	gotrace "runtime/trace"
	"sort"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/fagongzi/goetty/v2"
	"github.com/google/uuid"
	"go.uber.org/zap"

	"github.com/matrixorigin/matrixone/pkg/clusterservice"
	"github.com/matrixorigin/matrixone/pkg/common/runtime"
	"github.com/matrixorigin/matrixone/pkg/common/util"
	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/container/vector"
	"github.com/matrixorigin/matrixone/pkg/frontend/constant"
	"github.com/matrixorigin/matrixone/pkg/pb/metadata"
	"github.com/matrixorigin/matrixone/pkg/pb/timestamp"
	"github.com/matrixorigin/matrixone/pkg/sql/colexec"
	"github.com/matrixorigin/matrixone/pkg/sql/compile"
	"github.com/matrixorigin/matrixone/pkg/sql/parsers/dialect"
	"github.com/matrixorigin/matrixone/pkg/sql/parsers/dialect/mysql"
	plan2 "github.com/matrixorigin/matrixone/pkg/sql/plan"
	"github.com/matrixorigin/matrixone/pkg/sql/plan/explain"
	"github.com/matrixorigin/matrixone/pkg/txn/client"
	txnTrace "github.com/matrixorigin/matrixone/pkg/txn/trace"
	"github.com/matrixorigin/matrixone/pkg/util/metric"
	v2 "github.com/matrixorigin/matrixone/pkg/util/metric/v2"
	"github.com/matrixorigin/matrixone/pkg/util/trace/impl/motrace"
	"github.com/matrixorigin/matrixone/pkg/vm/engine"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/disttae"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/disttae/route"

	"github.com/confluentinc/confluent-kafka-go/v2/kafka"

	"github.com/matrixorigin/matrixone/pkg/common/moerr"
	"github.com/matrixorigin/matrixone/pkg/container/batch"
	"github.com/matrixorigin/matrixone/pkg/defines"
	"github.com/matrixorigin/matrixone/pkg/logutil"
	"github.com/matrixorigin/matrixone/pkg/pb/plan"
	"github.com/matrixorigin/matrixone/pkg/sql/parsers"
	"github.com/matrixorigin/matrixone/pkg/sql/parsers/tree"
	"github.com/matrixorigin/matrixone/pkg/util/trace"
	"github.com/matrixorigin/matrixone/pkg/util/trace/impl/motrace/statistic"
	"github.com/matrixorigin/matrixone/pkg/vm/process"
)

func createDropDatabaseErrorInfo() string {
	return "CREATE/DROP of database is not supported in transactions"
}

func onlyCreateStatementErrorInfo() string {
	return "Only CREATE of DDL is supported in transactions"
}

func administrativeCommandIsUnsupportedInTxnErrorInfo() string {
	return "administrative command is unsupported in transactions"
}

func unclassifiedStatementInUncommittedTxnErrorInfo() string {
	return "unclassified statement appears in uncommitted transaction"
}

func writeWriteConflictsErrorInfo() string {
	return "Write conflicts detected. Previous transaction need to be aborted."
}

const (
	prefixPrepareStmtName       = "__mo_stmt_id"
	prefixPrepareStmtSessionVar = "__mo_stmt_var"
)

func getPrepareStmtName(stmtID uint32) string {
	return fmt.Sprintf("%s_%d", prefixPrepareStmtName, stmtID)
}

func parsePrepareStmtID(s string) uint32 {
	if strings.HasPrefix(s, prefixPrepareStmtName) {
		ss := strings.Split(s, "_")
		v, err := strconv.ParseUint(ss[len(ss)-1], 10, 64)
		if err != nil {
			return 0
		}
		return uint32(v)
	}
	return 0
}

func GetPrepareStmtID(ctx context.Context, name string) (int, error) {
	idx := len(prefixPrepareStmtName) + 1
	if idx >= len(name) {
		return -1, moerr.NewInternalError(ctx, "can not get Prepare stmtID")
	}
	return strconv.Atoi(name[idx:])
}

func transferSessionConnType2StatisticConnType(c ConnType) statistic.ConnType {
	switch c {
	case ConnTypeUnset:
		return statistic.ConnTypeUnknown
	case ConnTypeInternal:
		return statistic.ConnTypeInternal
	case ConnTypeExternal:
		return statistic.ConnTypeExternal
	default:
		panic("unknown connection type")
	}
}

var RecordStatement = func(ctx context.Context, ses *Session, proc *process.Process, cw ComputationWrapper, envBegin time.Time, envStmt, sqlType string, useEnv bool) (context.Context, error) {
	// set StatementID
	var stmID uuid.UUID
	var statement tree.Statement = nil
	var text string
	if cw != nil {
		copy(stmID[:], cw.GetUUID())
		statement = cw.GetAst()

		ses.ast = statement

		execSql := makeExecuteSql(ses, statement)
		if len(execSql) != 0 {
			bb := strings.Builder{}
			bb.WriteString(envStmt)
			bb.WriteString(" // ")
			bb.WriteString(execSql)
			text = SubStringFromBegin(bb.String(), int(getGlobalPu().SV.LengthOfQueryPrinted))
		} else {
			text = SubStringFromBegin(envStmt, int(getGlobalPu().SV.LengthOfQueryPrinted))
		}
	} else {
		stmID, _ = uuid.NewV7()
		text = SubStringFromBegin(envStmt, int(getGlobalPu().SV.LengthOfQueryPrinted))
	}
	ses.SetStmtId(stmID)
	ses.SetStmtType(getStatementType(statement).GetStatementType())
	ses.SetQueryType(getStatementType(statement).GetQueryType())
	ses.SetSqlSourceType(sqlType)
	ses.SetSqlOfStmt(text)

	//note: txn id here may be empty
	if sqlType != constant.InternalSql {
		ses.pushQueryId(types.Uuid(stmID).ToString())
	}

	if !motrace.GetTracerProvider().IsEnable() {
		return ctx, nil
	}
	tenant := ses.GetTenantInfo()
	if tenant == nil {
		tenant, _ = GetTenantInfo(ctx, "internal")
	}
	stm := motrace.NewStatementInfo()
	// set TransactionID
	var txn TxnOperator
	var err error
	if handler := ses.GetTxnHandler(); handler.IsValidTxnOperator() {
		_, txn, err = handler.GetTxnOperator()
		if err != nil {
			return nil, err
		}
		copy(stm.TransactionID[:], txn.Txn().ID)
	}
	// set SessionID
	copy(stm.SessionID[:], ses.GetUUID())
	requestAt := envBegin
	if !useEnv {
		requestAt = time.Now()
	}

	copy(stm.StatementID[:], stmID[:])
	// END> set StatementID
	stm.Account = tenant.GetTenant()
	stm.RoleId = proc.SessionInfo.RoleId
	stm.User = tenant.GetUser()
	stm.Host = ses.proto.Peer()
	stm.Database = ses.GetDatabaseName()
	stm.Statement = text
	stm.StatementFingerprint = "" // fixme= (Reserved)
	stm.StatementTag = ""         // fixme= (Reserved)
	stm.SqlSourceType = sqlType
	stm.RequestAt = requestAt
	stm.StatementType = getStatementType(statement).GetStatementType()
	stm.QueryType = getStatementType(statement).GetQueryType()
	stm.ConnType = transferSessionConnType2StatisticConnType(ses.connType)
	if sqlType == constant.InternalSql && isCmdFieldListSql(envStmt) {
		// fix original issue #8165
		stm.User = ""
	}
	if sqlType != constant.InternalSql {
		ses.SetTStmt(stm)
	}
	if !stm.IsZeroTxnID() {
		stm.Report(ctx)
	}
	if stm.IsMoLogger() && stm.StatementType == "Load" && len(stm.Statement) > 128 {
		stm.Statement = envStmt[:40] + "..." + envStmt[len(envStmt)-45:]
	}

	return motrace.ContextWithStatement(ctx, stm), nil
}

var RecordParseErrorStatement = func(ctx context.Context, ses *Session, proc *process.Process, envBegin time.Time,
	envStmt []string, sqlTypes []string, err error) (context.Context, error) {
	retErr := moerr.NewParseError(ctx, err.Error())
	/*
		!!!NOTE: the sql may be empty string.
		So, the sqlTypes may be empty slice.
	*/
	sqlType := ""
	if len(sqlTypes) > 0 {
		sqlType = sqlTypes[0]
	} else {
		sqlType = constant.ExternSql
	}
	if len(envStmt) > 0 {
		for i, sql := range envStmt {
			if i < len(sqlTypes) {
				sqlType = sqlTypes[i]
			}
			ctx, err = RecordStatement(ctx, ses, proc, nil, envBegin, sql, sqlType, true)
			if err != nil {
				return nil, err
			}
			motrace.EndStatement(ctx, retErr, 0, 0, 0)
		}
	} else {
		ctx, err = RecordStatement(ctx, ses, proc, nil, envBegin, "", sqlType, true)
		if err != nil {
			return nil, err
		}
		motrace.EndStatement(ctx, retErr, 0, 0, 0)
	}

	tenant := ses.GetTenantInfo()
	if tenant == nil {
		tenant, _ = GetTenantInfo(ctx, "internal")
	}
	incStatementErrorsCounter(tenant.GetTenant(), nil)
	return ctx, nil
}

// RecordStatementTxnID record txnID after TxnBegin or Compile(autocommit=1)
var RecordStatementTxnID = func(ctx context.Context, fses FeSession) error {
	var ses *Session
	var ok bool
	if ses, ok = fses.(*Session); !ok {
		return nil
	}
	var txn TxnOperator
	var err error
	if stm := motrace.StatementFromContext(ctx); ses != nil && stm != nil && stm.IsZeroTxnID() {
		if handler := ses.GetTxnHandler(); handler.IsValidTxnOperator() {
			// simplify the logic of TxnOperator. refer to https://github.com/matrixorigin/matrixone/pull/13436#pullrequestreview-1779063200
			_, txn, err = handler.GetTxnOperator()
			if err != nil {
				return err
			}
			stm.SetTxnID(txn.Txn().ID)
			ses.SetTxnId(txn.Txn().ID)
		}
		stm.Report(ctx)
	}

	// set frontend statement's txn-id
	if upSes := ses.upstream; upSes != nil && upSes.tStmt != nil && upSes.tStmt.IsZeroTxnID() /* not record txn-id */ {
		// background session has valid txn
		if handler := ses.GetTxnHandler(); handler.IsValidTxnOperator() {
			_, txn, err = handler.GetTxnOperator()
			if err != nil {
				return err
			}
			// set upstream (the frontend session) statement's txn-id
			// PS: only skip ONE txn
			if stmt := upSes.tStmt; stmt.NeedSkipTxn() /* normally set by determineUserHasPrivilegeSet */ {
				// need to skip the whole txn, so it records the skipped txn-id
				stmt.SetSkipTxn(false)
				stmt.SetSkipTxnId(txn.Txn().ID)
			} else if txnId := txn.Txn().ID; !stmt.SkipTxnId(txnId) {
				upSes.tStmt.SetTxnID(txnId)
			}
		}
	}
	return nil
}

func handleShowTableStatus(ses *Session, stmt *tree.ShowTableStatus, proc *process.Process) error {
	var db engine.Database
	var err error

	ctx := ses.requestCtx
	// get db info as current account
	if db, err = ses.GetStorage().Database(ctx, stmt.DbName, proc.TxnOperator); err != nil {
		return err
	}

	if db.IsSubscription(ctx) {
		// get global unique (pubAccountName, pubName)
		var pubAccountName, pubName string
		if _, pubAccountName, pubName, err = getSubInfoFromSql(ctx, ses, db.GetCreateSql(ctx)); err != nil {
			return err
		}

		bh := GetRawBatchBackgroundExec(ctx, ses)
		ctx = context.WithValue(ctx, defines.TenantIDKey{}, uint32(sysAccountID))
		var pubAccountId int32
		if pubAccountId = getAccountIdByName(ctx, ses, bh, pubAccountName); pubAccountId == -1 {
			return moerr.NewInternalError(ctx, "publish account does not exist")
		}

		// get publication record
		var pubs []*published
		if pubs, err = getPubs(ctx, ses, bh, pubAccountId, pubAccountName, pubName, ses.GetTenantName()); err != nil {
			return err
		}
		if len(pubs) != 1 {
			return moerr.NewInternalError(ctx, "no satisfied publication")
		}

		// as pub account
		ctx = context.WithValue(ctx, defines.TenantIDKey{}, uint32(pubAccountId))
		// get db as pub account
		if db, err = ses.GetStorage().Database(ctx, pubs[0].pubDatabase, proc.TxnOperator); err != nil {
			return err
		}
	}

	getRoleName := func(roleId uint32) (roleName string, err error) {
		sql := getSqlForRoleNameOfRoleId(int64(roleId))

		var rets []ExecResult
		if rets, err = executeSQLInBackgroundSession(ctx, ses, ses.GetMemPool(), sql); err != nil {
			return "", err
		}

		if !execResultArrayHasData(rets) {
			return "", moerr.NewInternalError(ctx, "get role name failed")
		}

		if roleName, err = rets[0].GetString(ctx, 0, 0); err != nil {
			return "", err
		}
		return roleName, nil
	}

	mrs := ses.GetMysqlResultSet()
	for _, row := range ses.data {
		tableName := string(row[0].([]byte))
		r, err := db.Relation(ctx, tableName, nil)
		if err != nil {
			return err
		}
		if row[3], err = r.Rows(ctx); err != nil {
			return err
		}
		if row[5], err = r.Size(ctx, disttae.AllColumns); err != nil {
			return err
		}
		roleId := row[17].(uint32)
		// role name
		if row[18], err = getRoleName(roleId); err != nil {
			return err
		}
		mrs.AddRow(row)
	}
	return nil
}

// getDataFromPipeline: extract the data from the pipeline.
// obj: session
func getDataFromPipeline(obj interface{}, bat *batch.Batch) error {
	_, task := gotrace.NewTask(context.TODO(), "frontend.WriteDataToClient")
	defer task.End()
	ses := obj.(*Session)
	if openSaveQueryResult(ses) {
		if bat == nil {
			if err := saveQueryResultMeta(ses); err != nil {
				return err
			}
		} else {
			if err := saveQueryResult(ses, bat); err != nil {
				return err
			}
		}
	}
	if bat == nil {
		return nil
	}

	begin := time.Now()
	proto := ses.GetMysqlProtocol()

	ec := ses.GetExportConfig()
	oq := NewOutputQueue(ses.GetRequestContext(), ses, len(bat.Vecs), nil, nil)
	row2colTime := time.Duration(0)
	procBatchBegin := time.Now()
	n := bat.Vecs[0].Length()
	requestCtx := ses.GetRequestContext()

	if ec.needExportToFile() {
		initExportFirst(oq)
	}

	for j := 0; j < n; j++ { //row index
		if ec.needExportToFile() {
			select {
			case <-requestCtx.Done():
				return nil
			default:
			}
			continue
		}

		row, err := extractRowFromEveryVector(ses, bat, j, oq, true)
		if err != nil {
			return err
		}
		if oq.showStmtType == ShowTableStatus {
			row2 := make([]interface{}, len(row))
			copy(row2, row)
			ses.AppendData(row2)
		}
	}

	if ec.needExportToFile() {
		oq.rowIdx = uint64(n)
		bat2 := preCopyBat(obj, bat)
		go constructByte(obj, bat2, oq.ep.Index, oq.ep.ByteChan, oq)
	}
	err := oq.flush()
	if err != nil {
		return err
	}

	procBatchTime := time.Since(procBatchBegin)
	tTime := time.Since(begin)
	ses.sentRows.Add(int64(n))
	logDebugf(ses.GetDebugString(), "rowCount %v \n"+
		"time of getDataFromPipeline : %s \n"+
		"processBatchTime %v \n"+
		"row2colTime %v \n"+
		"restTime(=totalTime - row2colTime) %v \n"+
		"protoStats %s",
		n,
		tTime,
		procBatchTime,
		row2colTime,
		tTime-row2colTime,
		proto.GetStats())

	return nil
}

func doUse(ctx context.Context, ses FeSession, db string) error {
	defer RecordStatementTxnID(ctx, ses)
	txnHandler := ses.GetTxnHandler()
	var txnCtx context.Context
	var txn TxnOperator
	var err error
	var dbMeta engine.Database
	txnCtx, txn, err = txnHandler.GetTxn()
	if err != nil {
		return err
	}
	//TODO: check meta data
	if dbMeta, err = getGlobalPu().StorageEngine.Database(txnCtx, db, txn); err != nil {
		//echo client. no such database
		return moerr.NewBadDB(ctx, db)
	}
	if dbMeta.IsSubscription(ctx) {
		_, err = checkSubscriptionValid(ctx, ses, dbMeta.GetCreateSql(ctx))
		if err != nil {
			return err
		}
	}
	oldDB := ses.GetDatabaseName()
	ses.SetDatabaseName(db)

	logDebugf(ses.GetDebugString(), "User %s change database from [%s] to [%s]", ses.GetUserName(), oldDB, ses.GetDatabaseName())

	return nil
}

func handleChangeDB(requestCtx context.Context, ses FeSession, db string) error {
	return doUse(requestCtx, ses, db)
}

func handleDump(requestCtx context.Context, ses FeSession, dump *tree.MoDump) error {
	return doDumpQueryResult(requestCtx, ses.(*Session), dump.ExportParams)
}

/*
handle "SELECT @@xxx.yyyy"
*/
func handleSelectVariables(ses FeSession, ve *tree.VarExpr, isLastStmt bool) error {
	var err error = nil
	mrs := ses.GetMysqlResultSet()

	col := new(MysqlColumn)
	col.SetColumnType(defines.MYSQL_TYPE_VARCHAR)
	col.SetName("@@" + ve.Name)
	mrs.AddColumn(col)

	row := make([]interface{}, 1)
	if ve.System {
		if ve.Global {
			val, err := ses.GetGlobalVar(ve.Name)
			if err != nil {
				return err
			}
			row[0] = val
		} else {
			val, err := ses.GetSessionVar(ve.Name)
			if err != nil {
				return err
			}
			row[0] = val
		}
	} else {
		//user defined variable
		_, val, err := ses.GetUserDefinedVar(ve.Name)
		if err != nil {
			return err
		}
		if val != nil {
			row[0] = val.Value
		} else {
			row[0] = nil
		}
	}

	mrs.AddRow(row)
	return err
}

func doCmdFieldList(requestCtx context.Context, ses *Session, _ *InternalCmdFieldList) error {
	dbName := ses.GetDatabaseName()
	if dbName == "" {
		return moerr.NewNoDB(requestCtx)
	}

	//Get table infos for the database from the cube
	//case 1: there are no table infos for the db
	//case 2: db changed
	//NOTE: it costs too much time.
	//It just reduces the information in the auto-completion (auto-rehash) of the mysql client.
	//var attrs []ColumnInfo
	//
	//if tableInfos == nil || db != dbName {
	//	txnHandler := ses.GetTxnHandler()
	//	eng := ses.GetStorage()
	//	db, err := eng.Database(requestCtx, dbName, txnHandler.GetTxn())
	//	if err != nil {
	//		return err
	//	}
	//
	//	names, err := db.Relations(requestCtx)
	//	if err != nil {
	//		return err
	//	}
	//	for _, name := range names {
	//		table, err := db.Relation(requestCtx, name)
	//		if err != nil {
	//			return err
	//		}
	//
	//		defs, err := table.TableDefs(requestCtx)
	//		if err != nil {
	//			return err
	//		}
	//		for _, def := range defs {
	//			if attr, ok := def.(*engine.AttributeDef); ok {
	//				attrs = append(attrs, &engineColumnInfo{
	//					name: attr.Attr.Name,
	//					typ:  attr.Attr.Type,
	//				})
	//			}
	//		}
	//	}
	//
	//	if tableInfos == nil {
	//		tableInfos = make(map[string][]ColumnInfo)
	//	}
	//	tableInfos[tableName] = attrs
	//}
	//
	//cols, ok := tableInfos[tableName]
	//if !ok {
	//	//just give the empty info when there is no such table.
	//	attrs = make([]ColumnInfo, 0)
	//} else {
	//	attrs = cols
	//}
	//
	//for _, c := range attrs {
	//	col := new(MysqlColumn)
	//	col.SetName(c.GetName())
	//	err = convertEngineTypeToMysqlType(c.GetType(), col)
	//	if err != nil {
	//		return err
	//	}
	//
	//	/*
	//		mysql CMD_FIELD_LIST response: send the column definition per column
	//	*/
	//	err = proto.SendColumnDefinitionPacket(col, int(COM_FIELD_LIST))
	//	if err != nil {
	//		return err
	//	}
	//}
	return nil
}

/*
handle cmd CMD_FIELD_LIST
*/
func handleCmdFieldList(requestCtx context.Context, ses FeSession, icfl *InternalCmdFieldList) error {
	var err error
	proto := ses.GetMysqlProtocol()

	ses.SetMysqlResultSet(nil)
	err = doCmdFieldList(requestCtx, ses.(*Session), icfl)
	if err != nil {
		return err
	}

	/*
		mysql CMD_FIELD_LIST response: End after the column has been sent.
		send EOF packet
	*/
	err = proto.sendEOFOrOkPacket(0, ses.GetTxnHandler().GetServerStatus())
	if err != nil {
		return err
	}

	return err
}

func doSetVar(ctx context.Context, ses *Session, sv *tree.SetVar, sql string) error {
	var err error = nil
	var ok bool
	setVarFunc := func(system, global bool, name string, value interface{}, sql string) error {
		var oldValueRaw interface{}
		if system {
			if global {
				err = doCheckRole(ctx, ses)
				if err != nil {
					return err
				}
				err = ses.SetGlobalVar(name, value)
				if err != nil {
					return err
				}
				err = doSetGlobalSystemVariable(ctx, ses, name, value)
				if err != nil {
					return err
				}
			} else {
				if strings.ToLower(name) == "autocommit" {
					oldValueRaw, err = ses.GetSessionVar("autocommit")
					if err != nil {
						return err
					}
				}
				err = ses.SetSessionVar(name, value)
				if err != nil {
					return err
				}
			}

			if strings.ToLower(name) == "autocommit" {
				oldValue, err := valueIsBoolTrue(oldValueRaw)
				if err != nil {
					return err
				}
				newValue, err := valueIsBoolTrue(value)
				if err != nil {
					return err
				}
				err = ses.GetTxnHandler().SetAutocommit(oldValue, newValue)
				if err != nil {
					return err
				}
			}
		} else {
			err = ses.SetUserDefinedVar(name, value, sql)
			if err != nil {
				return err
			}
		}
		return nil
	}
	for _, assign := range sv.Assignments {
		name := assign.Name
		var value interface{}

		value, err = getExprValue(assign.Value, ses)
		if err != nil {
			return err
		}

		if systemVar, ok := gSysVarsDefs[name]; ok {
			if isDefault, ok := value.(bool); ok && isDefault {
				value = systemVar.Default
			}
		}

		//TODO : fix SET NAMES after parser is ready
		if name == "names" {
			//replaced into three system variable:
			//character_set_client, character_set_connection, and character_set_results
			replacedBy := []string{
				"character_set_client", "character_set_connection", "character_set_results",
			}
			for _, rb := range replacedBy {
				err = setVarFunc(assign.System, assign.Global, rb, value, sql)
				if err != nil {
					return err
				}
			}
		} else if name == "syspublications" {
			if !ses.GetTenantInfo().IsSysTenant() {
				return moerr.NewInternalError(ses.GetRequestContext(), "only system account can set system variable syspublications")
			}
			err = setVarFunc(assign.System, assign.Global, name, value, sql)
			if err != nil {
				return err
			}
		} else if name == "clear_privilege_cache" {
			//if it is global variable, it does nothing.
			if !assign.Global {
				//if the value is 'on or off', just invalidate the privilege cache
				ok, err = valueIsBoolTrue(value)
				if err != nil {
					return err
				}

				if ok {
					cache := ses.GetPrivilegeCache()
					if cache != nil {
						cache.invalidate()
					}
				}
				err = setVarFunc(assign.System, assign.Global, name, value, sql)
				if err != nil {
					return err
				}
			}
		} else if name == "enable_privilege_cache" {
			ok, err = valueIsBoolTrue(value)
			if err != nil {
				return err
			}

			//disable privilege cache. clean the cache.
			if !ok {
				cache := ses.GetPrivilegeCache()
				if cache != nil {
					cache.invalidate()
				}
			}
			err = setVarFunc(assign.System, assign.Global, name, value, sql)
			if err != nil {
				return err
			}
		} else if name == "runtime_filter_limit_in" {
			err = setVarFunc(assign.System, assign.Global, name, value, sql)
			if err != nil {
				return err
			}
			runtime.ProcessLevelRuntime().SetGlobalVariables("runtime_filter_limit_in", value)
		} else if name == "runtime_filter_limit_bloom_filter" {
			err = setVarFunc(assign.System, assign.Global, name, value, sql)
			if err != nil {
				return err
			}
			runtime.ProcessLevelRuntime().SetGlobalVariables("runtime_filter_limit_bloom_filter", value)
		} else {
			err = setVarFunc(assign.System, assign.Global, name, value, sql)
			if err != nil {
				return err
			}
		}
	}
	return err
}

/*
handle setvar
*/
func handleSetVar(ctx context.Context, ses FeSession, sv *tree.SetVar, sql string) error {
	err := doSetVar(ctx, ses.(*Session), sv, sql)
	if err != nil {
		return err
	}

	return nil
}

func doShowErrors(ses *Session) error {
	var err error

	levelCol := new(MysqlColumn)
	levelCol.SetColumnType(defines.MYSQL_TYPE_VARCHAR)
	levelCol.SetName("Level")

	CodeCol := new(MysqlColumn)
	CodeCol.SetColumnType(defines.MYSQL_TYPE_SHORT)
	CodeCol.SetName("Code")

	MsgCol := new(MysqlColumn)
	MsgCol.SetColumnType(defines.MYSQL_TYPE_VARCHAR)
	MsgCol.SetName("Message")

	mrs := ses.GetMysqlResultSet()

	mrs.AddColumn(levelCol)
	mrs.AddColumn(CodeCol)
	mrs.AddColumn(MsgCol)

	info := ses.GetErrInfo()

	for i := info.length() - 1; i >= 0; i-- {
		row := make([]interface{}, 3)
		row[0] = "Error"
		row[1] = info.codes[i]
		row[2] = info.msgs[i]
		mrs.AddRow(row)
	}

	return err
}

func handleShowErrors(ses FeSession, isLastStmt bool) error {
	err := doShowErrors(ses.(*Session))
	if err != nil {
		return err
	}
	return err
}

func doShowVariables(ses *Session, proc *process.Process, sv *tree.ShowVariables) error {
	if sv.Like != nil && sv.Where != nil {
		return moerr.NewSyntaxError(ses.GetRequestContext(), "like clause and where clause cannot exist at the same time")
	}

	var err error = nil

	col1 := new(MysqlColumn)
	col1.SetColumnType(defines.MYSQL_TYPE_VARCHAR)
	col1.SetName("Variable_name")

	col2 := new(MysqlColumn)
	col2.SetColumnType(defines.MYSQL_TYPE_VARCHAR)
	col2.SetName("Value")

	mrs := ses.GetMysqlResultSet()
	mrs.AddColumn(col1)
	mrs.AddColumn(col2)

	var hasLike = false
	var likePattern = ""
	var isIlike = false
	if sv.Like != nil {
		hasLike = true
		if sv.Like.Op == tree.ILIKE {
			isIlike = true
		}
		likePattern = strings.ToLower(sv.Like.Right.String())
	}

	var sysVars map[string]interface{}
	if sv.Global {
		sysVars, err = doGetGlobalSystemVariable(ses.GetRequestContext(), ses)
		if err != nil {
			return err
		}
	} else {
		sysVars = ses.CopyAllSessionVars()
	}

	rows := make([][]interface{}, 0, len(sysVars))
	for name, value := range sysVars {
		if hasLike {
			s := name
			if isIlike {
				s = strings.ToLower(s)
			}
			if !WildcardMatch(likePattern, s) {
				continue
			}
		}
		row := make([]interface{}, 2)
		row[0] = name
		gsv, ok := GSysVariables.GetDefinitionOfSysVar(name)
		if !ok {
			return moerr.NewInternalError(ses.GetRequestContext(), errorSystemVariableDoesNotExist())
		}
		row[1] = value
		if svbt, ok2 := gsv.GetType().(SystemVariableBoolType); ok2 {
			if svbt.IsTrue(value) {
				row[1] = "on"
			} else {
				row[1] = "off"
			}
		}
		rows = append(rows, row)
	}

	if sv.Where != nil {
		bat, err := constructVarBatch(ses, rows)
		if err != nil {
			return err
		}
		binder := plan2.NewDefaultBinder(proc.Ctx, nil, nil, &plan2.Type{Id: int32(types.T_varchar), Width: types.MaxVarcharLen}, []string{"variable_name", "value"})
		planExpr, err := binder.BindExpr(sv.Where.Expr, 0, false)
		if err != nil {
			return err
		}

		executor, err := colexec.NewExpressionExecutor(proc, planExpr)
		if err != nil {
			return err
		}
		vec, err := executor.Eval(proc, []*batch.Batch{bat})
		if err != nil {
			executor.Free()
			return err
		}

		bs := vector.MustFixedCol[bool](vec)
		sels := proc.Mp().GetSels()
		for i, b := range bs {
			if b {
				sels = append(sels, int64(i))
			}
		}
		executor.Free()

		bat.Shrink(sels, false)
		proc.Mp().PutSels(sels)
		v0 := vector.MustStrCol(bat.Vecs[0])
		v1 := vector.MustStrCol(bat.Vecs[1])
		rows = rows[:len(v0)]
		for i := range v0 {
			rows[i][0] = v0[i]
			rows[i][1] = v1[i]
		}
		bat.Clean(proc.Mp())
	}

	//sort by name
	sort.Slice(rows, func(i, j int) bool {
		return rows[i][0].(string) < rows[j][0].(string)
	})

	for _, row := range rows {
		mrs.AddRow(row)
	}

	return err
}

/*
handle show variables
*/
func handleShowVariables(ses FeSession, sv *tree.ShowVariables, proc *process.Process, isLastStmt bool) error {
	err := doShowVariables(ses.(*Session), proc, sv)
	if err != nil {
		return err
	}
	return err
}

func constructVarBatch(ses *Session, rows [][]interface{}) (*batch.Batch, error) {
	bat := batch.New(true, []string{"Variable_name", "Value"})
	typ := types.New(types.T_varchar, types.MaxVarcharLen, 0)
	cnt := len(rows)
	bat.SetRowCount(cnt)
	v0 := make([]string, cnt)
	v1 := make([]string, cnt)
	for i, row := range rows {
		v0[i] = row[0].(string)
		v1[i] = fmt.Sprintf("%v", row[1])
	}
	bat.Vecs[0] = vector.NewVec(typ)
	vector.AppendStringList(bat.Vecs[0], v0, nil, ses.GetMemPool())
	bat.Vecs[1] = vector.NewVec(typ)
	vector.AppendStringList(bat.Vecs[1], v1, nil, ses.GetMemPool())
	return bat, nil
}

func constructCollationBatch(ses *Session, rows [][]interface{}) (*batch.Batch, error) {
	bat := batch.New(true, []string{"Collation", "Charset", "Id", "Default", "Compiled", "Sortlen", "Pad_attribute"})
	typ := types.New(types.T_varchar, types.MaxVarcharLen, 0)
	longlongTyp := types.New(types.T_int64, 0, 0)
	longTyp := types.New(types.T_int32, 0, 0)
	cnt := len(rows)
	bat.SetRowCount(cnt)
	v0 := make([]string, cnt)
	v1 := make([]string, cnt)
	v2 := make([]int64, cnt)
	v3 := make([]string, cnt)
	v4 := make([]string, cnt)
	v5 := make([]int32, cnt)
	v6 := make([]string, cnt)
	for i, row := range rows {
		v0[i] = row[0].(string)
		v1[i] = row[1].(string)
		v2[i] = row[2].(int64)
		v3[i] = row[3].(string)
		v4[i] = row[4].(string)
		v5[i] = row[5].(int32)
		v6[i] = row[6].(string)
	}
	bat.Vecs[0] = vector.NewVec(typ)
	vector.AppendStringList(bat.Vecs[0], v0, nil, ses.GetMemPool())
	bat.Vecs[1] = vector.NewVec(typ)
	vector.AppendStringList(bat.Vecs[1], v1, nil, ses.GetMemPool())
	bat.Vecs[2] = vector.NewVec(longlongTyp)
	vector.AppendFixedList[int64](bat.Vecs[2], v2, nil, ses.GetMemPool())
	bat.Vecs[3] = vector.NewVec(typ)
	vector.AppendStringList(bat.Vecs[3], v3, nil, ses.GetMemPool())
	bat.Vecs[4] = vector.NewVec(typ)
	vector.AppendStringList(bat.Vecs[4], v4, nil, ses.GetMemPool())
	bat.Vecs[5] = vector.NewVec(longTyp)
	vector.AppendFixedList[int32](bat.Vecs[5], v5, nil, ses.GetMemPool())
	bat.Vecs[6] = vector.NewVec(typ)
	vector.AppendStringList(bat.Vecs[6], v6, nil, ses.GetMemPool())
	return bat, nil
}

func handleAnalyzeStmt(requestCtx context.Context, ses *Session, stmt *tree.AnalyzeStmt) error {
	// rewrite analyzeStmt to `select approx_count_distinct(col), .. from tbl`
	// IMO, this approach is simple and future-proof
	// Although this rewriting processing could have been handled in rewrite module,
	// `handleAnalyzeStmt` can be easily managed by cron jobs in the future
	ctx := tree.NewFmtCtx(dialect.MYSQL)
	ctx.WriteString("select ")
	for i, ident := range stmt.Cols {
		if i > 0 {
			ctx.WriteByte(',')
		}
		ctx.WriteString("approx_count_distinct(")
		ctx.WriteString(string(ident))
		ctx.WriteByte(')')
	}
	ctx.WriteString(" from ")
	stmt.Table.Format(ctx)
	sql := ctx.String()
	//backup the inside statement
	prevInsideStmt := ses.ReplaceDerivedStmt(true)
	defer func() {
		//restore the inside statement
		ses.ReplaceDerivedStmt(prevInsideStmt)
	}()
	return doComQuery(requestCtx, ses, &UserInput{sql: sql})
}

func doExplainStmt(ses *Session, stmt *tree.ExplainStmt) error {
	requestCtx := ses.GetRequestContext()

	//1. generate the plan
	es, err := getExplainOption(requestCtx, stmt.Options)
	if err != nil {
		return err
	}

	//get query optimizer and execute Optimize
	exPlan, err := buildPlan(requestCtx, ses, ses.GetTxnCompileCtx(), stmt.Statement)
	if err != nil {
		return err
	}
	if exPlan.GetDcl() != nil && exPlan.GetDcl().GetExecute() != nil {
		//replace the plan of the EXECUTE by the plan generated by the PREPARE
		execPlan := exPlan.GetDcl().GetExecute()
		replaced, _, err := ses.GetTxnCompileCtx().ReplacePlan(execPlan)
		if err != nil {
			return err
		}

		exPlan = replaced
		paramVals := ses.GetTxnCompileCtx().tcw.paramVals
		if len(paramVals) > 0 {
			//replace the param var in the plan by the param value
			exPlan, err = plan2.FillValuesOfParamsInPlan(requestCtx, exPlan, paramVals)
			if err != nil {
				return err
			}
			if exPlan == nil {
				return moerr.NewInternalError(requestCtx, "failed to copy exPlan")
			}
		}
	}
	if exPlan.GetQuery() == nil {
		return moerr.NewNotSupported(requestCtx, "the sql query plan does not support explain.")
	}
	// generator query explain
	explainQuery := explain.NewExplainQueryImpl(exPlan.GetQuery())

	// build explain data buffer
	buffer := explain.NewExplainDataBuffer()
	err = explainQuery.ExplainPlan(requestCtx, buffer, es)
	if err != nil {
		return err
	}

	//2. fill the result set
	explainColName := "QUERY PLAN"
	//column
	col1 := new(MysqlColumn)
	col1.SetColumnType(defines.MYSQL_TYPE_VAR_STRING)
	col1.SetName(explainColName)

	mrs := ses.GetMysqlResultSet()
	mrs.AddColumn(col1)

	for _, line := range buffer.Lines {
		mrs.AddRow([]any{line})
	}
	ses.rs = mysqlColDef2PlanResultColDef(mrs)

	if openSaveQueryResult(ses) {
		//3. fill the batch for saving the query result
		bat, err := fillQueryBatch(ses, explainColName, buffer.Lines)
		defer bat.Clean(ses.GetMemPool())
		if err != nil {
			return err
		}

		// save query result
		err = maySaveQueryResult(ses, bat)
		if err != nil {

			return err
		}
	}
	return nil
}

func fillQueryBatch(ses *Session, explainColName string, lines []string) (*batch.Batch, error) {
	bat := batch.New(true, []string{explainColName})
	typ := types.New(types.T_varchar, types.MaxVarcharLen, 0)

	cnt := len(lines)
	bat.SetRowCount(cnt)
	bat.Vecs[0] = vector.NewVec(typ)
	err := vector.AppendStringList(bat.Vecs[0], lines, nil, ses.GetMemPool())
	if err != nil {
		return nil, err
	}
	return bat, nil
}

// Note: for pass the compile quickly. We will remove the comments in the future.
func handleExplainStmt(_ context.Context, ses FeSession, stmt *tree.ExplainStmt) error {
	return doExplainStmt(ses.(*Session), stmt)
}

func doPrepareStmt(ctx context.Context, ses *Session, st *tree.PrepareStmt, sql string, paramTypes []byte) (*PrepareStmt, error) {
	preparePlan, err := buildPlan(ctx, ses, ses.GetTxnCompileCtx(), st)
	if err != nil {
		return nil, err
	}

	prepareStmt := &PrepareStmt{
		Name:                preparePlan.GetDcl().GetPrepare().GetName(),
		Sql:                 sql,
		PreparePlan:         preparePlan,
		PrepareStmt:         st.Stmt,
		getFromSendLongData: make(map[int]struct{}),
	}
	if len(paramTypes) > 0 {
		prepareStmt.ParamTypes = paramTypes
	}
	prepareStmt.InsertBat = ses.GetTxnCompileCtx().GetProcess().GetPrepareBatch()
	err = ses.SetPrepareStmt(preparePlan.GetDcl().GetPrepare().GetName(), prepareStmt)

	return prepareStmt, err
}

// handlePrepareStmt
func handlePrepareStmt(ctx context.Context, ses FeSession, st *tree.PrepareStmt, sql string) (*PrepareStmt, error) {
	return doPrepareStmt(ctx, ses.(*Session), st, sql, nil)
}

func doPrepareString(ctx context.Context, ses *Session, st *tree.PrepareString) (*PrepareStmt, error) {
	v, err := ses.GetGlobalVar("lower_case_table_names")
	if err != nil {
		return nil, err
	}

	origin, err := ses.GetGlobalVar("keep_user_target_list_in_result")
	if err != nil {
		return nil, err
	}

	stmts, err := mysql.Parse(ctx, st.Sql, v.(int64), origin.(int64))
	if err != nil {
		return nil, err
	}

	preparePlan, err := buildPlan(ses.GetRequestContext(), ses, ses.GetTxnCompileCtx(), st)
	if err != nil {
		return nil, err
	}
	prepareStmt := &PrepareStmt{
		Name:        preparePlan.GetDcl().GetPrepare().GetName(),
		Sql:         st.Sql,
		PreparePlan: preparePlan,
		PrepareStmt: stmts[0],
	}
	prepareStmt.InsertBat = ses.GetTxnCompileCtx().GetProcess().GetPrepareBatch()
	err = ses.SetPrepareStmt(preparePlan.GetDcl().GetPrepare().GetName(), prepareStmt)
	return prepareStmt, err
}

// handlePrepareString
func handlePrepareString(ctx context.Context, ses FeSession, st *tree.PrepareString) (*PrepareStmt, error) {
	return doPrepareString(ctx, ses.(*Session), st)
}

func doDeallocate(ctx context.Context, ses *Session, st *tree.Deallocate) error {
	deallocatePlan, err := buildPlan(ctx, ses, ses.GetTxnCompileCtx(), st)
	if err != nil {
		return err
	}
	ses.RemovePrepareStmt(deallocatePlan.GetDcl().GetDeallocate().GetName())
	return nil
}

func doReset(_ context.Context, _ *Session, _ *tree.Reset) error {
	return nil
}

// handleDeallocate
func handleDeallocate(ctx context.Context, ses FeSession, st *tree.Deallocate) error {
	return doDeallocate(ctx, ses.(*Session), st)
}

// handleReset
func handleReset(ctx context.Context, ses FeSession, st *tree.Reset) error {
	return doReset(ctx, ses.(*Session), st)
}

func handleCreatePublication(ctx context.Context, ses FeSession, cp *tree.CreatePublication) error {
	return doCreatePublication(ctx, ses.(*Session), cp)
}

func handleAlterPublication(ctx context.Context, ses FeSession, ap *tree.AlterPublication) error {
	return doAlterPublication(ctx, ses.(*Session), ap)
}

func handleDropPublication(ctx context.Context, ses FeSession, dp *tree.DropPublication) error {
	return doDropPublication(ctx, ses.(*Session), dp)
}

func handleCreateStage(ctx context.Context, ses FeSession, cs *tree.CreateStage) error {
	return doCreateStage(ctx, ses.(*Session), cs)
}

func handleAlterStage(ctx context.Context, ses FeSession, as *tree.AlterStage) error {
	return doAlterStage(ctx, ses.(*Session), as)
}

func handleDropStage(ctx context.Context, ses FeSession, ds *tree.DropStage) error {
	return doDropStage(ctx, ses.(*Session), ds)
}

func handleCreateSnapshot(ctx context.Context, ses *Session, ct *tree.CreateSnapShot) error {
	return doCreateSnapshot(ctx, ses, ct)
}

func handleDropSnapshot(ctx context.Context, ses *Session, ct *tree.DropSnapShot) error {
	return doDropSnapshot(ctx, ses, ct)
}

// handleCreateAccount creates a new user-level tenant in the context of the tenant SYS
// which has been initialized.
func handleCreateAccount(ctx context.Context, ses FeSession, ca *tree.CreateAccount, proc *process.Process) error {
	//step1 : create new account.
	create := &createAccount{
		IfNotExists:  ca.IfNotExists,
		IdentTyp:     ca.AuthOption.IdentifiedType.Typ,
		StatusOption: ca.StatusOption,
		Comment:      ca.Comment,
	}

	b := strParamBinder{
		ctx:    ctx,
		params: proc.GetPrepareParams(),
	}
	create.Name = b.bind(ca.Name)
	create.AdminName = b.bind(ca.AuthOption.AdminName)
	create.IdentStr = b.bindIdentStr(&ca.AuthOption.IdentifiedType)
	if b.err != nil {
		return b.err
	}

	return InitGeneralTenant(ctx, ses.(*Session), create)
}

func handleDropAccount(ctx context.Context, ses FeSession, da *tree.DropAccount, proc *process.Process) error {
	drop := &dropAccount{
		IfExists: da.IfExists,
	}

	b := strParamBinder{
		ctx:    ctx,
		params: proc.GetPrepareParams(),
	}
	drop.Name = b.bind(da.Name)
	if b.err != nil {
		return b.err
	}

	return doDropAccount(ctx, ses.(*Session), drop)
}

// handleDropAccount drops a new user-level tenant
func handleAlterAccount(ctx context.Context, ses FeSession, st *tree.AlterAccount, proc *process.Process) error {
	aa := &alterAccount{
		IfExists:     st.IfExists,
		StatusOption: st.StatusOption,
		Comment:      st.Comment,
	}

	b := strParamBinder{
		ctx:    ctx,
		params: proc.GetPrepareParams(),
	}

	aa.Name = b.bind(st.Name)
	if st.AuthOption.Exist {
		aa.AuthExist = true
		aa.AdminName = b.bind(st.AuthOption.AdminName)
		aa.IdentTyp = st.AuthOption.IdentifiedType.Typ
		aa.IdentStr = b.bindIdentStr(&st.AuthOption.IdentifiedType)
	}
	if b.err != nil {
		return b.err
	}

	return doAlterAccount(ctx, ses.(*Session), aa)
}

// handleAlterDatabaseConfig alter a database's mysql_compatibility_mode
func handleAlterDataBaseConfig(ctx context.Context, ses FeSession, ad *tree.AlterDataBaseConfig) error {
	return doAlterDatabaseConfig(ctx, ses.(*Session), ad)
}

// handleAlterAccountConfig alter a account's mysql_compatibility_mode
func handleAlterAccountConfig(ctx context.Context, ses FeSession, st *tree.AlterDataBaseConfig) error {
	return doAlterAccountConfig(ctx, ses.(*Session), st)
}

// handleCreateUser creates the user for the tenant
func handleCreateUser(ctx context.Context, ses FeSession, st *tree.CreateUser) error {
	tenant := ses.GetTenantInfo()

	cu := &createUser{
		IfNotExists:        st.IfNotExists,
		Role:               st.Role,
		Users:              make([]*user, 0, len(st.Users)),
		MiscOpt:            st.MiscOpt,
		CommentOrAttribute: st.CommentOrAttribute,
	}

	for _, u := range st.Users {
		v := user{
			Username: u.Username,
			Hostname: u.Hostname,
		}
		if u.AuthOption != nil {
			v.AuthExist = true
			v.IdentTyp = u.AuthOption.Typ
			switch v.IdentTyp {
			case tree.AccountIdentifiedByPassword,
				tree.AccountIdentifiedWithSSL:
				var err error
				v.IdentStr, err = unboxExprStr(ctx, u.AuthOption.Str)
				if err != nil {
					return err
				}
			}
		}
		cu.Users = append(cu.Users, &v)
	}

	//step1 : create the user
	return InitUser(ctx, ses.(*Session), tenant, cu)
}

// handleDropUser drops the user for the tenant
func handleDropUser(ctx context.Context, ses FeSession, du *tree.DropUser) error {
	return doDropUser(ctx, ses.(*Session), du)
}

func handleAlterUser(ctx context.Context, ses FeSession, st *tree.AlterUser) error {
	if len(st.Users) != 1 {
		return moerr.NewInternalError(ctx, "can only alter one user at a time")
	}
	su := st.Users[0]

	u := &user{
		Username: su.Username,
		Hostname: su.Hostname,
	}
	if su.AuthOption != nil {
		u.AuthExist = true
		u.IdentTyp = su.AuthOption.Typ
		switch u.IdentTyp {
		case tree.AccountIdentifiedByPassword,
			tree.AccountIdentifiedWithSSL:
			var err error
			u.IdentStr, err = unboxExprStr(ctx, su.AuthOption.Str)
			if err != nil {
				return err
			}
		}
	}

	au := &alterUser{
		IfExists: st.IfExists,
		User:     u,
		Role:     st.Role,
		MiscOpt:  st.MiscOpt,

		CommentOrAttribute: st.CommentOrAttribute,
	}

	return doAlterUser(ctx, ses.(*Session), au)
}

// handleCreateRole creates the new role
func handleCreateRole(ctx context.Context, ses FeSession, cr *tree.CreateRole) error {
	tenant := ses.GetTenantInfo()

	//step1 : create the role
	return InitRole(ctx, ses.(*Session), tenant, cr)
}

// handleDropRole drops the role
func handleDropRole(ctx context.Context, ses FeSession, dr *tree.DropRole) error {
	return doDropRole(ctx, ses.(*Session), dr)
}

func handleCreateFunction(ctx context.Context, ses FeSession, cf *tree.CreateFunction) error {
	tenant := ses.GetTenantInfo()
	return InitFunction(ctx, ses.(*Session), tenant, cf)
}

func handleDropFunction(ctx context.Context, ses FeSession, df *tree.DropFunction, proc *process.Process) error {
	return doDropFunction(ctx, ses.(*Session), df, func(path string) error {
		return proc.FileService.Delete(ctx, path)
	})
}
func handleCreateProcedure(ctx context.Context, ses FeSession, cp *tree.CreateProcedure) error {
	tenant := ses.GetTenantInfo()

	return InitProcedure(ctx, ses.(*Session), tenant, cp)
}

func handleDropProcedure(ctx context.Context, ses FeSession, dp *tree.DropProcedure) error {
	return doDropProcedure(ctx, ses.(*Session), dp)
}

func handleCallProcedure(ctx context.Context, ses FeSession, call *tree.CallStmt, proc *process.Process) error {
	proto := ses.GetMysqlProtocol()
	results, err := doInterpretCall(ctx, ses.(*Session), call)
	if err != nil {
		return err
	}

	ses.SetMysqlResultSet(nil)

	resp := NewGeneralOkResponse(COM_QUERY, ses.GetTxnHandler().GetServerStatus())

	if len(results) == 0 {
		if err := proto.SendResponse(ses.GetRequestContext(), resp); err != nil {
			return moerr.NewInternalError(ses.GetRequestContext(), "routine send response failed. error:%v ", err)
		}
	} else {
		for i, result := range results {
			mer := NewMysqlExecutionResult(0, 0, 0, 0, result.(*MysqlResultSet))
			resp = ses.SetNewResponse(ResultResponse, 0, int(COM_QUERY), mer, i == len(results)-1)
			if err := proto.SendResponse(ses.GetRequestContext(), resp); err != nil {
				return moerr.NewInternalError(ses.GetRequestContext(), "routine send response failed. error:%v ", err)
			}
		}
	}
	return nil
}

// handleGrantRole grants the role
func handleGrantRole(ctx context.Context, ses FeSession, gr *tree.GrantRole) error {
	return doGrantRole(ctx, ses.(*Session), gr)
}

// handleRevokeRole revokes the role
func handleRevokeRole(ctx context.Context, ses FeSession, rr *tree.RevokeRole) error {
	return doRevokeRole(ctx, ses.(*Session), rr)
}

// handleGrantRole grants the privilege to the role
func handleGrantPrivilege(ctx context.Context, ses FeSession, gp *tree.GrantPrivilege) error {
	return doGrantPrivilege(ctx, ses, gp)
}

// handleRevokePrivilege revokes the privilege from the user or role
func handleRevokePrivilege(ctx context.Context, ses FeSession, rp *tree.RevokePrivilege) error {
	return doRevokePrivilege(ctx, ses, rp)
}

// handleSwitchRole switches the role to another role
func handleSwitchRole(ctx context.Context, ses FeSession, sr *tree.SetRole) error {
	return doSwitchRole(ctx, ses.(*Session), sr)
}

func doKill(ctx context.Context, ses *Session, k *tree.Kill) error {
	var err error
	//true: kill a connection
	//false: kill a query in a connection
	idThatKill := uint64(ses.GetConnectionID())
	if !k.Option.Exist || k.Option.Typ == tree.KillTypeConnection {
		err = globalRtMgr.kill(ctx, true, idThatKill, k.ConnectionId, "")
	} else {
		err = globalRtMgr.kill(ctx, false, idThatKill, k.ConnectionId, k.StmtOption.StatementId)
	}
	return err
}

// handleKill kill a connection or query
func handleKill(ctx context.Context, ses *Session, k *tree.Kill) error {
	err := doKill(ctx, ses, k)
	if err != nil {
		return err
	}
	return err
}

// handleShowAccounts lists the info of accounts
func handleShowAccounts(ctx context.Context, ses FeSession, sa *tree.ShowAccounts, isLastStmt bool) error {
	err := doShowAccounts(ctx, ses.(*Session), sa)
	if err != nil {
		return err
	}
	return err
}

// handleShowCollation lists the info of collation
func handleShowCollation(ses FeSession, sc *tree.ShowCollation, proc *process.Process, isLastStmt bool) error {
	err := doShowCollation(ses.(*Session), proc, sc)
	if err != nil {
		return err
	}
	return err
}

func doShowCollation(ses *Session, proc *process.Process, sc *tree.ShowCollation) error {
	var err error
	var bat *batch.Batch
	// var outputBatches []*batch.Batch

	// Construct the columns.
	col1 := new(MysqlColumn)
	col1.SetColumnType(defines.MYSQL_TYPE_VAR_STRING)
	col1.SetName("Collation")

	col2 := new(MysqlColumn)
	col2.SetColumnType(defines.MYSQL_TYPE_VAR_STRING)
	col2.SetName("Charset")

	col3 := new(MysqlColumn)
	col3.SetColumnType(defines.MYSQL_TYPE_LONGLONG)
	col3.SetName("Id")

	col4 := new(MysqlColumn)
	col4.SetColumnType(defines.MYSQL_TYPE_VAR_STRING)
	col4.SetName("Default")

	col5 := new(MysqlColumn)
	col5.SetColumnType(defines.MYSQL_TYPE_VAR_STRING)
	col5.SetName("Compiled")

	col6 := new(MysqlColumn)
	col6.SetColumnType(defines.MYSQL_TYPE_LONG)
	col6.SetName("Sortlen")

	col7 := new(MysqlColumn)
	col7.SetColumnType(defines.MYSQL_TYPE_VAR_STRING)
	col7.SetName("Pad_attribute")

	mrs := ses.GetMysqlResultSet()
	mrs.AddColumn(col1)
	mrs.AddColumn(col2)
	mrs.AddColumn(col3)
	mrs.AddColumn(col4)
	mrs.AddColumn(col5)
	mrs.AddColumn(col6)
	mrs.AddColumn(col7)

	var hasLike = false
	var likePattern = ""
	var isIlike = false
	if sc.Like != nil {
		hasLike = true
		if sc.Like.Op == tree.ILIKE {
			isIlike = true
		}
		likePattern = strings.ToLower(sc.Like.Right.String())
	}

	// Construct the rows.
	rows := make([][]interface{}, 0, len(Collations))
	for _, collation := range Collations {
		if hasLike {
			s := collation.collationName
			if isIlike {
				s = strings.ToLower(s)
			}
			if !WildcardMatch(likePattern, s) {
				continue
			}
		}
		row := make([]interface{}, 7)
		row[0] = collation.collationName
		row[1] = collation.charset
		row[2] = collation.id
		row[3] = collation.isDefault
		row[4] = collation.isCompiled
		row[5] = collation.sortLen
		row[6] = collation.padAttribute
		rows = append(rows, row)
	}

	bat, err = constructCollationBatch(ses, rows)
	defer bat.Clean(proc.Mp())
	if err != nil {
		return err
	}

	if sc.Where != nil {
		binder := plan2.NewDefaultBinder(proc.Ctx, nil, nil, &plan2.Type{Id: int32(types.T_varchar), Width: types.MaxVarcharLen}, []string{"collation", "charset", "id", "default", "compiled", "sortlen", "pad_attribute"})
		planExpr, err := binder.BindExpr(sc.Where.Expr, 0, false)
		if err != nil {
			return err
		}

		executor, err := colexec.NewExpressionExecutor(proc, planExpr)
		if err != nil {
			return err
		}
		vec, err := executor.Eval(proc, []*batch.Batch{bat})
		if err != nil {
			executor.Free()
			return err
		}

		bs := vector.MustFixedCol[bool](vec)
		sels := proc.Mp().GetSels()
		for i, b := range bs {
			if b {
				sels = append(sels, int64(i))
			}
		}
		executor.Free()

		bat.Shrink(sels, false)
		proc.Mp().PutSels(sels)
		v0 := vector.MustStrCol(bat.Vecs[0])
		v1 := vector.MustStrCol(bat.Vecs[1])
		v2 := vector.MustFixedCol[int64](bat.Vecs[2])
		v3 := vector.MustStrCol(bat.Vecs[3])
		v4 := vector.MustStrCol(bat.Vecs[4])
		v5 := vector.MustFixedCol[int32](bat.Vecs[5])
		v6 := vector.MustStrCol(bat.Vecs[6])
		rows = rows[:len(v0)]
		for i := range v0 {
			rows[i][0] = v0[i]
			rows[i][1] = v1[i]
			rows[i][2] = v2[i]
			rows[i][3] = v3[i]
			rows[i][4] = v4[i]
			rows[i][5] = v5[i]
			rows[i][6] = v6[i]
		}
	}

	//sort by name
	sort.Slice(rows, func(i, j int) bool {
		return rows[i][0].(string) < rows[j][0].(string)
	})

	for _, row := range rows {
		mrs.AddRow(row)
	}

	// oq := newFakeOutputQueue(mrs)
	// if err = fillResultSet(oq, bat, ses); err != nil {
	// 	return err
	// }

	ses.SetMysqlResultSet(mrs)
	ses.rs = mysqlColDef2PlanResultColDef(mrs)

	// save query result
	if openSaveQueryResult(ses) {
		if err := saveQueryResult(ses, bat); err != nil {
			return err
		}
		if err := saveQueryResultMeta(ses); err != nil {
			return err
		}
	}

	return err
}

func handleShowSubscriptions(ctx context.Context, ses FeSession, ss *tree.ShowSubscriptions, isLastStmt bool) error {
	err := doShowSubscriptions(ctx, ses.(*Session), ss)
	if err != nil {
		return err
	}
	return err
}

func doShowBackendServers(ses *Session) error {
	// Construct the columns.
	col1 := new(MysqlColumn)
	col1.SetColumnType(defines.MYSQL_TYPE_VARCHAR)
	col1.SetName("UUID")

	col2 := new(MysqlColumn)
	col2.SetColumnType(defines.MYSQL_TYPE_VARCHAR)
	col2.SetName("Address")

	col3 := new(MysqlColumn)
	col3.SetColumnType(defines.MYSQL_TYPE_VARCHAR)
	col3.SetName("Work State")

	col4 := new(MysqlColumn)
	col4.SetColumnType(defines.MYSQL_TYPE_VARCHAR)
	col4.SetName("Labels")

	mrs := ses.GetMysqlResultSet()
	mrs.AddColumn(col1)
	mrs.AddColumn(col2)
	mrs.AddColumn(col3)
	mrs.AddColumn(col4)

	var filterLabels = func(labels map[string]string) map[string]string {
		var reservedLabels = map[string]struct{}{
			"os_user":      {},
			"os_sudouser":  {},
			"program_name": {},
		}
		for k := range labels {
			if _, ok := reservedLabels[k]; ok || strings.HasPrefix(k, "_") {
				delete(labels, k)
			}
		}
		return labels
	}

	var appendFn = func(s *metadata.CNService) {
		row := make([]interface{}, 4)
		row[0] = s.ServiceID
		row[1] = s.SQLAddress
		row[2] = s.WorkState.String()
		var labelStr string
		for key, value := range s.Labels {
			labelStr += fmt.Sprintf("%s:%s;", key, strings.Join(value.Labels, ","))
		}
		row[3] = labelStr
		mrs.AddRow(row)
	}

	tenant := ses.GetTenantInfo().GetTenant()
	var se clusterservice.Selector
	labels, err := ParseLabel(getLabelPart(ses.GetUserName()))
	if err != nil {
		return err
	}
	labels["account"] = tenant
	se = clusterservice.NewSelector().SelectByLabel(
		filterLabels(labels), clusterservice.Contain)
	if isSysTenant(tenant) {
		u := ses.GetTenantInfo().GetUser()
		// For super use dump and root, we should list all servers.
		if isSuperUser(u) {
			clusterservice.GetMOCluster().GetCNService(
				clusterservice.NewSelectAll(), func(s metadata.CNService) bool {
					appendFn(&s)
					return true
				})
		} else {
			route.RouteForSuperTenant(se, u, nil, appendFn)
		}
	} else {
		route.RouteForCommonTenant(se, nil, appendFn)
	}
	return nil
}

func handleShowBackendServers(ctx context.Context, ses FeSession, isLastStmt bool) error {
	var err error
	if err := doShowBackendServers(ses.(*Session)); err != nil {
		return err
	}
	return err
}

func handleEmptyStmt(ctx context.Context, ses FeSession, stmt *tree.EmptyStmt) error {
	var err error
	return err
}

func GetExplainColumns(ctx context.Context, explainColName string) ([]interface{}, error) {
	cols := []*plan2.ColDef{
		{Typ: plan2.Type{Id: int32(types.T_varchar)}, Name: explainColName},
	}
	columns := make([]interface{}, len(cols))
	var err error = nil
	for i, col := range cols {
		c := new(MysqlColumn)
		c.SetName(col.Name)
		err = convertEngineTypeToMysqlType(ctx, types.T(col.Typ.Id), c)
		if err != nil {
			return nil, err
		}
		columns[i] = c
	}
	return columns, err
}

func getExplainOption(requestCtx context.Context, options []tree.OptionElem) (*explain.ExplainOptions, error) {
	es := explain.NewExplainDefaultOptions()
	if options == nil {
		return es, nil
	} else {
		for _, v := range options {
			if strings.EqualFold(v.Name, "VERBOSE") {
				if strings.EqualFold(v.Value, "TRUE") || v.Value == "NULL" {
					es.Verbose = true
				} else if strings.EqualFold(v.Value, "FALSE") {
					es.Verbose = false
				} else {
					return nil, moerr.NewInvalidInput(requestCtx, "invalid explain option '%s', valud '%s'", v.Name, v.Value)
				}
			} else if strings.EqualFold(v.Name, "ANALYZE") {
				if strings.EqualFold(v.Value, "TRUE") || v.Value == "NULL" {
					es.Analyze = true
				} else if strings.EqualFold(v.Value, "FALSE") {
					es.Analyze = false
				} else {
					return nil, moerr.NewInvalidInput(requestCtx, "invalid explain option '%s', valud '%s'", v.Name, v.Value)
				}
			} else if strings.EqualFold(v.Name, "FORMAT") {
				if strings.EqualFold(v.Value, "TEXT") {
					es.Format = explain.EXPLAIN_FORMAT_TEXT
				} else if strings.EqualFold(v.Value, "JSON") {
					return nil, moerr.NewNotSupported(requestCtx, "Unsupport explain format '%s'", v.Value)
				} else if strings.EqualFold(v.Value, "DOT") {
					return nil, moerr.NewNotSupported(requestCtx, "Unsupport explain format '%s'", v.Value)
				} else {
					return nil, moerr.NewInvalidInput(requestCtx, "invalid explain option '%s', valud '%s'", v.Name, v.Value)
				}
			} else {
				return nil, moerr.NewInvalidInput(requestCtx, "invalid explain option '%s', valud '%s'", v.Name, v.Value)
			}
		}
		return es, nil
	}
}

func buildMoExplainQuery(explainColName string, buffer *explain.ExplainDataBuffer, session *Session, fill func(interface{}, *batch.Batch) error) error {
	bat := batch.New(true, []string{explainColName})
	rs := buffer.Lines
	vs := make([][]byte, len(rs))

	count := 0
	for _, r := range rs {
		str := []byte(r)
		vs[count] = str
		count++
	}
	vs = vs[:count]
	vec := vector.NewVec(types.T_varchar.ToType())
	defer vec.Free(session.GetMemPool())
	vector.AppendBytesList(vec, vs, nil, session.GetMemPool())
	bat.Vecs[0] = vec
	bat.SetRowCount(count)

	err := fill(session, bat)
	if err != nil {
		return err
	}
	// to trigger save result meta
	err = fill(session, nil)
	return err
}

func buildPlan(requestCtx context.Context, ses FeSession, ctx plan2.CompilerContext, stmt tree.Statement) (*plan2.Plan, error) {
	var ret *plan2.Plan
	var err error

	txnOp := ctx.GetProcess().TxnOperator
	start := time.Now()
	seq := uint64(0)
	if txnOp != nil {
		seq = txnOp.NextSequence()
		txnTrace.GetService().AddTxnDurationAction(
			txnOp,
			client.BuildPlanEvent,
			seq,
			0,
			0,
			err)
	}

	defer func() {
		cost := time.Since(start)

		if txnOp != nil {
			txnTrace.GetService().AddTxnDurationAction(
				txnOp,
				client.BuildPlanEvent,
				seq,
				0,
				cost,
				err)
		}
		v2.TxnStatementBuildPlanDurationHistogram.Observe(cost.Seconds())
	}()

	stats := statistic.StatsInfoFromContext(requestCtx)
	stats.PlanStart()
	defer stats.PlanEnd()

	isPrepareStmt := false
	if ses != nil {
		var accId uint32
		accId, err = defines.GetAccountId(requestCtx)
		if err != nil {
			return nil, err
		}
		ses.SetAccountId(accId)
		if len(ses.GetSql()) > 8 {
			prefix := strings.ToLower(ses.GetSql()[:8])
			isPrepareStmt = prefix == "execute " || prefix == "prepare "
		}
	}
	if s, ok := stmt.(*tree.Insert); ok {
		if _, ok := s.Rows.Select.(*tree.ValuesClause); ok {
			ret, err = plan2.BuildPlan(ctx, stmt, isPrepareStmt)
			if err != nil {
				return nil, err
			}
		}
	}
	if ret != nil {
		if ses != nil && ses.GetTenantInfo() != nil && !ses.IsBackgroundSession() {
			err = authenticateCanExecuteStatementAndPlan(requestCtx, ses.(*Session), stmt, ret)
			if err != nil {
				return nil, err
			}
		}
		return ret, err
	}
	switch stmt := stmt.(type) {
	case *tree.Select, *tree.ParenSelect, *tree.ValuesStatement,
		*tree.Update, *tree.Delete, *tree.Insert,
		*tree.ShowDatabases, *tree.ShowTables, *tree.ShowSequences, *tree.ShowColumns, *tree.ShowColumnNumber, *tree.ShowTableNumber,
		*tree.ShowCreateDatabase, *tree.ShowCreateTable, *tree.ShowIndex,
		*tree.ExplainStmt, *tree.ExplainAnalyze:
		opt := plan2.NewBaseOptimizer(ctx)
		optimized, err := opt.Optimize(stmt, isPrepareStmt)
		if err != nil {
			return nil, err
		}
		ret = &plan2.Plan{
			Plan: &plan2.Plan_Query{
				Query: optimized,
			},
		}
	default:
		ret, err = plan2.BuildPlan(ctx, stmt, isPrepareStmt)
	}
	if ret != nil {
		ret.IsPrepare = isPrepareStmt
		if ses != nil && ses.GetTenantInfo() != nil && !ses.IsBackgroundSession() {
			err = authenticateCanExecuteStatementAndPlan(requestCtx, ses.(*Session), stmt, ret)
			if err != nil {
				return nil, err
			}
		}
	}
	return ret, err
}

func checkModify(plan2 *plan.Plan, proc *process.Process, ses *Session) bool {
	if plan2 == nil {
		return true
	}
	checkFn := func(db string, tableName string, tableId uint64, version uint32) bool {
		_, tableDef := ses.GetTxnCompileCtx().Resolve(db, tableName, timestamp.Timestamp{})
		if tableDef == nil {
			return true
		}
		if tableDef.Version != version || tableDef.TblId != tableId {
			return true
		}
		return false
	}
	switch p := plan2.Plan.(type) {
	case *plan.Plan_Query:
		for i := range p.Query.Nodes {
			if def := p.Query.Nodes[i].TableDef; def != nil {
				if p.Query.Nodes[i].ObjRef == nil || checkFn(p.Query.Nodes[i].ObjRef.SchemaName, def.Name, def.TblId, def.Version) {
					return true
				}
			}
			if ctx := p.Query.Nodes[i].InsertCtx; ctx != nil {
				if ctx.Ref == nil || checkFn(ctx.Ref.SchemaName, ctx.TableDef.Name, ctx.TableDef.TblId, ctx.TableDef.Version) {
					return true
				}
			}
			if ctx := p.Query.Nodes[i].ReplaceCtx; ctx != nil {
				if ctx.Ref == nil || checkFn(ctx.Ref.SchemaName, ctx.TableDef.Name, ctx.TableDef.TblId, ctx.TableDef.Version) {
					return true
				}
			}
			if ctx := p.Query.Nodes[i].DeleteCtx; ctx != nil {
				if ctx.Ref == nil || checkFn(ctx.Ref.SchemaName, ctx.TableDef.Name, ctx.TableDef.TblId, ctx.TableDef.Version) {
					return true
				}
			}
			if ctx := p.Query.Nodes[i].PreInsertCtx; ctx != nil {
				if ctx.Ref == nil || checkFn(ctx.Ref.SchemaName, ctx.TableDef.Name, ctx.TableDef.TblId, ctx.TableDef.Version) {
					return true
				}
			}
			if ctx := p.Query.Nodes[i].PreInsertCtx; ctx != nil {
				if ctx.Ref == nil || checkFn(ctx.Ref.SchemaName, ctx.TableDef.Name, ctx.TableDef.TblId, ctx.TableDef.Version) {
					return true
				}
			}
			if ctx := p.Query.Nodes[i].OnDuplicateKey; ctx != nil {
				if p.Query.Nodes[i].ObjRef == nil || checkFn(p.Query.Nodes[i].ObjRef.SchemaName, ctx.TableName, ctx.TableId, ctx.TableVersion) {
					return true
				}
			}
		}
	default:
	}
	return false
}

/*
GetComputationWrapper gets the execs from the computation engine
*/
var GetComputationWrapper = func(db string, input *UserInput, user string, eng engine.Engine, proc *process.Process, ses *Session) ([]ComputationWrapper, error) {
	var cw []ComputationWrapper = nil
	if cached := ses.getCachedPlan(input.getSql()); cached != nil {
		modify := false
		for i, stmt := range cached.stmts {
			tcw := InitTxnComputationWrapper(ses, stmt, proc)
			tcw.plan = cached.plans[i]
			if tcw.plan == nil {
				modify = true
				break
			}
			if checkModify(tcw.plan, proc, ses) {
				modify = true
				break
			}
			cw = append(cw, tcw)
		}
		if modify {
			cw = nil
		} else {
			return cw, nil
		}
	}

	var stmts []tree.Statement = nil
	var cmdFieldStmt *InternalCmdFieldList
	var err error
	// if the input is an option ast, we should use it directly
	if input.getStmt() != nil {
		stmts = append(stmts, input.getStmt())
	} else if isCmdFieldListSql(input.getSql()) {
		cmdFieldStmt, err = parseCmdFieldList(proc.Ctx, input.getSql())
		if err != nil {
			return nil, err
		}
		stmts = append(stmts, cmdFieldStmt)
	} else {
		var v interface{}
		var origin interface{}
		v, err = ses.GetGlobalVar("lower_case_table_names")
		if err != nil {
			v = int64(1)
		}
		origin, err = ses.GetGlobalVar("keep_user_target_list_in_result")
		if err != nil {
			origin = int64(0)
		}
		stmts, err = parsers.Parse(proc.Ctx, dialect.MYSQL, input.getSql(), v.(int64), origin.(int64))
		if err != nil {
			return nil, err
		}
	}

	for _, stmt := range stmts {
		cw = append(cw, InitTxnComputationWrapper(ses, stmt, proc))
	}
	return cw, nil
}

func incTransactionCounter(tenant string) {
	metric.TransactionCounter(tenant).Inc()
}

func incTransactionErrorsCounter(tenant string, t metric.SQLType) {
	if t == metric.SQLTypeRollback {
		return
	}
	metric.TransactionErrorsCounter(tenant, t).Inc()
}

func incStatementErrorsCounter(tenant string, stmt tree.Statement) {
	metric.StatementErrorsCounter(tenant, getStatementType(stmt).GetQueryType()).Inc()
}

// authenticateUserCanExecuteStatement checks the user can execute the statement
func authenticateUserCanExecuteStatement(requestCtx context.Context, ses *Session, stmt tree.Statement) error {
	requestCtx, span := trace.Debug(requestCtx, "authenticateUserCanExecuteStatement")
	defer span.End()
	if getGlobalPu().SV.SkipCheckPrivilege {
		return nil
	}

	if ses.skipAuthForSpecialUser() {
		return nil
	}
	var havePrivilege bool
	var err error
	if ses.GetTenantInfo() != nil {
		ses.SetPrivilege(determinePrivilegeSetOfStatement(stmt))

		// can or not execute in retricted status
		if ses.getRoutine() != nil && ses.getRoutine().isRestricted() && !ses.GetPrivilege().canExecInRestricted {
			return moerr.NewInternalError(requestCtx, "do not have privilege to execute the statement")
		}

		havePrivilege, err = authenticateUserCanExecuteStatementWithObjectTypeAccountAndDatabase(requestCtx, ses, stmt)
		if err != nil {
			return err
		}

		if !havePrivilege {
			err = moerr.NewInternalError(requestCtx, "do not have privilege to execute the statement")
			return err
		}

		havePrivilege, err = authenticateUserCanExecuteStatementWithObjectTypeNone(requestCtx, ses, stmt)
		if err != nil {
			return err
		}

		if !havePrivilege {
			err = moerr.NewInternalError(requestCtx, "do not have privilege to execute the statement")
			return err
		}
	}
	return err
}

// authenticateCanExecuteStatementAndPlan checks the user can execute the statement and its plan
func authenticateCanExecuteStatementAndPlan(requestCtx context.Context, ses *Session, stmt tree.Statement, p *plan.Plan) error {
	_, task := gotrace.NewTask(context.TODO(), "frontend.authenticateCanExecuteStatementAndPlan")
	defer task.End()
	if getGlobalPu().SV.SkipCheckPrivilege {
		return nil
	}

	if ses.skipAuthForSpecialUser() {
		return nil
	}
	yes, err := authenticateUserCanExecuteStatementWithObjectTypeDatabaseAndTable(requestCtx, ses, stmt, p)
	if err != nil {
		return err
	}
	if !yes {
		return moerr.NewInternalError(requestCtx, "do not have privilege to execute the statement")
	}
	return nil
}

// authenticatePrivilegeOfPrepareAndExecute checks the user can execute the Prepare or Execute statement
func authenticateUserCanExecutePrepareOrExecute(requestCtx context.Context, ses *Session, stmt tree.Statement, p *plan.Plan) error {
	_, task := gotrace.NewTask(context.TODO(), "frontend.authenticateUserCanExecutePrepareOrExecute")
	defer task.End()
	if getGlobalPu().SV.SkipCheckPrivilege {
		return nil
	}
	err := authenticateUserCanExecuteStatement(requestCtx, ses, stmt)
	if err != nil {
		return err
	}
	err = authenticateCanExecuteStatementAndPlan(requestCtx, ses, stmt, p)
	if err != nil {
		return err
	}
	return err
}

// canExecuteStatementInUncommittedTxn checks the user can execute the statement in an uncommitted transaction
func canExecuteStatementInUncommittedTransaction(requestCtx context.Context, ses FeSession, stmt tree.Statement) error {
	can, err := statementCanBeExecutedInUncommittedTransaction(ses, stmt)
	if err != nil {
		return err
	}
	if !can {
		//is ddl statement
		if IsCreateDropDatabase(stmt) {
			return moerr.NewInternalError(requestCtx, createDropDatabaseErrorInfo())
		} else if IsDDL(stmt) {
			return moerr.NewInternalError(requestCtx, onlyCreateStatementErrorInfo())
		} else if IsAdministrativeStatement(stmt) {
			return moerr.NewInternalError(requestCtx, administrativeCommandIsUnsupportedInTxnErrorInfo())
		} else {
			return moerr.NewInternalError(requestCtx, unclassifiedStatementInUncommittedTxnErrorInfo())
		}
	}
	return nil
}

func processLoadLocal(ctx context.Context, ses FeSession, param *tree.ExternParam, writer *io.PipeWriter) (err error) {
	proto := ses.GetMysqlProtocol()
	defer func() {
		err2 := writer.Close()
		if err == nil {
			err = err2
		}
	}()
	err = plan2.InitInfileParam(param)
	if err != nil {
		return
	}
	err = proto.sendLocalInfileRequest(param.Filepath)
	if err != nil {
		return
	}
	start := time.Now()
	var msg interface{}
	msg, err = proto.GetTcpConnection().Read(goetty.ReadOptions{})
	if err != nil {
		proto.SetSequenceID(proto.GetSequenceId() + 1)
		if errors.Is(err, errorInvalidLength0) {
			return nil
		}
		if moerr.IsMoErrCode(err, moerr.ErrInvalidInput) {
			err = moerr.NewInvalidInput(ctx, "cannot read '%s' from client,please check the file path, user privilege and if client start with --local-infile", param.Filepath)
		}
		return
	}

	packet, ok := msg.(*Packet)
	if !ok {
		proto.SetSequenceID(proto.GetSequenceId() + 1)
		err = moerr.NewInvalidInput(ctx, "invalid packet")
		return
	}

	proto.SetSequenceID(uint8(packet.SequenceID + 1))
	seq := uint8(packet.SequenceID + 1)
	length := packet.Length
	if length == 0 {
		return
	}
	ses.CountPayload(len(packet.Payload))

	skipWrite := false
	// If inner error occurs(unexpected or expected(ctrl-c)), proc.LoadLocalReader will be closed.
	// Then write will return error, but we need to read the rest of the data and not write it to pipe.
	// So we need a flag[skipWrite] to tell us whether we need to write the data to pipe.
	// https://github.com/matrixorigin/matrixone/issues/6665#issuecomment-1422236478

	_, err = writer.Write(packet.Payload)
	if err != nil {
		skipWrite = true // next, we just need read the rest of the data,no need to write it to pipe.
		logError(ses, ses.GetDebugString(),
			"Failed to load local file",
			zap.String("path", param.Filepath),
			zap.Error(err))
	}
	epoch, printEvery, minReadTime, maxReadTime, minWriteTime, maxWriteTime := uint64(0), uint64(1024), 24*time.Hour, time.Nanosecond, 24*time.Hour, time.Nanosecond
	for {
		readStart := time.Now()
		msg, err = proto.GetTcpConnection().Read(goetty.ReadOptions{})
		if err != nil {
			if moerr.IsMoErrCode(err, moerr.ErrInvalidInput) {
				seq += 1
				proto.SetSequenceID(seq)
				err = nil
			}
			break
		}
		readTime := time.Since(readStart)
		if readTime > maxReadTime {
			maxReadTime = readTime
		}
		if readTime < minReadTime {
			minReadTime = readTime
		}
		packet, ok = msg.(*Packet)
		if !ok {
			err = moerr.NewInvalidInput(ctx, "invalid packet")
			seq += 1
			proto.SetSequenceID(seq)
			break
		}
		seq = uint8(packet.SequenceID + 1)
		proto.SetSequenceID(seq)
		ses.CountPayload(len(packet.Payload))

		writeStart := time.Now()
		if !skipWrite {
			_, err = writer.Write(packet.Payload)
			if err != nil {
				logError(ses, ses.GetDebugString(),
					"Failed to load local file",
					zap.String("path", param.Filepath),
					zap.Uint64("epoch", epoch),
					zap.Error(err))
				skipWrite = true
			}
			writeTime := time.Since(writeStart)
			if writeTime > maxWriteTime {
				maxWriteTime = writeTime
			}
			if writeTime < minWriteTime {
				minWriteTime = writeTime
			}
		}
		if epoch%printEvery == 0 {
			logDebugf(ses.GetDebugString(), "load local '%s', epoch: %d, skipWrite: %v, minReadTime: %s, maxReadTime: %s, minWriteTime: %s, maxWriteTime: %s,", param.Filepath, epoch, skipWrite, minReadTime.String(), maxReadTime.String(), minWriteTime.String(), maxWriteTime.String())
			minReadTime, maxReadTime, minWriteTime, maxWriteTime = 24*time.Hour, time.Nanosecond, 24*time.Hour, time.Nanosecond
		}
		epoch += 1
	}
	logDebugf(ses.GetDebugString(), "load local '%s', read&write all data from client cost: %s", param.Filepath, time.Since(start))
	return
}

func executeStmtWithResponse(requestCtx context.Context,
	ses *Session,
	execCtx *ExecCtx,
) (err error) {
	var span trace.Span
	requestCtx, span = trace.Start(requestCtx, "executeStmtWithResponse",
		trace.WithKind(trace.SpanKindStatement))
	defer span.End(trace.WithStatementExtra(ses.GetTxnId(), ses.GetStmtId(), ses.GetSqlOfStmt()))

	ses.SetQueryInProgress(true)
	ses.SetQueryStart(time.Now())
	ses.SetQueryInExecute(true)
	defer ses.SetQueryEnd(time.Now())
	defer ses.SetQueryInProgress(false)

	execCtx.proto.DisableAutoFlush()
	defer execCtx.proto.EnableAutoFlush()

	err = executeStmtWithTxn(requestCtx, ses, execCtx)
	if err != nil {
		return err
	}

	// TODO put in one txn
	// insert data after create table in create table ... as select ... stmt
	if ses.createAsSelectSql != "" {
		sql := ses.createAsSelectSql
		ses.createAsSelectSql = ""
		if err = doComQuery(requestCtx, ses, &UserInput{sql: sql}); err != nil {
			return err
		}
	}

	err = respClientWhenSuccess(requestCtx, ses, execCtx)
	if err != nil {
		return err
	}

	err = execCtx.proto.Flush()
	return
}

func executeStmtWithTxn(requestCtx context.Context,
	ses *Session,
	execCtx *ExecCtx,
) (err error) {
	// defer transaction state management.
	defer func() {
		err = finishTxnFunc(requestCtx, ses, err, execCtx)
	}()

	// statement management
	_, txnOp, err := ses.GetTxnHandler().GetTxnOperator()
	if err != nil {
		return err
	}

	//non derived statement
	if txnOp != nil && !ses.IsDerivedStmt() {
		//startStatement has been called
		ok, _ := ses.GetTxnHandler().calledStartStmt()
		if !ok {
			txnOp.GetWorkspace().StartStatement()
			ses.GetTxnHandler().enableStartStmt(txnOp.Txn().ID)
		}
	}

	// defer Start/End Statement management, called after finishTxnFunc()
	defer func() {
		// move finishTxnFunc() out to another defer so that if finishTxnFunc
		// paniced, the following is still called.
		var err3 error
		_, txnOp, err3 = ses.GetTxnHandler().GetTxnOperator()
		if err3 != nil {
			logError(ses, ses.GetDebugString(), err3.Error())
			return
		}
		//non derived statement
		if txnOp != nil && !ses.IsDerivedStmt() {
			//startStatement has been called
			ok, id := ses.GetTxnHandler().calledStartStmt()
			if ok && bytes.Equal(txnOp.Txn().ID, id) {
				txnOp.GetWorkspace().EndStatement()
			}
		}
		ses.GetTxnHandler().disableStartStmt()
	}()
	return executeStmt(requestCtx, ses, execCtx)
}

func executeStmt(requestCtx context.Context,
	ses *Session,
	execCtx *ExecCtx,
) (err error) {
	if txw, ok := execCtx.cw.(*TxnComputationWrapper); ok {
		ses.GetTxnCompileCtx().tcw = txw
	}

	// record goroutine info when ddl stmt run timeout
	switch execCtx.stmt.(type) {
	case *tree.CreateTable, *tree.DropTable, *tree.CreateDatabase, *tree.DropDatabase:
		_, span := trace.Start(requestCtx, "executeStmtHung",
			trace.WithHungThreshold(time.Minute), // be careful with this options
			trace.WithProfileGoroutine(),
			trace.WithProfileTraceSecs(10*time.Second),
		)
		defer span.End()
	default:
	}

	var cmpBegin time.Time
	var ret interface{}

	switch execCtx.stmt.StmtKind().HandleType() {
	case tree.EXEC_IN_FRONTEND:
		return execInFrontend(requestCtx, ses, execCtx)
	case tree.EXEC_IN_ENGINE:
		//in the computation engine
	}

	switch st := execCtx.stmt.(type) {
	case *tree.Select:
		if st.Ep != nil {
			if getGlobalPu().SV.DisableSelectInto {
				err = moerr.NewSyntaxError(requestCtx, "Unsupport select statement")
				return
			}
			ses.InitExportConfig(st.Ep)
			defer func() {
				ses.ClearExportParam()
			}()
			err = doCheckFilePath(requestCtx, ses, st.Ep)
			if err != nil {
				return
			}
		}
	case *tree.CreateDatabase:
		err = inputNameIsInvalid(execCtx.proc.Ctx, string(st.Name))
		if err != nil {
			return
		}
		if st.SubscriptionOption != nil && ses.GetTenantInfo() != nil && !ses.GetTenantInfo().IsAdminRole() {
			err = moerr.NewInternalError(execCtx.proc.Ctx, "only admin can create subscription")
			return
		}
		st.Sql = execCtx.sqlOfStmt
	case *tree.DropDatabase:
		err = inputNameIsInvalid(execCtx.proc.Ctx, string(st.Name))
		if err != nil {
			return
		}
		ses.InvalidatePrivilegeCache()
		// if the droped database is the same as the one in use, database must be reseted to empty.
		if string(st.Name) == ses.GetDatabaseName() {
			ses.SetDatabaseName("")
		}

	case *tree.ExplainAnalyze:
		ses.SetData(nil)
	case *tree.ShowTableStatus:
		ses.SetShowStmtType(ShowTableStatus)
		ses.SetData(nil)
	case *tree.Load:
		if st.Local {
			execCtx.proc.LoadLocalReader, execCtx.loadLocalWriter = io.Pipe()
		}
	case *tree.ShowGrants:
		if len(st.Username) == 0 {
			st.Username = execCtx.userName
		}
		if len(st.Hostname) == 0 || st.Hostname == "%" {
			st.Hostname = rootHost
		}
	}

	defer func() {
		// Serialize the execution plan as json
		if cwft, ok := execCtx.cw.(*TxnComputationWrapper); ok {
			_ = cwft.RecordExecPlan(requestCtx)
		}
	}()

	cmpBegin = time.Now()

	if ret, err = execCtx.cw.Compile(requestCtx, ses.GetOutputCallback()); err != nil {
		return
	}

	defer func() {
		if c, ok := ret.(*compile.Compile); ok {
			c.Release()
		}
	}()

	// cw.Compile may rewrite the stmt in the EXECUTE statement, we fetch the latest version
	//need to check again.
	execCtx.stmt = execCtx.cw.GetAst()
	switch execCtx.stmt.StmtKind().HandleType() {
	case tree.EXEC_IN_FRONTEND:
		return execInFrontend(requestCtx, ses, execCtx)
	case tree.EXEC_IN_ENGINE:

	}

	execCtx.runner = ret.(ComputationRunner)

	// only log if build time is longer than 1s
	if time.Since(cmpBegin) > time.Second {
		logInfo(ses, ses.GetDebugString(), fmt.Sprintf("time of Exec.Build : %s", time.Since(cmpBegin).String()))
	}

	//output result & status
	StmtKind := execCtx.stmt.StmtKind().OutputType()
	switch StmtKind {
	case tree.OUTPUT_RESULT_ROW:
		err = executeResultRowStmt(requestCtx, ses, execCtx)
		if err != nil {
			return err
		}
	case tree.OUTPUT_STATUS:
		err = executeStatusStmt(requestCtx, ses, execCtx)
		if err != nil {
			return err
		}
	case tree.OUTPUT_UNDEFINED:
		isExecute := false
		switch execCtx.stmt.(type) {
		case *tree.Execute:
			isExecute = true
		}
		if !isExecute {
			return moerr.NewInternalError(requestCtx, "need set result type for %s", execCtx.sqlOfStmt)
		}
	}

	return
}

// execute query
func doComQuery(requestCtx context.Context, ses *Session, input *UserInput) (retErr error) {
	// set the batch buf for stream scan
	var inMemStreamScan []*kafka.Message

	if batchValue, ok := requestCtx.Value(defines.SourceScanResKey{}).([]*kafka.Message); ok {
		inMemStreamScan = batchValue
	}

	beginInstant := time.Now()
	requestCtx = appendStatementAt(requestCtx, beginInstant)
	input.genSqlSourceType(ses)
	ses.SetShowStmtType(NotShowStatement)
	proto := ses.GetMysqlProtocol()
	ses.SetSql(input.getSql())

	//the ses.GetUserName returns the user_name with the account_name.
	//here,we only need the user_name.
	userNameOnly := rootName

	proc := process.New(
		requestCtx,
		ses.GetMemPool(),
		getGlobalPu().TxnClient,
		nil,
		getGlobalPu().FileService,
		getGlobalPu().LockService,
		getGlobalPu().QueryClient,
		getGlobalPu().HAKeeperClient,
		getGlobalPu().UdfService,
		globalAicm)
	proc.CopyVectorPool(ses.proc)
	proc.CopyValueScanBatch(ses.proc)
	proc.Id = ses.getNextProcessId()
	proc.Lim.Size = getGlobalPu().SV.ProcessLimitationSize
	proc.Lim.BatchRows = getGlobalPu().SV.ProcessLimitationBatchRows
	proc.Lim.MaxMsgSize = getGlobalPu().SV.MaxMessageSize
	proc.Lim.PartitionRows = getGlobalPu().SV.ProcessLimitationPartitionRows
	proc.SessionInfo = process.SessionInfo{
		User:                 ses.GetUserName(),
		Host:                 getGlobalPu().SV.Host,
		ConnectionID:         uint64(proto.ConnectionID()),
		Database:             ses.GetDatabaseName(),
		Version:              makeServerVersion(getGlobalPu(), serverVersion.Load().(string)),
		TimeZone:             ses.GetTimeZone(),
		StorageEngine:        getGlobalPu().StorageEngine,
		LastInsertID:         ses.GetLastInsertID(),
		SqlHelper:            ses.GetSqlHelper(),
		Buf:                  ses.GetBuffer(),
		SourceInMemScanBatch: inMemStreamScan,
	}
	proc.SetStmtProfile(&ses.stmtProfile)
	proc.SetResolveVariableFunc(ses.txnCompileCtx.ResolveVariable)
	proc.InitSeq()
	// Copy curvalues stored in session to this proc.
	// Deep copy the map, takes some memory.
	ses.CopySeqToProc(proc)
	if ses.GetTenantInfo() != nil {
		proc.SessionInfo.Account = ses.GetTenantInfo().GetTenant()
		proc.SessionInfo.AccountId = ses.GetTenantInfo().GetTenantID()
		proc.SessionInfo.Role = ses.GetTenantInfo().GetDefaultRole()
		proc.SessionInfo.RoleId = ses.GetTenantInfo().GetDefaultRoleID()
		proc.SessionInfo.UserId = ses.GetTenantInfo().GetUserID()

		if len(ses.GetTenantInfo().GetVersion()) != 0 {
			proc.SessionInfo.Version = ses.GetTenantInfo().GetVersion()
		}
		userNameOnly = ses.GetTenantInfo().GetUser()
	} else {
		var accountId uint32
		accountId, retErr = defines.GetAccountId(requestCtx)
		if retErr != nil {
			return retErr
		}
		proc.SessionInfo.AccountId = accountId
		proc.SessionInfo.UserId = defines.GetUserId(requestCtx)
		proc.SessionInfo.RoleId = defines.GetRoleId(requestCtx)
	}
	var span trace.Span
	requestCtx, span = trace.Start(requestCtx, "doComQuery",
		trace.WithKind(trace.SpanKindStatement))
	defer span.End()

	proc.SessionInfo.User = userNameOnly
	proc.SessionInfo.QueryId = ses.getQueryId(input.isInternal())
	ses.txnCompileCtx.SetProcess(proc)
	ses.proc.SessionInfo = proc.SessionInfo

	statsInfo := statistic.StatsInfo{ParseStartTime: beginInstant}
	requestCtx = statistic.ContextWithStatsInfo(requestCtx, &statsInfo)

	cws, err := GetComputationWrapper(ses.GetDatabaseName(),
		input,
		ses.GetUserName(),
		getGlobalPu().StorageEngine,
		proc, ses)

	ParseDuration := time.Since(beginInstant)

	if err != nil {
		statsInfo.ParseDuration = ParseDuration
		var err2 error
		requestCtx, err2 = RecordParseErrorStatement(requestCtx, ses, proc, beginInstant, parsers.HandleSqlForRecord(input.getSql()), input.getSqlSourceTypes(), err)
		if err2 != nil {
			return err2
		}
		retErr = err
		if _, ok := err.(*moerr.Error); !ok {
			retErr = moerr.NewParseError(requestCtx, err.Error())
		}
		logStatementStringStatus(requestCtx, ses, input.getSql(), fail, retErr)
		return retErr
	}

	singleStatement := len(cws) == 1
	if ses.GetCmd() == COM_STMT_PREPARE && !singleStatement {
		return moerr.NewNotSupported(requestCtx, "prepare multi statements")
	}

	defer func() {
		ses.SetMysqlResultSet(nil)
	}()

	canCache := true
	Cached := false
	defer func() {
		if !Cached {
			for i := 0; i < len(cws); i++ {
				if cwft, ok := cws[i].(*TxnComputationWrapper); ok {
					cwft.Free()
				}
				cws[i].Clear()
			}
		}
	}()
	sqlRecord := parsers.HandleSqlForRecord(input.getSql())

	for i, cw := range cws {
		if cwft, ok := cw.(*TxnComputationWrapper); ok {
			if cwft.stmt.GetQueryType() == tree.QueryTypeDDL || cwft.stmt.GetQueryType() == tree.QueryTypeDCL ||
				cwft.stmt.GetQueryType() == tree.QueryTypeOth ||
				cwft.stmt.GetQueryType() == tree.QueryTypeTCL {
				if _, ok := cwft.stmt.(*tree.SetVar); !ok {
					ses.cleanCache()
				}
				canCache = false
			}
		}

		ses.SetMysqlResultSet(&MysqlResultSet{})
		ses.sentRows.Store(int64(0))
		ses.writeCsvBytes.Store(int64(0))
		proto.ResetStatistics() // move from getDataFromPipeline, for record column fields' data
		stmt := cw.GetAst()
		sqlType := input.getSqlSourceType(i)
		var err2 error
		requestCtx, err2 = RecordStatement(requestCtx, ses, proc, cw, beginInstant, sqlRecord[i], sqlType, singleStatement)
		if err2 != nil {
			return err2
		}

		statsInfo.Reset()
		//average parse duration
		statsInfo.ParseDuration = time.Duration(ParseDuration.Nanoseconds() / int64(len(cws)))

		tenant := ses.GetTenantNameWithStmt(stmt)
		//skip PREPARE statement here
		if ses.GetTenantInfo() != nil && !IsPrepareStatement(stmt) {
			err = authenticateUserCanExecuteStatement(requestCtx, ses, stmt)
			if err != nil {
				logStatementStatus(requestCtx, ses, stmt, fail, err)
				return err
			}
		}

		/*
				if it is in an active or multi-statement transaction, we check the type of the statement.
				Then we decide that if we can execute the statement.

			If we check the active transaction, it will generate the case below.
			case:
			set autocommit = 0;  <- no active transaction
			                     <- no active transaction
			drop table test1;    <- no active transaction, no error
			                     <- has active transaction
			drop table test1;    <- has active transaction, error
			                     <- has active transaction
		*/
		if ses.GetTxnHandler().InActiveTransaction() {
			err = canExecuteStatementInUncommittedTransaction(requestCtx, ses, stmt)
			if err != nil {
				logStatementStatus(requestCtx, ses, stmt, fail, err)
				return err
			}
		}

		// update UnixTime for new query, which is used for now() / CURRENT_TIMESTAMP
		proc.UnixTime = time.Now().UnixNano()
		if ses.proc != nil {
			ses.proc.UnixTime = proc.UnixTime
		}
		execCtx := ExecCtx{
			stmt:       stmt,
			isLastStmt: i >= len(cws)-1,
			tenant:     tenant,
			userName:   userNameOnly,
			sqlOfStmt:  sqlRecord[i],
			cw:         cw,
			proc:       proc,
			proto:      proto,
		}
		err = executeStmtWithResponse(requestCtx, ses, &execCtx)
		if err != nil {
			return err
		}

	} // end of for

	if canCache && !ses.isCached(input.getSql()) {
		plans := make([]*plan.Plan, len(cws))
		stmts := make([]tree.Statement, len(cws))
		for i, cw := range cws {
			if cwft, ok := cw.(*TxnComputationWrapper); ok {
				if checkNodeCanCache(cwft.plan) {
					plans[i] = cwft.plan
					stmts[i] = cwft.stmt
				} else {
					return nil
				}
			}
			cw.Clear()
		}
		Cached = true
		ses.cachePlan(input.getSql(), stmts, plans)
	}

	return nil
}

func checkNodeCanCache(p *plan2.Plan) bool {
	if p == nil {
		return true
	}
	if q, ok := p.Plan.(*plan2.Plan_Query); ok {
		for _, node := range q.Query.Nodes {
			if node.NotCacheable {
				return false
			}
			if node.ObjRef != nil && len(node.ObjRef.SubscriptionName) > 0 {
				return false
			}
		}
	}
	return true
}

// ExecRequest the server execute the commands from the client following the mysql's routine
func ExecRequest(requestCtx context.Context, ses *Session, req *Request) (resp *Response, err error) {
	defer func() {
		if e := recover(); e != nil {
			moe, ok := e.(*moerr.Error)
			if !ok {
				err = moerr.ConvertPanicError(requestCtx, e)
				resp = NewGeneralErrorResponse(COM_QUERY, ses.txnHandler.GetServerStatus(), err)
			} else {
				resp = NewGeneralErrorResponse(COM_QUERY, ses.txnHandler.GetServerStatus(), moe)
			}
		}
	}()

	var span trace.Span
	requestCtx, span = trace.Start(requestCtx, "ExecRequest",
		trace.WithKind(trace.SpanKindStatement))
	defer span.End()

	var sql string
	logDebugf(ses.GetDebugString(), "cmd %v", req.GetCmd())
	ses.SetCmd(req.GetCmd())
	switch req.GetCmd() {
	case COM_QUIT:
		return resp, moerr.GetMysqlClientQuit()
	case COM_QUERY:
		var query = string(req.GetData().([]byte))
		ses.addSqlCount(1)
		logDebug(ses, ses.GetDebugString(), "query trace", logutil.ConnectionIdField(ses.GetConnectionID()), logutil.QueryField(SubStringFromBegin(query, int(getGlobalPu().SV.LengthOfQueryPrinted))))
		err = doComQuery(requestCtx, ses, &UserInput{sql: query})
		if err != nil {
			resp = NewGeneralErrorResponse(COM_QUERY, ses.GetTxnHandler().GetServerStatus(), err)
		}
		return resp, nil
	case COM_INIT_DB:
		var dbname = string(req.GetData().([]byte))
		ses.addSqlCount(1)
		query := "use `" + dbname + "`"
		err = doComQuery(requestCtx, ses, &UserInput{sql: query})
		if err != nil {
			resp = NewGeneralErrorResponse(COM_INIT_DB, ses.GetTxnHandler().GetServerStatus(), err)
		}

		return resp, nil
	case COM_FIELD_LIST:
		var payload = string(req.GetData().([]byte))
		ses.addSqlCount(1)
		query := makeCmdFieldListSql(payload)
		err = doComQuery(requestCtx, ses, &UserInput{sql: query})
		if err != nil {
			resp = NewGeneralErrorResponse(COM_FIELD_LIST, ses.GetTxnHandler().GetServerStatus(), err)
		}

		return resp, nil
	case COM_PING:
		resp = NewGeneralOkResponse(COM_PING, ses.GetTxnHandler().GetServerStatus())

		return resp, nil

	case COM_STMT_PREPARE:
		ses.SetCmd(COM_STMT_PREPARE)
		sql = string(req.GetData().([]byte))
		ses.addSqlCount(1)

		// rewrite to "Prepare stmt_name from 'xxx'"
		newLastStmtID := ses.GenNewStmtId()
		newStmtName := getPrepareStmtName(newLastStmtID)
		sql = fmt.Sprintf("prepare %s from %s", newStmtName, sql)
		logDebug(ses, ses.GetDebugString(), "query trace", logutil.ConnectionIdField(ses.GetConnectionID()), logutil.QueryField(sql))

		err = doComQuery(requestCtx, ses, &UserInput{sql: sql})
		if err != nil {
			resp = NewGeneralErrorResponse(COM_STMT_PREPARE, ses.GetTxnHandler().GetServerStatus(), err)
		}
		return resp, nil

	case COM_STMT_EXECUTE:
		ses.SetCmd(COM_STMT_EXECUTE)
		data := req.GetData().([]byte)
		var prepareStmt *PrepareStmt
		sql, prepareStmt, err = parseStmtExecute(requestCtx, ses, data)
		if err != nil {
			return NewGeneralErrorResponse(COM_STMT_EXECUTE, ses.GetTxnHandler().GetServerStatus(), err), nil
		}
		err = doComQuery(requestCtx, ses, &UserInput{sql: sql})
		if err != nil {
			resp = NewGeneralErrorResponse(COM_STMT_EXECUTE, ses.GetTxnHandler().GetServerStatus(), err)
		}
		if prepareStmt.params != nil {
			prepareStmt.params.GetNulls().Reset()
			for k := range prepareStmt.getFromSendLongData {
				delete(prepareStmt.getFromSendLongData, k)
			}
		}
		return resp, nil

	case COM_STMT_SEND_LONG_DATA:
		ses.SetCmd(COM_STMT_SEND_LONG_DATA)
		data := req.GetData().([]byte)
		err = parseStmtSendLongData(requestCtx, ses, data)
		if err != nil {
			resp = NewGeneralErrorResponse(COM_STMT_SEND_LONG_DATA, ses.GetTxnHandler().GetServerStatus(), err)
			return resp, nil
		}
		return nil, nil

	case COM_STMT_CLOSE:
		data := req.GetData().([]byte)

		// rewrite to "deallocate Prepare stmt_name"
		stmtID := binary.LittleEndian.Uint32(data[0:4])
		stmtName := getPrepareStmtName(stmtID)
		sql = fmt.Sprintf("deallocate prepare %s", stmtName)
		logDebug(ses, ses.GetDebugString(), "query trace", logutil.ConnectionIdField(ses.GetConnectionID()), logutil.QueryField(sql))

		err = doComQuery(requestCtx, ses, &UserInput{sql: sql})
		if err != nil {
			resp = NewGeneralErrorResponse(COM_STMT_CLOSE, ses.GetTxnHandler().GetServerStatus(), err)
		}
		return resp, nil

	case COM_STMT_RESET:
		data := req.GetData().([]byte)

		//Payload of COM_STMT_RESET
		stmtID := binary.LittleEndian.Uint32(data[0:4])
		stmtName := getPrepareStmtName(stmtID)
		sql = fmt.Sprintf("reset prepare %s", stmtName)
		logDebug(ses, ses.GetDebugString(), "query trace", logutil.ConnectionIdField(ses.GetConnectionID()), logutil.QueryField(sql))
		err = doComQuery(requestCtx, ses, &UserInput{sql: sql})
		if err != nil {
			resp = NewGeneralErrorResponse(COM_STMT_RESET, ses.GetTxnHandler().GetServerStatus(), err)
		}
		return resp, nil

	case COM_SET_OPTION:
		data := req.GetData().([]byte)
		err := handleSetOption(requestCtx, ses, data)
		if err != nil {
			resp = NewGeneralErrorResponse(COM_SET_OPTION, ses.GetTxnHandler().GetServerStatus(), err)
		}
		return NewGeneralOkResponse(COM_SET_OPTION, ses.GetTxnHandler().GetServerStatus()), nil

	default:
		resp = NewGeneralErrorResponse(req.GetCmd(), ses.GetTxnHandler().GetServerStatus(), moerr.NewInternalError(requestCtx, "unsupported command. 0x%x", req.GetCmd()))
	}
	return resp, nil
}

func parseStmtExecute(requestCtx context.Context, ses *Session, data []byte) (string, *PrepareStmt, error) {
	// see https://dev.mysql.com/doc/dev/mysql-server/latest/page_protocol_com_stmt_execute.html
	pos := 0
	if len(data) < 4 {
		return "", nil, moerr.NewInvalidInput(requestCtx, "sql command contains malformed packet")
	}
	stmtID := binary.LittleEndian.Uint32(data[0:4])
	pos += 4

	stmtName := fmt.Sprintf("%s_%d", prefixPrepareStmtName, stmtID)
	preStmt, err := ses.GetPrepareStmt(stmtName)
	if err != nil {
		return "", nil, err
	}

	sql := fmt.Sprintf("execute %s", stmtName)
	logDebug(ses, ses.GetDebugString(), "query trace", logutil.ConnectionIdField(ses.GetConnectionID()), logutil.QueryField(sql))
	err = ses.GetMysqlProtocol().ParseExecuteData(requestCtx, ses.GetTxnCompileCtx().GetProcess(), preStmt, data, pos)
	if err != nil {
		return "", nil, err
	}
	return sql, preStmt, nil
}

func parseStmtSendLongData(requestCtx context.Context, ses *Session, data []byte) error {
	// see https://dev.mysql.com/doc/dev/mysql-server/latest/page_protocol_com_stmt_send_long_data.html
	pos := 0
	if len(data) < 4 {
		return moerr.NewInvalidInput(requestCtx, "sql command contains malformed packet")
	}
	stmtID := binary.LittleEndian.Uint32(data[0:4])
	pos += 4

	stmtName := fmt.Sprintf("%s_%d", prefixPrepareStmtName, stmtID)
	preStmt, err := ses.GetPrepareStmt(stmtName)
	if err != nil {
		return err
	}

	sql := fmt.Sprintf("send long data for stmt %s", stmtName)
	logDebug(ses, ses.GetDebugString(), "query trace", logutil.ConnectionIdField(ses.GetConnectionID()), logutil.QueryField(sql))

	err = ses.GetMysqlProtocol().ParseSendLongData(requestCtx, ses.GetTxnCompileCtx().GetProcess(), preStmt, data, pos)
	if err != nil {
		return err
	}
	return nil
}

/*
convert the type in computation engine to the type in mysql.
*/
func convertEngineTypeToMysqlType(ctx context.Context, engineType types.T, col *MysqlColumn) error {
	switch engineType {
	case types.T_any:
		col.SetColumnType(defines.MYSQL_TYPE_NULL)
	case types.T_json:
		col.SetColumnType(defines.MYSQL_TYPE_JSON)
	case types.T_bool:
		col.SetColumnType(defines.MYSQL_TYPE_BOOL)
	case types.T_bit:
		col.SetColumnType(defines.MYSQL_TYPE_BIT)
		col.SetSigned(false)
	case types.T_int8:
		col.SetColumnType(defines.MYSQL_TYPE_TINY)
	case types.T_uint8:
		col.SetColumnType(defines.MYSQL_TYPE_TINY)
		col.SetSigned(false)
	case types.T_int16:
		col.SetColumnType(defines.MYSQL_TYPE_SHORT)
	case types.T_uint16:
		col.SetColumnType(defines.MYSQL_TYPE_SHORT)
		col.SetSigned(false)
	case types.T_int32:
		col.SetColumnType(defines.MYSQL_TYPE_LONG)
	case types.T_uint32:
		col.SetColumnType(defines.MYSQL_TYPE_LONG)
		col.SetSigned(false)
	case types.T_int64:
		col.SetColumnType(defines.MYSQL_TYPE_LONGLONG)
	case types.T_uint64:
		col.SetColumnType(defines.MYSQL_TYPE_LONGLONG)
		col.SetSigned(false)
	case types.T_float32:
		col.SetColumnType(defines.MYSQL_TYPE_FLOAT)
	case types.T_float64:
		col.SetColumnType(defines.MYSQL_TYPE_DOUBLE)
	case types.T_char:
		col.SetColumnType(defines.MYSQL_TYPE_STRING)
	case types.T_varchar:
		col.SetColumnType(defines.MYSQL_TYPE_VAR_STRING)
	case types.T_array_float32, types.T_array_float64:
		col.SetColumnType(defines.MYSQL_TYPE_VARCHAR)
	case types.T_binary:
		col.SetColumnType(defines.MYSQL_TYPE_VARCHAR)
	case types.T_varbinary:
		col.SetColumnType(defines.MYSQL_TYPE_VARCHAR)
	case types.T_date:
		col.SetColumnType(defines.MYSQL_TYPE_DATE)
	case types.T_datetime:
		col.SetColumnType(defines.MYSQL_TYPE_DATETIME)
	case types.T_time:
		col.SetColumnType(defines.MYSQL_TYPE_TIME)
	case types.T_timestamp:
		col.SetColumnType(defines.MYSQL_TYPE_TIMESTAMP)
	case types.T_decimal64:
		col.SetColumnType(defines.MYSQL_TYPE_DECIMAL)
	case types.T_decimal128:
		col.SetColumnType(defines.MYSQL_TYPE_DECIMAL)
	case types.T_blob:
		col.SetColumnType(defines.MYSQL_TYPE_BLOB)
	case types.T_text:
		col.SetColumnType(defines.MYSQL_TYPE_TEXT)
	case types.T_uuid:
		col.SetColumnType(defines.MYSQL_TYPE_UUID)
	case types.T_TS:
		col.SetColumnType(defines.MYSQL_TYPE_VARCHAR)
	case types.T_Blockid:
		col.SetColumnType(defines.MYSQL_TYPE_VARCHAR)
	case types.T_enum:
		col.SetColumnType(defines.MYSQL_TYPE_VARCHAR)
	default:
		return moerr.NewInternalError(ctx, "RunWhileSend : unsupported type %d", engineType)
	}
	return nil
}

func convertMysqlTextTypeToBlobType(col *MysqlColumn) {
	if col.ColumnType() == defines.MYSQL_TYPE_TEXT {
		col.SetColumnType(defines.MYSQL_TYPE_BLOB)
	}
}

// build plan json when marhal plan error
func buildErrorJsonPlan(buffer *bytes.Buffer, uuid uuid.UUID, errcode uint16, msg string) []byte {
	var bytes [36]byte
	util.EncodeUUIDHex(bytes[:], uuid[:])
	explainData := explain.ExplainData{
		Code:    errcode,
		Message: msg,
		Uuid:    util.UnsafeBytesToString(bytes[:]),
	}
	encoder := json.NewEncoder(buffer)
	encoder.SetEscapeHTML(false)
	encoder.Encode(explainData)
	return buffer.Bytes()
}

type jsonPlanHandler struct {
	jsonBytes  []byte
	statsBytes statistic.StatsArray
	stats      motrace.Statistic
	buffer     *bytes.Buffer
}

func NewJsonPlanHandler(ctx context.Context, stmt *motrace.StatementInfo, plan *plan2.Plan) *jsonPlanHandler {
	h := NewMarshalPlanHandler(ctx, stmt, plan)
	jsonBytes := h.Marshal(ctx)
	statsBytes, stats := h.Stats(ctx)
	return &jsonPlanHandler{
		jsonBytes:  jsonBytes,
		statsBytes: statsBytes,
		stats:      stats,
		buffer:     h.handoverBuffer(),
	}
}

func (h *jsonPlanHandler) Stats(ctx context.Context) (statistic.StatsArray, motrace.Statistic) {
	return h.statsBytes, h.stats
}

func (h *jsonPlanHandler) Marshal(ctx context.Context) []byte {
	return h.jsonBytes
}

func (h *jsonPlanHandler) Free() {
	if h.buffer != nil {
		releaseMarshalPlanBufferPool(h.buffer)
		h.buffer = nil
		h.jsonBytes = nil
	}
}

type marshalPlanHandler struct {
	query       *plan.Query
	marshalPlan *explain.ExplainData
	stmt        *motrace.StatementInfo
	uuid        uuid.UUID
	buffer      *bytes.Buffer
}

func NewMarshalPlanHandler(ctx context.Context, stmt *motrace.StatementInfo, plan *plan2.Plan) *marshalPlanHandler {
	// TODO: need mem improvement
	uuid := uuid.UUID(stmt.StatementID)
	stmt.MarkResponseAt()
	if plan == nil || plan.GetQuery() == nil {
		return &marshalPlanHandler{
			query:       nil,
			marshalPlan: nil,
			stmt:        stmt,
			uuid:        uuid,
			buffer:      nil,
		}
	}
	query := plan.GetQuery()
	h := &marshalPlanHandler{
		query:  query,
		stmt:   stmt,
		uuid:   uuid,
		buffer: nil,
	}
	// check longQueryTime, need after StatementInfo.MarkResponseAt
	// MoLogger NOT record ExecPlan
	if stmt.Duration > motrace.GetLongQueryTime() && !stmt.IsMoLogger() {
		h.marshalPlan = explain.BuildJsonPlan(ctx, h.uuid, &explain.MarshalPlanOptions, h.query)
	}
	return h
}

func (h *marshalPlanHandler) Free() {
	h.stmt = nil
	if h.buffer != nil {
		releaseMarshalPlanBufferPool(h.buffer)
		h.buffer = nil
	}
}

func (h *marshalPlanHandler) handoverBuffer() *bytes.Buffer {
	b := h.buffer
	h.buffer = nil
	return b
}

var marshalPlanBufferPool = sync.Pool{New: func() any {
	return bytes.NewBuffer(make([]byte, 0, 8192))
}}

// get buffer from marshalPlanBufferPool
func getMarshalPlanBufferPool() *bytes.Buffer {
	return marshalPlanBufferPool.Get().(*bytes.Buffer)
}

func releaseMarshalPlanBufferPool(b *bytes.Buffer) {
	marshalPlanBufferPool.Put(b)
}

// allocBufferIfNeeded should call just right before needed.
// It will reuse buffer from pool if possible.
func (h *marshalPlanHandler) allocBufferIfNeeded() {
	if h.buffer == nil {
		h.buffer = getMarshalPlanBufferPool()
	}
}

func (h *marshalPlanHandler) Marshal(ctx context.Context) (jsonBytes []byte) {
	var err error
	if h.marshalPlan != nil {
		h.allocBufferIfNeeded()
		h.buffer.Reset()
		var jsonBytesLen = 0
		// XXX, `buffer` can be used repeatedly as a global variable in the future
		// Provide a relatively balanced initial capacity [8192] for byte slice to prevent multiple memory requests
		encoder := json.NewEncoder(h.buffer)
		encoder.SetEscapeHTML(false)
		err = encoder.Encode(h.marshalPlan)
		if err != nil {
			moError := moerr.NewInternalError(ctx, "serialize plan to json error: %s", err.Error())
			h.buffer.Reset()
			jsonBytes = buildErrorJsonPlan(h.buffer, h.uuid, moError.ErrorCode(), moError.Error())
		} else {
			jsonBytesLen = h.buffer.Len()
		}
		// BG: bytes.Buffer maintain buf []byte.
		// if buf[off:] not enough but len(buf) is enough place, then it will reset off = 0.
		// So, in here, we need call Next(...) after all data has been written
		if jsonBytesLen > 0 {
			jsonBytes = h.buffer.Next(jsonBytesLen)
		}
	} else if h.query != nil {
		// DO NOT use h.buffer
		return sqlQueryIgnoreExecPlan
	} else {
		// DO NOT use h.buffer
		return sqlQueryNoRecordExecPlan
	}
	return
}

var sqlQueryIgnoreExecPlan = []byte(`{"code":200,"message":"sql query ignore execution plan","steps":null}`)
var sqlQueryNoRecordExecPlan = []byte(`{"code":200,"message":"sql query no record execution plan","steps":null}`)

func (h *marshalPlanHandler) Stats(ctx context.Context) (statsByte statistic.StatsArray, stats motrace.Statistic) {
	if h.query != nil {
		options := &explain.MarshalPlanOptions
		statsByte.Reset()
		for _, node := range h.query.Nodes {
			// part 1: for statistic.StatsArray
			s := explain.GetStatistic4Trace(ctx, node, options)
			statsByte.Add(&s)
			// part 2: for motrace.Statistic
			if node.NodeType == plan.Node_TABLE_SCAN || node.NodeType == plan.Node_EXTERNAL_SCAN {
				rows, bytes := explain.GetInputRowsAndInputSize(ctx, node, options)
				stats.RowsRead += rows
				stats.BytesScan += bytes
			}
		}
	} else {
		statsByte = statistic.DefaultStatsArray
	}
	statsInfo := statistic.StatsInfoFromContext(ctx)
	if statsInfo != nil {
		val := int64(statsByte.GetTimeConsumed()) +
			int64(statsInfo.ParseDuration+
				statsInfo.CompileDuration+
				statsInfo.PlanDuration) - (statsInfo.IOAccessTimeConsumption + statsInfo.IOLockTimeConsumption())
		if val < 0 {
			logutil.Warnf(" negative cpu (%s) + statsInfo(%d + %d + %d - %d - %d) = %d",
				uuid.UUID(h.stmt.StatementID).String(),
				statsInfo.ParseDuration,
				statsInfo.CompileDuration,
				statsInfo.PlanDuration,
				statsInfo.IOAccessTimeConsumption,
				statsInfo.IOLockTimeConsumption(),
				val)
			v2.GetTraceNegativeCUCounter("cpu").Inc()
		} else {
			statsByte.WithTimeConsumed(float64(val))
		}
	}

	return
}

func handleSetOption(ctx context.Context, ses *Session, data []byte) (err error) {
	if len(data) < 2 {
		return moerr.NewInternalError(ctx, "invalid cmd_set_option data length")
	}
	cap := ses.GetMysqlProtocol().GetCapability()
	switch binary.LittleEndian.Uint16(data[:2]) {
	case 0:
		// MO do not support CLIENT_MULTI_STATEMENTS in prepare, so do nothing here(Like MySQL)
		// cap |= CLIENT_MULTI_STATEMENTS
		// GetSession().GetMysqlProtocol().SetCapability(cap)

	case 1:
		cap &^= CLIENT_MULTI_STATEMENTS
		ses.GetMysqlProtocol().SetCapability(cap)

	default:
		return moerr.NewInternalError(ctx, "invalid cmd_set_option data")
	}

	return nil
}

func handleExecUpgrade(ctx context.Context, ses *Session, st *tree.UpgradeStatement) error {
	retryCount := st.Retry
	if st.Retry <= 0 {
		retryCount = 1
	}
	err := ses.UpgradeTenant(ctx, st.Target.AccountName, uint32(retryCount), st.Target.IsALLAccount)
	if err != nil {
		return err
	}

	return nil
}
