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
	"bufio"
	"bytes"
	"context"
	"encoding/binary"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net"
	gotrace "runtime/trace"
	"slices"
	"sort"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/confluentinc/confluent-kafka-go/v2/kafka"
	"github.com/google/uuid"
	"go.uber.org/zap"
	"go.uber.org/zap/zapcore"

	"github.com/matrixorigin/matrixone/pkg/catalog"
	"github.com/matrixorigin/matrixone/pkg/clusterservice"
	"github.com/matrixorigin/matrixone/pkg/common/moerr"
	"github.com/matrixorigin/matrixone/pkg/common/pubsub"
	"github.com/matrixorigin/matrixone/pkg/common/runtime"
	commonutil "github.com/matrixorigin/matrixone/pkg/common/util"
	"github.com/matrixorigin/matrixone/pkg/container/batch"
	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/container/vector"
	"github.com/matrixorigin/matrixone/pkg/defines"
	"github.com/matrixorigin/matrixone/pkg/frontend/constant"
	"github.com/matrixorigin/matrixone/pkg/logutil"
	"github.com/matrixorigin/matrixone/pkg/pb/metadata"
	"github.com/matrixorigin/matrixone/pkg/pb/plan"
	"github.com/matrixorigin/matrixone/pkg/pb/timestamp"
	"github.com/matrixorigin/matrixone/pkg/perfcounter"
	"github.com/matrixorigin/matrixone/pkg/sql/colexec"
	"github.com/matrixorigin/matrixone/pkg/sql/compile"
	"github.com/matrixorigin/matrixone/pkg/sql/models"
	"github.com/matrixorigin/matrixone/pkg/sql/parsers"
	"github.com/matrixorigin/matrixone/pkg/sql/parsers/dialect"
	"github.com/matrixorigin/matrixone/pkg/sql/parsers/dialect/mysql"
	"github.com/matrixorigin/matrixone/pkg/sql/parsers/tree"
	plan2 "github.com/matrixorigin/matrixone/pkg/sql/plan"
	"github.com/matrixorigin/matrixone/pkg/sql/plan/explain"
	"github.com/matrixorigin/matrixone/pkg/txn/client"
	txnTrace "github.com/matrixorigin/matrixone/pkg/txn/trace"
	"github.com/matrixorigin/matrixone/pkg/util"
	"github.com/matrixorigin/matrixone/pkg/util/fault"
	"github.com/matrixorigin/matrixone/pkg/util/metric"
	v2 "github.com/matrixorigin/matrixone/pkg/util/metric/v2"
	"github.com/matrixorigin/matrixone/pkg/util/trace"
	"github.com/matrixorigin/matrixone/pkg/util/trace/impl/motrace"
	"github.com/matrixorigin/matrixone/pkg/util/trace/impl/motrace/statistic"
	"github.com/matrixorigin/matrixone/pkg/vm/engine"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/disttae"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/disttae/route"
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
		binExec, prepareName := cw.BinaryExecute()
		execSql := makeExecuteSql(ctx, ses, statement, binExec, prepareName)
		if len(execSql) != 0 {
			bb := strings.Builder{}
			bb.WriteString(envStmt)
			bb.WriteString(" // ")
			bb.WriteString(execSql)
			text = commonutil.Abbreviate(bb.String(), int(getPu(ses.GetService()).SV.LengthOfQueryPrinted))
		} else {
			// ignore envStmt == ""
			// case: exec `set @t = 2;` will trigger an internal query with the same session.
			// If you need real sql, can try:
			//	+ fmtCtx := tree.NewFmtCtx(dialect.MYSQL, tree.WithQuoteString(true))
			//	+ cw.GetAst().Format(fmtCtx)
			//  + envStmt = fmtCtx.String()
			text = commonutil.Abbreviate(envStmt, int(getPu(ses.GetService()).SV.LengthOfQueryPrinted))
		}
	} else {
		u, _ := util.FastUuid()
		stmID = uuid.UUID(u)
		text = commonutil.Abbreviate(envStmt, int(getPu(ses.GetService()).SV.LengthOfQueryPrinted))
	}
	ses.SetStmtId(stmID)
	ses.SetStmtType(getStatementType(statement).GetStatementType())
	ses.SetQueryType(getStatementType(statement).GetQueryType())
	ses.SetSqlSourceType(sqlType)
	ses.SetSqlOfStmt(text)

	//note: txn id here may be empty
	// add by #9907, set the result of last_query_id(), this will pass those isCmdFieldListSql() from client.
	// fixme: this op leads all internal/background executor got NULL result if call last_query_id().
	if sqlType != constant.InternalSql {
		ses.pushQueryId(types.Uuid(stmID).String())
	}

	// -------------------------------------
	// Gen StatementInfo
	// -------------------------------------

	if !motrace.GetTracerProvider().IsEnable() {
		return ctx, nil
	}
	if sqlType == constant.InternalSql && envStmt == "" {
		// case: exec `set @ t= 2;` will trigger an internal query with the same session, like: `select 2 from dual`
		// ignore internal EMPTY query.
		return ctx, nil
	}

	tenant := ses.GetTenantInfo()
	if tenant == nil {
		tenant, _ = GetTenantInfo(ctx, "internal") // pls task care of mce.GetDoQueryFunc() call case.
	}
	stm := motrace.NewStatementInfo()
	// set TransactionID
	var txn TxnOperator
	var err error
	// fixme: use ses.GetTxnId to simple.
	if handler := ses.GetTxnHandler(); handler.InActiveTxn() {
		txn = handler.GetTxn()
		if err != nil {
			return nil, err
		}
		stm.SetTxnID(txn.Txn().ID)
	}
	// set SessionID
	copy(stm.SessionID[:], ses.GetUUID())
	copy(stm.StatementID[:], stmID[:])
	requestAt := envBegin
	if !useEnv {
		requestAt = time.Now()
	}

	stm.ConnectionId = ses.GetConnectionID()
	stm.Account = tenant.GetTenant()
	stm.AccountID = tenant.GetTenantID()
	stm.RoleId = tenant.GetDefaultRoleID()
	//stm.RoleId = proc.GetSessionInfo().RoleId
	stm.User = tenant.GetUser()
	stm.Host = ses.respr.GetStr(PEER)
	stm.Database = ses.respr.GetStr(DBNAME)
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
	if ses.disableAgg {
		stm.DisableAgg()
	}
	// RecordStatementSql need to be the last calling before Report
	stm.RecordStatementSql(text, envStmt)
	stm.Report(ctx) // pls keep it simple: Only call Report twice at most.
	ses.SetTStmt(stm)

	return ctx, nil
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
	// [!WARNING]
	// after Call EndStatement, MUST reset ses.tStmt = nil
	// avoid call EndStatement again in logStatementStringStatus()
	if len(envStmt) > 0 {
		for i, sql := range envStmt {
			if i < len(sqlTypes) {
				sqlType = sqlTypes[i]
			}
			ctx, err = RecordStatement(ctx, ses, proc, nil, envBegin, sql, sqlType, true)
			if err != nil {
				return nil, err
			}
			ses.tStmt.EndStatement(ctx, retErr, 0, 0, 0)
		}
	} else {
		ctx, err = RecordStatement(ctx, ses, proc, nil, envBegin, "", sqlType, true)
		if err != nil {
			return nil, err
		}
		ses.tStmt.EndStatement(ctx, retErr, 0, 0, 0)
	}
	ses.SetTStmt(nil) // see [!WARNING] above

	tenant := ses.GetTenantInfo()
	if tenant == nil {
		tenant, _ = GetTenantInfo(ctx, "internal")
	}
	incStatementErrorsCounter(tenant.GetTenant(), tenant.GetTenantID(), nil)
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
	if ses == nil {
		return nil
	}

	if stm := ses.tStmt; stm != nil && stm.IsZeroTxnID() {
		if handler := ses.GetTxnHandler(); handler.InActiveTxn() {
			// simplify the logic of TxnOperator. refer to https://github.com/matrixorigin/matrixone/pull/13436#pullrequestreview-1779063200
			txn = handler.GetTxn()
			if err != nil {
				return err
			}
			stm.SetTxnID(txn.Txn().ID)
			ses.SetTxnId(txn.Txn().ID)
		}
		// simplify the logic of query's CollectionTxnOperator. refer to https://github.com/matrixorigin/matrixone/pull/13625
		// only call at the beginning / or the end of query's life-cycle.
		// stm.Report(ctx)
	}

	// set frontend statement's txn-id
	if upSes := ses.upstream; upSes != nil && upSes.tStmt != nil && upSes.tStmt.IsZeroTxnID() /* not record txn-id */ {
		// background session has valid txn
		if handler := ses.GetTxnHandler(); handler.InActiveTxn() {
			txn = handler.GetTxn()
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

func handleShowTableStatus(ses *Session, execCtx *ExecCtx, stmt *tree.ShowTableStatus) error {
	var db engine.Database
	var err error

	txnOp := ses.GetTxnHandler().GetTxn()
	ctx := execCtx.reqCtx

	bh := ses.GetShareTxnBackgroundExec(ctx, false)
	defer bh.Close()

	subMeta, err := getSubscriptionMeta(ctx, stmt.DbName, ses, txnOp, bh)
	if err != nil {
		return err
	}

	dbName := stmt.DbName
	if subMeta != nil {
		dbName = subMeta.DbName
		ctx = context.WithValue(ctx, defines.TenantIDKey{}, uint32(subMeta.AccountId))
	}

	if db, err = ses.GetTxnHandler().GetStorage().Database(ctx, dbName, txnOp); err != nil {
		return err
	}

	getRoleName := func(roleId uint32) (roleName string, err error) {
		accountId, err := defines.GetAccountId(ctx)
		if err != nil {
			return
		}

		if accountId != sysAccountID && roleId == moAdminRoleID {
			roleName = accountAdminRoleName
			return
		}

		sql := getSqlForRoleNameOfRoleId(int64(roleId))

		var rets []ExecResult
		if rets, err = executeSQLInBackgroundSession(ctx, bh, sql); err != nil {
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

	needRowsAndSizeTableTypes := []string{catalog.SystemOrdinaryRel, catalog.SystemMaterializedRel}

	getTableStats := func(tblNames []string) (rows, sizes map[string]int64, err error) {
		if len(tblNames) == 0 {
			return
		}

		// set session variable
		if err = ses.SetSessionSysVar(ctx, "mo_table_stats.force_update", "yes"); err != nil {
			return
		}
		defer func() {
			_ = ses.SetSessionSysVar(ctx, "mo_table_stats.force_update", "no")
		}()

		sqlBuilder := strings.Builder{}
		sqlBuilder.WriteString("select tbl, mo_table_rows(db, tbl), mo_table_size(db, tbl) from (values ")
		for i, tblName := range tblNames {
			if i != 0 {
				sqlBuilder.WriteString(", ")
			}
			sqlBuilder.WriteString(fmt.Sprintf("row('%s', '%s')", dbName, tblName))
		}
		sqlBuilder.WriteString(") as tmp(db, tbl)")

		// get table stats
		var rets []ExecResult
		if rets, err = executeSQLInBackgroundSession(ctx, bh, sqlBuilder.String()); err != nil {
			return
		}

		var tblName string
		rows = make(map[string]int64, len(tblNames))
		sizes = make(map[string]int64, len(tblNames))
		for _, result := range rets {
			for i := uint64(0); i < result.GetRowCount(); i++ {
				if tblName, err = result.GetString(ctx, i, 0); err != nil {
					return
				}
				if rows[tblName], err = result.GetInt64(ctx, i, 1); err != nil {
					return
				}
				if sizes[tblName], err = result.GetInt64(ctx, i, 2); err != nil {
					return
				}
			}
		}
		return
	}

	var tblNames []string
	var tblIdxes []int
	mrs := ses.GetMysqlResultSet()
	for i, row := range ses.data {
		tableName := string(row[0].([]byte))
		// check if the table is in the subscription meta
		if subMeta != nil && !pubsub.InSubMetaTables(subMeta, tableName) {
			continue
		}

		r, err := db.Relation(ctx, tableName, nil)
		if err != nil {
			return err
		}

		if slices.Contains(needRowsAndSizeTableTypes, r.GetTableDef(ctx).TableType) {
			tblNames = append(tblNames, tableName)
			tblIdxes = append(tblIdxes, i)
		} else if r.GetTableDef(ctx).TableType == catalog.SystemViewRel {
			for i := 0; i < 16; i++ {
				// only remain name and created_time
				if i == 0 || i == 10 {
					continue
				}
				row[i] = nil
			}
			// comment
			row[16] = "VIEW"
		}
		roleId := row[17].(uint32)
		// role name
		if tableName == catalog.MO_DATABASE || tableName == catalog.MO_TABLES || tableName == catalog.MO_COLUMNS {
			row[18] = moAdminRoleName
		} else {
			if row[18], err = getRoleName(roleId); err != nil {
				return err
			}
		}
		mrs.AddRow(row)
	}

	// calculate table row and size
	rows, sizes, err := getTableStats(tblNames)
	if err != nil {
		return err
	}
	for i, tblName := range tblNames {
		idx := tblIdxes[i]
		ses.data[idx][3] = rows[tblName]
		ses.data[idx][5] = sizes[tblName]
	}
	return nil
}

// getDataFromPipeline: extract the data from the pipeline.
// obj: session
func getDataFromPipeline(obj FeSession, execCtx *ExecCtx, bat *batch.Batch, crs *perfcounter.CounterSet) error {
	_, task := gotrace.NewTask(context.TODO(), "frontend.WriteDataToClient")
	defer task.End()
	ses := obj.(*Session)

	begin := time.Now()
	err := ses.GetResponser().RespResult(execCtx, crs, bat)
	if err != nil {
		return err
	}
	tTime := time.Since(begin)
	n := 0
	if bat != nil && bat.Vecs[0] != nil {
		n = bat.Vecs[0].Length()
		ses.sentRows.Add(int64(n))
	}

	ses.Debugf(execCtx.reqCtx, "rowCount %v \n"+
		"time of getDataFromPipeline : %s \n",
		n,
		tTime)

	stats := statistic.StatsInfoFromContext(execCtx.reqCtx)
	stats.AddOutputTimeConsumption(tTime)
	return nil
}

func doUse(ctx context.Context, ses FeSession, db string) (err error) {
	defer RecordStatementTxnID(ctx, ses)

	// In order to be compatible with various GUI clients and BI tools, lower case db name if it's a mysql system db
	if slices.Contains(mysql.CaseInsensitiveDbs, strings.ToLower(db)) {
		db = strings.ToLower(db)
	}

	var dbMeta engine.Database
	txnHandler := ses.GetTxnHandler()
	txn := txnHandler.GetTxn()
	//TODO: check meta data
	if dbMeta, err = getPu(ses.GetService()).StorageEngine.Database(ctx, db, txn); err != nil {
		//echo client. no such database
		return moerr.NewBadDB(ctx, db)
	}

	if dbMeta.IsSubscription(ctx) {
		bh := ses.GetShareTxnBackgroundExec(ctx, false)
		defer bh.Close()
		if _, err = checkSubscriptionValid(ctx, ses, db, bh); err != nil {
			return
		}
	}
	oldDB := ses.GetDatabaseName()
	ses.SetDatabaseName(db)

	ses.Debugf(ctx, "User %s change database from [%s] to [%s]", ses.GetUserName(), oldDB, ses.GetDatabaseName())
	return
}

func handleChangeDB(ses FeSession, execCtx *ExecCtx, db string) error {
	return doUse(execCtx.reqCtx, ses, db)
}

func handleDump(ses FeSession, execCtx *ExecCtx, dump *tree.MoDump) error {
	return doDumpQueryResult(execCtx.reqCtx, ses.(*Session), dump.ExportParams)
}

func doCmdFieldList(reqCtx context.Context, ses *Session, _ *InternalCmdFieldList) error {
	dbName := ses.GetDatabaseName()
	if dbName == "" {
		return moerr.NewNoDB(reqCtx)
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
	//	db, err := eng.Database(reqCtx, dbName, txnHandler.GetTxn())
	//	if err != nil {
	//		return err
	//	}
	//
	//	names, err := db.Relations(reqCtx)
	//	if err != nil {
	//		return err
	//	}
	//	for _, name := range names {
	//		table, err := db.Relation(reqCtx, name)
	//		if err != nil {
	//			return err
	//		}
	//
	//		defs, err := table.TableDefs(reqCtx)
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
func handleCmdFieldList(ses FeSession, execCtx *ExecCtx, icfl *InternalCmdFieldList) error {
	var err error

	ses.SetMysqlResultSet(nil)
	err = doCmdFieldList(execCtx.reqCtx, ses.(*Session), icfl)
	if err != nil {
		return err
	}

	return err
}

func doSetVar(ses *Session, execCtx *ExecCtx, sv *tree.SetVar, sql string) error {
	var err error = nil
	var ok bool
	setVarFunc := func(system, global bool, name string, value interface{}, sql string) error {
		var oldValueRaw interface{}
		if system {
			if global {
				if err = doCheckRole(execCtx.reqCtx, ses); err != nil {
					return err
				}
				if err = ses.SetGlobalSysVar(execCtx.reqCtx, name, value); err != nil {
					return err
				}
			} else {
				if strings.ToLower(name) == "autocommit" {
					if oldValueRaw, err = ses.GetSessionSysVar("autocommit"); err != nil {
						return err
					}
				}
				if err = ses.SetSessionSysVar(execCtx.reqCtx, name, value); err != nil {
					return err
				}
				if strings.ToLower(name) == "autocommit" {
					var oldValue, newValue bool

					if oldValue, err = valueIsBoolTrue(oldValueRaw); err != nil {
						return err
					}

					if newValue, err = valueIsBoolTrue(value); err != nil {
						return err
					}

					if err = ses.GetTxnHandler().SetAutocommit(execCtx, oldValue, newValue); err != nil {
						return err
					}
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

		value, err = getExprValue(assign.Value, ses, execCtx)
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
		} else if name == "optimizer_hints" {
			err = setVarFunc(assign.System, assign.Global, name, value, sql)
			if err != nil {
				return err
			}
			runtime.ServiceRuntime(ses.service).SetGlobalVariables("optimizer_hints", value)
		} else if name == "runtime_filter_limit_in" {
			err = setVarFunc(assign.System, assign.Global, name, value, sql)
			if err != nil {
				return err
			}
			runtime.ServiceRuntime(ses.service).SetGlobalVariables("runtime_filter_limit_in", value)
		} else if name == "runtime_filter_limit_bloom_filter" {
			err = setVarFunc(assign.System, assign.Global, name, value, sql)
			if err != nil {
				return err
			}
			runtime.ServiceRuntime(ses.service).SetGlobalVariables("runtime_filter_limit_bloom_filter", value)
		} else if name == "disable_agg_statement" {
			err = setVarFunc(assign.System, assign.Global, name, value, sql)
			if err != nil {
				return err
			}
			boolVal := InitSystemVariableBoolType("_")
			ses.disableAgg = boolVal.IsTrue(value)
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
func handleSetVar(ses FeSession, execCtx *ExecCtx, sv *tree.SetVar, sql string) error {
	err := doSetVar(ses.(*Session), execCtx, sv, sql)
	if err != nil {
		return err
	}

	return nil
}

func doShowErrors(ses *Session, execCtx *ExecCtx) error {

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
		row[1] = int16(info.codes[i])
		row[2] = info.msgs[i]
		mrs.AddRow(row)
	}
	return trySaveQueryResult(execCtx.reqCtx, ses, mrs)
}

func handleShowErrors(ses FeSession, execCtx *ExecCtx) error {
	err := doShowErrors(ses.(*Session), execCtx)
	if err != nil {
		return err
	}
	return err
}

func doShowVariables(ses *Session, execCtx *ExecCtx, sv *tree.ShowVariables) error {
	if sv.Like != nil && sv.Where != nil {
		return moerr.NewSyntaxError(execCtx.reqCtx, "like clause and where clause cannot exist at the same time")
	}

	var err error

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

	rows := make([][]interface{}, 0, len(gSysVarsDefs))
	for name, def := range gSysVarsDefs {
		if hasLike {
			s := name
			if isIlike {
				s = strings.ToLower(s)
			}
			if !WildcardMatch(likePattern, s) {
				continue
			}
		}

		var value interface{}
		if sv.Global {
			if value, err = ses.GetGlobalSysVar(name); err != nil {
				continue
			}
		} else {
			if value, err = ses.GetSessionSysVar(name); err != nil {
				continue
			}
		}

		if boolType, ok := def.GetType().(SystemVariableBoolType); ok {
			if boolType.IsTrue(value) {
				value = "on"
			} else {
				value = "off"
			}
		}
		rows = append(rows, []interface{}{name, value})
	}

	if sv.Where != nil {
		bat, _, err := convertRowsIntoBatch(execCtx.proc.Mp(), mrs.Columns, rows)
		defer cleanBatch(execCtx.proc.Mp(), bat)
		if err != nil {
			return err
		}
		binder := plan2.NewDefaultBinder(execCtx.reqCtx, nil, nil, plan2.Type{Id: int32(types.T_varchar), Width: types.MaxVarcharLen}, []string{"variable_name", "value"})
		planExpr, err := binder.BindExpr(sv.Where.Expr, 0, false)
		if err != nil {
			return err
		}

		executor, err := colexec.NewExpressionExecutor(execCtx.proc, planExpr)
		if err != nil {
			return err
		}
		vec, err := executor.Eval(execCtx.proc, []*batch.Batch{bat}, nil)
		if err != nil {
			executor.Free()
			return err
		}

		bs := vector.MustFixedColWithTypeCheck[bool](vec)
		sels := vector.GetSels()
		for i, b := range bs {
			if b {
				sels = append(sels, int64(i))
			}
		}
		executor.Free()

		bat.Shrink(sels, false)
		vector.PutSels(sels)

		v0 := vector.GenerateFunctionStrParameter(bat.Vecs[0])
		v1 := vector.GenerateFunctionStrParameter(bat.Vecs[1])
		rows = rows[:bat.Vecs[0].Length()]
		for i := range rows {
			s0, isNull := v0.GetStrValue(uint64(i))
			if isNull {
				rows[i][0] = ""
			} else {
				rows[i][0] = string(s0)
			}
			s1, isNull := v1.GetStrValue(uint64(i))
			if isNull {
				rows[i][1] = ""
			} else {
				rows[i][1] = string(s1)
			}
		}
	}

	//sort by name
	sort.Slice(rows, func(i, j int) bool {
		return rows[i][0].(string) < rows[j][0].(string)
	})

	for _, row := range rows {
		mrs.AddRow(row)
	}

	return trySaveQueryResult(execCtx.reqCtx, ses, mrs)
}

/*
handle show variables
*/
func handleShowVariables(ses FeSession, execCtx *ExecCtx, sv *tree.ShowVariables) error {
	return doShowVariables(ses.(*Session), execCtx, sv)
}

func handleAnalyzeStmt(ses *Session, execCtx *ExecCtx, stmt *tree.AnalyzeStmt) error {
	ses.EnterFPrint(FPHandleAnalyzeStmt)
	defer ses.ExitFPrint(FPHandleAnalyzeStmt)
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
	tempExecCtx := ExecCtx{
		ses:    ses,
		reqCtx: execCtx.reqCtx,
	}
	defer tempExecCtx.Close()
	return doComQuery(ses, &tempExecCtx, &UserInput{sql: sql})
}

func doExplainStmt(reqCtx context.Context, ses *Session, stmt *tree.ExplainStmt) error {

	//1. generate the plan
	es, err := getExplainOption(reqCtx, stmt.Options)
	if err != nil {
		return err
	}

	//get query optimizer and execute Optimize
	exPlan, err := buildPlanWithAuthorization(reqCtx, ses, ses.GetTxnCompileCtx(), stmt.Statement)
	if err != nil {
		return err
	}
	if exPlan.GetDcl() != nil && exPlan.GetDcl().GetExecute() != nil {
		//replace the plan of the EXECUTE by the plan generated by the PREPARE
		execPlan := exPlan.GetDcl().GetExecute()
		replaced, _, err := ses.GetTxnCompileCtx().InitExecuteStmtParam(execPlan)
		if err != nil {
			return err
		}

		exPlan = replaced
		paramVals := ses.GetTxnCompileCtx().tcw.ParamVals()
		if len(paramVals) > 0 {
			//replace the param var in the plan by the param value
			exPlan, err = plan2.FillValuesOfParamsInPlan(reqCtx, exPlan, paramVals)
			if err != nil {
				return err
			}
			if exPlan == nil {
				return moerr.NewInternalError(reqCtx, "failed to copy exPlan")
			}
		}
	}
	if exPlan.GetQuery() == nil {
		return moerr.NewNotSupported(reqCtx, "the sql query plan does not support explain.")
	}
	// generator query explain
	explainQuery := explain.NewExplainQueryImpl(exPlan.GetQuery())

	// build explain data buffer
	buffer := explain.NewExplainDataBuffer()
	err = explainQuery.ExplainPlan(reqCtx, buffer, es)
	if err != nil {
		return err
	}

	//2. fill the result set
	//column
	txnHaveDDL := false
	ws := ses.proc.GetTxnOperator().GetWorkspace()
	if ws != nil {
		txnHaveDDL = ws.GetHaveDDL()
	}
	col1 := new(MysqlColumn)
	col1.SetColumnType(defines.MYSQL_TYPE_VAR_STRING)
	col1.SetName(plan2.GetPlanTitle(explainQuery.QueryPlan, txnHaveDDL))

	mrs := ses.GetMysqlResultSet()
	mrs.AddColumn(col1)

	for _, line := range buffer.Lines {
		mrs.AddRow([]any{line})
	}

	return trySaveQueryResult(reqCtx, ses, mrs)
}

// Note: for pass the compile quickly. We will remove the comments in the future.
func handleExplainStmt(ses FeSession, execCtx *ExecCtx, stmt *tree.ExplainStmt) error {
	return doExplainStmt(execCtx.reqCtx, ses.(*Session), stmt)
}

func doPrepareStmt(execCtx *ExecCtx, ses *Session, st *tree.PrepareStmt, sql string, paramTypes []byte) (*PrepareStmt, error) {
	idx := strings.Index(strings.ToLower(sql[:(len(st.Name)+20)]), "from") + 5
	originSql := strings.TrimLeft(sql[idx:], " ")
	// fmt.Print(originSql)
	prepareStmt, err := createPrepareStmt(execCtx, ses, originSql, st, st.Stmt)
	if err != nil {
		return nil, err
	}
	if len(paramTypes) > 0 {
		prepareStmt.ParamTypes = paramTypes
	}

	err = ses.SetPrepareStmt(execCtx.reqCtx, prepareStmt.Name, prepareStmt)
	return prepareStmt, err
}

// handlePrepareStmt
func handlePrepareStmt(ses FeSession, execCtx *ExecCtx, st *tree.PrepareStmt, sql string) (*PrepareStmt, error) {
	return doPrepareStmt(execCtx, ses.(*Session), st, sql, execCtx.executeParamTypes)
}

func handlePrepareVar(ses *Session, execCtx *ExecCtx, st *tree.PrepareVar) (*PrepareStmt, error) {
	wrapper := &tree.PrepareString{
		Name: st.Name,
		Sql:  st.Var,
	}
	p, err := ses.GetUserDefinedVar(st.Var)
	if err != nil {
		return nil, err
	}
	wrapper.Sql = p.Value.(string)

	return doPrepareString(ses, execCtx, wrapper)
}

func doPrepareString(ses *Session, execCtx *ExecCtx, st *tree.PrepareString) (*PrepareStmt, error) {
	v, err := ses.GetSessionSysVar("lower_case_table_names")
	if err != nil {
		return nil, err
	}

	stmts, err := mysql.Parse(execCtx.reqCtx, st.Sql, v.(int64))
	if err != nil {
		return nil, err
	}

	prepareStmt, err := createPrepareStmt(execCtx, ses, st.Sql, st, stmts[0])
	if err != nil {
		return nil, err
	}

	err = ses.SetPrepareStmt(execCtx.reqCtx, prepareStmt.Name, prepareStmt)
	return prepareStmt, err
}

// handlePrepareString
func handlePrepareString(ses FeSession, execCtx *ExecCtx, st *tree.PrepareString) (*PrepareStmt, error) {
	return doPrepareString(ses.(*Session), execCtx, st)
}

func createPrepareStmt(
	execCtx *ExecCtx,
	ses *Session,
	originSQL string,
	stmt tree.Statement,
	saveStmt tree.Statement) (*PrepareStmt, error) {

	preparePlan, err := buildPlanWithAuthorization(execCtx.reqCtx, ses, ses.GetTxnCompileCtx(), stmt)
	if err != nil {
		return nil, err
	}

	var comp *compile.Compile
	if _, ok := preparePlan.GetDcl().GetPrepare().Plan.Plan.(*plan.Plan_Query); ok {
		//only DQL & DML will pre compile
		comp, err = createCompile(execCtx, ses, ses.proc, originSQL, saveStmt, preparePlan.GetDcl().GetPrepare().Plan, ses.GetOutputCallback(execCtx), true)
		if err != nil {
			if !moerr.IsMoErrCode(err, moerr.ErrCantCompileForPrepare) {
				return nil, err
			}
		}
		// do not save ap query now()
		if comp != nil && !comp.IsTpQuery() {
			comp.SetIsPrepare(false)
			comp.Release()
			comp = nil
		}
	}

	prepareStmt := &PrepareStmt{
		Name:                preparePlan.GetDcl().GetPrepare().GetName(),
		Sql:                 originSQL,
		compile:             comp,
		PreparePlan:         preparePlan,
		PrepareStmt:         saveStmt,
		getFromSendLongData: make(map[int]struct{}),
	}

	dcPrepare, ok := preparePlan.GetDcl().Control.(*plan.DataControl_Prepare)
	if ok {
		columns := plan2.GetResultColumnsFromPlan(dcPrepare.Prepare.Plan)
		if prepareStmt.ColDefData, err = execCtx.resper.MysqlRrWr().MakeColumnDefData(execCtx.reqCtx, columns); err != nil {
			logutil.Errorf("Error make column def data for prepare statement: %v", err)
		}
	}
	if execCtx.input != nil {
		sqlSourceTypes := execCtx.input.getSqlSourceTypes()
		prepareStmt.IsCloudNonuser = slices.Contains(sqlSourceTypes, constant.CloudNoUserSql)
	}
	prepareStmt.Ts = timestamp.Timestamp{PhysicalTime: time.Now().Unix()}
	return prepareStmt, nil
}

func doDeallocate(ses *Session, execCtx *ExecCtx, st *tree.Deallocate) error {
	deallocatePlan, err := buildPlanWithAuthorization(execCtx.reqCtx, ses, ses.GetTxnCompileCtx(), st)
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
func handleDeallocate(ses FeSession, execCtx *ExecCtx, st *tree.Deallocate) error {
	return doDeallocate(ses.(*Session), execCtx, st)
}

// handleReset
func handleReset(ses FeSession, execCtx *ExecCtx, st *tree.Reset) error {
	return doReset(execCtx.reqCtx, ses.(*Session), st)
}

func handleCreatePublication(ses FeSession, execCtx *ExecCtx, cp *tree.CreatePublication) error {
	return doCreatePublication(execCtx.reqCtx, ses.(*Session), cp)
}

func handleAlterPublication(ses FeSession, execCtx *ExecCtx, ap *tree.AlterPublication) error {
	return doAlterPublication(execCtx.reqCtx, ses.(*Session), ap)
}

func handleDropPublication(ses FeSession, execCtx *ExecCtx, dp *tree.DropPublication) error {
	return doDropPublication(execCtx.reqCtx, ses.(*Session), dp)
}

func handleCreateStage(ses FeSession, execCtx *ExecCtx, cs *tree.CreateStage) error {
	return doCreateStage(execCtx.reqCtx, ses.(*Session), cs)
}

func handleAlterStage(ses FeSession, execCtx *ExecCtx, as *tree.AlterStage) error {
	return doAlterStage(execCtx.reqCtx, ses.(*Session), as)
}

func handleDropStage(ses FeSession, execCtx *ExecCtx, ds *tree.DropStage) error {
	return doDropStage(execCtx.reqCtx, ses.(*Session), ds)
}

func handleRemoveStageFiles(ses FeSession, execCtx *ExecCtx, rs *tree.RemoveStageFiles) error {
	return doRemoveStageFiles(execCtx.reqCtx, ses.(*Session), rs)
}

func handleCreateSnapshot(ses *Session, execCtx *ExecCtx, ct *tree.CreateSnapShot) error {
	return doCreateSnapshot(execCtx.reqCtx, ses, ct)
}

func handleDropSnapshot(ses *Session, execCtx *ExecCtx, ct *tree.DropSnapShot) error {
	return doDropSnapshot(execCtx.reqCtx, ses, ct)
}

func handleRestoreSnapshot(ses *Session, execCtx *ExecCtx, rs *tree.RestoreSnapShot) (statistic.StatsArray, error) {
	return doRestoreSnapshot(execCtx.reqCtx, ses, rs)
}

func handleCreatePitr(ses *Session, execCtx *ExecCtx, cp *tree.CreatePitr) error {
	return doCreatePitr(execCtx.reqCtx, ses, cp)
}

func handleDropPitr(ses *Session, execCtx *ExecCtx, dp *tree.DropPitr) error {
	return doDropPitr(execCtx.reqCtx, ses, dp)
}

func handleAlterPitr(ses *Session, execCtx *ExecCtx, ap *tree.AlterPitr) error {
	return doAlterPitr(execCtx.reqCtx, ses, ap)
}

func handleRestorePitr(ses *Session, execCtx *ExecCtx, rp *tree.RestorePitr) (statistic.StatsArray, error) {
	return doRestorePitr(execCtx.reqCtx, ses, rp)
}

// handleCreateAccount creates a new user-level tenant in the context of the tenant SYS
// which has been initialized.
func handleCreateAccount(ses FeSession, execCtx *ExecCtx, ca *tree.CreateAccount, proc *process.Process) error {
	//step1 : create new account.
	var err error
	create := &createAccount{
		IfNotExists:  ca.IfNotExists,
		IdentTyp:     ca.AuthOption.IdentifiedType.Typ,
		StatusOption: ca.StatusOption,
		Comment:      ca.Comment,
	}

	b := strParamBinder{
		ctx:    execCtx.reqCtx,
		params: proc.GetPrepareParams(),
	}
	create.Name = b.bind(ca.Name)
	create.AdminName = b.bind(ca.AuthOption.AdminName)
	create.IdentStr = b.bindIdentStr(&ca.AuthOption.IdentifiedType)
	if b.err != nil {
		return b.err
	}

	bh := ses.GetBackgroundExec(execCtx.reqCtx)
	defer bh.Close()

	err = bh.Exec(execCtx.reqCtx, "begin;")
	defer func() {
		err = finishTxn(execCtx.reqCtx, bh, err)
	}()
	if err != nil {
		return err
	}

	return InitGeneralTenant(execCtx.reqCtx, bh, ses.(*Session), create)
}

func handleDropAccount(ses FeSession, execCtx *ExecCtx, da *tree.DropAccount, proc *process.Process) error {
	var err error
	drop := &dropAccount{
		IfExists: da.IfExists,
	}

	b := strParamBinder{
		ctx:    execCtx.reqCtx,
		params: proc.GetPrepareParams(),
	}
	drop.Name = b.bind(da.Name)
	if b.err != nil {
		return b.err
	}

	bh := ses.GetBackgroundExec(execCtx.reqCtx)
	defer bh.Close()

	err = bh.Exec(execCtx.reqCtx, "begin;")
	defer func() {
		err = finishTxn(execCtx.reqCtx, bh, err)
	}()
	if err != nil {
		return err
	}

	return doDropAccount(execCtx.reqCtx, bh, ses.(*Session), drop)
}

// handleDropAccount drops a new user-level tenant
func handleAlterAccount(ses FeSession, execCtx *ExecCtx, st *tree.AlterAccount, proc *process.Process) error {
	aa := &alterAccount{
		IfExists:     st.IfExists,
		StatusOption: st.StatusOption,
		Comment:      st.Comment,
	}

	b := strParamBinder{
		ctx:    execCtx.reqCtx,
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

	return doAlterAccount(execCtx.reqCtx, ses.(*Session), aa)
}

// handleAlterDatabaseConfig alter a database's mysql_compatibility_mode
func handleAlterDataBaseConfig(ses FeSession, execCtx *ExecCtx, ad *tree.AlterDataBaseConfig) error {
	return doAlterDatabaseConfig(execCtx.reqCtx, ses.(*Session), ad)
}

// handleAlterAccountConfig alter a account's mysql_compatibility_mode
func handleAlterAccountConfig(ses FeSession, execCtx *ExecCtx, st *tree.AlterDataBaseConfig) error {
	return doAlterAccountConfig(execCtx.reqCtx, ses.(*Session), st)
}

// handleCreateUser creates the user for the tenant
func handleCreateUser(ses FeSession, execCtx *ExecCtx, st *tree.CreateUser) error {
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
				v.IdentStr, err = unboxExprStr(execCtx.reqCtx, u.AuthOption.Str)
				if err != nil {
					return err
				}
			}
		}
		cu.Users = append(cu.Users, &v)
	}

	//step1 : create the user
	return InitUser(execCtx.reqCtx, ses.(*Session), tenant, cu)
}

// handleDropUser drops the user for the tenant
func handleDropUser(ses FeSession, execCtx *ExecCtx, du *tree.DropUser) error {
	return doDropUser(execCtx.reqCtx, ses.(*Session), du)
}

func handleAlterUser(ses FeSession, execCtx *ExecCtx, st *tree.AlterUser) error {
	au := &alterUser{
		IfExists: st.IfExists,
		Users:    make([]*user, 0, len(st.Users)),
		Role:     st.Role,
		MiscOpt:  st.MiscOpt,

		CommentOrAttribute: st.CommentOrAttribute,
	}

	for _, su := range st.Users {
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
				u.IdentStr, err = unboxExprStr(execCtx.reqCtx, su.AuthOption.Str)
				if err != nil {
					return err
				}
			}
		}
		au.Users = append(au.Users, u)
	}
	return doAlterUser(execCtx.reqCtx, ses.(*Session), au)
}

// handleCreateRole creates the new role
func handleCreateRole(ses FeSession, execCtx *ExecCtx, cr *tree.CreateRole) error {
	tenant := ses.GetTenantInfo()

	//step1 : create the role
	return InitRole(execCtx.reqCtx, ses.(*Session), tenant, cr)
}

// handleDropRole drops the role
func handleDropRole(ses FeSession, execCtx *ExecCtx, dr *tree.DropRole) error {
	return doDropRole(execCtx.reqCtx, ses.(*Session), dr)
}

// handleAlterRole renames the role
func handleAlterRole(ses FeSession, execCtx *ExecCtx, ar *tree.AlterRole) error {
	return doAlterRole(execCtx.reqCtx, ses.(*Session), ar)
}

func handleCreateFunction(ses FeSession, execCtx *ExecCtx, cf *tree.CreateFunction) error {
	tenant := ses.GetTenantInfo()
	return InitFunction(ses.(*Session), execCtx, tenant, cf)
}

func handleDropFunction(ses FeSession, execCtx *ExecCtx, df *tree.DropFunction, proc *process.Process) error {
	return doDropFunction(execCtx.reqCtx, ses.(*Session), df, func(path string) error {
		return proc.Base.FileService.Delete(execCtx.reqCtx, path)
	})
}
func handleCreateProcedure(ses FeSession, execCtx *ExecCtx, cp *tree.CreateProcedure) error {
	tenant := ses.GetTenantInfo()

	return InitProcedure(execCtx.reqCtx, ses.(*Session), tenant, cp)
}

func handleDropProcedure(ses FeSession, execCtx *ExecCtx, dp *tree.DropProcedure) error {
	return doDropProcedure(execCtx.reqCtx, ses.(*Session), dp)
}

func handleCallProcedure(ses FeSession, execCtx *ExecCtx, call *tree.CallStmt, bg bool) error {
	results, err := doInterpretCall(execCtx.reqCtx, ses, call, bg)
	if err != nil {
		return err
	}

	ses.SetMysqlResultSet(nil)
	execCtx.results = results
	return nil
}

// handleGrantRole grants the role
func handleGrantRole(ses FeSession, execCtx *ExecCtx, gr *tree.GrantRole) error {
	return doGrantRole(execCtx.reqCtx, ses.(*Session), gr)
}

// handleRevokeRole revokes the role
func handleRevokeRole(ses FeSession, execCtx *ExecCtx, rr *tree.RevokeRole) error {
	return doRevokeRole(execCtx.reqCtx, ses.(*Session), rr)
}

// handleGrantRole grants the privilege to the role
func handleGrantPrivilege(ses FeSession, execCtx *ExecCtx, gp *tree.GrantPrivilege) (err error) {
	ctx := execCtx.reqCtx
	bh := ses.GetBackgroundExec(ctx)
	defer bh.Close()

	// put it into the single transaction
	err = bh.Exec(ctx, "begin;")
	defer func() {
		err = finishTxn(ctx, bh, err)
	}()
	if err != nil {
		return err
	}

	return doGrantPrivilege(ctx, ses, gp, bh)
}

// handleRevokePrivilege revokes the privilege from the user or role
func handleRevokePrivilege(ses FeSession, execCtx *ExecCtx, rp *tree.RevokePrivilege) (err error) {
	ctx := execCtx.reqCtx
	bh := ses.GetBackgroundExec(ctx)
	defer bh.Close()

	// put it into the single transaction
	err = bh.Exec(ctx, "begin;")
	defer func() {
		err = finishTxn(ctx, bh, err)
	}()
	if err != nil {
		return err
	}

	return doRevokePrivilege(ctx, ses, rp, bh)
}

// handleSwitchRole switches the role to another role
func handleSwitchRole(ses FeSession, execCtx *ExecCtx, sr *tree.SetRole) error {
	return doSwitchRole(execCtx.reqCtx, ses.(*Session), sr)
}

func doKill(ses *Session, execCtx *ExecCtx, k *tree.Kill) error {
	var err error
	//true: kill a connection
	//false: kill a query in a connection
	idThatKill := uint64(ses.GetConnectionID())
	if !k.Option.Exist || k.Option.Typ == tree.KillTypeConnection {
		err = getRtMgr(ses.GetService()).kill(execCtx.reqCtx, true, idThatKill, k.ConnectionId, "")
	} else {
		err = getRtMgr(ses.GetService()).kill(execCtx.reqCtx, false, idThatKill, k.ConnectionId, k.StmtOption.StatementId)
	}
	return err
}

// handleKill kill a connection or query
func handleKill(ses *Session, execCtx *ExecCtx, k *tree.Kill) error {
	err := doKill(ses, execCtx, k)
	if err != nil {
		return err
	}
	return err
}

// handleShowAccounts lists the info of accounts
func handleShowAccounts(ses FeSession, execCtx *ExecCtx, sa *tree.ShowAccounts) error {
	err := doShowAccounts(execCtx.reqCtx, ses.(*Session), sa)
	if err != nil {
		return err
	}
	return err
}

// handleShowRecoveryWindow lists the info of recovery window
func handleShowRecoveryWindow(ses FeSession, execCtx *ExecCtx, srw *tree.ShowRecoveryWindow) error {
	err := doShowRecoveryWindow(execCtx.reqCtx, ses.(*Session), srw)
	if err != nil {
		return err
	}
	return err
}

// handleShowCollation lists the info of collation
func handleShowCollation(ses FeSession, execCtx *ExecCtx, sc *tree.ShowCollation) error {
	err := doShowCollation(ses.(*Session), execCtx, execCtx.proc, sc)
	if err != nil {
		return err
	}
	return err
}

func doShowCollation(ses *Session, execCtx *ExecCtx, proc *process.Process, sc *tree.ShowCollation) error {
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

	bat, _, err = convertRowsIntoBatch(ses.GetMemPool(), mrs.Columns, rows)
	defer cleanBatch(ses.GetMemPool(), bat)
	if err != nil {
		return err
	}

	if sc.Where != nil {
		binder := plan2.NewDefaultBinder(execCtx.reqCtx, nil, nil, plan2.Type{Id: int32(types.T_varchar), Width: types.MaxVarcharLen}, []string{"collation", "charset", "id", "default", "compiled", "sortlen", "pad_attribute"})
		planExpr, err := binder.BindExpr(sc.Where.Expr, 0, false)
		if err != nil {
			return err
		}

		executor, err := colexec.NewExpressionExecutor(proc, planExpr)
		if err != nil {
			return err
		}
		vec, err := executor.Eval(proc, []*batch.Batch{bat}, nil)
		if err != nil {
			executor.Free()
			return err
		}

		bs := vector.MustFixedColWithTypeCheck[bool](vec)
		sels := vector.GetSels()
		for i, b := range bs {
			if b {
				sels = append(sels, int64(i))
			}
		}
		executor.Free()

		bat.Shrink(sels, false)
		vector.PutSels(sels)
		v0, area0 := vector.MustVarlenaRawData(bat.Vecs[0])
		v1, area1 := vector.MustVarlenaRawData(bat.Vecs[1])
		v2 := vector.MustFixedColWithTypeCheck[int64](bat.Vecs[2])
		v3, area3 := vector.MustVarlenaRawData(bat.Vecs[3])
		v4, area4 := vector.MustVarlenaRawData(bat.Vecs[4])
		v5 := vector.MustFixedColWithTypeCheck[int32](bat.Vecs[5])
		v6, area6 := vector.MustVarlenaRawData(bat.Vecs[6])
		rows = rows[:len(v0)]
		for i := range v0 {
			rows[i][0] = v0[i].GetString(area0)
			rows[i][1] = v1[i].GetString(area1)
			rows[i][2] = v2[i]
			rows[i][3] = v3[i].GetString(area3)
			rows[i][4] = v4[i].GetString(area4)
			rows[i][5] = v5[i]
			rows[i][6] = v6[i].GetString(area6)
		}
	}

	//sort by name
	sort.Slice(rows, func(i, j int) bool {
		return rows[i][0].(string) < rows[j][0].(string)
	})

	for _, row := range rows {
		mrs.AddRow(row)
	}

	ses.SetMysqlResultSet(mrs)

	if canSaveQueryResult(execCtx.reqCtx, ses) {
		//already have the batch
		ses.rs, _, _, err = mysqlColDef2PlanResultColDef(mrs.Columns)
		if err != nil {
			return err
		}

		// save query result
		err = saveQueryResult(execCtx.reqCtx, ses,
			func() ([]*batch.Batch, error) {
				return []*batch.Batch{bat}, nil
			},
			nil,
		)
		if err != nil {
			return err
		}
	}

	return err
}

func handleShowPublications(ses FeSession, execCtx *ExecCtx, sp *tree.ShowPublications) error {
	return doShowPublications(execCtx.reqCtx, ses.(*Session), sp)
}

func handleShowSubscriptions(ses FeSession, execCtx *ExecCtx, ss *tree.ShowSubscriptions) error {
	return doShowSubscriptions(execCtx.reqCtx, ses.(*Session), ss)
}

func doShowBackendServers(ses *Session, execCtx *ExecCtx) error {
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
	moc := clusterservice.GetMOCluster(ses.GetService())
	moc.ForceRefresh(true)
	if isSysTenant(tenant) {
		u := ses.GetTenantInfo().GetUser()
		// For super use dump and root, we should list all servers.
		if isSuperUser(u) {
			moc.GetCNService(
				clusterservice.NewSelectAll(), func(s metadata.CNService) bool {
					appendFn(&s)
					return true
				})
		} else {
			route.RouteForSuperTenant(
				ses.GetService(),
				se,
				u,
				nil,
				appendFn,
			)
		}
	} else {
		route.RouteForCommonTenant(ses.GetService(), se, nil, appendFn)
	}

	return trySaveQueryResult(execCtx.reqCtx, ses, mrs)
}

func handleShowBackendServers(ses FeSession, execCtx *ExecCtx) error {
	var err error
	if err := doShowBackendServers(ses.(*Session), execCtx); err != nil {
		return err
	}
	return err
}

func handleEmptyStmt(ses FeSession, execCtx *ExecCtx, stmt *tree.EmptyStmt) error {
	var err error
	return err
}

func getExplainOption(reqCtx context.Context, options []tree.OptionElem) (*explain.ExplainOptions, error) {
	es := explain.NewExplainDefaultOptions()
	if options == nil {
		return es, nil
	} else {
		for _, v := range options {
			if strings.EqualFold(v.Name, tree.VerboseOption) {
				if strings.EqualFold(v.Value, "TRUE") || v.Value == "NULL" {
					es.Verbose = true
				} else if strings.EqualFold(v.Value, "FALSE") {
					es.Verbose = false
				} else {
					return nil, moerr.NewInvalidInputf(reqCtx, "invalid explain option '%s', valud '%s'", v.Name, v.Value)
				}
			} else if strings.EqualFold(v.Name, tree.AnalyzeOption) {
				if strings.EqualFold(v.Value, "TRUE") || v.Value == "NULL" {
					es.Analyze = true
				} else if strings.EqualFold(v.Value, "FALSE") {
					es.Analyze = false
				} else {
					return nil, moerr.NewInvalidInputf(reqCtx, "invalid explain option '%s', valud '%s'", v.Name, v.Value)
				}
			} else if strings.EqualFold(v.Name, tree.FormatOption) {
				if strings.EqualFold(v.Value, "TEXT") {
					es.Format = explain.EXPLAIN_FORMAT_TEXT
				} else if strings.EqualFold(v.Value, "JSON") {
					return nil, moerr.NewNotSupportedf(reqCtx, "Unsupport explain format '%s'", v.Value)
				} else if strings.EqualFold(v.Value, "DOT") {
					return nil, moerr.NewNotSupportedf(reqCtx, "Unsupport explain format '%s'", v.Value)
				} else {
					return nil, moerr.NewInvalidInputf(reqCtx, "invalid explain option '%s', valud '%s'", v.Name, v.Value)
				}
			} else if strings.EqualFold(v.Name, tree.CheckOption) {
				if err := json.Unmarshal([]byte(v.Value), &es.CheckExpr); err != nil {
					return nil, moerr.NewInvalidInputf(reqCtx, "invalid explain option '%s', valud '%s': %s", v.Name, v.Value, err.Error())
				}
			} else {
				return nil, moerr.NewInvalidInputf(reqCtx, "invalid explain option '%s', valud '%s'", v.Name, v.Value)
			}
		}
		return es, nil
	}
}

func buildMoExplainQuery(execCtx *ExecCtx, explainColName string, buffer *explain.ExplainDataBuffer, session *Session, fill outputCallBackFunc) error {
	bat := batch.New([]string{explainColName})
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

	err := fill(session, execCtx, bat, nil)
	if err != nil {
		return err
	}
	// to trigger save result meta
	err = fill(session, execCtx, nil, nil)
	return err
}

func buildMoExplainPhyPlan(execCtx *ExecCtx, explainColName string, reader *bufio.Reader, session *Session, fill outputCallBackFunc) error {
	bat := batch.New([]string{explainColName})
	vs := make([][]byte, 0)
	count := 0
	for {
		line, err := reader.ReadString('\n')
		if err == io.EOF && len(line) > 0 {
			vs = append(vs, []byte(strings.TrimSuffix(line, "\n")))
			count++
			break
		}
		if err != nil {
			return moerr.NewInvalidInputf(execCtx.reqCtx, "Error when read explain phyplan buffer: %s", err.Error())
		}

		vs = append(vs, []byte(strings.TrimSuffix(line, "\n")))
		count++
	}

	vs = vs[:count]
	vec := vector.NewVec(types.T_varchar.ToType())
	defer vec.Free(session.GetMemPool())
	vector.AppendBytesList(vec, vs, nil, session.GetMemPool())
	bat.Vecs[0] = vec
	bat.SetRowCount(count)

	err := fill(session, execCtx, bat, nil)
	if err != nil {
		return err
	}
	// to trigger save result meta
	err = fill(session, execCtx, nil, nil)
	return err
}

func buildPlan(reqCtx context.Context, ses FeSession, ctx plan2.CompilerContext, stmt tree.Statement) (*plan2.Plan, error) {
	var ret *plan2.Plan
	var err error

	txnOp := ctx.GetProcess().GetTxnOperator()
	start := time.Now()
	seq := uint64(0)
	if txnOp != nil {
		seq = txnOp.NextSequence()
		txnTrace.GetService(ses.GetService()).AddTxnDurationAction(
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
			txnTrace.GetService(ses.GetService()).AddTxnDurationAction(
				txnOp,
				client.BuildPlanEvent,
				seq,
				0,
				cost,
				err)
		}
		v2.TxnStatementBuildPlanDurationHistogram.Observe(cost.Seconds())
	}()

	// NOTE: The context used by buildPlan comes from the CompilerContext object
	planContext := ctx.GetContext()
	stats := statistic.StatsInfoFromContext(planContext)
	stats.PlanStart()

	crs := new(perfcounter.CounterSet)
	planContext = perfcounter.AttachBuildPlanMarkKey(planContext, crs)
	ctx.SetContext(planContext)
	defer func() {
		stats.AddBuildPlanS3Request(statistic.S3Request{
			List:      crs.FileService.S3.List.Load(),
			Head:      crs.FileService.S3.Head.Load(),
			Put:       crs.FileService.S3.Put.Load(),
			Get:       crs.FileService.S3.Get.Load(),
			Delete:    crs.FileService.S3.Delete.Load(),
			DeleteMul: crs.FileService.S3.DeleteMulti.Load(),
		})
		stats.PlanEnd()
	}()

	isPrepareStmt := false
	if ses != nil {
		accId, err := defines.GetAccountId(reqCtx)
		if err != nil {
			return nil, err
		}
		ses.SetAccountId(accId)

		if len(ses.GetSql()) > 8 {
			prefix := strings.ToLower(ses.GetSql()[:8])
			isPrepareStmt = prefix == "execute " || prefix == "prepare "
		}
	}
	// Handle specific statement types
	if s, ok := stmt.(*tree.Insert); ok {
		if _, ok := s.Rows.Select.(*tree.ValuesClause); ok {
			ret, err = plan2.BuildPlan(ctx, stmt, isPrepareStmt)
			if err != nil {
				return nil, err
			}
		}
	}

	if ret != nil {
		ret.IsPrepare = isPrepareStmt
		return ret, err
	}

	// Default handling of various statements
	switch stmt := stmt.(type) {
	case *tree.Select, *tree.ParenSelect, *tree.ValuesStatement,
		*tree.Update, *tree.Delete, *tree.Insert,
		*tree.ShowDatabases, *tree.ShowTables, *tree.ShowSequences, *tree.ShowColumns, *tree.ShowColumnNumber,
		*tree.ShowTableNumber, *tree.ShowCreateDatabase, *tree.ShowCreateTable, *tree.ShowIndex,
		*tree.ExplainStmt, *tree.ExplainAnalyze, *tree.ExplainPhyPlan:
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
	}
	return ret, err
}

// buildPlanWithAuthorization wraps the buildPlan function to perform permission checks
// after the plan has been successfully built.
var buildPlanWithAuthorization = func(reqCtx context.Context, ses FeSession, ctx plan2.CompilerContext, stmt tree.Statement) (*plan2.Plan, error) {
	planContext := ctx.GetContext()
	stats := statistic.StatsInfoFromContext(planContext)

	// Step 1: Call buildPlan to construct the execution plan
	plan, err := buildPlan(reqCtx, ses, ctx, stmt)
	if err != nil {
		return nil, err
	}

	// Step 2: Perform permission check after the plan is built
	if ses != nil && ses.GetTenantInfo() != nil && !ses.IsBackgroundSession() {
		authStats, err := authenticateCanExecuteStatementAndPlan(reqCtx, ses.(*Session), stmt, plan)
		if err != nil {
			return nil, err
		}
		// record permission statistics.
		stats.PermissionAuth.Add(&authStats)
	}
	return plan, nil
}

func checkModify(plan0 *plan.Plan, resolveFn func(string, string, *plan2.Snapshot) (*plan2.ObjectRef, *plan2.TableDef, error)) (bool, error) {
	if plan0 == nil {
		return true, nil
	}

	checkFn := func(ref *plan.ObjectRef, def *plan.TableDef) (bool, error) {
		if ref == nil || def == nil {
			return true, nil
		}
		_, tableDef, err := resolveFn(ref.SchemaName, def.Name, nil)
		if err != nil {
			return true, err
		}
		if tableDef == nil {
			return true, nil
		}
		if tableDef.Version != def.Version || tableDef.TblId != def.TblId {
			return true, nil
		}
		return false, nil
	}
	switch p := plan0.Plan.(type) {
	case *plan.Plan_Query:
		for i := range p.Query.Nodes {
			if def := p.Query.Nodes[i].TableDef; def != nil {
				flag, err := checkFn(p.Query.Nodes[i].ObjRef, def)
				if err != nil || flag {
					return true, err
				}
			}
			if ctx := p.Query.Nodes[i].InsertCtx; ctx != nil {
				flag, err := checkFn(ctx.Ref, ctx.TableDef)
				if err != nil || flag {
					return true, err
				}
			}
			if ctx := p.Query.Nodes[i].ReplaceCtx; ctx != nil {
				flag, err := checkFn(ctx.Ref, ctx.TableDef)
				if err != nil || flag {
					return true, err
				}
			}
			if ctx := p.Query.Nodes[i].DeleteCtx; ctx != nil {
				flag, err := checkFn(ctx.Ref, ctx.TableDef)
				if err != nil || flag {
					return true, err
				}
			}
			if ctx := p.Query.Nodes[i].PreInsertCtx; ctx != nil {
				flag, err := checkFn(ctx.Ref, ctx.TableDef)
				if err != nil || flag {
					return true, err
				}
			}
			if ctx := p.Query.Nodes[i].OnDuplicateKey; ctx != nil {
				flag, err := checkFn(p.Query.Nodes[i].ObjRef, &plan.TableDef{
					Name:    ctx.TableName,
					TblId:   ctx.TableId,
					Version: ctx.TableVersion,
				})
				if err != nil || flag {
					return true, err
				}
			}
		}
	default:
	}
	return false, nil
}

var GetComputationWrapper = func(execCtx *ExecCtx, db string, user string, eng engine.Engine, proc *process.Process, ses *Session) ([]ComputationWrapper, error) {
	var cws []ComputationWrapper = nil
	if preparePlan := execCtx.input.getPreparePlan(); preparePlan != nil {
		tcw := InitTxnComputationWrapper(ses, execCtx.input.stmt, proc)
		tcw.plan = preparePlan.GetDcl().GetPrepare().Plan
		tcw.binaryPrepare = execCtx.input.isBinaryProtExecute
		tcw.prepareName = execCtx.input.stmtName
		cws = append(cws, tcw)
		return cws, nil
	} else if cached := ses.getCachedPlan(execCtx.input.getHash()); cached != nil {
		for i, stmt := range cached.stmts {
			tcw := InitTxnComputationWrapper(ses, stmt, proc)
			tcw.plan = cached.plans[i]
			cws = append(cws, tcw)
		}

		return cws, nil
	}

	var stmts []tree.Statement = nil
	var cmdFieldStmt *InternalCmdFieldList
	var err error
	// if the input is an option ast, we should use it directly
	if execCtx.input.getStmt() != nil {
		stmts = append(stmts, execCtx.input.getStmt())
	} else if isCmdFieldListSql(execCtx.input.getSql()) {
		cmdFieldStmt, err = parseCmdFieldList(execCtx.reqCtx, execCtx.input.getSql())
		if err != nil {
			return nil, err
		}
		stmts = append(stmts, cmdFieldStmt)
	} else {
		stmts, err = parseSql(execCtx, ses.GetMySQLParser())
		if err != nil {
			return nil, err
		}
		v, err := ses.GetSessionSysVar("enable_remap_hint")
		if err == nil {
			if on, convErr := valueIsBoolTrue(v); convErr == nil && on {
				err = parsers.AddRewriteHints(execCtx.reqCtx, stmts, execCtx.input.getSql())
				if err != nil {
					return nil, err
				}
			}
		}
	}

	for _, stmt := range stmts {
		cws = append(cws, InitTxnComputationWrapper(ses, stmt, proc))
	}
	return cws, nil
}

func parseSql(execCtx *ExecCtx, p *mysql.MySQLParser) (stmts []tree.Statement, err error) {
	v, err := execCtx.ses.GetSessionSysVar("lower_case_table_names")
	var lctn int64 = 1 // default
	if err == nil {
		switch vv := v.(type) {
		case int64:
			lctn = vv
		}
	}
	return p.Parse(execCtx.reqCtx, execCtx.input.getSql(), lctn)
}

func incTransactionCounter(tenant string, tenantId uint32) {
	metric.TransactionCounter(tenant, tenantId).Inc()
}

func incTransactionErrorsCounter(tenant string, tenantId uint32, t metric.SQLType) {
	if t == metric.SQLTypeRollback {
		return
	}
	metric.TransactionErrorsCounter(tenant, tenantId, t).Inc()
}

func incStatementErrorsCounter(tenant string, tenantId uint32, stmt tree.Statement) {
	metric.StatementErrorsCounter(tenant, tenantId, getStatementType(stmt).GetQueryType()).Inc()
}

// authenticateUserCanExecuteStatement checks the user can execute the statement
func authenticateUserCanExecuteStatement(reqCtx context.Context, ses *Session, stmt tree.Statement) (statistic.StatsArray, error) {
	var stats statistic.StatsArray
	stats.Reset()

	reqCtx, span := trace.Debug(reqCtx, "authenticateUserCanExecuteStatement")
	defer span.End()
	if getPu(ses.GetService()).SV.SkipCheckPrivilege {
		return stats, nil
	}

	if ses.skipAuthForSpecialUser() {
		return stats, nil
	}
	if ses.GetTenantInfo() != nil {
		ses.SetPrivilege(determinePrivilegeSetOfStatement(stmt))

		// can or not execute in retricted status
		if ses.getRoutine() != nil && ses.getRoutine().isRestricted() && !ses.GetPrivilege().canExecInRestricted {
			return stats, moerr.NewInternalError(reqCtx, "do not have enough storage to execute the statement")
		}

		// can or not execute in password expired status
		if ses.getRoutine() != nil && ses.getRoutine().isExpired() && !ses.GetPrivilege().canExecInPasswordExpired {
			return stats, moerr.NewInternalError(reqCtx, "password has expired, please change the password")
		}

		havePrivilege, delta, err := authenticateUserCanExecuteStatementWithObjectTypeAccountAndDatabase(reqCtx, ses, stmt)
		if err != nil {
			return stats, err
		}
		stats.Add(&delta)

		if !havePrivilege {
			err = moerr.NewInternalError(reqCtx, "do not have privilege to execute the statement")
			return stats, err
		}

		havePrivilege, delta, err = authenticateUserCanExecuteStatementWithObjectTypeNone(reqCtx, ses, stmt)
		if err != nil {
			return stats, err
		}
		stats.Add(&delta)

		if !havePrivilege {
			err = moerr.NewInternalError(reqCtx, "do not have privilege to execute the statement")
			return stats, err
		}
	}
	return stats, nil
}

// authenticateCanExecuteStatementAndPlan checks the user can execute the statement and its plan
func authenticateCanExecuteStatementAndPlan(reqCtx context.Context, ses *Session, stmt tree.Statement, p *plan.Plan) (statistic.StatsArray, error) {
	var stats statistic.StatsArray
	stats.Reset()

	_, task := gotrace.NewTask(reqCtx, "frontend.authenticateCanExecuteStatementAndPlan")
	defer task.End()
	if getPu(ses.GetService()).SV.SkipCheckPrivilege {
		return stats, nil
	}

	if ses.skipAuthForSpecialUser() {
		return stats, nil
	}
	yes, delta, err := authenticateUserCanExecuteStatementWithObjectTypeDatabaseAndTable(reqCtx, ses, stmt, p)
	if err != nil {
		return stats, err
	}
	stats.Add(&delta)

	if !yes {
		return stats, moerr.NewInternalError(reqCtx, "do not have privilege to execute the statement")
	}
	return stats, nil
}

// authenticatePrivilegeOfPrepareAndExecute checks the user can execute the Prepare or Execute statement
func authenticateUserCanExecutePrepareOrExecute(reqCtx context.Context, ses *Session, stmt tree.Statement, p *plan.Plan) (statistic.StatsArray, error) {
	var stats statistic.StatsArray
	stats.Reset()

	_, task := gotrace.NewTask(reqCtx, "frontend.authenticateUserCanExecutePrepareOrExecute")
	defer task.End()
	if getPu(ses.GetService()).SV.SkipCheckPrivilege {
		return stats, nil
	}
	delta, err := authenticateUserCanExecuteStatement(reqCtx, ses, stmt)
	if err != nil {
		return stats, err
	}
	stats.Add(&delta)

	delta, err = authenticateCanExecuteStatementAndPlan(reqCtx, ses, stmt, p)
	if err != nil {
		return stats, err
	}
	stats.Add(&delta)
	return stats, err
}

// canExecuteStatementInUncommittedTxn checks the user can execute the statement in an uncommitted transaction
func canExecuteStatementInUncommittedTransaction(
	reqCtx context.Context,
	ses FeSession,
	stmt tree.Statement,
) error {

	can, err := statementCanBeExecutedInUncommittedTransaction(reqCtx, ses, stmt)
	if err != nil {
		return err
	}
	if !can {
		//is ddl statement
		if IsCreateDropDatabase(stmt) {
			return moerr.NewInternalError(reqCtx, createDropDatabaseErrorInfo())
		} else if IsDDL(stmt) {
			return moerr.NewInternalError(reqCtx, onlyCreateStatementErrorInfo())
		} else if IsAdministrativeStatement(stmt) {
			return moerr.NewInternalError(reqCtx, administrativeCommandIsUnsupportedInTxnErrorInfo())
		} else {
			return moerr.NewInternalError(reqCtx, unclassifiedStatementInUncommittedTxnErrorInfo())
		}
	}
	return nil
}

func readThenWrite(ses FeSession, execCtx *ExecCtx, param *tree.ExternParam, writer *io.PipeWriter, mysqlRrWr MysqlRrWr, skipWrite bool, epoch uint64) (_ bool, _ time.Duration, _ time.Duration, err error) {
	var readTime, writeTime time.Duration
	var payload []byte
	start := time.Now()
	defer func() {
		if err != nil {
			mysqlRrWr.FreeLoadLocal()
		}
	}()
	payload, err = mysqlRrWr.ReadLoadLocalPacket()
	if err != nil {
		if errors.Is(err, errorInvalidLength0) {
			return skipWrite, readTime, writeTime, err
		}
		if moerr.IsMoErrCode(err, moerr.ErrInvalidInput) {
			err = moerr.NewInvalidInputf(execCtx.reqCtx, "cannot read '%s' from client,please check the file path, user privilege and if client start with --local-infile", param.Filepath)
		}
		return skipWrite, readTime, writeTime, err
	}
	readTime = time.Since(start)

	//empty packet means the file is over.
	size := len(payload)
	if size == 0 {
		return skipWrite, readTime, writeTime, errorInvalidLength0
	}
	ses.CountPayload(size)

	// If inner error occurs(unexpected or expected(ctrl-c)), proc.Base.LoadLocalReader will be closed.
	// Then write will return error, but we need to read the rest of the data and not write it to pipe.
	// So we need a flag[skipWrite] to tell us whether we need to write the data to pipe.
	// https://github.com/matrixorigin/matrixone/issues/6665#issuecomment-1422236478

	start = time.Now()
	if !skipWrite {
		_, err = writer.Write(payload)
		if err != nil {
			ses.Errorf(execCtx.reqCtx,
				"Failed to load local file",
				zap.String("path", param.Filepath),
				zap.Uint64("epoch", epoch),
				zap.Error(err))
			skipWrite = true
		}
		writeTime = time.Since(start)

	}
	return skipWrite, readTime, writeTime, err
}

// processLoadLocal executes the load data local.
// load data local interaction: https://dev.mysql.com/doc/dev/mysql-server/latest/page_protocol_com_query_response_local_infile_request.html
func processLoadLocal(ses FeSession, execCtx *ExecCtx, param *tree.ExternParam, writer *io.PipeWriter, reader *io.PipeReader) (err error) {
	//pipewriter may stick when there is no reader reading on the pipereader.
	//so we need to make sure the pipewriter.write returns.
	//issue3976
	quitC := make(chan int)
	go func(ctx context.Context, reader *io.PipeReader) {
		select {
		case <-ctx.Done():
			//close reader
			_ = reader.Close()
		case <-quitC:
		}
	}(execCtx.reqCtx, reader)
	defer func() {
		close(quitC)
	}()
	mysqlRwer := ses.GetResponser().MysqlRrWr()
	defer func() {
		err2 := writer.Close()
		if err == nil {
			err = err2
		}
		//free load local buffer anyway
		mysqlRwer.FreeLoadLocal()
	}()
	err = plan2.InitInfileParam(param)
	if err != nil {
		return
	}
	err = mysqlRwer.WriteLocalInfileRequest(param.Filepath)
	if err != nil {
		return
	}

	// handleNetworkTimeout checks if the error is a network timeout and disconnects the client
	handleNetworkTimeout := func(err error) error {
		if netErr, ok := err.(net.Error); ok && netErr.Timeout() {
			ses.Errorf(execCtx.reqCtx, "load local file failed: network read timeout: %v, disconnecting client", err)
			if disconnectErr := mysqlRwer.Disconnect(); disconnectErr != nil {
				ses.Errorf(execCtx.reqCtx, "failed to disconnect client: %v", disconnectErr)
			}
			return moerr.NewInternalErrorf(execCtx.reqCtx,
				"load local file failed: network read timeout, client connection closed")
		}
		return nil
	}

	var skipWrite bool
	skipWrite = false
	var readTime, writeTime time.Duration
	var retError error
	start := time.Now()
	epoch, printTime := uint64(0), uint64(1024*60)
	minReadTime, maxReadTime, minWriteTime, maxWriteTime := 24*time.Hour, time.Nanosecond, 24*time.Hour, time.Nanosecond

	// updateTimeStats updates min/max time statistics
	updateTimeStats := func(readTime, writeTime time.Duration) {
		if readTime > maxReadTime {
			maxReadTime = readTime
		}
		if readTime < minReadTime {
			minReadTime = readTime
		}
		if writeTime > maxWriteTime {
			maxWriteTime = writeTime
		}
		if writeTime < minWriteTime {
			minWriteTime = writeTime
		}
	}

	skipWrite, readTime, writeTime, err = readThenWrite(ses, execCtx, param, writer, mysqlRwer, skipWrite, epoch)
	if err != nil {
		if errors.Is(err, errorInvalidLength0) {
			return nil
		}
		if timeoutErr := handleNetworkTimeout(err); timeoutErr != nil {
			return timeoutErr
		}
		retError = err
	}
	updateTimeStats(readTime, writeTime)

	const maxRetries = 100               // Maximum number of consecutive errors
	const maxTotalTime = 3 * time.Minute // Maximum total consecutive processing time
	var consecutiveErrors int
	consecutiveLoopStartTime := time.Now()

	for {
		skipWrite, readTime, writeTime, err = readThenWrite(ses, execCtx, param, writer, mysqlRwer, skipWrite, epoch)
		if err != nil {
			if errors.Is(err, errorInvalidLength0) {
				if retError != nil {
					err = retError
					break
				}
				err = nil
				break
			}

			if timeoutErr := handleNetworkTimeout(err); timeoutErr != nil {
				return timeoutErr
			}

			retError = err
			consecutiveErrors++
			ses.Errorf(execCtx.reqCtx, "readThenWrite error (attempt %d): %v", consecutiveErrors, err)
			time.Sleep(10 * time.Millisecond)

			if consecutiveErrors >= maxRetries || time.Since(consecutiveLoopStartTime) > maxTotalTime {
				return moerr.NewInternalErrorf(execCtx.reqCtx,
					"load local file failed: consecutive errors (%d), timeout after %v", maxRetries, maxTotalTime)
			}
		} else {
			consecutiveErrors = 0
			consecutiveLoopStartTime = time.Now()
		}

		updateTimeStats(readTime, writeTime)

		if epoch%printTime == 0 {
			if execCtx.isIssue3482 {
				ses.Infof(execCtx.reqCtx, "load local '%s', epoch: %d, skipWrite: %v, minReadTime: %s, maxReadTime: %s, minWriteTime: %s, maxWriteTime: %s,\n", param.Filepath, epoch, skipWrite, minReadTime.String(), maxReadTime.String(), minWriteTime.String(), maxWriteTime.String())
			}
			minReadTime, maxReadTime, minWriteTime, maxWriteTime = 24*time.Hour, time.Nanosecond, 24*time.Hour, time.Nanosecond
		}
		epoch += 1
	}

	if execCtx.isIssue3482 {
		ses.Infof(execCtx.reqCtx, "load local '%s', read&write all data from client cost: %s\n", param.Filepath, time.Since(start))
	}
	return
}

func makeCompactTxnInfo(op TxnOperator) string {
	txn := op.Txn()
	return fmt.Sprintf("%s:%s", hex.EncodeToString(txn.ID), txn.SnapshotTS.DebugString())
}

func executeStmtWithResponse(ses *Session,
	execCtx *ExecCtx,
) (err error) {
	ses.EnterFPrint(FPStmtWithResponse)
	defer ses.ExitFPrint(FPStmtWithResponse)
	var span trace.Span
	execCtx.reqCtx, span = trace.Start(execCtx.reqCtx, "executeStmtWithResponse",
		trace.WithKind(trace.SpanKindStatement))
	defer span.End(trace.WithStatementExtra(ses.GetTxnId(), ses.GetStmtId(), ses.GetSqlOfStmt()))

	ses.SetQueryInProgress(true)
	ses.SetQueryStart(time.Now())
	ses.SetQueryInExecute(true)
	defer ses.SetQueryEnd(time.Now())
	defer ses.SetQueryInProgress(false)

	err = executeStmtWithTxn(ses, nil, execCtx)
	if err != nil {
		return err
	}

	// TODO put in one txn
	// insert data after create table in "create table ... as select ..." stmt
	if ses.createAsSelectSql != "" {
		ses.EnterFPrint(FPStmtWithResponseCreateAsSelect)
		defer ses.ExitFPrint(FPStmtWithResponseCreateAsSelect)
		sql := ses.createAsSelectSql
		ses.createAsSelectSql = ""
		tempExecCtx := ExecCtx{
			ses:    ses,
			reqCtx: execCtx.reqCtx,
		}
		defer tempExecCtx.Close()
		if err = doComQuery(ses, &tempExecCtx, &UserInput{sql: sql}); err != nil {
			return err
		}
	}

	err = respClientWhenSuccess(ses, execCtx)
	if err != nil {
		return err
	}

	return
}

func executeStmtWithTxn(ses FeSession,
	statsArr *statistic.StatsArray,
	execCtx *ExecCtx,
) (err error) {
	ses.EnterFPrint(FPExecStmtWithTxn)
	defer ses.ExitFPrint(FPExecStmtWithTxn)
	if !ses.IsDerivedStmt() {
		err = executeStmtWithWorkspace(ses, statsArr, execCtx)
	} else {

		txnOp := ses.GetTxnHandler().GetTxn()
		//refresh proc txnOp
		execCtx.proc.Base.TxnOperator = txnOp

		err = dispatchStmt(ses, statsArr, execCtx)
	}
	return
}

func executeStmtWithWorkspace(ses FeSession,
	statsArr *statistic.StatsArray,
	execCtx *ExecCtx,
) (err error) {
	ses.EnterFPrint(FPExecStmtWithWorkspace)
	defer ses.ExitFPrint(FPExecStmtWithWorkspace)
	if ses.IsDerivedStmt() {
		return
	}
	var autocommit bool
	//derived stmt shares the same txn with ancestor.
	//it only executes select statements.

	//7. pass or commit or rollback txn
	// defer transaction state management.
	defer func() {
		if e := recover(); e != nil {
			moe, ok := e.(*moerr.Error)
			if !ok {
				err = errors.Join(err, moerr.ConvertPanicError(execCtx.reqCtx, e))
			} else {
				err = errors.Join(err, moe)
			}

			ses.Error(execCtx.reqCtx, "recover from panic before finishTxnFunc", zap.Error(err))
		}
		err = finishTxnFunc(ses, err, execCtx)
	}()

	_, _, _ = fault.TriggerFault("executeStmtWithWorkspace_panic")

	//1. start txn
	//special BEGIN,COMMIT,ROLLBACK
	beginStmt := false
	execCtx.txnOpt.Close()
	switch execCtx.stmt.(type) {
	case *tree.BeginTransaction:
		execCtx.txnOpt.byBegin = true
		beginStmt = true
	case *tree.CommitTransaction:
		execCtx.txnOpt.byCommit = true
		return nil
	case *tree.RollbackTransaction:
		execCtx.txnOpt.byRollback = true
		return nil
	case *tree.SavePoint, *tree.ReleaseSavePoint:
		return nil
	case *tree.RollbackToSavePoint:
		return moerr.NewInternalError(execCtx.reqCtx, "savepoint has not been implemented yet. please rollback the transaction.")
	}

	//in session migration, the txn forced to be autocommit.
	//then the txn can be committed.
	if execCtx.inMigration {
		autocommit = true
	} else {
		autocommit, err = autocommitValue(ses)
		if err != nil {
			return err
		}
	}

	execCtx.txnOpt.autoCommit = autocommit
	err = ses.GetTxnHandler().Create(execCtx)
	if err != nil {
		return err
	}

	//skip BEGIN stmt
	if beginStmt {
		return err
	}

	if ses.GetTxnHandler() == nil {
		panic("need txn handler")
	}

	txnOp := ses.GetTxnHandler().GetTxn()

	//refresh txn id
	ses.SetTxnId(txnOp.Txn().ID)
	ses.SetStaticTxnInfo(makeCompactTxnInfo(txnOp))

	//refresh proc txnOp
	execCtx.proc.Base.TxnOperator = txnOp

	err = disttae.CheckTxnIsValid(txnOp)
	if err != nil {
		return err
	}

	ses.EnterFPrint(FPExecStmtWithWorkspaceBeforeStart)
	defer ses.ExitFPrint(FPExecStmtWithWorkspaceBeforeStart)
	//!!!NOTE!!!: statement management
	//2. start statement on workspace
	txnOp.GetWorkspace().StartStatement()
	//3. end statement on workspace
	// defer Start/End Statement management, called after finishTxnFunc()
	defer func() {
		if ses.GetTxnHandler() == nil {
			panic("need txn handler 2")
		}

		txnOp = ses.GetTxnHandler().GetTxn()
		if txnOp != nil {
			ses.EnterFPrint(FPExecStmtWithWorkspaceBeforeEnd)
			defer ses.ExitFPrint(FPExecStmtWithWorkspaceBeforeEnd)
			//most of the cases, txnOp will not nil except that "set autocommit = 1"
			//commit the txn immediately then the txnOp is nil.
			txnOp.GetWorkspace().EndStatement()
		}
	}()

	err = executeStmtWithIncrStmt(ses, statsArr, execCtx, txnOp)

	return
}

func executeStmtWithIncrStmt(ses FeSession,
	statsArr *statistic.StatsArray,
	execCtx *ExecCtx,
	txnOp TxnOperator,
) (err error) {
	var hasRecovered bool
	ses.EnterFPrint(FPExecStmtWithIncrStmt)
	defer ses.ExitFPrint(FPExecStmtWithIncrStmt)

	err = disttae.CheckTxnIsValid(txnOp)
	if err != nil {
		return err
	}

	if ses.IsDerivedStmt() {
		return
	}
	ses.EnterFPrint(FPExecStmtWithIncrStmtBeforeIncr)
	defer ses.ExitFPrint(FPExecStmtWithIncrStmtBeforeIncr)
	//3. increase statement id

	crs := new(perfcounter.CounterSet)
	newCtx := perfcounter.AttachS3RequestKey(execCtx.reqCtx, crs)
	err, hasRecovered = ExecuteFuncWithRecover(func() error {
		return txnOp.GetWorkspace().IncrStatementID(newCtx, false)
	})
	if err != nil || hasRecovered {
		return err
	}
	stats := statistic.StatsInfoFromContext(newCtx)
	stats.AddTxnIncrStatementS3Request(statistic.S3Request{
		List:      crs.FileService.S3.List.Load(),
		Head:      crs.FileService.S3.Head.Load(),
		Put:       crs.FileService.S3.Put.Load(),
		Get:       crs.FileService.S3.Get.Load(),
		Delete:    crs.FileService.S3.Delete.Load(),
		DeleteMul: crs.FileService.S3.DeleteMulti.Load(),
	})

	defer func() {
		if ses.GetTxnHandler() == nil {
			panic("need txn handler 3")
		}

		//!!!NOTE!!!: it does not work
		//_, txnOp = ses.GetTxnHandler().GetTxn()
		//if txnOp != nil {
		//	err = rollbackLastStmt(execCtx, txnOp, err)
		//}
	}()

	err = dispatchStmt(ses, statsArr, execCtx)
	return
}

func dispatchStmt(ses FeSession,
	statsArr *statistic.StatsArray,
	execCtx *ExecCtx) (err error) {
	ses.EnterFPrint(FPDispatchStmt)
	defer ses.ExitFPrint(FPDispatchStmt)
	//5. check plan within txn
	if !execCtx.input.isBinaryProtExecute && execCtx.cw.Plan() != nil {
		flag, err := checkModify(execCtx.cw.Plan(), ses.GetTxnCompileCtx().Resolve)
		if err != nil {
			return err
		}
		if flag {
			//plan changed
			//clear all cached plan and parse sql again
			var stmts []tree.Statement
			stmts, err = parseSql(execCtx, ses.GetMySQLParser())
			if err != nil {
				return err
			}
			if len(stmts) != len(execCtx.cws) {
				return moerr.NewInternalError(execCtx.reqCtx, "the count of stmts parsed from cached sql is not equal to cws length")
			}
			for i, cw := range execCtx.cws {
				cw.ResetPlanAndStmt(stmts[i])
			}
		}
	}

	//6. execute stmt within txn
	switch sesImpl := ses.(type) {
	case *Session:
		return executeStmt(sesImpl, execCtx)
	case *backSession:
		return executeStmtInBack(sesImpl, statsArr, execCtx)
	default:
		return moerr.NewInternalError(execCtx.reqCtx, "no such session implementation")
	}
}

func executeStmt(ses *Session,
	execCtx *ExecCtx,
) (err error) {
	ses.EnterFPrint(FPExecStmt)
	defer ses.ExitFPrint(FPExecStmt)
	ses.GetTxnCompileCtx().tcw = execCtx.cw

	// record goroutine info when ddl stmt run timeout
	switch execCtx.stmt.(type) {
	case *tree.CreateTable, *tree.DropTable, *tree.CreateDatabase, *tree.DropDatabase:
		_, span := trace.Start(execCtx.reqCtx, "executeStmtHung",
			trace.WithHungThreshold(time.Minute), // be careful with this options
			trace.WithProfileGoroutine(),
			trace.WithProfileTraceSecs(10*time.Second),
		)
		defer span.End()
	default:
	}

	var cmpBegin time.Time
	var ret interface{}

	getExecLocation := func() tree.ExecLocation {
		// because when isBinaryProtExecute is true, execCtx.stmt is preparestmt, actually it's execute
		if execCtx.input.isBinaryProtExecute {
			return tree.EXEC_IN_ENGINE
		}
		return execCtx.stmt.StmtKind().ExecLocation()
	}
	switch getExecLocation() {
	case tree.EXEC_IN_FRONTEND:
		stats, err := execInFrontend(ses, execCtx)
		defer execCtx.cw.RecordCompoundStmt(execCtx.reqCtx, stats)
		return err
	case tree.EXEC_IN_ENGINE:
		//in the computation engine
	}

	switch st := execCtx.stmt.(type) {
	case *tree.Select:
		if st.Ep != nil {
			if getPu(ses.GetService()).SV.DisableSelectInto {
				err = moerr.NewSyntaxError(execCtx.reqCtx, "Unsupport select statement")
				return
			}
			ses.InitExportConfig(st.Ep)
			defer func() {
				ses.ClearExportParam()
			}()
			err = doCheckFilePath(execCtx.reqCtx, ses, st.Ep)
			if err != nil {
				return
			}
		}
	case *tree.CreateDatabase:
		err = inputNameIsInvalid(execCtx.reqCtx, string(st.Name))
		if err != nil {
			return
		}
		if st.SubscriptionOption != nil && ses.GetTenantInfo() != nil && !ses.GetTenantInfo().IsAdminRole() {
			err = moerr.NewInternalError(execCtx.reqCtx, "only admin can create subscription")
			return
		}
		st.Sql = execCtx.sqlOfStmt
	case *tree.DropDatabase:
		err = inputNameIsInvalid(execCtx.reqCtx, string(st.Name))
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
	case *tree.ExplainPhyPlan:
		ses.SetData(nil)
	case *tree.ShowTableStatus:
		ses.SetShowStmtType(ShowTableStatus)
		ses.SetData(nil)
	case *tree.Load:
		if st.Local {
			execCtx.proc.Base.LoadLocalReader, execCtx.loadLocalWriter = io.Pipe()
		}
	case *tree.ShowGrants:
		if len(st.Username) == 0 {
			st.Username = execCtx.userName
		}
		if len(st.Hostname) == 0 || st.Hostname == "%" {
			st.Hostname = rootHost
		}
	}

	cmpBegin = time.Now()

	ses.EnterFPrint(FPExecStmtBeforeCompile)
	defer ses.ExitFPrint(FPExecStmtBeforeCompile)
	if ret, err = execCtx.cw.Compile(execCtx, ses.GetOutputCallback(execCtx)); err != nil {
		return
	}

	defer func() {
		if c, ok := ret.(*compile.Compile); ok {
			var phyPlan *models.PhyPlan
			analyzeModule := c.GetAnalyzeModule()
			if analyzeModule != nil {
				phyPlan = analyzeModule.GetPhyPlan()
				execCtx.cw.SetExplainBuffer(analyzeModule.GetExplainPhyBuffer())
			}

			// Serialize the execution plan as json
			_ = execCtx.cw.RecordExecPlan(execCtx.reqCtx, phyPlan)
			c.Release()
		}
	}()

	// cw.Compile may rewrite the stmt in the EXECUTE statement, we fetch the latest version
	//need to check again.
	execCtx.stmt = execCtx.cw.GetAst()
	switch execCtx.stmt.StmtKind().ExecLocation() {
	case tree.EXEC_IN_FRONTEND:
		_, err = execInFrontend(ses, execCtx)
		return err
	case tree.EXEC_IN_ENGINE:

	}

	execCtx.runner = ret.(ComputationRunner)

	// only log if build time is longer than 1s
	if time.Since(cmpBegin) > time.Second {
		ses.Infof(execCtx.reqCtx, "time of Exec.Build : %s", time.Since(cmpBegin).String())
	}

	//output result & status
	StmtKind := execCtx.stmt.StmtKind().OutputType()
	switch StmtKind {
	case tree.OUTPUT_RESULT_ROW:
		err = executeResultRowStmt(ses, execCtx)
		if err != nil {
			return err
		}
	case tree.OUTPUT_STATUS:
		err = executeStatusStmt(ses, execCtx)
		if err != nil {
			return err
		}
	case tree.OUTPUT_UNDEFINED:
		if _, ok := execCtx.stmt.(*tree.Execute); !ok {
			return moerr.NewInternalErrorf(execCtx.reqCtx, "need set result type for %s", execCtx.sqlOfStmt)
		}
	}

	return
}

// execute query
func doComQuery(ses *Session, execCtx *ExecCtx, input *UserInput) (retErr error) {
	ses.EnterFPrint(FPDoComQuery)
	defer ses.ExitFPrint(FPDoComQuery)
	ses.GetTxnCompileCtx().SetExecCtx(execCtx)
	// set the batch buf for stream scan
	var inMemStreamScan []*kafka.Message

	if batchValue, ok := execCtx.reqCtx.Value(defines.SourceScanResKey{}).([]*kafka.Message); ok {
		inMemStreamScan = batchValue
	}

	beginInstant := time.Now()
	execCtx.reqCtx = appendStatementAt(execCtx.reqCtx, beginInstant)
	input.genSqlSourceType(ses)
	ses.SetShowStmtType(NotShowStatement)
	resper := ses.GetResponser()
	ses.SetSql(input.getSql())
	input.genHash()
	version := ses.GetCreateVersion()
	if len(version) == 0 {
		version = serverVersion.Load().(string)
	}

	sqlLen := len(input.getSql())
	if sqlLen != 0 {
		v2.TotalSQLLengthHistogram.Observe(float64(sqlLen))
		if strings.HasPrefix(input.sql, "LOAD DATA INLINE") {
			v2.LoadDataInlineSQLLengthHistogram.Observe(float64(sqlLen))
		} else {
			v2.OtherSQLLengthHistogram.Observe(float64(sqlLen))
		}
	}

	//the ses.GetUserName returns the user_name with the account_name.
	//here,we only need the user_name.
	userNameOnly := rootName

	// case: exec `set @ t= 2;` will trigger an internal query, like: `select 1 from dual`, in the same session.
	defer func(stmt *motrace.StatementInfo) {
		if stmt != nil {
			ses.tStmt = stmt
		}
	}(ses.tStmt)
	ses.tStmt = nil

	proc := ses.proc
	proc.ReplaceTopCtx(execCtx.reqCtx)

	pu := getPu(ses.GetService())
	proc.Base.Id = ses.getNextProcessId()
	proc.Base.Lim.Size = pu.SV.ProcessLimitationSize
	proc.Base.Lim.BatchRows = pu.SV.ProcessLimitationBatchRows
	proc.Base.Lim.MaxMsgSize = pu.SV.MaxMessageSize
	proc.Base.Lim.PartitionRows = pu.SV.ProcessLimitationPartitionRows
	proc.Base.SessionInfo = process.SessionInfo{
		User:                 ses.GetUserName(),
		Host:                 pu.SV.Host,
		ConnectionID:         uint64(resper.GetU32(CONNID)),
		Database:             ses.GetDatabaseName(),
		Version:              makeServerVersion(pu, version),
		TimeZone:             ses.GetTimeZone(),
		StorageEngine:        pu.StorageEngine,
		LastInsertID:         ses.GetLastInsertID(),
		SqlHelper:            ses.GetSqlHelper(),
		Buf:                  ses.GetBuffer(),
		SourceInMemScanBatch: inMemStreamScan,
		LogLevel:             zapcore.InfoLevel, //TODO: need set by session level config
		SessionId:            ses.GetSessId(),
	}
	proc.SetLastInsertID(ses.GetLastInsertID())
	proc.SetResolveVariableFunc(ses.txnCompileCtx.ResolveVariable)
	proc.InitSeq()
	// Copy curvalues stored in session to this proc.
	// Deep copy the map, takes some memory.
	ses.CopySeqToProc(proc)
	if ses.GetTenantInfo() != nil {
		proc.Base.SessionInfo.Account = ses.GetTenantInfo().GetTenant()
		proc.Base.SessionInfo.Role = ses.GetTenantInfo().GetDefaultRole()

		if len(ses.GetTenantInfo().GetVersion()) != 0 {
			proc.Base.SessionInfo.Version = ses.GetTenantInfo().GetVersion()
		}
		userNameOnly = ses.GetTenantInfo().GetUser()
	}
	var span trace.Span
	execCtx.reqCtx, span = trace.Start(execCtx.reqCtx, "doComQuery",
		trace.WithKind(trace.SpanKindStatement))
	defer span.End()

	proc.Base.SessionInfo.User = userNameOnly
	proc.Base.SessionInfo.QueryId = ses.getQueryId(input.isInternal())

	statsInfo := statistic.NewStatsInfo()
	statsInfo.ParseStage.ParseStartTime = beginInstant

	execCtx.reqCtx = statistic.ContextWithStatsInfo(execCtx.reqCtx, statsInfo)
	execCtx.input = input
	execCtx.isIssue3482 = input.isIssue3482Sql()

	cws, err := GetComputationWrapper(execCtx, ses.GetDatabaseName(),
		ses.GetUserName(),
		pu.StorageEngine,
		proc, ses)

	ParseDuration := time.Since(beginInstant)

	if err != nil {
		statsInfo.ParseStage.ParseDuration = ParseDuration
		var err2 error
		execCtx.reqCtx, err2 = RecordParseErrorStatement(execCtx.reqCtx, ses, proc, beginInstant, parsers.HandleSqlForRecord(input.getSql()), input.getSqlSourceTypes(), err)
		if err2 != nil {
			return err2
		}
		retErr = err
		if _, ok := err.(*moerr.Error); !ok {
			retErr = moerr.NewParseError(execCtx.reqCtx, err.Error())
		}
		logStatementStringStatus(execCtx.reqCtx, ses, input.getSql(), fail, retErr)
		return retErr
	}

	singleStatement := len(cws) == 1
	if ses.GetCmd() == COM_STMT_PREPARE && !singleStatement {
		return moerr.NewNotSupported(execCtx.reqCtx, "prepare multi statements")
	}

	defer func() {
		ses.SetMysqlResultSet(nil)
		ses.rs = nil
		ses.p = nil
	}()

	canCache := true
	Cached := false
	defer func() {
		execCtx.stmt = nil
		execCtx.cw = nil
		execCtx.cws = nil
		execCtx.runner = nil
		if !Cached {
			for i := 0; i < len(cws); i++ {
				cws[i].Free()
			}
		}
	}()
	sqlRecord := parsers.HandleSqlForRecord(input.getSql())

	for i, cw := range cws {
		if cw.GetAst().GetQueryType() == tree.QueryTypeDDL || cw.GetAst().GetQueryType() == tree.QueryTypeDCL ||
			cw.GetAst().GetQueryType() == tree.QueryTypeOth ||
			cw.GetAst().GetQueryType() == tree.QueryTypeTCL {
			if _, ok := cw.GetAst().(*tree.SetVar); !ok {
				ses.cleanCache()
			}
			canCache = false
		}

		ses.SetMysqlResultSet(&MysqlResultSet{})
		ses.sentRows.Store(int64(0))
		ses.writeCsvBytes.Store(int64(0))
		resper.ResetStatistics() // move from getDataFromPipeline, for record column fields' data
		stmt := cw.GetAst()
		sqlType := input.getSqlSourceType(i)
		var err2 error
		execCtx.reqCtx, err2 = RecordStatement(execCtx.reqCtx, ses, proc, cw, beginInstant, sqlRecord[i], sqlType, singleStatement)
		if err2 != nil {
			return err2
		}

		statsInfo.Reset()
		//average parse duration
		statsInfo.ParseStage.ParseStartTime = beginInstant
		statsInfo.ParseStage.ParseDuration = time.Duration(ParseDuration.Nanoseconds() / int64(len(cws)))

		tenant := ses.GetTenantNameWithStmt(stmt)
		//skip PREPARE statement here
		if ses.GetTenantInfo() != nil && !IsPrepareStatement(stmt) {
			authStats, err := authenticateUserCanExecuteStatement(execCtx.reqCtx, ses, stmt)
			if err != nil {
				logStatementStatus(execCtx.reqCtx, ses, stmt, fail, err)
				return err
			}
			statsInfo.PermissionAuth.Add(&authStats)
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
		if ses.GetTxnHandler().InActiveTxn() {
			err = canExecuteStatementInUncommittedTransaction(execCtx.reqCtx, ses, stmt)
			if err != nil {
				logStatementStatus(execCtx.reqCtx, ses, stmt, fail, err)
				return err
			}
		}

		// update UnixTime for new query, which is used for now() / CURRENT_TIMESTAMP
		proc.Base.UnixTime = time.Now().UnixNano()
		if ses.proc != nil {
			ses.proc.Base.UnixTime = proc.Base.UnixTime
		}
		execCtx.txnOpt.Close()
		execCtx.stmt = stmt
		execCtx.isLastStmt = i >= len(cws)-1
		execCtx.tenant = tenant
		execCtx.userName = userNameOnly
		execCtx.sqlOfStmt = sqlRecord[i]
		execCtx.cw = cw
		execCtx.proc = proc
		execCtx.resper = resper
		execCtx.ses = ses
		execCtx.cws = cws
		execCtx.input = input

		err = executeStmtWithResponse(ses, execCtx)
		if err != nil {
			return err
		}

	} // end of for

	if canCache && !ses.isCached(input.getHash()) {
		plans := make([]*plan.Plan, len(cws))
		stmts := make([]tree.Statement, len(cws))
		for i, cw := range cws {
			if checkNodeCanCache(cw.Plan()) {
				plans[i] = cw.Plan()
				stmts[i] = cw.GetAst()
			} else {
				return nil
			}
			cw.Clear()
		}
		Cached = true
		ses.cachePlan(input.getHash(), stmts, plans)
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
func ExecRequest(ses *Session, execCtx *ExecCtx, req *Request) (resp *Response, err error) {
	defer func() {
		if e := recover(); e != nil {
			moe, ok := e.(*moerr.Error)
			if !ok {
				err = errors.Join(err, moerr.ConvertPanicError(execCtx.reqCtx, e))
				resp = NewGeneralErrorResponse(COM_QUERY, ses.txnHandler.GetServerStatus(), err)
			} else {
				err = errors.Join(err, moe)
				resp = NewGeneralErrorResponse(COM_QUERY, ses.txnHandler.GetServerStatus(), moe)
			}
			// log the query's statement and error info.
			logStatementStatus(execCtx.reqCtx, ses, execCtx.stmt, fail, err)
		}
	}()
	_, _, _ = fault.TriggerFaultInDomain(fault.DomainFrontend, "exec_request_panic")

	ses.EnterFPrint(FPExecRequest)
	defer ses.ExitFPrint(FPExecRequest)

	var span trace.Span
	execCtx.reqCtx, span = trace.Start(execCtx.reqCtx, "ExecRequest",
		trace.WithKind(trace.SpanKindStatement))
	defer span.End()

	var sql string
	ses.Debugf(execCtx.reqCtx, "cmd %v", req.GetCmd())
	ses.SetCmd(req.GetCmd())
	switch req.GetCmd() {
	case COM_QUIT:
		return resp, moerr.GetMysqlClientQuit()
	case COM_QUERY:
		var query = commonutil.UnsafeBytesToString(req.GetData().([]byte))
		ses.addSqlCount(1)
		ses.Debug(execCtx.reqCtx, "query trace", logutil.QueryField(commonutil.Abbreviate(query, int(getPu(ses.GetService()).SV.LengthOfQueryPrinted))))
		input := &UserInput{sql: query}
		err = doComQuery(ses, execCtx, input)
		if err != nil {
			resp = NewGeneralErrorResponse(COM_QUERY, ses.GetTxnHandler().GetServerStatus(), err)
			resp.isIssue3482 = input.isIssue3482Sql()
			if resp.isIssue3482 {
				resp.loadLocalFile = query
			}
		}
		return resp, nil
	case COM_INIT_DB:
		var dbname = commonutil.UnsafeBytesToString(req.GetData().([]byte))
		ses.addSqlCount(1)
		query := "use `" + dbname + "`"
		err = doComQuery(ses, execCtx, &UserInput{sql: query})
		if err != nil {
			resp = NewGeneralErrorResponse(COM_INIT_DB, ses.GetTxnHandler().GetServerStatus(), err)
		}

		return resp, nil
	case COM_FIELD_LIST:
		var payload = commonutil.UnsafeBytesToString(req.GetData().([]byte))
		ses.addSqlCount(1)
		query := makeCmdFieldListSql(payload)
		err = doComQuery(ses, execCtx, &UserInput{sql: query})
		if err != nil {
			resp = NewGeneralErrorResponse(COM_FIELD_LIST, ses.GetTxnHandler().GetServerStatus(), err)
		}

		return resp, nil
	case COM_PING:
		resp = NewGeneralOkResponse(COM_PING, ses.GetTxnHandler().GetServerStatus())

		return resp, nil

	case COM_STMT_PREPARE:
		ses.SetCmd(COM_STMT_PREPARE)
		sql = commonutil.UnsafeBytesToString(req.GetData().([]byte))
		ses.addSqlCount(1)

		// rewrite to "Prepare stmt_name from 'xxx'"
		newLastStmtID := ses.GenNewStmtId()
		newStmtName := getPrepareStmtName(newLastStmtID)
		sql = fmt.Sprintf("prepare %s from %s", newStmtName, sql)
		ses.Debug(execCtx.reqCtx, "query trace", logutil.QueryField(sql))

		err = doComQuery(ses, execCtx, &UserInput{sql: sql})
		if err != nil {
			resp = NewGeneralErrorResponse(COM_STMT_PREPARE, ses.GetTxnHandler().GetServerStatus(), err)
		}
		return resp, nil

	case COM_STMT_EXECUTE:
		ses.SetCmd(COM_STMT_EXECUTE)
		var prepareStmt *PrepareStmt
		sql, prepareStmt, err = parseStmtExecute(execCtx.reqCtx, ses, req.GetData().([]byte))
		if err != nil {
			return NewGeneralErrorResponse(COM_STMT_EXECUTE, ses.GetTxnHandler().GetServerStatus(), err), nil
		}
		execCtx.prepareColDef = prepareStmt.ColDefData
		err = doComQuery(ses, execCtx, &UserInput{sql: sql, stmtName: prepareStmt.Name, stmt: prepareStmt.PrepareStmt, preparePlan: prepareStmt.PreparePlan, isBinaryProtExecute: true})
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
		err = parseStmtSendLongData(execCtx.reqCtx, ses, req.GetData().([]byte))
		if err != nil {
			resp = NewGeneralErrorResponse(COM_STMT_SEND_LONG_DATA, ses.GetTxnHandler().GetServerStatus(), err)
			return resp, nil
		}
		return nil, nil

	case COM_STMT_CLOSE:
		// rewrite to "deallocate Prepare stmt_name"
		stmtID := binary.LittleEndian.Uint32(req.GetData().([]byte)[0:4])
		var preStmt *PrepareStmt
		stmtName := getPrepareStmtName(stmtID)
		preStmt, err = ses.GetPrepareStmt(execCtx.reqCtx, stmtName)
		if err != nil {
			resp = NewGeneralErrorResponse(COM_STMT_CLOSE, ses.GetTxnHandler().GetServerStatus(), err)
		}
		prefix := ""
		if preStmt.IsCloudNonuser {
			prefix = "/* cloud_nonuser */"
		}
		sql = fmt.Sprintf("%sdeallocate prepare %s", prefix, stmtName)
		ses.Debug(execCtx.reqCtx, "query trace", logutil.QueryField(sql))

		err = doComQuery(ses, execCtx, &UserInput{sql: sql})
		if err != nil {
			resp = NewGeneralErrorResponse(COM_STMT_CLOSE, ses.GetTxnHandler().GetServerStatus(), err)
		}
		return resp, nil

	case COM_STMT_RESET:
		//Payload of COM_STMT_RESET
		stmtID := binary.LittleEndian.Uint32(req.GetData().([]byte)[0:4])
		stmtName := getPrepareStmtName(stmtID)
		var preStmt *PrepareStmt
		preStmt, err = ses.GetPrepareStmt(execCtx.reqCtx, stmtName)
		if err != nil {
			resp = NewGeneralErrorResponse(COM_STMT_CLOSE, ses.GetTxnHandler().GetServerStatus(), err)
		}
		prefix := ""
		if preStmt.IsCloudNonuser {
			prefix = "/* cloud_nonuser */"
		}
		sql = fmt.Sprintf("%sreset prepare %s", prefix, stmtName)
		ses.Debug(execCtx.reqCtx, "query trace", logutil.QueryField(sql))
		err = doComQuery(ses, execCtx, &UserInput{sql: sql})
		if err != nil {
			resp = NewGeneralErrorResponse(COM_STMT_RESET, ses.GetTxnHandler().GetServerStatus(), err)
		}
		return resp, nil

	case COM_SET_OPTION:
		err = handleSetOption(ses, execCtx, req.GetData().([]byte))
		if err != nil {
			resp = NewGeneralErrorResponse(COM_SET_OPTION, ses.GetTxnHandler().GetServerStatus(), err)
		}
		return NewGeneralOkResponse(COM_SET_OPTION, ses.GetTxnHandler().GetServerStatus()), nil

	default:
		resp = NewGeneralErrorResponse(req.GetCmd(), ses.GetTxnHandler().GetServerStatus(), moerr.NewInternalErrorf(execCtx.reqCtx, "unsupported command. 0x%x", int64(req.GetCmd())))
	}
	return resp, nil
}

func parseStmtExecute(reqCtx context.Context, ses *Session, data []byte) (string, *PrepareStmt, error) {
	// see https://dev.mysql.com/doc/dev/mysql-server/latest/page_protocol_com_stmt_execute.html
	pos := 0
	if len(data) < 4 {
		return "", nil, moerr.NewInvalidInput(reqCtx, "sql command contains malformed packet")
	}
	stmtID := binary.LittleEndian.Uint32(data[0:4])
	pos += 4

	stmtName := fmt.Sprintf("%s_%d", prefixPrepareStmtName, stmtID)
	preStmt, err := ses.GetPrepareStmt(reqCtx, stmtName)
	if err != nil {
		return "", nil, err
	}

	var sql string
	prefix := ""
	if preStmt.IsCloudNonuser {
		prefix = "/* cloud_nonuser */"
	}
	sql = fmt.Sprintf("%sexecute %s", prefix, stmtName)

	ses.Debug(reqCtx, "query trace", logutil.QueryField(sql))
	err = ses.GetResponser().MysqlRrWr().ParseExecuteData(reqCtx, ses.GetProc(), preStmt, data, pos)
	if err != nil {
		return "", nil, err
	}
	return sql, preStmt, nil
}

func parseStmtSendLongData(reqCtx context.Context, ses *Session, data []byte) error {
	// see https://dev.mysql.com/doc/dev/mysql-server/latest/page_protocol_com_stmt_send_long_data.html
	pos := 0
	if len(data) < 4 {
		return moerr.NewInvalidInput(reqCtx, "sql command contains malformed packet")
	}
	stmtID := binary.LittleEndian.Uint32(data[0:4])
	pos += 4

	stmtName := fmt.Sprintf("%s_%d", prefixPrepareStmtName, stmtID)
	preStmt, err := ses.GetPrepareStmt(reqCtx, stmtName)
	if err != nil {
		return err
	}

	var sql string
	prefix := ""
	if preStmt.IsCloudNonuser {
		prefix = "/* cloud_nonuser */"
	}
	sql = fmt.Sprintf("%ssend long data for stmt %s", prefix, stmtName)

	ses.Debug(reqCtx, "query trace", logutil.QueryField(sql))

	err = ses.GetResponser().MysqlRrWr().ParseSendLongData(reqCtx, ses.GetProc(), preStmt, data, pos)
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
	case types.T_datalink:
		col.SetColumnType(defines.MYSQL_TYPE_TEXT)
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
	case types.T_year:
		col.SetColumnType(defines.MYSQL_TYPE_YEAR)
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
	case types.T_Rowid:
		col.SetColumnType(defines.MYSQL_TYPE_VARCHAR)
	default:
		return moerr.NewInternalErrorf(ctx, "RunWhileSend : unsupported type %d", engineType)
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
	commonutil.EncodeUUIDHex(bytes[:], uuid[:])
	explainData := models.ExplainData{
		Code:    errcode,
		Message: msg,
		Uuid:    commonutil.UnsafeBytesToString(bytes[:]),
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

func NewJsonPlanHandler(ctx context.Context, stmt *motrace.StatementInfo, ses FeSession, plan *plan2.Plan, phyPlan *models.PhyPlan, opts ...marshalPlanOptions) *jsonPlanHandler {
	h := NewMarshalPlanHandler(ctx, stmt, plan, phyPlan, opts...)
	jsonBytes := h.Marshal(ctx)
	statsBytes, stats := h.Stats(ctx, ses)
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

type marshalPlanConfig struct {
	waitActiveCost time.Duration
}

type marshalPlanOptions func(*marshalPlanConfig)

func WithWaitActiveCost(cost time.Duration) marshalPlanOptions {
	return func(h *marshalPlanConfig) {
		h.waitActiveCost = cost
	}
}

type marshalPlanHandler struct {
	query       *plan.Query
	marshalPlan *models.ExplainData
	stmt        *motrace.StatementInfo
	uuid        uuid.UUID
	buffer      *bytes.Buffer
	// internal sub statements, such as sub statements of compound statements, is not user SQL requests,
	isInternalSubStmt bool

	marshalPlanConfig
}

func NewMarshalPlanHandler(ctx context.Context, stmt *motrace.StatementInfo, plan *plan2.Plan, phyPlan *models.PhyPlan, opts ...marshalPlanOptions) *marshalPlanHandler {
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
	// END> new marshalPlanHandler

	// SET options
	for _, opt := range opts {
		opt(&h.marshalPlanConfig)
	}

	if h.needMarshalPlan() {
		h.marshalPlan = explain.BuildJsonPlan(ctx, h.uuid, &explain.MarshalPlanOptions, h.query)
		h.marshalPlan.NewPlanStats.SetWaitActiveCost(h.waitActiveCost)
		if phyPlan != nil {
			h.marshalPlan.PhyPlan = *phyPlan
		}
	}
	return h
}

// NewMarshalPlanHandlerCompositeSubStmt MarshalHandler for child statements of composite statements
func NewMarshalPlanHandlerCompositeSubStmt(ctx context.Context, plan *plan.Plan, opts ...marshalPlanOptions) *marshalPlanHandler {
	if plan == nil || plan.GetQuery() == nil {
		return &marshalPlanHandler{
			query:             nil,
			marshalPlan:       nil,
			buffer:            nil,
			isInternalSubStmt: true,
		}
	}
	query := plan.GetQuery()
	h := &marshalPlanHandler{
		query:             query,
		buffer:            nil,
		isInternalSubStmt: true,
	}

	// SET options
	for _, opt := range opts {
		opt(&h.marshalPlanConfig)
	}
	return h
}

// needMarshalPlan return true if statement.duration - waitActive > longQueryTime && NOT mo_logger query
// check longQueryTime, need after StatementInfo.MarkResponseAt
// MoLogger NOT record ExecPlan
func (h *marshalPlanHandler) needMarshalPlan() bool {
	return (h.stmt.Duration-h.waitActiveCost) > motrace.GetLongQueryTime() &&
		!h.stmt.IsMoLogger()
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
			moError := moerr.NewInternalErrorf(ctx, "serialize plan to json error: %s", err.Error())
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

var sqlQueryIgnoreExecPlan = []byte(`{}`)
var sqlQueryNoRecordExecPlan = []byte(`{"code":200,"message":"sql query no record execution plan"}`)

func (h *marshalPlanHandler) Stats(ctx context.Context, ses FeSession) (statsByte statistic.StatsArray, stats motrace.Statistic) {
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
		operatorTimeConsumed := int64(statsByte.GetTimeConsumed())
		totalTime := operatorTimeConsumed +
			int64(statsInfo.ParseStage.ParseDuration) +
			int64(statsInfo.PlanStage.PlanDuration) +
			int64(statsInfo.CompileStage.CompileDuration) +
			statsInfo.PrepareRunStage.ScopePrepareDuration +
			statsInfo.PrepareRunStage.CompilePreRunOnceDuration -
			statsInfo.PrepareRunStage.CompilePreRunOnceWaitLock -
			statsInfo.PlanStage.BuildPlanStatsIOConsumption -
			(statsInfo.IOAccessTimeConsumption + statsInfo.S3FSPrefetchFileIOMergerTimeConsumption)

		if totalTime < 0 {
			if !h.isInternalSubStmt {
				ses.Infof(ctx, "negative cpu statement_id:%s, statement_type:%s, statsInfo:[Parse(%d)+BuildPlan(%d)+Compile(%d)+PhyExec(%d)+PrepareRun(%d)-PreRunWaitLock(%d)-PlanStatsIO(%d)-IOAccess(%d)-IOMerge(%d) = %d]",
					uuid.UUID(h.stmt.StatementID).String(),
					h.stmt.StatementType,
					statsInfo.ParseStage.ParseDuration,
					statsInfo.PlanStage.PlanDuration,
					statsInfo.CompileStage.CompileDuration,
					operatorTimeConsumed,
					statsInfo.PrepareRunStage.ScopePrepareDuration+statsInfo.PrepareRunStage.CompilePreRunOnceDuration,
					statsInfo.PrepareRunStage.CompilePreRunOnceWaitLock,
					statsInfo.PlanStage.BuildPlanStatsIOConsumption,
					statsInfo.IOAccessTimeConsumption,
					statsInfo.S3FSPrefetchFileIOMergerTimeConsumption,
					totalTime,
				)
			}
			v2.GetTraceNegativeCUCounter("cpu").Inc()
		} else {
			statsByte.WithTimeConsumed(float64(totalTime))
		}

		planS3Input := statsInfo.PlanStage.BuildPlanS3Request.CountPUT()
		planS3Output := statsInfo.PlanStage.BuildPlanS3Request.CountGET()
		planS3List := statsInfo.PlanStage.BuildPlanS3Request.CountLIST()
		planS3Delete := statsInfo.PlanStage.BuildPlanS3Request.CountDELETE()

		compileS3Input := statsInfo.CompileStage.CompileS3Request.CountPUT()
		compileS3Output := statsInfo.CompileStage.CompileS3Request.CountGET()
		compileS3List := statsInfo.CompileStage.CompileS3Request.CountLIST()
		compileS3Delete := statsInfo.CompileStage.CompileS3Request.CountDELETE()

		preRunS3Input := statsInfo.PrepareRunStage.ScopePrepareS3Request.CountPUT()
		preRunS3Output := statsInfo.PrepareRunStage.ScopePrepareS3Request.CountGET()
		preRunS3List := statsInfo.PrepareRunStage.ScopePrepareS3Request.CountLIST()
		preRunS3Delete := statsInfo.PrepareRunStage.ScopePrepareS3Request.CountDELETE()

		totalS3Input := statsByte.GetS3IOInputCount() + float64(planS3Input+compileS3Input+preRunS3Input)
		totalS3Output := statsByte.GetS3IOOutputCount() + float64(planS3Output+compileS3Output+preRunS3Output)
		totalS3List := statsByte.GetS3IOListCount() + float64(planS3List+compileS3List+preRunS3List)
		totalS3Delete := statsByte.GetS3IODeleteCount() + float64(planS3Delete+compileS3Delete+preRunS3Delete)

		statsByte.WithS3IOInputCount(totalS3Input)
		statsByte.WithS3IOOutputCount(totalS3Output)
		statsByte.WithS3IOListCount(totalS3List)
		statsByte.WithS3IODeleteCount(totalS3Delete)

		// Additional permission authentication SQL statistics
		statsByte.Add(&statsInfo.PermissionAuth)
	}
	return
}

func handleSetOption(ses *Session, execCtx *ExecCtx, data []byte) (err error) {
	if len(data) < 2 {
		return moerr.NewInternalError(execCtx.reqCtx, "invalid cmd_set_option data length")
	}
	cap := ses.GetResponser().MysqlRrWr().GetU32(CAPABILITY)
	switch binary.LittleEndian.Uint16(data[:2]) {
	case 0:
		// MO do not support CLIENT_MULTI_STATEMENTS in prepare, so do nothing here(Like MySQL)
		// cap |= CLIENT_MULTI_STATEMENTS
		// GetSession().GetMysqlProtocol().SetCapability(cap)

	case 1:
		cap &^= CLIENT_MULTI_STATEMENTS
		ses.GetResponser().MysqlRrWr().SetU32(CAPABILITY, cap)

	default:
		return moerr.NewInternalError(execCtx.reqCtx, "invalid cmd_set_option data")
	}

	return nil
}

func handleExecUpgrade(ses *Session, execCtx *ExecCtx, st *tree.UpgradeStatement) error {
	retryCount := st.Retry
	if st.Retry <= 0 {
		retryCount = 1
	}
	err := ses.UpgradeTenant(execCtx.reqCtx, st.Target.AccountName, uint32(retryCount), st.Target.IsALLAccount)
	if err != nil {
		return err
	}

	return nil
}
