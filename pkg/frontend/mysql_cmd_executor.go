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
	"fmt"
	"io"
	"sort"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/matrixorigin/matrixone/pkg/config"

	"github.com/fagongzi/goetty/v2"
	"github.com/google/uuid"
	"github.com/matrixorigin/matrixone/pkg/clusterservice"
	"github.com/matrixorigin/matrixone/pkg/common/moerr"
	"github.com/matrixorigin/matrixone/pkg/container/batch"
	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/container/vector"
	"github.com/matrixorigin/matrixone/pkg/defines"
	"github.com/matrixorigin/matrixone/pkg/logutil"
	"github.com/matrixorigin/matrixone/pkg/pb/metadata"
	"github.com/matrixorigin/matrixone/pkg/pb/plan"
	"github.com/matrixorigin/matrixone/pkg/sql/colexec"
	"github.com/matrixorigin/matrixone/pkg/sql/parsers"
	"github.com/matrixorigin/matrixone/pkg/sql/parsers/dialect"
	"github.com/matrixorigin/matrixone/pkg/sql/parsers/dialect/mysql"
	"github.com/matrixorigin/matrixone/pkg/sql/parsers/tree"
	plan2 "github.com/matrixorigin/matrixone/pkg/sql/plan"
	"github.com/matrixorigin/matrixone/pkg/sql/plan/explain"
	"github.com/matrixorigin/matrixone/pkg/util/metric"
	"github.com/matrixorigin/matrixone/pkg/util/trace"
	"github.com/matrixorigin/matrixone/pkg/util/trace/impl/motrace"
	"github.com/matrixorigin/matrixone/pkg/vm/engine"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/disttae"
	"github.com/matrixorigin/matrixone/pkg/vm/process"
	"golang.org/x/sync/errgroup"
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

func parameterModificationInTxnErrorInfo() string {
	return "Uncommitted transaction exists. Please commit or rollback first."
}

func unclassifiedStatementInUncommittedTxnErrorInfo() string {
	return "unclassified statement appears in uncommitted transaction"
}

func abortTransactionErrorInfo() string {
	return "Previous DML conflicts with existing constraints or data format. This transaction has to be aborted"
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

func GetPrepareStmtID(ctx context.Context, name string) (int, error) {
	idx := len(prefixPrepareStmtName) + 1
	if idx >= len(name) {
		return -1, moerr.NewInternalError(ctx, "can not get Prepare stmtID")
	}
	return strconv.Atoi(name[idx:])
}

type MysqlCmdExecutor struct {
	CmdExecutorImpl

	//for cmd 0x4
	TableInfoCache

	//the count of sql has been processed
	sqlCount uint64

	ses *Session

	routineMgr *RoutineManager

	cancelRequestFunc context.CancelFunc

	doQueryFunc doComQueryFunc

	mu sync.Mutex
}

func NewMysqlCmdExecutor() *MysqlCmdExecutor {
	return &MysqlCmdExecutor{}
}

func (mce *MysqlCmdExecutor) CancelRequest() {
	mce.mu.Lock()
	defer mce.mu.Unlock()
	if mce.cancelRequestFunc != nil {
		mce.cancelRequestFunc()
	}
}

func (mce *MysqlCmdExecutor) ChooseDoQueryFunc(choice bool) {
	mce.mu.Lock()
	defer mce.mu.Unlock()
	if choice {
		mce.doQueryFunc = mce.doComQueryInProgress
	} else {
		mce.doQueryFunc = mce.doComQuery
	}
}

func (mce *MysqlCmdExecutor) GetDoQueryFunc() doComQueryFunc {
	mce.mu.Lock()
	defer mce.mu.Unlock()
	if mce.doQueryFunc == nil {
		mce.doQueryFunc = mce.doComQuery
	}
	return mce.doQueryFunc
}

func (mce *MysqlCmdExecutor) SetSession(ses *Session) {
	mce.mu.Lock()
	defer mce.mu.Unlock()
	mce.ses = ses
}

func (mce *MysqlCmdExecutor) GetSession() *Session {
	mce.mu.Lock()
	defer mce.mu.Unlock()
	return mce.ses
}

// get new process id
func (mce *MysqlCmdExecutor) getNextProcessId() string {
	/*
		temporary method:
		routineId + sqlCount
	*/
	routineId := mce.GetSession().GetMysqlProtocol().ConnectionID()
	return fmt.Sprintf("%d%d", routineId, mce.GetSqlCount())
}

func (mce *MysqlCmdExecutor) GetSqlCount() uint64 {
	mce.mu.Lock()
	defer mce.mu.Unlock()
	return mce.sqlCount
}

func (mce *MysqlCmdExecutor) addSqlCount(a uint64) {
	mce.mu.Lock()
	defer mce.mu.Unlock()
	mce.sqlCount += a
}

func (mce *MysqlCmdExecutor) SetRoutineManager(mgr *RoutineManager) {
	mce.mu.Lock()
	defer mce.mu.Unlock()
	mce.routineMgr = mgr
}

func (mce *MysqlCmdExecutor) GetRoutineManager() *RoutineManager {
	mce.mu.Lock()
	defer mce.mu.Unlock()
	return mce.routineMgr
}

var RecordStatement = func(ctx context.Context, ses *Session, proc *process.Process, cw ComputationWrapper, envBegin time.Time, envStmt, sqlType string, useEnv bool) context.Context {
	// set StatementID
	var stmID uuid.UUID
	var statement tree.Statement = nil
	var text string
	if cw != nil {
		copy(stmID[:], cw.GetUUID())
		statement = cw.GetAst()
		ses.ast = statement
		text = SubStringFromBegin(envStmt, int(ses.GetParameterUnit().SV.LengthOfQueryPrinted))
	} else {
		stmID = uuid.New()
		text = SubStringFromBegin(envStmt, int(ses.GetParameterUnit().SV.LengthOfQueryPrinted))
	}
	if sqlType != internalSql {
		ses.pushQueryId(types.Uuid(stmID).ToString())
	}

	if !motrace.GetTracerProvider().IsEnable() {
		return ctx
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
		_, txn, err = handler.GetTxn()
		if err != nil {
			logErrorf(ses.GetDebugString(), "RecordStatement. error:%v", err)
			copy(stm.TransactionID[:], motrace.NilTxnID[:])
		} else {
			copy(stm.TransactionID[:], txn.Txn().ID)
		}
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
	stm.Host = ses.protocol.Peer()
	stm.Database = ses.GetDatabaseName()
	stm.Statement = text
	stm.StatementFingerprint = "" // fixme= (Reserved)
	stm.StatementTag = ""         // fixme= (Reserved)
	stm.SqlSourceType = sqlType
	stm.RequestAt = requestAt
	stm.StatementType = getStatementType(statement).GetStatementType()
	stm.QueryType = getStatementType(statement).GetQueryType()
	if sqlType != internalSql {
		ses.tStmt = stm
	}
	if !stm.IsZeroTxnID() {
		stm.Report(ctx)
	}
	sc := trace.SpanContextWithID(trace.TraceID(stmID), trace.SpanKindStatement)
	proc.WithSpanContext(sc)
	reqCtx := ses.GetRequestContext()
	ses.SetRequestContext(trace.ContextWithSpanContext(reqCtx, sc))
	return motrace.ContextWithStatement(trace.ContextWithSpanContext(ctx, sc), stm)
}

var RecordParseErrorStatement = func(ctx context.Context, ses *Session, proc *process.Process, envBegin time.Time,
	envStmt []string, sqlTypes []string, err error) context.Context {
	retErr := moerr.NewParseError(ctx, err.Error())
	/*
		!!!NOTE: the sql may be empty string.
		So, the sqlTypes may be empty slice.
	*/
	sqlType := ""
	if len(sqlTypes) > 0 {
		sqlType = sqlTypes[0]
	} else {
		sqlType = externSql
	}
	if len(envStmt) > 0 {
		for i, sql := range envStmt {
			if i < len(sqlTypes) {
				sqlType = sqlTypes[i]
			}
			ctx = RecordStatement(ctx, ses, proc, nil, envBegin, sql, sqlType, true)
			motrace.EndStatement(ctx, retErr, 0)
		}
	} else {
		ctx = RecordStatement(ctx, ses, proc, nil, envBegin, "", sqlType, true)
		motrace.EndStatement(ctx, retErr, 0)
	}

	tenant := ses.GetTenantInfo()
	if tenant == nil {
		tenant, _ = GetTenantInfo(ctx, "internal")
	}
	incStatementCounter(tenant.GetTenant(), nil)
	incStatementErrorsCounter(tenant.GetTenant(), nil)
	return ctx
}

// RecordStatementTxnID record txnID after TxnBegin or Compile(autocommit=1)
var RecordStatementTxnID = func(ctx context.Context, ses *Session) {
	var err error
	var txn TxnOperator
	if stm := motrace.StatementFromContext(ctx); ses != nil && stm != nil && stm.IsZeroTxnID() {
		if handler := ses.GetTxnHandler(); handler.IsValidTxnOperator() {
			_, txn, err = handler.GetTxn()
			if err != nil {
				logErrorf(ses.GetDebugString(), "RecordStatementTxnID. error:%v", err)
			} else {
				stm.SetTxnID(txn.Txn().ID)
			}

		}
		stm.Report(ctx)
	}
}

func handleShowTableStatus(ses *Session, stmt *tree.ShowTableStatus, proc *process.Process) error {
	db, err := ses.GetStorage().Database(ses.requestCtx, stmt.DbName, proc.TxnOperator)
	if err != nil {
		return err
	}
	mrs := ses.GetMysqlResultSet()
	for _, row := range ses.data {
		tableName := string(row[0].([]byte))
		r, err := db.Relation(ses.requestCtx, tableName, nil)
		if err != nil {
			return err
		}
		_, err = r.Ranges(ses.requestCtx, nil)
		if err != nil {
			return err
		}
		row[3], err = r.Rows(ses.requestCtx)
		if err != nil {
			return err
		}
		mrs.AddRow(row)
	}
	if err := ses.GetMysqlProtocol().SendResultSetTextBatchRowSpeedup(mrs, mrs.GetRowCount()); err != nil {
		logErrorf(ses.GetDebugString(), "handleShowTableStatus error %v", err)
		return err
	}
	return nil
}

/*
extract the data from the pipeline.
obj: session
Warning: The pipeline is the multi-thread environment. The getDataFromPipeline will
access the shared data. Be careful when it writes the shared data.
*/
func getDataFromPipeline(obj interface{}, bat *batch.Batch) error {
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
	proto.ResetStatistics()

	oq := NewOutputQueue(ses.GetRequestContext(), ses, len(bat.Vecs), nil, nil)
	row2colTime := time.Duration(0)
	procBatchBegin := time.Now()
	n := bat.Vecs[0].Length()
	requestCtx := ses.GetRequestContext()

	if oq.ep.Outfile {
		initExportFirst(oq)
	}

	for j := 0; j < n; j++ { //row index
		if oq.ep.Outfile {
			select {
			case <-requestCtx.Done():
				return nil
			default:
			}
			continue
		}

		if bat.Zs[j] <= 0 {
			continue
		}
		row, err := extractRowFromEveryVector(ses, bat, j, oq, false)
		if err != nil {
			return err
		}
		if oq.showStmtType == ShowTableStatus {
			row2 := make([]interface{}, len(row))
			copy(row2, row)
			ses.AppendData(row2)
		}
	}

	if oq.ep.Outfile {
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

func doUse(ctx context.Context, ses *Session, db string) error {
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
	if dbMeta, err = ses.GetParameterUnit().StorageEngine.Database(txnCtx, db, txn); err != nil {
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

func (mce *MysqlCmdExecutor) handleChangeDB(requestCtx context.Context, db string) error {
	return doUse(requestCtx, mce.GetSession(), db)
}

func (mce *MysqlCmdExecutor) handleDump(requestCtx context.Context, dump *tree.MoDump) error {
	return doDumpQueryResult(requestCtx, mce.GetSession(), dump.ExportParams)
}

/*
handle "SELECT @@xxx.yyyy"
*/
func (mce *MysqlCmdExecutor) handleSelectVariables(ve *tree.VarExpr, cwIndex, cwsLen int) error {
	var err error = nil
	ses := mce.GetSession()
	mrs := ses.GetMysqlResultSet()
	proto := ses.GetMysqlProtocol()

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
		row[0] = val
	}

	mrs.AddRow(row)

	mer := NewMysqlExecutionResult(0, 0, 0, 0, mrs)
	resp := mce.ses.SetNewResponse(ResultResponse, 0, int(COM_QUERY), mer, cwIndex, cwsLen)

	if err := proto.SendResponse(ses.GetRequestContext(), resp); err != nil {
		return moerr.NewInternalError(ses.GetRequestContext(), "routine send response failed.")
	}
	return err
}

func doCmdFieldList(requestCtx context.Context, ses *Session, icfl *InternalCmdFieldList) error {
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
	//if mce.tableInfos == nil || mce.db != dbName {
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
	//	if mce.tableInfos == nil {
	//		mce.tableInfos = make(map[string][]ColumnInfo)
	//	}
	//	mce.tableInfos[tableName] = attrs
	//}
	//
	//cols, ok := mce.tableInfos[tableName]
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
func (mce *MysqlCmdExecutor) handleCmdFieldList(requestCtx context.Context, icfl *InternalCmdFieldList) error {
	var err error
	ses := mce.GetSession()
	proto := ses.GetMysqlProtocol()

	err = doCmdFieldList(requestCtx, ses, icfl)
	if err != nil {
		return err
	}

	/*
		mysql CMD_FIELD_LIST response: End after the column has been sent.
		send EOF packet
	*/
	err = proto.sendEOFOrOkPacket(0, ses.GetServerStatus())
	if err != nil {
		return err
	}

	return err
}

func doSetVar(ctx context.Context, mce *MysqlCmdExecutor, ses *Session, sv *tree.SetVar) error {
	var err error = nil
	var ok bool
	setVarFunc := func(system, global bool, name string, value interface{}) error {
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
				err = ses.SetSessionVar(name, value)
				if err != nil {
					return err
				}
			}

			if strings.ToLower(name) == "autocommit" {
				svbt := SystemVariableBoolType{}
				newValue, err2 := svbt.Convert(value)
				if err2 != nil {
					return err2
				}
				err = ses.SetAutocommit(svbt.IsTrue(newValue))
				if err != nil {
					return err
				}
			}
		} else {
			err = ses.SetUserDefinedVar(name, value)
			if err != nil {
				return err
			}
		}
		return nil
	}
	for _, assign := range sv.Assignments {
		name := assign.Name
		var value interface{}

		value, err = getExprValue(assign.Value, mce, ses)
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
				err = setVarFunc(assign.System, assign.Global, rb, value)
				if err != nil {
					return err
				}
			}
		} else if name == "syspublications" {
			if !ses.GetTenantInfo().IsSysTenant() {
				return moerr.NewInternalError(ses.GetRequestContext(), "only system account can set system variable syspublications")
			}
			err = setVarFunc(assign.System, assign.Global, name, value)
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
				err = setVarFunc(assign.System, assign.Global, name, value)
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
			err = setVarFunc(assign.System, assign.Global, name, value)
			if err != nil {
				return err
			}
		} else {
			err = setVarFunc(assign.System, assign.Global, name, value)
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
func (mce *MysqlCmdExecutor) handleSetVar(ctx context.Context, sv *tree.SetVar) error {
	ses := mce.GetSession()
	err := doSetVar(ctx, mce, ses, sv)
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

func (mce *MysqlCmdExecutor) handleShowErrors(cwIndex, cwsLen int) error {
	var err error
	ses := mce.GetSession()
	proto := ses.GetMysqlProtocol()
	err = doShowErrors(ses)
	if err != nil {
		return err
	}

	mer := NewMysqlExecutionResult(0, 0, 0, 0, ses.GetMysqlResultSet())
	resp := mce.ses.SetNewResponse(ResultResponse, 0, int(COM_QUERY), mer, cwIndex, cwsLen)

	if err := proto.SendResponse(ses.requestCtx, resp); err != nil {
		return moerr.NewInternalError(ses.requestCtx, "routine send response failed. error:%v ", err)
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
		if _, ok := gsv.GetType().(SystemVariableBoolType); ok {
			v, ok := value.(int8)
			if ok {
				if v == 1 {
					row[1] = "on"
				} else {
					row[1] = "off"
				}
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

		bat.Shrink(sels)
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
func (mce *MysqlCmdExecutor) handleShowVariables(sv *tree.ShowVariables, proc *process.Process, cwIndex, cwsLen int) error {
	ses := mce.GetSession()
	proto := ses.GetMysqlProtocol()
	err := doShowVariables(ses, proc, sv)
	if err != nil {
		return err
	}
	mer := NewMysqlExecutionResult(0, 0, 0, 0, ses.GetMysqlResultSet())
	resp := mce.ses.SetNewResponse(ResultResponse, 0, int(COM_QUERY), mer, cwIndex, cwsLen)

	if err := proto.SendResponse(ses.requestCtx, resp); err != nil {
		return moerr.NewInternalError(ses.requestCtx, "routine send response failed. error:%v ", err)
	}
	return err
}

func constructVarBatch(ses *Session, rows [][]interface{}) (*batch.Batch, error) {
	bat := batch.New(true, []string{"Variable_name", "Value"})
	typ := types.New(types.T_varchar, types.MaxVarcharLen, 0)
	cnt := len(rows)
	bat.Zs = make([]int64, cnt)
	for i := range bat.Zs {
		bat.Zs[i] = 1
	}
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

func (mce *MysqlCmdExecutor) handleAnalyzeStmt(requestCtx context.Context, stmt *tree.AnalyzeStmt) error {
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
	return mce.GetDoQueryFunc()(requestCtx, &UserInput{sql: sql})
}

// Note: for pass the compile quickly. We will remove the comments in the future.
func (mce *MysqlCmdExecutor) handleExplainStmt(requestCtx context.Context, stmt *tree.ExplainStmt) error {
	es, err := getExplainOption(requestCtx, stmt.Options)
	if err != nil {
		return err
	}

	ses := mce.GetSession()

	//get query optimizer and execute Optimize
	plan, err := buildPlan(requestCtx, ses, ses.GetTxnCompileCtx(), stmt.Statement)
	if err != nil {
		return err
	}
	if plan.GetQuery() == nil {
		return moerr.NewNotSupported(requestCtx, "the sql query plan does not support explain.")
	}
	// generator query explain
	explainQuery := explain.NewExplainQueryImpl(plan.GetQuery())

	// build explain data buffer
	buffer := explain.NewExplainDataBuffer()
	err = explainQuery.ExplainPlan(requestCtx, buffer, es)
	if err != nil {
		return err
	}

	protocol := ses.GetMysqlProtocol()

	explainColName := "QUERY PLAN"
	columns, err := GetExplainColumns(requestCtx, explainColName)
	if err != nil {
		return err
	}

	//	Step 1 : send column count and column definition.
	//send column count
	colCnt := uint64(len(columns))
	err = protocol.SendColumnCountPacket(colCnt)
	if err != nil {
		return err
	}
	//send columns
	//column_count * Protocol::ColumnDefinition packets
	cmd := ses.GetCmd()
	mrs := ses.GetMysqlResultSet()
	for _, c := range columns {
		mysqlc := c.(Column)
		mrs.AddColumn(mysqlc)
		//	mysql COM_QUERY response: send the column definition per column
		err := protocol.SendColumnDefinitionPacket(requestCtx, mysqlc, int(cmd))
		if err != nil {
			return err
		}
	}

	//	mysql COM_QUERY response: End after the column has been sent.
	//	send EOF packet
	err = protocol.SendEOFPacketIf(0, ses.GetServerStatus())
	if err != nil {
		return err
	}

	err = buildMoExplainQuery(explainColName, buffer, ses, getDataFromPipeline)
	if err != nil {
		return err
	}

	err = protocol.sendEOFOrOkPacket(0, ses.GetServerStatus())
	if err != nil {
		return err
	}
	return nil
}

func doPrepareStmt(ctx context.Context, ses *Session, st *tree.PrepareStmt) (*PrepareStmt, error) {
	preparePlan, err := buildPlan(ctx, ses, ses.GetTxnCompileCtx(), st)
	if err != nil {
		return nil, err
	}

	prepareStmt := &PrepareStmt{
		Name:                preparePlan.GetDcl().GetPrepare().GetName(),
		PreparePlan:         preparePlan,
		PrepareStmt:         st.Stmt,
		getFromSendLongData: make(map[int]struct{}),
	}
	prepareStmt.InsertBat = ses.GetTxnCompileCtx().GetProcess().GetPrepareBatch()
	err = ses.SetPrepareStmt(preparePlan.GetDcl().GetPrepare().GetName(), prepareStmt)

	return prepareStmt, err
}

// handlePrepareStmt
func (mce *MysqlCmdExecutor) handlePrepareStmt(ctx context.Context, st *tree.PrepareStmt) (*PrepareStmt, error) {
	return doPrepareStmt(ctx, mce.GetSession(), st)
}

func doPrepareString(ctx context.Context, ses *Session, st *tree.PrepareString) (*PrepareStmt, error) {
	v, err := ses.GetGlobalVar("lower_case_table_names")
	if err != nil {
		return nil, err
	}
	stmts, err := mysql.Parse(ctx, st.Sql, v.(int64))
	if err != nil {
		return nil, err
	}

	preparePlan, err := buildPlan(ses.GetRequestContext(), ses, ses.GetTxnCompileCtx(), st)
	if err != nil {
		return nil, err
	}
	prepareStmt := &PrepareStmt{
		Name:        preparePlan.GetDcl().GetPrepare().GetName(),
		PreparePlan: preparePlan,
		PrepareStmt: stmts[0],
	}
	prepareStmt.InsertBat = ses.GetTxnCompileCtx().GetProcess().GetPrepareBatch()
	err = ses.SetPrepareStmt(preparePlan.GetDcl().GetPrepare().GetName(), prepareStmt)
	return prepareStmt, err
}

// handlePrepareString
func (mce *MysqlCmdExecutor) handlePrepareString(ctx context.Context, st *tree.PrepareString) (*PrepareStmt, error) {
	return doPrepareString(ctx, mce.GetSession(), st)
}

func doDeallocate(ctx context.Context, ses *Session, st *tree.Deallocate) error {
	deallocatePlan, err := buildPlan(ctx, ses, ses.GetTxnCompileCtx(), st)
	if err != nil {
		return err
	}
	ses.RemovePrepareStmt(deallocatePlan.GetDcl().GetDeallocate().GetName())
	return nil
}

func doReset(ctx context.Context, ses *Session, st *tree.Reset) error {
	return nil
}

// handleDeallocate
func (mce *MysqlCmdExecutor) handleDeallocate(ctx context.Context, st *tree.Deallocate) error {
	return doDeallocate(ctx, mce.GetSession(), st)
}

// handleReset
func (mce *MysqlCmdExecutor) handleReset(ctx context.Context, st *tree.Reset) error {
	return doReset(ctx, mce.GetSession(), st)
}

func (mce *MysqlCmdExecutor) handleCreatePublication(ctx context.Context, cp *tree.CreatePublication) error {
	return doCreatePublication(ctx, mce.GetSession(), cp)
}

func (mce *MysqlCmdExecutor) handleAlterPublication(ctx context.Context, ap *tree.AlterPublication) error {
	return doAlterPublication(ctx, mce.GetSession(), ap)
}

func (mce *MysqlCmdExecutor) handleDropPublication(ctx context.Context, dp *tree.DropPublication) error {
	return doDropPublication(ctx, mce.GetSession(), dp)
}

// handleCreateAccount creates a new user-level tenant in the context of the tenant SYS
// which has been initialized.
func (mce *MysqlCmdExecutor) handleCreateAccount(ctx context.Context, ca *tree.CreateAccount) error {
	//step1 : create new account.
	return InitGeneralTenant(ctx, mce.GetSession(), ca)
}

// handleDropAccount drops a new user-level tenant
func (mce *MysqlCmdExecutor) handleDropAccount(ctx context.Context, da *tree.DropAccount) error {
	return doDropAccount(ctx, mce.GetSession(), da)
}

// handleDropAccount drops a new user-level tenant
func (mce *MysqlCmdExecutor) handleAlterAccount(ctx context.Context, aa *tree.AlterAccount) error {
	return doAlterAccount(ctx, mce.GetSession(), aa)
}

// handleAlterDatabaseConfig alter a database's mysql_compatibility_mode
func (mce *MysqlCmdExecutor) handleAlterDataBaseConfig(ctx context.Context, ses *Session, ad *tree.AlterDataBaseConfig) error {
	err := doCheckRole(ctx, ses)
	if err != nil {
		return err
	}
	return doAlterDatabaseConfig(ctx, mce.GetSession(), ad)
}

// handleAlterAccountConfig alter a account's mysql_compatibility_mode
func (mce *MysqlCmdExecutor) handleAlterAccountConfig(ctx context.Context, ses *Session, st *tree.AlterDataBaseConfig) error {
	err := doCheckRole(ctx, ses)
	if err != nil {
		return err
	}
	return doAlterAccountConfig(ctx, mce.GetSession(), st)
}

// handleCreateUser creates the user for the tenant
func (mce *MysqlCmdExecutor) handleCreateUser(ctx context.Context, cu *tree.CreateUser) error {
	ses := mce.GetSession()
	tenant := ses.GetTenantInfo()

	//step1 : create the user
	return InitUser(ctx, ses, tenant, cu)
}

// handleDropUser drops the user for the tenant
func (mce *MysqlCmdExecutor) handleDropUser(ctx context.Context, du *tree.DropUser) error {
	return doDropUser(ctx, mce.GetSession(), du)
}

func (mce *MysqlCmdExecutor) handleAlterUser(ctx context.Context, au *tree.AlterUser) error {
	return doAlterUser(ctx, mce.GetSession(), au)
}

// handleCreateRole creates the new role
func (mce *MysqlCmdExecutor) handleCreateRole(ctx context.Context, cr *tree.CreateRole) error {
	ses := mce.GetSession()
	tenant := ses.GetTenantInfo()

	//step1 : create the role
	return InitRole(ctx, ses, tenant, cr)
}

// handleDropRole drops the role
func (mce *MysqlCmdExecutor) handleDropRole(ctx context.Context, dr *tree.DropRole) error {
	return doDropRole(ctx, mce.GetSession(), dr)
}

func (mce *MysqlCmdExecutor) handleCreateFunction(ctx context.Context, cf *tree.CreateFunction) error {
	ses := mce.GetSession()
	tenant := ses.GetTenantInfo()

	return InitFunction(ctx, ses, tenant, cf)
}

func (mce *MysqlCmdExecutor) handleDropFunction(ctx context.Context, df *tree.DropFunction) error {
	return doDropFunction(ctx, mce.GetSession(), df)
}

func (mce *MysqlCmdExecutor) handleCreateProcedure(ctx context.Context, cp *tree.CreateProcedure) error {
	ses := mce.GetSession()
	tenant := ses.GetTenantInfo()

	return InitProcedure(ctx, ses, tenant, cp)
}

func (mce *MysqlCmdExecutor) handleDropProcedure(ctx context.Context, dp *tree.DropProcedure) error {
	return doDropProcedure(ctx, mce.GetSession(), dp)
}

func (mce *MysqlCmdExecutor) handleCallProcedure(ctx context.Context, call *tree.CallStmt, proc *process.Process, cwIndex, cwsLen int) error {
	ses := mce.GetSession()
	proto := ses.GetMysqlProtocol()
	results, err := doInterpretCall(ctx, mce.GetSession(), call)
	if err != nil {
		return err
	}

	resp := NewGeneralOkResponse(COM_QUERY, mce.ses.GetServerStatus())

	if len(results) == 0 {
		if err := proto.SendResponse(ses.requestCtx, resp); err != nil {
			return moerr.NewInternalError(ses.requestCtx, "routine send response failed. error:%v ", err)
		}
	} else {
		for i, result := range results {
			mer := NewMysqlExecutionResult(0, 0, 0, 0, result.(*MysqlResultSet))
			resp = mce.ses.SetNewResponse(ResultResponse, 0, int(COM_QUERY), mer, i, len(results))
			if err := proto.SendResponse(ses.requestCtx, resp); err != nil {
				return moerr.NewInternalError(ses.requestCtx, "routine send response failed. error:%v ", err)
			}
		}
	}
	return nil
}

// handleGrantRole grants the role
func (mce *MysqlCmdExecutor) handleGrantRole(ctx context.Context, gr *tree.GrantRole) error {
	return doGrantRole(ctx, mce.GetSession(), gr)
}

// handleRevokeRole revokes the role
func (mce *MysqlCmdExecutor) handleRevokeRole(ctx context.Context, rr *tree.RevokeRole) error {
	return doRevokeRole(ctx, mce.GetSession(), rr)
}

// handleGrantRole grants the privilege to the role
func (mce *MysqlCmdExecutor) handleGrantPrivilege(ctx context.Context, gp *tree.GrantPrivilege) error {
	return doGrantPrivilege(ctx, mce.GetSession(), gp)
}

// handleRevokePrivilege revokes the privilege from the user or role
func (mce *MysqlCmdExecutor) handleRevokePrivilege(ctx context.Context, rp *tree.RevokePrivilege) error {
	return doRevokePrivilege(ctx, mce.GetSession(), rp)
}

// handleSwitchRole switches the role to another role
func (mce *MysqlCmdExecutor) handleSwitchRole(ctx context.Context, sr *tree.SetRole) error {
	return doSwitchRole(ctx, mce.GetSession(), sr)
}

func doKill(ctx context.Context, rm *RoutineManager, ses *Session, k *tree.Kill) error {
	var err error
	//true: kill a connection
	//false: kill a query in a connection
	idThatKill := uint64(ses.GetConnectionID())
	if !k.Option.Exist || k.Option.Typ == tree.KillTypeConnection {
		err = rm.kill(ctx, true, idThatKill, k.ConnectionId, "")
	} else {
		err = rm.kill(ctx, false, idThatKill, k.ConnectionId, k.StmtOption.StatementId)
	}
	return err
}

// handleKill kill a connection or query
func (mce *MysqlCmdExecutor) handleKill(ctx context.Context, k *tree.Kill) error {
	var err error
	ses := mce.GetSession()
	proto := ses.GetMysqlProtocol()
	err = doKill(ctx, mce.GetRoutineManager(), ses, k)
	if err != nil {
		return err
	}
	resp := NewGeneralOkResponse(COM_QUERY, mce.ses.GetServerStatus())
	if err = proto.SendResponse(ctx, resp); err != nil {
		return moerr.NewInternalError(ctx, "routine send response failed. error:%v ", err)
	}
	return err
}

// handleShowAccounts lists the info of accounts
func (mce *MysqlCmdExecutor) handleShowAccounts(ctx context.Context, sa *tree.ShowAccounts, cwIndex, cwsLen int) error {
	var err error
	ses := mce.GetSession()
	proto := ses.GetMysqlProtocol()
	err = doShowAccounts(ctx, ses, sa)
	if err != nil {
		return err
	}
	mer := NewMysqlExecutionResult(0, 0, 0, 0, ses.GetMysqlResultSet())
	resp := mce.ses.SetNewResponse(ResultResponse, 0, int(COM_QUERY), mer, cwIndex, cwsLen)

	if err = proto.SendResponse(ctx, resp); err != nil {
		return moerr.NewInternalError(ctx, "routine send response failed. error:%v ", err)
	}
	return err
}

func (mce *MysqlCmdExecutor) handleShowSubscriptions(ctx context.Context, ss *tree.ShowSubscriptions, cwIndex, cwsLen int) error {
	var err error
	ses := mce.GetSession()
	proto := ses.GetMysqlProtocol()
	err = doShowSubscriptions(ctx, ses, ss)
	if err != nil {
		return err
	}
	mer := NewMysqlExecutionResult(0, 0, 0, 0, ses.GetMysqlResultSet())
	resp := mce.ses.SetNewResponse(ResultResponse, 0, int(COM_QUERY), mer, cwIndex, cwsLen)

	if err = proto.SendResponse(ctx, resp); err != nil {
		return moerr.NewInternalError(ctx, "routine send response failed. error:%v ", err)
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
	col3.SetName("Labels")

	mrs := ses.GetMysqlResultSet()
	mrs.AddColumn(col1)
	mrs.AddColumn(col2)
	mrs.AddColumn(col3)

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
		row := make([]interface{}, 3)
		row[0] = s.ServiceID
		row[1] = s.SQLAddress
		var labelStr string
		for key, value := range s.Labels {
			labelStr += fmt.Sprintf("%s:%s;", key, strings.Join(value.Labels, ","))
		}
		row[2] = labelStr
		mrs.AddRow(row)
	}

	tenant := ses.GetTenantInfo().GetTenant()
	var se clusterservice.Selector
	labels := ses.GetMysqlProtocol().GetConnectAttrs()
	labels["account"] = tenant
	se = clusterservice.NewSelector().SelectByLabel(
		filterLabels(labels), clusterservice.EQ)
	if isSysTenant(tenant) {
		u := ses.GetUserName()
		// For super use dump and root, we should list all servers.
		if isSuperUser(u) {
			clusterservice.GetMOCluster().GetCNService(
				clusterservice.NewSelector(), func(s metadata.CNService) bool {
					appendFn(&s)
					return true
				})
		} else {
			disttae.SelectForSuperTenant(se, u, nil, appendFn)
		}
	} else {
		disttae.SelectForCommonTenant(se, nil, appendFn)
	}
	return nil
}

func (mce *MysqlCmdExecutor) handleShowBackendServers(ctx context.Context, cwIndex, cwsLen int) error {
	var err error
	ses := mce.GetSession()
	proto := ses.GetMysqlProtocol()
	if err := doShowBackendServers(ses); err != nil {
		return err
	}

	mer := NewMysqlExecutionResult(0, 0, 0, 0, ses.GetMysqlResultSet())
	resp := mce.ses.SetNewResponse(ResultResponse, 0, int(COM_QUERY), mer, cwIndex, cwsLen)
	if err := proto.SendResponse(ses.requestCtx, resp); err != nil {
		return moerr.NewInternalError(ses.requestCtx, "routine send response failed, error: %v ", err)
	}
	return err
}

func GetExplainColumns(ctx context.Context, explainColName string) ([]interface{}, error) {
	cols := []*plan2.ColDef{
		{Typ: &plan2.Type{Id: int32(types.T_varchar)}, Name: explainColName},
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
	bat.InitZsOne(count)

	err := fill(session, bat)
	if err != nil {
		return err
	}
	// to trigger save result meta
	err = fill(session, nil)
	return err
}

func buildPlan(requestCtx context.Context, ses *Session, ctx plan2.CompilerContext, stmt tree.Statement) (*plan2.Plan, error) {
	var ret *plan2.Plan
	var err error
	if ses != nil {
		ses.accountId = defines.GetAccountId(requestCtx)
	}
	if s, ok := stmt.(*tree.Insert); ok {
		if _, ok := s.Rows.Select.(*tree.ValuesClause); ok {
			ret, err = plan2.BuildPlan(ctx, stmt, false)
			if err != nil {
				return nil, err
			}
		}
	}
	if ret != nil {
		if ses != nil && ses.GetTenantInfo() != nil {
			err = authenticateCanExecuteStatementAndPlan(requestCtx, ses, stmt, ret)
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
		optimized, err := opt.Optimize(stmt, false)
		if err != nil {
			return nil, err
		}
		ret = &plan2.Plan{
			Plan: &plan2.Plan_Query{
				Query: optimized,
			},
		}
	default:
		ret, err = plan2.BuildPlan(ctx, stmt, false)
	}
	if ret != nil {
		if ses != nil && ses.GetTenantInfo() != nil {
			err = authenticateCanExecuteStatementAndPlan(requestCtx, ses, stmt, ret)
			if err != nil {
				return nil, err
			}
		}
	}
	return ret, err
}

func checkColModify(plan2 *plan.Plan, proc *process.Process, ses *Session) bool {
	switch p := plan2.Plan.(type) {
	case *plan.Plan_Query:
		for i := range p.Query.Nodes {
			if p.Query.Nodes[i].NodeType == plan.Node_TABLE_SCAN {
				// check table_scan node' s tableDef whether be modified
				tblName := p.Query.Nodes[i].TableDef.Name
				dbName := p.Query.Nodes[i].ObjRef.SchemaName
				_, tableDef := ses.GetTxnCompileCtx().Resolve(dbName, tblName)
				if tableDef == nil {
					return true
				}
				colCnt1, colCnt2 := 0, 0
				for j := 0; j < len(p.Query.Nodes[i].TableDef.Cols); j++ {
					if p.Query.Nodes[i].TableDef.Cols[j].Hidden {
						continue
					}
					colCnt1++
				}
				for j := 0; j < len(tableDef.Cols); j++ {
					if tableDef.Cols[j].Hidden {
						continue
					}
					colCnt2++
				}
				if colCnt1 != colCnt2 {
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
			if checkColModify(tcw.plan, proc, ses) {
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
		v, err = ses.GetGlobalVar("lower_case_table_names")
		if err != nil {
			v = int64(1)
		}
		stmts, err = parsers.Parse(proc.Ctx, dialect.MYSQL, input.getSql(), v.(int64))
		if err != nil {
			return nil, err
		}
	}

	for _, stmt := range stmts {
		cw = append(cw, InitTxnComputationWrapper(ses, stmt, proc))
	}
	return cw, nil
}

func getStmtExecutor(ses *Session, proc *process.Process, base *baseStmtExecutor, stmt tree.Statement) (StmtExecutor, error) {
	var err error
	var ret StmtExecutor
	switch st := stmt.(type) {
	//PART 1: the statements with the result set
	case *tree.Select:
		ret = &SelectExecutor{
			resultSetStmtExecutor: &resultSetStmtExecutor{
				base,
			},
			sel: st,
		}
	case *tree.ValuesStatement:
		ret = &ValuesStmtExecutor{
			resultSetStmtExecutor: &resultSetStmtExecutor{
				base,
			},
			sel: st,
		}
	case *tree.ShowCreateTable:
		ret = &ShowCreateTableExecutor{
			resultSetStmtExecutor: &resultSetStmtExecutor{
				base,
			},
			sct: st,
		}
	case *tree.ShowCreateDatabase:
		ret = &ShowCreateDatabaseExecutor{
			resultSetStmtExecutor: &resultSetStmtExecutor{
				base,
			},
			scd: st,
		}
	case *tree.ShowTables:
		ret = &ShowTablesExecutor{
			resultSetStmtExecutor: &resultSetStmtExecutor{
				base,
			},
			st: st,
		}
	case *tree.ShowSequences:
		ret = &ShowSequencesExecutor{
			resultSetStmtExecutor: &resultSetStmtExecutor{
				base,
			},
			ss: st,
		}
	case *tree.ShowDatabases:
		ret = &ShowDatabasesExecutor{
			resultSetStmtExecutor: &resultSetStmtExecutor{
				base,
			},
			sd: st,
		}
	case *tree.ShowColumns:
		ret = &ShowColumnsExecutor{
			resultSetStmtExecutor: &resultSetStmtExecutor{
				base,
			},
			sc: st,
		}
	case *tree.ShowProcessList:
		ret = &ShowProcessListExecutor{
			resultSetStmtExecutor: &resultSetStmtExecutor{
				base,
			},
			spl: st,
		}
	case *tree.ShowStatus:
		ret = &ShowStatusExecutor{
			resultSetStmtExecutor: &resultSetStmtExecutor{
				base,
			},
			ss: st,
		}
	case *tree.ShowTableStatus:
		ret = &ShowTableStatusExecutor{
			resultSetStmtExecutor: &resultSetStmtExecutor{
				base,
			},
			sts: st,
		}
	case *tree.ShowGrants:
		ret = &ShowGrantsExecutor{
			resultSetStmtExecutor: &resultSetStmtExecutor{
				base,
			},
			sg: st,
		}
	case *tree.ShowIndex:
		ret = &ShowIndexExecutor{
			resultSetStmtExecutor: &resultSetStmtExecutor{
				base,
			},
			si: st,
		}
	case *tree.ShowCreateView:
		ret = &ShowCreateViewExecutor{
			resultSetStmtExecutor: &resultSetStmtExecutor{
				base,
			},
			scv: st,
		}
	case *tree.ShowTarget:
		ret = &ShowTargetExecutor{
			resultSetStmtExecutor: &resultSetStmtExecutor{
				base,
			},
			st: st,
		}
	case *tree.ExplainFor:
		ret = &ExplainForExecutor{
			resultSetStmtExecutor: &resultSetStmtExecutor{
				base,
			},
			ef: st,
		}
	case *tree.ExplainStmt:
		base.ComputationWrapper = InitNullComputationWrapper(ses, st, proc)
		ret = &ExplainStmtExecutor{
			resultSetStmtExecutor: &resultSetStmtExecutor{
				base,
			},
			es: st,
		}
	case *tree.ShowVariables:
		base.ComputationWrapper = InitNullComputationWrapper(ses, st, proc)
		ret = &ShowVariablesExecutor{
			resultSetStmtExecutor: &resultSetStmtExecutor{
				base,
			},
			sv: st,
		}
	case *tree.ShowErrors:
		base.ComputationWrapper = InitNullComputationWrapper(ses, st, proc)
		ret = &ShowErrorsExecutor{
			resultSetStmtExecutor: &resultSetStmtExecutor{
				base,
			},
			se: st,
		}
	case *tree.ShowWarnings:
		base.ComputationWrapper = InitNullComputationWrapper(ses, st, proc)
		ret = &ShowWarningsExecutor{
			resultSetStmtExecutor: &resultSetStmtExecutor{
				base,
			},
			sw: st,
		}
	case *tree.AnalyzeStmt:
		base.ComputationWrapper = InitNullComputationWrapper(ses, st, proc)
		ret = &AnalyzeStmtExecutor{
			resultSetStmtExecutor: &resultSetStmtExecutor{
				base,
			},
			as: st,
		}
	case *tree.ExplainAnalyze:
		ret = &ExplainAnalyzeExecutor{
			resultSetStmtExecutor: &resultSetStmtExecutor{
				base,
			},
			ea: st,
		}
	case *InternalCmdFieldList:
		base.ComputationWrapper = InitNullComputationWrapper(ses, st, proc)
		ret = &InternalCmdFieldListExecutor{
			resultSetStmtExecutor: &resultSetStmtExecutor{
				base,
			},
			icfl: st,
		}
	//PART 2: the statement with the status only
	case *tree.BeginTransaction:
		base.ComputationWrapper = InitNullComputationWrapper(ses, st, proc)
		ret = &BeginTxnExecutor{
			statusStmtExecutor: &statusStmtExecutor{
				base,
			},
			bt: st,
		}
	case *tree.CommitTransaction:
		base.ComputationWrapper = InitNullComputationWrapper(ses, st, proc)
		ret = &CommitTxnExecutor{
			statusStmtExecutor: &statusStmtExecutor{
				base,
			},
			ct: st,
		}
	case *tree.RollbackTransaction:
		base.ComputationWrapper = InitNullComputationWrapper(ses, st, proc)
		ret = &RollbackTxnExecutor{
			statusStmtExecutor: &statusStmtExecutor{
				base,
			},
			rt: st,
		}
	case *tree.SetRole:
		base.ComputationWrapper = InitNullComputationWrapper(ses, st, proc)
		ret = &SetRoleExecutor{
			statusStmtExecutor: &statusStmtExecutor{
				base,
			},
			sr: st,
		}
	case *tree.Use:
		base.ComputationWrapper = InitNullComputationWrapper(ses, st, proc)
		ret = &UseExecutor{
			statusStmtExecutor: &statusStmtExecutor{
				base,
			},
			u: st,
		}
	case *tree.MoDump:
		//TODO:
		err = moerr.NewInternalError(proc.Ctx, "needs to add modump")
	case *tree.DropDatabase:
		ret = &DropDatabaseExecutor{
			statusStmtExecutor: &statusStmtExecutor{
				base,
			},
			dd: st,
		}
	case *tree.PrepareStmt:
		base.ComputationWrapper = InitNullComputationWrapper(ses, st, proc)
		ret = &PrepareStmtExecutor{
			statusStmtExecutor: &statusStmtExecutor{
				base,
			},
			ps: st,
		}
	case *tree.PrepareString:
		base.ComputationWrapper = InitNullComputationWrapper(ses, st, proc)
		ret = &PrepareStringExecutor{
			statusStmtExecutor: &statusStmtExecutor{
				base,
			},
			ps: st,
		}
	case *tree.Deallocate:
		base.ComputationWrapper = InitNullComputationWrapper(ses, st, proc)
		ret = &DeallocateExecutor{
			statusStmtExecutor: &statusStmtExecutor{
				base,
			},
			d: st,
		}
	case *tree.SetVar:
		base.ComputationWrapper = InitNullComputationWrapper(ses, st, proc)
		ret = &SetVarExecutor{
			statusStmtExecutor: &statusStmtExecutor{
				base,
			},
			sv: st,
		}
	case *tree.Delete:
		ret = &DeleteExecutor{
			statusStmtExecutor: &statusStmtExecutor{
				base,
			},
			d: st,
		}
	case *tree.Update:
		ret = &UpdateExecutor{
			statusStmtExecutor: &statusStmtExecutor{
				base,
			},
			u: st,
		}
	case *tree.CreatePublication:
		base.ComputationWrapper = InitNullComputationWrapper(ses, st, proc)
		ret = &CreatePublicationExecutor{
			statusStmtExecutor: &statusStmtExecutor{
				base,
			},
			cp: st,
		}
	case *tree.AlterPublication:
		base.ComputationWrapper = InitNullComputationWrapper(ses, st, proc)
		ret = &AlterPublicationExecutor{
			statusStmtExecutor: &statusStmtExecutor{
				base,
			},
			ap: st,
		}
	case *tree.DropPublication:
		base.ComputationWrapper = InitNullComputationWrapper(ses, st, proc)
		ret = &DropPublicationExecutor{
			statusStmtExecutor: &statusStmtExecutor{
				base,
			},
			dp: st,
		}
	case *tree.CreateAccount:
		base.ComputationWrapper = InitNullComputationWrapper(ses, st, proc)
		ret = &CreateAccountExecutor{
			statusStmtExecutor: &statusStmtExecutor{
				base,
			},
			ca: st,
		}
	case *tree.DropAccount:
		base.ComputationWrapper = InitNullComputationWrapper(ses, st, proc)
		ret = &DropAccountExecutor{
			statusStmtExecutor: &statusStmtExecutor{
				base,
			},
			da: st,
		}
	case *tree.AlterAccount:
		ret = &AlterAccountExecutor{
			statusStmtExecutor: &statusStmtExecutor{
				base,
			},
			aa: st,
		}
	case *tree.CreateUser:
		base.ComputationWrapper = InitNullComputationWrapper(ses, st, proc)
		ret = &CreateUserExecutor{
			statusStmtExecutor: &statusStmtExecutor{
				base,
			},
			cu: st,
		}
	case *tree.DropUser:
		base.ComputationWrapper = InitNullComputationWrapper(ses, st, proc)
		ret = &DropUserExecutor{
			statusStmtExecutor: &statusStmtExecutor{
				base,
			},
			du: st,
		}
	case *tree.AlterUser:
		ret = &AlterUserExecutor{
			statusStmtExecutor: &statusStmtExecutor{
				base,
			},
			au: st,
		}
	case *tree.CreateRole:
		base.ComputationWrapper = InitNullComputationWrapper(ses, st, proc)
		ret = &CreateRoleExecutor{
			statusStmtExecutor: &statusStmtExecutor{
				base,
			},
			cr: st,
		}
	case *tree.DropRole:
		base.ComputationWrapper = InitNullComputationWrapper(ses, st, proc)
		ret = &DropRoleExecutor{
			statusStmtExecutor: &statusStmtExecutor{
				base,
			},
			dr: st,
		}
	case *tree.Grant:
		base.ComputationWrapper = InitNullComputationWrapper(ses, st, proc)
		ret = &GrantExecutor{
			statusStmtExecutor: &statusStmtExecutor{
				base,
			},
			g: st,
		}
	case *tree.Revoke:
		base.ComputationWrapper = InitNullComputationWrapper(ses, st, proc)
		ret = &RevokeExecutor{
			statusStmtExecutor: &statusStmtExecutor{
				base,
			},
			r: st,
		}
	case *tree.CreateTable:
		ret = &CreateTableExecutor{
			statusStmtExecutor: &statusStmtExecutor{
				base,
			},
			ct: st,
		}
	case *tree.DropTable:
		ret = &DropTableExecutor{
			statusStmtExecutor: &statusStmtExecutor{
				base,
			},
			dt: st,
		}
	case *tree.CreateDatabase:
		ret = &CreateDatabaseExecutor{
			statusStmtExecutor: &statusStmtExecutor{
				base,
			},
			cd: st,
		}
	case *tree.CreateIndex:
		ret = &CreateIndexExecutor{
			statusStmtExecutor: &statusStmtExecutor{
				base,
			},
			ci: st,
		}
	case *tree.DropIndex:
		ret = &DropIndexExecutor{
			statusStmtExecutor: &statusStmtExecutor{
				base,
			},
			di: st,
		}
	case *tree.CreateSequence:
		ret = &CreateSequenceExecutor{
			statusStmtExecutor: &statusStmtExecutor{
				base,
			},
			cs: st,
		}
	case *tree.DropSequence:
		ret = &DropSequenceExecutor{
			statusStmtExecutor: &statusStmtExecutor{
				base,
			},
			ds: st,
		}
	case *tree.CreateView:
		ret = &CreateViewExecutor{
			statusStmtExecutor: &statusStmtExecutor{
				base,
			},
			cv: st,
		}
	case *tree.AlterView:
		ret = &AlterViewExecutor{
			statusStmtExecutor: &statusStmtExecutor{
				base,
			},
			av: st,
		}
	case *tree.AlterTable:
		ret = &AlterTableExecutor{
			statusStmtExecutor: &statusStmtExecutor{
				base,
			},
			at: st,
		}
	case *tree.DropView:
		ret = &DropViewExecutor{
			statusStmtExecutor: &statusStmtExecutor{
				base,
			},
			dv: st,
		}
	case *tree.Insert:
		ret = &InsertExecutor{
			statusStmtExecutor: &statusStmtExecutor{
				base,
			},
			i: st,
		}
	case *tree.Load:
		ret = &LoadExecutor{
			statusStmtExecutor: &statusStmtExecutor{
				base,
			},
			l: st,
		}
	case *tree.SetDefaultRole:
		ret = &SetDefaultRoleExecutor{
			statusStmtExecutor: &statusStmtExecutor{
				base,
			},
			sdr: st,
		}
	case *tree.SetPassword:
		ret = &SetPasswordExecutor{
			statusStmtExecutor: &statusStmtExecutor{
				base,
			},
			sp: st,
		}
	case *tree.TruncateTable:
		ret = &TruncateTableExecutor{
			statusStmtExecutor: &statusStmtExecutor{
				base,
			},
			tt: st,
		}
	//PART 3: hybrid
	case *tree.Execute:
		ret = &ExecuteExecutor{
			baseStmtExecutor: base,
			e:                st,
		}
	default:
		return nil, moerr.NewInternalError(proc.Ctx, "no such statement %s", stmt.String())
	}
	return ret, err
}

var GetStmtExecList = func(db, sql, user string, eng engine.Engine, proc *process.Process, ses *Session) ([]StmtExecutor, error) {
	var stmtExecList []StmtExecutor = nil
	var stmtExec StmtExecutor
	var stmts []tree.Statement = nil
	var cmdFieldStmt *InternalCmdFieldList
	var err error

	appendStmtExec := func(se StmtExecutor) {
		stmtExecList = append(stmtExecList, se)
	}

	if isCmdFieldListSql(sql) {
		cmdFieldStmt, err = parseCmdFieldList(proc.Ctx, sql)
		if err != nil {
			return nil, err
		}
		stmts = append(stmts, cmdFieldStmt)
	} else {
		v, err := ses.GetGlobalVar("lower_case_table_names")
		if err != nil {
			return nil, err
		}
		stmts, err = parsers.Parse(proc.Ctx, dialect.MYSQL, sql, v.(int64))
		if err != nil {
			return nil, err
		}
	}

	for _, stmt := range stmts {
		cw := InitTxnComputationWrapper(ses, stmt, proc)
		base := &baseStmtExecutor{}
		base.ComputationWrapper = cw
		stmtExec, err = getStmtExecutor(ses, proc, base, stmt)
		if err != nil {
			return nil, err
		}
		appendStmtExec(stmtExec)
	}
	return stmtExecList, nil
}

func incStatementCounter(tenant string, stmt tree.Statement) {
	metric.StatementCounter(tenant, getStatementType(stmt).GetQueryType()).Inc()
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
	if ses.pu.SV.SkipCheckPrivilege {
		return nil
	}

	if ses.skipAuthForSpecialUser() {
		return nil
	}
	var havePrivilege bool
	var err error
	if ses.GetTenantInfo() != nil {
		ses.SetPrivilege(determinePrivilegeSetOfStatement(stmt))
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
	if ses.pu.SV.SkipCheckPrivilege {
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
	if ses.pu.SV.SkipCheckPrivilege {
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
func (mce *MysqlCmdExecutor) canExecuteStatementInUncommittedTransaction(requestCtx context.Context, stmt tree.Statement) error {
	can, err := statementCanBeExecutedInUncommittedTransaction(mce.GetSession(), stmt)
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
		} else if IsParameterModificationStatement(stmt) {
			return moerr.NewInternalError(requestCtx, parameterModificationInTxnErrorInfo())
		} else {
			return moerr.NewInternalError(requestCtx, unclassifiedStatementInUncommittedTxnErrorInfo())
		}
	}
	return nil
}

func (mce *MysqlCmdExecutor) processLoadLocal(ctx context.Context, param *tree.ExternParam, writer *io.PipeWriter) (err error) {
	ses := mce.GetSession()
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
		if moerr.IsMoErrCode(err, moerr.ErrInvalidInput) {
			err = moerr.NewInvalidInput(ctx, "cannot read '%s' from client,please check the file path, user privilege and if client start with --local-infile", param.Filepath)
		}
		proto.SetSequenceID(proto.GetSequenceId() + 1)
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

	skipWrite := false
	// If inner error occurs(unexpected or expected(ctrl-c)), proc.LoadLocalReader will be closed.
	// Then write will return error, but we need to read the rest of the data and not write it to pipe.
	// So we need a flag[skipWrite] to tell us whether we need to write the data to pipe.
	// https://github.com/matrixorigin/matrixone/issues/6665#issuecomment-1422236478

	_, err = writer.Write(packet.Payload)
	if err != nil {
		skipWrite = true // next, we just need read the rest of the data,no need to write it to pipe.
		logErrorf(ses.GetDebugString(), "load local '%s', write error: %v", param.Filepath, err)
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

		writeStart := time.Now()
		if !skipWrite {
			_, err = writer.Write(packet.Payload)
			if err != nil {
				logErrorf(ses.GetDebugString(), "load local '%s', epoch: %d, write error: %v", param.Filepath, epoch, err)
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

// executeStmt runs the single statement
func (mce *MysqlCmdExecutor) executeStmt(requestCtx context.Context,
	ses *Session,
	stmt tree.Statement,
	proc *process.Process,
	cw ComputationWrapper,
	i int,
	cws []ComputationWrapper,
	proto MysqlProtocol,
	pu *config.ParameterUnit,
	tenant string,
	userName string,
) (retErr error) {
	var err error
	var cmpBegin time.Time
	var ret interface{}
	var runner ComputationRunner
	var selfHandle bool
	var txnErr error
	var rspLen uint64
	var prepareStmt *PrepareStmt
	var err2 error
	var columns []interface{}
	var mrs *MysqlResultSet
	var loadLocalErrGroup *errgroup.Group
	var loadLocalWriter *io.PipeWriter

	//execution succeeds during the transaction. commit the transaction
	commitTxnFunc := func() error {
		//load data handle txn failure internally
		incStatementCounter(tenant, stmt)
		txnErr = ses.TxnCommitSingleStatement(stmt)
		if txnErr != nil {
			logStatementStatus(requestCtx, ses, stmt, fail, txnErr)
			return txnErr
		}
		//response the client
		respClient := func() error {
			switch stmt.(type) {
			case *tree.Select:
				if len(proc.SessionInfo.SeqAddValues) != 0 {
					ses.AddSeqValues(proc)
				}
				ses.SetSeqLastValue(proc)
			case *tree.CreateTable, *tree.DropTable, *tree.CreateDatabase, *tree.DropDatabase,
				*tree.CreateIndex, *tree.DropIndex, *tree.Insert, *tree.Update,
				*tree.CreateView, *tree.DropView, *tree.AlterView, *tree.AlterTable, *tree.Load, *tree.MoDump,
				*tree.CreateSequence, *tree.DropSequence,
				*tree.CreateAccount, *tree.DropAccount, *tree.AlterAccount, *tree.AlterDataBaseConfig, *tree.CreatePublication, *tree.AlterPublication, *tree.DropPublication,
				*tree.CreateFunction, *tree.DropFunction,
				*tree.CreateProcedure, *tree.DropProcedure,
				*tree.CreateUser, *tree.DropUser, *tree.AlterUser,
				*tree.CreateRole, *tree.DropRole, *tree.Revoke, *tree.Grant,
				*tree.SetDefaultRole, *tree.SetRole, *tree.SetPassword, *tree.Delete, *tree.TruncateTable, *tree.Use,
				*tree.BeginTransaction, *tree.CommitTransaction, *tree.RollbackTransaction:
				resp := mce.setResponse(i, len(cws), rspLen)
				if _, ok := stmt.(*tree.Insert); ok {
					resp.lastInsertId = proc.GetLastInsertID()
					if proc.GetLastInsertID() != 0 {
						ses.SetLastInsertID(proc.GetLastInsertID())
					}
				}
				if len(proc.SessionInfo.SeqDeleteKeys) != 0 {
					ses.DeleteSeqValues(proc)
				}

				if st, ok := cw.GetAst().(*tree.CreateTable); ok {
					_ = doGrantPrivilegeImplicitly(requestCtx, ses, st)
				}

				if st, ok := cw.GetAst().(*tree.CreateDatabase); ok {
					_ = insertRecordToMoMysqlCompatibilityMode(requestCtx, ses, stmt)
					_ = doGrantPrivilegeImplicitly(requestCtx, ses, st)
				}

				if _, ok := cw.GetAst().(*tree.DropDatabase); ok {
					_ = deleteRecordToMoMysqlCompatbilityMode(requestCtx, ses, stmt)
				}

				if err2 = mce.GetSession().GetMysqlProtocol().SendResponse(requestCtx, resp); err2 != nil {
					retErr = moerr.NewInternalError(requestCtx, "routine send response failed. error:%v ", err2)
					logStatementStatus(requestCtx, ses, stmt, fail, retErr)
					return retErr
				}

			case *tree.PrepareStmt, *tree.PrepareString:
				if ses.GetCmd() == COM_STMT_PREPARE {
					if err2 = mce.GetSession().GetMysqlProtocol().SendPrepareResponse(requestCtx, prepareStmt); err2 != nil {
						retErr = moerr.NewInternalError(requestCtx, "routine send response failed. error:%v ", err2)
						logStatementStatus(requestCtx, ses, stmt, fail, retErr)
						return retErr
					}
				} else {
					resp := mce.setResponse(i, len(cws), rspLen)
					if err2 = mce.GetSession().GetMysqlProtocol().SendResponse(requestCtx, resp); err2 != nil {
						retErr = moerr.NewInternalError(requestCtx, "routine send response failed. error:%v ", err2)
						logStatementStatus(requestCtx, ses, stmt, fail, retErr)
						return retErr
					}
				}

			case *tree.SetVar, *tree.SetTransaction:
				resp := mce.setResponse(i, len(cws), rspLen)
				if err = proto.SendResponse(requestCtx, resp); err != nil {
					return moerr.NewInternalError(requestCtx, "routine send response failed. error:%v ", err)
				}

			case *tree.Deallocate:
				//we will not send response in COM_STMT_CLOSE command
				if ses.GetCmd() != COM_STMT_CLOSE {
					resp := mce.setResponse(i, len(cws), rspLen)
					if err2 = mce.GetSession().GetMysqlProtocol().SendResponse(requestCtx, resp); err2 != nil {
						retErr = moerr.NewInternalError(requestCtx, "routine send response failed. error:%v ", err2)
						logStatementStatus(requestCtx, ses, stmt, fail, retErr)
						return retErr
					}
				}

			case *tree.Reset:
				resp := mce.setResponse(i, len(cws), rspLen)
				if err2 = mce.GetSession().GetMysqlProtocol().SendResponse(requestCtx, resp); err2 != nil {
					retErr = moerr.NewInternalError(requestCtx, "routine send response failed. error:%v ", err2)
					logStatementStatus(requestCtx, ses, stmt, fail, retErr)
					return retErr
				}
			}
			logStatementStatus(requestCtx, ses, stmt, success, nil)
			return retErr
		}
		retErr = respClient()
		if retErr != nil {
			return retErr
		}
		return retErr
	}

	//get errors during the transaction. rollback the transaction
	rollbackTxnFunc := func() error {
		incStatementCounter(tenant, stmt)
		incStatementErrorsCounter(tenant, stmt)
		/*
			Cases    | set Autocommit = 1/0 | BEGIN statement |
			---------------------------------------------------
			Case1      1                       Yes
			Case2      1                       No
			Case3      0                       Yes
			Case4      0                       No
			---------------------------------------------------
			update error message in Case1,Case3,Case4.
		*/
		if ses.InMultiStmtTransactionMode() && ses.InActiveTransaction() {
			ses.cleanCache()
			ses.SetOptionBits(OPTION_ATTACH_ABORT_TRANSACTION_ERROR)
		}
		logError(ses, ses.GetDebugString(), err.Error())
		txnErr = ses.TxnRollbackSingleStatement(stmt)
		if txnErr != nil {
			logStatementStatus(requestCtx, ses, stmt, fail, txnErr)
			return txnErr
		}
		logStatementStatus(requestCtx, ses, stmt, fail, err)
		return err
	}

	//finish the transaction
	finishTxnFunc := func() error {
		if err == nil {
			retErr = commitTxnFunc()
			if retErr != nil {
				return retErr
			}
			return retErr
		}

		retErr = rollbackTxnFunc()
		if retErr != nil {
			return retErr
		}
		return retErr
	}

	defer func() {
		retErr = finishTxnFunc()
	}()

	//check transaction states
	switch stmt.(type) {
	case *tree.BeginTransaction:
		err = ses.TxnBegin()
		if err != nil {
			return err
		}
		RecordStatementTxnID(requestCtx, ses)
	case *tree.CommitTransaction:
		err = ses.TxnCommit()
		if err != nil {
			return err
		}
	case *tree.RollbackTransaction:
		err = ses.TxnRollback()
		if err != nil {
			return err
		}
	}

	switch st := stmt.(type) {
	case *tree.Select:
		if st.Ep != nil {
			ses.SetExportParam(st.Ep)
		}
	}

	selfHandle = false

	switch st := stmt.(type) {
	case *tree.BeginTransaction, *tree.CommitTransaction, *tree.RollbackTransaction:
		selfHandle = true
	case *tree.SetRole:
		selfHandle = true
		ses.InvalidatePrivilegeCache()
		//switch role
		err = mce.handleSwitchRole(requestCtx, st)
		if err != nil {
			return err
		}
	case *tree.Use:
		selfHandle = true
		var v interface{}
		v, err = ses.GetGlobalVar("lower_case_table_names")
		if err != nil {
			return err
		}
		st.Name.SetConfig(v.(int64))
		//use database
		err = mce.handleChangeDB(requestCtx, st.Name.Compare())
		if err != nil {
			return err
		}
		err = changeVersion(requestCtx, ses, st.Name.Compare())
		if err != nil {
			return err
		}
	case *tree.MoDump:
		selfHandle = true
		//dump
		err = mce.handleDump(requestCtx, st)
		if err != nil {
			return err
		}
	case *tree.CreateDatabase:
		err = inputNameIsInvalid(proc.Ctx, string(st.Name))
		if err != nil {
			return err
		}
		if st.SubscriptionOption != nil && ses.GetTenantInfo() != nil && !ses.GetTenantInfo().IsAdminRole() {
			return moerr.NewInternalError(proc.Ctx, "only admin can create subscription")
		}
	case *tree.DropDatabase:
		err = inputNameIsInvalid(proc.Ctx, string(st.Name))
		if err != nil {
			return err
		}
		ses.InvalidatePrivilegeCache()
		// if the droped database is the same as the one in use, database must be reseted to empty.
		if string(st.Name) == ses.GetDatabaseName() {
			ses.SetDatabaseName("")
		}
	case *tree.PrepareStmt:
		selfHandle = true
		prepareStmt, err = mce.handlePrepareStmt(requestCtx, st)
		if err != nil {
			return err
		}
		err = authenticateUserCanExecutePrepareOrExecute(requestCtx, ses, prepareStmt.PrepareStmt, prepareStmt.PreparePlan.GetDcl().GetPrepare().GetPlan())
		if err != nil {
			mce.GetSession().RemovePrepareStmt(prepareStmt.Name)
			return err
		}
	case *tree.PrepareString:
		selfHandle = true
		prepareStmt, err = mce.handlePrepareString(requestCtx, st)
		if err != nil {
			return err
		}
		err = authenticateUserCanExecutePrepareOrExecute(requestCtx, ses, prepareStmt.PrepareStmt, prepareStmt.PreparePlan.GetDcl().GetPrepare().GetPlan())
		if err != nil {
			mce.GetSession().RemovePrepareStmt(prepareStmt.Name)
			return err
		}
	case *tree.Deallocate:
		selfHandle = true
		err = mce.handleDeallocate(requestCtx, st)
		if err != nil {
			return err
		}
	case *tree.Reset:
		selfHandle = true
		err = mce.handleReset(requestCtx, st)
		if err != nil {
			return err
		}
	case *tree.SetVar:
		selfHandle = true
		err = mce.handleSetVar(requestCtx, st)
		if err != nil {
			return err
		}
	case *tree.ShowVariables:
		selfHandle = true
		err = mce.handleShowVariables(st, proc, i, len(cws))
		if err != nil {
			return err
		}
	case *tree.ShowErrors, *tree.ShowWarnings:
		selfHandle = true
		err = mce.handleShowErrors(i, len(cws))
		if err != nil {
			return err
		}
	case *tree.AnalyzeStmt:
		selfHandle = true
		if err = mce.handleAnalyzeStmt(requestCtx, st); err != nil {
			return err
		}
	case *tree.ExplainStmt:
		selfHandle = true
		if err = mce.handleExplainStmt(requestCtx, st); err != nil {
			return err
		}
	case *tree.ExplainAnalyze:
		ses.SetData(nil)
	case *tree.ShowTableStatus:
		ses.SetShowStmtType(ShowTableStatus)
		ses.SetData(nil)
	case *InternalCmdFieldList:
		selfHandle = true
		if err = mce.handleCmdFieldList(requestCtx, st); err != nil {
			return err
		}
	case *tree.CreatePublication:
		selfHandle = true
		if err = mce.handleCreatePublication(requestCtx, st); err != nil {
			return err
		}
	case *tree.AlterPublication:
		selfHandle = true
		if err = mce.handleAlterPublication(requestCtx, st); err != nil {
			return err
		}
	case *tree.DropPublication:
		selfHandle = true
		if err = mce.handleDropPublication(requestCtx, st); err != nil {
			return err
		}
	case *tree.ShowSubscriptions:
		selfHandle = true
		if err = mce.handleShowSubscriptions(requestCtx, st, i, len(cws)); err != nil {
			return err
		}
	case *tree.CreateAccount:
		selfHandle = true
		ses.InvalidatePrivilegeCache()
		if err = mce.handleCreateAccount(requestCtx, st); err != nil {
			return err
		}
	case *tree.DropAccount:
		selfHandle = true
		ses.InvalidatePrivilegeCache()
		if err = mce.handleDropAccount(requestCtx, st); err != nil {
			return err
		}
	case *tree.AlterAccount:
		ses.InvalidatePrivilegeCache()
		selfHandle = true
		if err = mce.handleAlterAccount(requestCtx, st); err != nil {
			return err
		}
	case *tree.AlterDataBaseConfig:
		ses.InvalidatePrivilegeCache()
		selfHandle = true
		if st.IsAccountLevel {
			if err = mce.handleAlterAccountConfig(requestCtx, ses, st); err != nil {
				return err
			}
		} else {
			if err = mce.handleAlterDataBaseConfig(requestCtx, ses, st); err != nil {
				return err
			}
		}
	case *tree.CreateUser:
		selfHandle = true
		ses.InvalidatePrivilegeCache()
		if err = mce.handleCreateUser(requestCtx, st); err != nil {
			return err
		}
	case *tree.DropUser:
		selfHandle = true
		ses.InvalidatePrivilegeCache()
		if err = mce.handleDropUser(requestCtx, st); err != nil {
			return err
		}
	case *tree.AlterUser: //TODO
		selfHandle = true
		ses.InvalidatePrivilegeCache()
		if err = mce.handleAlterUser(requestCtx, st); err != nil {
			return err
		}
	case *tree.CreateRole:
		selfHandle = true
		ses.InvalidatePrivilegeCache()
		if err = mce.handleCreateRole(requestCtx, st); err != nil {
			return err
		}
	case *tree.DropRole:
		selfHandle = true
		ses.InvalidatePrivilegeCache()
		if err = mce.handleDropRole(requestCtx, st); err != nil {
			return err
		}
	case *tree.CreateFunction:
		selfHandle = true
		if err = mce.handleCreateFunction(requestCtx, st); err != nil {
			return err
		}
	case *tree.DropFunction:
		selfHandle = true
		if err = mce.handleDropFunction(requestCtx, st); err != nil {
			return err
		}
	case *tree.CreateProcedure:
		selfHandle = true
		if err = mce.handleCreateProcedure(requestCtx, st); err != nil {
			return err
		}
	case *tree.DropProcedure:
		selfHandle = true
		if err = mce.handleDropProcedure(requestCtx, st); err != nil {
			return err
		}
	case *tree.CallStmt:
		selfHandle = true
		if err = mce.handleCallProcedure(requestCtx, st, proc, i, len(cws)); err != nil {
			return err
		}
	case *tree.Grant:
		selfHandle = true
		ses.InvalidatePrivilegeCache()
		switch st.Typ {
		case tree.GrantTypeRole:
			if err = mce.handleGrantRole(requestCtx, &st.GrantRole); err != nil {
				return err
			}
		case tree.GrantTypePrivilege:
			if err = mce.handleGrantPrivilege(requestCtx, &st.GrantPrivilege); err != nil {
				return err
			}
		}
	case *tree.Revoke:
		selfHandle = true
		ses.InvalidatePrivilegeCache()
		switch st.Typ {
		case tree.RevokeTypeRole:
			if err = mce.handleRevokeRole(requestCtx, &st.RevokeRole); err != nil {
				return err
			}
		case tree.RevokeTypePrivilege:
			if err = mce.handleRevokePrivilege(requestCtx, &st.RevokePrivilege); err != nil {
				return err
			}
		}
	case *tree.Kill:
		selfHandle = true
		ses.InvalidatePrivilegeCache()
		if err = mce.handleKill(requestCtx, st); err != nil {
			return err
		}
	case *tree.ShowAccounts:
		selfHandle = true
		if err = mce.handleShowAccounts(requestCtx, st, i, len(cws)); err != nil {
			return err
		}
	case *tree.Load:
		if st.Local {
			proc.LoadLocalReader, loadLocalWriter = io.Pipe()
		}
	case *tree.ShowBackendServers:
		selfHandle = true
		if err = mce.handleShowBackendServers(requestCtx, i, len(cws)); err != nil {
			return err
		}
	case *tree.SetTransaction:
		selfHandle = true
		//TODO: handle set transaction
	case *tree.ShowGrants:
		if len(st.Username) == 0 {
			st.Username = userName
		}
		if len(st.Hostname) == 0 || st.Hostname == "%" {
			st.Hostname = rootHost
		}
	}

	if selfHandle {
		return err
	}
	if err = cw.SetDatabaseName(ses.GetDatabaseName()); err != nil {
		return err
	}

	cmpBegin = time.Now()

	if ret, err = cw.Compile(requestCtx, ses, ses.GetOutputCallback()); err != nil {
		return err
	}
	stmt = cw.GetAst()
	// reset some special stmt for execute statement
	switch st := stmt.(type) {
	case *tree.SetVar:
		err = mce.handleSetVar(requestCtx, st)
		if err != nil {
			return err
		} else {
			return err
		}
	case *tree.ShowVariables:
		err = mce.handleShowVariables(st, proc, i, len(cws))
		if err != nil {
			return err
		} else {
			return err
		}
	case *tree.ShowErrors, *tree.ShowWarnings:
		err = mce.handleShowErrors(i, len(cws))
		if err != nil {
			return err
		} else {
			return err
		}
	}

	runner = ret.(ComputationRunner)

	// only log if build time is longer than 1s
	if time.Since(cmpBegin) > time.Second {
		logInfof(ses.GetDebugString(), "time of Exec.Build : %s", time.Since(cmpBegin).String())
	}

	mrs = ses.GetMysqlResultSet()
	// cw.Compile might rewrite sql, here we fetch the latest version
	switch statement := cw.GetAst().(type) {
	//produce result set
	case *tree.Select,
		*tree.ShowCreateTable, *tree.ShowCreateDatabase, *tree.ShowTables, *tree.ShowSequences, *tree.ShowDatabases, *tree.ShowColumns,
		*tree.ShowProcessList, *tree.ShowStatus, *tree.ShowTableStatus, *tree.ShowGrants, *tree.ShowRolesStmt,
		*tree.ShowIndex, *tree.ShowCreateView, *tree.ShowTarget, *tree.ShowCollation, *tree.ValuesStatement,
		*tree.ExplainFor, *tree.ExplainStmt, *tree.ShowTableNumber, *tree.ShowColumnNumber, *tree.ShowTableValues, *tree.ShowLocks, *tree.ShowNodeList, *tree.ShowFunctionOrProcedureStatus, *tree.ShowPublications, *tree.ShowCreatePublications:
		columns, err = cw.GetColumns()
		if err != nil {
			logErrorf(ses.GetDebugString(), "GetColumns from Computation handler failed. error: %v", err)
			return err
		}
		if c, ok := cw.(*TxnComputationWrapper); ok {
			ses.rs = &plan.ResultColDef{ResultCols: plan2.GetResultColumnsFromPlan(c.plan)}
		}
		/*
			Step 1 : send column count and column definition.
		*/
		//send column count
		colCnt := uint64(len(columns))
		err = proto.SendColumnCountPacket(colCnt)
		if err != nil {
			return err
		}
		//send columns
		//column_count * Protocol::ColumnDefinition packets
		cmd := ses.GetCmd()
		for _, c := range columns {
			mysqlc := c.(Column)
			mrs.AddColumn(mysqlc)
			/*
				mysql COM_QUERY response: send the column definition per column
			*/
			err = proto.SendColumnDefinitionPacket(requestCtx, mysqlc, int(cmd))
			if err != nil {
				return err
			}
		}

		/*
			mysql COM_QUERY response: End after the column has been sent.
			send EOF packet
		*/
		err = proto.SendEOFPacketIf(0, ses.GetServerStatus())
		if err != nil {
			return err
		}

		runBegin := time.Now()
		/*
			Step 2: Start pipeline
			Producing the data row and sending the data row
		*/
		ep := ses.GetExportParam()
		if ep.Outfile {
			ep.DefaultBufSize = pu.SV.ExportDataDefaultFlushSize
			initExportFileParam(ep, mrs)
			if err = openNewFile(requestCtx, ep, mrs); err != nil {
				return err
			}
		}
		// todo: add trace
		if err = runner.Run(0); err != nil {
			return err
		}

		switch ses.GetShowStmtType() {
		case ShowTableStatus:
			if err = handleShowTableStatus(ses, statement.(*tree.ShowTableStatus), proc); err != nil {
				return err
			}
		}

		if ep.Outfile {
			oq := NewOutputQueue(ses.GetRequestContext(), ses, 0, nil, nil)
			if err = exportAllData(oq); err != nil {
				return err
			}
			if err = ep.Writer.Flush(); err != nil {
				return err
			}
			if err = ep.File.Close(); err != nil {
				return err
			}
		}

		// only log if run time is longer than 1s
		if time.Since(runBegin) > time.Second {
			logInfof(ses.GetDebugString(), "time of Exec.Run : %s", time.Since(runBegin).String())
		}

		/*
			Step 3: Say goodbye
			mysql COM_QUERY response: End after the data row has been sent.
			After all row data has been sent, it sends the EOF or OK packet.
		*/
		err = proto.sendEOFOrOkPacket(0, ses.GetServerStatus())
		if err != nil {
			return err
		}

		/*
			Step 4: Serialize the execution plan by json
		*/
		if cwft, ok := cw.(*TxnComputationWrapper); ok {
			_ = cwft.RecordExecPlan(requestCtx)
		}
	//just status, no result set
	case *tree.CreateTable, *tree.DropTable, *tree.CreateDatabase, *tree.DropDatabase,
		*tree.CreateIndex, *tree.DropIndex,
		*tree.CreateView, *tree.DropView, *tree.AlterView, *tree.AlterTable,
		*tree.CreateSequence, *tree.DropSequence,
		*tree.Insert, *tree.Update,
		*tree.BeginTransaction, *tree.CommitTransaction, *tree.RollbackTransaction,
		*tree.SetVar,
		*tree.Load,
		*tree.CreateUser, *tree.DropUser, *tree.AlterUser,
		*tree.CreateRole, *tree.DropRole,
		*tree.Revoke, *tree.Grant,
		*tree.SetDefaultRole, *tree.SetRole, *tree.SetPassword,
		*tree.Delete, *tree.TruncateTable, *tree.LockTableStmt, *tree.UnLockTableStmt:
		//change privilege
		switch cw.GetAst().(type) {
		case *tree.DropTable, *tree.DropDatabase, *tree.DropIndex, *tree.DropView, *tree.DropSequence,
			*tree.CreateUser, *tree.DropUser, *tree.AlterUser,
			*tree.CreateRole, *tree.DropRole,
			*tree.Revoke, *tree.Grant,
			*tree.SetDefaultRole, *tree.SetRole:
			ses.InvalidatePrivilegeCache()
		}
		runBegin := time.Now()
		/*
			Step 1: Start
		*/

		if st, ok := cw.GetAst().(*tree.Load); ok {
			if st.Local {
				loadLocalErrGroup = new(errgroup.Group)
				loadLocalErrGroup.Go(func() error {
					return mce.processLoadLocal(proc.Ctx, st.Param, loadLocalWriter)
				})
			}
		}

		if err = runner.Run(0); err != nil {
			if loadLocalErrGroup != nil { // release resources
				err2 = proc.LoadLocalReader.Close()
				if err2 != nil {
					logErrorf(ses.GetDebugString(), "processLoadLocal reader close failed: %s", err2.Error())
				}
				err2 = loadLocalErrGroup.Wait() // executor failed, but processLoadLocal is still running, wait for it
				if err2 != nil {
					logErrorf(ses.GetDebugString(), "processLoadLocal goroutine failed: %s", err2.Error())
				}
			}
			return err
		}

		if loadLocalErrGroup != nil {
			if err = loadLocalErrGroup.Wait(); err != nil { //executor success, but processLoadLocal goroutine failed
				return err
			}
		}

		// only log if run time is longer than 1s
		if time.Since(runBegin) > time.Second {
			logInfof(ses.GetDebugString(), "time of Exec.Run : %s", time.Since(runBegin).String())
		}

		rspLen = cw.GetAffectedRows()
		echoTime := time.Now()

		logDebugf(ses.GetDebugString(), "time of SendResponse %s", time.Since(echoTime).String())

		/*
			Step 4: Serialize the execution plan by json
		*/
		if cwft, ok := cw.(*TxnComputationWrapper); ok {
			_ = cwft.RecordExecPlan(requestCtx)
		}
	case *tree.ExplainAnalyze:
		explainColName := "QUERY PLAN"
		columns, err = GetExplainColumns(requestCtx, explainColName)
		if err != nil {
			logErrorf(ses.GetDebugString(), "GetColumns from ExplainColumns handler failed, error: %v", err)
			return err
		}
		/*
			Step 1 : send column count and column definition.
		*/
		//send column count
		colCnt := uint64(len(columns))
		err = proto.SendColumnCountPacket(colCnt)
		if err != nil {
			return err
		}
		//send columns
		//column_count * Protocol::ColumnDefinition packets
		cmd := ses.GetCmd()
		for _, c := range columns {
			mysqlc := c.(Column)
			mrs.AddColumn(mysqlc)
			/*
				mysql COM_QUERY response: send the column definition per column
			*/
			err = proto.SendColumnDefinitionPacket(requestCtx, mysqlc, int(cmd))
			if err != nil {
				return err
			}
		}
		/*
			mysql COM_QUERY response: End after the column has been sent.
			send EOF packet
		*/
		err = proto.SendEOFPacketIf(0, ses.GetServerStatus())
		if err != nil {
			return err
		}

		runBegin := time.Now()
		/*
			Step 1: Start
		*/
		if err = runner.Run(0); err != nil {
			return err
		}

		// only log if run time is longer than 1s
		if time.Since(runBegin) > time.Second {
			logInfof(ses.GetDebugString(), "time of Exec.Run : %s", time.Since(runBegin).String())
		}

		if cwft, ok := cw.(*TxnComputationWrapper); ok {
			queryPlan := cwft.plan
			// generator query explain
			explainQuery := explain.NewExplainQueryImpl(queryPlan.GetQuery())

			// build explain data buffer
			buffer := explain.NewExplainDataBuffer()
			var option *explain.ExplainOptions
			option, err = getExplainOption(requestCtx, statement.Options)
			if err != nil {
				return err
			}

			err = explainQuery.ExplainPlan(requestCtx, buffer, option)
			if err != nil {
				return err
			}

			err = buildMoExplainQuery(explainColName, buffer, ses, getDataFromPipeline)
			if err != nil {
				return err
			}

			/*
				Step 3: Say goodbye
				mysql COM_QUERY response: End after the data row has been sent.
				After all row data has been sent, it sends the EOF or OK packet.
			*/
			err = proto.sendEOFOrOkPacket(0, ses.GetServerStatus())
			if err != nil {
				return err
			}
		}
	}
	return retErr
}

// execute query
func (mce *MysqlCmdExecutor) doComQuery(requestCtx context.Context, input *UserInput) (retErr error) {
	beginInstant := time.Now()
	ses := mce.GetSession()
	input.genSqlSourceType(ses)
	ses.SetShowStmtType(NotShowStatement)
	proto := ses.GetMysqlProtocol()
	ses.SetSql(input.getSql())
	ses.GetExportParam().Outfile = false
	pu := ses.GetParameterUnit()
	userName := rootName
	proc := process.New(
		requestCtx,
		ses.GetMemPool(),
		ses.GetTxnHandler().GetTxnClient(),
		nil,
		pu.FileService,
		pu.LockService,
		ses.GetAutoIncrCacheManager())
	proc.CopyVectorPool(ses.proc)
	proc.CopyValueScanBatch(ses.proc)
	proc.Id = mce.getNextProcessId()
	proc.Lim.Size = pu.SV.ProcessLimitationSize
	proc.Lim.BatchRows = pu.SV.ProcessLimitationBatchRows
	proc.Lim.MaxMsgSize = pu.SV.MaxMessageSize
	proc.Lim.PartitionRows = pu.SV.ProcessLimitationPartitionRows
	proc.SessionInfo = process.SessionInfo{
		User:          ses.GetUserName(),
		Host:          pu.SV.Host,
		ConnectionID:  uint64(proto.ConnectionID()),
		Database:      ses.GetDatabaseName(),
		Version:       makeServerVersion(pu, serverVersion.Load().(string)),
		TimeZone:      ses.GetTimeZone(),
		StorageEngine: pu.StorageEngine,
		LastInsertID:  ses.GetLastInsertID(),
		SqlHelper:     ses.GetSqlHelper(),
	}
	proc.SetResolveVariableFunc(mce.ses.txnCompileCtx.ResolveVariable)
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
		userName = ses.GetTenantInfo().GetUser()
	} else {
		proc.SessionInfo.Account = sysAccountName
		proc.SessionInfo.AccountId = sysAccountID
		proc.SessionInfo.RoleId = moAdminRoleID
		proc.SessionInfo.UserId = rootID
	}
	proc.SessionInfo.QueryId = ses.getQueryId(input.isInternal())
	ses.txnCompileCtx.SetProcess(ses.proc)
	ses.proc.SessionInfo = proc.SessionInfo
	cws, err := GetComputationWrapper(ses.GetDatabaseName(),
		input,
		ses.GetUserName(),
		pu.StorageEngine,
		proc, ses)
	if err != nil {
		requestCtx = RecordParseErrorStatement(requestCtx, ses, proc, beginInstant, parsers.HandleSqlForRecord(input.getSql()), input.getSqlSourceTypes(), err)
		retErr = err
		if _, ok := err.(*moerr.Error); !ok {
			retErr = moerr.NewParseError(requestCtx, err.Error())
		}
		logStatementStringStatus(requestCtx, ses, input.getSql(), fail, retErr)
		return retErr
	}

	defer func() {
		ses.SetMysqlResultSet(nil)
	}()

	canCache := true

	singleStatement := len(cws) == 1
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
		stmt := cw.GetAst()
		sqlType := input.getSqlSourceType(i)
		requestCtx = RecordStatement(requestCtx, ses, proc, cw, beginInstant, sqlRecord[i], sqlType, singleStatement)
		tenant := ses.GetTenantName(stmt)
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
		if ses.InActiveTransaction() {
			err = mce.canExecuteStatementInUncommittedTransaction(requestCtx, stmt)
			if err != nil {
				logStatementStatus(requestCtx, ses, stmt, fail, err)
				return err
			}
		}

		err = mce.executeStmt(requestCtx, ses, stmt, proc, cw, i, cws, proto, pu, tenant, userName)
		if err != nil {
			return err
		}
	} // end of for

	if canCache && !ses.isCached(input.getSql()) {
		plans := make([]*plan.Plan, len(cws))
		stmts := make([]tree.Statement, len(cws))
		for i, cw := range cws {
			if cwft, ok := cw.(*TxnComputationWrapper); ok && checkNodeCanCache(cwft.plan) {
				plans[i] = cwft.plan
				stmts[i] = cwft.stmt
			} else {
				return nil
			}
		}
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

// execute query. Currently, it is developing. Finally, it will replace the doComQuery.
func (mce *MysqlCmdExecutor) doComQueryInProgress(requestCtx context.Context, input *UserInput) (retErr error) {
	var stmtExecs []StmtExecutor
	var err error
	beginInstant := time.Now()
	ses := mce.GetSession()
	ses.SetShowStmtType(NotShowStatement)
	proto := ses.GetMysqlProtocol()
	ses.SetSql(input.getSql())
	ses.GetExportParam().Outfile = false
	pu := ses.GetParameterUnit()
	proc := process.New(
		requestCtx,
		ses.GetMemPool(),
		pu.TxnClient,
		nil,
		pu.FileService,
		pu.LockService,
		ses.GetAutoIncrCacheManager())
	proc.CopyVectorPool(ses.proc)
	proc.CopyValueScanBatch(ses.proc)
	proc.Id = mce.getNextProcessId()
	proc.Lim.Size = pu.SV.ProcessLimitationSize
	proc.Lim.BatchRows = pu.SV.ProcessLimitationBatchRows
	proc.Lim.PartitionRows = pu.SV.ProcessLimitationPartitionRows
	proc.SessionInfo = process.SessionInfo{
		User:          ses.GetUserName(),
		Host:          pu.SV.Host,
		ConnectionID:  uint64(proto.ConnectionID()),
		Database:      ses.GetDatabaseName(),
		Version:       makeServerVersion(pu, serverVersion.Load().(string)),
		TimeZone:      ses.GetTimeZone(),
		StorageEngine: pu.StorageEngine,
	}
	proc.SetResolveVariableFunc(mce.ses.txnCompileCtx.ResolveVariable)
	proc.InitSeq()
	// Copy curvalues stored in session to this proc.
	// Deep copy the map, takes some memory.
	ses.CopySeqToProc(proc)
	if ses.GetTenantInfo() != nil {
		proc.SessionInfo.Account = ses.GetTenantInfo().GetTenant()
		proc.SessionInfo.AccountId = ses.GetTenantInfo().GetTenantID()
		proc.SessionInfo.RoleId = ses.GetTenantInfo().GetDefaultRoleID()
		proc.SessionInfo.UserId = ses.GetTenantInfo().GetUserID()
	} else {
		proc.SessionInfo.Account = sysAccountName
		proc.SessionInfo.AccountId = sysAccountID
		proc.SessionInfo.RoleId = moAdminRoleID
		proc.SessionInfo.UserId = rootID
	}
	proc.SessionInfo.QueryId = ses.getQueryId(input.isInternal())
	ses.txnCompileCtx.SetProcess(ses.proc)
	ses.proc.SessionInfo = proc.SessionInfo
	stmtExecs, err = GetStmtExecList(ses.GetDatabaseName(),
		input.getSql(),
		ses.GetUserName(),
		pu.StorageEngine,
		proc, ses)
	if err != nil {
		requestCtx = RecordParseErrorStatement(requestCtx, ses, proc, beginInstant, parsers.HandleSqlForRecord(input.getSql()), input.getSqlSourceTypes(), err)
		retErr = moerr.NewParseError(requestCtx, err.Error())
		logStatementStringStatus(requestCtx, ses, input.getSql(), fail, retErr)
		return retErr
	}

	singleStatement := len(stmtExecs) == 1
	sqlRecord := parsers.HandleSqlForRecord(input.getSql())
	for i, exec := range stmtExecs {
		err = Execute(requestCtx, ses, proc, exec, beginInstant, sqlRecord[i], "", singleStatement)
		if err != nil {
			return err
		}
	}
	return err
}

func (mce *MysqlCmdExecutor) setResponse(cwIndex, cwsLen int, rspLen uint64) *Response {
	return mce.ses.SetNewResponse(OkResponse, rspLen, int(COM_QUERY), "", cwIndex, cwsLen)
}

// ExecRequest the server execute the commands from the client following the mysql's routine
func (mce *MysqlCmdExecutor) ExecRequest(requestCtx context.Context, ses *Session, req *Request) (resp *Response, err error) {
	defer func() {
		if e := recover(); e != nil {
			moe, ok := e.(*moerr.Error)
			if !ok {
				err = moerr.ConvertPanicError(requestCtx, e)
				resp = NewGeneralErrorResponse(COM_QUERY, mce.ses.GetServerStatus(), err)
			} else {
				resp = NewGeneralErrorResponse(COM_QUERY, mce.ses.GetServerStatus(), moe)
			}
		}
	}()

	var sql string
	logDebugf(ses.GetDebugString(), "cmd %v", req.GetCmd())
	ses.SetCmd(req.GetCmd())
	doComQuery := mce.GetDoQueryFunc()
	switch req.GetCmd() {
	case COM_QUIT:
		/*resp = NewResponse(
			OkResponse,
			0,
			int(COM_QUIT),
			nil,
		)*/
		return resp, moerr.NewInternalError(requestCtx, "client send quit")
	case COM_QUERY:
		var query = string(req.GetData().([]byte))
		mce.addSqlCount(1)
		logDebug(ses, ses.GetDebugString(), "query trace", logutil.ConnectionIdField(ses.GetConnectionID()), logutil.QueryField(SubStringFromBegin(query, int(ses.GetParameterUnit().SV.LengthOfQueryPrinted))))
		err = doComQuery(requestCtx, &UserInput{sql: query})
		if err != nil {
			resp = NewGeneralErrorResponse(COM_QUERY, mce.ses.GetServerStatus(), err)
		}
		return resp, nil
	case COM_INIT_DB:
		var dbname = string(req.GetData().([]byte))
		mce.addSqlCount(1)
		query := "use `" + dbname + "`"
		err = doComQuery(requestCtx, &UserInput{sql: query})
		if err != nil {
			resp = NewGeneralErrorResponse(COM_INIT_DB, mce.ses.GetServerStatus(), err)
		}

		return resp, nil
	case COM_FIELD_LIST:
		var payload = string(req.GetData().([]byte))
		mce.addSqlCount(1)
		query := makeCmdFieldListSql(payload)
		err = doComQuery(requestCtx, &UserInput{sql: query})
		if err != nil {
			resp = NewGeneralErrorResponse(COM_FIELD_LIST, mce.ses.GetServerStatus(), err)
		}

		return resp, nil
	case COM_PING:
		resp = NewGeneralOkResponse(COM_PING, mce.ses.GetServerStatus())

		return resp, nil

	case COM_STMT_PREPARE:
		ses.SetCmd(COM_STMT_PREPARE)
		sql = string(req.GetData().([]byte))
		mce.addSqlCount(1)

		// rewrite to "Prepare stmt_name from 'xxx'"
		newLastStmtID := ses.GenNewStmtId()
		newStmtName := getPrepareStmtName(newLastStmtID)
		sql = fmt.Sprintf("prepare %s from %s", newStmtName, sql)
		logDebug(ses, ses.GetDebugString(), "query trace", logutil.ConnectionIdField(ses.GetConnectionID()), logutil.QueryField(sql))

		err = doComQuery(requestCtx, &UserInput{sql: sql})
		if err != nil {
			resp = NewGeneralErrorResponse(COM_STMT_PREPARE, mce.ses.GetServerStatus(), err)
		}
		return resp, nil

	case COM_STMT_EXECUTE:
		ses.SetCmd(COM_STMT_EXECUTE)
		data := req.GetData().([]byte)
		var prepareStmt *PrepareStmt
		sql, prepareStmt, err = mce.parseStmtExecute(requestCtx, data)
		if err != nil {
			return NewGeneralErrorResponse(COM_STMT_EXECUTE, mce.ses.GetServerStatus(), err), nil
		}
		err = doComQuery(requestCtx, &UserInput{sql: sql})
		if err != nil {
			resp = NewGeneralErrorResponse(COM_STMT_EXECUTE, mce.ses.GetServerStatus(), err)
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
		err = mce.parseStmtSendLongData(requestCtx, data)
		if err != nil {
			resp = NewGeneralErrorResponse(COM_STMT_SEND_LONG_DATA, mce.ses.GetServerStatus(), err)
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

		err = doComQuery(requestCtx, &UserInput{sql: sql})
		if err != nil {
			resp = NewGeneralErrorResponse(COM_STMT_CLOSE, mce.ses.GetServerStatus(), err)
		}
		return resp, nil

	case COM_STMT_RESET:
		data := req.GetData().([]byte)

		//Payload of COM_STMT_RESET
		stmtID := binary.LittleEndian.Uint32(data[0:4])
		stmtName := getPrepareStmtName(stmtID)
		sql = fmt.Sprintf("reset prepare %s", stmtName)
		logDebug(ses, ses.GetDebugString(), "query trace", logutil.ConnectionIdField(ses.GetConnectionID()), logutil.QueryField(sql))
		err = doComQuery(requestCtx, &UserInput{sql: sql})
		if err != nil {
			resp = NewGeneralErrorResponse(COM_STMT_RESET, mce.ses.GetServerStatus(), err)
		}
		return resp, nil

	default:
		resp = NewGeneralErrorResponse(req.GetCmd(), mce.ses.GetServerStatus(), moerr.NewInternalError(requestCtx, "unsupported command. 0x%x", req.GetCmd()))
	}
	return resp, nil
}

func (mce *MysqlCmdExecutor) parseStmtExecute(requestCtx context.Context, data []byte) (string, *PrepareStmt, error) {
	// see https://dev.mysql.com/doc/dev/mysql-server/latest/page_protocol_com_stmt_execute.html
	pos := 0
	if len(data) < 4 {
		return "", nil, moerr.NewInvalidInput(requestCtx, "sql command contains malformed packet")
	}
	stmtID := binary.LittleEndian.Uint32(data[0:4])
	pos += 4

	stmtName := fmt.Sprintf("%s_%d", prefixPrepareStmtName, stmtID)
	ses := mce.GetSession()
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

func (mce *MysqlCmdExecutor) parseStmtSendLongData(requestCtx context.Context, data []byte) error {
	// see https://dev.mysql.com/doc/dev/mysql-server/latest/page_protocol_com_stmt_send_long_data.html
	pos := 0
	if len(data) < 4 {
		return moerr.NewInvalidInput(requestCtx, "sql command contains malformed packet")
	}
	stmtID := binary.LittleEndian.Uint32(data[0:4])
	pos += 4

	stmtName := fmt.Sprintf("%s_%d", prefixPrepareStmtName, stmtID)
	ses := mce.GetSession()
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

func (mce *MysqlCmdExecutor) SetCancelFunc(cancelFunc context.CancelFunc) {
	mce.mu.Lock()
	defer mce.mu.Unlock()
	mce.cancelRequestFunc = cancelFunc
}

func (mce *MysqlCmdExecutor) Close() {}

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
func buildErrorJsonPlan(uuid uuid.UUID, errcode uint16, msg string) []byte {
	explainData := explain.ExplainData{
		Code:    errcode,
		Message: msg,
		Success: false,
		Uuid:    uuid.String(),
	}
	buffer := &bytes.Buffer{}
	encoder := json.NewEncoder(buffer)
	encoder.SetEscapeHTML(false)
	encoder.Encode(explainData)
	return buffer.Bytes()
}

type jsonPlanHandler struct {
	jsonBytes  []byte
	statsBytes []byte
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

func (h *jsonPlanHandler) Stats(ctx context.Context) ([]byte, motrace.Statistic) {
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
		h.statsBytes = nil
	}
}

type marshalPlanHandler struct {
	marshalPlan *explain.ExplainData
	stmt        *motrace.StatementInfo
	uuid        uuid.UUID
	buffer      *bytes.Buffer
}

func NewMarshalPlanHandler(ctx context.Context, stmt *motrace.StatementInfo, plan *plan2.Plan) *marshalPlanHandler {
	// TODO: need mem improvement
	uuid := uuid.UUID(stmt.StatementID)
	if plan == nil || plan.GetQuery() == nil {
		return &marshalPlanHandler{
			marshalPlan: nil,
			stmt:        stmt,
			uuid:        uuid,
			buffer:      getMarshalPlanBufferPool(),
		}
	}
	return &marshalPlanHandler{
		marshalPlan: explain.BuildJsonPlan(ctx, uuid, &explain.MarshalPlanOptions, plan.GetQuery()),
		stmt:        stmt,
		uuid:        uuid,
		buffer:      getMarshalPlanBufferPool(),
	}
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

func (h *marshalPlanHandler) Marshal(ctx context.Context) (jsonBytes []byte) {
	var err error
	if h.marshalPlan != nil {
		var jsonBytesLen = 0
		h.buffer.Reset()
		// XXX, `buffer` can be used repeatedly as a global variable in the future
		// Provide a relatively balanced initial capacity [8192] for byte slice to prevent multiple memory requests
		encoder := json.NewEncoder(h.buffer)
		encoder.SetEscapeHTML(false)
		if time.Since(h.stmt.RequestAt) > motrace.GetLongQueryTime() {
			err = encoder.Encode(h.marshalPlan)
			if err != nil {
				moError := moerr.NewInternalError(ctx, "serialize plan to json error: %s", err.Error())
				jsonBytes = buildErrorJsonPlan(h.uuid, moError.ErrorCode(), moError.Error())
			} else {
				jsonBytesLen = h.buffer.Len()
			}
		} else {
			jsonBytes = buildErrorJsonPlan(h.uuid, moerr.ErrWarn, "sql query ignore execution plan")
		}
		// BG: bytes.Buffer maintain buf []byte.
		// if buf[off:] not enough but len(buf) is enough place, then it will reset off = 0.
		// So, in here, we need call Next(...) after all data has been written
		if jsonBytesLen > 0 {
			jsonBytes = h.buffer.Next(jsonBytesLen)
		}
	} else {
		jsonBytes = buildErrorJsonPlan(h.uuid, moerr.ErrWarn, "sql query no record execution plan")
	}
	return
}

func (h *marshalPlanHandler) Stats(ctx context.Context) (statsByte []byte, stats motrace.Statistic) {
	if h.marshalPlan != nil && len(h.marshalPlan.Steps) > 0 {
		stats.RowsRead, stats.BytesScan = h.marshalPlan.StatisticsRead()
		global := h.marshalPlan.Steps[0].GraphData.Global
		statsValues := getStatsFromGlobal(global, uint64(h.stmt.Duration))
		statsByte = []byte(fmt.Sprintf("%v", statsValues))
	} else {
		statsByte = []byte(fmt.Sprintf("%v", []uint64{1, 0, 0, 0, 0}))
	}
	return
}

func getStatsFromGlobal(global explain.Global, duration uint64) []uint64 {
	var timeConsumed, memorySize, s3IOInputCount, s3IOOutputCount uint64

	for _, stat := range global.Statistics.Time {
		if stat.Name == "Time Consumed" {
			timeConsumed = uint64(stat.Value)
			break
		}
	}

	for _, stat := range global.Statistics.Memory {
		if stat.Name == "Memory Size" {
			memorySize = uint64(stat.Value)
			break
		}
	}

	for _, stat := range global.Statistics.IO {
		if stat.Name == "S3 IO Input Count" {
			s3IOInputCount = uint64(stat.Value)
		} else if stat.Name == "S3 IO Output Count" {
			s3IOOutputCount = uint64(stat.Value)
		}
	}

	statsValues := make([]uint64, 5)
	statsValues[0] = 1 // this is the version number
	statsValues[1] = timeConsumed
	statsValues[2] = memorySize * duration
	statsValues[3] = s3IOInputCount
	statsValues[4] = s3IOOutputCount

	return statsValues
}
