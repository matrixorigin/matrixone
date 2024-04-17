// Copyright 2022 Matrix Origin
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
// http://www.apache.org/licenses/LICENSE-2.0
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
	"fmt"
	"time"

	"github.com/matrixorigin/matrixone/pkg/sql/compile"

	"github.com/google/uuid"

	"github.com/matrixorigin/matrixone/pkg/common/buffer"
	"github.com/matrixorigin/matrixone/pkg/common/moerr"
	"github.com/matrixorigin/matrixone/pkg/common/mpool"
	"github.com/matrixorigin/matrixone/pkg/container/batch"
	"github.com/matrixorigin/matrixone/pkg/defines"
	"github.com/matrixorigin/matrixone/pkg/logutil"
	"github.com/matrixorigin/matrixone/pkg/pb/metadata"
	"github.com/matrixorigin/matrixone/pkg/pb/plan"
	"github.com/matrixorigin/matrixone/pkg/pb/timestamp"
	"github.com/matrixorigin/matrixone/pkg/sql/parsers"
	"github.com/matrixorigin/matrixone/pkg/sql/parsers/dialect"
	"github.com/matrixorigin/matrixone/pkg/sql/parsers/dialect/mysql"
	"github.com/matrixorigin/matrixone/pkg/sql/parsers/tree"
	plan2 "github.com/matrixorigin/matrixone/pkg/sql/plan"
	"github.com/matrixorigin/matrixone/pkg/txn/clock"
	"github.com/matrixorigin/matrixone/pkg/txn/storage/memorystorage"
	"github.com/matrixorigin/matrixone/pkg/util/trace"
	"github.com/matrixorigin/matrixone/pkg/util/trace/impl/motrace"
	"github.com/matrixorigin/matrixone/pkg/vm/engine"
	"github.com/matrixorigin/matrixone/pkg/vm/process"
)

type backExec struct {
	backSes *backSession
}

func (back *backExec) Close() {
	back.Clear()
	back.backSes.Close()
	back.backSes.Clear()
	back.backSes = nil
}

func (back *backExec) Exec(ctx context.Context, sql string) error {
	if ctx == nil {
		ctx = back.backSes.GetRequestContext()
	} else {
		back.backSes.SetRequestContext(ctx)
	}
	_, err := defines.GetAccountId(ctx)
	if err != nil {
		return err
	}

	// For determine this is a background sql.
	ctx = context.WithValue(ctx, defines.BgKey{}, true)
	back.backSes.requestCtx = ctx
	//logutil.Debugf("-->bh:%s", sql)
	v, err := back.backSes.GetGlobalVar("lower_case_table_names")
	if err != nil {
		return err
	}
	statements, err := mysql.Parse(ctx, sql, v.(int64), 0)
	if err != nil {
		return err
	}
	defer func() {
		for _, stmt := range statements {
			stmt.Free()
		}
	}()
	if len(statements) > 1 {
		return moerr.NewInternalError(ctx, "Exec() can run one statement at one time. but get '%d' statements now, sql = %s", len(statements), sql)
	}
	//share txn can not run transaction statement
	if back.backSes.GetTxnHandler().IsShareTxn() {
		for _, stmt := range statements {
			switch stmt.(type) {
			case *tree.BeginTransaction, *tree.CommitTransaction, *tree.RollbackTransaction:
				return moerr.NewInternalError(ctx, "Exec() can not run transaction statement in share transaction, sql = %s", sql)
			}
		}
	}
	return doComQueryInBack(ctx, back.backSes, &UserInput{sql: sql})
}

func (back *backExec) ExecStmt(ctx context.Context, statement tree.Statement) error {
	return nil
}

func (back *backExec) GetExecResultSet() []interface{} {
	mrs := back.backSes.allResultSet
	ret := make([]interface{}, len(mrs))
	for i, mr := range mrs {
		ret[i] = mr
	}
	return ret
}

func (back *backExec) ClearExecResultSet() {
	back.backSes.allResultSet = nil
}

func (back *backExec) GetExecResultBatches() []*batch.Batch {
	return back.backSes.resultBatches
}

func (back *backExec) ClearExecResultBatches() {
	back.backSes.resultBatches = nil
}

func (back *backExec) Clear() {
	back.backSes.Clear()
}

// execute query
func doComQueryInBack(requestCtx context.Context,
	backSes *backSession,
	input *UserInput) (retErr error) {
	backSes.SetSql(input.getSql())
	//the ses.GetUserName returns the user_name with the account_name.
	//here,we only need the user_name.
	userNameOnly := rootName
	proc := process.New(
		requestCtx,
		backSes.pool,
		getGlobalPu().TxnClient,
		nil,
		getGlobalPu().FileService,
		getGlobalPu().LockService,
		getGlobalPu().QueryClient,
		getGlobalPu().HAKeeperClient,
		getGlobalPu().UdfService,
		globalAicm)
	proc.Id = backSes.getNextProcessId()
	proc.Lim.Size = getGlobalPu().SV.ProcessLimitationSize
	proc.Lim.BatchRows = getGlobalPu().SV.ProcessLimitationBatchRows
	proc.Lim.MaxMsgSize = getGlobalPu().SV.MaxMessageSize
	proc.Lim.PartitionRows = getGlobalPu().SV.ProcessLimitationPartitionRows
	proc.SessionInfo = process.SessionInfo{
		User:          backSes.proto.GetUserName(),
		Host:          getGlobalPu().SV.Host,
		Database:      backSes.proto.GetDatabaseName(),
		Version:       makeServerVersion(getGlobalPu(), serverVersion.Load().(string)),
		TimeZone:      backSes.GetTimeZone(),
		StorageEngine: getGlobalPu().StorageEngine,
		Buf:           backSes.buf,
	}
	proc.SetStmtProfile(&backSes.stmtProfile)
	proc.SetResolveVariableFunc(backSes.txnCompileCtx.ResolveVariable)
	//!!!does not init sequence in the background exec
	if backSes.tenant != nil {
		proc.SessionInfo.Account = backSes.tenant.GetTenant()
		proc.SessionInfo.AccountId = backSes.tenant.GetTenantID()
		proc.SessionInfo.Role = backSes.tenant.GetDefaultRole()
		proc.SessionInfo.RoleId = backSes.tenant.GetDefaultRoleID()
		proc.SessionInfo.UserId = backSes.tenant.GetUserID()

		if len(backSes.tenant.GetVersion()) != 0 {
			proc.SessionInfo.Version = backSes.tenant.GetVersion()
		}
		userNameOnly = backSes.tenant.GetUser()
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
	requestCtx, span = trace.Start(requestCtx, "backExec.doComQueryInBack",
		trace.WithKind(trace.SpanKindStatement))
	defer span.End()

	proc.SessionInfo.User = userNameOnly
	backSes.txnCompileCtx.SetProcess(proc)

	cws, err := GetComputationWrapperInBack(backSes.proto.GetDatabaseName(),
		input,
		backSes.proto.GetUserName(),
		getGlobalPu().StorageEngine,
		proc, backSes)

	if err != nil {
		retErr = err
		if _, ok := err.(*moerr.Error); !ok {
			retErr = moerr.NewParseError(requestCtx, err.Error())
		}
		return retErr
	}

	defer func() {
		backSes.SetMysqlResultSet(nil)
	}()

	defer func() {
		for i := 0; i < len(cws); i++ {
			if cwft, ok := cws[i].(*TxnComputationWrapper); ok {
				cwft.Free()
			}
			cws[i].Clear()
		}
	}()

	sqlRecord := parsers.HandleSqlForRecord(input.getSql())

	for i, cw := range cws {
		backSes.mrs = &MysqlResultSet{}
		stmt := cw.GetAst()
		tenant := backSes.GetTenantNameWithStmt(stmt)

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
		if backSes.GetTxnHandler().InActiveTransaction() {
			err = canExecuteStatementInUncommittedTransaction(requestCtx, backSes, stmt)
			if err != nil {
				return err
			}
		}

		execCtx := ExecCtx{
			stmt:       stmt,
			isLastStmt: i >= len(cws)-1,
			tenant:     tenant,
			userName:   userNameOnly,
			sqlOfStmt:  sqlRecord[i],
			cw:         cw,
			proc:       proc,
		}
		err = executeStmtInBackWithTxn(requestCtx, backSes, &execCtx)
		if err != nil {
			return err
		}
	} // end of for

	return nil
}

func executeStmtInBackWithTxn(requestCtx context.Context,
	backSes *backSession,
	execCtx *ExecCtx,
) (err error) {
	// defer transaction state management.
	defer func() {
		err = finishTxnFunc(requestCtx, backSes, err, execCtx)
	}()

	// statement management
	_, txnOp, err := backSes.GetTxnHandler().GetTxnOperator()
	if err != nil {
		return err
	}

	//non derived statement
	if txnOp != nil && !backSes.IsDerivedStmt() {
		//startStatement has been called
		ok, _ := backSes.GetTxnHandler().calledStartStmt()
		if !ok {
			txnOp.GetWorkspace().StartStatement()
			backSes.GetTxnHandler().enableStartStmt(txnOp.Txn().ID)
		}
	}

	// defer Start/End Statement management, called after finishTxnFunc()
	defer func() {
		// move finishTxnFunc() out to another defer so that if finishTxnFunc
		// paniced, the following is still called.
		var err3 error
		_, txnOp, err3 = backSes.GetTxnHandler().GetTxnOperator()
		if err3 != nil {
			logError(backSes, backSes.GetDebugString(), err3.Error())
			return
		}
		//non derived statement
		if txnOp != nil && !backSes.IsDerivedStmt() {
			//startStatement has been called
			ok, id := backSes.GetTxnHandler().calledStartStmt()
			if ok && bytes.Equal(txnOp.Txn().ID, id) {
				txnOp.GetWorkspace().EndStatement()
			}
		}
		backSes.GetTxnHandler().disableStartStmt()
	}()
	return executeStmtInBack(requestCtx, backSes, execCtx)
}

func executeStmtInBack(requestCtx context.Context,
	backSes *backSession,
	execCtx *ExecCtx,
) (err error) {
	var cmpBegin time.Time
	var ret interface{}

	switch execCtx.stmt.StmtKind().HandleType() {
	case tree.EXEC_IN_FRONTEND:
		return execInFrontendInBack(requestCtx, backSes, execCtx)
	case tree.EXEC_IN_ENGINE:
	}

	switch st := execCtx.stmt.(type) {
	case *tree.CreateDatabase:
		err = inputNameIsInvalid(execCtx.proc.Ctx, string(st.Name))
		if err != nil {
			return
		}
		if st.SubscriptionOption != nil && backSes.tenant != nil && !backSes.tenant.IsAdminRole() {
			err = moerr.NewInternalError(execCtx.proc.Ctx, "only admin can create subscription")
			return
		}
		st.Sql = execCtx.sqlOfStmt
	case *tree.DropDatabase:
		err = inputNameIsInvalid(execCtx.proc.Ctx, string(st.Name))
		if err != nil {
			return
		}
		// if the droped database is the same as the one in use, database must be reseted to empty.
		if string(st.Name) == backSes.GetDatabaseName() {
			backSes.SetDatabaseName("")
		}
	}

	cmpBegin = time.Now()

	if ret, err = execCtx.cw.Compile(requestCtx, backSes.GetOutputCallback()); err != nil {
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
		return execInFrontendInBack(requestCtx, backSes, execCtx)
	case tree.EXEC_IN_ENGINE:

	}

	execCtx.runner = ret.(ComputationRunner)

	// only log if build time is longer than 1s
	if time.Since(cmpBegin) > time.Second {
		logInfo(backSes, backSes.GetDebugString(), fmt.Sprintf("time of Exec.Build : %s", time.Since(cmpBegin).String()))
	}

	StmtKind := execCtx.stmt.StmtKind().OutputType()
	switch StmtKind {
	case tree.OUTPUT_RESULT_ROW:
		err = executeResultRowStmtInBack(requestCtx, backSes, execCtx)
		if err != nil {
			return err
		}
	case tree.OUTPUT_STATUS:
		err = executeStatusStmtInBack(requestCtx, backSes, execCtx)
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

var GetComputationWrapperInBack = func(db string, input *UserInput, user string, eng engine.Engine, proc *process.Process, ses FeSession) ([]ComputationWrapper, error) {
	var cw []ComputationWrapper = nil

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

var NewBackgroundExec = func(
	reqCtx context.Context,
	upstream FeSession,
	mp *mpool.MPool) BackgroundExec {
	txnHandler := InitTxnHandler(getGlobalPu().StorageEngine, nil, nil)
	backSes := &backSession{
		requestCtx: reqCtx,
		connectCtx: upstream.GetConnectContext(),
		feSessionImpl: feSessionImpl{
			pool:           mp,
			proto:          &FakeProtocol{},
			buf:            buffer.New(),
			stmtProfile:    process.StmtProfile{},
			tenant:         nil,
			txnHandler:     txnHandler,
			txnCompileCtx:  InitTxnCompilerContext(txnHandler, ""),
			mrs:            nil,
			outputCallback: fakeDataSetFetcher2,
			allResultSet:   nil,
			resultBatches:  nil,
			derivedStmt:    false,
			gSysVars:       GSysVariables,
			label:          make(map[string]string),
			timeZone:       time.Local,
		},
	}
	backSes.uuid, _ = uuid.NewV7()
	backSes.GetTxnCompileCtx().SetSession(backSes)
	backSes.GetTxnHandler().SetSession(backSes)
	bh := &backExec{
		backSes: backSes,
	}

	return bh
}

// executeSQLInBackgroundSession executes the sql in an independent session and transaction.
// It sends nothing to the client.
func executeSQLInBackgroundSession(reqCtx context.Context, upstream *Session, mp *mpool.MPool, sql string) ([]ExecResult, error) {
	bh := NewBackgroundExec(reqCtx, upstream, mp)
	defer bh.Close()
	logutil.Debugf("background exec sql:%v", sql)
	err := bh.Exec(reqCtx, sql)
	logutil.Debugf("background exec sql done")
	if err != nil {
		return nil, err
	}
	return getResultSet(reqCtx, bh)
}

// executeStmtInSameSession executes the statement in the same session.
// To be clear, only for the select statement derived from the set_var statement
// in an independent transaction
func executeStmtInSameSession(ctx context.Context, ses *Session, stmt tree.Statement) error {
	switch stmt.(type) {
	case *tree.Select, *tree.ParenSelect:
	default:
		return moerr.NewInternalError(ctx, "executeStmtInSameSession can not run non select statement in the same session")
	}

	prevDB := ses.GetDatabaseName()
	prevOptionBits := ses.GetTxnHandler().GetOptionBits()
	prevServerStatus := ses.GetTxnHandler().GetServerStatus()
	//autocommit = on
	ses.GetTxnHandler().setAutocommitOn()
	//1. replace output callback by batchFetcher.
	// the result batch will be saved in the session.
	// you can get the result batch by calling GetResultBatches()
	ses.SetOutputCallback(batchFetcher)
	//2. replace protocol by FakeProtocol.
	// Any response yielded during running query will be dropped by the FakeProtocol.
	// The client will not receive any response from the FakeProtocol.
	prevProto := ses.ReplaceProtocol(&FakeProtocol{})
	// inherit database
	ses.SetDatabaseName(prevDB)
	proc := ses.GetTxnCompileCtx().GetProcess()
	//restore normal protocol and output callback
	defer func() {
		//@todo we need to improve: make one session, one proc, one txnOperator
		p := ses.GetTxnCompileCtx().GetProcess()
		p.FreeVectors()
		ses.GetTxnCompileCtx().SetProcess(proc)
		ses.GetTxnHandler().SetOptionBits(prevOptionBits)
		ses.GetTxnHandler().SetServerStatus(prevServerStatus)
		ses.SetOutputCallback(getDataFromPipeline)
		ses.ReplaceProtocol(prevProto)
	}()
	logDebug(ses, ses.GetDebugString(), "query trace(ExecStmtInSameSession)",
		logutil.ConnectionIdField(ses.GetConnectionID()))
	//3. execute the statement
	return doComQuery(ctx, ses, &UserInput{stmt: stmt})
}

// fakeDataSetFetcher2 gets the result set from the pipeline and save it in the session.
// It will not send the result to the client.
func fakeDataSetFetcher2(handle interface{}, dataSet *batch.Batch) error {
	if handle == nil || dataSet == nil {
		return nil
	}

	back := handle.(*backSession)
	oq := newFakeOutputQueue(back.mrs)
	err := fillResultSet(oq, dataSet, back)
	if err != nil {
		return err
	}
	back.SetMysqlResultSetOfBackgroundTask(back.mrs)
	return nil
}

func fillResultSet(oq outputPool, dataSet *batch.Batch, ses FeSession) error {
	n := dataSet.RowCount()
	for j := 0; j < n; j++ { //row index
		//needCopyBytes = true. we need to copy the bytes from the batch.Batch
		//to avoid the data being changed after the batch.Batch returned to the
		//pipeline.
		_, err := extractRowFromEveryVector(ses, dataSet, j, oq, true)
		if err != nil {
			return err
		}
	}
	return oq.flush()
}

// batchFetcher2 gets the result batches from the pipeline and save the origin batches in the session.
// It will not send the result to the client.
func batchFetcher2(handle interface{}, dataSet *batch.Batch) error {
	if handle == nil {
		return nil
	}
	back := handle.(*backSession)
	back.SaveResultSet()
	if dataSet == nil {
		return nil
	}
	return back.AppendResultBatch(dataSet)
}

// batchFetcher gets the result batches from the pipeline and save the origin batches in the session.
// It will not send the result to the client.
func batchFetcher(handle interface{}, dataSet *batch.Batch) error {
	if handle == nil {
		return nil
	}
	ses := handle.(*Session)
	ses.SaveResultSet()
	if dataSet == nil {
		return nil
	}
	return ses.AppendResultBatch(dataSet)
}

// getResultSet extracts the result set
func getResultSet(ctx context.Context, bh BackgroundExec) ([]ExecResult, error) {
	results := bh.GetExecResultSet()
	rsset := make([]ExecResult, len(results))
	for i, value := range results {
		if er, ok := value.(ExecResult); ok {
			rsset[i] = er
		} else {
			return nil, moerr.NewInternalError(ctx, "it is not the type of result set")
		}
	}
	return rsset, nil
}

type backSession struct {
	feSessionImpl
	requestCtx context.Context
	connectCtx context.Context
}

func (backSes *backSession) Close() {
	backSes.feSessionImpl.Close()
	backSes.requestCtx = nil
	backSes.connectCtx = nil
}

func (backSes *backSession) Clear() {
	backSes.feSessionImpl.Clear()
}

func (backSes *backSession) GetOutputCallback() func(*batch.Batch) error {
	return func(bat *batch.Batch) error {
		return backSes.outputCallback(backSes, bat)
	}
}

func (backSes *backSession) SetTStmt(stmt *motrace.StatementInfo) {

}
func (backSes *backSession) SendRows() int64 {
	return 0
}

func (backSes *backSession) GetTxnInfo() string {
	txnH := backSes.GetTxnHandler()
	if txnH == nil {
		return ""
	}
	_, txnOp, err := txnH.GetTxnOperator()
	if err != nil {
		return ""
	}
	if txnOp == nil {
		return ""
	}
	meta := txnOp.Txn()
	return meta.DebugString()
}

func (backSes *backSession) GetStmtInfo() *motrace.StatementInfo {
	return nil
}

func (backSes *backSession) getNextProcessId() string {
	/*
		temporary method:
		routineId + sqlCount
	*/
	routineId := backSes.GetMysqlProtocol().ConnectionID()
	return fmt.Sprintf("%d%d", routineId, backSes.GetSqlCount())
}

func (backSes *backSession) GetSqlCount() uint64 {
	return backSes.sqlCount
}

func (backSes *backSession) addSqlCount(a uint64) {
	backSes.sqlCount += a
}

func (backSes *backSession) cleanCache() {
}

func (backSes *backSession) GetUpstream() FeSession {
	return backSes.upstream
}

func (backSes *backSession) EnableInitTempEngine() {

}

func (backSes *backSession) SetTempEngine(ctx context.Context, te engine.Engine) error {
	return nil
}

func (backSes *backSession) SetTempTableStorage(getClock clock.Clock) (*metadata.TNService, error) {
	return nil, nil
}

func (backSes *backSession) getCNLabels() map[string]string {
	return backSes.label
}

func (backSes *backSession) SetData(i [][]interface{}) {

}

func (backSes *backSession) GetIsInternal() bool {
	return false
}

func (backSes *backSession) SetPlan(plan *plan.Plan) {
}

func (backSes *backSession) SetAccountId(u uint32) {
	backSes.accountId = u
}

func (backSes *backSession) GetRawBatchBackgroundExec(ctx context.Context) BackgroundExec {
	//TODO implement me
	panic("implement me")
}

func (backSes *backSession) SetRequestContext(ctx context.Context) {
	backSes.requestCtx = ctx
}

func (backSes *backSession) GetConnectionID() uint32 {
	return 0
}

func (backSes *backSession) SetMysqlResultSet(mrs *MysqlResultSet) {
	backSes.mrs = mrs
}

func (backSes *backSession) getQueryId(internal bool) []string {
	return nil
}

func (backSes *backSession) CopySeqToProc(proc *process.Process) {

}

func (backSes *backSession) GetStmtProfile() *process.StmtProfile {
	return &backSes.stmtProfile
}

func (backSes *backSession) GetBuffer() *buffer.Buffer {
	return backSes.buf
}

func (backSes *backSession) GetSqlHelper() *SqlHelper {
	return nil
}

func (backSes *backSession) GetProc() *process.Process {
	return nil
}

func (backSes *backSession) GetLastInsertID() uint64 {
	return 0
}

func (backSes *backSession) GetMemPool() *mpool.MPool {
	return backSes.pool
}

func (backSes *backSession) SetSql(sql string) {
	backSes.sql = sql
}

func (backSes *backSession) SetShowStmtType(statement ShowStatementType) {
}

func (backSes *backSession) RemovePrepareStmt(name string) {

}

func (backSes *backSession) CountPayload(i int) {

}

func (backSes *backSession) GetPrepareStmt(name string) (*PrepareStmt, error) {
	return nil, moerr.NewInternalError(backSes.requestCtx, "do not support prepare in background exec")
}

func (backSes *backSession) IsBackgroundSession() bool {
	return true
}

func (backSes *backSession) GetTxnCompileCtx() *TxnCompilerContext {
	return backSes.txnCompileCtx
}

func (backSes *backSession) GetCmd() CommandType {
	return COM_QUERY
}

func (backSes *backSession) SetNewResponse(category int, affectedRows uint64, cmd int, d interface{}, isLastStmt bool) *Response {
	return nil
}

func (backSes *backSession) GetMysqlResultSet() *MysqlResultSet {
	return backSes.mrs
}

func (backSes *backSession) GetTxnHandler() *TxnHandler {
	return backSes.txnHandler
}

func (backSes *backSession) GetMysqlProtocol() MysqlProtocol {
	return backSes.proto
}

func (backSes *backSession) updateLastCommitTS(lastCommitTS timestamp.Timestamp) {
	if lastCommitTS.Greater(backSes.lastCommitTS) {
		backSes.lastCommitTS = lastCommitTS
	}
	if backSes.upstream != nil {
		backSes.upstream.updateLastCommitTS(lastCommitTS)
	}
}

func (backSes *backSession) GetSqlOfStmt() string {
	return ""
}

func (backSes *backSession) GetStmtId() uuid.UUID {
	return [16]byte{}
}

func (backSes *backSession) GetTxnId() uuid.UUID {
	return backSes.stmtProfile.GetTxnId()
}

func (backSes *backSession) SetTxnId(id []byte) {
	backSes.stmtProfile.SetTxnId(id)
}

// GetTenantName return tenant name according to GetTenantInfo and stmt.
//
// With stmt = nil, should be only called in TxnHandler.NewTxn, TxnHandler.CommitTxn, TxnHandler.RollbackTxn
func (backSes *backSession) GetTenantNameWithStmt(stmt tree.Statement) string {
	tenant := sysAccountName
	if backSes.GetTenantInfo() != nil && (stmt == nil || !IsPrepareStatement(stmt)) {
		tenant = backSes.GetTenantInfo().GetTenant()
	}
	return tenant
}

func (backSes *backSession) GetTenantName() string {
	return backSes.GetTenantNameWithStmt(nil)
}

func (backSes *backSession) getLastCommitTS() timestamp.Timestamp {
	minTS := backSes.lastCommitTS
	if backSes.upstream != nil {
		v := backSes.upstream.getLastCommitTS()
		if v.Greater(minTS) {
			minTS = v
		}
	}
	return minTS
}

func (backSes *backSession) GetFromRealUser() bool {
	return false
}

func (backSes *backSession) GetDebugString() string {
	return ""
}

func (backSes *backSession) GetTempTableStorage() *memorystorage.Storage {
	return nil
}

func (backSes *backSession) IfInitedTempEngine() bool {
	return false
}

func (backSes *backSession) GetConnectContext() context.Context {
	return backSes.connectCtx
}

func (backSes *backSession) GetUserDefinedVar(name string) (SystemVariableType, *UserDefinedVar, error) {
	return nil, nil, moerr.NewInternalError(backSes.requestCtx, "do not support user defined var in background exec")
}

func (backSes *backSession) GetSessionVar(name string) (interface{}, error) {
	return nil, nil
}

func (backSes *backSession) getGlobalSystemVariableValue(name string) (interface{}, error) {
	return nil, moerr.NewInternalError(backSes.requestCtx, "do not support system variable in background exec")
}

func (backSes *backSession) GetBackgroundExec(ctx context.Context) BackgroundExec {
	return NewBackgroundExec(
		ctx,
		backSes,
		backSes.GetMemPool())
}

func (backSes *backSession) GetStorage() engine.Engine {
	return getGlobalPu().StorageEngine
}

func (backSes *backSession) GetTenantInfo() *TenantInfo {
	return backSes.tenant
}

func (backSes *backSession) GetAccountId() uint32 {
	return backSes.accountId
}

func (backSes *backSession) GetSql() string {
	return backSes.sql
}

func (backSes *backSession) GetUserName() string {
	return backSes.proto.GetUserName()
}

func (backSes *backSession) GetStatsCache() *plan2.StatsCache {
	return nil
}

func (backSes *backSession) GetRequestContext() context.Context {
	return backSes.requestCtx
}

func (backSes *backSession) GetTimeZone() *time.Location {
	return backSes.timeZone
}

func (backSes *backSession) IsDerivedStmt() bool {
	return backSes.derivedStmt
}

func (backSes *backSession) GetDatabaseName() string {
	return backSes.proto.GetDatabaseName()
}

func (backSes *backSession) SetDatabaseName(s string) {
	backSes.proto.SetDatabaseName(s)
	backSes.GetTxnCompileCtx().SetDatabase(s)
}

func (backSes *backSession) GetGlobalVar(name string) (interface{}, error) {
	if def, val, ok := backSes.gSysVars.GetGlobalSysVar(name); ok {
		if def.GetScope() == ScopeSession {
			//empty
			return nil, moerr.NewInternalError(backSes.requestCtx, errorSystemVariableSessionEmpty())
		}
		return val, nil
	}
	return nil, moerr.NewInternalError(backSes.requestCtx, errorSystemVariableDoesNotExist())
}

func (backSes *backSession) SetMysqlResultSetOfBackgroundTask(mrs *MysqlResultSet) {
	if len(backSes.allResultSet) == 0 {
		backSes.allResultSet = append(backSes.allResultSet, mrs)
	}
}

func (backSes *backSession) SaveResultSet() {
	if len(backSes.allResultSet) == 0 && backSes.mrs != nil {
		backSes.allResultSet = []*MysqlResultSet{backSes.mrs}
	}
}

func (backSes *backSession) AppendResultBatch(bat *batch.Batch) error {
	copied, err := bat.Dup(backSes.pool)
	if err != nil {
		return err
	}
	backSes.resultBatches = append(backSes.resultBatches, copied)
	return nil
}

func (backSes *backSession) ReplaceDerivedStmt(b bool) bool {
	prev := backSes.derivedStmt
	backSes.derivedStmt = b
	return prev
}

type SqlHelper struct {
	ses *Session
}

func (sh *SqlHelper) GetCompilerContext() any {
	return sh.ses.txnCompileCtx
}

func (sh *SqlHelper) GetSubscriptionMeta(dbName string) (*plan.SubscriptionMeta, error) {
	return sh.ses.txnCompileCtx.GetSubscriptionMeta(dbName, timestamp.Timestamp{})
}

// Made for sequence func. nextval, setval.
func (sh *SqlHelper) ExecSql(sql string) (ret []interface{}, err error) {
	var erArray []ExecResult

	ctx := sh.ses.GetRequestContext()
	/*
		if we run the transaction statement (BEGIN, ect) here , it creates an independent transaction.
		if we do not run the transaction statement (BEGIN, ect) here, it runs the sql in the share transaction
		and committed outside this function.
		!!!NOTE: wen can not execute the transaction statement(BEGIN,COMMIT,ROLLBACK,START TRANSACTION ect) here.
	*/
	bh := sh.ses.GetShareTxnBackgroundExec(ctx, false)
	defer bh.Close()

	bh.ClearExecResultSet()
	err = bh.Exec(ctx, sql)
	if err != nil {
		return nil, err
	}

	erArray, err = getResultSet(ctx, bh)
	if err != nil {
		return nil, err
	}

	if len(erArray) == 0 {
		return nil, nil
	}

	return erArray[0].(*MysqlResultSet).Data[0], nil
}
