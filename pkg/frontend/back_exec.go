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
	"context"
	"fmt"
	"strings"
	"time"

	"github.com/google/uuid"
	"go.uber.org/zap"
	"go.uber.org/zap/zapcore"

	"github.com/matrixorigin/matrixone/pkg/common/buffer"
	"github.com/matrixorigin/matrixone/pkg/common/log"
	"github.com/matrixorigin/matrixone/pkg/common/moerr"
	"github.com/matrixorigin/matrixone/pkg/common/mpool"
	"github.com/matrixorigin/matrixone/pkg/container/batch"
	"github.com/matrixorigin/matrixone/pkg/defines"
	"github.com/matrixorigin/matrixone/pkg/logutil"
	"github.com/matrixorigin/matrixone/pkg/pb/plan"
	"github.com/matrixorigin/matrixone/pkg/pb/timestamp"
	"github.com/matrixorigin/matrixone/pkg/sql/compile"
	"github.com/matrixorigin/matrixone/pkg/sql/parsers"
	"github.com/matrixorigin/matrixone/pkg/sql/parsers/dialect"
	"github.com/matrixorigin/matrixone/pkg/sql/parsers/dialect/mysql"
	"github.com/matrixorigin/matrixone/pkg/sql/parsers/tree"
	plan2 "github.com/matrixorigin/matrixone/pkg/sql/plan"
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
		return moerr.NewInternalError(context.Background(), "context is nil")
	}
	_, err := defines.GetAccountId(ctx)
	if err != nil {
		return err
	}

	// For determine this is a background sql.
	ctx = context.WithValue(ctx, defines.BgKey{}, true)
	//logutil.Debugf("-->bh:%s", sql)
	v, err := back.backSes.GetGlobalVar(ctx, "lower_case_table_names")
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
	execCtx := ExecCtx{
		reqCtx: ctx,
		ses:    back.backSes,
	}
	return doComQueryInBack(back.backSes, &execCtx, &UserInput{sql: sql})
}

func (back *backExec) ExecRestore(ctx context.Context, sql string, fromAccount uint32, toAccount uint32) error {
	if ctx == nil {
		return moerr.NewInternalError(context.Background(), "context is nil")
	}
	_, err := defines.GetAccountId(ctx)
	if err != nil {
		return err
	}

	// For determine this is a background sql.
	ctx = context.WithValue(ctx, defines.BgKey{}, true)
	//logutil.Debugf("-->bh:%s", sql)
	v, err := back.backSes.GetGlobalVar(ctx, "lower_case_table_names")
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

	userInput := &UserInput{
		sql:         sql,
		isRetstore:  true,
		fromAccount: fromAccount,
		toAccount:   toAccount,
	}

	execCtx := ExecCtx{
		reqCtx: ctx,
		ses:    back.backSes,
	}
	return doComQueryInBack(back.backSes, &execCtx, userInput)
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
func doComQueryInBack(backSes *backSession, execCtx *ExecCtx,
	input *UserInput) (retErr error) {
	//fmt.Fprintln(os.Stderr, "doComQueryInBack", input.getSql())
	//defer func() {
	//	if retErr != nil {
	//		fmt.Fprintln(os.Stderr, "doComQueryInBack", retErr)
	//	}
	//}()
	backSes.GetTxnCompileCtx().SetExecCtx(execCtx)
	backSes.SetSql(input.getSql())
	//the ses.GetUserName returns the user_name with the account_name.
	//here,we only need the user_name.
	userNameOnly := rootName
	proc := process.New(
		execCtx.reqCtx,
		backSes.pool,
		getGlobalPu().TxnClient,
		nil,
		getGlobalPu().FileService,
		getGlobalPu().LockService,
		getGlobalPu().QueryClient,
		getGlobalPu().HAKeeperClient,
		getGlobalPu().UdfService,
		getGlobalAic())
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
		accountId, retErr = defines.GetAccountId(execCtx.reqCtx)
		if retErr != nil {
			return retErr
		}
		proc.SessionInfo.AccountId = accountId
		proc.SessionInfo.UserId = defines.GetUserId(execCtx.reqCtx)
		proc.SessionInfo.RoleId = defines.GetRoleId(execCtx.reqCtx)
	}
	var span trace.Span
	execCtx.reqCtx, span = trace.Start(execCtx.reqCtx, "backExec.doComQueryInBack",
		trace.WithKind(trace.SpanKindStatement))
	defer span.End()

	proc.SessionInfo.User = userNameOnly
	cws, err := GetComputationWrapperInBack(execCtx, backSes.proto.GetDatabaseName(),
		input,
		backSes.proto.GetUserName(),
		getGlobalPu().StorageEngine,
		proc, backSes)

	if err != nil {
		retErr = err
		if _, ok := err.(*moerr.Error); !ok {
			retErr = moerr.NewParseError(execCtx.reqCtx, err.Error())
		}
		return retErr
	}

	defer func() {
		backSes.SetMysqlResultSet(nil)
	}()

	defer func() {
		for i := 0; i < len(cws); i++ {
			cws[i].Free()
		}
	}()

	sqlRecord := parsers.HandleSqlForRecord(input.getSql())

	for i, cw := range cws {
		backSes.mrs = &MysqlResultSet{}
		stmt := cw.GetAst()

		if insertStmt, ok := stmt.(*tree.Insert); ok && input.isRetstore {
			insertStmt.IsRestore = true
			insertStmt.FromDataTenantID = input.fromAccount
			insertStmt.DestTableTenantID = input.toAccount
		}

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
		if backSes.GetTxnHandler().InActiveTxn() {
			err = canExecuteStatementInUncommittedTransaction(execCtx.reqCtx, backSes, stmt)
			if err != nil {
				return err
			}
		}

		execCtx.stmt = stmt
		execCtx.isLastStmt = i >= len(cws)-1
		execCtx.tenant = tenant
		execCtx.userName = userNameOnly
		execCtx.sqlOfStmt = sqlRecord[i]
		execCtx.cw = cw
		execCtx.proc = proc
		execCtx.ses = backSes
		execCtx.cws = cws
		err = executeStmtWithTxn(backSes, execCtx)
		if err != nil {
			return err
		}
	} // end of for

	return nil
}

func executeStmtInBack(backSes *backSession,
	execCtx *ExecCtx,
) (err error) {
	var cmpBegin time.Time
	var ret interface{}

	switch execCtx.stmt.StmtKind().ExecLocation() {
	case tree.EXEC_IN_FRONTEND:
		return execInFrontendInBack(backSes, execCtx)
	case tree.EXEC_IN_ENGINE:
	}

	switch st := execCtx.stmt.(type) {
	case *tree.CreateDatabase:
		err = inputNameIsInvalid(execCtx.reqCtx, string(st.Name))
		if err != nil {
			return
		}
		if st.SubscriptionOption != nil && backSes.tenant != nil && !backSes.tenant.IsAdminRole() {
			err = moerr.NewInternalError(execCtx.reqCtx, "only admin can create subscription")
			return
		}
		st.Sql = execCtx.sqlOfStmt
	case *tree.DropDatabase:
		err = inputNameIsInvalid(execCtx.reqCtx, string(st.Name))
		if err != nil {
			return
		}
		// if the droped database is the same as the one in use, database must be reseted to empty.
		if string(st.Name) == backSes.GetDatabaseName() {
			backSes.SetDatabaseName("")
		}
	}

	cmpBegin = time.Now()

	if ret, err = execCtx.cw.Compile(execCtx, backSes.GetOutputCallback(execCtx)); err != nil {
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
	switch execCtx.stmt.StmtKind().ExecLocation() {
	case tree.EXEC_IN_FRONTEND:
		return execInFrontendInBack(backSes, execCtx)
	case tree.EXEC_IN_ENGINE:

	}

	execCtx.runner = ret.(ComputationRunner)

	// only log if build time is longer than 1s
	if time.Since(cmpBegin) > time.Second {
		backSes.Infof(execCtx.reqCtx, "time of Exec.Build : %s", time.Since(cmpBegin).String())
	}

	StmtKind := execCtx.stmt.StmtKind().OutputType()
	switch StmtKind {
	case tree.OUTPUT_RESULT_ROW:
		err = executeResultRowStmtInBack(backSes, execCtx)
		if err != nil {
			return err
		}
	case tree.OUTPUT_STATUS:
		err = executeStatusStmtInBack(backSes, execCtx)
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
			return moerr.NewInternalError(execCtx.reqCtx, "need set result type for %s", execCtx.sqlOfStmt)
		}
	}

	return
}

var GetComputationWrapperInBack = func(exeCtx *ExecCtx, db string, input *UserInput, user string, eng engine.Engine, proc *process.Process, ses FeSession) ([]ComputationWrapper, error) {
	var cw []ComputationWrapper = nil

	var stmts []tree.Statement = nil
	var cmdFieldStmt *InternalCmdFieldList
	var err error
	// if the input is an option ast, we should use it directly
	if input.getStmt() != nil {
		stmts = append(stmts, input.getStmt())
	} else if isCmdFieldListSql(input.getSql()) {
		cmdFieldStmt, err = parseCmdFieldList(exeCtx.reqCtx, input.getSql())
		if err != nil {
			return nil, err
		}
		stmts = append(stmts, cmdFieldStmt)
	} else {
		var v interface{}
		var origin interface{}
		v, err = ses.GetGlobalVar(exeCtx.reqCtx, "lower_case_table_names")
		if err != nil {
			v = int64(1)
		}
		origin, err = ses.GetGlobalVar(exeCtx.reqCtx, "keep_user_target_list_in_result")
		if err != nil {
			origin = int64(0)
		}
		stmts, err = parsers.Parse(exeCtx.reqCtx, dialect.MYSQL, input.getSql(), v.(int64), origin.(int64))
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
	txnHandler := InitTxnHandler(getGlobalPu().StorageEngine, upstream.GetTxnHandler().GetConnCtx(), nil)
	backSes := &backSession{
		feSessionImpl: feSessionImpl{
			pool:           mp,
			proto:          &FakeProtocol{},
			buf:            buffer.New(),
			stmtProfile:    process.StmtProfile{},
			tenant:         nil,
			txnHandler:     txnHandler,
			txnCompileCtx:  InitTxnCompilerContext(""),
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
	if up, ok := upstream.(*Session); ok {
		backSes.upstream = up
	}
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
	upstream.Debugf(reqCtx, "background exec sql:%v", sql)
	err := bh.Exec(reqCtx, sql)
	upstream.Debug(reqCtx, "background exec sql done")
	if err != nil {
		return nil, err
	}
	return getResultSet(reqCtx, bh)
}

// executeStmtInSameSession executes the statement in the same session.
// To be clear, only for the select statement derived from the set_var statement
// in an independent transaction
func executeStmtInSameSession(ctx context.Context, ses *Session, execCtx *ExecCtx, stmt tree.Statement) error {
	switch stmt.(type) {
	case *tree.Select, *tree.ParenSelect:
	default:
		return moerr.NewInternalError(ctx, "executeStmtInSameSession can not run non select statement in the same session")
	}

	if ses.GetTxnHandler() == nil {
		panic("need txn handler 3")
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
	//3. replace the derived stmt
	prevDerivedStmt := ses.ReplaceDerivedStmt(true)
	// inherit database
	ses.SetDatabaseName(prevDB)
	proc := ses.GetTxnCompileCtx().GetProcess()
	//restore normal protocol and output callback
	defer func() {
		ses.ReplaceDerivedStmt(prevDerivedStmt)
		//@todo we need to improve: make one session, one proc, one txnOperator
		p := ses.GetTxnCompileCtx().GetProcess()
		p.FreeVectors()
		execCtx.proc = proc
		ses.GetTxnHandler().SetOptionBits(prevOptionBits)
		ses.GetTxnHandler().SetServerStatus(prevServerStatus)
		ses.SetOutputCallback(getDataFromPipeline)
		ses.ReplaceProtocol(prevProto)
		if ses.GetTxnHandler() == nil {
			panic("need txn handler 4")
		}
	}()
	ses.Debug(ctx, "query trace(ExecStmtInSameSession)",
		logutil.ConnectionIdField(ses.GetConnectionID()))
	//3. execute the statement
	return doComQuery(ses, execCtx, &UserInput{stmt: stmt})
}

// fakeDataSetFetcher2 gets the result set from the pipeline and save it in the session.
// It will not send the result to the client.
func fakeDataSetFetcher2(handle FeSession, execCtx *ExecCtx, dataSet *batch.Batch) error {
	if handle == nil || dataSet == nil {
		return nil
	}

	back := handle.(*backSession)
	oq := newFakeOutputQueue(back.mrs)
	err := fillResultSet(execCtx.reqCtx, oq, dataSet, back)
	if err != nil {
		return err
	}
	back.SetMysqlResultSetOfBackgroundTask(back.mrs)
	return nil
}

func fillResultSet(ctx context.Context, oq outputPool, dataSet *batch.Batch, ses FeSession) error {
	n := dataSet.RowCount()
	for j := 0; j < n; j++ { //row index
		//needCopyBytes = true. we need to copy the bytes from the batch.Batch
		//to avoid the data being changed after the batch.Batch returned to the
		//pipeline.
		_, err := extractRowFromEveryVector(ctx, ses, dataSet, j, oq, true)
		if err != nil {
			return err
		}
	}
	return oq.flush()
}

// batchFetcher2 gets the result batches from the pipeline and save the origin batches in the session.
// It will not send the result to the client.
func batchFetcher2(handle FeSession, _ *ExecCtx, dataSet *batch.Batch) error {
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
func batchFetcher(handle FeSession, _ *ExecCtx, dataSet *batch.Batch) error {
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
}

func (backSes *backSession) getCachedPlan(sql string) *cachedPlan {
	return nil
}

func (backSes *backSession) Close() {
	backSes.feSessionImpl.Close()
	backSes.upstream = nil
}

func (backSes *backSession) Clear() {
	backSes.feSessionImpl.Clear()
}

func (backSes *backSession) GetOutputCallback(execCtx *ExecCtx) func(*batch.Batch) error {
	return func(bat *batch.Batch) error {
		return backSes.outputCallback(backSes, execCtx, bat)
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
	txnOp := txnH.GetTxn()
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

func (backSes *backSession) cleanCache() {
}

func (backSes *backSession) GetUpstream() FeSession {
	return backSes.upstream
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

func (backSes *backSession) GetRawBatchBackgroundExec(ctx context.Context) BackgroundExec {
	//TODO implement me
	panic("implement me")
}

func (backSes *backSession) GetConnectionID() uint32 {
	return 0
}

func (backSes *backSession) getQueryId(internal bool) []string {
	return nil
}

func (backSes *backSession) CopySeqToProc(proc *process.Process) {

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

func (backSes *backSession) SetShowStmtType(statement ShowStatementType) {
}

func (backSes *backSession) RemovePrepareStmt(name string) {

}

func (backSes *backSession) CountPayload(i int) {

}

func (backSes *backSession) GetPrepareStmt(ctx context.Context, name string) (*PrepareStmt, error) {
	return nil, moerr.NewInternalError(ctx, "do not support prepare in background exec")
}

func (backSes *backSession) IsBackgroundSession() bool {
	return true
}

func (backSes *backSession) GetCmd() CommandType {
	return COM_QUERY
}

func (backSes *backSession) SetNewResponse(category int, affectedRows uint64, cmd int, d interface{}, isLastStmt bool) *Response {
	return nil
}

func (backSes *backSession) GetSqlOfStmt() string {
	return ""
}

func (backSes *backSession) GetStmtId() uuid.UUID {
	return [16]byte{}
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

func (backSes *backSession) GetFromRealUser() bool {
	return false
}

func (backSes *backSession) GetDebugString() string {
	if backSes.upstream != nil {
		return backSes.upstream.GetDebugString()
	}
	return ""
}

func (backSes *backSession) GetUserDefinedVar(name string) (SystemVariableType, *UserDefinedVar, error) {
	return nil, nil, moerr.NewInternalError(context.Background(), "do not support user defined var in background exec")
}

func (backSes *backSession) GetSessionVar(ctx context.Context, name string) (interface{}, error) {
	switch strings.ToLower(name) {
	case "autocommit":
		return true, nil
	}
	return nil, nil
}

func (backSes *backSession) GetGlobalSystemVariableValue(ctx context.Context, name string) (interface{}, error) {
	return nil, moerr.NewInternalError(ctx, "do not support system variable in background exec")
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

func (backSes *backSession) GetStatsCache() *plan2.StatsCache {
	return nil
}

func (backSes *backSession) GetGlobalVar(ctx context.Context, name string) (interface{}, error) {
	if def, val, ok := backSes.gSysVars.GetGlobalSysVar(name); ok {
		if def.GetScope() == ScopeSession {
			//empty
			return nil, moerr.NewInternalError(ctx, errorSystemVariableSessionEmpty())
		}
		return val, nil
	}
	return nil, moerr.NewInternalError(ctx, errorSystemVariableDoesNotExist())
}

func (backSes *backSession) GetSessId() uuid.UUID {
	return uuid.UUID(backSes.GetUUID())
}

func (backSes *backSession) GetLogLevel() zapcore.Level {
	if backSes.upstream == nil {
		config := logutil.GetDefaultConfig()
		return config.GetLevel().Level()
	}
	return backSes.upstream.GetLogLevel()
}

func (backSes *backSession) GetLogger() SessionLogger {
	return backSes
}

func (backSes *backSession) getMOLogger() *log.MOLogger {
	if backSes.upstream == nil {
		return getLogger()
	} else {
		return backSes.upstream.logger
	}
}

func (backSes *backSession) log(ctx context.Context, level zapcore.Level, msg string, fields ...zap.Field) {
	logger := backSes.getMOLogger()
	if logger.Enabled(level) {
		fields = append(fields, zap.String("session_info", backSes.GetDebugString()), zap.Bool("background", true))
		fields = appendSessionField(fields, backSes)
		fields = appendTraceField(fields, ctx)
		logger.Log(msg, log.DefaultLogOptions().WithLevel(level).AddCallerSkip(2), fields...)
	}
}

func (backSes *backSession) logf(ctx context.Context, level zapcore.Level, msg string, args ...any) {
	logger := backSes.getMOLogger()
	if logger.Enabled(level) {
		fields := make([]zap.Field, 0, 5)
		fields = append(fields, zap.String("session_info", backSes.GetDebugString()), zap.Bool("background", true))
		fields = appendSessionField(fields, backSes)
		fields = appendTraceField(fields, ctx)
		logger.Log(fmt.Sprintf(msg, args...), log.DefaultLogOptions().WithLevel(level).AddCallerSkip(2), fields...)
	}
}

func (backSes *backSession) Info(ctx context.Context, msg string, fields ...zap.Field) {
	backSes.log(ctx, zap.InfoLevel, msg, fields...)
}

func (backSes *backSession) Error(ctx context.Context, msg string, fields ...zap.Field) {
	backSes.log(ctx, zap.ErrorLevel, msg, fields...)
}

func (backSes *backSession) Warn(ctx context.Context, msg string, fields ...zap.Field) {
	backSes.log(ctx, zap.WarnLevel, msg, fields...)
}

func (backSes *backSession) Fatal(ctx context.Context, msg string, fields ...zap.Field) {
	backSes.log(ctx, zap.FatalLevel, msg, fields...)
}

func (backSes *backSession) Debug(ctx context.Context, msg string, fields ...zap.Field) {
	backSes.log(ctx, zap.DebugLevel, msg, fields...)
}

func (backSes *backSession) Infof(ctx context.Context, msg string, args ...any) {
	backSes.logf(ctx, zap.InfoLevel, msg, args...)
}

func (backSes *backSession) Errorf(ctx context.Context, msg string, args ...any) {
	backSes.logf(ctx, zap.ErrorLevel, msg, args...)
}

func (backSes *backSession) Warnf(ctx context.Context, msg string, args ...any) {
	backSes.logf(ctx, zap.WarnLevel, msg, args...)
}

func (backSes *backSession) Fatalf(ctx context.Context, msg string, args ...any) {
	backSes.logf(ctx, zap.FatalLevel, msg, args...)
}

func (backSes *backSession) Debugf(ctx context.Context, msg string, args ...any) {
	backSes.logf(ctx, zap.DebugLevel, msg, args...)
}

type SqlHelper struct {
	ses *Session
}

func (sh *SqlHelper) GetCompilerContext() any {
	return sh.ses.txnCompileCtx
}

func (sh *SqlHelper) GetSubscriptionMeta(dbName string) (*plan.SubscriptionMeta, error) {
	return sh.ses.txnCompileCtx.GetSubscriptionMeta(dbName, plan2.Snapshot{TS: &timestamp.Timestamp{}})
}

// Made for sequence func. nextval, setval.
func (sh *SqlHelper) ExecSql(sql string) (ret []interface{}, err error) {
	var erArray []ExecResult

	ctx := sh.ses.txnCompileCtx.execCtx.reqCtx
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
