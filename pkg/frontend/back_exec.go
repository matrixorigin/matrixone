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
	"regexp"
	"strconv"
	"strings"
	"time"

	"github.com/google/uuid"
	"go.uber.org/zap"
	"go.uber.org/zap/zapcore"

	"github.com/matrixorigin/matrixone/pkg/common/buffer"
	"github.com/matrixorigin/matrixone/pkg/common/log"
	"github.com/matrixorigin/matrixone/pkg/common/moerr"
	"github.com/matrixorigin/matrixone/pkg/container/batch"
	"github.com/matrixorigin/matrixone/pkg/defines"
	"github.com/matrixorigin/matrixone/pkg/logutil"
	"github.com/matrixorigin/matrixone/pkg/pb/plan"
	"github.com/matrixorigin/matrixone/pkg/perfcounter"
	"github.com/matrixorigin/matrixone/pkg/sql/compile"
	"github.com/matrixorigin/matrixone/pkg/sql/parsers"
	"github.com/matrixorigin/matrixone/pkg/sql/parsers/dialect/mysql"
	"github.com/matrixorigin/matrixone/pkg/sql/parsers/tree"
	plan2 "github.com/matrixorigin/matrixone/pkg/sql/plan"
	"github.com/matrixorigin/matrixone/pkg/util"
	"github.com/matrixorigin/matrixone/pkg/util/trace"
	"github.com/matrixorigin/matrixone/pkg/util/trace/impl/motrace"
	"github.com/matrixorigin/matrixone/pkg/util/trace/impl/motrace/statistic"
	"github.com/matrixorigin/matrixone/pkg/vm/engine"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/disttae"
	"github.com/matrixorigin/matrixone/pkg/vm/process"
)

type backExec struct {
	backSes    *backSession
	statsArray *statistic.StatsArray
}

func (back *backExec) init(ses FeSession, txnOp TxnOperator, db string, callBack outputCallBackFunc) {
	back.backSes = newBackSession(ses, txnOp, db, callBack)
	if back.statsArray != nil {
		back.statsArray.Reset()
	} else {
		back.statsArray = statistic.NewStatsArray()
	}
}

func (back *backExec) Service() string {
	return back.backSes.GetService()
}

func (back *backExec) Close() {
	if back == nil {
		return
	}
	back.Clear()
	back.backSes.Close()
	back.backSes = nil
}

// UpdateTxn updates the transaction operator without recreating the entire backExec.
// This allows reusing the backExec across transaction boundaries in autocommit mode.
func (back *backExec) UpdateTxn(txnOp TxnOperator) {
	back.backSes.GetTxnHandler().SetShareTxn(txnOp)
}

func (back *backExec) GetExecStatsArray() statistic.StatsArray {
	if back.statsArray != nil {
		return *back.statsArray
	} else {
		var stats statistic.StatsArray
		stats.Reset()
		return stats
	}
}

var restoreSqlRegx = regexp.MustCompile("MO_TS.*=")

func (back *backExec) Exec(ctx context.Context, sql string) (retErr error) {
	back.backSes.EnterFPrint(FPBackExecExec)
	defer back.backSes.ExitFPrint(FPBackExecExec)
	if ctx == nil {
		return moerr.NewInternalError(context.Background(), "context is nil")
	}
	ctx = perfcounter.AttachBackgroundExecutorKey(ctx)

	_, err := defines.GetAccountId(ctx)
	if err != nil {
		return err
	}

	// For determine this is a background sql.
	ctx = context.WithValue(ctx, defines.BgKey{}, true)

	//logutil.Debugf("-->bh:%s", sql)
	v, err := back.backSes.GetSessionSysVar("lower_case_table_names")
	if err != nil {
		v = int64(1)
	}

	statements, err := mysql.Parse(ctx, sql, v.(int64))
	if err != nil {
		return err
	}
	defer func() {
		for _, stmt := range statements {
			stmt.Free()
		}
	}()

	if len(statements) > 1 {
		return moerr.NewInternalErrorf(ctx, "Exec() can run one statement at one time. but get '%d' statements now, sql = %s", len(statements), sql)
	}
	// uncomment this to enable backExec export data to CSV file.
	//
	//if st, ok := statements[0].(*tree.Select); ok && st != nil && st.Ep != nil {
	//	back.backSes.ep = &ExportConfig{
	//		userConfig: st.Ep,
	//		service:    back.Service(),
	//	}
	//
	//	back.backSes.ep.init()
	//	back.backSes.ep.DefaultBufSize = getPu(back.backSes.GetService()).SV.ExportDataDefaultFlushSize
	//	initExportFileParam(back.backSes.ep, back.backSes.mrs)
	//	if err = openNewFile(ctx, back.backSes.ep, back.backSes.mrs); err != nil {
	//		return err
	//	}
	//
	//	defer func() {
	//		retErr = errors.Join(retErr, Close(back.backSes.ep))
	//	}()
	//}

	//share txn can not run transaction statement
	if back.backSes.GetTxnHandler().IsShareTxn() {
		for _, stmt := range statements {
			switch stmt.(type) {
			case *tree.BeginTransaction, *tree.CommitTransaction, *tree.RollbackTransaction, *tree.SavePoint, *tree.ReleaseSavePoint, *tree.RollbackToSavePoint:
				return moerr.NewInternalErrorf(ctx, "Exec() can not run transaction statement in share transaction, sql = %s", sql)
			}
		}
	}

	var isRestore bool
	if restoreSqlRegx.MatchString(sql) {
		switch statements[0].(type) {
		case *tree.Insert:
			isRestore = true
		case *tree.CloneTable:
			isRestore = true
		}
	}

	userInput := &UserInput{
		sql:       sql,
		isRestore: isRestore,
	}
	execCtx := ExecCtx{
		reqCtx: ctx,
		ses:    back.backSes,
	}
	defer execCtx.Close()

	tmpStatsArray := statistic.NewStatsArray()
	defer func() {
		back.statsArray.Add(tmpStatsArray)
	}()

	err = doComQueryInBack(back.backSes, tmpStatsArray, &execCtx, userInput) // statsInfo ,
	if err != nil {
		// if is restore and stmt is create view
		if back.backSes.GetRestore() {
			if _, ok := statements[0].(*tree.CreateView); ok {
				// reset restore flag
				// redo doComQueryInBack
				back.backSes.SetRestoreFail(true)
				defer func() {
					back.backSes.SetRestoreFail(false)
				}()
				err = doComQueryInBack(back.backSes, tmpStatsArray, &execCtx, userInput)
				if err != nil {
					return err
				}
			}
		}
	}
	return err
}

func (back *backExec) ExecRestore(ctx context.Context, sql string, opAccount uint32, toAccount uint32) error {
	back.backSes.EnterFPrint(FPBackExecRestore)
	defer back.backSes.ExitFPrint(FPBackExecRestore)
	if ctx == nil {
		return moerr.NewInternalError(context.Background(), "context is nil")
	}

	ctx = perfcounter.AttachBackgroundExecutorKey(ctx)

	_, err := defines.GetAccountId(ctx)
	if err != nil {
		return err
	}

	// For determine this is a background sql.
	ctx = context.WithValue(ctx, defines.BgKey{}, true)
	//logutil.Debugf("-->bh:%s", sql)
	v, err := back.backSes.GetSessionSysVar("lower_case_table_names")
	if err != nil {
		v = int64(1)
	}
	statements, err := mysql.Parse(ctx, sql, v.(int64))
	if err != nil {
		return err
	}
	defer func() {
		for _, stmt := range statements {
			stmt.Free()
		}
	}()
	if len(statements) > 1 {
		return moerr.NewInternalErrorf(ctx, "Exec() can run one statement at one time. but get '%d' statements now, sql = %s", len(statements), sql)
	}
	//share txn can not run transaction statement
	if back.backSes.GetTxnHandler().IsShareTxn() {
		for _, stmt := range statements {
			switch stmt.(type) {
			case *tree.BeginTransaction, *tree.CommitTransaction, *tree.RollbackTransaction, *tree.SavePoint, *tree.ReleaseSavePoint, *tree.RollbackToSavePoint:
				return moerr.NewInternalErrorf(ctx, "Exec() can not run transaction statement in share transaction, sql = %s", sql)
			}
		}
	}

	userInput := &UserInput{
		sql:           sql,
		isRestore:     true,
		opAccount:     opAccount,
		toAccount:     toAccount,
		isRestoreByTs: true,
	}

	execCtx := ExecCtx{
		reqCtx: ctx,
		ses:    back.backSes,
	}
	defer execCtx.Close()

	tmpStatsArray := statistic.NewStatsArray()
	defer func() {
		back.statsArray.Add(tmpStatsArray)
	}()
	return doComQueryInBack(back.backSes, tmpStatsArray, &execCtx, userInput)
}

func (back *backExec) ExecStmt(ctx context.Context, statement tree.Statement) error {
	if ctx == nil {
		return moerr.NewInternalError(context.Background(), "context is nil")
	}
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

func (back *backExec) SetRestore(b bool) {
	back.backSes.SetRestore(b)
}

func (back *backExec) ClearExecResultSet() {
	back.backSes.allResultSet = nil
}

func (back *backExec) GetExecResultBatches() []*batch.Batch {
	return back.backSes.resultBatches
}

func (back *backExec) ClearExecResultBatches() {
	for _, bat := range back.backSes.resultBatches {
		if bat != nil {
			bat.Clean(back.backSes.pool)
		}
	}
	back.backSes.resultBatches = nil
}

func (back *backExec) Clear() {
	if back == nil {
		return
	}
	back.statsArray = nil
	back.backSes.Clear()
}

// execute query
func doComQueryInBack(
	backSes *backSession,
	statsArr *statistic.StatsArray,
	execCtx *ExecCtx,
	input *UserInput,
) (retErr error) {
	backSes.EnterFPrint(FPDoComQueryInBack)
	defer backSes.ExitFPrint(FPDoComQueryInBack)
	backSes.GetTxnCompileCtx().SetExecCtx(execCtx)
	backSes.SetSql(input.getSql())

	// record start time of parsing
	beginInstant := time.Now()

	//the ses.GetUserName returns the user_name with the account_name.
	//here,we only need the user_name.
	userNameOnly := rootName
	service := backSes.GetService()
	pu := getPu(service)
	proc := process.NewTopProcess(
		execCtx.reqCtx,
		backSes.pool,
		pu.TxnClient,
		nil,
		pu.FileService,
		pu.LockService,
		pu.QueryClient,
		pu.HAKeeperClient,
		pu.UdfService,
		getAicm(service),
		getPu(backSes.GetService()).GetTaskService())
	proc.Base.Id = backSes.getNextProcessId()
	proc.Base.Lim.Size = pu.SV.ProcessLimitationSize
	proc.Base.Lim.BatchRows = pu.SV.ProcessLimitationBatchRows
	proc.Base.Lim.MaxMsgSize = pu.SV.MaxMessageSize
	proc.Base.Lim.PartitionRows = pu.SV.ProcessLimitationPartitionRows
	proc.Base.SessionInfo = process.SessionInfo{
		User:          backSes.respr.GetStr(USERNAME),
		Host:          pu.SV.Host,
		Database:      backSes.respr.GetStr(DBNAME),
		Version:       makeServerVersion(pu, serverVersion.Load().(string)),
		TimeZone:      backSes.GetTimeZone(),
		StorageEngine: pu.StorageEngine,
		Buf:           backSes.buf,
	}
	proc.SetStmtProfile(&backSes.stmtProfile)
	proc.SetResolveVariableFunc(backSes.txnCompileCtx.ResolveVariable)
	//!!!does not init sequence in the background exec
	if backSes.tenant != nil {
		proc.Base.SessionInfo.Account = backSes.tenant.GetTenant()
		proc.Base.SessionInfo.Role = backSes.tenant.GetDefaultRole()

		if len(backSes.tenant.GetVersion()) != 0 {
			proc.Base.SessionInfo.Version = backSes.tenant.GetVersion()
		}
		userNameOnly = backSes.tenant.GetUser()
	}

	var span trace.Span
	execCtx.reqCtx, span = trace.Start(execCtx.reqCtx, "backExec.doComQueryInBack",
		trace.WithKind(trace.SpanKindStatement))
	defer span.End()

	// Instantiate StatsInfo to track SQL resource statistics
	//statsInfo := new(statistic.StatsInfo)
	statsInfo := statistic.NewStatsInfo()
	statsInfo.ParseStage.ParseStartTime = beginInstant
	execCtx.reqCtx = statistic.ContextWithStatsInfo(execCtx.reqCtx, statsInfo)
	execCtx.input = input

	proc.Base.SessionInfo.User = userNameOnly
	cws, err := GetComputationWrapperInBack(
		execCtx, backSes.respr.GetStr(DBNAME),
		input,
		backSes.respr.GetStr(USERNAME),
		pu.StorageEngine,
		proc,
		backSes,
	)
	// SQL parsing completed, record end time
	ParseDuration := time.Since(beginInstant)

	if err != nil {
		statsInfo.ParseStage.ParseDuration = ParseDuration

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
		execCtx.stmt = nil
		execCtx.cw = nil
		execCtx.cws = nil
		execCtx.runner = nil
		for i := 0; i < len(cws); i++ {
			cws[i].Free()
		}
	}()

	sqlRecord := parsers.HandleSqlForRecord(input.getSql())

	for i, cw := range cws {
		backSes.mrs = &MysqlResultSet{}
		stmt := cw.GetAst()

		if input.isRestore {
			switch s := stmt.(type) {
			case *tree.Insert:
				s.IsRestore = true
				s.FromDataTenantID = input.opAccount
				s.IsRestoreByTs = s.IsRestoreByTs || input.isRestoreByTs

			case *tree.CloneTable:
				s.Sql = input.sql
				s.IsRestore = true
				s.FromAccount = input.opAccount
				s.ToAccountId = input.toAccount
				s.IsRestoreByTS = s.IsRestoreByTS || input.isRestoreByTs

				s.FlipStmtKind()
			}
		}

		statsInfo.Reset()
		//average parse duration
		statsInfo.ParseStage.ParseStartTime = beginInstant
		statsInfo.ParseStage.ParseDuration = time.Duration(ParseDuration.Nanoseconds() / int64(len(cws)))

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
		execCtx.txnOpt.Close()
		execCtx.stmt = stmt
		execCtx.isLastStmt = i >= len(cws)-1
		execCtx.tenant = tenant
		execCtx.userName = userNameOnly
		execCtx.sqlOfStmt = sqlRecord[i]
		execCtx.cw = cw
		execCtx.proc = proc
		execCtx.ses = backSes
		execCtx.cws = cws
		err = executeStmtWithTxn(backSes, statsArr, execCtx)
		if err != nil {
			return err
		}
	} // end of for

	return nil
}

func executeStmtInBack(backSes *backSession,
	statsArr *statistic.StatsArray,
	execCtx *ExecCtx,
) (err error) {
	execCtx.ses.EnterFPrint(FPExecStmtInBack)
	defer execCtx.ses.ExitFPrint(FPExecStmtInBack)
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

	execCtx.ses.EnterFPrint(FPExecStmtInBackBeforeCompile)
	defer execCtx.ses.ExitFPrint(FPExecStmtInBackBeforeCompile)

	err = disttae.CheckTxnIsValid(execCtx.ses.GetTxnHandler().GetTxn())
	if err != nil {
		return err
	}

	if ret, err = execCtx.cw.Compile(execCtx, backSes.GetOutputCallback(execCtx)); err != nil {
		return
	}

	defer func() {
		if c, ok := ret.(*compile.Compile); ok && statsArr != nil {
			statsByte := execCtx.cw.StatsCompositeSubStmtResource(execCtx.reqCtx)
			statsArr.Reset()
			statsArr.Add(&statsByte)
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
		if _, ok := execCtx.stmt.(*tree.Execute); !ok {
			return moerr.NewInternalErrorf(execCtx.reqCtx, "need set result type for %s", execCtx.sqlOfStmt)
		}
	}

	return
}

var GetComputationWrapperInBack = func(execCtx *ExecCtx, db string, input *UserInput, user string, eng engine.Engine, proc *process.Process, ses FeSession) ([]ComputationWrapper, error) {
	var cw []ComputationWrapper = nil

	var stmts []tree.Statement = nil
	var cmdFieldStmt *InternalCmdFieldList
	var err error
	// if the input is an option ast, we should use it directly
	if input.getStmt() != nil {
		stmts = append(stmts, input.getStmt())
	} else if isCmdFieldListSql(input.getSql()) {
		cmdFieldStmt, err = parseCmdFieldList(execCtx.reqCtx, input.getSql())
		if err != nil {
			return nil, err
		}
		stmts = append(stmts, cmdFieldStmt)
	} else {
		stmts, err = parseSql(execCtx, ses.GetMySQLParser())
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
	opts ...*BackgroundExecOption,
) BackgroundExec {
	// XXXSP
	var txnOp TxnOperator
	//
	// We do not compute and pass in txnOp, but when InitBackExec sees nil, it will pass to its upsteam.
	// txnOp = upstream.GetTxnHandler().GetTxn()
	//
	return upstream.InitBackExec(txnOp, "", backSesOutputCallback, opts...)
}

func backSesOutputCallback(handle FeSession, execCtx *ExecCtx, dataSet *batch.Batch, _ *perfcounter.CounterSet) error {
	if handle == nil || dataSet == nil {
		return nil
	}

	// uncomment this to enable backExec export data to CSV file.
	//back := handle.(*backSession)
	//if back.ep != nil {
	//	back.ep.Index.Add(1)
	//	copied, err := dataSet.Dup(execCtx.ses.GetMemPool())
	//	if err != nil {
	//		return err
	//	}
	//
	//	constructByte(execCtx.reqCtx, execCtx.ses, copied, back.ep.Index.Load(), back.ep.ByteChan, back.ep)
	//
	//	if err = exportDataFromBatchToCSVFile(back.ep); err != nil {
	//		execCtx.ses.Error(execCtx.reqCtx,
	//			"Error occurred while exporting to CSV file",
	//			zap.Error(err))
	//		return err
	//	}
	//
	//	return nil
	//}

	return fakeDataSetFetcher2(handle, execCtx, dataSet, nil)
}

var NewShareTxnBackgroundExec = func(ctx context.Context, ses FeSession, rawBatch bool) BackgroundExec {
	return ses.GetShareTxnBackgroundExec(ctx, rawBatch)
}

// ExeSqlInBgSes for mock stub
var ExeSqlInBgSes = func(reqCtx context.Context, bh BackgroundExec, sql string) ([]ExecResult, error) {
	return executeSQLInBackgroundSession(reqCtx, bh, sql)
}

// executeSQLInBackgroundSession executes the sql in an independent session and transaction.
// It sends nothing to the client.
func executeSQLInBackgroundSession(reqCtx context.Context, bh BackgroundExec, sql string) ([]ExecResult, error) {
	bh.ClearExecResultSet()
	err := bh.Exec(reqCtx, sql)
	if err != nil {
		return nil, err
	}

	return getResultSet(reqCtx, bh)
}

// executeStmtInSameSession executes the statement in the same session.
// To be clear, only for the select statement derived from the set_var statement
// in an independent transaction
func executeStmtInSameSession(ctx context.Context, ses *Session, execCtx *ExecCtx, stmt tree.Statement) error {
	ses.EnterFPrint(FPExecStmtInSameSession)
	defer ses.ExitFPrint(FPExecStmtInSameSession)
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
	// Any response yielded during running query will be dropped by the NullResp.
	// The client will not receive any response from the NullResp.
	prevProto := ses.ReplaceResponser(&NullResp{})
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
		p.Free()
		execCtx.proc = proc
		ses.GetTxnHandler().SetOptionBits(prevOptionBits)
		ses.GetTxnHandler().SetServerStatus(prevServerStatus)
		ses.SetOutputCallback(getDataFromPipeline)
		ses.ReplaceResponser(prevProto)
		if ses.GetTxnHandler() == nil {
			panic("need txn handler 4")
		}
	}()
	ses.Debug(ctx, "query trace(ExecStmtInSameSession)",
		logutil.ConnectionIdField(ses.GetConnectionID()))
	//3. execute the statement
	return doComQuery(ses, execCtx, &UserInput{stmt: stmt, isInternalInput: true})
}

// fakeDataSetFetcher2 gets the result set from the pipeline and save it in the session.
// It will not send the result to the client.
func fakeDataSetFetcher2(handle FeSession, execCtx *ExecCtx, dataSet *batch.Batch, _ *perfcounter.CounterSet) error {
	if handle == nil || dataSet == nil {
		return nil
	}

	back := handle.(*backSession)
	err := fillResultSet(execCtx.reqCtx, dataSet, back, back.mrs)
	if err != nil {
		return err
	}
	if err = back.AppendResultBatch(dataSet); err != nil {
		return err
	}
	back.SetMysqlResultSetOfBackgroundTask(back.mrs)
	return nil
}

func fillResultSet(ctx context.Context, dataSet *batch.Batch, ses FeSession, mrs *MysqlResultSet) error {
	n := dataSet.RowCount()
	for j := 0; j < n; j++ { //row index
		row := make([]any, mrs.GetColumnCount())
		err := extractRowFromEveryVector(ctx, ses, dataSet, j, row, false)
		if err != nil {
			return err
		}
		mrs.AddRow(row)
	}
	return nil
}

// batchFetcher2 gets the result batches from the pipeline and save the origin batches in the session.
// It will not send the result to the client.
func batchFetcher2(handle FeSession, _ *ExecCtx, dataSet *batch.Batch, _ *perfcounter.CounterSet) error {
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
func batchFetcher(handle FeSession, _ *ExecCtx, dataSet *batch.Batch, _ *perfcounter.CounterSet) error {
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
	//ep *ExportConfig
}

func newBackSession(ses FeSession, txnOp TxnOperator, db string, callBack outputCallBackFunc) *backSession {
	service := ses.GetService()
	var connCtx context.Context
	if ses.GetTxnHandler() != nil {
		connCtx = ses.GetTxnHandler().GetConnCtx()
	}
	txnHandler := InitTxnHandler(ses.GetService(), getPu(service).StorageEngine, connCtx, txnOp)
	backSes := &backSession{}
	backSes.initFeSes(ses, txnHandler, db, callBack)
	u, _ := util.FastUuid()
	backSes.uuid = uuid.UUID(u)
	return backSes
}

func (backSes *backSession) initFeSes(
	ses FeSession,
	txnHandler *TxnHandler,
	db string,
	callBack outputCallBackFunc) *backSession {
	backSes.pool = ses.GetMemPool()
	backSes.buf = buffer.New()
	backSes.stmtProfile = process.StmtProfile{}
	backSes.tenant = nil
	backSes.txnHandler = txnHandler
	backSes.txnCompileCtx = InitTxnCompilerContext(db)
	backSes.mrs = nil
	backSes.outputCallback = callBack
	backSes.allResultSet = nil
	backSes.resultBatches = nil
	backSes.derivedStmt = false
	backSes.label = make(map[string]string)
	backSes.timeZone = time.Local
	backSes.respr = defResper
	backSes.service = ses.GetService()
	return backSes
}

func (backSes *backSession) InitBackExec(txnOp TxnOperator, db string, callBack outputCallBackFunc, opts ...*BackgroundExecOption) BackgroundExec {
	if txnOp != nil {
		be := &backExec{}
		be.init(backSes, txnOp, db, callBack)
		return be
	} else if backSes.upstream != nil {
		// XXXSP
		// If we have an upstream, use it.
		return backSes.upstream.InitBackExec(nil, db, callBack, opts...)
	} else {
		panic("backSession does not support non-txn-shared backExec recursively")
	}
}

func (backSes *backSession) getCachedPlan(sql string) *cachedPlan {
	return nil
}

func (backSes *backSession) getCleanupContext() context.Context {
	if txnHandler := backSes.GetTxnHandler(); txnHandler != nil {
		if ctx := txnHandler.GetTxnCtx(); ctx != nil {
			return ctx
		}
	}
	return context.Background()
}

func (backSes *backSession) Close() {
	if backSes == nil {
		return
	}
	txnHandler := backSes.GetTxnHandler()
	if txnHandler != nil {
		tempExecCtx := ExecCtx{
			reqCtx: backSes.getCleanupContext(),
			ses:    backSes,
			txnOpt: FeTxnOption{byRollback: true},
		}
		defer tempExecCtx.Close()
		err := txnHandler.Rollback(&tempExecCtx)
		if err != nil {
			backSes.Error(tempExecCtx.reqCtx,
				"Failed to rollback txn in back session",
				zap.Error(err))
		}
	}

	//if the txn is not shared outside, we clean feSessionImpl.
	//reset else
	if txnHandler == nil || !txnHandler.IsShareTxn() {
		backSes.feSessionImpl.Close()
	} else {
		backSes.feSessionImpl.Reset()
	}
	backSes.upstream = nil
}

func (backSes *backSession) Clear() {
	if backSes == nil {
		return
	}
	backSes.feSessionImpl.Clear()
}

func (backSes *backSession) GetOutputCallback(execCtx *ExecCtx) func(*batch.Batch, *perfcounter.CounterSet) error {
	return func(bat *batch.Batch, crs *perfcounter.CounterSet) error {
		return backSes.outputCallback(backSes, execCtx, bat, crs)
	}
}

func (backSes *backSession) SetTStmt(stmt *motrace.StatementInfo) {

}
func (backSes *backSession) SendRows() int64 {
	return 0
}

func (backSes *backSession) GetConfig(ctx context.Context, varName, dbName, tblName string) (any, error) {
	return nil, moerr.NewInternalError(ctx, "do not support get config in background exec")
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
	routineId := backSes.respr.GetU32(CONNID)
	// Optimize: use strconv instead of fmt.Sprintf
	var buf [24]byte
	b := strconv.AppendUint(buf[:0], uint64(routineId), 10)
	b = strconv.AppendUint(b, backSes.GetSqlCount(), 10)
	return string(b)
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
	return backSes.fromRealUser
}

func (backSes *backSession) GetDebugString() string {
	if backSes.upstream != nil {
		return backSes.upstream.GetDebugString()
	}
	return "backSes without upstream"
}

func (backSes *backSession) GetShareTxnBackgroundExec(ctx context.Context, newRawBatch bool) BackgroundExec {
	backSes.EnterFPrint(FPGetShareTxnBackgroundExecInBackSession)
	defer backSes.ExitFPrint(FPGetShareTxnBackgroundExecInBackSession)
	var txnOp TxnOperator
	if backSes.GetTxnHandler() != nil {
		txnOp = backSes.GetTxnHandler().GetTxn()
	}

	be := backSes.InitBackExec(txnOp, "", backSesOutputCallback)
	//the derived statement execute in a shared transaction in background session
	be.(*backExec).backSes.ReplaceDerivedStmt(true)
	return be
}

func (backSes *backSession) GetUserDefinedVar(name string) (*UserDefinedVar, error) {
	if backSes.upstream == nil {
		return nil, moerr.NewInternalError(context.Background(), "do not support user defined var in background exec")
	}
	return backSes.upstream.GetUserDefinedVar(name)
}

func (backSes *backSession) SetUserDefinedVar(name string, value interface{}, sql string) error {
	if backSes.upstream == nil {
		return moerr.NewInternalError(context.Background(), "do not support set user defined var in background exec")
	}
	return backSes.upstream.SetUserDefinedVar(name, value, sql)
}

func (backSes *backSession) GetSessionSysVar(name string) (interface{}, error) {
	switch strings.ToLower(name) {
	case "autocommit":
		return true, nil
	case "lower_case_table_names":
		if backSes.GetRestore() && !backSes.GetRestoreFail() {
			return int64(0), nil
		}
		return int64(1), nil
	case "mo_table_stats.force_update", "mo_table_stats.use_old_impl", "mo_table_stats.reset_update_time":
		return backSes.upstream.GetSessionSysVar(name)
	}
	return nil, nil
}

func (backSes *backSession) GetBackgroundExec(ctx context.Context, opts ...*BackgroundExecOption) BackgroundExec {
	backSes.EnterFPrint(FPGetBackgroundExecInBackSession)
	defer backSes.ExitFPrint(FPGetBackgroundExecInBackSession)
	return NewBackgroundExec(ctx, backSes, opts...)
}

func (backSes *backSession) GetStorage() engine.Engine {
	return getPu(backSes.GetService()).StorageEngine
}

func (backSes *backSession) GetStatsCache() *plan2.StatsCache {
	return nil
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
	if backSes.upstream == nil || backSes.upstream.logger == nil {
		return getLogger(backSes.GetService())
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

func (backSes *backSession) LogDebug() bool {
	return backSes.getMOLogger().Enabled(zap.DebugLevel)
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

func (backSes *backSession) SetRestore(b bool) {
	backSes.isRestore = b
}

func (backSes *backSession) GetRestore() bool {
	return backSes.isRestore
}

func (backSes *backSession) SetRestoreFail(b bool) {
	backSes.isRestoreFail = b
}

func (backSes *backSession) GetRestoreFail() bool {
	return backSes.isRestoreFail
}

type SqlHelper struct {
	ses *Session
}

func (sh *SqlHelper) GetCompilerContext() any {
	return sh.ses.txnCompileCtx
}

func (sh *SqlHelper) GetSubscriptionMeta(dbName string) (*plan.SubscriptionMeta, error) {
	return sh.ses.txnCompileCtx.GetSubscriptionMeta(dbName, nil)
}

func (sh *SqlHelper) execSql(
	ctx context.Context,
	sql string,
) (ret [][]interface{}, err error) {
	var erArray []ExecResult

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

	return erArray[0].(*MysqlResultSet).Data, nil
}

// Made for sequence func. nextval, setval.
func (sh *SqlHelper) ExecSql(sql string) (ret [][]interface{}, err error) {
	ctx := sh.ses.txnCompileCtx.execCtx.reqCtx
	return sh.execSql(ctx, sql)
}

func (sh *SqlHelper) ExecSqlWithCtx(ctx context.Context, sql string) ([][]interface{}, error) {
	return sh.execSql(ctx, sql)
}

func (backSes *backSession) GetTempTable(dbName, alias string) (string, bool) {
	if backSes == nil || backSes.upstream == nil {
		return "", false
	}
	return backSes.upstream.GetTempTable(dbName, alias)
}

func (backSes *backSession) AddTempTable(dbName, alias, realName string) {
	if backSes == nil || backSes.upstream == nil {
		return
	}
	backSes.upstream.AddTempTable(dbName, alias, realName)
}

func (backSes *backSession) RemoveTempTableByRealName(realName string) {
	if backSes == nil || backSes.upstream == nil {
		return
	}
	backSes.upstream.RemoveTempTableByRealName(realName)
}

func (backSes *backSession) RemoveTempTable(dbName, alias string) {
	if backSes == nil || backSes.upstream == nil {
		return
	}
	backSes.upstream.RemoveTempTable(dbName, alias)
}

func (backSes *backSession) GetSqlModeNoAutoValueOnZero() (bool, bool) {
	if backSes == nil || backSes.upstream == nil {
		return false, false
	}
	return backSes.upstream.GetSqlModeNoAutoValueOnZero()
}
