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
	"fmt"
	"io"
	gotrace "runtime/trace"
	"time"

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

// ExecRequest the server execute the commands from the client following the mysql's routine
func ExecRequest(requestCtx context.Context, ses *Session, req *Request) (resp *Response, err error) {
	//defer func() {
	//	if e := recover(); e != nil {
	//		moe, ok := e.(*moerr.Error)
	//		if !ok {
	//			err = moerr.ConvertPanicError(requestCtx, e)
	//			resp = NewGeneralErrorResponse(COM_QUERY, ses.GetServerStatus(), err)
	//		} else {
	//			resp = NewGeneralErrorResponse(COM_QUERY, ses.GetServerStatus(), moe)
	//		}
	//	}
	//}()

	var span trace.Span
	requestCtx, span = trace.Start(requestCtx, "MysqlCmdExecutor.ExecRequest",
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
		logDebug(ses, ses.GetDebugString(), "query trace", logutil.ConnectionIdField(ses.GetConnectionID()), logutil.QueryField(SubStringFromBegin(query, int(gPu.SV.LengthOfQueryPrinted))))
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
		gPu.TxnClient,
		nil,
		gPu.FileService,
		gPu.LockService,
		gPu.QueryClient,
		gPu.HAKeeperClient,
		gPu.UdfService,
		gAicm)
	proc.CopyVectorPool(ses.proc)
	proc.CopyValueScanBatch(ses.proc)
	proc.Id = ses.getNextProcessId()
	proc.Lim.Size = gPu.SV.ProcessLimitationSize
	proc.Lim.BatchRows = gPu.SV.ProcessLimitationBatchRows
	proc.Lim.MaxMsgSize = gPu.SV.MaxMessageSize
	proc.Lim.PartitionRows = gPu.SV.ProcessLimitationPartitionRows
	proc.SessionInfo = process.SessionInfo{
		User:                 ses.GetUserName(),
		Host:                 gPu.SV.Host,
		ConnectionID:         uint64(proto.ConnectionID()),
		Database:             ses.GetDatabaseName(),
		Version:              makeServerVersion(gPu, serverVersion.Load().(string)),
		TimeZone:             ses.GetTimeZone(),
		StorageEngine:        gPu.StorageEngine,
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
	requestCtx, span = trace.Start(requestCtx, "MysqlCmdExecutor.doComQuery",
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
		gPu.StorageEngine,
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
		err = executeStmtWithTxn(requestCtx, ses, &execCtx)
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

		err = respClientWhenSuccess(requestCtx, ses, &execCtx)
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
		}
		Cached = true
		ses.cachePlan(input.getSql(), stmts, plans)
	}

	return nil
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
	var span trace.Span
	requestCtx, span = trace.Start(requestCtx, "MysqlCmdExecutor.executeStmt",
		trace.WithKind(trace.SpanKindStatement))
	defer span.End(trace.WithStatementExtra(ses.GetTxnId(), ses.GetStmtId(), ses.GetSqlOfStmt()))

	ses.SetQueryInProgress(true)
	ses.SetQueryStart(time.Now())
	ses.SetQueryInExecute(true)
	if txw, ok := execCtx.cw.(*TxnComputationWrapper); ok {
		ses.GetTxnCompileCtx().tcw = txw
	}

	defer ses.SetQueryEnd(time.Now())
	defer ses.SetQueryInProgress(false)

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
	case tree.IN_FRONTEND:
		return handleInFrontend(requestCtx, ses, execCtx)
	case tree.IN_BACKEND:
		//in the computation engine
	}

	switch st := execCtx.stmt.(type) {
	case *tree.Select:
		if st.Ep != nil {
			if gPu.SV.DisableSelectInto {
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
	}

	switch st := execCtx.stmt.(type) {
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

	cmpBegin = time.Now()

	if ret, err = execCtx.cw.Compile(requestCtx, ses, ses.GetOutputCallback()); err != nil {
		return
	}

	// cw.Compile may rewrite the stmt in the EXECUTE statement, we fetch the latest version
	//need to check again.
	execCtx.stmt = execCtx.cw.GetAst()
	switch execCtx.stmt.StmtKind().HandleType() {
	case tree.IN_FRONTEND:
		return handleInFrontend(requestCtx, ses, execCtx)
	case tree.IN_BACKEND:

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
