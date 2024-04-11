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
	"errors"
	"sync"

	"github.com/google/uuid"
	"github.com/matrixorigin/matrixone/pkg/logutil"

	"github.com/matrixorigin/matrixone/pkg/common/moerr"
	moruntime "github.com/matrixorigin/matrixone/pkg/common/runtime"
	"github.com/matrixorigin/matrixone/pkg/defines"
	"github.com/matrixorigin/matrixone/pkg/sql/parsers/tree"
	"github.com/matrixorigin/matrixone/pkg/txn/client"
	"github.com/matrixorigin/matrixone/pkg/txn/storage/memorystorage"
	db_holder "github.com/matrixorigin/matrixone/pkg/util/export/etl/db"
	"github.com/matrixorigin/matrixone/pkg/util/metric"
	"github.com/matrixorigin/matrixone/pkg/util/trace"
	"github.com/matrixorigin/matrixone/pkg/vm/engine"
	"go.uber.org/zap"
)

var (
	dumpUUID = uuid.UUID{}
)

type TxnHandler struct {
	storage     engine.Engine
	ses         FeSession
	txnOperator TxnOperator

	// it is for the transaction and different from the requestCtx.
	// it is created before the transaction is started and
	// released after the transaction is commit or rollback.
	// the lifetime of txnCtx is longer than the requestCtx.
	// the timeout of txnCtx is from the FrontendParameters.SessionTimeout with
	// default 24 hours.
	txnCtx             context.Context
	txnCtxCancel       context.CancelFunc
	shareTxn           bool
	mu                 sync.Mutex
	entryMu            sync.Mutex
	hasCalledStartStmt bool
	prevTxnId          []byte
	hasCalledIncrStmt  bool
	prevIncrTxnId      []byte

	//the server status
	serverStatus uint16

	//the option bits
	optionBits uint32

	//start a new statement
	inStmt bool
}

func InitTxnHandler(storage engine.Engine, txnCtx context.Context, txnOp TxnOperator) *TxnHandler {
	h := &TxnHandler{
		storage:     &engine.EntireEngine{Engine: storage},
		txnCtx:      txnCtx,
		txnOperator: txnOp,
		shareTxn:    txnCtx != nil && txnOp != nil,
	}
	return h
}

func (th *TxnHandler) createTxnCtx() (context.Context, error) {
	if th.txnCtx == nil {
		th.txnCtx, th.txnCtxCancel = context.WithTimeout(th.ses.GetConnectContext(),
			gPu.SV.SessionTimeout.Duration)
	}

	reqCtx := th.ses.GetRequestContext()
	retTxnCtx := th.txnCtx

	accountId, err := defines.GetAccountId(reqCtx)
	if err != nil {
		return nil, err
	}
	retTxnCtx = defines.AttachAccountId(retTxnCtx, accountId)
	retTxnCtx = defines.AttachUserId(retTxnCtx, defines.GetUserId(reqCtx))
	retTxnCtx = defines.AttachRoleId(retTxnCtx, defines.GetRoleId(reqCtx))
	if v := reqCtx.Value(defines.NodeIDKey{}); v != nil {
		retTxnCtx = context.WithValue(retTxnCtx, defines.NodeIDKey{}, v)
	}
	retTxnCtx = trace.ContextWithSpan(retTxnCtx, trace.SpanFromContext(reqCtx))
	if th.ses != nil && th.ses.GetTenantInfo() != nil && th.ses.GetTenantInfo().User == db_holder.MOLoggerUser {
		retTxnCtx = context.WithValue(retTxnCtx, defines.IsMoLogger{}, true)
	}

	if storage, ok := reqCtx.Value(defines.TemporaryTN{}).(*memorystorage.Storage); ok {
		retTxnCtx = context.WithValue(retTxnCtx, defines.TemporaryTN{}, storage)
	} else if th.ses.IfInitedTempEngine() {
		retTxnCtx = context.WithValue(retTxnCtx, defines.TemporaryTN{}, th.ses.GetTempTableStorage())
	}
	return retTxnCtx, nil
}

func (th *TxnHandler) AttachTempStorageToTxnCtx() error {
	th.mu.Lock()
	defer th.mu.Unlock()
	if th.ses.IfInitedTempEngine() {
		ctx, err := th.createTxnCtx()
		if err != nil {
			return err
		}
		th.txnCtx = context.WithValue(ctx, defines.TemporaryTN{}, th.ses.GetTempTableStorage())
	}
	return nil
}

// we don't need to lock. TxnHandler is holded by one session.
func (th *TxnHandler) SetTempEngine(te engine.Engine) {
	th.mu.Lock()
	defer th.mu.Unlock()
	ee := th.storage.(*engine.EntireEngine)
	ee.TempEngine = te
}

// NewTxnOperator creates a new txn operator using TxnClient
func (th *TxnHandler) NewTxnOperator() (context.Context, TxnOperator, error) {
	var err error
	th.mu.Lock()
	defer th.mu.Unlock()
	if gPu.TxnClient == nil {
		panic("must set txn client")
	}

	if th.shareTxn {
		return nil, nil, moerr.NewInternalError(th.ses.GetRequestContext(), "NewTxnOperator: the share txn is not allowed to create new txn")
	}

	var opts []client.TxnOption
	rt := moruntime.ProcessLevelRuntime()
	if rt != nil {
		if v, ok := rt.GetGlobalVariables(moruntime.TxnOptions); ok {
			opts = v.([]client.TxnOption)
		}
	}

	txnCtx, err := th.createTxnCtx()
	if err != nil {
		return nil, nil, err
	}
	if txnCtx == nil {
		panic("context should not be nil")
	}

	accountID := uint32(0)
	userName := ""
	connectionID := uint32(0)
	if th.ses.GetMysqlProtocol() != nil {
		connectionID = th.ses.GetMysqlProtocol().ConnectionID()
	}
	if th.ses.GetTenantInfo() != nil {
		accountID = th.ses.GetTenantInfo().TenantID
		userName = th.ses.GetTenantInfo().User
	}
	sessionInfo := th.ses.GetDebugString()
	opts = append(opts,
		client.WithTxnCreateBy(
			accountID,
			userName,
			th.ses.GetUUIDString(),
			connectionID),
		client.WithSessionInfo(sessionInfo))

	if th.ses != nil && th.ses.GetFromRealUser() {
		opts = append(opts,
			client.WithUserTxn())
	}

	if th.ses != nil {
		if th.ses.IsBackgroundSession() ||
			th.ses.DisableTrace() {
			opts = append(opts, client.WithDisableTrace(true))
		} else {
			varVal, err := th.ses.GetSessionVar("disable_txn_trace")
			if err != nil {
				return nil, nil, err
			}
			if gsv, ok := GSysVariables.GetDefinitionOfSysVar("disable_txn_trace"); ok {
				if svbt, ok2 := gsv.GetType().(SystemVariableBoolType); ok2 {
					if svbt.IsTrue(varVal) {
						opts = append(opts, client.WithDisableTrace(true))
					}
				}
			}
		}
	}

	th.txnOperator, err = gPu.TxnClient.New(
		txnCtx,
		th.ses.getLastCommitTS(),
		opts...)
	if err != nil {
		return nil, nil, err
	}
	if th.txnOperator == nil {
		return nil, nil, moerr.NewInternalError(th.ses.GetRequestContext(), "NewTxnOperator: txnClient new a null txn")
	}
	return txnCtx, th.txnOperator, err
}

func (th *TxnHandler) enableStartStmt(txnId []byte) {
	th.mu.Lock()
	defer th.mu.Unlock()
	th.hasCalledStartStmt = true
	th.prevTxnId = txnId
}

func (th *TxnHandler) disableStartStmt() {
	th.mu.Lock()
	defer th.mu.Unlock()
	th.hasCalledStartStmt = false
	th.prevTxnId = nil
}

func (th *TxnHandler) calledStartStmt() (bool, []byte) {
	th.mu.Lock()
	defer th.mu.Unlock()
	return th.hasCalledStartStmt, th.prevTxnId
}

func (th *TxnHandler) enableIncrStmt(txnId []byte) {
	th.mu.Lock()
	defer th.mu.Unlock()
	th.hasCalledIncrStmt = true
	th.prevIncrTxnId = txnId
}

func (th *TxnHandler) disableIncrStmt() {
	th.mu.Lock()
	defer th.mu.Unlock()
	th.hasCalledIncrStmt = false
	th.prevIncrTxnId = nil
}

func (th *TxnHandler) calledIncrStmt() (bool, []byte) {
	th.mu.Lock()
	defer th.mu.Unlock()
	return th.hasCalledIncrStmt, th.prevIncrTxnId
}

// NewTxn commits the old transaction if it existed.
// Then it creates the new transaction by Engin.New.
func (th *TxnHandler) NewTxn() (context.Context, TxnOperator, error) {
	var err error
	var txnCtx context.Context
	var txnOp TxnOperator
	if th.IsShareTxn() {
		return nil, nil, moerr.NewInternalError(th.GetSession().GetRequestContext(), "NewTxn: the share txn is not allowed to create new txn")
	}
	if th.IsValidTxnOperator() {
		err = th.CommitTxn()
		if err != nil {
			/*
				fix issue 6024.
				When we get a w-w conflict during commit the txn,
				we convert the error into a readable error.
			*/
			if moerr.IsMoErrCode(err, moerr.ErrTxnWWConflict) {
				return nil, nil, moerr.NewInternalError(th.GetSession().GetRequestContext(), writeWriteConflictsErrorInfo())
			}
			return nil, nil, err
		}
	}
	th.SetTxnOperatorInvalid()
	defer func() {
		if err != nil {
			tenant := th.ses.GetTenantName()
			incTransactionErrorsCounter(tenant, metric.SQLTypeBegin)
		}
	}()
	txnCtx, txnOp, err = th.NewTxnOperator()
	if err != nil {
		return nil, nil, err
	}
	if txnCtx == nil {
		panic("context should not be nil")
	}
	storage := th.GetStorage()
	err = storage.New(txnCtx, txnOp)
	//if txnOp != nil && !th.GetSession().IsDerivedStmt() {
	//	fmt.Println("===> start statement 1", txnOp.Txn().DebugString())
	//	txnOp.GetWorkspace().StartStatement()
	//	th.enableStartStmt()
	//}
	if err != nil {
		th.ses.SetTxnId(dumpUUID[:])
	} else {
		th.ses.SetTxnId(txnOp.Txn().ID)
	}
	return txnCtx, txnOp, err
}

// IsValidTxnOperator checks the txn operator is valid
func (th *TxnHandler) IsValidTxnOperator() bool {
	th.mu.Lock()
	defer th.mu.Unlock()
	return th.txnOperator != nil && th.txnCtx != nil
}

func (th *TxnHandler) SetTxnOperatorInvalid() {
	th.mu.Lock()
	defer th.mu.Unlock()
	th.txnOperator = nil
	if th.txnCtxCancel != nil {
		//fmt.Printf("**> %v\n", th.txnCtx)
		th.txnCtxCancel()
		th.txnCtxCancel = nil
	}
	th.txnCtx = nil
}

func (th *TxnHandler) GetTxnOperator() (context.Context, TxnOperator, error) {
	th.mu.Lock()
	defer th.mu.Unlock()
	ctx, err := th.createTxnCtx()
	if err != nil {
		return nil, nil, err
	}
	return ctx, th.txnOperator, nil
}

func (th *TxnHandler) SetSession(ses FeSession) {
	th.mu.Lock()
	defer th.mu.Unlock()
	th.ses = ses
}

func (th *TxnHandler) GetSession() FeSession {
	th.mu.Lock()
	defer th.mu.Unlock()
	return th.ses
}

func (th *TxnHandler) CommitTxn() error {
	_, span := trace.Start(th.ses.GetRequestContext(), "TxnHandler.CommitTxn",
		trace.WithKind(trace.SpanKindStatement))
	defer span.End(trace.WithStatementExtra(th.ses.GetTxnId(), th.ses.GetStmtId(), th.ses.GetSqlOfStmt()))

	th.entryMu.Lock()
	defer th.entryMu.Unlock()
	if !th.IsValidTxnOperator() || th.IsShareTxn() {
		return nil
	}
	ses := th.GetSession()
	sessionInfo := ses.GetDebugString()
	txnCtx, txnOp, err := th.GetTxnOperator()
	if err != nil {
		return err
	}
	if txnOp == nil {
		th.SetTxnOperatorInvalid()
	}
	if txnCtx == nil {
		panic("context should not be nil")
	}
	if ses.IfInitedTempEngine() && ses.GetTempTableStorage() != nil {
		txnCtx = context.WithValue(txnCtx, defines.TemporaryTN{}, ses.GetTempTableStorage())
	}
	storage := th.GetStorage()
	ctx2, cancel := context.WithTimeout(
		txnCtx,
		storage.Hints().CommitOrRollbackTimeout,
	)
	defer cancel()
	val, e := ses.GetSessionVar("mo_pk_check_by_dn")
	if e != nil {
		return e
	}
	if val != nil {
		ctx2 = context.WithValue(ctx2, defines.PkCheckByTN{}, val.(int8))
	}
	defer func() {
		// metric count
		tenant := ses.GetTenantName()
		incTransactionCounter(tenant)
		if err != nil {
			incTransactionErrorsCounter(tenant, metric.SQLTypeCommit)
		}
	}()

	if logutil.GetSkip1Logger().Core().Enabled(zap.DebugLevel) {
		txnId := txnOp.Txn().DebugString()
		logDebugf(sessionInfo, "CommitTxn txnId:%s", txnId)
		defer func() {
			logDebugf(sessionInfo, "CommitTxn exit txnId:%s", txnId)
		}()
	}
	if txnOp != nil {
		th.ses.SetTxnId(txnOp.Txn().ID)
		err = txnOp.Commit(ctx2)
		if err != nil {
			th.SetTxnOperatorInvalid()
		}
		ses.updateLastCommitTS(txnOp.Txn().CommitTS)
	}
	th.SetTxnOperatorInvalid()
	th.ses.SetTxnId(dumpUUID[:])
	return err
}

func (th *TxnHandler) RollbackTxn() error {
	_, span := trace.Start(th.ses.GetRequestContext(), "TxnHandler.RollbackTxn",
		trace.WithKind(trace.SpanKindStatement))
	defer span.End(trace.WithStatementExtra(th.ses.GetTxnId(), th.ses.GetStmtId(), th.ses.GetSqlOfStmt()))

	th.entryMu.Lock()
	defer th.entryMu.Unlock()
	if !th.IsValidTxnOperator() || th.IsShareTxn() {
		return nil
	}
	ses := th.GetSession()
	sessionInfo := ses.GetDebugString()
	txnCtx, txnOp, err := th.GetTxnOperator()
	if err != nil {
		return err
	}
	if txnOp == nil {
		th.SetTxnOperatorInvalid()
	}
	if txnCtx == nil {
		panic("context should not be nil")
	}
	if ses.IfInitedTempEngine() && ses.GetTempTableStorage() != nil {
		txnCtx = context.WithValue(txnCtx, defines.TemporaryTN{}, ses.GetTempTableStorage())
	}
	storage := th.GetStorage()
	ctx2, cancel := context.WithTimeout(
		txnCtx,
		storage.Hints().CommitOrRollbackTimeout,
	)
	defer cancel()
	defer func() {
		// metric count
		tenant := ses.GetTenantName()
		incTransactionCounter(tenant)
		incTransactionErrorsCounter(tenant, metric.SQLTypeOther) // exec rollback cnt
		if err != nil {
			incTransactionErrorsCounter(tenant, metric.SQLTypeRollback)
		}
	}()
	if logutil.GetSkip1Logger().Core().Enabled(zap.DebugLevel) {
		txnId := txnOp.Txn().DebugString()
		logDebugf(sessionInfo, "RollbackTxn txnId:%s", txnId)
		defer func() {
			logDebugf(sessionInfo, "RollbackTxn exit txnId:%s", txnId)
		}()
	}
	if txnOp != nil {
		th.ses.SetTxnId(txnOp.Txn().ID)
		err = txnOp.Rollback(ctx2)
		if err != nil {
			th.SetTxnOperatorInvalid()
		}
	}
	th.SetTxnOperatorInvalid()
	th.ses.SetTxnId(dumpUUID[:])
	return err
}

func (th *TxnHandler) GetStorage() engine.Engine {
	th.mu.Lock()
	defer th.mu.Unlock()
	return th.storage
}

func (th *TxnHandler) GetTxn() (context.Context, TxnOperator, error) {
	txnCtx, txnOp, err := th.TxnCreate()
	if err != nil {
		return nil, nil, err
	}
	return txnCtx, txnOp, err
}

func (th *TxnHandler) cancelTxnCtx() {
	th.mu.Lock()
	defer th.mu.Unlock()
	if th.txnCtxCancel != nil {
		th.txnCtxCancel()
	}
}

func (th *TxnHandler) IsShareTxn() bool {
	th.mu.Lock()
	defer th.mu.Unlock()
	return th.shareTxn
}

func (th *TxnHandler) SetOptionBits(bit uint32) {
	th.mu.Lock()
	defer th.mu.Unlock()
	th.optionBits |= bit
}

func (th *TxnHandler) ClearOptionBits(bit uint32) {
	th.mu.Lock()
	defer th.mu.Unlock()
	th.optionBits &= ^bit
}

func (th *TxnHandler) OptionBitsIsSet(bit uint32) bool {
	th.mu.Lock()
	defer th.mu.Unlock()
	return th.optionBits&bit != 0
}

func (th *TxnHandler) GetOptionBits() uint32 {
	th.mu.Lock()
	defer th.mu.Unlock()
	return th.optionBits
}

func (th *TxnHandler) SetServerStatus(bit uint16) {
	th.mu.Lock()
	defer th.mu.Unlock()
	th.serverStatus |= bit
}

func (th *TxnHandler) ClearServerStatus(bit uint16) {
	th.mu.Lock()
	defer th.mu.Unlock()
	th.serverStatus &= ^bit
}

func (th *TxnHandler) ServerStatusIsSet(bit uint16) bool {
	th.mu.Lock()
	defer th.mu.Unlock()
	return th.serverStatus&bit != 0
}

func (th *TxnHandler) GetServerStatus() uint16 {
	th.mu.Lock()
	defer th.mu.Unlock()
	return th.serverStatus
}

/*
InMultiStmtTransactionMode checks the session is in multi-statement transaction mode.
OPTION_NOT_AUTOCOMMIT: After the autocommit is off, the multi-statement transaction is
started implicitly by the first statement of the transaction.
OPTION_BEGIN: Whenever the autocommit is on or off, the multi-statement transaction is
started explicitly by the BEGIN statement.

But it does not denote the transaction is active or not.

Cases    | set Autocommit = 1/0 | BEGIN statement |
---------------------------------------------------
Case1      1                       Yes
Case2      1                       No
Case3      0                       Yes
Case4      0                       No
---------------------------------------------------

If it is Case1,Case3,Cass4, Then

	InMultiStmtTransactionMode returns true.
	Also, the bit SERVER_STATUS_IN_TRANS will be set.

If it is Case2, Then

	InMultiStmtTransactionMode returns false
*/
func (th *TxnHandler) InMultiStmtTransactionMode() bool {
	return th.OptionBitsIsSet(OPTION_NOT_AUTOCOMMIT | OPTION_BEGIN)
}

/*
InActiveMultiStmtTransaction checks the session is in multi-statement transaction mode
and there is an active transaction.

But sometimes, the session does not start an active transaction even if it is in multi-
statement transaction mode.

For example: there is no active transaction.
set autocommit = 0;
select 1;

For example: there is an active transaction.
begin;
select 1;

When the statement starts the multi-statement transaction(select * from table), this flag
won't be set until we access the tables.
*/
func (th *TxnHandler) InActiveMultiStmtTransaction() bool {
	return th.ServerStatusIsSet(SERVER_STATUS_IN_TRANS)
}

/*
TxnCreate creates the transaction implicitly and idempotent

When it is in multi-statement transaction mode:

	Set SERVER_STATUS_IN_TRANS bit;
	Starts a new transaction if there is none. Reuse the current transaction if there is one.

When it is not in single statement transaction mode:

	Starts a new transaction if there is none. Reuse the current transaction if there is one.
*/
func (th *TxnHandler) TxnCreate() (context.Context, TxnOperator, error) {
	// SERVER_STATUS_IN_TRANS should be set to true regardless of whether autocommit is equal to 1.
	th.SetServerStatus(SERVER_STATUS_IN_TRANS)

	if !th.IsValidTxnOperator() {
		return th.NewTxn()
	}
	txnCtx, txnOp, err := th.GetTxnOperator()
	return txnCtx, txnOp, err
}

/*
TxnBegin begins a new transaction.
It commits the current transaction implicitly.
*/
func (th *TxnHandler) TxnBegin() error {
	var err error
	if th.InMultiStmtTransactionMode() {
		th.ClearServerStatus(SERVER_STATUS_IN_TRANS)
		err = th.CommitTxn()
	}
	th.ClearOptionBits(OPTION_BEGIN)
	if err != nil {
		/*
			fix issue 6024.
			When we get a w-w conflict during commit the txn,
			we convert the error into a readable error.
		*/
		if moerr.IsMoErrCode(err, moerr.ErrTxnWWConflict) {
			return moerr.NewInternalError(th.ses.GetRequestContext(), writeWriteConflictsErrorInfo())
		}
		return err
	}
	th.SetOptionBits(OPTION_BEGIN)
	th.SetServerStatus(SERVER_STATUS_IN_TRANS)
	_, _, err = th.NewTxn()
	return err
}

// TxnCommit commits the current transaction.
func (th *TxnHandler) TxnCommit() error {
	var err error
	th.ClearServerStatus(SERVER_STATUS_IN_TRANS | SERVER_STATUS_IN_TRANS_READONLY)
	err = th.CommitTxn()
	th.ClearServerStatus(SERVER_STATUS_IN_TRANS)
	th.ClearOptionBits(OPTION_BEGIN)
	return err
}

// TxnRollback rollbacks the current transaction.
func (th *TxnHandler) TxnRollback() error {
	var err error
	th.ClearServerStatus(SERVER_STATUS_IN_TRANS | SERVER_STATUS_IN_TRANS_READONLY)
	err = th.RollbackTxn()
	th.ClearOptionBits(OPTION_BEGIN)
	return err
}

/*
TxnCommitSingleStatement commits the single statement transaction.

Cases    | set Autocommit = 1/0 | BEGIN statement |
---------------------------------------------------
Case1      1                       Yes
Case2      1                       No
Case3      0                       Yes
Case4      0                       No
---------------------------------------------------

If it is Case1,Case3,Cass4, Then

	InMultiStmtTransactionMode returns true.
	Also, the bit SERVER_STATUS_IN_TRANS will be set.

If it is Case2, Then

	InMultiStmtTransactionMode returns false
*/
func (th *TxnHandler) TxnCommitSingleStatement(stmt tree.Statement) error {
	var err error
	/*
		Commit Rules:
		1, if it is in single-statement mode:
			it commits.
		2, if it is in multi-statement mode:
			if the statement is the one can be executed in the active transaction,
				the transaction need to be committed at the end of the statement.
	*/
	if !th.InMultiStmtTransactionMode() ||
		th.InActiveTransaction() && NeedToBeCommittedInActiveTransaction(stmt) {
		err = th.CommitTxn()
		th.ClearServerStatus(SERVER_STATUS_IN_TRANS)
		th.ClearOptionBits(OPTION_BEGIN)
	}
	return err
}

/*
TxnRollbackSingleStatement rollbacks the single statement transaction.

Cases    | set Autocommit = 1/0 | BEGIN statement |
---------------------------------------------------
Case1      1                       Yes
Case2      1                       No
Case3      0                       Yes
Case4      0                       No
---------------------------------------------------

If it is Case1,Case3,Cass4, Then

	InMultiStmtTransactionMode returns true.
	Also, the bit SERVER_STATUS_IN_TRANS will be set.

If it is Case2, Then

	InMultiStmtTransactionMode returns false
*/
func (th *TxnHandler) TxnRollbackSingleStatement(stmt tree.Statement, inputErr error) error {
	var err error
	var rollbackWholeTxn bool
	if inputErr != nil {
		rollbackWholeTxn = isErrorRollbackWholeTxn(inputErr)
	}
	/*
			Rollback Rules:
			1, if it is in single-statement mode (Case2):
				it rollbacks.
			2, if it is in multi-statement mode (Case1,Case3,Case4):
		        the transaction need to be rollback at the end of the statement.
				(every error will abort the transaction.)
	*/
	if !th.InMultiStmtTransactionMode() ||
		th.InActiveTransaction() && NeedToBeCommittedInActiveTransaction(stmt) ||
		rollbackWholeTxn {
		//Case1.1: autocommit && not_begin
		//Case1.2: (not_autocommit || begin) && activeTxn && needToBeCommitted
		//Case1.3: the error that should rollback the whole txn
		err = th.rollbackWholeTxn()
	} else {
		//Case2: not ( autocommit && !begin ) && not ( activeTxn && needToBeCommitted )
		//<==>  ( not_autocommit || begin ) && not ( activeTxn && needToBeCommitted )
		//just rollback statement
		var err3 error
		txnCtx, txnOp, err3 := th.GetTxnOperator()
		if err3 != nil {
			logError(th.ses, th.ses.GetDebugString(), err3.Error())
			return err3
		}

		//non derived statement
		if txnOp != nil && !th.ses.IsDerivedStmt() {
			//incrStatement has been called
			ok, id := th.calledIncrStmt()
			if ok && bytes.Equal(txnOp.Txn().ID, id) {
				err = txnOp.GetWorkspace().RollbackLastStatement(txnCtx)
				th.disableIncrStmt()
				if err != nil {
					err4 := th.rollbackWholeTxn()
					return errors.Join(err, err4)
				}
			}
		}
	}
	return err
}

// rollbackWholeTxn
func (th *TxnHandler) rollbackWholeTxn() error {
	err := th.RollbackTxn()
	th.ClearServerStatus(SERVER_STATUS_IN_TRANS)
	th.ClearOptionBits(OPTION_BEGIN)
	return err
}

/*
InActiveTransaction checks if it is in an active transaction.
*/
func (th *TxnHandler) InActiveTransaction() bool {
	if th.InActiveMultiStmtTransaction() {
		return true
	} else {
		return th.IsValidTxnOperator()
	}
}

/*
SetAutocommit sets the value of the system variable 'autocommit'.

The rule is that we can not execute the statement 'set parameter = value' in
an active transaction whichever it is started by BEGIN or in 'set autocommit = 0;'.
*/
func (th *TxnHandler) SetAutocommit(old, on bool) error {
	//on -> on : do nothing
	//off -> on : commit active txn
	//	if commit failed, clean OPTION_AUTOCOMMIT
	//	if commit succeeds, clean OPTION_BEGIN | OPTION_NOT_AUTOCOMMIT
	//		and set SERVER_STATUS_AUTOCOMMIT
	//on -> off :
	//	clean OPTION_AUTOCOMMIT
	//	clean SERVER_STATUS_AUTOCOMMIT
	//	set OPTION_NOT_AUTOCOMMIT
	//off -> off : do nothing
	if !old && on { //off -> on
		//activating autocommit
		err := th.CommitTxn()
		if err != nil {
			th.ClearOptionBits(OPTION_AUTOCOMMIT)
			return err
		}
		th.ClearOptionBits(OPTION_BEGIN | OPTION_NOT_AUTOCOMMIT)
		th.SetServerStatus(SERVER_STATUS_AUTOCOMMIT)
	} else if old && !on { //on -> off
		th.ClearServerStatus(SERVER_STATUS_AUTOCOMMIT)
		th.SetOptionBits(OPTION_NOT_AUTOCOMMIT)
	}
	return nil
}

func (th *TxnHandler) setAutocommitOn() {
	th.ClearOptionBits(OPTION_BEGIN | OPTION_NOT_AUTOCOMMIT)
	th.SetServerStatus(SERVER_STATUS_AUTOCOMMIT)
}

// get errors during the transaction. rollback the transaction
func rollbackTxnFunc(reqCtx context.Context, ses FeSession, execErr error, execCtx *ExecCtx) error {
	incStatementErrorsCounter(execCtx.tenant, execCtx.stmt)
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
	if ses.GetTxnHandler().InMultiStmtTransactionMode() && ses.GetTxnHandler().InActiveTransaction() {
		ses.cleanCache()
	}
	//logError(ses, ses.GetDebugString(), execErr.Error())
	txnErr := ses.GetTxnHandler().TxnRollbackSingleStatement(execCtx.stmt, execErr)
	if txnErr != nil {
		logStatementStatus(reqCtx, ses, execCtx.stmt, fail, txnErr)
		return txnErr
	}
	logStatementStatus(reqCtx, ses, execCtx.stmt, fail, execErr)
	return execErr
}

// execution succeeds during the transaction. commit the transaction
func commitTxnFunc(requestCtx context.Context,
	ses FeSession,
	execCtx *ExecCtx) (retErr error) {
	// Call a defer function -- if TxnCommitSingleStatement paniced, we
	// want to catch it and convert it to an error.
	//defer func() {
	//	if r := recover(); r != nil {
	//		retErr = moerr.ConvertPanicError(requestCtx, r)
	//	}
	//}()

	//load data handle txn failure internally
	retErr = ses.GetTxnHandler().TxnCommitSingleStatement(execCtx.stmt)
	if retErr != nil {
		logStatementStatus(requestCtx, ses, execCtx.stmt, fail, retErr)
	}
	return
}

// finish the transaction
func finishTxnFunc(reqCtx context.Context, ses FeSession, execErr error, execCtx *ExecCtx) (err error) {
	// First recover all panics.   If paniced, we will abort.
	//if r := recover(); r != nil {
	//	err = moerr.ConvertPanicError(requestCtx, r)
	//}

	if execErr == nil {
		err = commitTxnFunc(reqCtx, ses, execCtx)
		if err == nil {
			return err
		}
		// if commitTxnFunc failed, we will rollback the transaction.
		execErr = err
	}

	return rollbackTxnFunc(reqCtx, ses, execErr, execCtx)
}
