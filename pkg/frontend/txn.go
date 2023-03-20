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
	"context"
	"github.com/matrixorigin/matrixone/pkg/common/moerr"
	moruntime "github.com/matrixorigin/matrixone/pkg/common/runtime"
	"github.com/matrixorigin/matrixone/pkg/defines"
	"github.com/matrixorigin/matrixone/pkg/sql/parsers/tree"
	"github.com/matrixorigin/matrixone/pkg/txn/client"
	"github.com/matrixorigin/matrixone/pkg/txn/storage/memorystorage"
	"github.com/matrixorigin/matrixone/pkg/util/metric"
	"github.com/matrixorigin/matrixone/pkg/vm/engine"
	"sync"
)

type TxnHandler struct {
	storage     engine.Engine
	txnClient   TxnClient
	ses         *Session
	txnOperator TxnOperator

	// it is for the transaction and different from the requestCtx.
	// it is created before the transaction is started and
	// released after the transaction is commit or rollback.
	// the lifetime of txnCtx is longer than the requestCtx.
	// the timeout of txnCtx is from the FrontendParameters.SessionTimeout with
	// default 24 hours.
	txnCtx       context.Context
	txnCtxCancel context.CancelFunc
	mu           sync.Mutex
	entryMu      sync.Mutex
}

func InitTxnHandler(storage engine.Engine, txnClient TxnClient) *TxnHandler {
	h := &TxnHandler{
		storage:   &engine.EntireEngine{Engine: storage},
		txnClient: txnClient,
	}
	return h
}

func (th *TxnHandler) ResetTxnCtx() {
	th.mu.Lock()
	defer th.mu.Unlock()
	if th.txnCtxCancel != nil {
		th.txnCtxCancel()
	}
	th.txnCtx = nil
}

func (th *TxnHandler) GetTxnCtx() context.Context {
	th.mu.Lock()
	defer th.mu.Unlock()
	return th.createTxnCtx()
}

func (th *TxnHandler) createTxnCtx() context.Context {
	if th.txnCtx == nil {
		th.txnCtx, th.txnCtxCancel = context.WithTimeout(th.ses.GetConnectContext(),
			th.ses.GetParameterUnit().SV.SessionTimeout.Duration)
	}

	reqCtx := th.ses.GetRequestContext()
	retTxnCtx := th.txnCtx
	if v := reqCtx.Value(defines.TenantIDKey{}); v != nil {
		retTxnCtx = context.WithValue(retTxnCtx, defines.TenantIDKey{}, v)
	}
	if v := reqCtx.Value(defines.UserIDKey{}); v != nil {
		retTxnCtx = context.WithValue(retTxnCtx, defines.UserIDKey{}, v)
	}
	if v := reqCtx.Value(defines.RoleIDKey{}); v != nil {
		retTxnCtx = context.WithValue(retTxnCtx, defines.RoleIDKey{}, v)
	}

	if storage, ok := reqCtx.Value(defines.TemporaryDN{}).(*memorystorage.Storage); ok {
		retTxnCtx = context.WithValue(retTxnCtx, defines.TemporaryDN{}, storage)
	}
	return retTxnCtx
}

func (th *TxnHandler) AttachTempStorageToTxnCtx() {
	th.mu.Lock()
	defer th.mu.Unlock()
	th.txnCtx = context.WithValue(th.createTxnCtx(), defines.TemporaryDN{}, th.ses.GetTempTableStorage())
}

// we don't need to lock. TxnHandler is holded by one session.
func (th *TxnHandler) SetTempEngine(te engine.Engine) {
	th.mu.Lock()
	defer th.mu.Unlock()
	ee := th.storage.(*engine.EntireEngine)
	ee.TempEngine = te
}

func (th *TxnHandler) GetTxnClient() TxnClient {
	th.mu.Lock()
	defer th.mu.Unlock()
	return th.txnClient
}

// NewTxnOperator creates a new txn operator using TxnClient
func (th *TxnHandler) NewTxnOperator() error {
	var err error
	th.mu.Lock()
	defer th.mu.Unlock()
	if th.txnClient == nil {
		panic("must set txn client")
	}

	var opts []client.TxnOption
	rt := moruntime.ProcessLevelRuntime()
	if rt != nil {
		if v, ok := rt.GetGlobalVariables(moruntime.TxnOptions); ok {
			opts = v.([]client.TxnOption)
		}
	}

	th.txnOperator, err = th.txnClient.New(opts...)
	if err != nil {
		return err
	}
	if th.txnOperator == nil {
		return moerr.NewInternalError(th.ses.GetRequestContext(), "NewTxnOperator: txnClient new a null txn")
	}
	return err
}

// NewTxn commits the old transaction if it existed.
// Then it creates the new transaction by Engin.New.
func (th *TxnHandler) NewTxn() error {
	var err error
	if th.IsValidTxnOperator() {
		err = th.CommitTxn()
		if err != nil {
			/*
				fix issue 6024.
				When we get a w-w conflict during commit the txn,
				we convert the error into a readable error.
			*/
			if moerr.IsMoErrCode(err, moerr.ErrTxnWWConflict) {
				return moerr.NewInternalError(th.GetSession().GetRequestContext(), writeWriteConflictsErrorInfo())
			}
			return err
		}
	}
	th.SetTxnOperatorInvalid()
	th.ResetTxnCtx()
	defer func() {
		if err != nil {
			tenant := th.ses.GetTenantName(nil)
			incTransactionErrorsCounter(tenant, metric.SQLTypeBegin)
		}
	}()
	err = th.NewTxnOperator()
	if err != nil {
		return err
	}
	txnCtx := th.GetTxnCtx()
	if txnCtx == nil {
		panic("context should not be nil")
	}
	storage := th.GetStorage()
	err = storage.New(txnCtx, th.GetTxnOperator())
	return err
}

// IsValidTxnOperator checks the txn operator is valid
func (th *TxnHandler) IsValidTxnOperator() bool {
	th.mu.Lock()
	defer th.mu.Unlock()
	return th.txnOperator != nil
}

func (th *TxnHandler) SetTxnOperatorInvalid() {
	th.mu.Lock()
	defer th.mu.Unlock()
	th.txnOperator = nil
}

func (th *TxnHandler) GetTxnOperator() TxnOperator {
	th.mu.Lock()
	defer th.mu.Unlock()
	return th.txnOperator
}

func (th *TxnHandler) SetSession(ses *Session) {
	th.mu.Lock()
	defer th.mu.Unlock()
	th.ses = ses
}

func (th *TxnHandler) GetSession() *Session {
	th.mu.Lock()
	defer th.mu.Unlock()
	return th.ses
}

func (th *TxnHandler) CommitTxn() error {
	th.entryMu.Lock()
	defer th.entryMu.Unlock()
	if !th.IsValidTxnOperator() {
		return nil
	}
	ses := th.GetSession()
	sessionInfo := ses.GetDebugString()
	txnCtx := th.GetTxnCtx()
	if txnCtx == nil {
		panic("context should not be nil")
	}
	if ses.tempTablestorage != nil {
		txnCtx = context.WithValue(txnCtx, defines.TemporaryDN{}, ses.tempTablestorage)
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
		ctx2 = context.WithValue(ctx2, defines.PkCheckByDN{}, val.(int8))
	}
	var err, err2 error
	defer func() {
		// metric count
		tenant := ses.GetTenantName(nil)
		incTransactionCounter(tenant)
		if err != nil {
			incTransactionErrorsCounter(tenant, metric.SQLTypeCommit)
		}
	}()
	txnOp := th.GetTxnOperator()
	if txnOp == nil {
		th.SetTxnOperatorInvalid()
		th.ResetTxnCtx()
		logErrorf(sessionInfo, "CommitTxn: txn operator is null")
	}

	txnId := txnOp.Txn().DebugString()
	logDebugf(sessionInfo, "CommitTxn txnId:%s", txnId)
	defer func() {
		logDebugf(sessionInfo, "CommitTxn exit txnId:%s", txnId)
	}()
	if err = storage.Commit(ctx2, txnOp); err != nil {
		th.SetTxnOperatorInvalid()
		th.ResetTxnCtx()
		logErrorf(sessionInfo, "CommitTxn: storage commit failed. txnId:%s error:%v", txnId, err)
		if txnOp != nil {
			err2 = txnOp.Rollback(ctx2)
			if err2 != nil {
				logErrorf(sessionInfo, "CommitTxn: txn operator rollback failed. txnId:%s error:%v", txnId, err2)
			}
		}
		return err
	}
	if txnOp != nil {
		err = txnOp.Commit(ctx2)
		if err != nil {
			th.SetTxnOperatorInvalid()
			th.ResetTxnCtx()
			logErrorf(sessionInfo, "CommitTxn: txn operator commit failed. txnId:%s error:%v", txnId, err)
		}
	}
	th.SetTxnOperatorInvalid()
	th.ResetTxnCtx()
	return err
}

func (th *TxnHandler) RollbackTxn() error {
	th.entryMu.Lock()
	defer th.entryMu.Unlock()
	if !th.IsValidTxnOperator() {
		return nil
	}
	ses := th.GetSession()
	sessionInfo := ses.GetDebugString()
	txnCtx := th.GetTxnCtx()
	if txnCtx == nil {
		panic("context should not be nil")
	}
	if ses.tempTablestorage != nil {
		txnCtx = context.WithValue(txnCtx, defines.TemporaryDN{}, ses.tempTablestorage)
	}
	storage := th.GetStorage()
	ctx2, cancel := context.WithTimeout(
		txnCtx,
		storage.Hints().CommitOrRollbackTimeout,
	)
	defer cancel()
	var err, err2 error
	defer func() {
		// metric count
		tenant := ses.GetTenantName(nil)
		incTransactionCounter(tenant)
		incTransactionErrorsCounter(tenant, metric.SQLTypeOther) // exec rollback cnt
		if err != nil {
			incTransactionErrorsCounter(tenant, metric.SQLTypeRollback)
		}
	}()
	txnOp := th.GetTxnOperator()
	if txnOp == nil {
		th.ResetTxnCtx()
		logErrorf(sessionInfo, "RollbackTxn: txn operator is null")
	}
	txnId := txnOp.Txn().DebugString()
	logDebugf(sessionInfo, "RollbackTxn txnId:%s", txnId)
	defer func() {
		logDebugf(sessionInfo, "RollbackTxn exit txnId:%s", txnId)
	}()
	if err = storage.Rollback(ctx2, txnOp); err != nil {
		th.SetTxnOperatorInvalid()
		th.ResetTxnCtx()
		logErrorf(sessionInfo, "RollbackTxn: storage rollback failed. txnId:%s error:%v", txnId, err)
		if txnOp != nil {
			err2 = txnOp.Rollback(ctx2)
			if err2 != nil {
				logErrorf(sessionInfo, "RollbackTxn: txn operator rollback failed. txnId:%s error:%v", txnId, err2)
			}
		}
		return err
	}
	if txnOp != nil {
		err = txnOp.Rollback(ctx2)
		th.ResetTxnCtx()
		if err != nil {
			th.SetTxnOperatorInvalid()
			logErrorf(sessionInfo, "RollbackTxn: txn operator commit failed. txnId:%s error:%v", txnId, err)
		}
	}
	th.SetTxnOperatorInvalid()
	th.ResetTxnCtx()
	return err
}

func (th *TxnHandler) GetStorage() engine.Engine {
	th.mu.Lock()
	defer th.mu.Unlock()
	return th.storage
}

func (th *TxnHandler) GetTxn() (TxnOperator, error) {
	ses := th.GetSession()
	err := ses.TxnCreate()
	if err != nil {
		logErrorf(ses.GetDebugString(), "GetTxn. error:%v", err)
		return nil, err
	}
	return th.GetTxnOperator(), nil
}

func (ses *Session) SetOptionBits(bit uint32) {
	ses.mu.Lock()
	defer ses.mu.Unlock()
	ses.optionBits |= bit
}

func (ses *Session) ClearOptionBits(bit uint32) {
	ses.mu.Lock()
	defer ses.mu.Unlock()
	ses.optionBits &= ^bit
}

func (ses *Session) OptionBitsIsSet(bit uint32) bool {
	ses.mu.Lock()
	defer ses.mu.Unlock()
	return ses.optionBits&bit != 0
}

func (ses *Session) SetServerStatus(bit uint16) {
	ses.mu.Lock()
	defer ses.mu.Unlock()
	ses.serverStatus |= bit
}

func (ses *Session) ClearServerStatus(bit uint16) {
	ses.mu.Lock()
	defer ses.mu.Unlock()
	ses.serverStatus &= ^bit
}

func (ses *Session) ServerStatusIsSet(bit uint16) bool {
	ses.mu.Lock()
	defer ses.mu.Unlock()
	return ses.serverStatus&bit != 0
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
func (ses *Session) InMultiStmtTransactionMode() bool {
	return ses.OptionBitsIsSet(OPTION_NOT_AUTOCOMMIT | OPTION_BEGIN)
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
func (ses *Session) InActiveMultiStmtTransaction() bool {
	return ses.ServerStatusIsSet(SERVER_STATUS_IN_TRANS)
}

/*
TxnCreate creates the transaction implicitly and idempotent

When it is in multi-statement transaction mode:

	Set SERVER_STATUS_IN_TRANS bit;
	Starts a new transaction if there is none. Reuse the current transaction if there is one.

When it is not in single statement transaction mode:

	Starts a new transaction if there is none. Reuse the current transaction if there is one.
*/
func (ses *Session) TxnCreate() error {
	var err error
	if ses.InMultiStmtTransactionMode() {
		ses.SetServerStatus(SERVER_STATUS_IN_TRANS)
	}
	if !ses.GetTxnHandler().IsValidTxnOperator() {
		err = ses.GetTxnHandler().NewTxn()
	}
	return err
}

/*
TxnBegin begins a new transaction.
It commits the current transaction implicitly.
*/
func (ses *Session) TxnBegin() error {
	var err error
	if ses.InMultiStmtTransactionMode() {
		ses.ClearServerStatus(SERVER_STATUS_IN_TRANS)
		err = ses.GetTxnHandler().CommitTxn()
	}
	ses.ClearOptionBits(OPTION_BEGIN)
	if err != nil {
		/*
			fix issue 6024.
			When we get a w-w conflict during commit the txn,
			we convert the error into a readable error.
		*/
		if moerr.IsMoErrCode(err, moerr.ErrTxnWWConflict) {
			return moerr.NewInternalError(ses.GetRequestContext(), writeWriteConflictsErrorInfo())
		}
		return err
	}
	ses.SetOptionBits(OPTION_BEGIN)
	ses.SetServerStatus(SERVER_STATUS_IN_TRANS)
	err = ses.GetTxnHandler().NewTxn()
	return err
}

// TxnCommit commits the current transaction.
func (ses *Session) TxnCommit() error {
	var err error
	ses.ClearServerStatus(SERVER_STATUS_IN_TRANS | SERVER_STATUS_IN_TRANS_READONLY)
	err = ses.GetTxnHandler().CommitTxn()
	ses.ClearServerStatus(SERVER_STATUS_IN_TRANS)
	ses.ClearOptionBits(OPTION_BEGIN)
	return err
}

// TxnRollback rollbacks the current transaction.
func (ses *Session) TxnRollback() error {
	var err error
	ses.ClearServerStatus(SERVER_STATUS_IN_TRANS | SERVER_STATUS_IN_TRANS_READONLY)
	err = ses.GetTxnHandler().RollbackTxn()
	ses.ClearOptionBits(OPTION_BEGIN)
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
func (ses *Session) TxnCommitSingleStatement(stmt tree.Statement) error {
	var err error
	/*
		Commit Rules:
		1, if it is in single-statement mode:
			it commits.
		2, if it is in multi-statement mode:
			if the statement is the one can be executed in the active transaction,
				the transaction need to be committed at the end of the statement.
	*/
	if !ses.InMultiStmtTransactionMode() ||
		ses.InActiveTransaction() && NeedToBeCommittedInActiveTransaction(stmt) {
		err = ses.GetTxnHandler().CommitTxn()
		ses.ClearServerStatus(SERVER_STATUS_IN_TRANS)
		ses.ClearOptionBits(OPTION_BEGIN)
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
func (ses *Session) TxnRollbackSingleStatement(stmt tree.Statement) error {
	var err error
	/*
			Rollback Rules:
			1, if it is in single-statement mode (Case2):
				it rollbacks.
			2, if it is in multi-statement mode (Case1,Case3,Case4):
		        the transaction need to be rollback at the end of the statement.
				(every error will abort the transaction.)
	*/
	if !ses.InMultiStmtTransactionMode() ||
		ses.InActiveTransaction() {
		err = ses.GetTxnHandler().RollbackTxn()
		ses.ClearServerStatus(SERVER_STATUS_IN_TRANS)
		ses.ClearOptionBits(OPTION_BEGIN)
	}
	return err
}

/*
InActiveTransaction checks if it is in an active transaction.
*/
func (ses *Session) InActiveTransaction() bool {
	if ses.InActiveMultiStmtTransaction() {
		return true
	} else {
		return ses.GetTxnHandler().IsValidTxnOperator()
	}
}

/*
SetAutocommit sets the value of the system variable 'autocommit'.

The rule is that we can not execute the statement 'set parameter = value' in
an active transaction whichever it is started by BEGIN or in 'set autocommit = 0;'.
*/
func (ses *Session) SetAutocommit(on bool) error {
	if ses.InActiveTransaction() {
		return moerr.NewInternalError(ses.requestCtx, parameterModificationInTxnErrorInfo())
	}
	if on {
		ses.ClearOptionBits(OPTION_BEGIN | OPTION_NOT_AUTOCOMMIT)
		ses.SetServerStatus(SERVER_STATUS_AUTOCOMMIT)
	} else {
		ses.ClearServerStatus(SERVER_STATUS_AUTOCOMMIT)
		ses.SetOptionBits(OPTION_NOT_AUTOCOMMIT)
	}
	return nil
}
