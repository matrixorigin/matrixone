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
	"errors"
	"sync"

	"github.com/google/uuid"
	"go.uber.org/zap"

	"github.com/matrixorigin/matrixone/pkg/clusterservice"
	"github.com/matrixorigin/matrixone/pkg/common/moerr"
	"github.com/matrixorigin/matrixone/pkg/common/mpool"
	moruntime "github.com/matrixorigin/matrixone/pkg/common/runtime"
	"github.com/matrixorigin/matrixone/pkg/defines"
	"github.com/matrixorigin/matrixone/pkg/pb/metadata"
	"github.com/matrixorigin/matrixone/pkg/txn/client"
	"github.com/matrixorigin/matrixone/pkg/txn/clock"
	"github.com/matrixorigin/matrixone/pkg/txn/storage/memorystorage"
	"github.com/matrixorigin/matrixone/pkg/util/metric"
	"github.com/matrixorigin/matrixone/pkg/util/trace"
	"github.com/matrixorigin/matrixone/pkg/vm/engine"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/disttae"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/memoryengine"
)

var (
	dumpUUID = uuid.UUID{}
)

// get errors during the transaction. rollback the transaction
func rollbackTxnFunc(ses FeSession, execErr error, execCtx *ExecCtx) error {
	execCtx.ses.EnterFPrint(88)
	defer execCtx.ses.ExitFPrint(88)
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
	if ses.GetTxnHandler().InMultiStmtTransactionMode() && ses.GetTxnHandler().InActiveTxn() {
		ses.cleanCache()
	}
	ses.Error(execCtx.reqCtx, execErr.Error())
	execCtx.txnOpt.byRollback = execCtx.txnOpt.byRollback || isErrorRollbackWholeTxn(execErr)
	txnErr := ses.GetTxnHandler().Rollback(execCtx)
	if txnErr != nil {
		logStatementStatus(execCtx.reqCtx, ses, execCtx.stmt, fail, txnErr)
		return txnErr
	}
	logStatementStatus(execCtx.reqCtx, ses, execCtx.stmt, fail, execErr)
	return execErr
}

// execution succeeds during the transaction. commit the transaction
func commitTxnFunc(ses FeSession,
	execCtx *ExecCtx) (retErr error) {
	execCtx.ses.EnterFPrint(87)
	defer execCtx.ses.ExitFPrint(87)
	// Call a defer function -- if TxnCommitSingleStatement paniced, we
	// want to catch it and convert it to an error.
	defer func() {
		if r := recover(); r != nil {
			retErr = moerr.ConvertPanicError(execCtx.reqCtx, r)
		}
	}()

	//load data handle txn failure internally
	retErr = ses.GetTxnHandler().Commit(execCtx)
	if retErr != nil {
		logStatementStatus(execCtx.reqCtx, ses, execCtx.stmt, fail, retErr)
	}
	return
}

// finish the transaction
func finishTxnFunc(ses FeSession, execErr error, execCtx *ExecCtx) (err error) {
	ses.EnterFPrint(74)
	defer ses.ExitFPrint(74)
	// First recover all panics.   If paniced, we will abort.
	if r := recover(); r != nil {
		recoverErr := moerr.ConvertPanicError(execCtx.reqCtx, r)
		ses.Error(execCtx.reqCtx, "recover from panic", zap.Error(recoverErr), zap.Error(execErr))
	}

	if execCtx.txnOpt.byCommit {
		//commit the txn by the COMMIT statement
		err = ses.GetTxnHandler().Commit(execCtx)
		if err != nil {
			logStatementStatus(execCtx.reqCtx, ses, execCtx.stmt, fail, err)
		}
	} else if execCtx.txnOpt.byRollback {
		//roll back the txn by the ROLLBACK statement
		err = ses.GetTxnHandler().Rollback(execCtx)
		if err != nil {
			logStatementStatus(execCtx.reqCtx, ses, execCtx.stmt, fail, err)
			return err
		}
	} else {
		if execErr == nil {
			err = commitTxnFunc(ses, execCtx)
			if err == nil {
				return err
			}
			// if commitTxnFunc failed, we will roll back the transaction.
			execErr = err
		}

		return rollbackTxnFunc(ses, execErr, execCtx)
	}
	return
}

type FeTxnOption struct {
	//byBegin denotes the txn started by the BEGIN stmt
	byBegin bool
	//autoCommit the variable AUTOCOMMIT is enabled
	autoCommit bool
	//byCommit denotes the txn committed by the COMMIT
	byCommit bool
	//byRollback denotes the txn rolled back by the ROLLBACK.
	//or error types that need to roll back the whole txn.
	byRollback bool
}

const (
	defaultServerStatus uint32 = uint32(SERVER_STATUS_AUTOCOMMIT)
	defaultOptionBits   uint32 = OPTION_AUTOCOMMIT
)

type TxnHandler struct {
	mu sync.Mutex

	storage       engine.Engine
	tempStorage   *memorystorage.Storage
	tempTnService *metadata.TNService
	tempEngine    *memoryengine.Engine
	txnOp         TxnOperator

	//connCtx is the ancestor of the txnCtx.
	//it is initialized at the TxnHandler object created and
	//exists always.
	//it starts from the routineCtx.
	connCtx context.Context

	// it is for the transaction and different from the requestCtx.
	// it is created before the transaction is started and
	// is not released after the transaction is commit or rollback.
	// the lifetime of txnCtx is longer than the requestCtx and
	// the same as the connCtx.
	// it inherits the connCtx.
	// it can not be canceled at the KillQuery
	txnCtx       context.Context
	txnCtxCancel context.CancelFunc

	shareTxn bool

	//the server status
	serverStatus uint32

	//the option bits
	optionBits uint32
}

func InitTxnHandler(storage engine.Engine, connCtx context.Context, txnOp TxnOperator) *TxnHandler {
	ret := &TxnHandler{
		storage:      &engine.EntireEngine{Engine: storage},
		connCtx:      connCtx,
		txnOp:        txnOp,
		shareTxn:     txnOp != nil,
		serverStatus: defaultServerStatus,
		optionBits:   defaultOptionBits,
	}
	ret.txnCtx, ret.txnCtxCancel = context.WithCancel(connCtx)
	return ret
}

func (th *TxnHandler) Close() {
	th.mu.Lock()
	defer th.mu.Unlock()
	th.storage = nil
	th.tempStorage = nil
	th.tempTnService = nil
	th.tempEngine = nil
	th.txnOp = nil
	th.connCtx = nil
	if th.txnCtxCancel != nil {
		th.txnCtxCancel()
	}
	th.txnCtx = nil
}

func (th *TxnHandler) GetConnCtx() context.Context {
	th.mu.Lock()
	defer th.mu.Unlock()
	return th.connCtx
}

func (th *TxnHandler) GetTxnCtx() context.Context {
	th.mu.Lock()
	defer th.mu.Unlock()
	return th.txnCtx
}

// invalidateTxnUnsafe releases the txnOp and clears the server status bit SERVER_STATUS_IN_TRANS
func (th *TxnHandler) invalidateTxnUnsafe() {
	th.txnOp = nil
	resetBits(&th.serverStatus, defaultServerStatus)
	resetBits(&th.optionBits, defaultOptionBits)
}

func (th *TxnHandler) InActiveTxn() bool {
	th.mu.Lock()
	defer th.mu.Unlock()
	return th.inActiveTxnUnsafe()
}

// inActiveTxnUnsafe can not be used outside the TxnHandler.
// refresh server status also
func (th *TxnHandler) inActiveTxnUnsafe() bool {
	if th.txnOp != nil && th.txnCtx == nil {
		panic("txnOp != nil and txnCtx == nil")
	}
	return th.txnOp != nil && th.txnCtx != nil
}

// Create starts a new txn.
// option bits decide the actual behaviour
func (th *TxnHandler) Create(execCtx *ExecCtx) error {
	var err error
	th.mu.Lock()
	defer th.mu.Unlock()

	// check BEGIN stmt
	if execCtx.txnOpt.byBegin || !th.inActiveTxnUnsafe() {
		//commit existed txn anyway
		err = th.createUnsafe(execCtx)
		if err != nil {
			return err
		}
		resetBits(&th.serverStatus, defaultServerStatus)
		resetBits(&th.optionBits, defaultOptionBits)
		setBits(&th.serverStatus, uint32(SERVER_STATUS_IN_TRANS))

		if execCtx.txnOpt.byBegin {
			setBits(&th.optionBits, OPTION_BEGIN)
		} else {
			clearBits(&th.optionBits, OPTION_BEGIN)
		}

		if execCtx.txnOpt.autoCommit {
			clearBits(&th.optionBits, OPTION_NOT_AUTOCOMMIT)
			setBits(&th.serverStatus, uint32(SERVER_STATUS_AUTOCOMMIT))
		} else {
			setBits(&th.optionBits, OPTION_NOT_AUTOCOMMIT)
			clearBits(&th.serverStatus, uint32(SERVER_STATUS_AUTOCOMMIT))
		}
	}
	return nil
}

// starts a new txn.
// if there is a txn existed, commit it before creating a new one.
func (th *TxnHandler) createUnsafe(execCtx *ExecCtx) error {
	var err error
	defer th.inActiveTxnUnsafe()
	if th.shareTxn {
		return moerr.NewInternalError(execCtx.reqCtx, "NewTxn: the share txn is not allowed to create new txn")
	}

	//in active txn
	//commit existed txn first
	err = th.commitUnsafe(execCtx)
	if err != nil {
		/*
			fix issue 6024.
			When we get a w-w conflict during commit the txn,
			we convert the error into a readable error.
		*/
		if moerr.IsMoErrCode(err, moerr.ErrTxnWWConflict) {
			return moerr.NewInternalError(execCtx.reqCtx, writeWriteConflictsErrorInfo())
		}
		return err
	}

	defer func() {
		if err != nil {
			tenant := execCtx.tenant
			incTransactionErrorsCounter(tenant, metric.SQLTypeBegin)
		}
	}()
	err = th.createTxnOpUnsafe(execCtx)
	if err != nil {
		return err
	}
	if th.txnCtx == nil {
		panic("context should not be nil")
	}
	var accId uint32
	accId, err = defines.GetAccountId(execCtx.reqCtx)
	if err != nil {
		return err
	}
	tempCtx := defines.AttachAccountId(th.txnCtx, accId)
	err = th.storage.New(tempCtx, th.txnOp)
	if err != nil {
		execCtx.ses.SetTxnId(dumpUUID[:])
	} else {
		execCtx.ses.SetTxnId(th.txnOp.Txn().ID)
		err = disttae.CheckTxnIsValid(th.txnOp)
		if err != nil {
			return err
		}
	}
	return err
}

// createTxnOpUnsafe creates a new txn operator using TxnClient. Should not be called outside txn
func (th *TxnHandler) createTxnOpUnsafe(execCtx *ExecCtx) error {
	var err error
	if getGlobalPu().TxnClient == nil {
		panic("must set txn client")
	}

	if th.shareTxn {
		return moerr.NewInternalError(execCtx.reqCtx, "NewTxnOperator: the share txn is not allowed to create new txn")
	}

	var opts []client.TxnOption
	rt := moruntime.ProcessLevelRuntime()
	if rt != nil {
		if v, ok := rt.GetGlobalVariables(moruntime.TxnOptions); ok {
			opts = v.([]client.TxnOption)
		}
	}
	if th.txnCtx == nil {
		panic("context should not be nil")
	}

	accountID := uint32(0)
	userName := ""
	connectionID := uint32(0)
	if execCtx.resper != nil {
		connectionID = execCtx.resper.GetU32(CONNID)
	}
	if execCtx.ses.GetTenantInfo() != nil {
		accountID = execCtx.ses.GetTenantInfo().TenantID
		userName = execCtx.ses.GetTenantInfo().User
	}
	sessionInfo := execCtx.ses.GetDebugString()
	opts = append(opts,
		client.WithTxnCreateBy(
			accountID,
			userName,
			execCtx.ses.GetUUIDString(),
			connectionID),
		client.WithSessionInfo(sessionInfo),
		client.WithBeginAutoCommit(execCtx.txnOpt.byBegin, execCtx.txnOpt.autoCommit))

	if execCtx.ses.GetFromRealUser() {
		opts = append(opts,
			client.WithUserTxn())
	}

	if execCtx.ses.IsBackgroundSession() ||
		execCtx.ses.DisableTrace() {
		opts = append(opts, client.WithDisableTrace(true))
	} else {
		varVal, err := execCtx.ses.GetSessionSysVar("disable_txn_trace")
		if err != nil {
			return err
		}
		if def, ok := gSysVarsDefs["disable_txn_trace"]; ok {
			if boolType, ok := def.GetType().(SystemVariableBoolType); ok {
				if boolType.IsTrue(varVal) {
					opts = append(opts, client.WithDisableTrace(true))
				}
			}
		}
	}

	th.txnOp, err = getGlobalPu().TxnClient.New(
		th.txnCtx,
		execCtx.ses.getLastCommitTS(),
		opts...)
	if err != nil {
		return err
	}
	if th.txnOp == nil {
		return moerr.NewInternalError(execCtx.reqCtx, "NewTxnOperator: txnClient new a null txn")
	}
	setFPrints(th.txnOp, execCtx.ses.GetFPrints())
	return err
}

func (th *TxnHandler) GetTxn() TxnOperator {
	th.mu.Lock()
	defer th.mu.Unlock()
	return th.txnOp
}

// Commit commits the txn.
// option bits decide the actual commit behaviour
func (th *TxnHandler) Commit(execCtx *ExecCtx) error {
	execCtx.ses.EnterFPrint(75)
	defer execCtx.ses.ExitFPrint(75)
	var err error
	th.mu.Lock()
	defer th.mu.Unlock()
	/*
		Commit Rules:
		1, if it is in single-statement mode:
			it commits.
		2, if it is in multi-statement mode:
			if the statement is the one can be executed in the active transaction,
				the transaction need to be committed at the end of the statement.
	*/
	if !bitsIsSet(th.optionBits, OPTION_BEGIN|OPTION_NOT_AUTOCOMMIT) ||
		th.inActiveTxnUnsafe() && NeedToBeCommittedInActiveTransaction(execCtx.stmt) ||
		execCtx.txnOpt.byCommit {
		execCtx.ses.EnterFPrint(76)
		defer execCtx.ses.ExitFPrint(76)
		err = th.commitUnsafe(execCtx)
		if err != nil {
			return err
		}
	}
	//do nothing
	return nil
}

func (th *TxnHandler) commitUnsafe(execCtx *ExecCtx) error {
	execCtx.ses.EnterFPrint(77)
	defer execCtx.ses.ExitFPrint(77)
	_, span := trace.Start(execCtx.reqCtx, "TxnHandler.CommitTxn",
		trace.WithKind(trace.SpanKindStatement))
	defer span.End(trace.WithStatementExtra(execCtx.ses.GetTxnId(), execCtx.ses.GetStmtId(), execCtx.ses.GetSqlOfStmt()))
	var err error
	defer th.inActiveTxnUnsafe()
	if !th.inActiveTxnUnsafe() || th.shareTxn {
		return nil
	}
	if th.txnOp == nil {
		th.invalidateTxnUnsafe()
	}
	if th.txnCtx == nil {
		panic("context should not be nil")
	}
	if th.hasTempEngineUnsafe() && th.tempStorage != nil {
		if th.txnCtx.Value(defines.TemporaryTN{}) == nil {
			th.txnCtx = context.WithValue(th.txnCtx, defines.TemporaryTN{}, th.tempStorage)
		}
	}
	storage := th.storage
	ctx2, cancel := context.WithTimeout(
		th.txnCtx,
		storage.Hints().CommitOrRollbackTimeout,
	)
	defer cancel()
	val, e := execCtx.ses.GetSessionSysVar("mo_pk_check_by_dn")
	if e != nil {
		return e
	}
	if val != nil {
		ctx2 = context.WithValue(ctx2, defines.PkCheckByTN{}, val.(int8))
	}
	defer func() {
		// metric count
		tenant := execCtx.ses.GetTenantName()
		incTransactionCounter(tenant)
		if err != nil {
			incTransactionErrorsCounter(tenant, metric.SQLTypeCommit)
		}
	}()

	if execCtx.ses.GetLogLevel().Enabled(zap.DebugLevel) {
		txnId := th.txnOp.Txn().DebugString()
		execCtx.ses.Debugf(execCtx.reqCtx, "CommitTxn txnId:%s", txnId)
		defer func() {
			execCtx.ses.Debugf(execCtx.reqCtx, "CommitTxn exit txnId:%s", txnId)
		}()
	}
	execCtx.ses.EnterFPrint(78)
	defer execCtx.ses.ExitFPrint(78)
	if th.txnOp != nil {
		execCtx.ses.EnterFPrint(79)
		defer execCtx.ses.ExitFPrint(79)
		commitTs := th.txnOp.Txn().CommitTS
		execCtx.ses.SetTxnId(th.txnOp.Txn().ID)
		err = th.txnOp.Commit(ctx2)
		if err != nil {
			th.invalidateTxnUnsafe()
		}
		execCtx.ses.updateLastCommitTS(commitTs)
	}
	th.invalidateTxnUnsafe()
	execCtx.ses.SetTxnId(dumpUUID[:])
	return err
}

// Rollback rolls back the txn
// the option bits decide the actual behavior
func (th *TxnHandler) Rollback(execCtx *ExecCtx) error {
	execCtx.ses.EnterFPrint(80)
	defer execCtx.ses.ExitFPrint(80)
	var err error
	th.mu.Lock()
	defer th.mu.Unlock()
	/*
			Rollback Rules:
			1, if it is in single-statement mode (Case2):
				it rollbacks.
			2, if it is in multi-statement mode (Case1,Case3,Case4):
		        the transaction need to be rollback at the end of the statement.
				(every error will abort the transaction.)
	*/
	if !bitsIsSet(th.optionBits, OPTION_BEGIN|OPTION_NOT_AUTOCOMMIT) ||
		th.inActiveTxnUnsafe() && NeedToBeCommittedInActiveTransaction(execCtx.stmt) ||
		execCtx.txnOpt.byRollback {
		execCtx.ses.EnterFPrint(81)
		defer execCtx.ses.ExitFPrint(81)
		//Case1.1: autocommit && not_begin
		//Case1.2: (not_autocommit || begin) && activeTxn && needToBeCommitted
		//Case1.3: the error that should rollback the whole txn
		err = th.rollbackUnsafe(execCtx)
	} else {
		//Case2: not ( autocommit && !begin ) && not ( activeTxn && needToBeCommitted )
		//<==>  ( not_autocommit || begin ) && not ( activeTxn && needToBeCommitted )
		//just rollback statement
		execCtx.ses.EnterFPrint(85)
		defer execCtx.ses.ExitFPrint(85)
		//non derived statement
		if th.txnOp != nil && !execCtx.ses.IsDerivedStmt() {
			err = th.txnOp.GetWorkspace().RollbackLastStatement(th.txnCtx)
			if err != nil {
				err4 := th.rollbackUnsafe(execCtx)
				return errors.Join(err, err4)
			}
		}
	}
	return err
}

func (th *TxnHandler) rollbackUnsafe(execCtx *ExecCtx) error {
	execCtx.ses.EnterFPrint(82)
	defer execCtx.ses.ExitFPrint(82)
	_, span := trace.Start(execCtx.reqCtx, "TxnHandler.RollbackTxn",
		trace.WithKind(trace.SpanKindStatement))
	defer span.End(trace.WithStatementExtra(execCtx.ses.GetTxnId(), execCtx.ses.GetStmtId(), execCtx.ses.GetSqlOfStmt()))
	var err error
	defer th.inActiveTxnUnsafe()
	if !th.inActiveTxnUnsafe() || th.shareTxn {
		return nil
	}

	if th.txnOp == nil {
		th.invalidateTxnUnsafe()
	}
	if th.txnCtx == nil {
		panic("context should not be nil")
	}
	if th.hasTempEngineUnsafe() && th.tempStorage != nil {
		if th.txnCtx.Value(defines.TemporaryTN{}) == nil {
			th.txnCtx = context.WithValue(th.txnCtx, defines.TemporaryTN{}, th.tempStorage)
		}
	}
	ctx2, cancel := context.WithTimeout(
		th.txnCtx,
		th.storage.Hints().CommitOrRollbackTimeout,
	)
	defer cancel()
	defer func() {
		// metric count
		tenant := execCtx.ses.GetTenantName()
		incTransactionCounter(tenant)
		incTransactionErrorsCounter(tenant, metric.SQLTypeOther) // exec rollback cnt
		if err != nil {
			incTransactionErrorsCounter(tenant, metric.SQLTypeRollback)
		}
	}()
	if execCtx.ses.GetLogLevel().Enabled(zap.DebugLevel) {
		txnId := th.txnOp.Txn().DebugString()
		execCtx.ses.Debugf(execCtx.reqCtx, "RollbackTxn txnId:%s", txnId)
		defer func() {
			execCtx.ses.Debugf(execCtx.reqCtx, "RollbackTxn exit txnId:%s", txnId)
		}()
	}
	execCtx.ses.EnterFPrint(83)
	defer execCtx.ses.ExitFPrint(83)
	if th.txnOp != nil {
		execCtx.ses.EnterFPrint(84)
		defer execCtx.ses.ExitFPrint(84)
		execCtx.ses.SetTxnId(th.txnOp.Txn().ID)
		err = th.txnOp.Rollback(ctx2)
		if err != nil {
			th.invalidateTxnUnsafe()
		}
	}
	th.invalidateTxnUnsafe()
	execCtx.ses.SetTxnId(dumpUUID[:])
	return err
}

/*
SetAutocommit sets the value of the system variable 'autocommit'.

It commits the active transaction if the old value is false and the new value is true.
*/
func (th *TxnHandler) SetAutocommit(execCtx *ExecCtx, old, on bool) error {
	execCtx.ses.EnterFPrint(86)
	defer execCtx.ses.ExitFPrint(86)
	th.mu.Lock()
	defer th.mu.Unlock()
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
		err := th.commitUnsafe(execCtx)
		if err != nil {
			clearBits(&th.optionBits, OPTION_AUTOCOMMIT)
			return err
		}
		clearBits(&th.optionBits, OPTION_BEGIN|OPTION_NOT_AUTOCOMMIT)
		setBits(&th.serverStatus, uint32(SERVER_STATUS_AUTOCOMMIT))
	} else if old && !on { //on -> off
		clearBits(&th.optionBits, OPTION_AUTOCOMMIT)
		clearBits(&th.serverStatus, uint32(SERVER_STATUS_AUTOCOMMIT))
		setBits(&th.optionBits, OPTION_NOT_AUTOCOMMIT)
	}
	return nil
}

func (th *TxnHandler) setAutocommitOn() {
	th.mu.Lock()
	defer th.mu.Unlock()
	clearBits(&th.optionBits, OPTION_BEGIN|OPTION_NOT_AUTOCOMMIT)
	setBits(&th.optionBits, OPTION_AUTOCOMMIT)
	setBits(&th.serverStatus, uint32(SERVER_STATUS_AUTOCOMMIT))
}

func (th *TxnHandler) IsShareTxn() bool {
	th.mu.Lock()
	defer th.mu.Unlock()
	return th.shareTxn
}

func (th *TxnHandler) SetOptionBits(bits uint32) {
	th.mu.Lock()
	defer th.mu.Unlock()
	setBits(&th.optionBits, bits)
}

func (th *TxnHandler) GetOptionBits() uint32 {
	th.mu.Lock()
	defer th.mu.Unlock()
	return th.optionBits
}

func (th *TxnHandler) SetServerStatus(status uint16) {
	th.mu.Lock()
	defer th.mu.Unlock()
	setBits(&th.serverStatus, uint32(status))
}

func (th *TxnHandler) GetServerStatus() uint16 {
	th.mu.Lock()
	defer th.mu.Unlock()
	return uint16(th.serverStatus)
}

func (th *TxnHandler) InMultiStmtTransactionMode() bool {
	th.mu.Lock()
	defer th.mu.Unlock()
	return bitsIsSet(th.optionBits, OPTION_NOT_AUTOCOMMIT|OPTION_BEGIN)
}

func (th *TxnHandler) GetStorage() engine.Engine {
	th.mu.Lock()
	defer th.mu.Unlock()
	return th.storage
}

func (th *TxnHandler) HasTempEngine() bool {
	th.mu.Lock()
	defer th.mu.Unlock()
	return th.hasTempEngineUnsafe()
}

func (th *TxnHandler) hasTempEngineUnsafe() bool {
	if entireEng, ok := th.storage.(*engine.EntireEngine); ok {
		return entireEng.TempEngine != nil
	}
	return false
}

func (th *TxnHandler) OptionBitsIsSet(bit uint32) bool {
	th.mu.Lock()
	defer th.mu.Unlock()
	return bitsIsSet(th.optionBits, bit)
}

func (th *TxnHandler) CreateTempStorage(ck clock.Clock) error {
	th.mu.Lock()
	defer th.mu.Unlock()
	return th.createTempStorageUnsafe(ck)
}

func (th *TxnHandler) GetTempStorage() *memorystorage.Storage {
	th.mu.Lock()
	defer th.mu.Unlock()
	if th.tempStorage == nil {
		panic("temp table storage is not initialized")
	}
	return th.tempStorage
}

func (th *TxnHandler) GetTempTNService() *metadata.TNService {
	th.mu.Lock()
	defer th.mu.Unlock()
	return th.tempTnService
}

func (th *TxnHandler) createTempStorageUnsafe(ck clock.Clock) error {
	// Without concurrency, there is no potential for data competition
	// Arbitrary value is OK since it's single sharded. Let's use 0xbeef
	// suggested by @reusee
	shards := []metadata.TNShard{
		{
			ReplicaID:     0xbeef,
			TNShardRecord: metadata.TNShardRecord{ShardID: 0xbeef},
		},
	}
	// Arbitrary value is OK, for more information about TEMPORARY_TABLE_DN_ADDR, please refer to the comment in defines/const.go
	tnAddr := defines.TEMPORARY_TABLE_TN_ADDR
	uid, err := uuid.NewV7()
	if err != nil {
		return err
	}
	th.tempTnService = &metadata.TNService{
		ServiceID:         uid.String(),
		TxnServiceAddress: tnAddr,
		Shards:            shards,
	}

	ms, err := memorystorage.NewMemoryStorage(
		mpool.MustNewZeroNoFixed(),
		ck,
		memoryengine.RandomIDGenerator,
	)
	if err != nil {
		return err
	}
	th.tempStorage = ms
	return nil
}

func (th *TxnHandler) CreateTempEngine() {
	th.mu.Lock()
	defer th.mu.Unlock()

	th.tempEngine = memoryengine.New(
		context.TODO(), //!!!NOTE: memoryengine.New will neglect this context.
		memoryengine.NewDefaultShardPolicy(
			mpool.MustNewZeroNoFixed(),
		),
		memoryengine.RandomIDGenerator,
		clusterservice.NewMOCluster(
			nil,
			0,
			clusterservice.WithDisableRefresh(),
			clusterservice.WithServices(nil, []metadata.TNService{
				*th.tempTnService,
			})),
	)
	updateTempEngine(th.storage, th.tempEngine)
}

func (th *TxnHandler) GetTempEngine() *memoryengine.Engine {
	th.mu.Lock()
	defer th.mu.Unlock()
	return th.tempEngine
}
