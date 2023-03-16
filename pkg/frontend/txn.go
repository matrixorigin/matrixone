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
	"github.com/matrixorigin/matrixone/pkg/txn/client"
	"github.com/matrixorigin/matrixone/pkg/util/metric"
	"github.com/matrixorigin/matrixone/pkg/vm/engine"
	"sync"
)

type TxnHandler struct {
	storage   engine.Engine
	txnClient TxnClient
	ses       *Session
	txn       TxnOperator
	mu        sync.Mutex
	entryMu   sync.Mutex
}

func InitTxnHandler(storage engine.Engine, txnClient TxnClient) *TxnHandler {
	h := &TxnHandler{
		storage:   &engine.EntireEngine{Engine: storage},
		txnClient: txnClient,
	}
	return h
}

// we don't need to lock. TxnHandler is holded by one session.
func (th *TxnHandler) SetTempEngine(te engine.Engine) {
	ee := th.storage.(*engine.EntireEngine)
	ee.TempEngine = te
}

func (th *TxnHandler) GetTxnClient() TxnClient {
	th.mu.Lock()
	defer th.mu.Unlock()
	return th.txnClient
}

// TxnClientNew creates a new txn
func (th *TxnHandler) TxnClientNew() error {
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

	th.txn, err = th.txnClient.New(opts...)
	if err != nil {
		return err
	}
	if th.txn == nil {
		return moerr.NewInternalError(th.ses.GetRequestContext(), "TxnClientNew: txnClient new a null txn")
	}
	return err
}

// NewTxn commits the old transaction if it existed.
// Then it creates the new transaction.
func (th *TxnHandler) NewTxn() error {
	var err error
	ctx := th.GetSession().GetRequestContext()
	if th.IsValidTxn() {
		err = th.CommitTxn()
		if err != nil {
			/*
				fix issue 6024.
				When we get a w-w conflict during commit the txn,
				we convert the error into a readable error.
			*/
			if moerr.IsMoErrCode(err, moerr.ErrTxnWWConflict) {
				return moerr.NewInternalError(ctx, writeWriteConflictsErrorInfo())
			}
			return err
		}
	}
	th.SetInvalid()
	defer func() {
		if err != nil {
			tenant := th.ses.GetTenantName(nil)
			incTransactionErrorsCounter(tenant, metric.SQLTypeBegin)
		}
	}()
	err = th.TxnClientNew()
	if err != nil {
		return err
	}
	if ctx == nil {
		panic("context should not be nil")
	}
	storage := th.GetStorage()
	err = storage.New(ctx, th.GetTxnOperator())
	return err
}

// IsValidTxn checks the transaction is true or not.
func (th *TxnHandler) IsValidTxn() bool {
	th.mu.Lock()
	defer th.mu.Unlock()
	return th.txn != nil
}

func (th *TxnHandler) SetInvalid() {
	th.mu.Lock()
	defer th.mu.Unlock()
	th.txn = nil
}

func (th *TxnHandler) GetTxnOperator() TxnOperator {
	th.mu.Lock()
	defer th.mu.Unlock()
	return th.txn
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
	if !th.IsValidTxn() {
		return nil
	}
	ses := th.GetSession()
	sessionProfile := ses.GetConciseProfile()
	ctx := ses.GetRequestContext()
	if ctx == nil {
		panic("context should not be nil")
	}
	if ses.tempTablestorage != nil {
		ctx = context.WithValue(ctx, defines.TemporaryDN{}, ses.tempTablestorage)
	}
	storage := th.GetStorage()
	ctx, cancel := context.WithTimeout(
		ctx,
		storage.Hints().CommitOrRollbackTimeout,
	)
	defer cancel()
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
		logErrorf(sessionProfile, "CommitTxn: txn operator is null")
	}

	txnId := txnOp.Txn().DebugString()
	logDebugf(sessionProfile, "CommitTxn txnId:%s", txnId)
	defer func() {
		logDebugf(sessionProfile, "CommitTxn exit txnId:%s", txnId)
	}()
	if err = storage.Commit(ctx, txnOp); err != nil {
		th.SetInvalid()
		logErrorf(sessionProfile, "CommitTxn: storage commit failed. txnId:%s error:%v", txnId, err)
		if txnOp != nil {
			err2 = txnOp.Rollback(ctx)
			if err2 != nil {
				logErrorf(sessionProfile, "CommitTxn: txn operator rollback failed. txnId:%s error:%v", txnId, err2)
			}
		}
		return err
	}
	if txnOp != nil {
		err = txnOp.Commit(ctx)
		if err != nil {
			th.SetInvalid()
			logErrorf(sessionProfile, "CommitTxn: txn operator commit failed. txnId:%s error:%v", txnId, err)
		}
	}
	th.SetInvalid()
	return err
}

func (th *TxnHandler) RollbackTxn() error {
	th.entryMu.Lock()
	defer th.entryMu.Unlock()
	if !th.IsValidTxn() {
		return nil
	}
	ses := th.GetSession()
	sessionProfile := ses.GetConciseProfile()
	ctx := ses.GetRequestContext()
	if ctx == nil {
		panic("context should not be nil")
	}
	if ses.tempTablestorage != nil {
		ctx = context.WithValue(ctx, defines.TemporaryDN{}, ses.tempTablestorage)
	}
	storage := th.GetStorage()
	ctx, cancel := context.WithTimeout(
		ctx,
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
		logErrorf(sessionProfile, "RollbackTxn: txn operator is null")
	}
	txnId := txnOp.Txn().DebugString()
	logDebugf(sessionProfile, "RollbackTxn txnId:%s", txnId)
	defer func() {
		logDebugf(sessionProfile, "RollbackTxn exit txnId:%s", txnId)
	}()
	if err = storage.Rollback(ctx, txnOp); err != nil {
		th.SetInvalid()
		logErrorf(sessionProfile, "RollbackTxn: storage rollback failed. txnId:%s error:%v", txnId, err)
		if txnOp != nil {
			err2 = txnOp.Rollback(ctx)
			if err2 != nil {
				logErrorf(sessionProfile, "RollbackTxn: txn operator rollback failed. txnId:%s error:%v", txnId, err2)
			}
		}
		return err
	}
	if txnOp != nil {
		err = txnOp.Rollback(ctx)
		if err != nil {
			th.SetInvalid()
			logErrorf(sessionProfile, "RollbackTxn: txn operator commit failed. txnId:%s error:%v", txnId, err)
		}
	}
	th.SetInvalid()
	return err
}

func (th *TxnHandler) GetStorage() engine.Engine {
	th.mu.Lock()
	defer th.mu.Unlock()
	return th.storage
}

func (th *TxnHandler) GetTxn() (TxnOperator, error) {
	ses := th.GetSession()
	err := ses.TxnStart()
	if err != nil {
		logErrorf(ses.GetConciseProfile(), "GetTxn. error:%v", err)
		return nil, err
	}
	return th.GetTxnOperator(), nil
}

func (th *TxnHandler) GetTxnOnly() TxnOperator {
	th.mu.Lock()
	defer th.mu.Unlock()
	return th.txn
}
