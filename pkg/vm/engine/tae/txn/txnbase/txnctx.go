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

package txnbase

import (
	"fmt"
	"sync"

	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/iface/txnif"
)

type TxnCtx struct {
	*sync.RWMutex
	ID                uint64
	StartTS, CommitTS uint64
	Info              []byte
	State             int32
}

func NewTxnCtx(rwlocker *sync.RWMutex, id, start uint64, info []byte) *TxnCtx {
	if rwlocker == nil {
		rwlocker = new(sync.RWMutex)
	}
	return &TxnCtx{
		ID:       id,
		RWMutex:  rwlocker,
		StartTS:  start,
		CommitTS: txnif.UncommitTS,
		Info:     info,
	}
}

func (ctx *TxnCtx) Repr() string {
	ctx.RLock()
	defer ctx.RUnlock()
	repr := fmt.Sprintf("Txn[%d][%d->%d][%s]", ctx.ID, ctx.StartTS, ctx.CommitTS, txnif.TxnStrState(ctx.State))
	return repr
}

func (ctx *TxnCtx) String() string     { return ctx.Repr() }
func (ctx *TxnCtx) GetID() uint64      { return ctx.ID }
func (ctx *TxnCtx) GetInfo() []byte    { return ctx.Info }
func (ctx *TxnCtx) GetStartTS() uint64 { return ctx.StartTS }
func (ctx *TxnCtx) GetCommitTS() uint64 {
	ctx.RLock()
	defer ctx.RUnlock()
	return ctx.CommitTS
}

func (ctx *TxnCtx) IsVisible(o txnif.TxnReader) bool {
	ostart := o.GetStartTS()
	ctx.RLock()
	defer ctx.RUnlock()
	return ostart <= ctx.StartTS
}

func (ctx *TxnCtx) IsActiveLocked() bool {
	return ctx.CommitTS == txnif.UncommitTS
}

func (ctx *TxnCtx) ToCommittingLocked(ts uint64) error {
	if ts <= ctx.StartTS {
		panic(fmt.Sprintf("start ts %d should be less than commit ts %d", ctx.StartTS, ts))
	}
	if ctx.CommitTS != txnif.UncommitTS {
		return ErrTxnNotActive
	}
	ctx.CommitTS = ts
	ctx.State = txnif.TxnStateCommitting
	return nil
}

func (ctx *TxnCtx) ToCommittedLocked() error {
	if ctx.State != txnif.TxnStateCommitting {
		return ErrTxnNotCommitting
	}
	ctx.State = txnif.TxnStateCommitted
	return nil
}

func (ctx *TxnCtx) ToRollbackingLocked(ts uint64) error {
	if ts <= ctx.StartTS {
		panic(fmt.Sprintf("start ts %d should be less than commit ts %d", ctx.StartTS, ts))
	}
	if (ctx.State != txnif.TxnStateActive) && (ctx.State != txnif.TxnStateCommitting) {
		return ErrTxnCannotRollback
	}
	ctx.CommitTS = ts
	ctx.State = txnif.TxnStateRollbacking
	return nil
}

func (ctx *TxnCtx) ToRollbackedLocked() error {
	if ctx.State != txnif.TxnStateRollbacking {
		return ErrTxnNotRollbacking
	}
	ctx.State = txnif.TxnStateRollbacked
	return nil
}

// For testing
func (ctx *TxnCtx) MockSetCommitTSLocked(ts uint64) { ctx.CommitTS = ts }
