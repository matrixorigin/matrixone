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
	"encoding/binary"
	"fmt"
	"sync"

	"github.com/matrixorigin/matrixone/pkg/common/moerr"
	"github.com/matrixorigin/matrixone/pkg/container/types"

	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/iface/txnif"
)

func IDToIDCtx(id uint64) []byte {
	ctx := make([]byte, 8)
	binary.BigEndian.PutUint64(ctx, id)
	return ctx
}

func IDCtxToID(buf []byte) uint64 {
	return binary.BigEndian.Uint64(buf)
}

type TxnCtx struct {
	sync.RWMutex
	DoneCond                     sync.Cond
	ID                           uint64
	IDCtx                        []byte
	StartTS, CommitTS, PrepareTS types.TS
	Info                         []byte
	State                        txnif.TxnState
	Kind2PC                      bool
}

func NewTxnCtx(id uint64, start types.TS, info []byte) *TxnCtx {
	ctx := &TxnCtx{
		ID:        id,
		IDCtx:     IDToIDCtx(id),
		StartTS:   start,
		PrepareTS: txnif.UncommitTS,
		CommitTS:  txnif.UncommitTS,
		Info:      info,
	}
	ctx.DoneCond = *sync.NewCond(ctx)
	return ctx
}

func (ctx *TxnCtx) Is2PC() bool { return ctx.Kind2PC }

func (ctx *TxnCtx) GetCtx() []byte {
	return ctx.IDCtx
}

func (ctx *TxnCtx) Repr() string {
	ctx.RLock()
	defer ctx.RUnlock()
	repr := fmt.Sprintf("ctx[%d][%d->%d][%s]", ctx.ID, ctx.StartTS, ctx.CommitTS, txnif.TxnStrState(ctx.State))
	return repr
}

func (ctx *TxnCtx) SameTxn(startTs types.TS) bool { return ctx.StartTS.Equal(startTs) }
func (ctx *TxnCtx) CommitBefore(startTs types.TS) bool {
	return ctx.GetCommitTS().Less(startTs)
}
func (ctx *TxnCtx) CommitAfter(startTs types.TS) bool {
	return ctx.GetCommitTS().Greater(startTs)
}

func (ctx *TxnCtx) String() string       { return ctx.Repr() }
func (ctx *TxnCtx) GetID() uint64        { return ctx.ID }
func (ctx *TxnCtx) GetInfo() []byte      { return ctx.Info }
func (ctx *TxnCtx) GetStartTS() types.TS { return ctx.StartTS }
func (ctx *TxnCtx) GetCommitTS() types.TS {
	ctx.RLock()
	defer ctx.RUnlock()
	return ctx.CommitTS
}
func (ctx *TxnCtx) GetPrepareTS() types.TS {
	ctx.RLock()
	defer ctx.RUnlock()
	return ctx.PrepareTS
}

// Atomically returns the current txn state
func (ctx *TxnCtx) getTxnState() txnif.TxnState {
	ctx.RLock()
	defer ctx.RUnlock()
	return ctx.State
}

// Wait txn state to be rollbacked or committed
func (ctx *TxnCtx) resolveTxnState() txnif.TxnState {
	ctx.DoneCond.L.Lock()
	defer ctx.DoneCond.L.Unlock()
	state := ctx.State
	if state != txnif.TxnStatePreparing {
		return state
	}
	ctx.DoneCond.Wait()
	return ctx.State
}

// False when atomically get the current txn state
//
// True when the txn state is committing, wait it to be committed or rollbacked. It
// is used during snapshot reads. If TxnStateActive is currently returned, this value will
// definitely not be used, because even if it becomes TxnStatePreparing later, the timestamp
// would be larger than the current read timestamp.
func (ctx *TxnCtx) GetTxnState(waitIfCommitting bool) (state txnif.TxnState) {
	// Quick get the current txn state
	// If waitIfCommitting is false, return the state
	// If state is not txnif.TxnStatePreparing, return the state
	if state = ctx.getTxnState(); !waitIfCommitting || state != txnif.TxnStatePreparing {
		return state
	}

	// Wait the committing txn to be committed or rollbacked
	state = ctx.resolveTxnState()
	return
}

func (ctx *TxnCtx) IsVisible(o txnif.TxnReader) bool {
	ostart := o.GetStartTS()
	ctx.RLock()
	defer ctx.RUnlock()
	return ostart.LessEq(ctx.StartTS)
}

func (ctx *TxnCtx) IsActiveLocked() bool {
	return ctx.CommitTS == txnif.UncommitTS
}

func (ctx *TxnCtx) ToPreparingLocked(ts types.TS) error {
	if ts.LessEq(ctx.StartTS) {
		panic(fmt.Sprintf("start ts %d should be less than commit ts %d", ctx.StartTS, ts))
	}
	if !ctx.CommitTS.Equal(txnif.UncommitTS) {
		return moerr.NewTxnNotActive("")
	}
	ctx.PrepareTS = ts
	ctx.CommitTS = ts
	ctx.State = txnif.TxnStatePreparing
	return nil
}

func (ctx *TxnCtx) ToPrepared() (err error) {
	ctx.Lock()
	defer ctx.Unlock()
	return ctx.ToPreparedLocked()
}

func (ctx *TxnCtx) ToPreparedLocked() (err error) {
	if ctx.State != txnif.TxnStatePreparing {
		err = moerr.NewTAEPrepare("ToPreparedLocked: state is not preparing")
		return
	}
	ctx.State = txnif.TxnStatePrepared
	return
}

func (ctx *TxnCtx) ToCommittingFinished() (err error) {
	ctx.Lock()
	defer ctx.Unlock()
	return ctx.ToCommittingFinishedLocked()
}

func (ctx *TxnCtx) ToCommittingFinishedLocked() (err error) {
	if ctx.State != txnif.TxnStatePrepared {
		err = moerr.NewTAECommit("ToCommittingFinishedLocked: state is not prepared")
		return
	}
	ctx.State = txnif.TxnStateCommittingFinished
	return
}

func (ctx *TxnCtx) ToCommittedLocked() error {
	if ctx.State != txnif.TxnStatePreparing {
		return moerr.NewTAECommit("ToCommittedLocked: state is not preparing")
	}
	ctx.State = txnif.TxnStateCommitted
	return nil
}

func (ctx *TxnCtx) ToRollbacking(ts types.TS) error {
	ctx.Lock()
	defer ctx.Unlock()
	return ctx.ToRollbackingLocked(ts)
}

func (ctx *TxnCtx) ToRollbackingLocked(ts types.TS) error {
	if ts.Less(ctx.StartTS) {
		panic(fmt.Sprintf("commit ts %d should not be less than start ts %d", ts, ctx.StartTS))
	}
	if (ctx.State != txnif.TxnStateActive) && (ctx.State != txnif.TxnStatePreparing) {
		return moerr.NewTAERollback("ToRollbackingLocked: state is not active or preparing")
	}
	ctx.CommitTS = ts
	ctx.PrepareTS = ts
	ctx.State = txnif.TxnStateRollbacking
	return nil
}

func (ctx *TxnCtx) ToRollbackedLocked() error {
	if ctx.State != txnif.TxnStateRollbacking {
		return moerr.NewTAERollback("state %s", txnif.TxnStrState(ctx.State))
	}
	ctx.State = txnif.TxnStateRollbacked
	return nil
}

func (ctx *TxnCtx) ToUnknownLocked() {
	ctx.State = txnif.TxnStateUnknown
}

// MockSetCommitTSLocked is for testing
func (ctx *TxnCtx) MockSetCommitTSLocked(ts types.TS) { ctx.CommitTS = ts }
