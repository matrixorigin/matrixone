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

	"github.com/matrixorigin/matrixone/pkg/common/moerr"
	"github.com/matrixorigin/matrixone/pkg/container/types"

	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/iface/txnif"
)

func IDToIDCtx(id uint64) []byte {
	ctx := make([]byte, 8)
	copy(ctx, types.EncodeUint64(&id))
	return ctx
}

func IDCtxToID(buf []byte) string {
	return string(buf)
}

type TxnCtx struct {
	sync.RWMutex
	sync.WaitGroup
	DoneCond                     sync.Cond
	ID                           string
	IDCtx                        []byte
	StartTS, CommitTS, PrepareTS types.TS

	// SnapshotTS is the specified snapshot timestamp used by this txn
	SnapshotTS types.TS

	State        txnif.TxnState
	Participants []uint64

	// Memo is not thread-safe
	// It will be readonly when txn state is not txnif.TxnStateActive
	Memo *txnif.TxnMemo
}

func NewTxnCtx(id []byte, start, snapshot types.TS) *TxnCtx {
	if snapshot.IsEmpty() {
		snapshot = start
	}
	ctx := &TxnCtx{
		ID:         string(id),
		IDCtx:      id,
		StartTS:    start,
		PrepareTS:  txnif.UncommitTS,
		CommitTS:   txnif.UncommitTS,
		SnapshotTS: snapshot,
		Memo:       txnif.NewTxnMemo(),
	}
	ctx.DoneCond = *sync.NewCond(ctx)
	return ctx
}

func NewEmptyTxnCtx() *TxnCtx {
	ctx := &TxnCtx{
		Memo: txnif.NewTxnMemo(),
	}
	ctx.DoneCond = *sync.NewCond(ctx)
	return ctx
}

func (ctx *TxnCtx) IsReplay() bool { return false }
func (ctx *TxnCtx) GetMemo() *txnif.TxnMemo {
	return ctx.Memo
}

func (ctx *TxnCtx) Is2PC() bool { return len(ctx.Participants) > 1 }

func (ctx *TxnCtx) GetCtx() []byte {
	return ctx.IDCtx
}

func (ctx *TxnCtx) Repr() string {
	ctx.RLock()
	defer ctx.RUnlock()
	if ctx.HasSnapshotLag() {
		return fmt.Sprintf(
			"ctx[%X][%s->%s->%s][%s]",
			ctx.ID,
			ctx.SnapshotTS.ToString(),
			ctx.StartTS.ToString(),
			ctx.PrepareTS.ToString(),
			txnif.TxnStrState(ctx.State),
		)
	}
	return fmt.Sprintf(
		"ctx[%X][%s->%s][%s]",
		ctx.ID,
		ctx.StartTS.ToString(),
		ctx.PrepareTS.ToString(),
		txnif.TxnStrState(ctx.State),
	)
}

func (ctx *TxnCtx) SameTxn(txn txnif.TxnReader) bool { return ctx.ID == txn.GetID() }
func (ctx *TxnCtx) CommitBefore(startTs types.TS) bool {
	commitTS := ctx.GetCommitTS()
	return commitTS.LT(&startTs)
}
func (ctx *TxnCtx) CommitAfter(startTs types.TS) bool {
	commitTS := ctx.GetCommitTS()
	return commitTS.Greater(&startTs)
}

func (ctx *TxnCtx) String() string            { return ctx.Repr() }
func (ctx *TxnCtx) GetID() string             { return ctx.ID }
func (ctx *TxnCtx) HasSnapshotLag() bool      { return ctx.SnapshotTS.LT(&ctx.StartTS) }
func (ctx *TxnCtx) GetSnapshotTS() types.TS   { return ctx.SnapshotTS }
func (ctx *TxnCtx) SetSnapshotTS(ts types.TS) { ctx.SnapshotTS = ts }
func (ctx *TxnCtx) GetStartTS() types.TS      { return ctx.StartTS }
func (ctx *TxnCtx) SetStartTS(ts types.TS)    { ctx.StartTS = ts }
func (ctx *TxnCtx) GetCommitTS() types.TS {
	ctx.RLock()
	defer ctx.RUnlock()
	return ctx.CommitTS
}

// test only
// Note: unsafe
func (ctx *TxnCtx) MockStartTS(ts types.TS) {
	ctx.StartTS = ts
}

func (ctx *TxnCtx) SetCommitTS(cts types.TS) (err error) {
	ctx.RLock()
	defer ctx.RUnlock()
	ctx.CommitTS = cts
	return
}

func (ctx *TxnCtx) GetParticipants() []uint64 {
	ctx.RLock()
	defer ctx.RUnlock()
	return ctx.Participants
}

func (ctx *TxnCtx) SetParticipants(ids []uint64) (err error) {
	ctx.RLock()
	defer ctx.RUnlock()
	ctx.Participants = ids
	return
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
	//if state != txnif.TxnStatePreparing {
	if state == txnif.TxnStateActive ||
		state == txnif.TxnStateRollbacked ||
		state == txnif.TxnStateCommitted {
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

	//if state = ctx.getTxnState(); !waitIfCommitting || state != txnif.TxnStatePreparing {
	if state = ctx.getTxnState(); !waitIfCommitting ||
		state == txnif.TxnStateActive ||
		state == txnif.TxnStateCommitted ||
		state == txnif.TxnStateRollbacked {
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
	return ostart.LessEq(&ctx.StartTS)
}

func (ctx *TxnCtx) IsActiveLocked() bool {
	return ctx.CommitTS == txnif.UncommitTS
}

func (ctx *TxnCtx) ToPreparingLocked(ts types.TS) error {
	if ts.LessEq(&ctx.StartTS) {
		panic(fmt.Sprintf("start ts %d should be less than commit ts %d", ctx.StartTS, ts))
	}
	// if !ctx.CommitTS.Equal(txnif.UncommitTS) {
	// 	return moerr.NewTxnNotActive("")
	// }
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
		err = moerr.NewTAEPrepareNoCtx("ToPreparedLocked: state is not preparing")
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
		err = moerr.NewTAECommitNoCtx("ToCommittingFinishedLocked: state is not prepared")
		return
	}
	ctx.State = txnif.TxnStateCommittingFinished
	return
}

func (ctx *TxnCtx) ToCommittedLocked() error {
	if ctx.Is2PC() {
		if ctx.State != txnif.TxnStateCommittingFinished &&
			ctx.State != txnif.TxnStatePrepared {
			return moerr.NewTAECommitNoCtx("ToCommittedLocked: 2PC txn's state " +
				"is not Prepared or CommittingFinished")
		}
		ctx.State = txnif.TxnStateCommitted
		return nil
	}
	if ctx.State != txnif.TxnStatePreparing {
		return moerr.NewTAECommitNoCtx("ToCommittedLocked: 1PC txn's state is not preparing")
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
	if ts.LT(&ctx.StartTS) {
		panic(fmt.Sprintf("commit ts %d should not be less than start ts %d", ts, ctx.StartTS))
	}
	if (ctx.State != txnif.TxnStateActive) && (ctx.State != txnif.TxnStatePreparing) {
		return moerr.NewTAERollbackNoCtx("ToRollbackingLocked: state is not active or preparing")
	}
	ctx.CommitTS = ts
	ctx.PrepareTS = ts
	ctx.State = txnif.TxnStateRollbacking
	return nil
}

func (ctx *TxnCtx) ToRollbackedLocked() error {
	if ctx.Is2PC() {
		if ctx.State != txnif.TxnStatePrepared &&
			ctx.State != txnif.TxnStateRollbacking {
			return moerr.NewTAERollbackNoCtxf("state %s", txnif.TxnStrState(ctx.State))
		}
		ctx.State = txnif.TxnStateRollbacked
		return nil
	}
	if ctx.State != txnif.TxnStateRollbacking {
		return moerr.NewTAERollbackNoCtxf("state %s", txnif.TxnStrState(ctx.State))
	}
	ctx.State = txnif.TxnStateRollbacked
	return nil
}

func (ctx *TxnCtx) ToUnknownLocked() {
	ctx.State = txnif.TxnStateUnknown
}
