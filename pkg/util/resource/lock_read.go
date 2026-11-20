// Copyright 2026 Matrix Origin
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//      http://www.apache.org/licenses/LICENSE-2.0

// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package resource

import (
	"context"
	"encoding/hex"
	"errors"
	"math"
	"sync/atomic"
	"time"

	"github.com/google/uuid"
	"github.com/matrixorigin/matrixone/pkg/common/moerr"
)

type LockReadStage uint8

const (
	LockReadAcquisition LockReadStage = iota + 1
	LockReadLogtail
	LockReadPKCheck
	LockReadPKReadiness
)

func (s LockReadStage) String() string {
	switch s {
	case LockReadAcquisition:
		return "acquisition"
	case LockReadLogtail:
		return "logtail"
	case LockReadPKCheck:
		return "pk_check"
	case LockReadPKReadiness:
		return "pk_readiness"
	default:
		return ""
	}
}

// LockReadDiagnostic is the admitted, immutable terminal projection. Seen is
// a bit mask in stage order. PKCheckNS includes PKReadinessNS.
type LockReadDiagnostic struct {
	StatementID    uuid.UUID `json:"statement_id"`
	AttemptCount   uint64    `json:"attempt_count"`
	AcquisitionNS  uint64    `json:"acquisition_ns"`
	LogtailNS      uint64    `json:"logtail_ns"`
	PKCheckNS      uint64    `json:"pk_check_ns"`
	PKReadinessNS  uint64    `json:"pk_readiness_ns"`
	Seen           uint8     `json:"seen"`
	Conflict       bool      `json:"conflict"`
	ConflictTxnID  string    `json:"conflict_txn_id,omitempty"`
	LastIncomplete string    `json:"last_incomplete,omitempty"`
	Outcome        string    `json:"outcome"`
}

type lockReadTotals struct {
	elapsed     [4]uint64
	failedAt    time.Time
	holder      [16]byte
	seen        uint8
	incomplete  LockReadStage
	interrupted bool
	conflict    bool
	hasHolder   bool
}

func (t *lockReadTotals) merge(other lockReadTotals) {
	for i, elapsed := range other.elapsed {
		t.elapsed[i] += min(elapsed, math.MaxUint64-t.elapsed[i])
	}
	if other.failedAt.After(t.failedAt) {
		t.failedAt, t.incomplete = other.failedAt, other.incomplete
	}
	t.seen |= other.seen
	t.interrupted = t.interrupted || other.interrupted
	t.conflict = t.conflict || other.conflict
	if !t.hasHolder && other.hasHolder {
		t.holder, t.hasHolder = other.holder, true
	}
}

// LockReadObservation belongs to one physical lock call. Only the lock
// service can write the atomic conflict witness. The caller owns the timings
// and merges them after returning from the lock service and PK checker.
type LockReadObservation struct {
	totals          lockReadTotals
	holder          atomic.Pointer[[16]byte]
	queued          atomic.Bool
	readinessFailed bool
}

func (o *LockReadObservation) Start() time.Time {
	if o == nil {
		return time.Time{}
	}
	return time.Now()
}

func (o *LockReadObservation) Finish(ctx context.Context, stage LockReadStage, start time.Time, err error) {
	if o != nil {
		o.Record(ctx, stage, start, time.Now(), err)
	}
}

func (o *LockReadObservation) Record(ctx context.Context, stage LockReadStage, start, end time.Time, err error) {
	t := &o.totals
	i := stage - 1
	t.seen |= 1 << i
	t.elapsed[i] += min(uint64(max(0, end.Sub(start))), math.MaxUint64-t.elapsed[i])
	if err != nil {
		t.interrupted = t.interrupted || lockReadInterrupted(ctx, err)
		// Readiness errors propagate directly through the enclosing PK
		// check. Keep the leaf stage and its original completion order.
		if stage != LockReadPKCheck || !o.readinessFailed {
			t.failedAt, t.incomplete = end, stage
		}
	}
	if stage == LockReadPKReadiness {
		o.readinessFailed = err != nil
	} else if stage == LockReadPKCheck {
		o.readinessFailed = false
	}
}

func lockReadInterrupted(ctx context.Context, err error) bool {
	if errors.Is(err, context.Canceled) || errors.Is(err, context.DeadlineExceeded) {
		return true
	}
	var moErr *moerr.Error
	if errors.As(err, &moErr) {
		switch moErr.ErrorCode() {
		case moerr.ErrLockWaitTimeout, moerr.ErrRemoteLockWaitTimeout, moerr.ErrQueryInterrupted:
			return true
		}
	}
	return ctx.Err() != nil && errors.Is(err, context.Cause(ctx))
}

func (o *LockReadObservation) ObserveAcquisitionConflict(hasConflict bool, err error) {
	if o == nil {
		return
	}
	var moErr *moerr.Error
	if errors.As(err, &moErr) {
		switch moErr.ErrorCode() {
		case moerr.ErrLockConflict, moerr.ErrDeadLockDetected, moerr.ErrLockNowait:
			hasConflict = true
		}
	}
	o.totals.conflict = hasConflict || o.queued.Load()
	if holder := o.holder.Load(); holder != nil {
		o.totals.holder, o.totals.hasHolder = *holder, true
	}
}

type lockReadRootKey struct{}
type lockReadAcquisitionKey struct{}
type lockReadPKKey struct{}

// ContextWithLockReadRoot starts a compile generation's pipeline context.
// A nil receiver masks any eligibility inherited from an outer execution.
func ContextWithLockReadRoot(ctx context.Context, r *Root) context.Context {
	if r == nil && LockReadRootFromContext(ctx) == nil {
		return ctx
	}
	return context.WithValue(ctx, lockReadRootKey{}, r)
}

func LockReadRootFromContext(ctx context.Context) *Root {
	if ctx == nil {
		return nil
	}
	r, _ := ctx.Value(lockReadRootKey{}).(*Root)
	return r
}

func ContextWithLockReadAcquisition(ctx context.Context, o *LockReadObservation) context.Context {
	if o == nil {
		return ctx
	}
	return context.WithValue(ctx, lockReadAcquisitionKey{}, o)
}

// ObserveLocalLockConflict copies one real holder while the local waiter
// owns its admission snapshot. The lock table never takes the resource lock.
func ObserveLocalLockConflict(ctx context.Context, holders [][]byte) {
	o, _ := ctx.Value(lockReadAcquisitionKey{}).(*LockReadObservation)
	if o == nil {
		return
	}
	o.queued.Store(true)
	if len(holders) != 0 && len(holders[0]) == 16 && o.holder.Load() == nil {
		holder := [16]byte(holders[0])
		o.holder.CompareAndSwap(nil, &holder)
	}
}

func ContextWithLockReadPK(ctx context.Context, o *LockReadObservation) context.Context {
	if o == nil {
		return ctx
	}
	return context.WithValue(ctx, lockReadPKKey{}, o)
}

func LockReadPKFromContext(ctx context.Context) *LockReadObservation {
	o, _ := ctx.Value(lockReadPKKey{}).(*LockReadObservation)
	return o
}

// EnableLockRead associates this root with a terminal diagnostics owner.
// Frontend calls it before exposing the new root to execution.
func (r *Root) EnableLockRead() { r.lockReadEnabled = true }

func (r *Root) LockReadEnabled() bool { return r != nil && r.lockReadEnabled }

func (r *Root) MergeLockRead(o *LockReadObservation) {
	if o == nil || o.totals.seen == 0 {
		return
	}
	r.mu.Lock()
	defer r.mu.Unlock()
	if r.sealed {
		return
	}
	if r.lockRead == nil {
		r.lockRead = new(lockReadTotals)
	}
	r.lockRead.merge(o.totals)
}

type LockReadSnapshot struct{ totals lockReadTotals }

func (s LockReadSnapshot) Qualifies(duration, threshold time.Duration) bool {
	return s.totals.seen != 0 && (s.totals.interrupted || duration > threshold)
}

func (s LockReadSnapshot) Diagnostic(id uuid.UUID, attempts uint64, failed bool) LockReadDiagnostic {
	t := s.totals
	d := LockReadDiagnostic{
		StatementID: id, AttemptCount: attempts, Seen: t.seen,
		AcquisitionNS: t.elapsed[0], LogtailNS: t.elapsed[1],
		PKCheckNS: t.elapsed[2], PKReadinessNS: t.elapsed[3],
		Conflict: t.conflict, LastIncomplete: t.incomplete.String(), Outcome: "success",
	}
	if failed {
		d.Outcome = "failed"
	}
	if t.hasHolder {
		d.ConflictTxnID = hex.EncodeToString(t.holder[:])
	}
	return d
}
