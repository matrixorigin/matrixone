// Copyright 2023 Matrix Origin
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

package lockop

import (
	"github.com/matrixorigin/matrixone/pkg/common/log"
	"github.com/matrixorigin/matrixone/pkg/common/reuse"
	"github.com/matrixorigin/matrixone/pkg/container/batch"
	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/container/vector"
	"github.com/matrixorigin/matrixone/pkg/pb/lock"
	"github.com/matrixorigin/matrixone/pkg/pb/timestamp"
	"github.com/matrixorigin/matrixone/pkg/sql/plan"
	"github.com/matrixorigin/matrixone/pkg/vm"
	"github.com/matrixorigin/matrixone/pkg/vm/engine"
	"github.com/matrixorigin/matrixone/pkg/vm/process"
)

var _ vm.Operator = new(LockOp)

// FetchLockRowsFunc fetch lock rows from vector.
type FetchLockRowsFunc func(
	// primary data vector
	vec *vector.Vector,
	// holder to encode primary key to lock row
	parker *types.Packer,
	// primary key type
	tp types.Type,
	// global config: max lock rows bytes per lock
	max int,
	// is lock table lock
	lockTable bool,
	// used to filter rows
	filter RowsFilter,
	// used by filter rows func
	filterCols []int32) (bool, [][]byte, lock.Granularity)

// LockOptions lock operation options
type LockOptions struct {
	maxCountPerLock          int
	mode                     lock.LockMode
	sharding                 lock.Sharding
	group                    uint32
	lockTable                bool
	changeDef                bool
	refreshTableSnapshot     bool
	admissionOnly            bool
	admissionResult          *lock.Result
	noWait                   bool
	skipLocked               bool
	waitPolicy               *lock.WaitPolicy
	parker                   *types.Packer
	fetchFunc                FetchLockRowsFunc
	filter                   RowsFilter
	filterCols               []int32
	hasNewVersionInRangeFunc hasNewVersionInRangeFunc
	// markingCurrentRead marks a target of an admitted rev-14 locking
	// current-read execution (design D-1/D-5): the lock-side revalidation uses
	// THIS ATTEMPT's frontier (markingFrontier) as its comparison view instead
	// of the mutable RC snapshot, never advances the transaction snapshot,
	// and returns refresh outcomes through the retry-floor accumulator
	// (markingFloor) that performLock surfaces once as
	// moerr.ErrTxnNeedRetryLockingRead. Unmarked (today's) targets leave all
	// three fields zero and keep the existing branches byte-identically.
	markingCurrentRead bool
	markingFrontier    timestamp.Timestamp
	markingFloor       *timestamp.Timestamp
}

// recordMarkingFloor raises the retry-floor accumulator to a branch-computed
// required timestamp (max across every marked target of this performLock
// call; design D-5's single surface and aggregation).
func (opts *LockOptions) recordMarkingFloor(to timestamp.Timestamp) {
	if opts.markingFloor == nil {
		return
	}
	if to.Greater(*opts.markingFloor) {
		*opts.markingFloor = to
	}
}

// LockOp lock op argument.
type LockOp struct {
	vm.OperatorBase

	logger  *log.MOLogger
	ctr     state
	engine  engine.Engine
	targets []lockTarget

	// markingFrontier carries this execution's current-read frontier to the
	// lock-side revalidation (design D-5/D-8), stamped by compile after this
	// attempt's source assembly, before any pipeline starts. The bind-time
	// presence it activates is per OCCURRENCE (pre-PR review round 10,
	// finding 2, D-1's per-target representation): each target carries its
	// own lockingRead flag — installed by constructLockOp from the plan
	// target's own LockingRead, rebuilt per target from the wire, retained
	// by Reset with the retained physical generation — together with its
	// raw bind-time explicitSnapshot provenance, and never as a table-ID
	// set — so an unmarked occurrence of a MARKED table (the LEFT-lowered
	// same-table scalar-subquery shape) never inherits the marked
	// suppression/retry contract. Rev 24 pre-PR review round 11: the stamp
	// is also where each occurrence's EFFECTIVE activation resolves per
	// execution from presence + raw H against the executing SnapshotTS (the
	// same engagement predicate compile's scan admission uses), so an
	// occurrence whose explicit snapshot engages the historical cohort for
	// this execution keeps today's lock-only revalidation and a later
	// execution that no longer engages re-attaches the marked contract
	// (G2/R8-F3 with R9-F3). Without a stamp — a declined statement, an
	// inapplicable path — every marked occurrence keeps today's lock-only
	// behavior.
	markingFrontier    timestamp.Timestamp
	markingFrontierSet bool
}

func (lockOp *LockOp) GetOperatorBase() *vm.OperatorBase {
	return &lockOp.OperatorBase
}

func init() {
	reuse.CreatePool[LockOp](
		func() *LockOp {
			return &LockOp{}
		},
		func(a *LockOp) {
			*a = LockOp{}
		},
		reuse.DefaultOptions[LockOp]().
			WithEnableChecker(),
	)
}

func (lockOp LockOp) TypeName() string {
	return opName
}

func NewArgument() *LockOp {
	return reuse.Alloc[LockOp](nil)
}

func (lockOp *LockOp) Release() {
	if lockOp != nil {
		reuse.Free[LockOp](lockOp, nil)
	}
}

type lockTarget struct {
	tableID                      uint64
	objRef                       *plan.ObjectRef
	primaryColumnIndexInBatch    int32
	refreshTimestampIndexInBatch int32
	primaryColumnType            types.Type
	partitionColumnIndexInBatch  int32
	filter                       RowsFilter
	filterColIndexInBatch        int32
	lockTable                    bool
	changeDef                    bool
	mode                         lock.LockMode
	noWait                       bool
	skipLocked                   bool
	lockRows                     *plan.Expr
	lockTableAtTheEnd            bool
	// lockingRead is this OCCURRENCE'S bind-time locking-current-read
	// presence (pre-PR review round 10, finding 2; design D-1: presence is
	// the plan.LockTarget's own per-target field). It is installed by
	// constructLockOp from the target it adds, rebuilt per target from the
	// wire's additive fields and preserved by CopyTargetsFrom and Reset; it
	// acts only while this execution's marking frontier is stamped, and a
	// same-table occurrence whose presence is unmarked never inherits a
	// marked occurrence's contract.
	lockingRead bool
	// explicitSnapshot is the occurrence's raw bind-time explicit-snapshot
	// provenance TS (rev 24, pre-PR review round 11; D-1's stored
	// `OuterLockingWithExplicitSnapshot(H)`), installed together with the
	// presence from the plan target's own additive fields and never derived
	// from a table identity. It is a bind fact, NOT an effective status:
	// the per-execution engagement of the historical cohort is resolved at
	// every stamp against the executing transaction's SnapshotTS (the same
	// predicate compile's scan admission uses) and is never stored beyond
	// the attempt.
	explicitSnapshot timestamp.Timestamp
	// lockingReadEffective is the occurrence's EFFECTIVE marked activation
	// for the attempt the operator is stamped for, resolved at the stamp
	// from presence + explicitSnapshot together with the frontier: an
	// occurrence whose stored H is nonzero and strictly older than the
	// executing SnapshotTS engages the historical cohort for that
	// execution and stays EXEMPT from the marked revalidation — its
	// lock keeps today's lock-only behavior, its mid-wait commit surfaces
	// the legacy terminal conflict, and a later execution whose SnapshotTS
	// no longer engages re-attaches the marked contract (G2/R8-F3 with
	// R9-F3). Retired by Reset with the per-attempt frontier; presence and
	// explicitSnapshot survive.
	lockingReadEffective bool
}

// RowsFilter used to filter row from primary vector. The row will not lock if filter return false.
type RowsFilter func(row int, filterCols []int32) bool

type hasNewVersionInRangeFunc func(
	proc *process.Process,
	rel engine.Relation,
	analyzer process.Analyzer,
	tableID uint64,
	eng engine.Engine,
	bat *batch.Batch,
	idx int32,
	partitionIdx int32,
	from, to timestamp.Timestamp) (bool, error)

type state struct {
	parker *types.Packer
	// lockingReadFloor remembers this operator's last surfaced marking
	// floor for diagnostics; the authoritative value travels in the
	// surfaced moerr.ErrTxnNeedRetryLockingRead token (D-5).
	lockingReadFloor timestamp.Timestamp
	// skipLockedOutput is the operator-owned copy returned to downstream
	// consumers. It remains live until the next Call or operator cleanup so a
	// consumer may finish using the borrowed batch before it is reclaimed.
	skipLockedOutput     *batch.Batch
	retryError           error
	defChanged           bool
	fetchers             []FetchLockRowsFunc
	relations            []engine.Relation
	hasNewVersionInRange hasNewVersionInRangeFunc
	lockCount            int64
}
