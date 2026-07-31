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

package dedupjoin

import (
	"context"
	"sync"

	"github.com/matrixorigin/matrixone/pkg/common/bitmap"
	"github.com/matrixorigin/matrixone/pkg/common/hashmap"
	"github.com/matrixorigin/matrixone/pkg/common/moerr"
	"github.com/matrixorigin/matrixone/pkg/common/mpool"
	"github.com/matrixorigin/matrixone/pkg/common/reuse"
	"github.com/matrixorigin/matrixone/pkg/container/batch"
	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/container/vector"
	"github.com/matrixorigin/matrixone/pkg/pb/plan"
	"github.com/matrixorigin/matrixone/pkg/sql/colexec"
	"github.com/matrixorigin/matrixone/pkg/sql/colexec/hashbuild"
	"github.com/matrixorigin/matrixone/pkg/sql/colexec/spillutil"
	"github.com/matrixorigin/matrixone/pkg/vm"
	"github.com/matrixorigin/matrixone/pkg/vm/message"
	"github.com/matrixorigin/matrixone/pkg/vm/process"
)

var _ vm.Operator = new(DedupJoin)

const (
	Build = iota
	Probe
	Finalize
	End
)

const (
	dedupJoinAllocationSiteMatched mpool.AllocationSite = iota + 82
	dedupJoinAllocationSiteCaptured
	dedupJoinAllocationSiteCaptureData
	dedupJoinAllocationSiteCaptureArea
	dedupJoinAllocationSiteCaptureNulls
	dedupJoinAllocationSiteCaptureGrouping
	dedupJoinAllocationSiteFinalizeSelections
)

// WorkerJoinMsg carries per-worker state from non-merger workers to the
// merger worker at finalize time. Regular DEDUP JOIN only populates matched;
// the REPLACE INTO merged main-table scan path (OldColCapture) additionally
// populates captured and capturedVecs.
//
// Ownership: once a non-merger successfully publishes this message to the
// mailbox, it must relinquish captured / capturedVecs. The merger then becomes
// the sole owner and is responsible for Free'ing capturedVecs.
type WorkerJoinMsg struct {
	matched      *bitmap.Bitmap
	captured     *bitmap.Bitmap
	capturedVecs []*vector.Vector
	aborted      bool
	err          error
}

// WorkerJoinMailbox coordinates the single finalize status emitted by each
// non-merger worker. stopAndDrain and trySend share one lock so ownership
// cannot transfer into the mailbox after the merger has stopped consuming.
//
// A stopped mailbox is reopened only after every parallel operator has Reset.
// That generation barrier prevents a late sender from the failed execution
// from being mistaken for a worker in a reused prepared pipeline. The lock is
// taken once per worker per finalize bucket and once per Reset, never per row.
type WorkerJoinMailbox struct {
	mu           sync.Mutex
	ch           chan *WorkerJoinMsg
	roundDone    chan struct{}
	participants int
	resetCount   int
	stopped      bool
}

func NewWorkerJoinMailbox(participants int) *WorkerJoinMailbox {
	if participants < 1 {
		participants = 1
	}
	return &WorkerJoinMailbox{
		ch:           make(chan *WorkerJoinMsg, participants),
		roundDone:    make(chan struct{}),
		participants: participants,
	}
}

func (m *WorkerJoinMailbox) trySend(msg *WorkerJoinMsg) (sent, stopped bool, roundDone <-chan struct{}) {
	if m == nil {
		return false, false, nil
	}
	m.mu.Lock()
	defer m.mu.Unlock()
	if m.stopped {
		return false, true, nil
	}
	select {
	case m.ch <- msg:
		return true, false, m.roundDone
	default:
		return false, false, nil
	}
}

func (m *WorkerJoinMailbox) receiveState() (roundDone <-chan struct{}, stopped bool) {
	if m == nil {
		return nil, true
	}
	m.mu.Lock()
	defer m.mu.Unlock()
	return m.roundDone, m.stopped
}

// completeRound releases every worker only after the merger has collected the
// complete status set for the current spill bucket. Without this barrier a
// fast worker can publish its next bucket before a slow worker publishes the
// current one, and a count-only merger can combine different bucket layouts.
func (m *WorkerJoinMailbox) completeRound() {
	if m == nil {
		return
	}
	m.mu.Lock()
	defer m.mu.Unlock()
	if m.stopped {
		return
	}
	close(m.roundDone)
	m.roundDone = make(chan struct{})
}

func (m *WorkerJoinMailbox) stopAndDrain(proc *process.Process) {
	if m == nil {
		return
	}
	m.mu.Lock()
	defer m.mu.Unlock()
	if !m.stopped {
		m.stopped = true
		close(m.roundDone)
		m.roundDone = nil
	}
	m.drainLocked(proc)
}

func (m *WorkerJoinMailbox) drain(proc *process.Process) {
	if m == nil {
		return
	}
	m.mu.Lock()
	defer m.mu.Unlock()
	m.drainLocked(proc)
}

func (m *WorkerJoinMailbox) drainLocked(proc *process.Process) {
	for {
		select {
		case msg, ok := <-m.ch:
			if !ok {
				return
			}
			freeWorkerJoinMsg(msg, proc)
		default:
			return
		}
	}
}

// resetParticipant closes one participant's use of the current generation.
// The last Reset drains any residual status and reopens a clean mailbox for a
// prepared pipeline's next execution.
func (m *WorkerJoinMailbox) resetParticipant(proc *process.Process) {
	if m == nil {
		return
	}
	m.mu.Lock()
	defer m.mu.Unlock()
	m.resetCount++
	if m.resetCount < m.participants {
		return
	}
	m.drainLocked(proc)
	m.resetCount = 0
	m.stopped = false
	m.roundDone = make(chan struct{})
}

// freeCapturedVecs releases vectors owned by a WorkerJoinMsg. Intended to be
// called by the merger after it has finished merging captures out of the
// message (ownership was transferred from the sender).
func freeCapturedVecs(vecs []*vector.Vector, proc *process.Process) {
	for _, v := range vecs {
		if v != nil {
			v.Free(proc.GetMPool())
		}
	}
}

func freeWorkerJoinMsg(msg *WorkerJoinMsg, proc *process.Process) {
	if msg != nil {
		colexec.FreeAccountedBitmap(msg.matched, proc.Mp())
		colexec.FreeAccountedBitmap(msg.captured, proc.Mp())
		freeCapturedVecs(msg.capturedVecs, proc)
	}
}

type evalVector struct {
	executor colexec.ExpressionExecutor
	vec      *vector.Vector
}

type container struct {
	state   int
	lastPos int

	batches       []*batch.Batch
	batchRowCount int64

	exprExecs []colexec.ExpressionExecutor

	joinBat1 *batch.Batch
	cfs1     []func(*vector.Vector, *vector.Vector, int64, int) error

	joinBat2 *batch.Batch
	cfs2     []func(*vector.Vector, *vector.Vector, int64, int) error

	savedVecs []*vector.Vector

	evecs []evalVector
	vecs  []*vector.Vector

	mp        *message.JoinMap
	cachedItr hashmap.Iterator

	matched *bitmap.Bitmap
	// roundStatusPublished is true only while this worker's status for the
	// current finalize round is in the mailbox and has not yet been
	// acknowledged by completeRound. Reset publishes an abort when false so a
	// merger that already advanced to the next spill bucket cannot wait
	// forever after a normal worker early-stop.
	roundStatusPublished bool

	// Capture buffers for the REPLACE INTO merged main-table scan. When
	// OldColCapturePlaceholderIdxList is non-empty, each entry i in the list
	// owns capturedVecs[i], a vector of length batchRowCount pre-filled with
	// NULL. When a probe row hits build bucket `sel`, we Copy the probe-side
	// source column into capturedVecs[i] at position `sel`. In finalize() the
	// captured values are emitted into the Result slots that point at the
	// build-side placeholder columns.
	capturedVecs     []*vector.Vector
	captured         *bitmap.Bitmap
	captureResultIdx []int32

	maxAllocSize int64
	rbat         *batch.Batch
	buf          []*batch.Batch

	// Spill support for large build sides.
	spillEngine    *spillutil.SpillEngine
	spillThreshold int64
}

type DedupJoin struct {
	ctr        container
	Result     []colexec.ResultPos
	LeftTypes  []types.Type
	RightTypes []types.Type
	Conditions [][]*plan.Expr

	IsShuffle          bool
	ShuffleIdx         int32
	RuntimeFilterSpecs []*plan.RuntimeFilterSpec
	JoinMapTag         int32

	Mailbox  *WorkerJoinMailbox
	NumCPU   uint64
	IsMerger bool

	OnDuplicateAction         plan.Node_OnDuplicateAction
	DedupBuildKeepLast        bool
	DedupColName              string
	SpillThreshold            int64
	DedupColTypes             []plan.Type
	DelColIdx                 int32
	DedupDeleteMarkerColIdx   int32
	DedupDeleteKeepColIdxList []int32
	UpdateColIdxList          []int32
	UpdateColExprList         []*plan.Expr

	// OldColCapturePlaceholderIdxList / OldColCaptureProbeIdxList are parallel
	// arrays. For each i, when probe hits a build bucket the probe-side column
	// at OldColCaptureProbeIdxList[i] is captured and, in finalize(), emitted
	// into every Result entry whose (Rel=1, Pos) equals
	// OldColCapturePlaceholderIdxList[i]. Used by the REPLACE INTO merged
	// main-table scan path; empty for regular INSERT/UPDATE.
	OldColCapturePlaceholderIdxList []int32
	OldColCaptureProbeIdxList       []int32
	allocationAccount               *mpool.AllocationAccount
	stateAllocation                 *vector.AllocationAccountSelection

	vm.OperatorBase
}

func (dedupJoin *DedupJoin) SetAllocationAccount(
	account *mpool.AllocationAccount,
) error {
	if account == nil || account.Handle() == 0 {
		return mpool.ErrAllocationAccountInvalid
	}
	if dedupJoin.allocationAccount != nil &&
		dedupJoin.allocationAccount != account {
		return mpool.ErrAllocationAccountMismatch
	}
	if dedupJoin.allocationAccount == account {
		return nil
	}
	selection, err := vector.NewAllocationAccountSelection(
		account,
		hashbuild.HashBuildAllocationOwner,
		dedupJoinAllocationSiteCaptureData,
		dedupJoinAllocationSiteCaptureArea,
		dedupJoinAllocationSiteCaptureNulls,
		dedupJoinAllocationSiteCaptureGrouping,
	)
	if err != nil {
		return err
	}
	dedupJoin.allocationAccount = account
	dedupJoin.stateAllocation = selection
	return nil
}

func (dedupJoin *DedupJoin) ClearAllocationAccount(
	account *mpool.AllocationAccount,
) error {
	if dedupJoin.allocationAccount == nil {
		return nil
	}
	if dedupJoin.allocationAccount != account {
		return mpool.ErrAllocationAccountMismatch
	}
	if dedupJoin.ctr.mp != nil || dedupJoin.ctr.spillEngine != nil ||
		len(dedupJoin.ctr.evecs) != 0 || len(dedupJoin.ctr.exprExecs) != 0 ||
		dedupJoin.ctr.matched != nil || dedupJoin.ctr.captured != nil ||
		len(dedupJoin.ctr.capturedVecs) != 0 {
		return mpool.ErrAllocationAccountInvariant
	}
	dedupJoin.allocationAccount = nil
	dedupJoin.stateAllocation = nil
	return nil
}

func (dedupJoin *DedupJoin) GetOperatorBase() *vm.OperatorBase {
	return &dedupJoin.OperatorBase
}

func init() {
	reuse.CreatePool(
		func() *DedupJoin {
			return &DedupJoin{}
		},
		func(a *DedupJoin) {
			*a = DedupJoin{}
		},
		reuse.DefaultOptions[DedupJoin]().
			WithEnableChecker(),
	)
}

func (dedupJoin DedupJoin) TypeName() string {
	return opName
}

func NewArgument() *DedupJoin {
	return reuse.Alloc[DedupJoin](nil)
}

func (dedupJoin *DedupJoin) Release() {
	if dedupJoin != nil {
		reuse.Free[DedupJoin](dedupJoin, nil)
	}
}

// needsFinalizeMerge reports whether parallel workers share one build map and
// therefore must merge their matched state before a single worker emits the
// build rows. Shuffle workers own disjoint build partitions, so each worker
// must finalize and emit its partition independently.
func (dedupJoin *DedupJoin) needsFinalizeMerge() bool {
	return dedupJoin.NumCPU > 1 && !dedupJoin.IsShuffle
}

func (dedupJoin *DedupJoin) Reset(proc *process.Process, pipelineFailed bool, err error) {
	ctr := &dedupJoin.ctr
	if dedupJoin.needsFinalizeMerge() {
		if dedupJoin.IsMerger {
			// Reset follows process cancellation. Stop before cleaning merger
			// state so no late worker can transfer capture ownership to an
			// execution that no longer has a consumer.
			dedupJoin.Mailbox.stopAndDrain(proc)
		} else if !ctr.roundStatusPublished {
			if pipelineFailed && err == nil {
				err = context.Cause(proc.Ctx)
				if err == nil {
					err = moerr.NewInternalErrorNoCtx("dedup join worker failed without an error")
				}
			}
			// trySend never waits. If the merger has already stopped, this
			// worker retains and releases its local capture state below.
			dedupJoin.Mailbox.trySend(&WorkerJoinMsg{
				aborted: true,
				err:     err,
			})
		}
		dedupJoin.Mailbox.resetParticipant(proc)
	}
	if dedupJoin.OpAnalyzer != nil {
		dedupJoin.OpAnalyzer.Alloc(ctr.maxAllocSize)
	}
	ctr.maxAllocSize = 0

	ctr.cleanBuf(proc)
	ctr.cleanBucketState(proc)
	ctr.cleanExprExecutor()
	if ctr.spillEngine != nil {
		ctr.spillEngine.Cleanup(proc)
		ctr.spillEngine = nil
	}
	ctr.cleanEvalVectors()
	ctr.roundStatusPublished = false
	ctr.state = Build
	ctr.lastPos = 0
}

func (dedupJoin *DedupJoin) Free(proc *process.Process, pipelineFailed bool, err error) {
	ctr := &dedupJoin.ctr
	if dedupJoin.IsMerger && dedupJoin.needsFinalizeMerge() {
		// Pipeline cleanup always calls Reset first. Do not leave a newly
		// reopened prepared-pipeline generation stopped from Free.
		dedupJoin.Mailbox.drain(proc)
	}
	ctr.cleanBuf(proc)
	ctr.cleanBucketState(proc)
	ctr.cleanBatch(proc)
	ctr.cleanExprExecutor()
	if ctr.spillEngine != nil {
		ctr.spillEngine.Cleanup(proc)
		ctr.spillEngine = nil
	}
	ctr.cleanEvalVectors()
}

func (dedupJoin *DedupJoin) ExecProjection(proc *process.Process, input *batch.Batch) (*batch.Batch, error) {
	return input, nil
}

func (ctr *container) cleanExprExecutor() {
	for i := range ctr.exprExecs {
		if ctr.exprExecs[i] != nil {
			ctr.exprExecs[i].Free()
		}
	}
	ctr.exprExecs = nil
}

func (ctr *container) cleanBuf(proc *process.Process) {
	for _, bat := range ctr.buf {
		if bat != nil && bat != ctr.rbat {
			bat.Clean(proc.GetMPool())
		}
	}
	ctr.buf = nil
}

func (ctr *container) cleanCaptured(proc *process.Process) {
	for _, v := range ctr.capturedVecs {
		if v != nil {
			v.Free(proc.GetMPool())
		}
	}
	ctr.capturedVecs = nil
	colexec.FreeAccountedBitmap(ctr.captured, proc.Mp())
	ctr.captured = nil
	ctr.captureResultIdx = nil
}

func (ctr *container) cleanBatch(proc *process.Process) {
	ctr.batches = nil

	if ctr.rbat != nil {
		ctr.rbat.Clean(proc.GetMPool())
		ctr.rbat = nil
	}
	if ctr.joinBat1 != nil {
		ctr.joinBat1.Clean(proc.GetMPool())
		ctr.joinBat1 = nil
	}
	if ctr.joinBat2 != nil {
		ctr.joinBat2.Clean(proc.GetMPool())
		ctr.joinBat2 = nil
	}
	clear(ctr.savedVecs)
	ctr.savedVecs = nil
}

// cleanBucketState releases per-bucket state before advancing to the next
// spill bucket. This prevents stale JoinMap / capture state from leaking
// across bucket boundaries.
func (ctr *container) cleanBucketState(proc *process.Process) {
	ctr.cleanCaptured(proc)
	ctr.cleanHashMap()
	ctr.batches = nil
	ctr.batchRowCount = 0
	colexec.FreeAccountedBitmap(ctr.matched, proc.Mp())
	ctr.matched = nil
}

func (ctr *container) cleanHashMap() {
	hashmap.IteratorClearOwner(ctr.cachedItr)
	ctr.cachedItr = nil
	if ctr.mp != nil {
		ctr.mp.Free()
		ctr.mp = nil
	}
}

func (ctr *container) cleanEvalVectors() {
	for i := range ctr.evecs {
		if ctr.evecs[i].executor != nil {
			ctr.evecs[i].executor.Free()
		}
		ctr.evecs[i].vec = nil
	}
	ctr.evecs = nil
	ctr.vecs = nil
}
