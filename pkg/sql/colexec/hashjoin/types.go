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

package hashjoin

import (
	"os"

	"github.com/matrixorigin/matrixone/pkg/common"
	"github.com/matrixorigin/matrixone/pkg/common/bitmap"
	"github.com/matrixorigin/matrixone/pkg/common/hashmap"
	"github.com/matrixorigin/matrixone/pkg/common/reuse"
	"github.com/matrixorigin/matrixone/pkg/common/rscthrottler"
	"github.com/matrixorigin/matrixone/pkg/common/system"
	"github.com/matrixorigin/matrixone/pkg/container/batch"
	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/container/vector"
	"github.com/matrixorigin/matrixone/pkg/fileservice"
	"github.com/matrixorigin/matrixone/pkg/pb/plan"
	"github.com/matrixorigin/matrixone/pkg/sql/colexec"
	"github.com/matrixorigin/matrixone/pkg/vm"
	"github.com/matrixorigin/matrixone/pkg/vm/message"
	"github.com/matrixorigin/matrixone/pkg/vm/process"
)

var _ vm.Operator = new(HashJoin)

const (
	Build = iota
	Probe
	SyncBitmap
	Finalize
	End
)

type spillBucket struct {
	buildFd  *os.File // fd-based build file; nil = empty build
	probeFd  *os.File // fd-based probe file; nil = empty probe
	baseName string   // base name for deriving sub-bucket file paths on re-spill
	depth    int
}

type probeState int

const (
	psNextBatch probeState = iota
	psSelsForOneRow
	psBatchRow
)

// Sealed topology is a nonowning handle. Only actual execution acquires a
// participant claim; retirement must survive Pipeline.Cleanup's Reset -> Free.
type spillParticipantState uint8

const (
	spillParticipantNone spillParticipantState = iota
	spillParticipantExpected
	spillParticipantRegistered
	spillParticipantFinalizer
	spillParticipantRetired
)

type container struct {
	state       int
	itr         hashmap.Iterator
	rightBats   []*batch.Batch
	rightRowCnt int64

	leftBat *batch.Batch
	resBat  *batch.Batch

	lastIdx int
	// process idx for zvs and vs, which returned by hashmap.Iterator.Find()
	// guarantee: vs[ctr.vsIdx] is the result of inbat[ctr.lastIdx]
	vsIdx int
	zvs   []int64
	vs    []uint64
	sels  []int32

	leftRowMatched bool

	probeState probeState

	nonEqCondExec colexec.ExpressionExecutor

	joinBats []*batch.Batch
	cfs1     []func(*vector.Vector, *vector.Vector, int64, int) error
	cfs2     []func(*vector.Vector, *vector.Vector, int64, int) error

	eqCondExecs []colexec.ExpressionExecutor
	eqCondVecs  []*vector.Vector

	skipProbe bool

	mp *message.JoinMap

	rightRowsMatched *bitmap.Bitmap
	rightMatchedIter bitmap.Iterator
	bitmapSynced     bool

	joinInputLeases [2]*rscthrottler.InputLease
	outputLease     *rscthrottler.InputLease
	outputTarget    int64
	outputRowWidth  int64
	outputBytes     int64
	maxAllocSize    int64

	// spill support
	spillGeneration  bool
	spillParticipant spillParticipantState
	spillConsumer    bool
	spillWork        *message.JoinSpillWork
	skew             *skewJoin
	activeBucket     *spillBucket
	spillQueue       []spillBucket // multi-level spill work queue
	spillThreshold   int64

	// state for processing current bucket
	probeBucketReader *spillBucketReader             // reused across buckets; buffer allocated once
	probeBucketActive bool                           // true while reading a probe file
	spillBuildReader  *spillBucketReader             // reused for build file (and probe in reSpillBucket)
	spillFS           fileservice.MutableFileService // cached once; avoids repeated registry lookups

	// reusable buffers for spill operations
	spillHashValues      []uint64
	spillBucketRowIds    [][]int32
	spillNonEmptyBuckets []int
	spillKeyVecs         []*vector.Vector
	spillBuildReadBatch  *batch.Batch
	spillProbeReadBatch  *batch.Batch
	spillBuildSubBufs    []*batch.Batch
	spillProbeSubBufs    []*batch.Batch

	// cached expression executors for re-spill (reused across batches)
	spillBuildExprExecs []colexec.ExpressionExecutor
}

type HashJoin struct {
	ctr container

	JoinType    plan.Node_JoinType
	IsRightJoin bool

	ResultCols []colexec.ResultPos
	LeftTypes  []types.Type
	RightTypes []types.Type
	NonEqCond  *plan.Expr
	EqConds    [][]*plan.Expr

	Channel chan *bitmap.Bitmap
	NumCPU  uint64

	HashOnPK     bool
	CanSkipProbe bool

	IsShuffle  bool
	ShuffleIdx int32

	IsMerger           bool
	RuntimeFilterSpecs []*plan.RuntimeFilterSpec
	JoinMapTag         int32
	SpillThreshold     int64

	SpillScopeID        uint32
	SpillExpectedScopes []uint32
	SpillGroup          *message.JoinSpillGroup
	SpillEpoch          uint64
	SpillParticipantID  uint64

	vm.OperatorBase
}

func (hashJoin *HashJoin) GetOperatorBase() *vm.OperatorBase {
	return &hashJoin.OperatorBase
}

func init() {
	reuse.CreatePool[HashJoin](
		func() *HashJoin {
			return &HashJoin{}
		},
		func(a *HashJoin) {
			*a = HashJoin{}
		},
		reuse.DefaultOptions[HashJoin]().
			WithEnableChecker(),
	)
}

func (hashJoin HashJoin) TypeName() string {
	return opName
}

func NewArgument() *HashJoin {
	return reuse.Alloc[HashJoin](nil)
}

func (hashJoin *HashJoin) Release() {
	if hashJoin != nil {
		reuse.Free[HashJoin](hashJoin, nil)
	}
}

func (hashJoin *HashJoin) ExecProjection(proc *process.Process, input *batch.Batch) (*batch.Batch, error) {
	return input, nil
}

// retireSpillParticipant makes the actual's one terminal group transition.
// Pipeline.Cleanup cancels proc even on success, so context cancellation alone
// cannot classify its outcome. Local buffers/readers are destroyed by the
// caller before the existing sole work owner closes its remaining files.
func (hashJoin *HashJoin) retireSpillParticipant(pipelineFailed bool, err error) {
	ctr := &hashJoin.ctr
	state := ctr.spillParticipant
	if state == spillParticipantNone || state == spillParticipantRetired {
		return
	}
	ctr.spillParticipant = spillParticipantRetired
	if hashJoin.SpillGroup == nil {
		return
	}
	failed := pipelineFailed || err != nil || ctr.state != End
	switch state {
	case spillParticipantExpected:
		if failed || hashJoin.spillGeneration() {
			hashJoin.SpillGroup.Abort(hashJoin.SpillEpoch)
		} else {
			hashJoin.SpillGroup.FinishParticipant(hashJoin.SpillEpoch, hashJoin.SpillParticipantID)
		}
	case spillParticipantRegistered, spillParticipantFinalizer:
		if failed {
			hashJoin.SpillGroup.Abort(hashJoin.SpillEpoch)
		}
	}
}

func (hashJoin *HashJoin) Reset(proc *process.Process, pipelineFailed bool, err error) {
	ctr := &hashJoin.ctr
	hashJoin.retireSpillParticipant(pipelineFailed, err)
	ctr.itr = nil
	if !ctr.bitmapSynced && !hashJoin.spillGeneration() && hashJoin.NumCPU > 1 && !hashJoin.IsMerger {
		hashJoin.Channel <- nil
	}
	ctr.cleanBucketBatches(proc)
	ctr.cleanHashMap()
	ctr.resetNonEqCondExecutor()
	ctr.resetEqCondExecutors()
	ctr.rightRowsMatched = nil
	ctr.rightMatchedIter = nil
	ctr.skipProbe = false
	ctr.bitmapSynced = false
	ctr.state = Build
	ctr.probeState = psNextBatch
	ctr.lastIdx = 0
	ctr.cleanupSpillFiles(proc)
	// A controlled attempt must retire cached expression/output capacity before
	// returning its admitted slots. ResetForNextQuery preserves function results.
	if ctr.joinInputLeases[0] != nil || ctr.joinInputLeases[1] != nil || ctr.outputLease != nil {
		ctr.cleanBatch(proc)
		ctr.cleanNonEqCondExecutor()
		ctr.cleanEqCondExecutors()
		ctr.eqCondVecs = nil
		ctr.vs, ctr.zvs, ctr.sels = nil, nil, nil
	}
	for i, lease := range ctr.joinInputLeases {
		if lease != nil {
			lease.Release()
			ctr.joinInputLeases[i] = nil
		}
	}
	if ctr.spillWork != nil {
		ctr.spillWork.Close()
		ctr.spillWork = nil
	}
	ctr.spillGeneration, ctr.spillConsumer = false, false

	if hashJoin.OpAnalyzer != nil {
		hashJoin.OpAnalyzer.Alloc(ctr.maxAllocSize)
	}
	if ctr.outputLease != nil {
		ctr.cleanBatch(proc)
		ctr.outputLease.Release()
		ctr.outputLease = nil
	}
	ctr.leftBat = nil
	ctr.maxAllocSize = 0
}

func (hashJoin *HashJoin) Free(proc *process.Process, pipelineFailed bool, err error) {
	ctr := &hashJoin.ctr
	hashJoin.retireSpillParticipant(pipelineFailed, err)

	ctr.cleanBatch(proc)
	if ctr.outputLease != nil {
		ctr.outputLease.Release()
		ctr.outputLease = nil
	}
	ctr.cleanBucketBatches(proc)
	ctr.cleanHashMap()
	ctr.cleanNonEqCondExecutor()
	ctr.cleanEqCondExecutors()
	ctr.cleanupSpillFiles(proc)
	for i, lease := range ctr.joinInputLeases {
		if lease != nil {
			lease.Release()
			ctr.joinInputLeases[i] = nil
		}
	}
	if ctr.spillWork != nil {
		ctr.spillWork.Close()
		ctr.spillWork = nil
	}
	ctr.freeSpillBuildExprExecs()
	ctr.spillBucketRowIds = nil
	ctr.spillNonEmptyBuckets = nil
}

func (ctr *container) cleanupSpillFiles(proc *process.Process) {
	if ctr.skew != nil {
		ctr.skew.close(proc)
		ctr.skew = nil
	}
	if ctr.probeBucketActive && ctr.probeBucketReader != nil {
		ctr.probeBucketReader.close()
		ctr.probeBucketActive = false
	}
	if ctr.activeBucket != nil {
		if ctr.activeBucket.buildFd != nil {
			_ = ctr.activeBucket.buildFd.Close()
		}
		if ctr.activeBucket.probeFd != nil {
			_ = ctr.activeBucket.probeFd.Close()
		}
		ctr.activeBucket = nil
	}
	if len(ctr.spillQueue) == 0 {
		return
	}
	for _, sb := range ctr.spillQueue {
		if sb.buildFd != nil {
			sb.buildFd.Close()
		}
		if sb.probeFd != nil {
			sb.probeFd.Close()
		}
	}
	ctr.spillQueue = nil
}

func (ctr *container) resetNonEqCondExecutor() {
	if ctr.nonEqCondExec != nil {
		ctr.nonEqCondExec.ResetForNextQuery()
	}
}

func (ctr *container) cleanNonEqCondExecutor() {
	if ctr.nonEqCondExec != nil {
		ctr.nonEqCondExec.Free()
		ctr.nonEqCondExec = nil
	}
}

func (ctr *container) cleanBatch(proc *process.Process) {
	ctr.rightBats = nil
	if ctr.resBat != nil {
		ctr.resBat.Clean(proc.GetMPool())
		ctr.resBat = nil
	}
	for i := range ctr.joinBats {
		if ctr.joinBats[i] != nil {
			ctr.joinBats[i].Clean(proc.Mp())
			ctr.joinBats[i] = nil
		}
	}
	if ctr.rightRowsMatched != nil {
		ctr.rightRowsMatched = nil
	}
}

func (ctr *container) cleanBucketBatches(proc *process.Process) {
	if ctr.spillBuildReader != nil {
		ctr.spillBuildReader.close()
		ctr.spillBuildReader = nil
	}
	if ctr.probeBucketReader != nil {
		ctr.probeBucketReader.close()
		ctr.probeBucketReader = nil
		ctr.probeBucketActive = false
	}
	if ctr.spillBuildReadBatch != nil {
		ctr.spillBuildReadBatch.Clean(proc.Mp())
		ctr.spillBuildReadBatch = nil
	}
	if ctr.spillProbeReadBatch != nil {
		ctr.spillProbeReadBatch.Clean(proc.Mp())
		ctr.spillProbeReadBatch = nil
	}
	ctr.cleanSpillBufferPool(proc)
}

func (ctr *container) cleanHashMap() {
	if ctr.mp != nil {
		ctr.mp.Free()
		ctr.mp = nil
	}
}

func (ctr *container) cleanEqCondExecutors() {
	for i := range ctr.eqCondExecs {
		if ctr.eqCondExecs[i] != nil {
			ctr.eqCondExecs[i].Free()
		}
	}
	ctr.eqCondExecs = nil
}

func (ctr *container) resetEqCondExecutors() {
	for i := range ctr.eqCondExecs {
		if ctr.eqCondExecs[i] != nil {
			ctr.eqCondExecs[i].ResetForNextQuery()
		}
	}
}

func (hashJoin *HashJoin) IsInner() bool {
	return hashJoin.JoinType == plan.Node_INNER
}

func (hashJoin *HashJoin) IsLeftOuter() bool {
	return hashJoin.JoinType == plan.Node_LEFT
}

func (hashJoin *HashJoin) IsRightOuter() bool {
	return hashJoin.JoinType == plan.Node_RIGHT
}

func (hashJoin *HashJoin) IsFullOuter() bool {
	return hashJoin.JoinType == plan.Node_OUTER
}

func (hashJoin *HashJoin) IsSemi() bool {
	return hashJoin.JoinType == plan.Node_SEMI
}

func (hashJoin *HashJoin) IsLeftSemi() bool {
	return !hashJoin.IsRightJoin && hashJoin.JoinType == plan.Node_SEMI
}

func (hashJoin *HashJoin) IsRightSemi() bool {
	return hashJoin.IsRightJoin && hashJoin.JoinType == plan.Node_SEMI
}

func (hashJoin *HashJoin) IsAnti() bool {
	return hashJoin.JoinType == plan.Node_ANTI
}

func (hashJoin *HashJoin) IsLeftAnti() bool {
	return !hashJoin.IsRightJoin && hashJoin.JoinType == plan.Node_ANTI
}

func (hashJoin *HashJoin) IsRightAnti() bool {
	return hashJoin.IsRightJoin && hashJoin.JoinType == plan.Node_ANTI
}

func (hashJoin *HashJoin) IsSingle() bool {
	return hashJoin.JoinType == plan.Node_SINGLE
}

func (hashJoin *HashJoin) IsLeftSingle() bool {
	return !hashJoin.IsRightJoin && hashJoin.JoinType == plan.Node_SINGLE
}

func (hashJoin *HashJoin) IsRightSingle() bool {
	return hashJoin.IsRightJoin && hashJoin.JoinType == plan.Node_SINGLE
}

// EmitUnmatchedProbe reports whether the join must emit unmatched rows from
// the probe (left) side. True for LEFT OUTER, LEFT SINGLE, LEFT ANTI, and
// FULL OUTER joins.
func (hashJoin *HashJoin) EmitUnmatchedProbe() bool {
	return hashJoin.IsLeftOuter() || hashJoin.IsLeftSingle() || hashJoin.IsLeftAnti() || hashJoin.IsFullOuter()
}

// EmitUnmatchedBuild reports whether the join must emit unmatched rows from
// the build (right) side. True when IsRightJoin is set (RIGHT-flavoured joins,
// including RIGHT OUTER / RIGHT SEMI / RIGHT ANTI / RIGHT SINGLE / RIGHT
// DEDUP), or for FULL OUTER which always needs both sides.
func (hashJoin *HashJoin) EmitUnmatchedBuild() bool {
	return hashJoin.IsRightJoin || hashJoin.IsFullOuter()
}

func (ctr *container) setSpillThreshold(threshold int64) {
	if threshold == 0 {
		// 0 means auto config
		fileCacheMem := fileservice.GlobalMemoryCacheSizeHint.Load()
		mem := (int64(system.MemoryTotal()) - fileCacheMem) / int64(system.GoMaxProcs()) / 8
		// min 128MB
		if mem < common.MiB*128 {
			mem = common.MiB * 128
		}
		ctr.spillThreshold = mem
	} else {
		ctr.spillThreshold = threshold
	}
}

func (hashJoin *HashJoin) spillGeneration() bool {
	return hashJoin.ctr.spillGeneration || hashJoin.SpillGroup != nil && hashJoin.SpillGroup.IsSpilled()
}
