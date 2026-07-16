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

package hashbuild

import (
	"bytes"
	"os"
	"sync"
	"sync/atomic"

	"github.com/matrixorigin/matrixone/pkg/common/moerr"
	"github.com/matrixorigin/matrixone/pkg/common/reuse"
	"github.com/matrixorigin/matrixone/pkg/container/batch"
	"github.com/matrixorigin/matrixone/pkg/container/vector"
	"github.com/matrixorigin/matrixone/pkg/fileservice"
	"github.com/matrixorigin/matrixone/pkg/pb/plan"
	"github.com/matrixorigin/matrixone/pkg/sql/colexec"
	"github.com/matrixorigin/matrixone/pkg/vm"
	"github.com/matrixorigin/matrixone/pkg/vm/message"
	"github.com/matrixorigin/matrixone/pkg/vm/process"
)

var _ vm.Operator = new(HashBuild)

const (
	BuildHashMap = iota
	HandleRuntimeFilter
	SendJoinMap
	SendSucceed
)

type container struct {
	state           int
	runtimeFilterIn bool
	// terminalPublished is the per-execution generation gate for the JoinMap
	// dependency.  A successful JoinMap or a BuildError wins this gate exactly
	// once; Reset/Free/cancel paths cannot replace or duplicate it.
	terminalPublished uint32
	terminalMu        sync.Mutex
	runtimeFilterDone bool
	hashmapBuilder    HashmapBuilder
	spilledFds        []*os.File // anonymous build-side spill fds (ownership transferred to JoinMap)
	spillFS           fileservice.MutableFileService
	spillUUID         string // unique prefix for anonymous file paths
	spillThreshold    int64

	// reusable buffers for spill operations
	spillHashValues   []uint64
	spillBucketRowIds [][]int32
	spillWriteBuf     bytes.Buffer
	spillKeyVecs      []*vector.Vector

	// cached expression executors for spill (reused across batches)
	spillExprExecs []colexec.ExpressionExecutor
}

type HashBuild struct {
	ctr               container
	NeedHashMap       bool
	HashOnPK          bool
	NeedBatches       bool
	NeedAllocateSels  bool
	TrackNullKeys     bool
	IsShuffle         bool
	Conditions        []*plan.Expr
	JoinMapTag        int32
	JoinMapRefCnt     int32
	ShuffleIdx        int32
	RuntimeFilterSpec *plan.RuntimeFilterSpec
	SpillThreshold    int64

	IsDedup                   bool
	DedupBuildKeepLast        bool
	DelColIdx                 int32
	OnDuplicateAction         plan.Node_OnDuplicateAction
	DedupColName              string
	DedupColTypes             []plan.Type
	DedupDeleteMarkerColIdx   int32
	DedupDeleteKeepColIdxList []int32

	vm.OperatorBase
}

func (hashBuild *HashBuild) GetOperatorBase() *vm.OperatorBase {
	return &hashBuild.OperatorBase
}

func init() {
	reuse.CreatePool[HashBuild](
		func() *HashBuild {
			return &HashBuild{}
		},
		func(a *HashBuild) {
			// Preserve cached iterators across pool resets to avoid reallocating
			// short-lived iterator buffers. Detach owners so old hashmaps can be GC'd.
			intItr, strItr := a.ctr.hashmapBuilder.ExtractCachedIteratorsForReuse()

			*a = HashBuild{}
			a.ctr.hashmapBuilder.RestoreCachedIterators(intItr, strItr)
		},
		reuse.DefaultOptions[HashBuild]().
			WithEnableChecker(),
	)
}

func (hashBuild *HashBuild) TypeName() string {
	return opName
}

func NewArgument() *HashBuild {
	return reuse.Alloc[HashBuild](nil)
}

func (hashBuild *HashBuild) Release() {
	if hashBuild != nil {
		reuse.Free[HashBuild](hashBuild, nil)
	}
}

func (hashBuild *HashBuild) Reset(proc *process.Process, pipelineFailed bool, err error) {
	hashBuild.ctr.terminalMu.Lock()
	defer hashBuild.ctr.terminalMu.Unlock()
	runtimeSucceed := hashBuild.ctr.state > HandleRuntimeFilter
	mapSucceed := hashBuild.ctr.state == SendSucceed

	// Call does not publish pipeline terminal signals.  Reset owns dependency
	// finalization and is intentionally non-blocking: publication only appends
	// one immutable value to the current-CN MessageBoard.
	if !mapSucceed {
		if pipelineFailed || err != nil {
			if err == nil {
				err = moerr.NewQueryInterrupted(proc.Ctx)
			}
			hashBuild.publishBuildError(proc, err)
		} else {
			// Preserve the established nil JoinMap convention for a true empty
			// build and for legacy cleanup paths that completed without a map.
			hashBuild.publishJoinMap(proc, nil)
		}
	}

	hashBuild.ctr.hashmapBuilder.Reset(proc, !mapSucceed)
	// Only clean up build files when the join map was NOT successfully sent.
	// When mapSucceed=true, hashjoin owns the files and deletes them after reading.
	if !mapSucceed {
		hashBuild.cleanupSpillFiles(proc)
	}
	hashBuild.ctr.spilledFds = nil
	hashBuild.ctr.state = BuildHashMap
	hashBuild.ctr.runtimeFilterIn = false
	if !hashBuild.ctr.runtimeFilterDone {
		if pipelineFailed || err != nil {
			// A failed build must complete the runtime-filter dependency with
			// PASS.  DROP would incorrectly filter all probe rows because no
			// unique keys were published.
			message.FinalizeRuntimeFilterOnBuildError(hashBuild.RuntimeFilterSpec, proc.GetMessageBoard())
		} else {
			message.FinalizeRuntimeFilter(hashBuild.RuntimeFilterSpec, runtimeSucceed, proc.GetMessageBoard())
		}
	}
	hashBuild.ctr.runtimeFilterDone = false
}
func (hashBuild *HashBuild) Free(proc *process.Process, pipelineFailed bool, err error) {
	hashBuild.ctr.terminalMu.Lock()
	defer hashBuild.ctr.terminalMu.Unlock()
	// Normally Reset runs before Free.  Keep Free as a safe fallback for
	// cancellation/error cleanup paths that bypass Reset, while preserving the
	// exactly-once generation gate.
	if atomic.LoadUint32(&hashBuild.ctr.terminalPublished) == 0 && (pipelineFailed || err != nil) {
		if err == nil {
			err = moerr.NewQueryInterrupted(proc.Ctx)
		}
		hashBuild.publishBuildError(proc, err)
	}
	hashBuild.cleanupSpillFiles(proc)
	hashBuild.ctr.hashmapBuilder.Free(proc)
	hashBuild.ctr.freeSpillExprExecs()
	hashBuild.ctr.spillKeyVecs = nil
	hashBuild.ctr.spillHashValues = nil
	hashBuild.ctr.spillBucketRowIds = nil
}

func (hashBuild *HashBuild) publishJoinMap(proc *process.Process, jm *message.JoinMap) bool {
	if !atomic.CompareAndSwapUint32(&hashBuild.ctr.terminalPublished, 0, 1) {
		return false
	}
	message.SendJoinMapResult(
		message.NewJoinMapResult(jm),
		hashBuild.JoinMapTag,
		hashBuild.IsShuffle,
		hashBuild.ShuffleIdx,
		proc.GetMessageBoard(),
	)
	return true
}

func (hashBuild *HashBuild) publishBuildError(proc *process.Process, err error) bool {
	if !atomic.CompareAndSwapUint32(&hashBuild.ctr.terminalPublished, 0, 1) {
		return false
	}
	message.FinalizeJoinMapBuildError(
		proc.GetMessageBoard(),
		hashBuild.JoinMapTag,
		hashBuild.IsShuffle,
		hashBuild.ShuffleIdx,
		err,
	)
	return true
}

func (hashBuild *HashBuild) cleanupSpillFiles(proc *process.Process) {
	for i, fd := range hashBuild.ctr.spilledFds {
		if fd != nil {
			fd.Close()
			hashBuild.ctr.spilledFds[i] = nil
		}
	}
	hashBuild.ctr.spilledFds = nil
}

func (hashBuild *HashBuild) ExecProjection(proc *process.Process, input *batch.Batch) (*batch.Batch, error) {
	return input, nil
}

func (ctr *container) setSpillThreshold(threshold int64) {
	ctr.spillThreshold = colexec.ResolveSpillThreshold(threshold)
}
