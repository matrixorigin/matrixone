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

package shuffle

import (
	"context"
	"sync"

	"github.com/matrixorigin/matrixone/pkg/common/bloomfilter"
	"github.com/matrixorigin/matrixone/pkg/common/reuse"
	"github.com/matrixorigin/matrixone/pkg/container/batch"
	"github.com/matrixorigin/matrixone/pkg/container/vector"
	"github.com/matrixorigin/matrixone/pkg/pb/plan"
	"github.com/matrixorigin/matrixone/pkg/sql/colexec"
	"github.com/matrixorigin/matrixone/pkg/sql/colexec/runtimefilter"
	"github.com/matrixorigin/matrixone/pkg/vm"
	"github.com/matrixorigin/matrixone/pkg/vm/process"
)

var _ vm.Operator = new(Shuffle)

type Shuffle struct {
	LocalFilter        *runtimefilter.LocalShuffleFilter
	ctr                container
	CurrentShuffleIdx  int32
	ShuffleColIdx      int32
	ShuffleType        int32
	BucketNum          int32
	ShuffleColMin      int64
	ShuffleColMax      int64
	ShuffleRangeUint64 []uint64
	ShuffleRangeInt64  []int64
	RuntimeFilterSpec  *plan.RuntimeFilterSpec
	ShuffleExpr        *plan.Expr
	DrainAllBuckets    bool
	StringHashKey      bool
	// ShuffleKeyColumns selects a complete equality tuple for local-only
	// synchronous hash routing. It is not part of the remote pipeline format.
	ShuffleKeyColumns []int32
	vm.OperatorBase
}

func (shuffle *Shuffle) GetOperatorBase() *vm.OperatorBase {
	return &shuffle.OperatorBase
}

// ResetBeforeChildren reports that a local Shuffle owns an asynchronous task
// which may still be executing its child. Pipeline cleanup must join that task
// before resetting descendants.
func (shuffle *Shuffle) ResetBeforeChildren() bool {
	return !shuffle.DrainAllBuckets
}

func init() {
	reuse.CreatePool[Shuffle](
		func() *Shuffle {
			return &Shuffle{}
		},
		func(a *Shuffle) {
			*a = Shuffle{}
		},
		reuse.DefaultOptions[Shuffle]().
			WithEnableChecker(),
	)
}

func (shuffle *Shuffle) TypeName() string {
	return opName
}

func NewArgument() *Shuffle {
	return reuse.Alloc[Shuffle](nil)
}

func (shuffle *Shuffle) Release() {
	if shuffle != nil {
		reuse.Free[Shuffle](shuffle, nil)
	}
}

type container struct {
	localBloom           *bloomfilter.CBloomFilter
	filterScratch        []uint8
	filterTested         int64
	filterRejected       int64
	filterBypassed       bool
	ending               bool
	sels                 [][]int32
	buf                  *batch.Batch
	pendingBat           *batch.Batch
	pendingBucket        int
	pendingOffset        int
	shufflePool          *ShufflePool
	runtimeFilterHandled bool
	stableStringHash     bool
	exprExec             colexec.ExpressionExecutor
	held                 bool
	writingStopped       bool

	producerOnce    sync.Once
	producerStarted bool
	producerProc    *process.Process
	producerDone    chan struct{}
	producerRows    int64
	producerSize    int64

	directBatches chan directHandoff
	directAck     chan struct{}
	consumerDone  chan struct{}
	consumerOnce  sync.Once
	bufFromPool   bool
	keyVectors    []*vector.Vector
	keyHashes     *vector.Vector
}

type directHandoff struct {
	bat *batch.Batch
	ack chan struct{}
}

func (shuffle *Shuffle) SetShufflePool(sp *ShufflePool) {
	shuffle.ctr.shufflePool = sp
}

func (shuffle *Shuffle) GetShufflePool() *ShufflePool {
	return shuffle.ctr.shufflePool
}

func (shuffle *Shuffle) Reset(proc *process.Process, pipelineFailed bool, err error) {
	if (pipelineFailed || err != nil) && shuffle.ctr.shufflePool != nil {
		abortErr := err
		if abortErr == nil {
			abortErr = context.Canceled
		}
		shuffle.ctr.shufflePool.abortWithError(proc.Mp(), abortErr)
	}
	if !shuffle.DrainAllBuckets {
		if shuffle.ctr.held {
			shuffle.ackDirectBatch()
			if shuffle.ctr.buf != nil && shuffle.ctr.bufFromPool && shuffle.ctr.shufflePool != nil {
				shuffle.ctr.shufflePool.discardBatch(shuffle.ctr.buf, proc.Mp())
			}
			shuffle.ctr.buf = nil
			shuffle.ctr.bufFromPool = false
			shuffle.closeLocalConsumer(proc)
		}
		shuffle.stopLocalProducer(pipelineFailed || err != nil, err)
		if shuffle.ctr.held && shuffle.OpAnalyzer != nil {
			stats := shuffle.OpAnalyzer.GetOpStats()
			if shuffle.LocalFilter != nil {
				stats.AddExtraStat("ShuffleRuntimeFilterTestedRows", shuffle.ctr.filterTested)
				stats.AddExtraStat("ShuffleRuntimeFilterRejectedRows", shuffle.ctr.filterRejected)
			}
			stats.InputRows += shuffle.ctr.producerRows
			stats.InputSize += shuffle.ctr.producerSize
		}
	} else if !pipelineFailed && err == nil {
		shuffle.stopWritingOnce()
	}

	shuffle.ackDirectBatch()
	if shuffle.ctr.buf != nil {
		if shuffle.DrainAllBuckets || shuffle.ctr.bufFromPool {
			shuffle.ctr.shufflePool.discardBatch(shuffle.ctr.buf, proc.Mp())
		}
		shuffle.ctr.buf = nil
	}
	if shuffle.ctr.shufflePool != nil {
		var peak int64
		var ownsStats bool
		if pipelineFailed || err != nil {
			if shuffle.ctr.held {
				peak, ownsStats = shuffle.ctr.shufflePool.release(proc.Mp(), true)
			} else {
				shuffle.ctr.shufflePool.abort(proc.Mp())
			}
		} else if shuffle.ctr.held {
			peak, ownsStats = shuffle.ctr.shufflePool.release(proc.Mp(), false)
		}
		if ownsStats && shuffle.OpAnalyzer != nil {
			shuffle.OpAnalyzer.SetMemUsed(peak)
		}
	}
	shuffle.ctr.shufflePool = nil
	shuffle.ctr.sels = nil
	shuffle.ctr.ending = false
	shuffle.ctr.pendingBat = nil
	shuffle.ctr.pendingBucket = 0
	shuffle.ctr.pendingOffset = 0
	shuffle.ctr.runtimeFilterHandled = false
	shuffle.ctr.localBloom = nil
	shuffle.ctr.filterScratch = nil
	shuffle.ctr.filterTested = 0
	shuffle.ctr.filterRejected = 0
	shuffle.ctr.filterBypassed = false
	shuffle.ctr.stableStringHash = false
	shuffle.ctr.held = false
	shuffle.ctr.writingStopped = false
	shuffle.ctr.producerOnce = sync.Once{}
	shuffle.ctr.producerStarted = false
	shuffle.ctr.producerProc = nil
	shuffle.ctr.producerDone = nil
	shuffle.ctr.producerRows = 0
	shuffle.ctr.producerSize = 0
	shuffle.ctr.directBatches = nil
	shuffle.ctr.directAck = nil
	shuffle.ctr.consumerDone = nil
	shuffle.ctr.consumerOnce = sync.Once{}
	shuffle.ctr.bufFromPool = false
	shuffle.freeKeyScratch(proc)
}

func (shuffle *Shuffle) freeKeyScratch(proc *process.Process) {
	clear(shuffle.ctr.keyVectors)
	shuffle.ctr.keyVectors = nil
	if shuffle.ctr.keyHashes != nil {
		shuffle.ctr.keyHashes.Free(proc.Mp())
		shuffle.ctr.keyHashes = nil
	}
}

func (shuffle *Shuffle) closeLocalConsumer(proc *process.Process) {
	shuffle.ctr.consumerOnce.Do(func() {
		if shuffle.ctr.consumerDone != nil {
			close(shuffle.ctr.consumerDone)
		}
		if shuffle.ctr.shufflePool != nil {
			shuffle.ctr.shufflePool.closeConsumer(shuffle.CurrentShuffleIdx, proc.Mp())
		}
	})
}

func (shuffle *Shuffle) stopLocalProducer(failed bool, err error) {
	if shuffle.ctr.producerProc == nil {
		return
	}
	if failed {
		if err == nil {
			err = context.Canceled
		}
		shuffle.ctr.producerProc.Cancel(err)
	}
	if shuffle.ctr.producerStarted && shuffle.ctr.producerDone != nil {
		<-shuffle.ctr.producerDone
	} else if shuffle.ctr.held {
		shuffle.stopWritingOnce()
	}
	shuffle.ctr.producerProc.Cancel(nil)
}

func (shuffle *Shuffle) Free(proc *process.Process, pipelineFailed bool, err error) {
	shuffle.freeKeyScratch(proc)
	shuffle.ctr.buf = nil
	shuffle.ctr.pendingBat = nil
	shuffle.ctr.shufflePool = nil
	if shuffle.ctr.exprExec != nil {
		shuffle.ctr.exprExec.Free()
		shuffle.ctr.exprExec = nil
	}
}
