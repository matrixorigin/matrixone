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

	"github.com/matrixorigin/matrixone/pkg/common/reuse"
	"github.com/matrixorigin/matrixone/pkg/container/batch"
	"github.com/matrixorigin/matrixone/pkg/pb/plan"
	"github.com/matrixorigin/matrixone/pkg/sql/colexec"
	"github.com/matrixorigin/matrixone/pkg/vm"
	"github.com/matrixorigin/matrixone/pkg/vm/process"
)

var _ vm.Operator = new(Shuffle)

type Shuffle struct {
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
	ending               bool
	sels                 [][]int32
	buf                  *batch.Batch
	pendingBat           *batch.Batch
	pendingBucket        int
	pendingOffset        int
	shufflePool          *ShufflePool
	runtimeFilterHandled bool
	exprExec             colexec.ExpressionExecutor
	held                 bool
	writingStopped       bool

	producerOnce    sync.Once
	producerStarted bool
	producerProc    *process.Process
	producerCancel  context.CancelCauseFunc
	producerDone    chan struct{}
	producerRows    int64
	producerSize    int64

	directBatches chan directHandoff
	directAck     chan struct{}
	consumerDone  chan struct{}
	consumerOnce  sync.Once
	bufFromPool   bool
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
	if !shuffle.DrainAllBuckets {
		if (pipelineFailed || err != nil) && shuffle.ctr.shufflePool != nil {
			abortErr := err
			if abortErr == nil {
				abortErr = context.Canceled
			}
			shuffle.ctr.shufflePool.abortWithError(proc.Mp(), abortErr)
		}
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
	shuffle.ctr.held = false
	shuffle.ctr.writingStopped = false
	shuffle.ctr.producerOnce = sync.Once{}
	shuffle.ctr.producerStarted = false
	shuffle.ctr.producerProc = nil
	shuffle.ctr.producerCancel = nil
	shuffle.ctr.producerDone = nil
	shuffle.ctr.producerRows = 0
	shuffle.ctr.producerSize = 0
	shuffle.ctr.directBatches = nil
	shuffle.ctr.directAck = nil
	shuffle.ctr.consumerDone = nil
	shuffle.ctr.consumerOnce = sync.Once{}
	shuffle.ctr.bufFromPool = false
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
	if failed && shuffle.ctr.producerCancel != nil {
		if err == nil {
			err = context.Canceled
		}
		shuffle.ctr.producerCancel(err)
	}
	if shuffle.ctr.producerStarted && shuffle.ctr.producerDone != nil {
		<-shuffle.ctr.producerDone
	} else if shuffle.ctr.held {
		shuffle.stopWritingOnce()
	}
	if shuffle.ctr.producerCancel != nil {
		shuffle.ctr.producerCancel(nil)
	}
}

func (shuffle *Shuffle) Free(proc *process.Process, pipelineFailed bool, err error) {
	shuffle.ctr.buf = nil
	shuffle.ctr.pendingBat = nil
	shuffle.ctr.shufflePool = nil
	if shuffle.ctr.exprExec != nil {
		shuffle.ctr.exprExec.Free()
		shuffle.ctr.exprExec = nil
	}
}

func (shuffle *Shuffle) ExecProjection(proc *process.Process, input *batch.Batch) (*batch.Batch, error) {
	return input, nil
}
