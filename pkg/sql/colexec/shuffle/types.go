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
	"github.com/matrixorigin/matrixone/pkg/common/reuse"
	"github.com/matrixorigin/matrixone/pkg/container/batch"
	"github.com/matrixorigin/matrixone/pkg/pb/plan"
	"github.com/matrixorigin/matrixone/pkg/vm"
	"github.com/matrixorigin/matrixone/pkg/vm/message"
	"github.com/matrixorigin/matrixone/pkg/vm/process"
)

var _ vm.Operator = new(Shuffle)

type Shuffle struct {
	ctr                container
	ShuffleColIdx      int32
	ShuffleType        int32
	BucketNum          int32
	ShuffleColMin      int64
	ShuffleColMax      int64
	ShuffleRangeUint64 []uint64
	ShuffleRangeInt64  []int64
	RuntimeFilterSpec  *plan.RuntimeFilterSpec
	msgReceiver        *message.MessageReceiver
	vm.OperatorBase
}

func (shuffle *Shuffle) GetOperatorBase() *vm.OperatorBase {
	return &shuffle.OperatorBase
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

func (shuffle Shuffle) TypeName() string {
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
	sels                 [][]int64
	buf                  *batch.Batch
	shufflePool          *ShufflePool
	runtimeFilterHandled bool
}

func (shuffle *Shuffle) SetShufflePool(sp *ShufflePool) {
	shuffle.ctr.shufflePool = sp
}

func (shuffle *Shuffle) Reset(proc *process.Process, pipelineFailed bool, err error) {
	if shuffle.RuntimeFilterSpec != nil {
		shuffle.ctr.runtimeFilterHandled = false
	}
	if shuffle.ctr.buf != nil {
		shuffle.ctr.buf.Clean(proc.Mp())
	}
	if shuffle.ctr.shufflePool != nil {
		shuffle.ctr.shufflePool.Reset(proc.Mp())
	}
	shuffle.ctr.sels = nil
	shuffle.ctr.ending = false
}

func (shuffle *Shuffle) Free(proc *process.Process, pipelineFailed bool, err error) {
	shuffle.ctr.buf = nil
	shuffle.ctr.shufflePool = nil
}
