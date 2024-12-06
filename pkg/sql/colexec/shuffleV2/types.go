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

package shuffleV2

import (
	"github.com/matrixorigin/matrixone/pkg/common/reuse"
	"github.com/matrixorigin/matrixone/pkg/container/batch"
	"github.com/matrixorigin/matrixone/pkg/vm"
	"github.com/matrixorigin/matrixone/pkg/vm/process"
)

var _ vm.Operator = new(ShuffleV2)

type ShuffleV2 struct {
	ctr                container
	CurrentShuffleIdx  int32
	ShuffleColIdx      int32
	ShuffleType        int32
	BucketNum          int32
	ShuffleColMin      int64
	ShuffleColMax      int64
	ShuffleRangeUint64 []uint64
	ShuffleRangeInt64  []int64
	IsDebug            bool //used for unit test
	vm.OperatorBase
}

func (shuffle *ShuffleV2) GetOperatorBase() *vm.OperatorBase {
	return &shuffle.OperatorBase
}

func init() {
	reuse.CreatePool[ShuffleV2](
		func() *ShuffleV2 {
			return &ShuffleV2{}
		},
		func(a *ShuffleV2) {
			*a = ShuffleV2{}
		},
		reuse.DefaultOptions[ShuffleV2]().
			WithEnableChecker(),
	)
}

func (shuffle ShuffleV2) TypeName() string {
	return opName
}

func NewArgument() *ShuffleV2 {
	return reuse.Alloc[ShuffleV2](nil)
}

func (shuffle *ShuffleV2) Release() {
	if shuffle != nil {
		reuse.Free[ShuffleV2](shuffle, nil)
	}
}

type container struct {
	ending      bool
	sels        [][]int32
	buf         *batch.Batch
	shufflePool *ShufflePoolV2
}

func (shuffle *ShuffleV2) SetShufflePool(sp *ShufflePoolV2) {
	shuffle.ctr.shufflePool = sp
}

func (shuffle *ShuffleV2) GetShufflePool() *ShufflePoolV2 {
	return shuffle.ctr.shufflePool
}

func (shuffle *ShuffleV2) Reset(proc *process.Process, pipelineFailed bool, err error) {
	if shuffle.ctr.buf != nil {
		shuffle.ctr.buf.Clean(proc.Mp())
	}
	if shuffle.ctr.shufflePool != nil {
		shuffle.ctr.shufflePool.Reset(proc.Mp())
	}
	shuffle.ctr.shufflePool = nil
	shuffle.ctr.sels = nil
	shuffle.ctr.ending = false
}

func (shuffle *ShuffleV2) Free(proc *process.Process, pipelineFailed bool, err error) {
	shuffle.ctr.buf = nil
	shuffle.ctr.shufflePool = nil
}

func (shuffle *ShuffleV2) ExecProjection(proc *process.Process, input *batch.Batch) (*batch.Batch, error) {
	return input, nil
}
