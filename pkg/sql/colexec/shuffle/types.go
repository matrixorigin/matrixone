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
	"github.com/matrixorigin/matrixone/pkg/vm"
	"github.com/matrixorigin/matrixone/pkg/vm/process"
)

var _ vm.Operator = new(Argument)

type Argument struct {
	ctr                *container
	ShuffleColIdx      int32
	ShuffleType        int32
	AliveRegCnt        int32
	ShuffleColMin      int64
	ShuffleColMax      int64
	info               *vm.OperatorInfo
	children           []vm.Operator
	ShuffleRangeUint64 []uint64
	ShuffleRangeInt64  []int64
}

func init() {
	reuse.CreatePool[Argument](
		func() *Argument {
			return &Argument{}
		},
		func(a *Argument) {
			*a = Argument{}
		},
		reuse.DefaultOptions[Argument]().
			WithEnableChecker(),
	)
}

func (arg Argument) TypeName() string {
	return argName
}

func NewArgument() *Argument {
	return reuse.Alloc[Argument](nil)
}

func (arg *Argument) Release() {
	if arg != nil {
		reuse.Free[Argument](arg, nil)
	}
}

func (arg *Argument) SetInfo(info *vm.OperatorInfo) {
	arg.info = info
}

func (arg *Argument) GetCnAddr() string {
	return arg.info.CnAddr
}

func (arg *Argument) GetOperatorID() int32 {
	return arg.info.OperatorID
}

func (arg *Argument) GetParalleID() int32 {
	return arg.info.ParallelID
}

func (arg *Argument) GetMaxParallel() int32 {
	return arg.info.MaxParallel
}

func (arg *Argument) AppendChild(child vm.Operator) {
	arg.children = append(arg.children, child)
}

type container struct {
	ending        bool
	sels          [][]int32
	shufflePool   []*batch.Batch
	sendPool      []*batch.Batch
	lastSentBatch *batch.Batch
}

func (arg *Argument) Free(proc *process.Process, pipelineFailed bool, err error) {
	if arg.ctr != nil {
		for i := range arg.ctr.shufflePool {
			if arg.ctr.shufflePool[i] != nil {
				arg.ctr.shufflePool[i].Clean(proc.Mp())
				arg.ctr.shufflePool[i] = nil
			}
		}
		arg.ctr.sels = nil
	}
}
