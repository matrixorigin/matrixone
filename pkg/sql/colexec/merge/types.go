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

package merge

import (
	"github.com/matrixorigin/matrixone/pkg/common/reuse"
	"github.com/matrixorigin/matrixone/pkg/container/batch"
	"github.com/matrixorigin/matrixone/pkg/sql/colexec"
	"github.com/matrixorigin/matrixone/pkg/vm"

	"github.com/matrixorigin/matrixone/pkg/vm/process"
)

var _ vm.Operator = new(Merge)

type container struct {
	buf *batch.Batch
	colexec.ReceiverOperator
}

type Merge struct {
	ctr      container
	SinkScan bool
	Partial  bool  // false means listening on all merge receivers
	StartIDX int32 // if partial, listening on receivers[start:end]
	EndIDX   int32
	vm.OperatorBase
}

func (merge *Merge) GetOperatorBase() *vm.OperatorBase {
	return &merge.OperatorBase
}

func init() {
	reuse.CreatePool[Merge](
		func() *Merge {
			return &Merge{}
		},
		func(a *Merge) {
			*a = Merge{}
		},
		reuse.DefaultOptions[Merge]().
			WithEnableChecker(),
	)
}

func (merge Merge) TypeName() string {
	return opName
}

func NewArgument() *Merge {
	return reuse.Alloc[Merge](nil)
}

func (merge *Merge) WithSinkScan(sinkScan bool) *Merge {
	merge.SinkScan = sinkScan
	return merge
}

func (merge *Merge) WithPartial(start, end int32) *Merge {
	merge.Partial = true
	merge.StartIDX = start
	merge.EndIDX = end
	return merge
}

func (merge *Merge) Release() {
	if merge != nil {
		reuse.Free[Merge](merge, nil)
	}
}

func (merge *Merge) Reset(proc *process.Process, pipelineFailed bool, err error) {
	merge.ctr.FreeMergeTypeOperator(pipelineFailed)
}

func (merge *Merge) Free(proc *process.Process, pipelineFailed bool, err error) {
	if merge.ctr.buf != nil {
		// merge.ctr.buf.Clean(proc.Mp())
		proc.PutBatch(merge.ctr.buf)
		merge.ctr.buf = nil
	}
}
