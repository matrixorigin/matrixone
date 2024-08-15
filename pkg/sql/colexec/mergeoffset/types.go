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

package mergeoffset

import (
	"github.com/matrixorigin/matrixone/pkg/common/reuse"
	"github.com/matrixorigin/matrixone/pkg/sql/colexec"
	"github.com/matrixorigin/matrixone/pkg/sql/plan"
	"github.com/matrixorigin/matrixone/pkg/vm"

	"github.com/matrixorigin/matrixone/pkg/vm/process"
)

var _ vm.Operator = new(MergeOffset)

type container struct {
	seen           uint64
	offset         uint64
	offsetExecutor colexec.ExpressionExecutor
}

type MergeOffset struct {
	// Offset records the offset number of mergeOffset operator
	Offset *plan.Expr
	// ctr contains the attributes needn't do serialization work
	ctr *container
	vm.OperatorBase
}

func (mergeOffset *MergeOffset) GetOperatorBase() *vm.OperatorBase {
	return &mergeOffset.OperatorBase
}

func init() {
	reuse.CreatePool[MergeOffset](
		func() *MergeOffset {
			return &MergeOffset{}
		},
		func(a *MergeOffset) {
			*a = MergeOffset{}
		},
		reuse.DefaultOptions[MergeOffset]().
			WithEnableChecker(),
	)
}

func (mergeOffset MergeOffset) TypeName() string {
	return opName
}

func NewArgument() *MergeOffset {
	return reuse.Alloc[MergeOffset](nil)
}

func (mergeOffset *MergeOffset) WithOffset(offset *plan.Expr) *MergeOffset {
	mergeOffset.Offset = offset
	return mergeOffset
}

func (mergeOffset *MergeOffset) Release() {
	if mergeOffset != nil {
		reuse.Free[MergeOffset](mergeOffset, nil)
	}
}

func (mergeOffset *MergeOffset) Reset(proc *process.Process, pipelineFailed bool, err error) {
	mergeOffset.Free(proc, pipelineFailed, err)
}

func (mergeOffset *MergeOffset) Free(proc *process.Process, pipelineFailed bool, err error) {
	if mergeOffset.ctr != nil {
		if mergeOffset.ctr.offsetExecutor != nil {
			mergeOffset.ctr.offsetExecutor.Free()
			mergeOffset.ctr.offsetExecutor = nil
		}
		mergeOffset.ctr = nil
	}

}
