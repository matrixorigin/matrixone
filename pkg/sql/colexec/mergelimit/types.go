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

package mergelimit

import (
	"github.com/matrixorigin/matrixone/pkg/common/reuse"
	"github.com/matrixorigin/matrixone/pkg/sql/colexec"
	"github.com/matrixorigin/matrixone/pkg/sql/plan"
	"github.com/matrixorigin/matrixone/pkg/vm"

	"github.com/matrixorigin/matrixone/pkg/vm/process"
)

var _ vm.Operator = new(MergeLimit)

type container struct {
	seen          uint64
	limit         uint64
	limitExecutor colexec.ExpressionExecutor
}

type MergeLimit struct {
	// Limit records the limit number of this operator
	Limit *plan.Expr
	// ctr stores the attributes needn't do Serialization work
	ctr *container
	vm.OperatorBase
}

func (mergeLimit *MergeLimit) GetOperatorBase() *vm.OperatorBase {
	return &mergeLimit.OperatorBase
}

func init() {
	reuse.CreatePool[MergeLimit](
		func() *MergeLimit {
			return &MergeLimit{}
		},
		func(a *MergeLimit) {
			*a = MergeLimit{}
		},
		reuse.DefaultOptions[MergeLimit]().
			WithEnableChecker(),
	)
}

func (mergeLimit MergeLimit) TypeName() string {
	return opName
}

func NewArgument() *MergeLimit {
	return reuse.Alloc[MergeLimit](nil)
}

func (mergeLimit *MergeLimit) WithLimit(limit *plan.Expr) *MergeLimit {
	mergeLimit.Limit = limit
	return mergeLimit
}

func (mergeLimit *MergeLimit) Release() {
	if mergeLimit != nil {
		reuse.Free[MergeLimit](mergeLimit, nil)
	}
}

func (mergeLimit *MergeLimit) Reset(proc *process.Process, pipelineFailed bool, err error) {
	mergeLimit.Free(proc, pipelineFailed, err)
}

func (mergeLimit *MergeLimit) Free(proc *process.Process, pipelineFailed bool, err error) {
	if mergeLimit.ctr != nil {
		if mergeLimit.ctr.limitExecutor != nil {
			mergeLimit.ctr.limitExecutor.Free()
			mergeLimit.ctr.limitExecutor = nil
		}
		mergeLimit.ctr = nil
	}

}
