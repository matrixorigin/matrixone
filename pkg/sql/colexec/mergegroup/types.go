// Copyright 2024 Matrix Origin
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

package mergegroup

import (
	"github.com/matrixorigin/matrixone/pkg/common/reuse"
	"github.com/matrixorigin/matrixone/pkg/container/batch"
	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/sql/colexec"
	"github.com/matrixorigin/matrixone/pkg/sql/colexec/group"
	"github.com/matrixorigin/matrixone/pkg/vm"
	"github.com/matrixorigin/matrixone/pkg/vm/process"
)

const (
	thisOperatorName = "merge_group"
)

type MergeGroup struct {
	vm.OperatorBase
	colexec.Projection

	ctr container

	PartialResults     []any
	PartialResultTypes []types.T
}

func (mergeGroup *MergeGroup) ExecProjection(proc *process.Process, input *batch.Batch) (*batch.Batch, error) {
	if mergeGroup.ProjectList == nil {
		return input, nil
	}
	return mergeGroup.EvalProjection(input, proc)
}

type container struct {
	state vm.CtrState

	// hash.
	hr group.ResHashRelated
	// res.
	result group.GroupResultBuffer
}

func (mergeGroup *MergeGroup) Reset(proc *process.Process, _ bool, _ error) {
	mergeGroup.Free(proc, false, nil)
	mergeGroup.ResetProjection(proc)
}

func (mergeGroup *MergeGroup) Free(proc *process.Process, _ bool, _ error) {
	mergeGroup.ctr.result.Free0(proc.Mp())
	mergeGroup.ctr.hr.Free0()
	mergeGroup.FreeProjection(proc)
}

func (mergeGroup *MergeGroup) GetOperatorBase() *vm.OperatorBase {
	return &mergeGroup.OperatorBase
}

func (mergeGroup *MergeGroup) OpType() vm.OpType {
	return vm.MergeGroup
}

func (mergeGroup MergeGroup) TypeName() string {
	return thisOperatorName
}

func init() {
	reuse.CreatePool[MergeGroup](
		func() *MergeGroup {
			return &MergeGroup{}
		},
		func(a *MergeGroup) {
			*a = MergeGroup{}
		},
		reuse.DefaultOptions[MergeGroup]().
			WithEnableChecker(),
	)
}

func NewArgument() *MergeGroup {
	return reuse.Alloc[MergeGroup](nil)
}

func (mergeGroup *MergeGroup) Release() {
	if mergeGroup != nil {
		reuse.Free[MergeGroup](mergeGroup, nil)
	}
}
