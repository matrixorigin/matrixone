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

package cmsMergeGroup

import (
	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/sql/colexec"
	"github.com/matrixorigin/matrixone/pkg/sql/colexec/cmsgroup"
	"github.com/matrixorigin/matrixone/pkg/vm"
	"github.com/matrixorigin/matrixone/pkg/vm/process"
)

type MergeGroup struct {
	vm.OperatorBase
	colexec.Projection

	ctr container

	PartialResults     []any
	PartialResultTypes []types.T
}

type container struct {
	state vm.CtrState

	// hash.
	hr          cmsgroup.ResHashRelated
	mtyp        int
	keyWidth    int
	keyNullable bool

	result cmsgroup.GroupResultBuffer
}

func (mergeGroup *MergeGroup) Reset(proc *process.Process, _ bool, _ error) {
	mergeGroup.Free(proc, false, nil)
}

func (mergeGroup *MergeGroup) Free(proc *process.Process, _ bool, _ error) {
	mergeGroup.ctr.result.Free0(proc.Mp())
	mergeGroup.ctr.hr.Free0()
}
