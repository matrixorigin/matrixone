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

package apply

import (
	"github.com/matrixorigin/matrixone/pkg/common/mpool"
	"github.com/matrixorigin/matrixone/pkg/common/reuse"
	"github.com/matrixorigin/matrixone/pkg/container/batch"
	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/sql/colexec"
	"github.com/matrixorigin/matrixone/pkg/sql/colexec/table_function"
	"github.com/matrixorigin/matrixone/pkg/vm"
	"github.com/matrixorigin/matrixone/pkg/vm/process"
)

var _ vm.Operator = new(Apply)

const (
	Build = iota
	Probe
	End
)

const (
	CROSS = iota
	OUTER
)

type container struct {
	rbat  *batch.Batch
	inbat *batch.Batch

	batIdx   int
	tfFinish bool
	sels     []int32
}

type Apply struct {
	ctr       container
	ApplyType int
	Result    []colexec.ResultPos
	Typs      []types.Type

	TableFunction *table_function.TableFunction
	vm.OperatorBase
	colexec.Projection
}

func (apply *Apply) GetOperatorBase() *vm.OperatorBase {
	return &apply.OperatorBase
}

func init() {
	reuse.CreatePool[Apply](
		func() *Apply {
			return &Apply{}
		},
		func(a *Apply) {
			*a = Apply{}
		},
		reuse.DefaultOptions[Apply]().
			WithEnableChecker(),
	)
}

func (apply Apply) TypeName() string {
	return opName
}

func NewArgument() *Apply {
	return reuse.Alloc[Apply](nil)
}

func (apply *Apply) Release() {
	if apply != nil {
		if apply.TableFunction != nil {
			reuse.Free[table_function.TableFunction](apply.TableFunction, nil)
		}
		reuse.Free[Apply](apply, nil)
	}
}

func (apply *Apply) Reset(proc *process.Process, pipelineFailed bool, err error) {
	ctr := &apply.ctr

	ctr.inbat = nil
	apply.TableFunction.Reset(proc, pipelineFailed, err)

	if apply.ProjectList != nil {
		apply.OpAnalyzer.Alloc(apply.ProjectAllocSize)
		apply.ResetProjection(proc)
	}

}

func (apply *Apply) Free(proc *process.Process, pipelineFailed bool, err error) {
	ctr := &apply.ctr

	ctr.cleanBatch(proc.Mp())

	apply.TableFunction.Free(proc, pipelineFailed, err)

	if apply.ProjectList != nil {
		apply.FreeProjection(proc)
	}

}

func (ctr *container) cleanBatch(mp *mpool.MPool) {
	if ctr.rbat != nil {
		ctr.rbat.Clean(mp)
		ctr.rbat = nil
	}
}
