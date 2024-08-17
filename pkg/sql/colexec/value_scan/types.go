// Copyright 2021-2023 Matrix Origin
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

package value_scan

import (
	"github.com/matrixorigin/matrixone/pkg/common/reuse"
	"github.com/matrixorigin/matrixone/pkg/container/batch"
	"github.com/matrixorigin/matrixone/pkg/sql/colexec"
	"github.com/matrixorigin/matrixone/pkg/vm"
	"github.com/matrixorigin/matrixone/pkg/vm/process"
)

var _ vm.Operator = new(ValueScan)

type container struct {
	idx int
}
type ValueScan struct {
	ctr    container
	Batchs []*batch.Batch

	vm.OperatorBase
	colexec.Projection
}

func (valueScan *ValueScan) GetOperatorBase() *vm.OperatorBase {
	return &valueScan.OperatorBase
}

func init() {
	reuse.CreatePool[ValueScan](
		func() *ValueScan {
			return &ValueScan{}
		},
		func(a *ValueScan) {
			*a = ValueScan{}
		},
		reuse.DefaultOptions[ValueScan]().
			WithEnableChecker(),
	)
}

func (valueScan ValueScan) TypeName() string {
	return opName
}

func NewArgument() *ValueScan {
	return reuse.Alloc[ValueScan](nil)
}

func (valueScan *ValueScan) Release() {
	if valueScan != nil {
		reuse.Free[ValueScan](valueScan, nil)
	}
}

func (valueScan *ValueScan) Reset(proc *process.Process, pipelineFailed bool, err error) {
	//@todo need move make batchs function from Scope.Run to value_scan.Prepare, then batchs will not be cleaned here
	valueScan.cleanBatchs(proc)
	valueScan.ctr.idx = 0
	valueScan.ResetProjection(proc)
}

func (valueScan *ValueScan) Free(proc *process.Process, pipelineFailed bool, err error) {
	valueScan.FreeProjection(proc)
}

func (valueScan *ValueScan) cleanBatchs(proc *process.Process) {
	for _, bat := range valueScan.Batchs {
		if bat != nil {
			bat.Clean(proc.Mp())
		}
	}
	valueScan.Batchs = nil
}
