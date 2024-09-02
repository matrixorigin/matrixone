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
	plan2 "github.com/matrixorigin/matrixone/pkg/pb/plan"
	"github.com/matrixorigin/matrixone/pkg/sql/colexec"
	"github.com/matrixorigin/matrixone/pkg/sql/plan"
	"github.com/matrixorigin/matrixone/pkg/vm"
	"github.com/matrixorigin/matrixone/pkg/vm/process"
)

var _ vm.Operator = new(ValueScan)

type container struct {
	idx int
	bat *batch.Batch
}
type ValueScan struct {
	ctr        container
	RowsetData *plan.RowsetData
	ColCount   int
	Uuid       []byte
	NodeType   plan2.Node_NodeType

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
	valueScan.ctr.idx = 0
	if valueScan.RowsetData == nil {
		if valueScan.ctr.bat != nil {
			valueScan.ctr.bat.Clean(proc.GetMPool())
			valueScan.ctr.bat = nil
		}
	}
	valueScan.ResetProjection(proc)
}

func (valueScan *ValueScan) Free(proc *process.Process, pipelineFailed bool, err error) {
	valueScan.FreeProjection(proc)
}

func (valueScan *ValueScan) SetValueScanBatch(bat *batch.Batch) {
	valueScan.ctr.bat = bat
}

func (valueScan *ValueScan) ResetBatchIdx() {
	valueScan.ctr.idx = 0
}
