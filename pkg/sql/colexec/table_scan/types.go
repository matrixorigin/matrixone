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

package table_scan

import (
	"github.com/matrixorigin/matrixone/pkg/common/reuse"
	"github.com/matrixorigin/matrixone/pkg/container/batch"
	"github.com/matrixorigin/matrixone/pkg/vm"
	"github.com/matrixorigin/matrixone/pkg/vm/engine"
	"github.com/matrixorigin/matrixone/pkg/vm/message"
	"github.com/matrixorigin/matrixone/pkg/vm/process"
)

var _ vm.Operator = new(TableScan)

type container struct {
	maxAllocSize int
	buf          *batch.Batch
	msgReceiver  *message.MessageReceiver
}
type TableScan struct {
	ctr            *container
	TopValueMsgTag int32
	Reader         engine.Reader
	// letter case: origin
	Attrs   []string
	TableID uint64

	vm.OperatorBase
}

func (tableScan *TableScan) GetOperatorBase() *vm.OperatorBase {
	return &tableScan.OperatorBase
}

func init() {
	reuse.CreatePool[TableScan](
		func() *TableScan {
			return &TableScan{}
		},
		func(a *TableScan) {
			*a = TableScan{}
		},
		reuse.DefaultOptions[TableScan]().
			WithEnableChecker(),
	)
}

func (tableScan TableScan) TypeName() string {
	return opName
}

func NewArgument() *TableScan {
	return reuse.Alloc[TableScan](nil)
}

func (tableScan *TableScan) Release() {
	if tableScan != nil {
		reuse.Free[TableScan](tableScan, nil)
	}
}

func (tableScan *TableScan) Reset(proc *process.Process, pipelineFailed bool, err error) {
	tableScan.Free(proc, pipelineFailed, err)
}

func (tableScan *TableScan) Free(proc *process.Process, pipelineFailed bool, err error) {
	if tableScan.ctr != nil {
		if tableScan.ctr.buf != nil {
			tableScan.ctr.buf.Clean(proc.Mp())
			tableScan.ctr.buf = nil
		}
		//anal := proc.GetAnalyze(tableScan.GetIdx(), tableScan.GetParallelIdx(), tableScan.GetParallelMajor())
		//anal.Alloc(int64(tableScan.ctr.maxAllocSize))
		if tableScan.OpAnalyzer != nil {
			tableScan.OpAnalyzer.Alloc(int64(tableScan.ctr.maxAllocSize))
		}

		if tableScan.ctr.msgReceiver != nil {
			tableScan.ctr.msgReceiver.Free()
			tableScan.ctr.msgReceiver = nil
		}
		tableScan.ctr = nil
	}
}
