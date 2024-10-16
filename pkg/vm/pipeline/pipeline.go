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

package pipeline

import (
	"bytes"
	"context"

	"github.com/matrixorigin/matrixone/pkg/common/moerr"
	"github.com/matrixorigin/matrixone/pkg/logutil"
	"github.com/matrixorigin/matrixone/pkg/sql/colexec/table_scan"
	"github.com/matrixorigin/matrixone/pkg/vm"
	"github.com/matrixorigin/matrixone/pkg/vm/engine"
	"github.com/matrixorigin/matrixone/pkg/vm/process"
)

func New(tableID uint64, attrs []string, op vm.Operator) *Pipeline {
	return &Pipeline{
		rootOp:  op,
		attrs:   attrs,
		tableID: tableID,
	}
}

func NewMerge(op vm.Operator) *Pipeline {
	return &Pipeline{
		rootOp: op,
	}
}

func (p *Pipeline) String() string {
	var buf bytes.Buffer

	vm.String(p.rootOp, &buf)
	return buf.String()
}

func (p *Pipeline) Run(r engine.Reader, topValueMsgTag int32, proc *process.Process) (end bool, err error) {

	if tableScanOperator, ok := vm.GetLeafOp(p.rootOp).(*table_scan.TableScan); ok {
		tableScanOperator.Reader = r
		tableScanOperator.TopValueMsgTag = topValueMsgTag
		tableScanOperator.Attrs = p.attrs
		tableScanOperator.TableID = p.tableID
	}

	return p.run(proc)
}

func (p *Pipeline) ConstRun(proc *process.Process) (end bool, err error) {
	return p.run(proc)
}

func (p *Pipeline) MergeRun(proc *process.Process) (end bool, err error) {
	return p.run(proc)
}

func (p *Pipeline) run(proc *process.Process) (end bool, err error) {
	if err = vm.Prepare(p.rootOp, proc); err != nil {
		return false, err
	}

	defer catchPanic(proc.Ctx, &err)

	vm.ModifyOutputOpNodeIdx(p.rootOp, proc)

	for end := false; !end; {
		result, err := p.rootOp.Call(proc)
		if err != nil {
			return true, err
		}
		end = result.Status == vm.ExecStop || result.Batch == nil
	}

	return true, nil
}

func catchPanic(ctx context.Context, errPtr *error) {
	if e := recover(); e != nil {
		*errPtr = moerr.ConvertPanicError(ctx, e)
		logutil.Errorf("panic in pipeline: %v", *errPtr)
	}
}
