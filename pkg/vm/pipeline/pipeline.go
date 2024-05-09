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

	"github.com/matrixorigin/matrixone/pkg/container/batch"
	"github.com/matrixorigin/matrixone/pkg/perfcounter"
	"github.com/matrixorigin/matrixone/pkg/sql/colexec/table_scan"
	"github.com/matrixorigin/matrixone/pkg/sql/colexec/value_scan"
	"github.com/matrixorigin/matrixone/pkg/vm"
	"github.com/matrixorigin/matrixone/pkg/vm/engine"
	"github.com/matrixorigin/matrixone/pkg/vm/process"
)

func New(tableID uint64, attrs []string, ins vm.Instructions, reg *process.WaitRegister) *Pipeline {
	return &Pipeline{
		reg:          reg,
		instructions: ins,
		attrs:        attrs,
		tableID:      tableID,
	}
}

func NewMerge(ins vm.Instructions, reg *process.WaitRegister) *Pipeline {
	return &Pipeline{
		reg:          reg,
		instructions: ins,
	}
}

func (p *Pipeline) String() string {
	var buf bytes.Buffer

	vm.String(p.instructions, &buf)
	return buf.String()
}

func (p *Pipeline) Run(r engine.Reader, topValueMsgTag int32, proc *process.Process) (end bool, err error) {
	// performance counter
	perfCounterSet := new(perfcounter.CounterSet)
	proc.Ctx = perfcounter.WithCounterSet(proc.Ctx, perfCounterSet)
	defer func() {
		_ = perfCounterSet //TODO
	}()

	// var bat *batch.Batch
	// used to handle some push-down request
	if p.reg != nil {
		select {
		case <-p.reg.Ctx.Done():
		case <-p.reg.Ch:
		}
	}

	tableScanOperator := table_scan.Argument{
		Reader:         r,
		TopValueMsgTag: topValueMsgTag,
		Attrs:          p.attrs,
		TableID:        p.tableID,
	}
	p.instructions = append([]vm.Instruction{
		{
			Op:      vm.TableScan,
			Idx:     -1,
			Arg:     &tableScanOperator,
			IsFirst: true,
			IsLast:  false,
		},
	}, p.instructions...)

	if err = vm.Prepare(p.instructions, proc); err != nil {
		return false, err
	}
	for {
		end, err = vm.Run(p.instructions, proc)
		if err != nil {
			return end, err
		}
		if end {
			// end is true means pipeline successfully completed
			return end, nil
		}
	}
}

func (p *Pipeline) ConstRun(bat *batch.Batch, proc *process.Process) (end bool, err error) {
	// used to handle some push-down request
	if p.reg != nil {
		select {
		case <-p.reg.Ctx.Done():
		case <-p.reg.Ch:
		}
	}
	pipelineInputBatches := []*batch.Batch{bat, nil}

	for _, ins := range p.instructions {
		ins.Idx += 1
		ins.IsFirst = false
	}

	valueScanOperator := value_scan.Argument{
		Batchs: pipelineInputBatches,
	}
	p.instructions = append([]vm.Instruction{
		{
			Op:      vm.ValueScan,
			Idx:     0,
			Arg:     &valueScanOperator,
			IsFirst: true,
			IsLast:  false,
		},
	}, p.instructions...)

	if err = vm.Prepare(p.instructions, proc); err != nil {
		return false, err
	}

	for {
		end, err = vm.Run(p.instructions, proc)
		if err != nil {
			return end, err
		}
		if end {
			return end, nil
		}
	}
}

func (p *Pipeline) MergeRun(proc *process.Process) (end bool, err error) {
	// used to handle some push-down request
	if p.reg != nil {
		select {
		case <-p.reg.Ctx.Done():
		case <-p.reg.Ch:
		}
	}

	if err = vm.Prepare(p.instructions, proc); err != nil {
		return false, err
	}
	for {
		end, err = vm.Run(p.instructions, proc)
		if err != nil {
			return end, err
		}
		if end {
			return end, nil
		}
	}
}
