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
	"github.com/matrixorigin/matrixone/pkg/vm"
	"github.com/matrixorigin/matrixone/pkg/vm/engine"
	"github.com/matrixorigin/matrixone/pkg/vm/process"
)

func New(attrs []string, ins vm.Instructions, reg *process.WaitRegister) *Pipeline {
	return &Pipeline{
		reg:          reg,
		instructions: ins,
		attrs:        attrs,
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

func (p *Pipeline) Run(r engine.Reader, proc *process.Process) (bool, error) {
	var end bool // exist flag
	var err error
	var bat *batch.Batch

	if p.reg != nil { // used to handle some push-down request
		select {
		case <-p.reg.Ctx.Done():
		case <-p.reg.Ch:
		}
	}
	if err = vm.Prepare(p.instructions, proc); err != nil {
		cleanup(p, proc, false)
		return false, err
	}
	for {
		// read data from storage engine
		if bat, err = r.Read(p.attrs, nil, proc.Mp()); err != nil {
			cleanup(p, proc, false)
			return false, err
		}
		if bat != nil {
			bat.Cnt = 1
		}
		// processing the batch according to the instructions
		proc.Reg.InputBatch = bat
		end, err = vm.Run(p.instructions, proc)
		if err != nil {
			cleanup(p, proc, false)
			return end, err
		}
		if end {
			cleanup(p, proc, true)
			return end, nil
		}
	}
}

func (p *Pipeline) ConstRun(bat *batch.Batch, proc *process.Process) (bool, error) {
	var end bool // exist flag
	var err error

	if p.reg != nil { // used to handle some push-down request
		select {
		case <-p.reg.Ctx.Done():
		case <-p.reg.Ch:
		}
	}
	if err = vm.Prepare(p.instructions, proc); err != nil {
		return false, err
	}
	bat.Cnt = 1
	// processing the batch according to the instructions
	pipelineInputs := []*batch.Batch{bat, nil}
	for {
		for i := range pipelineInputs {
			proc.Reg.InputBatch = pipelineInputs[i]
			end, err = vm.Run(p.instructions, proc)
			if err != nil {
				cleanup(p, proc, false)
				return end, err
			}
			if end {
				cleanup(p, proc, true)
				return end, nil
			}
		}
	}
}

func (p *Pipeline) MergeRun(proc *process.Process) (bool, error) {
	var end bool
	var err error

	defer proc.Cancel()
	if p.reg != nil { // used to handle some push-down request
		select {
		case <-p.reg.Ctx.Done():
		case <-p.reg.Ch:
		}
	}
	if err := vm.Prepare(p.instructions, proc); err != nil {
		cleanup(p, proc, false)
		return false, err
	}
	for {
		proc.Reg.InputBatch = nil
		end, err = vm.Run(p.instructions, proc)
		if err != nil {
			cleanup(p, proc, false)
			return end, err
		}
		if end {
			cleanup(p, proc, true)
			return end, nil
		}
	}
}

// cleanup do memory release work for a whole pipeline.
// clean the coming batches and template space of each pipeline operator.
func cleanup(p *Pipeline, proc *process.Process, pipelineSucceed bool) {
	// clean all the coming batches.
	if !pipelineSucceed {
		bat := proc.InputBatch()
		if bat != nil {
			bat.Clean(proc.Mp())
		}
		proc.SetInputBatch(nil)
	}
	for i := range proc.Reg.MergeReceivers {
		for len(proc.Reg.MergeReceivers[i].Ch) > 0 {
			bat := <-proc.Reg.MergeReceivers[i].Ch
			if bat == nil {
				break
			}
			bat.Clean(proc.Mp())
		}
	}

	// clean operator space.
	for i := range p.instructions {
		p.instructions[i].Arg.Free(proc, !pipelineSucceed)
	}
}
