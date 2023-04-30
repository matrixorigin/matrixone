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

func (p *Pipeline) Run(r engine.Reader, proc *process.Process) (end bool, err error) {
	// performance counter
	perfCounterSet := new(perfcounter.CounterSet)
	proc.Ctx = perfcounter.WithCounterSet(proc.Ctx, perfCounterSet)
	defer func() {
		_ = perfCounterSet //TODO
	}()

	var bat *batch.Batch
	// used to handle some push-down request
	if p.reg != nil {
		select {
		case <-p.reg.Ctx.Done():
		case <-p.reg.Ch:
		}
	}
	if err = vm.Prepare(p.instructions, proc); err != nil {
		p.cleanup(proc, true)
		return false, err
	}

	for {
		select {
		case <-proc.Ctx.Done():
			proc.SetInputBatch(nil)
			p.cleanup(proc, false)
			return true, nil
		default:
		}
		// read data from storage engine
		if bat, err = r.Read(proc.Ctx, p.attrs, nil, proc.Mp(), proc); err != nil {
			p.cleanup(proc, true)
			return false, err
		}
		if bat != nil {
			bat.Cnt = 1

			analyzeIdx := p.instructions[0].Idx
			a := proc.GetAnalyze(analyzeIdx)
			a.S3IOByte(bat)
			a.Alloc(int64(bat.Size()))
		}

		proc.SetInputBatch(bat)
		end, err = vm.Run(p.instructions, proc)
		if err != nil {
			p.cleanup(proc, true)
			return end, err
		}
		if end {
			// end is true means pipeline successfully completed
			p.cleanup(proc, false)
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

	if err = vm.Prepare(p.instructions, proc); err != nil {
		p.cleanup(proc, true)
		return false, err
	}
	pipelineInputBatches := []*batch.Batch{bat, nil}
	for {
		for i := range pipelineInputBatches {
			proc.SetInputBatch(pipelineInputBatches[i])
			end, err = vm.Run(p.instructions, proc)
			if err != nil {
				p.cleanup(proc, true)
				return end, err
			}
			if end {
				p.cleanup(proc, false)
				return end, nil
			}
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
		proc.Cancel()
		p.cleanup(proc, true)
		return false, err
	}
	for {
		end, err = vm.Run(p.instructions, proc)
		if err != nil {
			proc.Cancel()
			p.cleanup(proc, true)
			return end, err
		}
		if end {
			proc.Cancel()
			p.cleanup(proc, false)
			return end, nil
		}
	}
}
