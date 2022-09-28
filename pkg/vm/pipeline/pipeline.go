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
	"github.com/matrixorigin/matrixone/pkg/logutil"
	"github.com/matrixorigin/matrixone/pkg/sql/colexec/dispatch"

	"github.com/matrixorigin/matrixone/pkg/container/batch"
	"github.com/matrixorigin/matrixone/pkg/sql/colexec/connector"
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

	defer cleanup(p, proc)
	if p.reg != nil { // used to handle some push-down request
		select {
		case <-p.reg.Ctx.Done():
		case <-p.reg.Ch:
		}
	}
	if err = vm.Prepare(p.instructions, proc); err != nil {
		return false, err
	}
	for {
		// read data from storage engine
		if bat, err = r.Read(p.attrs, nil, proc.Mp()); err != nil {
			return false, err
		}
		if bat != nil {
			bat.Cnt = 1
		}
		if bat != nil {
			logutil.Infof("read data: bat.Vecs:%v,attrs:%s,zs:%v", bat.Vecs, bat.Attrs, bat.Zs)
		}
		// processing the batch according to the instructions
		proc.Reg.InputBatch = bat
		if end, err = vm.Run(p.instructions, proc); err != nil || end { // end is true means pipeline successfully completed
			return end, err
		}
	}
}

func (p *Pipeline) ConstRun(bat *batch.Batch, proc *process.Process) (bool, error) {
	var end bool // exist flag
	var err error

	defer cleanup(p, proc)
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
	for {
		proc.Reg.InputBatch = bat
		if end, err = vm.Run(p.instructions, proc); err != nil || end {
			return end, err
		}
		proc.Reg.InputBatch = nil
		if end, err = vm.Run(p.instructions, proc); err != nil || end {
			return end, err
		}
	}
}

func (p *Pipeline) MergeRun(proc *process.Process) (bool, error) {
	var end bool
	var err error

	// XXX Here is the big problem.   In side defer, we call cleanup
	// which in turn can calls Run.   Using defer to trigger normal
	// execution flow is simply WRONG.   Calling Run in cleanup, at
	// best is extremely bad naming.
	//
	// I have observed a panic within, calls defer, calls cleanup,
	// calls Run, may create a deadlock.
	//
	// Will try to repro it with fault inj.
	//
	defer func() {
		cleanup(p, proc)
		for i := 0; i < len(proc.Reg.MergeReceivers); i++ { // simulating the end of a pipeline
			for len(proc.Reg.MergeReceivers[i].Ch) > 0 {
				bat := <-proc.Reg.MergeReceivers[i].Ch
				if bat != nil {
					bat.Clean(proc.Mp())
				}
			}
		}
		proc.Cancel()
	}()
	if p.reg != nil { // used to handle some push-down request
		select {
		case <-p.reg.Ctx.Done():
		case <-p.reg.Ch:
		}
	}
	if err := vm.Prepare(p.instructions, proc); err != nil {
		return false, err
	}
	for {
		proc.Reg.InputBatch = nil
		if end, err = vm.Run(p.instructions, proc); err != nil || end {
			return end, err
		}
	}
}

func cleanup(p *Pipeline, proc *process.Process) {
	proc.Reg.InputBatch = nil
	_, _ = vm.Run(p.instructions, proc)
	for i, in := range p.instructions {
		if in.Op == vm.Connector {
			arg := p.instructions[i].Arg.(*connector.Argument)
			if len(arg.Reg.Ch) > 0 {
				break
			}
			select {
			case <-arg.Reg.Ctx.Done():
			case arg.Reg.Ch <- nil:
			}
			break
		}
		if in.Op == vm.Dispatch {
			arg := p.instructions[i].Arg.(*dispatch.Argument)
			for _, reg := range arg.Regs {
				if len(reg.Ch) > 0 {
					break
				}
				select {
				case <-reg.Ctx.Done():
				case reg.Ch <- nil:
				}
			}
			break
		}
	}
}
