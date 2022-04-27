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
	"github.com/matrixorigin/matrixone/pkg/sql/colexec/connector"
	"github.com/matrixorigin/matrixone/pkg/vm"
	"github.com/matrixorigin/matrixone/pkg/vm/engine"
	"github.com/matrixorigin/matrixone/pkg/vm/process"
)

func New(cs []uint64, attrs []string, ins vm.Instructions) *Pipeline {
	return &Pipeline{
		refCnts:      cs,
		instructions: ins,
		attrs:        attrs,
	}
}

func NewMerge(ins vm.Instructions) *Pipeline {
	return &Pipeline{
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

	defer func() {
		if err != nil {
			for i, in := range p.instructions {
				if in.Op == vm.Connector {
					arg := p.instructions[i].Arg.(*connector.Argument)
					arg.Reg.Ch <- nil
					break
				}
			}
		} else {
			proc.Reg.InputBatch = nil
			vm.Run(p.instructions, proc)
		}
	}()
	if err = vm.Prepare(p.instructions, proc); err != nil {
		return false, err
	}
	for {
		// read data from storage engine
		if bat, err = r.Read(p.refCnts, p.attrs); err != nil {
			return false, err
		}
		// processing the batch according to the instructions
		proc.Reg.InputBatch = bat
		if end, err = vm.Run(p.instructions, proc); err != nil || end { // end is true means pipeline successfully completed
			return end, err
		}
	}
}

func (p *Pipeline) RunMerge(proc *process.Process) (bool, error) {
	var end bool
	var err error

	defer func() {
		if err != nil {
			for i, in := range p.instructions {
				if in.Op == vm.Connector {
					arg := p.instructions[i].Arg.(*connector.Argument)
					arg.Reg.Ch <- nil
					break
				}
			}
		} else {
			proc.Reg.InputBatch = nil
			vm.Run(p.instructions, proc)
		}
		proc.Cancel()
	}()
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
