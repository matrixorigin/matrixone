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

package dispatch

import (
	"github.com/matrixorigin/matrixone/pkg/container/vector"
	"github.com/matrixorigin/matrixone/pkg/vm/process"
)

type container struct {
	flag []bool
}

type Argument struct {
	ctr  *container
	All  bool // dispatch batch to each consumer
	vecs []*vector.Vector
	Regs []*process.WaitRegister
}

func (arg *Argument) Free(proc *process.Process, pipelineFailed bool) {
	if pipelineFailed {
		for i := range arg.Regs {
			for len(arg.Regs[i].Ch) > 0 {
				bat := <-arg.Regs[i].Ch
				if bat == nil {
					break
				}
				bat.Clean(proc.Mp())
			}
		}
	}

	for i := range arg.Regs {
		select {
		case <-arg.Regs[i].Ctx.Done():
		case arg.Regs[i].Ch <- nil:
		}
		close(arg.Regs[i].Ch)
	}
}
