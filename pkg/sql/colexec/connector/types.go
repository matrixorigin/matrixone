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

package connector

import (
	"fmt"

	"github.com/matrixorigin/matrixone/pkg/vm/process"
)

// Argument pipe connector
type Argument struct {
	Reg *process.WaitRegister
}

func (arg *Argument) Free(proc *process.Process, pipelineFailed bool) {
	fmt.Printf("[colexecConnector] begin connector free. proc = %p, pipelineFailed = %t\n", proc, pipelineFailed)
	if pipelineFailed {
		for len(arg.Reg.Ch) > 0 {
			bat := <-arg.Reg.Ch
			if bat == nil {
				break
			}
			bat.Clean(proc.Mp())
		}
	}

	select {
	case arg.Reg.Ch <- nil:
		fmt.Printf("[colexecConnector] put nil to ch %p. proc = %p\n", &(arg.Reg.Ch), proc)
	case <-arg.Reg.Ctx.Done():
		fmt.Printf("[colexecConnector] ctx done. proc = %p\n", proc)
	}
	close(arg.Reg.Ch)
	fmt.Printf("[colexecConnector] connector close done. proc = %p\n", proc)
}
