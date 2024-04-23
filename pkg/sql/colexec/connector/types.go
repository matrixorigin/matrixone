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
	"github.com/matrixorigin/matrixone/pkg/vm"
	"github.com/matrixorigin/matrixone/pkg/vm/process"
)

var _ vm.Operator = new(Argument)

// Argument pipe connector
type Argument struct {
	Reg      *process.WaitRegister
	info     *vm.OperatorInfo
	Children []vm.Operator
}

func (arg *Argument) SetInfo(info *vm.OperatorInfo) {
	arg.info = info
}

func (arg *Argument) AppendChild(child vm.Operator) {
	arg.Children = append(arg.Children, child)
}

func (arg *Argument) Free(proc *process.Process, pipelineFailed bool, err error) {
	// told the next operator to stop if it is still running.
	select {
	case arg.Reg.Ch <- nil:
	case <-arg.Reg.Ctx.Done():
	}
}
