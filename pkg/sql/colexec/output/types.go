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

package output

import (
	"github.com/matrixorigin/matrixone/pkg/common/reuse"
	"github.com/matrixorigin/matrixone/pkg/container/batch"
	"github.com/matrixorigin/matrixone/pkg/vm"
	"github.com/matrixorigin/matrixone/pkg/vm/process"
)

var _ vm.Operator = new(Output)

type Output struct {
	Data interface{}
	Func func(*batch.Batch) error

	vm.OperatorBase
}

func (output *Output) GetOperatorBase() *vm.OperatorBase {
	return &output.OperatorBase
}

func init() {
	reuse.CreatePool[Output](
		func() *Output {
			return &Output{}
		},
		func(a *Output) {
			*a = Output{}
		},
		reuse.DefaultOptions[Output]().
			WithEnableChecker(),
	)
}

func (output Output) TypeName() string {
	return opName
}

func NewArgument() *Output {
	return reuse.Alloc[Output](nil)
}

func (output *Output) WithData(data interface{}) *Output {
	output.Data = data
	return output
}

func (output *Output) WithFunc(Func func(*batch.Batch) error) *Output {
	output.Func = Func
	return output
}

func (output *Output) Release() {
	if output != nil {
		reuse.Free[Output](output, nil)
	}
}

func (output *Output) Reset(proc *process.Process, pipelineFailed bool, err error) {
	output.Free(proc, pipelineFailed, err)
}

func (output *Output) Free(proc *process.Process, pipelineFailed bool, err error) {
	// if !pipelineFailed {
	// _ = output.Func(nil)
	// }
}
