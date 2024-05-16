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

package limit

import (
	"github.com/matrixorigin/matrixone/pkg/common/reuse"
	"github.com/matrixorigin/matrixone/pkg/sql/colexec"
	"github.com/matrixorigin/matrixone/pkg/sql/plan"
	"github.com/matrixorigin/matrixone/pkg/vm"
	"github.com/matrixorigin/matrixone/pkg/vm/process"
)

var _ vm.Operator = new(Argument)

type Argument struct {
	Seen          uint64 // seen is the number of tuples seen so far
	LimitExpr     *plan.Expr
	limit         uint64
	limitExecutor colexec.ExpressionExecutor
	vm.OperatorBase
}

func (arg *Argument) GetOperatorBase() *vm.OperatorBase {
	return &arg.OperatorBase
}

func init() {
	reuse.CreatePool[Argument](
		func() *Argument {
			return &Argument{}
		},
		func(a *Argument) {
			*a = Argument{}
		},
		reuse.DefaultOptions[Argument]().
			WithEnableChecker(),
	)
}

func (arg Argument) TypeName() string {
	return argName
}

func NewArgument() *Argument {
	return reuse.Alloc[Argument](nil)
}

func (arg *Argument) WithLimit(limit *plan.Expr) *Argument {
	arg.LimitExpr = limit
	return arg
}

func (arg *Argument) Release() {
	if arg != nil {
		reuse.Free[Argument](arg, nil)
	}
}

func (arg *Argument) Reset(proc *process.Process, pipelineFailed bool, err error) {
	arg.Free(proc, pipelineFailed, err)
}

func (arg *Argument) Free(proc *process.Process, pipelineFailed bool, err error) {
	if arg.limitExecutor != nil {
		arg.limitExecutor.Free()
		arg.limitExecutor = nil
	}
}
