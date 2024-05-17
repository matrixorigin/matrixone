// Copyright 2022 Matrix Origin
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

package onduplicatekey

import (
	"github.com/matrixorigin/matrixone/pkg/common/reuse"
	"github.com/matrixorigin/matrixone/pkg/container/batch"
	"github.com/matrixorigin/matrixone/pkg/sql/colexec"
	"github.com/matrixorigin/matrixone/pkg/sql/plan"
	"github.com/matrixorigin/matrixone/pkg/vm"
	"github.com/matrixorigin/matrixone/pkg/vm/engine"
	"github.com/matrixorigin/matrixone/pkg/vm/process"
)

var _ vm.Operator = new(Argument)

const (
	Build = iota
	Eval
	End
)

type container struct {
	colexec.ReceiverOperator

	state            int
	checkConflictBat *batch.Batch // batch to check conflict
	rbat             *batch.Batch
}

type Argument struct {
	Affected uint64
	Engine   engine.Engine

	// Source       engine.Relation
	// UniqueSource []engine.Relation
	// Ref          *plan.ObjectRef
	Attrs              []string
	InsertColCount     int32
	UniqueColCheckExpr []*plan.Expr
	UniqueCols         []string
	OnDuplicateIdx     []int32
	OnDuplicateExpr    map[string]*plan.Expr

	IdxIdx []int32

	ctr      *container
	IsIgnore bool

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

func (arg *Argument) Release() {
	if arg != nil {
		reuse.Free[Argument](arg, nil)
	}
}

func (arg *Argument) Reset(proc *process.Process, pipelineFailed bool, err error) {
	arg.Free(proc, pipelineFailed, err)
}

func (arg *Argument) Free(proc *process.Process, pipelineFailed bool, err error) {
	if arg.ctr != nil {
		arg.ctr.FreeMergeTypeOperator(pipelineFailed)
		if arg.ctr.rbat != nil {
			arg.ctr.rbat.Clean(proc.GetMPool())
		}
		if arg.ctr.checkConflictBat != nil {
			arg.ctr.checkConflictBat.Clean(proc.GetMPool())
		}
		arg.ctr = nil
	}
}
