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
	"github.com/matrixorigin/matrixone/pkg/vm/process"
)

var _ vm.Operator = new(OnDuplicatekey)

const (
	Build = iota
	Eval
	End
)

type container struct {
	state            int
	checkConflictBat *batch.Batch // batch to check conflict
	rbat             *batch.Batch // return batch
	uniqueCheckExes  []colexec.ExpressionExecutor
}

type OnDuplicatekey struct {
	Affected uint64

	// Source       engine.Relation
	// UniqueSource []engine.Relation
	// Ref          *plan.ObjectRef

	// letter case: origin
	Attrs              []string
	InsertColCount     int32
	UniqueColCheckExpr []*plan.Expr
	UniqueCols         []string
	OnDuplicateIdx     []int32
	OnDuplicateExpr    map[string]*plan.Expr

	ctr      container
	IsIgnore bool

	vm.OperatorBase
}

func (onDuplicatekey *OnDuplicatekey) GetOperatorBase() *vm.OperatorBase {
	return &onDuplicatekey.OperatorBase
}

func init() {
	reuse.CreatePool[OnDuplicatekey](
		func() *OnDuplicatekey {
			return &OnDuplicatekey{}
		},
		func(a *OnDuplicatekey) {
			*a = OnDuplicatekey{}
		},
		reuse.DefaultOptions[OnDuplicatekey]().
			WithEnableChecker(),
	)
}

func (onDuplicatekey OnDuplicatekey) TypeName() string {
	return opName
}

func NewArgument() *OnDuplicatekey {
	return reuse.Alloc[OnDuplicatekey](nil)
}

func (onDuplicatekey *OnDuplicatekey) Release() {
	if onDuplicatekey != nil {
		reuse.Free[OnDuplicatekey](onDuplicatekey, nil)
	}
}

func (onDuplicatekey *OnDuplicatekey) Reset(proc *process.Process, pipelineFailed bool, err error) {
	if onDuplicatekey.ctr.rbat != nil {
		onDuplicatekey.ctr.rbat.CleanOnlyData()
	}
	if onDuplicatekey.ctr.checkConflictBat != nil {
		onDuplicatekey.ctr.checkConflictBat.CleanOnlyData()
	}
	for _, exe := range onDuplicatekey.ctr.uniqueCheckExes {
		if exe != nil {
			exe.ResetForNextQuery()
		}
	}
}

func (onDuplicatekey *OnDuplicatekey) Free(proc *process.Process, pipelineFailed bool, err error) {
	if onDuplicatekey.ctr.rbat != nil {
		onDuplicatekey.ctr.rbat.Clean(proc.GetMPool())
	}
	if onDuplicatekey.ctr.checkConflictBat != nil {
		onDuplicatekey.ctr.checkConflictBat.Clean(proc.GetMPool())
	}
	for _, exe := range onDuplicatekey.ctr.uniqueCheckExes {
		if exe != nil {
			exe.Free()
		}
	}
}
