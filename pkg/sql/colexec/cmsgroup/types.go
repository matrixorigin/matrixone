// Copyright 2024 Matrix Origin
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

package cmsgroup

import (
	"github.com/matrixorigin/matrixone/pkg/container/batch"
	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/container/vector"
	"github.com/matrixorigin/matrixone/pkg/pb/plan"
	"github.com/matrixorigin/matrixone/pkg/sql/colexec"
	"github.com/matrixorigin/matrixone/pkg/sql/colexec/aggexec"
	"github.com/matrixorigin/matrixone/pkg/vm"
	"github.com/matrixorigin/matrixone/pkg/vm/process"
)

const (
	H0 = iota
	H8
	HStr
)

type ExprEvalVector struct {
	Executor []colexec.ExpressionExecutor
	Vec      []*vector.Vector
	Typ      []types.Type
}

func MakeEvalVector(proc *process.Process, expressions []*plan.Expr) (ev ExprEvalVector, err error) {
	if len(expressions) == 0 {
		return
	}

	ev.Executor, err = colexec.NewExpressionExecutorsFromPlanExpressions(proc, expressions)
	if err != nil {
		return
	}
	ev.Vec = make([]*vector.Vector, len(ev.Executor))
	ev.Typ = make([]types.Type, len(ev.Executor))
	for i, expr := range expressions {
		ev.Typ[i] = types.New(types.T(expr.Typ.Id), expr.Typ.Width, expr.Typ.Scale)
	}
	return
}

// Group
// the group operator using new implement.
type Group struct {
	vm.OperatorBase
	colexec.Projection

	ctr          container
	NeedEval     bool
	PreAllocSize uint64

	// group-by column.
	Exprs        []*plan.Expr
	GroupingFlag []bool
	// agg info and agg column.
	Aggs []aggexec.AggFuncExecExpression
}

// requireDefaultAggResult
// see the comments for the filed `alreadyOutputAnything` of container.
func (group *Group) requireDefaultAggResult(proc *process.Process) (*batch.Batch, bool, error) {
	if !group.NeedEval {
		return nil, false, nil
	}
	if group.ctr.alreadyOutputAnything || len(group.Exprs) > 0 {
		return nil, false, nil
	}
	group.ctr.result1.cleanLastPopped(proc.Mp())

	aggs, err := group.generateAggExec(proc)
	if err != nil {
		return nil, false, err
	}
	b := batch.NewOffHeapEmpty()
	group.ctr.result1.Popped = b
	b.Aggs = aggs
	b.SetRowCount(1)
	for i := range b.Aggs {
		if err = b.Aggs[i].GroupGrow(1); err != nil {
			return nil, false, err
		}
	}
	for i := range b.Aggs {
		vs, er := b.Aggs[i].Flush()
		if er != nil {
			return nil, false, er
		}
		b.Vecs = append(b.Vecs, vs...)
	}
	return group.ctr.result1.Popped, true, nil
}

func (group *Group) evaluateGroupByAndAgg(proc *process.Process, bat *batch.Batch) error {
	return nil
}

// container
// running context.
type container struct {
	state vm.CtrState

	// hash.
	hr          ResHashRelated
	mtyp        int
	keyWidth    int
	keyNullable bool

	// x, y of `group by x, y`.
	// m, n of `select agg1(m, n), agg2(m, n)`.
	groupByEvaluate   ExprEvalVector
	aggregateEvaluate []ExprEvalVector

	// result if NeedEval is true.
	result1 GroupResultBuffer
	// result if NeedEval is false.
	result2 GroupResultNoneBlock

	// alreadyOutputAnything is a special flag for the case
	// `select agg(x) from data_source` and data_source is empty.
	// we should return 0 for count, and return NULL for the others.
	alreadyOutputAnything bool
}

func (ctr *container) freeAggEvaluate() {
	for i := range ctr.aggregateEvaluate {
		for j := range ctr.aggregateEvaluate[i].Executor {
			ctr.aggregateEvaluate[i].Executor[j].Free()
		}
	}
	ctr.aggregateEvaluate = nil
}

func (ctr *container) freeGroupEvaluate() {
	for i := range ctr.groupByEvaluate.Executor {
		ctr.groupByEvaluate.Executor[i].Free()
	}
	ctr.groupByEvaluate = ExprEvalVector{}
}
