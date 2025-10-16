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

package group

import (
	"github.com/matrixorigin/matrixone/pkg/common/mpool"
	"github.com/matrixorigin/matrixone/pkg/common/reuse"
	"github.com/matrixorigin/matrixone/pkg/container/batch"
	"github.com/matrixorigin/matrixone/pkg/container/vector"
	"github.com/matrixorigin/matrixone/pkg/pb/plan"
	"github.com/matrixorigin/matrixone/pkg/sql/colexec"
	"github.com/matrixorigin/matrixone/pkg/sql/colexec/aggexec"
	"github.com/matrixorigin/matrixone/pkg/vm"
	"github.com/matrixorigin/matrixone/pkg/vm/process"
)

var _ vm.Operator = &GroupOld{}

// Group
// the group operator using new implement.
type GroupOld struct {
	vm.OperatorBase
	colexec.Projection

	ctr          containerOld
	NeedEval     bool
	PreAllocSize uint64
	SpillMem     int64

	// group-by column.
	Exprs        []*plan.Expr
	GroupingFlag []bool
	// agg info and agg column.
	Aggs []aggexec.AggFuncExecExpression
}

func (group *GroupOld) evaluateGroupByAndAgg(proc *process.Process, bat *batch.Batch) (err error) {
	input := []*batch.Batch{bat}

	// group.
	for i := range group.ctr.groupByEvaluate.Vec {
		if group.ctr.groupByEvaluate.Vec[i], err = group.ctr.groupByEvaluate.Executor[i].Eval(proc, input, nil); err != nil {
			return err
		}
	}

	// agg.
	for i := range group.ctr.aggregateEvaluate {
		for j := range group.ctr.aggregateEvaluate[i].Vec {
			if group.ctr.aggregateEvaluate[i].Vec[j], err = group.ctr.aggregateEvaluate[i].Executor[j].Eval(proc, input, nil); err != nil {
				return err
			}
		}
	}

	// grouping flag.
	for i, flag := range group.GroupingFlag {
		if !flag {
			group.ctr.groupByEvaluate.Vec[i] = vector.NewRollupConst(group.ctr.groupByEvaluate.Typ[i], group.ctr.groupByEvaluate.Vec[i].Length(), proc.Mp())
		}
	}

	return nil
}

func (group *GroupOld) AnyDistinctAgg() bool {
	for _, agg := range group.Aggs {
		if agg.IsDistinct() {
			return true
		}
	}
	return false
}

func (group *GroupOld) ExecProjection(proc *process.Process, input *batch.Batch) (*batch.Batch, error) {
	if group.ProjectList == nil {
		return input, nil
	}
	return group.EvalProjection(input, proc)
}

// container
// running context.
type containerOld struct {
	state             vm.CtrState
	dataSourceIsEmpty bool

	// hash.
	hr          ResHashRelated
	mtyp        int
	keyWidth    int
	keyNullable bool

	// x, y of `group by x, y`.
	// m, n of `select agg1(m, n), agg2(m, n)`.
	groupByEvaluate   colexec.ExprEvalVector
	aggregateEvaluate []colexec.ExprEvalVector

	// result if NeedEval is true.
	result1 GroupResultBuffer
	// result if NeedEval is false.
	result2 GroupResultNoneBlock
}

func (ctr *containerOld) isDataSourceEmpty() bool {
	return ctr.dataSourceIsEmpty
}

func (group *GroupOld) Free(proc *process.Process, _ bool, _ error) {
	group.freeCannotReuse(proc.Mp())

	group.ctr.freeGroupEvaluate()
	group.ctr.freeAggEvaluate()
	group.FreeProjection(proc)
}

func (group *GroupOld) Reset(proc *process.Process, pipelineFailed bool, err error) {
	group.freeCannotReuse(proc.Mp())

	group.ctr.groupByEvaluate.ResetForNextQuery()
	for i := range group.ctr.aggregateEvaluate {
		group.ctr.aggregateEvaluate[i].ResetForNextQuery()
	}
	group.ResetProjection(proc)
}

func (group *GroupOld) freeCannotReuse(mp *mpool.MPool) {
	group.ctr.hr.Free0()
	group.ctr.result1.Free0(mp)
	group.ctr.result2.Free0(mp)
}

func (ctr *containerOld) freeAggEvaluate() {
	for i := range ctr.aggregateEvaluate {
		ctr.aggregateEvaluate[i].Free()
	}
	ctr.aggregateEvaluate = nil
}

func (ctr *containerOld) freeGroupEvaluate() {
	ctr.groupByEvaluate.Free()
	ctr.groupByEvaluate = colexec.ExprEvalVector{}
}

func (group *GroupOld) OpType() vm.OpType {
	return vm.Group
}

func (group GroupOld) TypeName() string {
	return thisOperatorName
}

func (group *GroupOld) GetOperatorBase() *vm.OperatorBase {
	return &group.OperatorBase
}

func init() {
	reuse.CreatePool[GroupOld](
		func() *GroupOld {
			return &GroupOld{}
		},
		func(a *GroupOld) {
			*a = GroupOld{}
		},
		reuse.DefaultOptions[GroupOld]().
			WithEnableChecker(),
	)
}

func NewArgumentOld() *GroupOld {
	return reuse.Alloc[GroupOld](nil)
}

func (group *GroupOld) Release() {
	if group != nil {
		reuse.Free[GroupOld](group, nil)
	}
}
