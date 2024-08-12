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

package group

import (
	"github.com/matrixorigin/matrixone/pkg/common/hashmap"
	"github.com/matrixorigin/matrixone/pkg/common/mpool"
	"github.com/matrixorigin/matrixone/pkg/common/reuse"
	"github.com/matrixorigin/matrixone/pkg/container/batch"
	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/container/vector"
	"github.com/matrixorigin/matrixone/pkg/pb/plan"
	"github.com/matrixorigin/matrixone/pkg/sql/colexec"
	"github.com/matrixorigin/matrixone/pkg/sql/colexec/aggexec"
	"github.com/matrixorigin/matrixone/pkg/vm"
	"github.com/matrixorigin/matrixone/pkg/vm/process"
)

var _ vm.Operator = new(Group)

const (
	H8 = iota
	HStr
	HIndex
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

func (ev *ExprEvalVector) Free() {
	for i := range ev.Executor {
		if ev.Executor[i] == nil {
			continue
		}
		ev.Executor[i].Free()
		ev.Executor[i] = nil
	}
}

func (ev *ExprEvalVector) ResetForNextQuery() {
	for i := range ev.Executor {
		if ev.Executor[i] != nil {
			ev.Executor[i].ResetForNextQuery()
		}
	}
}

type container struct {
	typ       int
	state     vm.CtrState
	inserted  []uint8
	zInserted []uint8

	intHashMap *hashmap.IntHashMap
	strHashMap *hashmap.StrHashMap
	// idx        *index.LowCardinalityIndex

	aggVecs   []ExprEvalVector
	groupVecs ExprEvalVector

	// keyWidth is the width of group by columns, it determines which hash map to use.
	keyWidth          int
	groupVecsNullable bool

	bat *batch.Batch
}

type Group struct {
	ctr          *container
	IsShuffle    bool // is shuffle group
	NeedEval     bool // need to projection the aggregate column
	PreAllocSize uint64

	Exprs []*plan.Expr // group Expressions
	Types []types.Type
	Aggs  []aggexec.AggFuncExecExpression

	vm.OperatorBase
	colexec.Projection
}

func (group *Group) GetOperatorBase() *vm.OperatorBase {
	return &group.OperatorBase
}

func (group *Group) AnyDistinctAgg() bool {
	for _, agg := range group.Aggs {
		if agg.IsDistinct() {
			return true
		}
	}
	return false
}

func init() {
	reuse.CreatePool[Group](
		func() *Group {
			return &Group{}
		},
		func(a *Group) {
			*a = Group{}
		},
		reuse.DefaultOptions[Group]().
			WithEnableChecker(),
	)
}

func (group Group) TypeName() string {
	return opName
}

func NewArgument() *Group {
	return reuse.Alloc[Group](nil)
}

func (group *Group) WithExprs(exprs []*plan.Expr) *Group {
	group.Exprs = exprs
	return group
}

func (group *Group) WithTypes(types []types.Type) *Group {
	group.Types = types
	return group
}

func (group *Group) WithAggsNew(aggs []aggexec.AggFuncExecExpression) *Group {
	group.Aggs = aggs
	return group
}

func (group *Group) Release() {
	if group != nil {
		reuse.Free[Group](group, nil)
	}
}

func (group *Group) Reset(proc *process.Process, pipelineFailed bool, err error) {
	group.Free(proc, pipelineFailed, err)
}

func (group *Group) Free(proc *process.Process, pipelineFailed bool, err error) {
	ctr := group.ctr
	if ctr != nil {
		mp := proc.Mp()
		ctr.cleanBatch(mp)
		ctr.cleanHashMap()
		ctr.cleanAggVectors()
		ctr.cleanGroupVectors()
		group.ctr = nil
	}
	if group.ProjectList != nil {
		anal := proc.GetAnalyze(group.GetIdx(), group.GetParallelIdx(), group.GetParallelMajor())
		anal.Alloc(group.ProjectAllocSize)
		group.FreeProjection(proc)
	}
}

func (ctr *container) cleanBatch(mp *mpool.MPool) {
	if ctr.bat != nil {
		ctr.bat.Clean(mp)
		ctr.bat = nil
	}
}

func (ctr *container) cleanAggVectors() {
	for i := range ctr.aggVecs {
		ctr.aggVecs[i].Free()
	}
	ctr.aggVecs = nil
}

func (ctr *container) cleanGroupVectors() {
	ctr.groupVecs.Free()
}

func (ctr *container) cleanHashMap() {
	if ctr.intHashMap != nil {
		ctr.intHashMap.Free()
		ctr.intHashMap = nil
	}
	if ctr.strHashMap != nil {
		ctr.strHashMap.Free()
		ctr.strHashMap = nil
	}
}
