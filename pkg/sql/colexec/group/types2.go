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
	"bytes"
	"fmt"
	"os"

	"github.com/matrixorigin/matrixone/pkg/common/moerr"
	"github.com/matrixorigin/matrixone/pkg/common/reuse"
	"github.com/matrixorigin/matrixone/pkg/container/batch"
	"github.com/matrixorigin/matrixone/pkg/container/vector"
	"github.com/matrixorigin/matrixone/pkg/pb/plan"
	"github.com/matrixorigin/matrixone/pkg/sql/colexec"
	"github.com/matrixorigin/matrixone/pkg/sql/colexec/aggexec"
	"github.com/matrixorigin/matrixone/pkg/sql/plan/function"
	"github.com/matrixorigin/matrixone/pkg/vm"
	"github.com/matrixorigin/matrixone/pkg/vm/process"
)

const (
	H0 = iota
	H8
	HStr
)

const (
	thisOperatorName = "group"
)

var _ vm.Operator = &Group{}

// Group
// the group operator using new implement.
type Group struct {
	vm.OperatorBase
	colexec.Projection

	ctr      container
	NeedEval bool
	SpillMem int64

	// group-by column.
	Exprs        []*plan.Expr
	GroupingFlag []bool
	// agg info and agg column.
	Aggs []aggexec.AggFuncExecExpression

	// XXX To remove.  This is not used, but keep it for compatibility.
	PreAllocSize uint64
}

type spillBucket struct {
	lv      int           // spill level
	reading int           // current reading bucket index
	again   []spillBucket // spill buckets
	gbBatch *batch.Batch  // group by batch
	file    *os.File      // spill file
}

// container running context.
type container struct {
	state        vm.CtrState
	inputDone    bool
	currBatchIdx int

	// hash.
	hr          ResHashRelated
	mtyp        int
	keyWidth    int
	keyNullable bool

	// x, y of `group by x, y`.
	groupByEvaluate colexec.ExprEvalVector
	// m, n of `select agg1(m, n), agg2(m, n)`.
	aggArgEvaluate []colexec.ExprEvalVector

	// group by columns
	groupByBatches []*batch.Batch

	// aggs, which holds the intermediate state of agg functions.
	aggList []aggexec.AggFuncExec
	flushed [][]*vector.Vector

	// spill
	spillBkt spillBucket
}

func (group *Group) evaluateGroupByAndAggArgs(proc *process.Process, bat *batch.Batch) (err error) {
	input := []*batch.Batch{bat}

	// FUBAR: check if the grouping flag length is too big,
	if len(group.ctr.groupByEvaluate.Vec) >= len(group.GroupingFlag) {
		return moerr.NewInternalErrorNoCtx("grouping flag length too big")
	}

	// group.
	for i := range group.ctr.groupByEvaluate.Vec {
		if i < len(group.GroupingFlag) && !group.GroupingFlag[i] {
			group.ctr.groupByEvaluate.Vec[i] = vector.NewRollupConst(group.ctr.groupByEvaluate.Typ[i], bat.RowCount(), proc.Mp())
			continue
		}

		if group.ctr.groupByEvaluate.Vec[i], err = group.ctr.groupByEvaluate.Executor[i].Eval(proc, input, nil); err != nil {
			return err
		}
	}

	// agg args.
	for i := range group.ctr.aggArgEvaluate {
		for j := range group.ctr.aggArgEvaluate[i].Vec {
			if group.ctr.aggArgEvaluate[i].Vec[j], err = group.ctr.aggArgEvaluate[i].Executor[j].Eval(proc, input, nil); err != nil {
				return err
			}
		}
	}

	return nil
}

func (group *Group) AnyDistinctAgg() bool {
	for _, agg := range group.Aggs {
		if agg.IsDistinct() {
			return true
		}
	}
	return false
}

func (group *Group) ExecProjection(proc *process.Process, input *batch.Batch) (*batch.Batch, error) {
	if group.ProjectList == nil {
		return input, nil
	}
	return group.EvalProjection(input, proc)
}

func (group *Group) Free(proc *process.Process, _ bool, _ error) {
	group.ctr.free(proc)
	// free projection stuff,
	group.FreeProjection(proc)
}

func (group *Group) Reset(proc *process.Process, pipelineFailed bool, err error) {
	group.ctr.reset(proc)
	group.ResetProjection(proc)
}

func (ctr *container) freeAggList(proc *process.Process) {
	for i := range ctr.aggList {
		if ctr.aggList[i] != nil {
			ctr.aggList[i].Free()
			ctr.aggList[i] = nil
		}
	}
	ctr.aggList = nil
}

func (ctr *container) free(proc *process.Process) {
	// free container stuff, WTH is the Free0?
	ctr.hr.Free0()

	ctr.groupByEvaluate.Free()

	for i := range ctr.aggArgEvaluate {
		ctr.aggArgEvaluate[i].Free()
	}
	ctr.aggArgEvaluate = nil

	for i := range ctr.groupByBatches {
		if ctr.groupByBatches[i] != nil {
			ctr.groupByBatches[i].Clean(proc.Mp())
			ctr.groupByBatches[i] = nil
		}
	}
	ctr.groupByBatches = nil

	ctr.freeAggList(proc)
}

func (ctr *container) reset(proc *process.Process) {
	ctr.state = vm.Build

	// Reset also frees the hash related stuff.
	ctr.hr.Free0()

	ctr.groupByEvaluate.ResetForNextQuery()

	for i := range ctr.aggArgEvaluate {
		ctr.aggArgEvaluate[i].ResetForNextQuery()
	}

	// still, free all groupByBatches and aggList,
	for i := range ctr.groupByBatches {
		if ctr.groupByBatches[i] != nil {
			ctr.groupByBatches[i].Clean(proc.Mp())
			ctr.groupByBatches[i] = nil
		}
	}
	ctr.groupByBatches = nil
	ctr.currBatchIdx = 0

	// OK, we just call Free on ag and it suppose will
	// reset ag state and ready to accept next batch.
	// no idea if this is true.
	for _, ag := range ctr.aggList {
		ag.Free()
		if ctr.mtyp == H0 {
			ag.GroupGrow(1)
		}
	}
}

func (group *Group) OpType() vm.OpType {
	return vm.Group
}

func (group Group) TypeName() string {
	return thisOperatorName
}

func (group *Group) GetOperatorBase() *vm.OperatorBase {
	return &group.OperatorBase
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

func NewArgument() *Group {
	return reuse.Alloc[Group](nil)
}

func (group *Group) Release() {
	if group != nil {
		reuse.Free(group, nil)
	}
}

func (group *Group) String(buf *bytes.Buffer) {
	buf.WriteString(thisOperatorName + ": group([")
	for i, expr := range group.Exprs {
		if i > 0 {
			buf.WriteString(", ")
		}
		buf.WriteString(fmt.Sprintf("%v", expr))
	}
	buf.WriteString("], [")

	for i, ag := range group.Aggs {
		if i > 0 {
			buf.WriteString(", ")
		}
		buf.WriteString(fmt.Sprintf("%v(%v)", function.GetAggFunctionNameByID(ag.GetAggID()), ag.GetArgExpressions()))
	}
	buf.WriteString("])")
}
