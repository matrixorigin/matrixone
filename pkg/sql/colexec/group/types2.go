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

	"github.com/matrixorigin/matrixone/pkg/common"
	"github.com/matrixorigin/matrixone/pkg/common/mpool"
	"github.com/matrixorigin/matrixone/pkg/common/reuse"
	"github.com/matrixorigin/matrixone/pkg/common/system"
	"github.com/matrixorigin/matrixone/pkg/container/batch"
	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/container/vector"
	"github.com/matrixorigin/matrixone/pkg/pb/plan"
	"github.com/matrixorigin/matrixone/pkg/sql/colexec"
	"github.com/matrixorigin/matrixone/pkg/sql/colexec/aggexec"
	"github.com/matrixorigin/matrixone/pkg/sql/plan/function"
	"github.com/matrixorigin/matrixone/pkg/util/list"
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
	GroupBy      []*plan.Expr
	GroupingFlag []bool

	Aggs []aggexec.AggFuncExecExpression
}

type spillBucket struct {
	lv   int      // spill level
	name string   // spill bucket name
	cnt  int64    // number of rows in this spill bucket
	file *os.File // spill file
}

func (bkt *spillBucket) free() {
	if bkt != nil && bkt.file != nil {
		bkt.file.Close()
		bkt.file = nil
	}
}

// container running context.
type container struct {
	state vm.CtrState
	mp    *mpool.MPool

	inputDone    bool
	currBatchIdx int

	// hash.
	hr          ResHashRelated
	mtyp        int32
	keyWidth    int32
	keyNullable bool

	// x, y of `group by x, y`.
	groupByEvaluate colexec.ExprEvalVector
	// m, n of `select agg1(m, n), agg2(m, n)`.
	aggArgEvaluate []colexec.ExprEvalVector

	// group by columns
	groupByTypes   []types.Type
	groupByBatches []*batch.Batch

	// aggs, which holds the intermediate state of agg functions.
	aggList []aggexec.AggFuncExec

	// spill, agglist to load spilled data.
	spillMem        int64
	spillAggList    []aggexec.AggFuncExec
	spillBkts       list.Deque[*spillBucket]
	currentSpillBkt []*spillBucket
}

func (ctr *container) isSpilling() bool {
	return len(ctr.currentSpillBkt) > 0
}

func (ctr *container) setSpillMem(m int64, aggs []aggexec.AggFuncExecExpression) {
	// BUG #22725
	// We simply cannot spill distinct agg at this moment.
	for _, ag := range aggs {
		if ag.IsDistinct() {
			// Set to TiB, effectively disabling spill for distinct agg.
			// If we cannot fix this before TB mem is commonly available
			// it will be very sad.
			ctr.spillMem = common.TiB
			return
		}
	}

	if m == 0 {
		// 0 means auto config.   Here the formula is made up on the fly.
		mem := int64(system.MemoryTotal()) / int64(system.GoMaxProcs()) / 8
		// min 128MB
		if mem < common.MiB*128 {
			mem = common.MiB * 128
		}
		ctr.spillMem = mem
	} else {
		ctr.spillMem = m
	}
}

func (ctr *container) freeAggList() {
	for i := range ctr.aggList {
		if ctr.aggList[i] != nil {
			ctr.aggList[i].Free()
			ctr.aggList[i] = nil
		}
	}
	ctr.aggList = nil
}

func (ctr *container) freeSpillAggList() {
	for i := range ctr.spillAggList {
		if ctr.spillAggList[i] != nil {
			ctr.spillAggList[i].Free()
			ctr.spillAggList[i] = nil
		}
	}
	ctr.spillAggList = nil
}

func (ctr *container) freeSpillBkts() {
	// free all spill buckets.
	if ctr.spillBkts != nil {
		ctr.spillBkts.Iter(0, func(bkt *spillBucket) bool {
			bkt.free()
			return true
		})
		ctr.spillBkts.Clear()
	}

	for _, bkt := range ctr.currentSpillBkt {
		bkt.free()
	}
	ctr.currentSpillBkt = nil
}

func (ctr *container) freeGroupByBatches() {
	for i := range ctr.groupByBatches {
		if ctr.groupByBatches[i] != nil {
			ctr.groupByBatches[i].Clean(ctr.mp)
			ctr.groupByBatches[i] = nil
		}
	}
	ctr.groupByBatches = nil
	ctr.currBatchIdx = 0
}

func (ctr *container) free() {
	// free container stuff, WTH is the Free0?
	ctr.inputDone = false
	ctr.hr.Free0()

	ctr.groupByEvaluate.Free()

	for i := range ctr.aggArgEvaluate {
		ctr.aggArgEvaluate[i].Free()
	}
	ctr.aggArgEvaluate = nil

	ctr.freeGroupByBatches()
	ctr.freeAggList()
	ctr.freeSpillAggList()
	ctr.freeSpillBkts()

	mpool.DeleteMPool(ctr.mp)
	ctr.mp = nil
}

func (ctr *container) reset() {
	ctr.free()
}

func (ctr *container) resetForSpill() {
	// Reset also frees the hash related stuff.
	ctr.hr.Free0()

	ctr.groupByEvaluate.ResetForNextQuery()

	for i := range ctr.aggArgEvaluate {
		ctr.aggArgEvaluate[i].ResetForNextQuery()
	}
	// free group by batches, agg list and spill buckets, do not reuse for now.
	ctr.freeGroupByBatches()
	ctr.currBatchIdx = 0

	ctr.freeAggList()
	ctr.freeSpillAggList()
}

func (group *Group) evaluateGroupByAndAggArgs(proc *process.Process, bat *batch.Batch) (err error) {
	input := []*batch.Batch{bat}

	// group.
	for i := range group.ctr.groupByEvaluate.Vec {
		if group.ctr.groupByEvaluate.Vec[i], err =
			group.ctr.groupByEvaluate.Executor[i].Eval(proc, input, nil); err != nil {
			return err
		}
	}

	// agg args.
	for i := range group.ctr.aggArgEvaluate {
		for j := range group.ctr.aggArgEvaluate[i].Vec {
			if group.ctr.aggArgEvaluate[i].Vec[j], err =
				group.ctr.aggArgEvaluate[i].Executor[j].Eval(proc, input, nil); err != nil {
				return err
			}
		}
	}

	// grouping flag
	for i, flag := range group.GroupingFlag {
		if !flag {
			group.ctr.groupByEvaluate.Vec[i] = vector.NewRollupConst(
				group.ctr.groupByEvaluate.Typ[i],
				group.ctr.groupByEvaluate.Vec[i].Length(),
				proc.Mp())
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
	group.ctr.free()
	// free projection stuff,
	group.FreeProjection(proc)
}

func (group *Group) Reset(proc *process.Process, pipelineFailed bool, err error) {
	group.ctr.reset()
	group.ResetProjection(proc)
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
	for i, expr := range group.GroupBy {
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

const (
	mergeGroupOperatorName = "merge_group"
)

type MergeGroup struct {
	vm.OperatorBase
	colexec.Projection

	ctr      container
	SpillMem int64

	Aggs []aggexec.AggFuncExecExpression

	PartialResults     []any
	PartialResultTypes []types.T
}

func (mergeGroup *MergeGroup) ExecProjection(proc *process.Process, input *batch.Batch) (*batch.Batch, error) {
	if mergeGroup.ProjectList == nil {
		return input, nil
	}
	return mergeGroup.EvalProjection(input, proc)
}

func (mergeGroup *MergeGroup) Reset(proc *process.Process, _ bool, _ error) {
	mergeGroup.ctr.reset()
	mergeGroup.ResetProjection(proc)
}

func (mergeGroup *MergeGroup) Free(proc *process.Process, _ bool, _ error) {
	mergeGroup.ctr.free()
	mergeGroup.FreeProjection(proc)
}

func (mergeGroup *MergeGroup) GetOperatorBase() *vm.OperatorBase {
	return &mergeGroup.OperatorBase
}

func (mergeGroup *MergeGroup) OpType() vm.OpType {
	return vm.MergeGroup
}

func (mergeGroup MergeGroup) TypeName() string {
	return mergeGroupOperatorName
}

func (mergeGroup *MergeGroup) String(buf *bytes.Buffer) {
	buf.WriteString(mergeGroupOperatorName)
}

func NewArgumentMergeGroup() *MergeGroup {
	return reuse.Alloc[MergeGroup](nil)
}

func (mergeGroup *MergeGroup) Release() {
	if mergeGroup != nil {
		reuse.Free(mergeGroup, nil)
	}
}

func init() {
	reuse.CreatePool(
		func() *Group {
			return &Group{}
		},
		func(a *Group) {
			*a = Group{}
		},
		reuse.DefaultOptions[Group]().
			WithEnableChecker(),
	)

	reuse.CreatePool(
		func() *MergeGroup {
			return &MergeGroup{}
		},
		func(a *MergeGroup) {
			*a = MergeGroup{}
		},
		reuse.DefaultOptions[MergeGroup]().
			WithEnableChecker(),
	)
}
