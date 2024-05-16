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

package sample

import (
	"github.com/matrixorigin/matrixone/pkg/common/hashmap"
	"github.com/matrixorigin/matrixone/pkg/common/reuse"
	"github.com/matrixorigin/matrixone/pkg/container/batch"
	"github.com/matrixorigin/matrixone/pkg/container/vector"
	"github.com/matrixorigin/matrixone/pkg/pb/pipeline"
	planpb "github.com/matrixorigin/matrixone/pkg/pb/plan"
	"github.com/matrixorigin/matrixone/pkg/sql/colexec"
	"github.com/matrixorigin/matrixone/pkg/sql/plan"
	"github.com/matrixorigin/matrixone/pkg/vm"
	"github.com/matrixorigin/matrixone/pkg/vm/process"
)

const (
	sampleByRow = iota
	sampleByPercent
	mergeSampleByRow
)

var _ vm.Operator = new(Argument)

type Argument struct {
	ctr *container

	// it determines which sample action (random sample by rows / percents, sample by order and so on) to take.
	Type int
	// UsingBlock is used to speed up the sample process but will cause centroids skewed.
	// If true, the sample action will randomly stop the sample process after it has sampled enough rows.
	UsingBlock bool
	// NeedOutputRowSeen indicates whether the sample operator needs to output the count of row seen as the last column.
	NeedOutputRowSeen bool

	Rows     int
	Percents float64

	// sample(expression1, expression2, ..., number)'s expression.
	SampleExprs []*plan.Expr

	// group by expr1, expr2 ...
	GroupExprs []*plan.Expr

	buf *batch.Batch

	vm.OperatorBase
}

func (arg *Argument) GetOperatorBase() *vm.OperatorBase {
	return &arg.OperatorBase
}

type container struct {
	// safe check.
	workDone bool

	samplePool *sPool

	isGroupBy     bool
	isMultiSample bool

	// executor for group-by columns.
	groupExecutors       []colexec.ExpressionExecutor
	groupVectors         []*vector.Vector
	groupVectorsNullable bool

	// executor for sample(expression, number)'s expression.
	sampleExecutors []colexec.ExpressionExecutor
	tempBatch1      []*batch.Batch
	sampleVectors   []*vector.Vector

	// hash map related.
	useIntHashMap bool
	intHashMap    *hashmap.IntHashMap
	strHashMap    *hashmap.StrHashMap
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

func NewMergeSample(rowSampleArg *Argument, outputRowCount bool) *Argument {
	if rowSampleArg.Type != sampleByRow {
		panic("invalid sample type to merge")
	}

	newGroupExpr := make([]*plan.Expr, len(rowSampleArg.GroupExprs))
	newSampleExpr := make([]*plan.Expr, len(rowSampleArg.SampleExprs))
	for i, expr := range rowSampleArg.GroupExprs {
		newGroupExpr[i] = &plan.Expr{
			Expr: &planpb.Expr_Col{
				Col: &plan.ColRef{
					RelPos: 0,
					ColPos: int32(i),
				},
			},
			Typ: expr.Typ,
		}
	}
	for i, expr := range rowSampleArg.SampleExprs {
		newSampleExpr[i] = &plan.Expr{
			Expr: &planpb.Expr_Col{
				Col: &plan.ColRef{
					RelPos: 0,
					ColPos: int32(i + len(rowSampleArg.GroupExprs)),
				},
			},
			Typ: expr.Typ,
		}
	}

	rowSampleArg.NeedOutputRowSeen = true
	arg := NewArgument()
	arg.Type = mergeSampleByRow
	arg.UsingBlock = rowSampleArg.UsingBlock
	arg.NeedOutputRowSeen = outputRowCount
	arg.Rows = rowSampleArg.Rows
	arg.GroupExprs = newGroupExpr
	arg.SampleExprs = newSampleExpr
	return arg
}

func NewSampleByRows(rows int, sampleExprs, groupExprs []*plan.Expr, usingRow bool, outputRowCount bool) *Argument {
	arg := NewArgument()
	arg.Type = sampleByRow
	arg.UsingBlock = !usingRow
	arg.NeedOutputRowSeen = outputRowCount
	arg.Rows = rows
	arg.SampleExprs = sampleExprs
	arg.GroupExprs = groupExprs
	return arg
}

func NewSampleByPercent(percent float64, sampleExprs, groupExprs []*plan.Expr) *Argument {
	arg := NewArgument()
	arg.Type = sampleByPercent
	arg.UsingBlock = false
	arg.NeedOutputRowSeen = false
	arg.Percents = percent
	arg.SampleExprs = sampleExprs
	arg.GroupExprs = groupExprs
	return arg
}

func (arg *Argument) IsMergeSampleByRow() bool {
	return arg.Type == mergeSampleByRow
}

func (arg *Argument) IsByPercent() bool {
	return arg.Type == sampleByPercent
}

func (arg *Argument) SimpleDup() *Argument {
	a := NewArgument()
	a.Type = arg.Type
	a.UsingBlock = arg.UsingBlock
	a.NeedOutputRowSeen = arg.NeedOutputRowSeen
	a.Rows = arg.Rows
	a.Percents = arg.Percents
	a.SampleExprs = arg.SampleExprs
	a.GroupExprs = arg.GroupExprs
	return a
}

func (arg *Argument) Reset(proc *process.Process, pipelineFailed bool, err error) {
	arg.Free(proc, pipelineFailed, err)
}

func (arg *Argument) Free(proc *process.Process, pipelineFailed bool, err error) {
	if arg.buf != nil {
		arg.buf.Clean(proc.Mp())
		arg.buf = nil
	}

	if arg.ctr != nil {
		if arg.ctr.intHashMap != nil {
			arg.ctr.intHashMap.Free()
		}
		if arg.ctr.strHashMap != nil {
			arg.ctr.strHashMap.Free()
		}
		for _, executor := range arg.ctr.sampleExecutors {
			if executor != nil {
				executor.Free()
			}
		}
		arg.ctr.sampleExecutors = nil

		for _, executor := range arg.ctr.groupExecutors {
			if executor != nil {
				executor.Free()
			}
		}
		arg.ctr.groupExecutors = nil

		if arg.ctr.samplePool != nil {
			arg.ctr.samplePool.Free()
		}

		arg.ctr = nil
	}
}

func (arg *Argument) ConvertToPipelineOperator(in *pipeline.Instruction) {
	in.Agg = &pipeline.Group{
		Exprs:    arg.GroupExprs,
		NeedEval: arg.UsingBlock,
	}
	if arg.NeedOutputRowSeen {
		in.Agg.PreAllocSize = 1
	}

	in.SampleFunc = &pipeline.SampleFunc{
		SampleColumns: arg.SampleExprs,
		SampleType:    pipeline.SampleFunc_Rows,
		SampleRows:    int32(arg.Rows),
		SamplePercent: arg.Percents,
	}
	if arg.Type == sampleByPercent {
		in.SampleFunc.SampleType = pipeline.SampleFunc_Percent
	}
	if arg.Type == mergeSampleByRow {
		in.SampleFunc.SampleType = pipeline.SampleFunc_MergeRows
	}
}

func GenerateFromPipelineOperator(opr *pipeline.Instruction) *Argument {
	s := opr.GetSampleFunc()
	g := opr.GetAgg()
	needOutputRowSeen := g.PreAllocSize == 1

	if s.SampleType == pipeline.SampleFunc_Rows {
		return NewSampleByRows(int(s.SampleRows), s.SampleColumns, g.Exprs, !g.NeedEval, needOutputRowSeen)
	}
	if s.SampleType == pipeline.SampleFunc_Percent {
		return NewSampleByPercent(s.SamplePercent, s.SampleColumns, g.Exprs)
	}
	arg := NewSampleByRows(int(s.SampleRows), s.SampleColumns, g.Exprs, !g.NeedEval, needOutputRowSeen)
	arg.Type = mergeSampleByRow
	return arg
}
