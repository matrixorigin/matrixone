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

var _ vm.Operator = new(Sample)

type Sample struct {
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

	vm.OperatorBase
}

func (sample *Sample) GetOperatorBase() *vm.OperatorBase {
	return &sample.OperatorBase
}

type container struct {
	samplePool *sPool
	// safe check.
	workDone             bool
	isGroupBy            bool
	isMultiSample        bool
	groupVectorsNullable bool
	useIntHashMap        bool

	// executor for group-by columns.
	groupExecutors []colexec.ExpressionExecutor
	groupVectors   []*vector.Vector

	// executor for sample(expression, number)'s expression.
	sampleExecutors []colexec.ExpressionExecutor
	tempBatch1      []*batch.Batch
	sampleVectors   []*vector.Vector

	buf *batch.Batch

	// hash map related.
	intHashMap *hashmap.IntHashMap
	strHashMap *hashmap.StrHashMap
}

func init() {
	reuse.CreatePool[Sample](
		func() *Sample {
			return &Sample{}
		},
		func(a *Sample) {
			*a = Sample{}
		},
		reuse.DefaultOptions[Sample]().
			WithEnableChecker(),
	)
}

func (sample Sample) TypeName() string {
	return opName
}

func NewArgument() *Sample {
	return reuse.Alloc[Sample](nil)
}

func (sample *Sample) Release() {
	if sample != nil {
		reuse.Free[Sample](sample, nil)
	}
}

func NewMergeSample(rowSampleArg *Sample, outputRowCount bool) *Sample {
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

func NewSampleByRows(rows int, sampleExprs, groupExprs []*plan.Expr, usingRow bool, outputRowCount bool) *Sample {
	arg := NewArgument()
	arg.Type = sampleByRow
	arg.UsingBlock = !usingRow
	arg.NeedOutputRowSeen = outputRowCount
	arg.Rows = rows
	arg.SampleExprs = sampleExprs
	arg.GroupExprs = groupExprs
	return arg
}

func NewSampleByPercent(percent float64, sampleExprs, groupExprs []*plan.Expr) *Sample {
	arg := NewArgument()
	arg.Type = sampleByPercent
	arg.UsingBlock = false
	arg.NeedOutputRowSeen = false
	arg.Percents = percent
	arg.SampleExprs = sampleExprs
	arg.GroupExprs = groupExprs
	return arg
}

func (sample *Sample) IsMergeSampleByRow() bool {
	return sample.Type == mergeSampleByRow
}

func (sample *Sample) IsByPercent() bool {
	return sample.Type == sampleByPercent
}

func (sample *Sample) SampleDup() *Sample {
	a := NewArgument()
	a.Type = sample.Type
	a.UsingBlock = sample.UsingBlock
	a.NeedOutputRowSeen = sample.NeedOutputRowSeen
	a.Rows = sample.Rows
	a.Percents = sample.Percents
	a.SampleExprs = sample.SampleExprs
	a.GroupExprs = sample.GroupExprs
	return a
}

func (sample *Sample) Reset(proc *process.Process, pipelineFailed bool, err error) {
	sample.Free(proc, pipelineFailed, err)
}

func (sample *Sample) Free(proc *process.Process, pipelineFailed bool, err error) {
	if sample.ctr != nil {
		if sample.ctr.intHashMap != nil {
			sample.ctr.intHashMap.Free()
		}
		if sample.ctr.strHashMap != nil {
			sample.ctr.strHashMap.Free()
		}
		for _, executor := range sample.ctr.sampleExecutors {
			if executor != nil {
				executor.Free()
			}
		}
		sample.ctr.sampleExecutors = nil

		for _, executor := range sample.ctr.groupExecutors {
			if executor != nil {
				executor.Free()
			}
		}
		sample.ctr.groupExecutors = nil

		if sample.ctr.samplePool != nil {
			sample.ctr.samplePool.Free()
		}

		if sample.ctr.buf != nil {
			sample.ctr.buf.Clean(proc.Mp())
			sample.ctr.buf = nil
		}

		sample.ctr = nil
	}
}

func (sample *Sample) ConvertToPipelineOperator(in *pipeline.Instruction) {
	in.Agg = &pipeline.Group{
		Exprs:    sample.GroupExprs,
		NeedEval: sample.UsingBlock,
	}
	if sample.NeedOutputRowSeen {
		in.Agg.PreAllocSize = 1
	}

	in.SampleFunc = &pipeline.SampleFunc{
		SampleColumns: sample.SampleExprs,
		SampleType:    pipeline.SampleFunc_Rows,
		SampleRows:    int32(sample.Rows),
		SamplePercent: sample.Percents,
	}
	if sample.Type == sampleByPercent {
		in.SampleFunc.SampleType = pipeline.SampleFunc_Percent
	}
	if sample.Type == mergeSampleByRow {
		in.SampleFunc.SampleType = pipeline.SampleFunc_MergeRows
	}
}

func GenerateFromPipelineOperator(opr *pipeline.Instruction) *Sample {
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
