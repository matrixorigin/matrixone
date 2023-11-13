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
	"github.com/matrixorigin/matrixone/pkg/container/batch"
	"github.com/matrixorigin/matrixone/pkg/container/vector"
	"github.com/matrixorigin/matrixone/pkg/pb/pipeline"
	"github.com/matrixorigin/matrixone/pkg/sql/colexec"
	"github.com/matrixorigin/matrixone/pkg/sql/plan"
	"github.com/matrixorigin/matrixone/pkg/vm"
	"github.com/matrixorigin/matrixone/pkg/vm/process"
)

const (
	sampleByRow = iota
	sampleByPercent
)

var _ vm.Operator = new(Argument)

type Argument struct {
	ctr *container

	// it determines which sample action (random sample by rows / percents, sample by order and so on) to take.
	Type int

	Rows     int
	Percents float64
	// sample(expression1, expression2, ..., number)'s expression.
	SampleExprs []*plan.Expr

	// group by expr1, expr2 ...
	GroupExprs []*plan.Expr

	IBucket, NBucket int

	info     *vm.OperatorInfo
	children []vm.Operator
}

type container struct {
	samplePool *sPool

	isGroupBy     bool
	isMultiSample bool

	// key width for group-by columns. it determines which hash function to use.
	keyWidth int

	// executor for group-by columns.
	groupExecutors       []colexec.ExpressionExecutor
	groupVectors         []*vector.Vector
	groupVectorsNullable bool

	// executor for sample(expression, number)'s expression.
	sampleExecutors []colexec.ExpressionExecutor
	tempBatch1      []*batch.Batch
	tempVectors     []*vector.Vector

	// hash map related.
	intHashMap *hashmap.IntHashMap
	strHashMap *hashmap.StrHashMap
}

func NewSampleByRows(rows int, sampleExprs, groupExprs []*plan.Expr) *Argument {
	return &Argument{
		Type:        sampleByRow,
		Rows:        rows,
		SampleExprs: sampleExprs,
		GroupExprs:  groupExprs,
		IBucket:     0,
		NBucket:     0,
	}
}

func (arg *Argument) SetInfo(info *vm.OperatorInfo) {
	arg.info = info
}

func (arg *Argument) AppendChild(child vm.Operator) {
	arg.children = append(arg.children, child)
}

func (arg *Argument) SimpleDup() *Argument {
	return &Argument{
		Type:        arg.Type,
		Rows:        arg.Rows,
		Percents:    arg.Percents,
		SampleExprs: arg.SampleExprs,
		GroupExprs:  arg.GroupExprs,
		IBucket:     arg.IBucket,
		NBucket:     arg.NBucket,
	}
}

func (arg *Argument) Free(proc *process.Process, pipelineFailed bool, err error) {
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
		for _, executor := range arg.ctr.groupExecutors {
			if executor != nil {
				executor.Free()
			}
		}
		for _, p := range arg.ctr.samplePool.sPools {
			if p.bat != nil {
				proc.PutBatch(p.bat)
			}
		}
		for _, p := range arg.ctr.samplePool.mPools {
			if p.bat != nil {
				proc.PutBatch(p.bat)
			}
		}
	}
}

func (arg *Argument) ConvertToPipelineOperator(in *pipeline.Instruction) {
	in.Agg = &pipeline.Group{
		Ibucket: uint64(arg.IBucket),
		Nbucket: uint64(arg.NBucket),
		Exprs:   arg.GroupExprs,
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
}

func GenerateFromPipelineOperator(opr *pipeline.Instruction) *Argument {
	s := opr.GetSampleFunc()
	g := opr.GetAgg()
	if s.SampleType == pipeline.SampleFunc_Rows {
		return NewSampleByRows(int(s.SampleRows), s.SampleColumns, g.Exprs)
	} else {
		panic("unsupported sample type")
	}
}
