// Copyright 2022 Matrix Origin
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package plan

import (
	"github.com/matrixorigin/matrixone/pkg/common/moerr"
	"github.com/matrixorigin/matrixone/pkg/pb/plan"
	"github.com/matrixorigin/matrixone/pkg/sql/parsers/dialect"
	"github.com/matrixorigin/matrixone/pkg/sql/parsers/tree"
)

const (
	NotSampleByRows     = -1
	NotSampleByPercents = -1.0
)

type SampleFuncCtx struct {
	hasSampleFunc bool

	sRows    bool // if true, sample rows.
	rows     int32
	percents float64
	columns  []*plan.Expr

	// sampleUsingRow will scan all the blocks to avoid the centroids skewed.
	// but this may cost much time.
	sampleUsingRow bool
	// start and offset in the select clause.
	start  int
	offset int
}

func (s *SampleFuncCtx) GenerateSampleFunc(se *tree.SampleExpr) error {
	if err := se.Valid(); err != nil {
		return err
	}

	if s.hasSampleFunc {
		return moerr.NewSyntaxErrorNoCtx("cannot use more than one sample function at select clause.")
	}
	s.hasSampleFunc = true
	s.sRows, s.sampleUsingRow, s.rows, s.percents = se.GetSampleDetail()

	return nil
}

func (s *SampleFuncCtx) BindSampleColumn(ctx *BindContext, binder *ProjectionBinder, sampleList tree.SelectExprs) ([]*plan.Expr, error) {
	s.columns = make([]*plan.Expr, 0, s.offset)

	pList := make([]*plan.Expr, 0, len(sampleList))
	for _, se := range sampleList {
		astStr := tree.String(se.Expr, dialect.MYSQL)

		if _, ok := ctx.groupByAst[astStr]; ok {
			return nil, moerr.NewInternalErrorNoCtx("cannot sample the group by column.")
		}

		if colPos, ok := ctx.sampleByAst[astStr]; ok {
			expr := &plan.Expr{
				Typ: ctx.sampleFunc.columns[colPos].Typ,
				Expr: &plan.Expr_Col{
					Col: &plan.ColRef{
						RelPos: ctx.sampleTag,
						ColPos: colPos,
					},
				},
			}
			ctx.projects = append(ctx.projects, expr)
			continue
		}
		expr, err := binder.baseBindExpr(se.Expr, 0, true)
		if err != nil {
			return nil, err
		}
		colPos := int32(len(s.columns))
		ctx.sampleByAst[astStr] = colPos
		s.columns = append(s.columns, expr)

		pList = append(pList, &plan.Expr{
			Typ: ctx.sampleFunc.columns[colPos].Typ,
			Expr: &plan.Expr_Col{
				Col: &plan.ColRef{
					RelPos: ctx.sampleTag,
					ColPos: colPos,
				},
			},
		})
	}
	return pList, nil
}

func (s *SampleFuncCtx) SetStartOffset(start, offset int) {
	s.start = start
	s.offset = offset
}

func validSample(ctx *BindContext, builder *QueryBuilder) error {
	if ctx.sampleFunc.hasSampleFunc {
		if len(ctx.aggregates) > 0 {
			return moerr.NewSyntaxError(builder.GetContext(), "cannot fixed non-scalar function and scalar function in the same query")
		}
		if ctx.recSelect || builder.isForUpdate {
			return moerr.NewInternalError(builder.GetContext(), "not support sample function recursive cte or for update")
		}
		if len(ctx.windows) > 0 {
			return moerr.NewNYI(builder.GetContext(), "sample for window function not support now")
		}
	}
	return nil
}

//type SampleClauseCtx struct {
//	hasSampleClause bool
//
//	sRows    bool // if true, sample rows.
//	rows     int32
//	percents float64
//}

func generateSamplePlanNode(ctx *BindContext, childNodeID int32) *plan.Node {
	sampleNode := &plan.Node{
		NodeType:    plan.Node_SAMPLE,
		Children:    []int32{childNodeID},
		GroupBy:     ctx.groups,
		AggList:     ctx.sampleFunc.columns,
		BindingTags: []int32{ctx.groupTag, ctx.sampleTag},
		SampleFunc:  &plan.SampleFuncSpec{Rows: NotSampleByRows, Percent: NotSampleByPercents},
	}
	if ctx.sampleFunc.sRows {
		sampleNode.SampleFunc.Rows = ctx.sampleFunc.rows
		sampleNode.SampleFunc.UsingRow = ctx.sampleFunc.sampleUsingRow
	} else {
		sampleNode.SampleFunc.Percent = ctx.sampleFunc.percents
		sampleNode.SampleFunc.UsingRow = true
	}
	return sampleNode
}
