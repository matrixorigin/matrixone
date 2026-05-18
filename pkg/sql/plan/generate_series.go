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
	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/pb/plan"
	"github.com/matrixorigin/matrixone/pkg/sql/parsers/tree"
)

var (
	generateSeriesColDefs = [3][]*plan.ColDef{}
)

func init() {
	retTyp := types.T_int64.ToType()
	generateSeriesColDefs[0] = []*plan.ColDef{
		{
			Name: "result",
			Typ:  makePlan2Type(&retTyp),
		},
	}
	retTyp = types.T_datetime.ToType()
	generateSeriesColDefs[1] = []*plan.ColDef{
		{
			Name: "result",
			Typ:  makePlan2Type(&retTyp),
		},
	}
	retTyp = types.T_varchar.ToType()
	generateSeriesColDefs[2] = []*plan.ColDef{
		{
			Name: "result",
			Typ:  makePlan2Type(&retTyp),
		},
	}
}

func (builder *QueryBuilder) buildGenerateSeries(tbl *tree.TableFunction, ctx *BindContext, exprs []*plan.Expr, children []int32) int32 {
	var retsIdx int
	if types.T(exprs[0].Typ.Id).IsInteger() {
		retsIdx = 0
	} else if types.T(exprs[0].Typ.Id).IsDateRelate() {
		retsIdx = 1
	} else {
		retsIdx = 2
	}
	node := &plan.Node{
		NodeType: plan.Node_FUNCTION_SCAN,
		Stats:    &plan.Stats{},
		TableDef: &plan.TableDef{
			TableType: "func_table", //test if ok
			//Name:               tbl.String(),
			TblFunc: &plan.TableFunction{
				Name: "generate_series",
			},
			Cols: generateSeriesColDefs[retsIdx],
		},
		BindingTags:     []int32{builder.genNewBindTag()},
		Children:        children,
		TblFuncExprList: exprs,
	}
	return builder.appendNode(node, ctx)
}

func (builder *QueryBuilder) buildGenerateRandomInt64(tbl *tree.TableFunction, ctx *BindContext, exprs []*plan.Expr, children []int32) int32 {
	i64Typ := types.T_int64.ToType()
	node := &plan.Node{
		NodeType: plan.Node_FUNCTION_SCAN,
		Stats:    &plan.Stats{},
		TableDef: &plan.TableDef{
			TableType: "func_table", //test if ok
			//Name:               tbl.String(),
			TblFunc: &plan.TableFunction{
				Name: "generate_random_int64",
			},
			Cols: []*plan.ColDef{
				{
					Name: "nth",
					Typ:  makePlan2Type(&i64Typ),
				},
				{
					Name: "i64",
					Typ:  makePlan2Type(&i64Typ),
				},
			},
		},
		BindingTags:     []int32{builder.genNewBindTag()},
		Children:        children,
		TblFuncExprList: exprs,
	}
	return builder.appendNode(node, ctx)
}

func (builder *QueryBuilder) buildGenerateRandomFloat64(tbl *tree.TableFunction, ctx *BindContext, exprs []*plan.Expr, children []int32) int32 {
	i64Typ := types.T_int64.ToType()
	f64Typ := types.T_float64.ToType()
	node := &plan.Node{
		NodeType: plan.Node_FUNCTION_SCAN,
		Stats:    &plan.Stats{},
		TableDef: &plan.TableDef{
			TableType: "func_table", //test if ok
			//Name:               tbl.String(),
			TblFunc: &plan.TableFunction{
				Name: "generate_random_float64",
			},
			Cols: []*plan.ColDef{
				{
					Name: "nth",
					Typ:  makePlan2Type(&i64Typ),
				},
				{
					Name: "f64",
					Typ:  makePlan2Type(&f64Typ),
				},
			},
		},
		BindingTags:     []int32{builder.genNewBindTag()},
		Children:        children,
		TblFuncExprList: exprs,
	}
	return builder.appendNode(node, ctx)
}
