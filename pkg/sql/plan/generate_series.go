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
	gsColDefs = [2][]*plan.ColDef{}
)

func init() {
	retTyp := types.T_int64.ToType()
	gsColDefs[0] = []*plan.ColDef{
		{
			Name: "result",
			Typ:  makePlan2Type(&retTyp),
		},
	}
	retTyp = types.T_datetime.ToType()
	gsColDefs[1] = []*plan.ColDef{
		{
			Name: "result",
			Typ:  makePlan2Type(&retTyp),
		},
	}
}

func (builder *QueryBuilder) buildGenerateSeries(tbl *tree.TableFunction, ctx *BindContext, exprs []*plan.Expr, childId int32) int32 {
	var retsIdx int
	if types.IsInteger(types.T(exprs[0].Typ.Id)) {
		retsIdx = 0
	} else {
		retsIdx = 1
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
			Cols: gsColDefs[retsIdx],
		},
		BindingTags:     []int32{builder.genNewTag()},
		Children:        []int32{childId},
		TblFuncExprList: exprs,
	}
	return builder.appendNode(node, ctx)
}
