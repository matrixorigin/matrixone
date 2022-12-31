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
	"github.com/matrixorigin/matrixone/pkg/container/batch"
	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/container/vector"
	"github.com/matrixorigin/matrixone/pkg/pb/plan"
	"github.com/matrixorigin/matrixone/pkg/sql/colexec"
	"github.com/matrixorigin/matrixone/pkg/sql/parsers/tree"
)

func (builder *QueryBuilder) buildResultScan(tbl *tree.TableFunction, ctx *BindContext, exprs []*plan.Expr, childId int32) (int32, error) {
	var err error
	exprs[0], err = appendCastBeforeExpr(builder.GetContext(), exprs[0], &plan.Type{
		Id:          int32(types.T_uuid),
		NotNullable: true,
	})
	if err != nil {
		return 0, err
	}
	// calculate uuid
	bat := batch.NewWithSize(0)
	bat.Zs = []int64{1}
	vec, err := colexec.EvalExpr(bat, builder.compCtx.GetProcess(), exprs[0])
	if err != nil {
		return 0, err
	}
	uuid := vector.MustTCols[types.Uuid](vec)[0]
	// get cols
	cols, err := builder.compCtx.GetQueryResultColDefs(uuid.ToString())
	if err != nil {
		return 0, err
	}
	typs := make([]types.Type, len(cols))
	for i, c := range cols {
		typs[i] = types.New(types.T(c.Typ.Id), c.Typ.Width, c.Typ.Scale, c.Typ.Precision)
	}
	builder.compCtx.GetProcess().SessionInfo.ResultColTypes = typs
	node := &plan.Node{
		NodeType: plan.Node_FUNCTION_SCAN,
		Stats:    &plan.Stats{},
		TableDef: &plan.TableDef{
			TableType: "func_table",
			TblFunc: &plan.TableFunction{
				Name: "result_scan",
			},
			Cols: cols,
		},
		BindingTags:     []int32{builder.genNewTag()},
		Children:        []int32{childId},
		TblFuncExprList: exprs,
	}
	return builder.appendNode(node, ctx), nil
}
