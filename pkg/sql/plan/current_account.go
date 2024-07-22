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
	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/pb/plan"
	"github.com/matrixorigin/matrixone/pkg/sql/parsers/tree"
)

func (builder *QueryBuilder) buildCurrentAccount(tbl *tree.TableFunction, ctx *BindContext, exprs []*plan.Expr, childId int32) (int32, error) {
	if len(tbl.Func.Exprs) > 0 {
		return 0, moerr.NewInvalidArg(builder.GetContext(), "current_account function has invalid input args length", len(tbl.Func.Exprs))
	}
	node := &plan.Node{
		NodeType: plan.Node_FUNCTION_SCAN,
		Stats:    &plan.Stats{},
		TableDef: &plan.TableDef{
			TableType: "func_table",
			TblFunc: &plan.TableFunction{
				Name: "current_account",
			},
			Cols: []*plan.ColDef{
				{
					Name: "account_name",
					Typ: plan.Type{
						Id:    int32(types.T_varchar),
						Width: types.MaxVarcharLen,
					},
				},
				{
					Name: "account_id",
					Typ: plan.Type{
						Id:    int32(types.T_uint32),
						Width: types.MaxVarcharLen,
					},
				},
				{
					Name: "user_name",
					Typ: plan.Type{
						Id:    int32(types.T_varchar),
						Width: types.MaxVarcharLen,
					},
				},
				{
					Name: "user_id",
					Typ: plan.Type{
						Id:    int32(types.T_uint32),
						Width: types.MaxVarcharLen,
					},
				},
				{
					Name: "role_name",
					Typ: plan.Type{
						Id:    int32(types.T_varchar),
						Width: types.MaxVarcharLen,
					},
				},
				{
					Name: "role_id",
					Typ: plan.Type{
						Id:    int32(types.T_uint32),
						Width: types.MaxVarcharLen,
					},
				},
			},
		},
		BindingTags:     []int32{builder.genNewTag()},
		Children:        []int32{childId},
		TblFuncExprList: exprs,
	}
	return builder.appendNode(node, ctx), nil
}
