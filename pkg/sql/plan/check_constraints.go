// Copyright 2026 Matrix Origin
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
	planpb "github.com/matrixorigin/matrixone/pkg/pb/plan"
	"github.com/matrixorigin/matrixone/pkg/sql/parsers/tree"
)

var checkConstraintsColDefs = []*planpb.ColDef{
	{
		Name: "constraint_catalog",
		Typ: planpb.Type{
			Id:    int32(types.T_varchar),
			Width: 64,
		},
	},
	{
		Name: "constraint_schema",
		Typ: planpb.Type{
			Id:    int32(types.T_varchar),
			Width: 64,
		},
	},
	{
		Name: "constraint_name",
		Typ: planpb.Type{
			Id:    int32(types.T_varchar),
			Width: 64,
		},
	},
	{
		Name: "check_clause",
		Typ: planpb.Type{
			Id:    int32(types.T_varchar),
			Width: types.MaxVarcharLen,
		},
	},
	{
		Name: "table_name",
		Typ: planpb.Type{
			Id:    int32(types.T_varchar),
			Width: 64,
		},
	},
	{
		Name: "constraint_type",
		Typ: planpb.Type{
			Id:    int32(types.T_varchar),
			Width: 64,
		},
	},
	{
		Name: "enforced",
		Typ: planpb.Type{
			Id:    int32(types.T_varchar),
			Width: 3,
		},
	},
}

func (builder *QueryBuilder) buildCheckConstraints(
	tbl *tree.TableFunction,
	ctx *BindContext,
	exprs []*planpb.Expr,
	children []int32,
) (int32, error) {
	if len(tbl.Func.Exprs) != 0 {
		return 0, moerr.NewInvalidArg(builder.GetContext(),
			"mo_check_constraints function has invalid input args length", len(tbl.Func.Exprs))
	}

	node := &planpb.Node{
		NodeType: planpb.Node_FUNCTION_SCAN,
		Stats:    &planpb.Stats{},
		TableDef: &planpb.TableDef{
			TableType: "func_table",
			TblFunc: &planpb.TableFunction{
				Name: "mo_check_constraints",
			},
			Cols: DeepCopyColDefList(checkConstraintsColDefs),
		},
		BindingTags:     []int32{builder.genNewBindTag()},
		Children:        children,
		TblFuncExprList: exprs,
	}
	return builder.appendNode(node, ctx), nil
}
