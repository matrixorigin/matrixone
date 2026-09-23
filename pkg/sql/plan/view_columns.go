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
	"strings"

	"github.com/matrixorigin/matrixone/pkg/common/moerr"
	"github.com/matrixorigin/matrixone/pkg/container/types"
	planpb "github.com/matrixorigin/matrixone/pkg/pb/plan"
	"github.com/matrixorigin/matrixone/pkg/sql/parsers/tree"
)

const (
	ViewColumnsFunctionName             = "mo_view_columns"
	SubscriptionViewColumnsFunctionName = "mo_subscription_view_columns"
)

func (builder *QueryBuilder) buildViewColumns(tbl *tree.TableFunction, ctx *BindContext, exprs []*Expr, children []int32) (int32, error) {
	functionName := strings.ToLower(tbl.Func.FuncName.Origin())
	if err := requireSubscriptionMetadataView(
		builder.GetContext(), ctx, builder.persistedViewTarget, functionName); err != nil {
		return 0, err
	}
	wantArgs := 1
	if functionName == SubscriptionViewColumnsFunctionName {
		wantArgs = 2
	}
	if len(exprs) != wantArgs {
		return 0, moerr.NewInvalidInputf(builder.GetContext(), "%s requires %d arguments", functionName, wantArgs)
	}
	args := make([]*Expr, wantArgs)
	var err error
	if wantArgs == 2 {
		args[0], err = forceCastExpr(builder.GetContext(), exprs[0], Type{Id: int32(types.T_uint32)})
		if err != nil {
			return 0, err
		}
	}
	args[wantArgs-1], err = forceCastExpr(builder.GetContext(), exprs[wantArgs-1], Type{Id: int32(types.T_uint64)})
	if err != nil {
		return 0, err
	}
	columns := []struct {
		name string
		typ  types.T
	}{
		{"attname", types.T_varchar}, {"attnum", types.T_int32}, {"atttyp", types.T_varbinary}, {"att_default", types.T_varbinary},
		{"attnotnull", types.T_int8}, {"attr_enum", types.T_varchar}, {"att_constraint_type", types.T_varchar},
		{"att_is_auto_increment", types.T_int8}, {"attr_has_generated", types.T_int8}, {"attr_generated", types.T_varbinary},
		{"att_comment", types.T_varchar}, {"att_is_hidden", types.T_int8},
	}
	defs := make([]*ColDef, len(columns))
	for i, c := range columns {
		defs[i] = &ColDef{Name: c.name, Typ: Type{Id: int32(c.typ)}}
	}
	node := &planpb.Node{NodeType: planpb.Node_FUNCTION_SCAN, Stats: &planpb.Stats{},
		TableDef:    &TableDef{TableType: "func_table", TblFunc: &planpb.TableFunction{Name: functionName, IsSingle: true}, Cols: defs},
		BindingTags: []int32{builder.genNewBindTag()}, Children: children, TblFuncExprList: args}
	return builder.appendNode(node, ctx), nil
}
