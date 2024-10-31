// Copyright 2021 - 2023 Matrix Origin
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
	"github.com/matrixorigin/matrixone/pkg/pb/plan"
	"github.com/matrixorigin/matrixone/pkg/pb/status"
	"github.com/matrixorigin/matrixone/pkg/sql/parsers/tree"
)

var SessionsColTypes []types.Type

func (builder *QueryBuilder) buildProcesslist(tbl *tree.TableFunction, ctx *BindContext, exprs []*plan.Expr, childId int32) (int32, error) {
	if len(tbl.Func.Exprs) > 0 {
		return 0, moerr.NewInvalidArg(builder.GetContext(), "processlist function has invalid input args length", len(tbl.Func.Exprs))
	}
	SessionsColTypes = make([]types.Type, len(status.SessionField_name))
	sessionsColDefs := make([]*plan.ColDef, len(status.SessionField_name))

	for i := range status.SessionField_name {
		var typ types.Type
		switch status.SessionField(i) {
		case status.SessionField_CONN_ID:
			typ = types.New(types.T_uint32, 0, 0)
		default:
			typ = types.New(types.T_varchar, types.MaxVarcharLen, 0)
		}
		SessionsColTypes[i] = typ
		sessionsColDefs[i] = &plan.ColDef{
			Name: strings.ToLower(status.SessionField_name[i]),
			Typ: plan.Type{
				Id:    int32(typ.Oid),
				Width: typ.Width,
			},
		}
	}
	node := &plan.Node{
		NodeType: plan.Node_FUNCTION_SCAN,
		Stats:    &plan.Stats{},
		TableDef: &plan.TableDef{
			TableType: "func_table",
			TblFunc: &plan.TableFunction{
				Name: "processlist",
			},
			Cols: sessionsColDefs,
		},
		BindingTags:     []int32{builder.genNewTag()},
		Children:        []int32{childId},
		TblFuncExprList: exprs,
	}
	return builder.appendNode(node, ctx), nil
}
