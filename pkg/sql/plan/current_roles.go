// Copyright 2026 Matrix Origin
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

package plan

import (
	"context"

	"github.com/matrixorigin/matrixone/pkg/common/moerr"
	moruntime "github.com/matrixorigin/matrixone/pkg/common/runtime"
	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/defines"
	planpb "github.com/matrixorigin/matrixone/pkg/pb/plan"
	"github.com/matrixorigin/matrixone/pkg/sql/parsers/tree"
	"github.com/matrixorigin/matrixone/pkg/vm/process"
)

var currentRolesColDefs = []*planpb.ColDef{
	{
		Name: "role_id",
		Typ: planpb.Type{
			Id: int32(types.T_int64),
		},
	},
}

func requireCurrentRolesProtocol(ctx context.Context, proc *process.Process) error {
	if proc == nil {
		return nil
	}
	rt := moruntime.ServiceRuntime(proc.GetService())
	if rt == nil {
		return moerr.NewNotSupported(
			ctx,
			"mo_current_roles requires all CNs to support protocol version 41",
		)
	}
	value, ok := rt.GetGlobalVariables(moruntime.MOProtocolVersion)
	version, valid := value.(int64)
	if !ok || !valid || version < defines.MORPCVersion41 {
		return moerr.NewNotSupported(
			ctx,
			"mo_current_roles requires all CNs to support protocol version 41",
		)
	}
	return nil
}

func (builder *QueryBuilder) buildCurrentRoles(
	tbl *tree.TableFunction,
	ctx *BindContext,
	exprs []*planpb.Expr,
	children []int32,
) (int32, error) {
	if len(tbl.Func.Exprs) != 0 {
		return 0, moerr.NewInvalidArg(builder.GetContext(),
			"mo_current_roles function has invalid input args length", len(tbl.Func.Exprs))
	}
	if err := requireCurrentRolesProtocol(builder.GetContext(), builder.compCtx.GetProcess()); err != nil {
		return 0, err
	}

	node := &planpb.Node{
		NodeType: planpb.Node_FUNCTION_SCAN,
		Stats:    &planpb.Stats{},
		TableDef: &planpb.TableDef{
			TableType: "func_table",
			TblFunc: &planpb.TableFunction{
				Name: "mo_current_roles",
			},
			Cols: DeepCopyColDefList(currentRolesColDefs),
		},
		BindingTags:     []int32{builder.genNewBindTag()},
		Children:        children,
		TblFuncExprList: exprs,
	}
	return builder.appendNode(node, ctx), nil
}
