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

package frontend

import (
	"testing"

	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/pb/plan"
	"github.com/stretchr/testify/require"
)

func TestShouldCachePrepareCompileForeignKeyActions(t *testing.T) {
	makePlan := func(stmtType plan.Query_StatementType, hasForeignKeyAction bool) *plan.Plan {
		return &plan.Plan{
			Plan: &plan.Plan_Query{
				Query: &plan.Query{
					StmtType:            stmtType,
					HasForeignKeyAction: hasForeignKeyAction,
				},
			},
		}
	}

	require.True(t, shouldCachePrepareCompile(nil))
	require.True(t, shouldCachePrepareCompile(&plan.Plan{}))
	require.True(t, shouldCachePrepareCompile(makePlan(plan.Query_UPDATE, false)))
	require.True(t, shouldCachePrepareCompile(makePlan(plan.Query_DELETE, false)))

	require.False(t, shouldCachePrepareCompile(makePlan(plan.Query_UPDATE, true)))
	require.False(t, shouldCachePrepareCompile(makePlan(plan.Query_DELETE, true)))
}

func TestShouldCachePrepareCompileRejectsIcebergScan(t *testing.T) {
	p := &plan.Plan{Plan: &plan.Plan_Query{Query: &plan.Query{Nodes: []*plan.Node{{
		NodeType: plan.Node_EXTERNAL_SCAN,
		ExternScan: &plan.ExternScan{
			Type:        int32(plan.ExternType_ICEBERG_TB),
			IcebergScan: &plan.IcebergScan{},
		},
	}}}}}

	require.False(t, shouldCachePrepareCompile(p))
}

func TestShouldCachePrepareCompileRejectsRuntimeTypedStringComparison(t *testing.T) {
	param := &plan.Expr{
		Typ:  plan.Type{Id: int32(types.T_varchar)},
		Expr: &plan.Expr_P{P: &plan.ParamRef{Pos: 0}},
	}
	filter := &plan.Expr{
		Typ: plan.Type{Id: int32(types.T_bool)},
		Expr: &plan.Expr_F{F: &plan.Function{
			Func: &plan.ObjectRef{ObjName: "="},
			Args: []*plan.Expr{
				{
					Typ:  plan.Type{Id: int32(types.T_varchar)},
					Expr: &plan.Expr_Col{Col: &plan.ColRef{ColPos: 0}},
				},
				{
					Typ: plan.Type{Id: int32(types.T_varchar)},
					Expr: &plan.Expr_F{F: &plan.Function{
						Func: &plan.ObjectRef{ObjName: "cast"},
						Args: []*plan.Expr{param},
					}},
				},
			},
		}},
	}
	p := &plan.Plan{Plan: &plan.Plan_Query{Query: &plan.Query{Nodes: []*plan.Node{{
		NodeType:   plan.Node_TABLE_SCAN,
		FilterList: []*plan.Expr{filter},
	}}}}}

	require.False(t, shouldCachePrepareCompile(p))

	filter.GetF().Args[1] = &plan.Expr{
		Typ:  plan.Type{Id: int32(types.T_varchar)},
		Expr: &plan.Expr_Lit{Lit: &plan.Literal{Value: &plan.Literal_Sval{Sval: "1"}}},
	}
	require.True(t, shouldCachePrepareCompile(p))
}
