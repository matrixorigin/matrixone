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

	"github.com/matrixorigin/matrixone/pkg/pb/plan"
	"github.com/stretchr/testify/require"
)

func TestShouldCachePrepareCompileForeignKeyActions(t *testing.T) {
	makePlan := func(stmtType plan.Query_StatementType, fk *plan.ForeignKeyDef) *plan.Plan {
		return &plan.Plan{
			Plan: &plan.Plan_Query{
				Query: &plan.Query{
					StmtType: stmtType,
					Nodes: []*plan.Node{
						{
							NodeType: plan.Node_MULTI_UPDATE,
							UpdateCtxList: []*plan.UpdateCtx{
								{
									TableDef: &plan.TableDef{
										Fkeys: []*plan.ForeignKeyDef{fk},
									},
								},
							},
						},
					},
				},
			},
		}
	}

	require.True(t, shouldCachePrepareCompile(makePlan(plan.Query_SELECT, &plan.ForeignKeyDef{
		OnDelete: plan.ForeignKeyDef_SET_NULL,
		OnUpdate: plan.ForeignKeyDef_CASCADE,
	})))
	require.True(t, shouldCachePrepareCompile(makePlan(plan.Query_DELETE, &plan.ForeignKeyDef{
		OnDelete: plan.ForeignKeyDef_RESTRICT,
	})))
	require.True(t, shouldCachePrepareCompile(makePlan(plan.Query_UPDATE, &plan.ForeignKeyDef{
		OnUpdate: plan.ForeignKeyDef_NO_ACTION,
	})))

	require.False(t, shouldCachePrepareCompile(makePlan(plan.Query_DELETE, &plan.ForeignKeyDef{
		OnDelete: plan.ForeignKeyDef_SET_NULL,
	})))
	require.False(t, shouldCachePrepareCompile(makePlan(plan.Query_UPDATE, &plan.ForeignKeyDef{
		OnUpdate: plan.ForeignKeyDef_CASCADE,
	})))
}
