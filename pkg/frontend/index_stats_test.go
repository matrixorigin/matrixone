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

package frontend

import (
	"testing"

	"github.com/matrixorigin/matrixone/pkg/pb/plan"
	"github.com/stretchr/testify/require"
)

func TestMoIndexStatsRequiresSourceTableSelectPrivilege(t *testing.T) {
	p := &plan.Plan{Plan: &plan.Plan_Query{Query: &plan.Query{
		StmtType: plan.Query_SELECT,
		Nodes: []*plan.Node{{NodeType: plan.Node_FUNCTION_SCAN,
			ObjRef: &plan.ObjectRef{SchemaName: "private_db", ObjName: "private_table"},
			TableDef: &plan.TableDef{TableType: "func_table",
				TblFunc: &plan.TableFunction{Name: "mo_index_stats"}}}},
	}}}
	tips, err := extractPrivilegeTipsFromPlan(p)
	require.NoError(t, err)
	require.Len(t, tips, 1)
	require.Equal(t, PrivilegeTypeSelect, tips[0].typ)
	require.Equal(t, "private_db", tips[0].databaseName)
	require.Equal(t, "private_table", tips[0].tableName)
}
