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
	"testing"

	"github.com/matrixorigin/matrixone/pkg/common/objectkey"
	"github.com/matrixorigin/matrixone/pkg/common/runtime"
	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/defines"
	planpb "github.com/matrixorigin/matrixone/pkg/pb/plan"
	"github.com/matrixorigin/matrixone/pkg/sql/parsers/tree"
	"github.com/matrixorigin/matrixone/pkg/util/sysview"
	"github.com/stretchr/testify/require"
)

func TestBuildSubscriptionMetadataPersistedSystemViews(t *testing.T) {
	for _, ddl := range []string{
		sysview.InformationSchemaTablesDDL,
		sysview.InformationSchemaColumnsDDL,
	} {
		logicPlan, err := runOneStmt(NewMockOptimizer(false), t, ddl)
		require.NoError(t, err)
		require.NotNil(t, logicPlan.GetDdl())
	}
}

func TestBuildSubscriptionTablesUsesOneReachableProducer(t *testing.T) {
	logicPlan, err := runOneStmt(
		NewMockOptimizer(false),
		t,
		"SELECT * FROM information_schema.TABLES",
	)
	require.NoError(t, err)
	query := logicPlan.GetQuery()
	require.NotNil(t, query)
	require.Equal(t, 1, countReachableTableFunction(query, subscriptionTablesFunctionName))
	require.Equal(t, 1, countReachableTableFunction(query, "mo_current_roles"))
}

func TestBuildSubscriptionMetadataProtocolAndViewGate(t *testing.T) {
	mock := NewMockOptimizer(false)
	builder := NewQueryBuilder(planpb.Query_SELECT, mock.CurrentContext(), false, true)
	proc := builder.compCtx.GetProcess()
	rt := runtime.ServiceRuntime(proc.GetService())
	original, hadOriginal := rt.GetGlobalVariables(runtime.MOProtocolVersion)
	t.Cleanup(func() {
		if hadOriginal {
			rt.SetGlobalVariables(runtime.MOProtocolVersion, original)
		} else {
			rt.SetGlobalVariables(runtime.MOProtocolVersion, defines.MORPCLatestVersion)
		}
	})

	tableFunction := &tree.TableFunction{Func: &tree.FuncExpr{}}
	tablesCtx := NewBindContext(builder, nil)
	tablesCtx.viewChain = []string{objectkey.Encode("information_schema", "TABLES")}

	rt.SetGlobalVariables(runtime.MOProtocolVersion, defines.MORPCVersion45)
	_, err := builder.buildSubscriptionTables(tableFunction, tablesCtx, nil, nil)
	require.ErrorContains(t, err, "protocol version 46")

	rt.SetGlobalVariables(runtime.MOProtocolVersion, defines.MORPCVersion46)
	nodeID, err := builder.buildSubscriptionTables(tableFunction, tablesCtx, nil, nil)
	require.NoError(t, err)
	node := builder.qry.Nodes[nodeID]
	require.Equal(t, planpb.Node_FUNCTION_SCAN, node.NodeType)
	require.Equal(t, subscriptionTablesFunctionName, node.TableDef.TblFunc.Name)
	require.Equal(t, "account_id", node.TableDef.Cols[0].Name)
	require.Equal(t, int32(types.T_uint32), node.TableDef.Cols[0].Typ.Id)
	require.Equal(t, "owner", node.TableDef.Cols[len(node.TableDef.Cols)-1].Name)

	directCtx := NewBindContext(builder, nil)
	_, err = builder.buildSubscriptionTables(tableFunction, directCtx, nil, nil)
	require.ErrorContains(t, err, "private to information_schema metadata views")

	userViewCtx := NewBindContext(builder, nil)
	userViewCtx.viewChain = []string{objectkey.Encode("app", "metadata")}
	_, err = builder.buildSubscriptionTables(tableFunction, userViewCtx, nil, nil)
	require.ErrorContains(t, err, "private to information_schema metadata views")

	columnsCtx := NewBindContext(builder, nil)
	columnsCtx.viewChain = []string{
		objectkey.Encode("app", "wrapper"),
		objectkey.Encode("INFORMATION_SCHEMA", "columns"),
	}
	columnsNodeID, err := builder.buildSubscriptionColumns(tableFunction, columnsCtx, nil, nil)
	require.NoError(t, err)
	columnsNode := builder.qry.Nodes[columnsNodeID]
	require.Equal(t, subscriptionColumnsFunctionName, columnsNode.TableDef.TblFunc.Name)
	require.Equal(t, "key_priority", columnsNode.TableDef.Cols[17].Name)
	require.Equal(t, int32(types.T_int64), columnsNode.TableDef.Cols[17].Typ.Id)
	require.Equal(t, "table_owner", columnsNode.TableDef.Cols[len(columnsNode.TableDef.Cols)-1].Name)
	require.Equal(t, int32(types.T_uint32), columnsNode.TableDef.Cols[len(columnsNode.TableDef.Cols)-1].Typ.Id)

	_, err = builder.buildSubscriptionTables(tableFunction, columnsCtx, nil, nil)
	require.ErrorContains(t, err, "private to information_schema metadata views")

	_, err = builder.buildSubscriptionColumns(tableFunction, tablesCtx, nil, nil)
	require.ErrorContains(t, err, "private to information_schema metadata views")

	installCtx := NewBindContext(builder, nil)
	builder.persistedViewTarget = objectkey.Encode("information_schema", "tables")
	_, err = builder.buildSubscriptionTables(tableFunction, installCtx, nil, nil)
	require.NoError(t, err, "installing the exact owning system view must be admitted")
	_, err = builder.buildSubscriptionColumns(tableFunction, installCtx, nil, nil)
	require.ErrorContains(t, err, "private to information_schema metadata views")

	userInstallCtx := NewBindContext(builder, nil)
	builder.persistedViewTarget = objectkey.Encode("app", "tables")
	_, err = builder.buildSubscriptionTables(tableFunction, userInstallCtx, nil, nil)
	require.ErrorContains(t, err, "private to information_schema metadata views")

	builder.persistedViewTarget = objectkey.Encode("information_schema", "tables")
	detachedInstallCtx := NewBindContext(builder, nil)
	_, err = builder.buildSubscriptionTables(tableFunction, detachedInstallCtx, nil, nil)
	require.NoError(t, err, "detached CTE contexts must retain the statement-local trusted owner")
	builder.persistedViewTarget = ""

	tableFunction.Func.Exprs = tree.Exprs{tree.NewNumVal(int64(1), "1", false, tree.P_int64)}
	_, err = builder.buildSubscriptionTables(tableFunction, tablesCtx, nil, nil)
	require.ErrorContains(t, err, "invalid input args length")
}
