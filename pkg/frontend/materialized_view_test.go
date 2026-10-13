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
	"context"
	"encoding/json"
	"github.com/matrixorigin/matrixone/pkg/sql/parsers/tree"
	"testing"

	"github.com/matrixorigin/matrixone/pkg/catalog"
	"github.com/matrixorigin/matrixone/pkg/pb/plan"
	"github.com/stretchr/testify/require"
)

func TestMaterializedViewReadPrivileges(t *testing.T) {
	def := catalog.MVDefinition{FormatVersion: 1, Query: "select * from src", Sources: []catalog.MVSource{{Database: "db", Table: "src"}, {Database: "db", Table: "dimension"}}}
	data, err := json.Marshal(def)
	require.NoError(t, err)
	p := &plan.Plan{Plan: &plan.Plan_Query{Query: &plan.Query{StmtType: plan.Query_SELECT, Nodes: []*plan.Node{{NodeType: plan.Node_TABLE_SCAN, ObjRef: &plan.ObjectRef{SchemaName: "db", ObjName: "stored"}, TableDef: &plan.TableDef{TableType: catalog.SystemMaterializedViewRel, Props: []*plan.PropertyDef{{Key: catalog.MaterializedViewProperty, Value: string(data)}}}}}}}}
	tips, err := extractPrivilegeTipsFromPlan(p)
	require.NoError(t, err)
	require.Len(t, tips, 3)
	for i, name := range []string{"src", "dimension", "stored"} {
		require.Equal(t, name, tips[i].tableName)
		require.Equal(t, PrivilegeTypeSelect, tips[i].typ)
		require.Equal(t, "db", tips[i].databaseName)
	}
	p.GetQuery().Nodes[0].TableDef.Props[0].Value = "invalid"
	_, err = extractPrivilegeTipsFromPlan(p)
	require.Error(t, err)
}

func TestMaterializedViewGlobalCatalogOwnership(t *testing.T) {
	for _, ddl := range catalog.MaterializedViewCatalogDDLs {
		require.True(t, isMaterializedViewCatalogDDL(ddl), "tenant initialization must not recreate a global MV catalog")
	}
}

func TestMaterializedViewTransactionClassification(t *testing.T) {
	for _, statement := range []tree.Statement{&tree.CreateMaterializedView{}, &tree.RefreshMaterializedView{}, &tree.DropMaterializedView{}, &tree.AlterMaterializedView{}} {
		allowed, err := statementCanBeExecutedInUncommittedTransaction(context.Background(), nil, statement)
		require.NoError(t, err)
		require.False(t, allowed)
	}
	allowed, err := statementCanBeExecutedInUncommittedTransaction(context.Background(), nil, &tree.ShowMaterializedViews{})
	require.NoError(t, err)
	require.True(t, allowed)
}
