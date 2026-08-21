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

	"github.com/stretchr/testify/require"

	"github.com/matrixorigin/matrixone/pkg/catalog"
	planpb "github.com/matrixorigin/matrixone/pkg/pb/plan"
	"github.com/matrixorigin/matrixone/pkg/sql/parsers/dialect/mysql"
	"github.com/matrixorigin/matrixone/pkg/sql/util"
)

func TestLifecycleCleanupRootUsesExistingClusterTenantFilter(t *testing.T) {
	optimizer := NewMockOptimizer(false)
	createPlan, err := buildSingleStmt(
		optimizer,
		t,
		catalog.MoLifecycleCleanupRootsDDL,
	)
	require.NoError(t, err)
	tableDef := createPlan.GetDdl().GetCreateTable().GetTableDef()
	clusterKind := false
	for _, definition := range tableDef.GetDefs() {
		properties := definition.GetProperties()
		if properties == nil {
			continue
		}
		for _, property := range properties.GetProperties() {
			if property.GetKey() == catalog.SystemRelAttr_Kind &&
				property.GetValue() == catalog.SystemClusterRel {
				clusterKind = true
			}
		}
	}
	require.True(t, clusterKind)

	var accountColumn *planpb.ColDef
	for _, column := range tableDef.GetCols() {
		if column.GetName() == util.GetClusterTableAttributeName() {
			accountColumn = column
			break
		}
	}
	require.NotNil(t, accountColumn)
	require.True(t, accountColumn.GetNotNull())
	require.Equal(
		t,
		uint32(catalog.System_Account),
		accountColumn.GetDefault().GetExpr().GetLit().GetU32Val(),
	)

	optimizer = NewMockOptimizer(false)
	optimizer.ctxt.GetAccountIdFunc = func() (uint32, error) {
		return 17, nil
	}
	tableDef.TableType = catalog.SystemClusterRel
	optimizer.ctxt.tables[catalog.MO_LIFECYCLE_CLEANUP_ROOTS] = tableDef
	optimizer.ctxt.objects[catalog.MO_LIFECYCLE_CLEANUP_ROOTS] = &planpb.ObjectRef{
		SchemaName: catalog.MO_CATALOG,
		ObjName:    catalog.MO_LIFECYCLE_CLEANUP_ROOTS,
	}
	statement, err := mysql.ParseOne(
		optimizer.CurrentContext().GetContext(),
		"select root_id from mo_catalog.mo_lifecycle_cleanup_roots",
		1,
	)
	require.NoError(t, err)
	queryPlan, err := BuildPlan(optimizer.CurrentContext(), statement, false)
	require.NoError(t, err)

	var scan *planpb.Node
	for _, node := range queryPlan.GetQuery().GetNodes() {
		if node.GetNodeType() == planpb.Node_TABLE_SCAN &&
			node.GetTableDef().GetName() == catalog.MO_LIFECYCLE_CLEANUP_ROOTS {
			scan = node
			break
		}
	}
	require.NotNil(t, scan)
	require.Len(t, scan.GetFilterList(), 1)
	filter := scan.GetFilterList()[0].GetF()
	require.NotNil(t, filter)
	require.Equal(t, "=", filter.GetFunc().GetObjName())
	require.Len(t, filter.GetArgs(), 2)
	require.Equal(
		t,
		catalog.MO_LIFECYCLE_CLEANUP_ROOTS+"."+util.GetClusterTableAttributeName(),
		filter.GetArgs()[0].GetCol().GetName(),
	)
	require.Equal(t, uint32(17), filter.GetArgs()[1].GetLit().GetU32Val())
}
