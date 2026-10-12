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
	"testing"

	"github.com/matrixorigin/matrixone/pkg/container/types"
	planpb "github.com/matrixorigin/matrixone/pkg/pb/plan"
	"github.com/matrixorigin/matrixone/pkg/sql/parsers"
	"github.com/matrixorigin/matrixone/pkg/sql/parsers/dialect"
	"github.com/matrixorigin/matrixone/pkg/sql/parsers/tree"
	"github.com/stretchr/testify/require"
)

const issue29849LoadSQL = "LOAD DATA INLINE FORMAT='csv', DATA='1,2,alpha,NULL' INTO TABLE nation FIELDS TERMINATED BY ',' (n_nationkey, n_regionkey, n_name, @v) SET n_comment=NULLIF(@v, 'NULL')"

func TestIssue29849LoadDataSetNullIfUserVariable(t *testing.T) {
	modernPlan, err := buildMySQLDMLCompatibilityPlan(t, issue29849LoadSQL)
	require.NoError(t, err)
	assertIssue29849LoadUserVariablePlan(t, modernPlan)

	ctx := NewMockCompilerContext(true, newPlanTestProcess(t))
	statement, err := parsers.ParseOne(ctx.GetContext(), dialect.MYSQL, issue29849LoadSQL, 1)
	require.NoError(t, err)
	defer statement.Free()
	load, ok := statement.(*tree.Load)
	require.True(t, ok)
	// The legacy LOAD DATA builder is normally entered after DML resolution
	// has populated this index. Seed it in this direct builder test to match
	// that precondition.
	nation := ctx.tables["nation"]
	require.NotNil(t, nation)
	nation.Name2ColIndex = make(map[string]int32, len(nation.Cols))
	for i, column := range nation.Cols {
		nation.Name2ColIndex[column.Name] = int32(i)
	}

	legacyPlan, err := buildLoad(load, ctx, false)
	require.NoError(t, err)
	assertIssue29849LoadUserVariablePlan(t, legacyPlan)
}

func TestIssue29849LoadDataSetNullIfUserVariableSuppliesNotNullTarget(t *testing.T) {
	ctx := NewMockCompilerContext(true, newPlanTestProcess(t))
	statement, err := parsers.ParseOne(ctx.GetContext(), dialect.MYSQL, issue29849LoadSQL, 1)
	require.NoError(t, err)
	defer statement.Free()
	load, ok := statement.(*tree.Load)
	require.True(t, ok)

	// The legacy builder is normally entered after DML resolution has populated
	// this index. Give the target no default and a NOT NULL constraint so the
	// SET expression must count as the value supplied for its omitted column.
	nation := ctx.tables["nation"]
	require.NotNil(t, nation)
	nation.Name2ColIndex = make(map[string]int32, len(nation.Cols))
	for i, column := range nation.Cols {
		nation.Name2ColIndex[column.Name] = int32(i)
	}
	nationComment := nation.Cols[nation.Name2ColIndex["n_comment"]]
	nationComment.Typ.NotNullable = true
	nationComment.Default = nil

	legacyPlan, err := buildLoad(load, ctx, false)
	require.NoError(t, err)
	assertIssue29849LoadUserVariablePlan(t, legacyPlan)
}

func assertIssue29849LoadUserVariablePlan(t *testing.T, plan *planpb.Plan) {
	t.Helper()
	query := plan.GetQuery()
	require.NotNil(t, query)
	require.True(t, query.LoadTag)

	var scanIndex int
	var externalScan *planpb.Node
	for i, node := range query.Nodes {
		if node.NodeType == planpb.Node_EXTERNAL_SCAN {
			scanIndex = i
			externalScan = node
			break
		}
	}
	require.NotNil(t, externalScan)
	require.Len(t, externalScan.ExternScan.TbColToDataCol, 4)
	require.Equal(t, int32(0), externalScan.ExternScan.TbColToDataCol["n_nationkey"])
	require.Equal(t, int32(1), externalScan.ExternScan.TbColToDataCol["n_regionkey"])
	require.Equal(t, int32(2), externalScan.ExternScan.TbColToDataCol["n_name"])

	var rawColumn string
	for name, inputPosition := range externalScan.ExternScan.TbColToDataCol {
		if strings.HasPrefix(name, "__mo_load_user_var_") {
			rawColumn = name
			require.Equal(t, int32(3), inputPosition)
		}
	}
	require.NotEmpty(t, rawColumn)
	require.Len(t, externalScan.TableDef.Cols, 4)
	require.Equal(t, rawColumn, externalScan.TableDef.Cols[3].Name)
	require.Equal(t, int32(types.T_varchar), externalScan.TableDef.Cols[3].Typ.Id)

	var loadProject *planpb.Node
	for _, node := range query.Nodes {
		if node.NodeType == planpb.Node_PROJECT && len(node.Children) == 1 && node.Children[0] == int32(scanIndex) {
			loadProject = node
			break
		}
	}
	require.NotNil(t, loadProject)
	require.Len(t, loadProject.ProjectList, 4)
	assignment := loadProject.ProjectList[3].GetF()
	require.Equal(t, "cast_assign", assignment.GetFunc().GetObjName())
	nullIf := assignment.GetArgs()[0].GetF()
	require.Equal(t, "case", nullIf.GetFunc().GetObjName())
	comparison := nullIf.GetArgs()[0].GetF()
	require.Equal(t, "=", comparison.GetFunc().GetObjName())
	require.Equal(t, int32(3), comparison.GetArgs()[0].GetCol().GetColPos())
	require.Equal(t, "NULL", comparison.GetArgs()[1].GetLit().GetSval())
	require.Equal(t, int32(3), nullIf.GetArgs()[2].GetCol().GetColPos())
}
