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
	"testing"

	planpb "github.com/matrixorigin/matrixone/pkg/pb/plan"
	"github.com/matrixorigin/matrixone/pkg/sql/parsers"
	"github.com/matrixorigin/matrixone/pkg/sql/parsers/dialect"
	"github.com/matrixorigin/matrixone/pkg/sql/parsers/tree"
	"github.com/stretchr/testify/require"
)

func testInsertAliasTable() *planpb.TableDef {
	return &planpb.TableDef{
		Name: "t",
		Cols: []*planpb.ColDef{
			{Name: "id"},
			{Name: "a"},
			{Name: "b"},
		},
		Name2ColIndex: map[string]int32{"id": 0, "a": 1, "b": 2},
	}
}

func testInsertAliasName(parts ...string) *tree.UnresolvedName {
	cstrs := make([]*tree.CStr, len(parts))
	for i, part := range parts {
		cstrs[i] = tree.NewCStr(part, 1)
	}
	return tree.NewUnresolvedName(cstrs...)
}

func testInsertAliasNameWithTableCase(table, column string, lowerCaseTableNames int64) *tree.UnresolvedName {
	return tree.NewUnresolvedName(tree.NewCStr(table, lowerCaseTableNames), tree.NewCStr(column, 1))
}

func TestInsertRowAliasBindingMapsTargetIdentity(t *testing.T) {
	tableDef := testInsertAliasTable()
	binding, err := validateInsertRowAlias(
		context.Background(),
		&tree.AliasClause{Alias: "n", Cols: tree.IdentifierList{"x", "y"}},
		[]string{"b", "a"}, tableDef, "db", "t", 1,
	)
	require.NoError(t, err)
	require.Equal(t, insertRowAliasColumn{targetIdx: 2, incomingPos: 2}, binding.cols["x"])
	require.Equal(t, insertRowAliasColumn{targetIdx: 1, incomingPos: 1}, binding.cols["y"])

	require.NoError(t, binding.remapIncomingPositions(context.Background(), tableDef, map[string]int32{
		"t.a": 4,
		"t.b": 3,
	}))
	require.Equal(t, 3, binding.cols["x"].incomingPos)
	require.Equal(t, 4, binding.cols["y"].incomingPos)
}

func TestInsertRowAliasBindingRemapsGeneratedColumnByIdentity(t *testing.T) {
	tableDef := testInsertAliasTable()
	tableDef.Cols = append(tableDef.Cols, &planpb.ColDef{
		Name:         "g",
		GeneratedCol: &planpb.GeneratedCol{},
	})
	tableDef.Name2ColIndex["g"] = 3
	binding, err := validateInsertRowAlias(
		context.Background(),
		&tree.AliasClause{Alias: "n", Cols: tree.IdentifierList{"id", "g"}},
		[]string{"id", "g"}, tableDef, "db", "t", 1,
	)
	require.NoError(t, err)
	require.NoError(t, binding.remapIncomingPositions(context.Background(), tableDef, map[string]int32{
		"t.id": 5,
		"t.g":  2,
	}))
	require.Equal(t, 5, binding.cols["id"].incomingPos)
	require.Equal(t, 2, binding.cols["g"].incomingPos)
}

func TestInsertRowAliasBinderResolvesIncomingAndTargetRows(t *testing.T) {
	tableDef := testInsertAliasTable()
	binding, err := validateInsertRowAlias(
		context.Background(),
		&tree.AliasClause{Alias: "n", Cols: tree.IdentifierList{"x", "y"}},
		[]string{"b", "a"}, tableDef, "db", "t", 1,
	)
	require.NoError(t, err)
	binder := NewOndupUpdateBinder(context.Background(), nil, nil, 11, 7, tableDef, "db", "t", 1, binding)

	expr, err := binder.BindColRef(testInsertAliasName("n", "x"), 0, true)
	require.NoError(t, err)
	require.Equal(t, int32(7), expr.GetCol().RelPos)
	require.Equal(t, int32(2), expr.GetCol().ColPos)

	expr, err = binder.BindColRef(testInsertAliasName("t", "a"), 0, true)
	require.NoError(t, err)
	require.Equal(t, int32(11), expr.GetCol().RelPos)
	require.Equal(t, int32(1), expr.GetCol().ColPos)

	expr, err = binder.BindColRef(testInsertAliasName("n", "x"), 2, true)
	require.NoError(t, err)
	require.Equal(t, int32(2), expr.GetCorr().Depth)
	require.Equal(t, int32(7), expr.GetCorr().RelPos)
	require.Equal(t, int32(2), expr.GetCorr().ColPos)
}

func TestInsertRowAliasBinderUsesVisibleTargetForCorrelation(t *testing.T) {
	tableDef := testInsertAliasTable()
	binding, err := validateInsertRowAlias(
		context.Background(),
		&tree.AliasClause{Alias: "n", Cols: tree.IdentifierList{"id", "x", "y"}},
		[]string{"id", "a", "b"}, tableDef, "db", "t", 1,
	)
	require.NoError(t, err)
	binder := NewOndupUpdateBinder(context.Background(), nil, nil, 11, 7, tableDef, "db", "t", 1, binding)
	binder.SetTargetCorrelationTag(13)

	expr, err := binder.BindColRef(testInsertAliasName("t", "a"), 1, true)
	require.NoError(t, err)
	require.Equal(t, int32(1), expr.GetCorr().Depth)
	require.Equal(t, int32(13), expr.GetCorr().RelPos)
	require.Equal(t, int32(1), expr.GetCorr().ColPos)
}

func TestInsertRowAliasCorrelatedFromBuildPlanKeepsTargetLookupReachable(t *testing.T) {
	logicPlan, err := runOneStmt(NewMockOptimizer(true), t,
		"insert into constraint_test.dept(deptno, dname, loc) values (1, 'Sales', 'NY') as n(id, name, location) "+
			"on duplicate key update loc = (select e.ename from constraint_test.emp as e "+
			"where e.deptno = constraint_test.dept.deptno)")
	require.NoError(t, err)

	targetScans := 0
	for _, node := range logicPlan.GetQuery().Nodes {
		if node.NodeType == planpb.Node_TABLE_SCAN && node.TableDef != nil && node.TableDef.Name == "dept" {
			targetScans++
		}
	}
	// The target arbitration and DEDUP scans are siblings.  A third target
	// scan proves the correlated lookup was attached below the candidate side
	// before flattening the scalar subquery.
	require.GreaterOrEqual(t, targetScans, 3)
}

func TestInsertRowAliasGeneratedDefaultNoKeyFallbackBuilds(t *testing.T) {
	logicPlan, err := runOneStmt(NewMockOptimizer(true), t,
		"insert into constraint_test.fake_pk_no_unique_gen(a, g) values (1, default) as n(x, y) "+
			"on duplicate key update a = n.x")
	require.NoError(t, err)
	require.NotNil(t, logicPlan)
}

func TestInsertRowAliasBinderRejectsAmbiguousAndInvalidNames(t *testing.T) {
	tableDef := testInsertAliasTable()
	binding, err := validateInsertRowAlias(
		context.Background(),
		&tree.AliasClause{Alias: "n"}, []string{"a", "b"}, tableDef, "db", "t", 1,
	)
	require.NoError(t, err)
	binder := NewOndupUpdateBinder(context.Background(), nil, nil, 11, 7, tableDef, "db", "t", 1, binding)
	_, err = binder.BindColRef(testInsertAliasName("a"), 0, true)
	require.Error(t, err)
	require.Contains(t, err.Error(), "ambiguous")

	for _, alias := range []*tree.AliasClause{
		{Alias: "t"},
		{Alias: "n", Cols: tree.IdentifierList{"x", "x"}},
		{Alias: "n", Cols: tree.IdentifierList{"x"}},
	} {
		_, err = validateInsertRowAlias(context.Background(), alias, []string{"a", "b"}, tableDef, "db", "t", 1)
		require.Error(t, err)
	}
}

func TestInsertRowAliasColumnNamesIgnoreTableCaseSetting(t *testing.T) {
	tableDef := testInsertAliasTable()
	binding, err := validateInsertRowAlias(
		context.Background(),
		&tree.AliasClause{Alias: "N", Cols: tree.IdentifierList{"K", "X", "Y"}},
		[]string{"id", "a", "b"}, tableDef, "db", "t", 0,
	)
	require.NoError(t, err)
	require.Equal(t, "N", binding.name)
	require.Contains(t, binding.cols, "x")
	binder := NewOndupUpdateBinder(context.Background(), nil, nil, 11, 7, tableDef, "db", "t", 0, binding)
	// The parser stores column identifier parts with the column normalization
	// (lower=1), even when lower_case_table_names=0.
	expr, err := binder.BindColRef(testInsertAliasNameWithTableCase("N", "X", 0), 0, true)
	require.NoError(t, err)
	require.Equal(t, int32(7), expr.GetCol().RelPos)
	require.Equal(t, int32(1), expr.GetCol().ColPos)

	_, err = validateInsertRowAlias(
		context.Background(),
		&tree.AliasClause{Alias: "n", Cols: tree.IdentifierList{"X", "x", "Y"}},
		[]string{"id", "a", "b"}, tableDef, "db", "t", 0,
	)
	require.Error(t, err)
	require.ErrorContains(t, err, "column name")
}

func TestInsertRowAliasFallbackCollectsNestedParameters(t *testing.T) {
	stmt, err := parsers.ParseOne(
		context.Background(), dialect.MYSQL,
		"insert into t values (1) as n on duplicate key update a = case when ? then n.a + ? else (select ?) end", 1,
	)
	require.NoError(t, err)
	insert := stmt.(*tree.Insert)
	require.Equal(t, []int{1, 2, 3}, collectParamExprOffsets(insert.OnDuplicateUpdate[0].Expr))
}
