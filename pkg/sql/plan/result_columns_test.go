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
	"fmt"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/matrixorigin/matrixone/pkg/container/types"
	planpb "github.com/matrixorigin/matrixone/pkg/pb/plan"
)

func TestGetResultColumnsFromPlanCarriesConstraintMetadata(t *testing.T) {
	tableDef := &planpb.TableDef{
		Name: "t",
		Pkey: &planpb.PrimaryKeyDef{Names: []string{"id"}},
		Cols: []*planpb.ColDef{
			{
				Name:    "id",
				Primary: true,
				Typ: planpb.Type{
					Id:          int32(types.T_int32),
					NotNullable: true,
					AutoIncr:    true,
				},
			},
			{Name: "uk", Typ: planpb.Type{Id: int32(types.T_int32)}, NotNull: true},
			{Name: "nullable", Typ: planpb.Type{Id: int32(types.T_int32)}},
		},
		Indexes: []*planpb.IndexDef{{IndexName: "uk_t", Parts: []string{"uk"}, Unique: true}},
	}

	scanProjects := make([]*planpb.Expr, len(tableDef.Cols))
	for i, col := range tableDef.Cols {
		scanProjects[i] = &planpb.Expr{
			Typ: col.Typ,
			Expr: &planpb.Expr_Col{Col: &planpb.ColRef{
				ColPos: int32(i),
				Name:   "t." + col.Name,
			}},
		}
	}
	resultProjects := make([]*planpb.Expr, len(tableDef.Cols))
	for i, col := range tableDef.Cols {
		resultProjects[i] = &planpb.Expr{
			Typ: col.Typ,
			Expr: &planpb.Expr_Col{Col: &planpb.ColRef{
				ColPos:  int32(i),
				Name:    "t." + col.Name,
				TblName: "t",
			}},
		}
	}

	got := GetResultColumnsFromPlan(&planpb.Plan{Plan: &planpb.Plan_Query{Query: &planpb.Query{
		StmtType: planpb.Query_SELECT,
		Steps:    []int32{1},
		Nodes: []*planpb.Node{
			{NodeId: 0, NodeType: planpb.Node_TABLE_SCAN, TableDef: tableDef, ProjectList: scanProjects},
			{NodeId: 1, NodeType: planpb.Node_PROJECT, Children: []int32{0}, ProjectList: resultProjects},
		},
		Headings: []string{"id", "uk", "nullable"},
	}}})

	require.Len(t, got, 3)
	require.True(t, got[0].Primary)
	require.False(t, got[0].Unique)
	require.True(t, got[0].NotNull)
	require.True(t, got[0].Typ.NotNullable)
	require.True(t, got[0].Typ.AutoIncr)
	require.False(t, got[1].Primary)
	require.True(t, got[1].Unique)
	require.True(t, got[1].NotNull)
	require.False(t, got[2].Primary)
	require.False(t, got[2].Unique)
	require.False(t, got[2].NotNull)

	wire, err := got[1].Marshal()
	require.NoError(t, err)
	var roundTrip planpb.ColDef
	require.NoError(t, roundTrip.Unmarshal(wire))
	require.True(t, roundTrip.Unique)
}

func TestGetResultColumnsFromPlanPreservesReorderedConstraintMetadata(t *testing.T) {
	tableDef := &planpb.TableDef{
		Name: "t",
		Pkey: &planpb.PrimaryKeyDef{Names: []string{"id"}},
		Cols: []*planpb.ColDef{
			{
				Name:    "id",
				Primary: true,
				Typ: planpb.Type{
					Id:          int32(types.T_int32),
					NotNullable: true,
					AutoIncr:    true,
				},
			},
			{Name: "unique_value", Typ: planpb.Type{Id: int32(types.T_int32)}, NotNull: true},
		},
		Indexes: []*planpb.IndexDef{{IndexName: "uk_t", Parts: []string{"unique_value"}, Unique: true}},
	}

	const scanRelPos = int32(7)
	scanProjects := []*planpb.Expr{
		{Typ: tableDef.Cols[0].Typ, Expr: &planpb.Expr_Col{Col: &planpb.ColRef{
			RelPos: scanRelPos, ColPos: 0, Name: "t.id", TblName: "t",
		}}},
		{Typ: tableDef.Cols[1].Typ, Expr: &planpb.Expr_Col{Col: &planpb.ColRef{
			RelPos: scanRelPos, ColPos: 1, Name: "t.unique_value", TblName: "t",
		}}},
	}
	// The output order differs from the source order. In particular, ColPos=1
	// is the first output expression, not the second entry in ProjectList.
	resultProjects := []*planpb.Expr{
		{Typ: tableDef.Cols[1].Typ, Expr: &planpb.Expr_Col{Col: &planpb.ColRef{
			RelPos: scanRelPos, ColPos: 1, Name: "t.unique_value", TblName: "t",
		}}},
		{Typ: tableDef.Cols[0].Typ, Expr: &planpb.Expr_Col{Col: &planpb.ColRef{
			RelPos: scanRelPos, ColPos: 0, Name: "t.id", TblName: "t",
		}}},
	}

	got := GetResultColumnsFromPlan(&planpb.Plan{Plan: &planpb.Plan_Query{Query: &planpb.Query{
		StmtType: planpb.Query_SELECT,
		Steps:    []int32{1},
		Nodes: []*planpb.Node{
			{NodeId: 0, NodeType: planpb.Node_TABLE_SCAN, TableDef: tableDef, ProjectList: scanProjects},
			{NodeId: 1, NodeType: planpb.Node_PROJECT, Children: []int32{0}, ProjectList: resultProjects},
		},
		Headings: []string{"unique_value", "id"},
	}}})

	require.Len(t, got, 2)
	require.False(t, got[0].Primary)
	require.True(t, got[0].Unique)
	require.True(t, got[0].NotNull)
	require.False(t, got[0].Typ.AutoIncr)
	require.True(t, got[1].Primary)
	require.False(t, got[1].Unique)
	require.True(t, got[1].NotNull)
	require.True(t, got[1].Typ.AutoIncr)
}

func TestGetResultColumnsFromPlanCarriesJoinedColumnMetadata(t *testing.T) {
	mock := NewMockOptimizer(false, newPlanTestProcess(t))
	logicalPlan, err := runOneStmt(mock, t,
		"select n.n_nationkey from nation n join region r on n.n_regionkey = r.r_regionkey")
	require.NoError(t, err)

	columns := GetResultColumnsFromPlan(logicalPlan)
	require.Len(t, columns, 1)
	require.True(t, columns[0].Primary)
	require.True(t, columns[0].NotNull)
	require.True(t, columns[0].Typ.NotNullable)
}

func TestGetResultColumnsFromPlanClearsNotNullForOuterJoinedColumn(t *testing.T) {
	mock := NewMockOptimizer(false, newPlanTestProcess(t))
	logicalPlan, err := runOneStmt(mock, t,
		"select r.r_regionkey from nation n left join region r on n.n_regionkey = r.r_regionkey")
	require.NoError(t, err)

	columns := GetResultColumnsFromPlan(logicalPlan)
	require.Len(t, columns, 1)
	require.True(t, columns[0].Primary)
	require.False(t, columns[0].NotNull)
	require.False(t, columns[0].Typ.NotNullable)
}

func TestGetResultColumnsFromPlanCarriesOriginMetadata(t *testing.T) {
	mock := NewMockOptimizer(false, newPlanTestProcess(t))
	logicalPlan, err := runOneStmt(mock, t,
		"select n.n_nationkey as key_alias from nation as n")
	require.NoError(t, err)

	columns := GetResultColumnsFromPlan(logicalPlan)
	require.Len(t, columns, 1)
	require.Equal(t, "key_alias", columns[0].Name)
	require.Equal(t, "n_nationkey", columns[0].OriginName)
	require.Equal(t, "n", columns[0].TblName)
	require.Equal(t, "nation", columns[0].OriginTblName)
	require.Equal(t, "tpch", columns[0].DbName)

	wire, err := columns[0].Marshal()
	require.NoError(t, err)
	var roundTrip planpb.ColDef
	require.NoError(t, roundTrip.Unmarshal(wire))
	require.Equal(t, columns[0].OriginTblName, roundTrip.OriginTblName)
}

func TestGetResultColumnsFromPlanLeavesComputedOriginEmpty(t *testing.T) {
	mock := NewMockOptimizer(false, newPlanTestProcess(t))
	logicalPlan, err := runOneStmt(mock, t,
		"select n.n_nationkey + 1 as derived_key from nation as n")
	require.NoError(t, err)

	columns := GetResultColumnsFromPlan(logicalPlan)
	require.Len(t, columns, 1)
	require.Empty(t, columns[0].OriginName)
	require.Empty(t, columns[0].TblName)
	require.Empty(t, columns[0].OriginTblName)
	require.Empty(t, columns[0].DbName)
}

func TestGetResultColumnsFromPlanPreservesBooleanExpressionKind(t *testing.T) {
	mock := NewMockOptimizer(false, newPlanTestProcess(t))
	for _, tc := range []struct {
		name string
		sql  string
		kind planpb.ResultColumnKind
	}{
		{name: "native", sql: "select with_grant_option from mo_catalog.mo_role_privs", kind: planpb.ResultColumnKind_RESULT_COLUMN_KIND_STORED},
		{name: "distinct native", sql: "select distinct with_grant_option from mo_catalog.mo_role_privs", kind: planpb.ResultColumnKind_RESULT_COLUMN_KIND_STORED},
		{name: "distinct computed", sql: "select distinct n_nationkey is null as p from nation", kind: planpb.ResultColumnKind_RESULT_COLUMN_KIND_COMPUTED},
		{name: "aggregate min computed", sql: "select min(n_nationkey is null) as p from nation", kind: planpb.ResultColumnKind_RESULT_COLUMN_KIND_COMPUTED},
		{name: "aggregate max computed", sql: "select max(n_nationkey is null) as p from nation", kind: planpb.ResultColumnKind_RESULT_COLUMN_KIND_COMPUTED},
		{name: "window max computed", sql: "select max(n_nationkey is null) over () as p from nation", kind: planpb.ResultColumnKind_RESULT_COLUMN_KIND_COMPUTED},
		{name: "distinct window max computed", sql: "select distinct max(n_nationkey is null) over () as p from nation", kind: planpb.ResultColumnKind_RESULT_COLUMN_KIND_COMPUTED},
		{name: "window min computed", sql: "select min(n_nationkey is null) over () as p from nation", kind: planpb.ResultColumnKind_RESULT_COLUMN_KIND_COMPUTED},
		{name: "window max native input computed", sql: "select max(with_grant_option) over () as p from mo_catalog.mo_role_privs", kind: planpb.ResultColumnKind_RESULT_COLUMN_KIND_COMPUTED},
		{name: "window first computed", sql: "select first_value(n_nationkey is null) over (order by n_nationkey) as p from nation", kind: planpb.ResultColumnKind_RESULT_COLUMN_KIND_COMPUTED},
		{name: "window first native", sql: "select first_value(with_grant_option) over () as p from mo_catalog.mo_role_privs", kind: planpb.ResultColumnKind_RESULT_COLUMN_KIND_STORED},
		{name: "union native", sql: "select with_grant_option from mo_catalog.mo_role_privs union all select with_grant_option from mo_catalog.mo_role_privs", kind: planpb.ResultColumnKind_RESULT_COLUMN_KIND_STORED},
		{name: "derived native", sql: "select x from (select with_grant_option x from mo_catalog.mo_role_privs) u", kind: planpb.ResultColumnKind_RESULT_COLUMN_KIND_STORED},
		{name: "computed", sql: "select with_grant_option = 1 from mo_catalog.mo_role_privs", kind: planpb.ResultColumnKind_RESULT_COLUMN_KIND_COMPUTED},
		{name: "derived computed", sql: "select x from (select with_grant_option = 1 x from mo_catalog.mo_role_privs) u", kind: planpb.ResultColumnKind_RESULT_COLUMN_KIND_COMPUTED},
	} {
		t.Run(tc.name, func(t *testing.T) {
			p, err := runOneStmt(mock, t, tc.sql)
			require.NoError(t, err)
			cols := GetResultColumnsFromPlan(p)
			require.Len(t, cols, 1)
			require.Equal(t, tc.kind, cols[0].ResultColumnKind)
		})
	}
}

func TestGetResultColumnsFromPlanPreservesJoinedBooleanProvenance(t *testing.T) {
	mock := NewMockOptimizer(false, newPlanTestProcess(t))
	for _, barrier := range []string{"distinct", "group by"} {
		for _, nativeSide := range []string{"a", "b"} {
			for _, join := range []string{"cross join", "inner join", "left join", "right join"} {
				for _, projection := range []string{"a.v, b.v", "b.v, a.v"} {
					for _, outerDistinct := range []bool{false, true} {
						name := fmt.Sprintf("%s/native_%s/%s/%s/distinct_%t", barrier, nativeSide, join, projection, outerDistinct)
						t.Run(name, func(t *testing.T) {
							derived := func(side string) string {
								expr := "role_id is null"
								if side == nativeSide {
									expr = "with_grant_option"
								}
								if barrier == "distinct" {
									return "(select distinct " + expr + " as v from mo_catalog.mo_role_privs) " + side
								}
								return "(select " + expr + " as v from mo_catalog.mo_role_privs group by " + expr + ") " + side
							}
							sql := "select "
							if outerDistinct {
								sql += "distinct "
							}
							sql += projection + " from " + derived("a") + " " + join + " " + derived("b")
							if join != "cross join" {
								sql += " on a.v = b.v"
							}
							p, err := runOneStmt(mock, t, sql)
							require.NoError(t, err)
							columns := GetResultColumnsFromPlan(p)
							require.Len(t, columns, 2)
							firstSide := "a"
							if projection == "b.v, a.v" {
								firstSide = "b"
							}
							for i, column := range columns {
								native := (firstSide == nativeSide) == (i == 0)
								want := planpb.ResultColumnKind_RESULT_COLUMN_KIND_COMPUTED
								if native {
									want = planpb.ResultColumnKind_RESULT_COLUMN_KIND_STORED
								}
								require.Equal(t, want, column.ResultColumnKind, "column %d: %s", i, sql)
								// Value provenance crosses these barriers, but key/origin
								// metadata must not be restored through DISTINCT/GROUP BY.
								require.Empty(t, column.OriginName)
								require.False(t, column.Primary)
								require.False(t, column.Unique)
							}
						})
					}
				}
			}
		}
	}
}

func TestResultColumnJoinRouteUsesOwningChildOutput(t *testing.T) {
	col := func(rel, pos int32) *planpb.Expr {
		return &planpb.Expr{Expr: &planpb.Expr_Col{Col: &planpb.ColRef{RelPos: rel, ColPos: pos}}}
	}
	leftValue := col(-1, 3)
	rightValue := col(-1, 5)
	query := &planpb.Query{Nodes: []*planpb.Node{
		{NodeType: planpb.Node_AGG, ProjectList: []*planpb.Expr{leftValue}, BindingTags: []int32{7}},
		{NodeType: planpb.Node_AGG, ProjectList: []*planpb.Expr{col(-1, 0), rightValue}, BindingTags: []int32{8}},
		{
			NodeType: planpb.Node_JOIN, Children: []int32{0, 1},
			ProjectList: []*planpb.Expr{col(1, 1), col(0, 0)},
		},
	}}
	join := query.Nodes[2]
	for _, tc := range []struct {
		name        string
		ref         *planpb.ColRef
		preferLocal bool
		child       int
		expr        *planpb.Expr
	}{
		{"first output is right child slot one", col(0, 0).GetCol(), true, 1, rightValue},
		{"second output is left child slot zero", col(0, 1).GetCol(), true, 0, leftValue},
		{"resolved source identity is not another output slot", col(0, 0).GetCol(), false, 0, leftValue},
	} {
		t.Run(tc.name, func(t *testing.T) {
			route := resultColumnRouteAtJoin(query, join, tc.ref, tc.preferLocal)
			require.Equal(t, tc.child, route.childIdx)
			require.Same(t, tc.expr, route.childExpr)
			require.Same(t, tc.expr.GetCol(), route.ref)
		})
	}

	// Before local JOIN remapping, binding tags identify the owning input.
	join.ProjectList = nil
	ref := col(8, 1).GetCol()
	route := resultColumnRouteAtJoin(query, join, ref, false)
	require.Equal(t, 1, route.childIdx)
	require.Same(t, ref, route.ref)
	require.Nil(t, route.childExpr)

	// Preserve a child's defining expression, not merely ColRef-only origin.
	computed := &planpb.Expr{Expr: &planpb.Expr_F{F: &planpb.Function{}}}
	query.Nodes[1].ProjectList[1] = computed
	join.ProjectList = []*planpb.Expr{col(1, 1)}
	route = resultColumnRouteAtJoin(query, join, col(0, 0).GetCol(), true)
	require.Equal(t, 1, route.childIdx)
	require.Same(t, computed, route.childExpr)
}

func TestGetResultColumnsFromPlanPreservesStoredBooleanBesideWindowResult(t *testing.T) {
	mock := NewMockOptimizer(false, newPlanTestProcess(t))
	p, err := runOneStmt(mock, t,
		"select with_grant_option, max(with_grant_option is null) over () as p from mo_catalog.mo_role_privs")
	require.NoError(t, err)

	columns := GetResultColumnsFromPlan(p)
	require.Len(t, columns, 2)
	require.Equal(t, planpb.ResultColumnKind_RESULT_COLUMN_KIND_STORED, columns[0].ResultColumnKind)
	require.Equal(t, planpb.ResultColumnKind_RESULT_COLUMN_KIND_COMPUTED, columns[1].ResultColumnKind)
}

func TestGetResultColumnsFromPlanPreservesComputedStackedWindowResult(t *testing.T) {
	mock := NewMockOptimizer(false, newPlanTestProcess(t))
	p, err := runOneStmt(mock, t,
		"select first_value(with_grant_option) over (order by role_id), "+
			"max(with_grant_option is null) over (), "+
			"first_value(with_grant_option is null) over (order by role_id desc) "+
			"from mo_catalog.mo_role_privs")
	require.NoError(t, err)

	columns := GetResultColumnsFromPlan(p)
	require.Len(t, columns, 3)
	require.Equal(t, planpb.ResultColumnKind_RESULT_COLUMN_KIND_STORED, columns[0].ResultColumnKind)
	require.Equal(t, planpb.ResultColumnKind_RESULT_COLUMN_KIND_COMPUTED, columns[1].ResultColumnKind)
	require.Equal(t, planpb.ResultColumnKind_RESULT_COLUMN_KIND_COMPUTED, columns[2].ResultColumnKind)
}

func TestGetResultColumnsFromPlanPreservesTwoWindowOutputOrder(t *testing.T) {
	mock := NewMockOptimizer(false, newPlanTestProcess(t))
	for _, tc := range []struct {
		name string
		sql  string
		want []planpb.ResultColumnKind
	}{
		{
			name: "stored then computed",
			sql: "select first_value(with_grant_option) over (order by role_id), " +
				"first_value(role_id is null) over (order by role_id desc) " +
				"from mo_catalog.mo_role_privs",
			want: []planpb.ResultColumnKind{
				planpb.ResultColumnKind_RESULT_COLUMN_KIND_STORED,
				planpb.ResultColumnKind_RESULT_COLUMN_KIND_COMPUTED,
			},
		},
		{
			name: "computed then stored",
			sql: "select first_value(role_id is null) over (order by role_id desc), " +
				"first_value(with_grant_option) over (order by role_id) " +
				"from mo_catalog.mo_role_privs",
			want: []planpb.ResultColumnKind{
				planpb.ResultColumnKind_RESULT_COLUMN_KIND_COMPUTED,
				planpb.ResultColumnKind_RESULT_COLUMN_KIND_STORED,
			},
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			p, err := runOneStmt(mock, t, tc.sql)
			require.NoError(t, err)
			columns := GetResultColumnsFromPlan(p)
			require.Len(t, columns, len(tc.want))
			for i, want := range tc.want {
				require.Equal(t, want, columns[i].ResultColumnKind)
			}
		})
	}
}

func TestResultColumnWindowProjectionUsesOutputSlot(t *testing.T) {
	boolType := planpb.Type{Id: int32(types.T_bool)}
	stored := func(col int32, name string) *planpb.Expr {
		return &planpb.Expr{Typ: boolType, Expr: &planpb.Expr_Col{Col: &planpb.ColRef{
			RelPos: 0, ColPos: col, Name: name,
		}}}
	}
	computedWindow := &planpb.Expr{Typ: boolType, Expr: &planpb.Expr_Col{Col: &planpb.ColRef{
		RelPos: -1, ColPos: 2, Name: "MAX(t.flag IS NULL) OVER ()",
	}}}
	query := &planpb.Query{Nodes: []*planpb.Node{
		{
			NodeType:    planpb.Node_TABLE_SCAN,
			ProjectList: []*planpb.Expr{stored(0, "t.flag")},
		},
		{
			NodeType:    planpb.Node_WINDOW,
			Children:    []int32{0},
			ProjectList: []*planpb.Expr{stored(0, "t.flag"), stored(2, "FIRST_VALUE(t.flag) OVER ()"), computedWindow},
			WinSpecList: []*planpb.Expr{{Expr: &planpb.Expr_W{W: &planpb.WindowSpec{
				WindowFunc: &planpb.Expr{Typ: boolType, Expr: &planpb.Expr_F{F: &planpb.Function{
					Func: &planpb.ObjectRef{ObjName: "max"},
					Args: []*planpb.Expr{{Typ: boolType, Expr: &planpb.Expr_F{F: &planpb.Function{
						Func: &planpb.ObjectRef{ObjName: "isnull"}, Args: []*planpb.Expr{stored(0, "t.flag")},
					}}}},
				}}},
			}}}},
		},
		{
			NodeType:    planpb.Node_PROJECT,
			Children:    []int32{1},
			ProjectList: []*planpb.Expr{stored(2, "MAX(t.flag IS NULL) OVER ()")},
		},
	}}

	// The parent reference is a local output slot.  The earlier passthrough
	// expression deliberately carries the same source identity as that slot;
	// resolving by identity first would select it and report STORED.
	require.True(t, newResultColumnKinds(query).computed(2, query.Nodes[2].ProjectList[0]))
}
