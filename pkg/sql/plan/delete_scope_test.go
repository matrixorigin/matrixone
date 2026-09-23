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

	"github.com/matrixorigin/matrixone/pkg/catalog"
	"github.com/matrixorigin/matrixone/pkg/container/types"
	planpb "github.com/matrixorigin/matrixone/pkg/pb/plan"
	"github.com/matrixorigin/matrixone/pkg/sql/parsers/dialect/mysql"
	"github.com/matrixorigin/matrixone/pkg/sql/parsers/tree"
	"github.com/stretchr/testify/require"
)

func TestIsUnrestrictedDelete(t *testing.T) {
	tests := []struct {
		name        string
		sql         string
		targetCount int
		want        bool
	}{
		{name: "plain", sql: "delete from t", targetCount: 1, want: true},
		{name: "order only", sql: "delete from t order by a", targetCount: 1, want: true},
		{name: "where", sql: "delete from t where true", targetCount: 1},
		{name: "limit zero", sql: "delete from t limit 0", targetCount: 1},
		{name: "join without where", sql: "delete t from t join s on t.a = s.a", targetCount: 1},
		{name: "using without where", sql: "delete from t using t join s on t.a = s.a", targetCount: 1},
		{name: "partition", sql: "delete from t partition (p0)", targetCount: 1},
		{name: "multiple targets", sql: "delete t, s from t join s on t.a = s.a", targetCount: 2},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			stmt, err := mysql.ParseOne(t.Context(), test.sql, 1)
			require.NoError(t, err)
			defer stmt.Free()
			deleteStmt, ok := stmt.(*tree.Delete)
			require.True(t, ok)
			require.Equal(t, test.want, isUnrestrictedDelete(deleteStmt, test.targetCount))
		})
	}

	require.False(t, isUnrestrictedDelete(nil, 1))
}

func TestNormalizeDeleteOldValueExprPreservesStoredSpecialValue(t *testing.T) {
	ctx := context.Background()
	enumType := planpb.Type{Id: int32(types.T_enum), Enumvalues: "a,b,"}
	enumRaw := &planpb.Expr{
		Typ:  enumType,
		Expr: &planpb.Expr_Col{Col: &planpb.ColRef{RelPos: 1, ColPos: 2}},
	}
	enumDisplay, err := makeEnumOrSetDisplayValue(ctx, enumRaw)
	require.NoError(t, err)

	normalized, err := normalizeDeleteOldValueExpr(ctx, enumDisplay, enumType)
	require.NoError(t, err)
	require.Equal(t, enumType, normalized.Typ)
	require.Equal(t, enumRaw.GetCol(), normalized.GetCol(), "ENUM deletion must retain the stored ordinal expression")

	setType := planpb.Type{Id: int32(types.T_uint64), Enumvalues: "a,b,c"}
	setRaw := &planpb.Expr{
		Typ:  setType,
		Expr: &planpb.Expr_Col{Col: &planpb.ColRef{RelPos: 1, ColPos: 3}},
	}
	setDisplay, err := makeEnumOrSetDisplayValue(ctx, setRaw)
	require.NoError(t, err)

	normalized, err = normalizeDeleteOldValueExpr(ctx, setDisplay, setType)
	require.NoError(t, err)
	require.Equal(t, setType, normalized.Typ)
	require.Equal(t, moSetCastIndexValueToIndexFun, normalized.GetF().GetFunc().GetObjName())
	require.Equal(t, setRaw.GetCol(), normalized.GetF().GetArgs()[1].GetCol())
	require.Empty(t, normalized.GetF().GetArgs()[1].Typ.Enumvalues)

	emptySetType := planpb.Type{Id: int32(types.T_uint64), Enumvalues: ",a,b"}
	emptySetRaw := &planpb.Expr{
		Typ:  emptySetType,
		Expr: &planpb.Expr_Col{Col: &planpb.ColRef{RelPos: 1, ColPos: 4}},
	}
	emptySetDisplay, err := makeEnumOrSetDisplayValue(ctx, emptySetRaw)
	require.NoError(t, err)

	normalized, err = normalizeDeleteOldValueExpr(ctx, emptySetDisplay, emptySetType)
	require.NoError(t, err)
	require.Equal(t, emptySetType, normalized.Typ)
	require.Equal(t, moSetCastIndexValueToIndexFun, normalized.GetF().GetFunc().GetObjName())
	require.Equal(t, emptySetRaw.GetCol(), normalized.GetF().GetArgs()[1].GetCol())
	require.Empty(t, normalized.GetF().GetArgs()[1].Typ.Enumvalues)
}

func TestNormalizeDeleteOldValueExprUsesFallbackCast(t *testing.T) {
	ctx := context.Background()
	plain := &planpb.Expr{
		Typ:  planpb.Type{Id: int32(types.T_varchar)},
		Expr: &planpb.Expr_Col{Col: &planpb.ColRef{RelPos: 1, ColPos: 0}},
	}

	enum, err := normalizeDeleteOldValueExpr(ctx, plain, planpb.Type{
		Id: int32(types.T_enum), Enumvalues: "a,b",
	})
	require.NoError(t, err)
	require.Equal(t, moEnumCastValueToIndexFun, enum.GetF().GetFunc().GetObjName())

	set, err := normalizeDeleteOldValueExpr(ctx, plain, planpb.Type{
		Id: int32(types.T_uint64), Enumvalues: "a,b",
	})
	require.NoError(t, err)
	require.Equal(t, moSetCastValueToIndexFun, set.GetF().GetFunc().GetObjName())

	ordinary := &planpb.Expr{Typ: planpb.Type{Id: int32(types.T_int64)}}
	got, err := normalizeDeleteOldValueExpr(ctx, ordinary, ordinary.Typ)
	require.NoError(t, err)
	require.Same(t, ordinary, got)

	_, err = normalizeDeleteOldValueExpr(ctx, nil, planpb.Type{
		Id: int32(types.T_enum), Enumvalues: "a,b",
	})
	require.Error(t, err)
}

func TestNormalizeDeleteOldValueProjectionFollowsOrderProjection(t *testing.T) {
	enumType := planpb.Type{Id: int32(types.T_enum), Enumvalues: "a,b"}
	raw := &planpb.Expr{
		Typ:  enumType,
		Expr: &planpb.Expr_Col{Col: &planpb.ColRef{RelPos: 10, ColPos: 0}},
	}
	display, err := makeEnumOrSetDisplayValue(context.Background(), raw)
	require.NoError(t, err)

	mock := newMySQLSpecialOrderMock()
	builder := NewQueryBuilder(planpb.Query_DELETE, &mock.ctxt, false, true)
	builder.qry.Nodes = []*planpb.Node{
		{
			NodeType:    planpb.Node_PROJECT,
			ProjectList: []*planpb.Expr{display},
			Children:    []int32{3},
			BindingTags: []int32{10},
		},
		{NodeType: planpb.Node_SORT, Children: []int32{0}},
		{
			NodeType:    planpb.Node_PROJECT,
			ProjectList: []*planpb.Expr{GetColExpr(display.Typ, 10, 0)},
			Children:    []int32{1},
			BindingTags: []int32{11},
		},
		{NodeType: planpb.Node_TABLE_SCAN},
	}

	err = builder.normalizeDeleteOldValueProjection(
		2,
		[]*planpb.TableDef{{Cols: []*planpb.ColDef{{Name: "status", Typ: enumType}}}},
		[]map[string]int32{{"status": 0}},
	)
	require.NoError(t, err)
	require.Equal(t, enumType, builder.qry.Nodes[0].ProjectList[0].Typ)
	require.Equal(t, raw.GetCol(), builder.qry.Nodes[0].ProjectList[0].GetCol())
	require.Equal(t, enumType, builder.qry.Nodes[2].ProjectList[0].Typ)
}

func TestNormalizeDeleteOldValueProjectionRejectsInvalidMapping(t *testing.T) {
	enumType := planpb.Type{Id: int32(types.T_enum), Enumvalues: "a,b"}
	tableDef := &planpb.TableDef{Cols: []*planpb.ColDef{{Name: "status", Typ: enumType}}}
	tests := []struct {
		name        string
		nodes       []*planpb.Node
		nodeID      int32
		tableDefs   []*planpb.TableDef
		colName2Idx []map[string]int32
		want        string
	}{
		{
			name:   "invalid root node",
			nodeID: 0,
			want:   "projection is nil",
		},
		{
			name:        "missing table mapping",
			nodes:       []*planpb.Node{{NodeType: planpb.Node_TABLE_SCAN}},
			tableDefs:   []*planpb.TableDef{tableDef},
			colName2Idx: nil,
			want:        "invalid table mapping",
		},
		{
			name:        "nil table definition",
			nodes:       []*planpb.Node{{NodeType: planpb.Node_TABLE_SCAN}},
			tableDefs:   []*planpb.TableDef{nil},
			colName2Idx: []map[string]int32{{}},
			want:        "invalid table mapping",
		},
		{
			name:        "missing special column",
			nodes:       []*planpb.Node{{NodeType: planpb.Node_TABLE_SCAN}},
			tableDefs:   []*planpb.TableDef{tableDef},
			colName2Idx: []map[string]int32{{}},
			want:        "cannot locate column status",
		},
		{
			name:        "negative special column position",
			nodes:       []*planpb.Node{{NodeType: planpb.Node_TABLE_SCAN}},
			tableDefs:   []*planpb.TableDef{tableDef},
			colName2Idx: []map[string]int32{{"status": -1}},
			want:        "cannot locate column status",
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			mock := newMySQLSpecialOrderMock()
			builder := NewQueryBuilder(planpb.Query_DELETE, &mock.ctxt, false, true)
			builder.qry.Nodes = test.nodes

			err := builder.normalizeDeleteOldValueProjection(
				test.nodeID, test.tableDefs, test.colName2Idx,
			)
			require.ErrorContains(t, err, test.want)
		})
	}
}

func TestNormalizeDeleteOldValueProjectionRejectsInvalidChain(t *testing.T) {
	enumType := planpb.Type{Id: int32(types.T_enum), Enumvalues: "a,b"}
	forward := GetColExpr(enumType, 1, 0)
	tests := []struct {
		name   string
		nodes  []*planpb.Node
		colPos int32
		want   string
	}{
		{
			name: "project column out of range",
			nodes: []*planpb.Node{{
				NodeType:    planpb.Node_PROJECT,
				ProjectList: []*planpb.Expr{forward},
			}},
			colPos: 1,
			want:   "column is out of range",
		},
		{
			name: "forward reference without child",
			nodes: []*planpb.Node{{
				NodeType:    planpb.Node_PROJECT,
				ProjectList: []*planpb.Expr{forward},
			}},
			want: "reference is invalid",
		},
		{
			name: "forward reference to invalid child",
			nodes: []*planpb.Node{{
				NodeType:    planpb.Node_PROJECT,
				ProjectList: []*planpb.Expr{forward},
				Children:    []int32{2},
			}},
			want: "chain is invalid",
		},
		{
			name: "nil projected expression",
			nodes: []*planpb.Node{{
				NodeType:    planpb.Node_PROJECT,
				ProjectList: []*planpb.Expr{nil},
			}},
			want: "projection is nil",
		},
		{
			name: "non unary node",
			nodes: []*planpb.Node{{
				NodeType: planpb.Node_FILTER,
				Children: []int32{0, 1},
			}},
			want: "chain is not unary",
		},
		{
			name: "cyclic node chain",
			nodes: []*planpb.Node{{
				NodeType: planpb.Node_SORT,
				Children: []int32{0},
			}},
			want: "chain is cyclic",
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			mock := newMySQLSpecialOrderMock()
			builder := NewQueryBuilder(planpb.Query_DELETE, &mock.ctxt, false, true)
			builder.qry.Nodes = test.nodes
			visiting := make(map[int32]bool)

			err := builder.normalizeDeleteOldValueProjectionAtNode(
				0, test.colPos, enumType, visiting,
			)
			require.ErrorContains(t, err, test.want)
			require.Empty(t, visiting, "failed traversal must release its recursion state")
		})
	}
}

func TestNormalizeDeleteOldValueProjectionAcceptsRawScan(t *testing.T) {
	enumType := planpb.Type{Id: int32(types.T_enum), Enumvalues: "a,b"}
	mock := newMySQLSpecialOrderMock()
	builder := NewQueryBuilder(planpb.Query_DELETE, &mock.ctxt, false, true)
	builder.qry.Nodes = []*planpb.Node{{NodeType: planpb.Node_TABLE_SCAN}}

	err := builder.normalizeDeleteOldValueProjection(
		0,
		[]*planpb.TableDef{{Cols: []*planpb.ColDef{{Name: "status", Typ: enumType}}}},
		[]map[string]int32{{"status": 0}},
	)
	require.NoError(t, err)
}

func TestIrregularIndexDeleteJoinIsRowScoped(t *testing.T) {
	logicPlan, err := runOneStmt(
		NewMockOptimizer(true),
		t,
		"delete d from constraint_test.docs_ft d join constraint_test.dept s on d.id = s.deptno",
	)
	require.NoError(t, err)

	query := logicPlan.GetQuery()
	require.NotNil(t, query)

	var originDelete *planpb.Node
	fulltextDeleteCount := 0
	for _, node := range query.Nodes {
		if node.NodeType != planpb.Node_DELETE || node.DeleteCtx == nil || node.DeleteCtx.TableDef == nil {
			continue
		}
		switch node.DeleteCtx.TableDef.Name {
		case "docs_ft":
			originDelete = node
		case catalog.FullTextIndexTableNamePrefix + "docs_ft_body":
			fulltextDeleteCount++
		}
	}

	require.NotNil(t, originDelete, "missing origin-table delete")
	require.False(t, originDelete.DeleteCtx.CanTruncate, "a join must be evaluated before deleting target rows")
	require.Equal(t, 1, fulltextDeleteCount, "row-scoped fulltext maintenance must not be skipped")
}

func TestLegacyMultiTableDeleteNormalizesSpecialOldValues(t *testing.T) {
	mock := NewMockOptimizer(true)
	dept := mock.ctxt.tables["dept"]
	for _, col := range dept.Cols {
		if col.Name == "dname" {
			col.Typ = planpb.Type{Id: int32(types.T_enum), Enumvalues: "a,b"}
		}
	}
	docs := mock.ctxt.tables["docs_ft"]
	for _, col := range docs.Cols {
		if col.Name == "payload" {
			col.Typ = planpb.Type{Id: int32(types.T_uint64), Enumvalues: "a,b"}
		}
	}

	logicPlan, err := runOneStmt(
		mock,
		t,
		"delete s,d from constraint_test.docs_ft d join constraint_test.dept s on d.id = s.deptno",
	)
	require.NoError(t, err)

	query := logicPlan.GetQuery()
	var enumExpr, setExpr *planpb.Expr
	var hasColRef func(expr *planpb.Expr, name string) bool
	hasColRef = func(expr *planpb.Expr, name string) bool {
		if expr == nil {
			return false
		}
		if col := expr.GetCol(); col != nil {
			return col.Name == name
		}
		if fn := expr.GetF(); fn != nil {
			for _, arg := range fn.Args {
				if hasColRef(arg, name) {
					return true
				}
			}
		}
		return false
	}
	for _, node := range query.Nodes {
		if node.NodeType != planpb.Node_PROJECT || len(node.Children) != 1 {
			continue
		}
		childID := node.Children[0]
		if childID < 0 || int(childID) >= len(query.Nodes) || query.Nodes[childID].NodeType != planpb.Node_JOIN {
			continue
		}
		for _, expr := range node.ProjectList {
			if hasColRef(expr, "s.dname") {
				enumExpr = expr
			}
			if hasColRef(expr, "d.payload") {
				setExpr = expr
			}
		}
	}
	require.NotNil(t, enumExpr, "legacy DELETE source must include the ENUM old value")
	require.NotNil(t, enumExpr.GetCol(), "ENUM old value must be restored to its storage column")
	require.Equal(t, int32(types.T_enum), enumExpr.Typ.Id)
	require.Equal(t, "a,b", enumExpr.Typ.Enumvalues)

	require.NotNil(t, setExpr, "legacy DELETE source must include the SET old value")
	require.Equal(t, int32(types.T_uint64), setExpr.Typ.Id)
	if setExpr.GetF() != nil {
		require.Equal(t, moSetCastIndexValueToIndexFun, setExpr.GetF().GetFunc().GetObjName())
		require.Len(t, setExpr.GetF().Args, 2)
		require.True(t, hasColRef(setExpr.GetF().Args[1], "d.payload"))
		require.Empty(t, setExpr.GetF().Args[1].Typ.Enumvalues)
	}
}
