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
