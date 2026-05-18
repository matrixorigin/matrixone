// Copyright 2026 Matrix Origin
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     https://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package plan

import (
	"testing"

	"github.com/matrixorigin/matrixone/pkg/container/types"
	planpb "github.com/matrixorigin/matrixone/pkg/pb/plan"
	"github.com/stretchr/testify/require"
)

func TestDoMergeFiltersOnCompositeKeyKeepsNonSortKeyRanges(t *testing.T) {
	ctx := NewMockCompilerContext(true)
	builder := NewQueryBuilder(planpb.Query_SELECT, ctx, false, false)
	tag := builder.genNewBindTag()
	tableDef := makeExprOptCompositeSortKeyTableDef()

	aEq := makeExprOptBinaryInt64Expr(t, ctx, "=", makeExprOptInt64Col(tag, 0, "a"), 1)
	cGt := makeExprOptBinaryInt64Expr(t, ctx, ">", makeExprOptInt64Col(tag, 2, "c"), 2)
	cLt := makeExprOptBinaryInt64Expr(t, ctx, "<", makeExprOptInt64Col(tag, 2, "c"), 4)

	ret := builder.doMergeFiltersOnCompositeKey(tableDef, tag, aEq, cGt, cLt)

	require.Len(t, ret, 3)
	requireFuncNames(t, ret, ">", "<")
	requireNoFuncNames(t, ret, "in_range", "between")
}

func TestDoMergeFiltersOnCompositeKeyMergesSortKeyRanges(t *testing.T) {
	ctx := NewMockCompilerContext(true)
	builder := NewQueryBuilder(planpb.Query_SELECT, ctx, false, false)
	tag := builder.genNewBindTag()
	tableDef := makeExprOptCompositeSortKeyTableDef()

	aEq := makeExprOptBinaryInt64Expr(t, ctx, "=", makeExprOptInt64Col(tag, 0, "a"), 1)
	bGt := makeExprOptBinaryInt64Expr(t, ctx, ">", makeExprOptInt64Col(tag, 1, "b"), 2)
	bLt := makeExprOptBinaryInt64Expr(t, ctx, "<", makeExprOptInt64Col(tag, 1, "b"), 4)

	ret := builder.doMergeFiltersOnCompositeKey(tableDef, tag, aEq, bGt, bLt)

	requireFuncNames(t, ret, "in_range")
}

func makeExprOptCompositeSortKeyTableDef() *planpb.TableDef {
	return &planpb.TableDef{
		Cols: []*planpb.ColDef{
			{Name: "a", Typ: planpb.Type{Id: int32(types.T_int64)}},
			{Name: "b", Typ: planpb.Type{Id: int32(types.T_int64)}},
			{Name: "c", Typ: planpb.Type{Id: int32(types.T_int64)}},
			{Name: "__mo_cpkey", Typ: planpb.Type{Id: int32(types.T_varchar)}},
		},
		Name2ColIndex: map[string]int32{
			"a":          0,
			"b":          1,
			"c":          2,
			"__mo_cpkey": 3,
		},
		Pkey: &planpb.PrimaryKeyDef{
			PkeyColName: "__mo_cpkey",
			Names:       []string{"a", "b"},
		},
	}
}

func makeExprOptInt64Col(tag, pos int32, name string) *planpb.Expr {
	return &planpb.Expr{
		Typ: planpb.Type{Id: int32(types.T_int64)},
		Expr: &planpb.Expr_Col{
			Col: &planpb.ColRef{
				RelPos: tag,
				ColPos: pos,
				Name:   name,
			},
		},
	}
}

func makeExprOptBinaryInt64Expr(t *testing.T, ctx *MockCompilerContext, op string, col *planpb.Expr, val int64) *planpb.Expr {
	t.Helper()

	expr, err := BindFuncExprImplByPlanExpr(ctx.GetContext(), op, []*planpb.Expr{
		col,
		MakePlan2Int64ConstExprWithType(val),
	})
	require.NoError(t, err)
	return expr
}

func requireFuncNames(t *testing.T, filters []*planpb.Expr, names ...string) {
	t.Helper()

	hits := make(map[string]bool, len(names))
	for _, name := range names {
		hits[name] = false
	}
	for _, expr := range filters {
		if fn := expr.GetF(); fn != nil {
			if _, ok := hits[fn.Func.ObjName]; ok {
				hits[fn.Func.ObjName] = true
			}
		}
	}
	for name, hit := range hits {
		require.Truef(t, hit, "expected function %s in filters", name)
	}
}

func requireNoFuncNames(t *testing.T, filters []*planpb.Expr, names ...string) {
	t.Helper()

	for _, expr := range filters {
		fn := expr.GetF()
		if fn == nil {
			continue
		}
		for _, name := range names {
			require.NotEqual(t, name, fn.Func.ObjName)
		}
	}
}
