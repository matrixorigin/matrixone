// Copyright 2021 Matrix Origin
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
	"context"
	"testing"

	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/pb/plan"
	"github.com/matrixorigin/matrixone/pkg/sql/parsers/tree"
	"github.com/stretchr/testify/require"
)

func newReplaceBinderTestTableDef() *plan.TableDef {
	intTyp := plan.Type{Id: int32(types.T_int32)}
	return &plan.TableDef{
		Name: "t_set",
		Cols: []*plan.ColDef{
			{
				Name: "id",
				Typ:  intTyp,
				// no explicit default, but nullable so DEFAULT(id) is NULL
				Default: &plan.Default{NullAbility: true},
			},
			{
				Name: "v",
				Typ:  intTyp,
				// DEFAULT(v) == 5
				Default: &plan.Default{
					NullAbility: false,
					Expr:        makePlan2Int64ConstExprWithType(5),
				},
			},
		},
		Name2ColIndex: map[string]int32{"id": 0, "v": 1},
	}
}

func TestReplaceValueBinder_BindColRef(t *testing.T) {
	ctx := context.Background()
	tableDef := newReplaceBinderTestTableDef()
	b := NewReplaceValueBinder(ctx, nil, nil, plan.Type{}, tableDef)

	// A reference to column "v" resolves to its DEFAULT value (literal 5).
	expr, err := b.BindColRef(tree.NewUnresolvedColName("v"), 0, true)
	require.NoError(t, err)
	lit := expr.GetLit()
	require.NotNil(t, lit)
	require.Equal(t, int64(5), lit.GetI64Val())

	// Case-insensitive lookup also resolves to DEFAULT(v) == 5.
	expr, err = b.BindColRef(tree.NewUnresolvedColName("V"), 0, true)
	require.NoError(t, err)
	require.NotNil(t, expr.GetLit())
	require.Equal(t, int64(5), expr.GetLit().GetI64Val())

	// A reference to a nullable column with no explicit default resolves to NULL.
	expr, err = b.BindColRef(tree.NewUnresolvedColName("id"), 0, true)
	require.NoError(t, err)
	require.NotNil(t, expr.GetLit())
	require.True(t, expr.GetLit().GetIsnull())

	// A reference to a non-existent column errors.
	_, err = b.BindColRef(tree.NewUnresolvedColName("nope"), 0, true)
	require.Error(t, err)

	// A qualified reference must NOT be silently resolved to DEFAULT(v) of the
	// destination column just because the target table has a column named "v".
	// `other.v` (a column on some other table) is rejected.
	qualified := tree.NewUnresolvedName(tree.NewCStr("other", 1), tree.NewCStr("v", 1))
	require.Equal(t, 2, qualified.NumParts)
	_, err = b.BindColRef(qualified, 0, true)
	require.Error(t, err)

	// A db.table.col reference is likewise rejected.
	qualified3 := tree.NewUnresolvedName(tree.NewCStr("db", 1), tree.NewCStr("t", 1), tree.NewCStr("v", 1))
	require.Equal(t, 3, qualified3.NumParts)
	_, err = b.BindColRef(qualified3, 0, true)
	require.Error(t, err)
}

func TestReplaceValueBinder_BindExpr(t *testing.T) {
	ctx := context.Background()
	tableDef := newReplaceBinderTestTableDef()
	b := NewReplaceValueBinder(ctx, nil, nil, plan.Type{}, tableDef)

	// v + 1 binds to "+"(DEFAULT(v), 1) == "+"(5, 1). The binder does not fold
	// constants (that happens later in the optimizer), so assert the structure:
	// the column reference "v" has been replaced by its DEFAULT literal 5, which
	// makes the expression evaluate to 6.
	astExpr := &tree.BinaryExpr{
		Op:    tree.PLUS,
		Left:  tree.NewUnresolvedColName("v"),
		Right: tree.NewNumVal(int64(1), "1", false, tree.P_int64),
	}
	expr, err := b.BindExpr(astExpr, 0, true)
	require.NoError(t, err)
	fn := expr.GetF()
	require.NotNil(t, fn)
	require.Equal(t, "+", fn.Func.ObjName)
	require.Len(t, fn.Args, 2)
	require.Equal(t, int64(5), fn.Args[0].GetLit().GetI64Val())
	require.Equal(t, int64(1), fn.Args[1].GetLit().GetI64Val())
}

func TestReplaceValueBinder_UnsupportedBindings(t *testing.T) {
	ctx := context.Background()
	tableDef := newReplaceBinderTestTableDef()
	b := NewReplaceValueBinder(ctx, nil, nil, plan.Type{}, tableDef)

	_, err := b.BindAggFunc("sum", nil, 0, true)
	require.Error(t, err)

	_, err = b.BindWinFunc("rank", nil, 0, true)
	require.Error(t, err)

	_, err = b.BindSubquery(nil, true)
	require.Error(t, err)

	_, err = b.BindTimeWindowFunc("tumble", nil, 0, true)
	require.Error(t, err)
}

func TestReplaceValueBinder_DecimalPrecision(t *testing.T) {
	ctx := context.Background()

	// Table with a DECIMAL(38,18) column.
	decTyp := plan.Type{Id: int32(types.T_decimal128), Width: 38, Scale: 18}
	tableDef := &plan.TableDef{
		Name: "t",
		Cols: []*plan.ColDef{
			{Name: "id", Typ: plan.Type{Id: int32(types.T_int32)}, Default: &plan.Default{NullAbility: true}},
			{Name: "dec_col", Typ: decTyp, Default: &plan.Default{NullAbility: false, Expr: makePlan2Int64ConstExprWithType(0)}},
		},
		Name2ColIndex: map[string]int32{"id": 0, "dec_col": 1},
	}

	// Create binder with the destination column type, matching what
	// buildValueScan does for a DECIMAL column.
	b := NewReplaceValueBinder(ctx, nil, nil, decTyp, tableDef)

	// A scientific-notation NumVal bound with the DECIMAL column type must
	// keep its DECIMAL type rather than being degraded to float64.
	// `-1.234567890123456789e18` is within DECIMAL(38,18) range.
	numVal := tree.NewNumVal("-1.234567890123456789e18", "-1.234567890123456789e18", false, tree.P_decimal)
	expr, err := b.BindExpr(numVal, 0, true)
	require.NoError(t, err)
	lit := expr.GetLit()
	require.NotNil(t, lit, "scientific-notation literal should produce a literal, not a float64 cast")
	require.Equal(t, decTyp.Id, expr.Typ.Id, "literal type should be decimal128, not float64")
	require.Equal(t, decTyp.Width, expr.Typ.Width)
	require.Equal(t, decTyp.Scale, expr.Typ.Scale)
	// With a target column type, bindNumVal parses the decimal and stores it
	// as a Decimal128Val binary — not a float64.
	require.False(t, lit.GetIsnull(), "decimal value should not be null")
	require.NotNil(t, lit.GetDecimal128Val(), "decimal128 value should be populated")
}
