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
	b := NewReplaceValueBinder(ctx, nil, nil, tableDef)

	// A reference to column "v" resolves to its DEFAULT value (literal 5).
	expr, err := b.BindColRef(tree.NewUnresolvedColName("v"), 0, true)
	require.NoError(t, err)
	lit := expr.GetLit()
	require.NotNil(t, lit)
	require.Equal(t, int64(5), lit.GetI64Val())

	// Case-insensitive lookup.
	expr, err = b.BindColRef(tree.NewUnresolvedColName("V"), 0, true)
	require.NoError(t, err)
	require.NotNil(t, expr.GetLit())

	// A reference to a nullable column with no explicit default resolves to NULL.
	expr, err = b.BindColRef(tree.NewUnresolvedColName("id"), 0, true)
	require.NoError(t, err)
	require.NotNil(t, expr.GetLit())
	require.True(t, expr.GetLit().GetIsnull())

	// A reference to a non-existent column errors.
	_, err = b.BindColRef(tree.NewUnresolvedColName("nope"), 0, true)
	require.Error(t, err)
}

func TestReplaceValueBinder_BindExpr(t *testing.T) {
	ctx := context.Background()
	tableDef := newReplaceBinderTestTableDef()
	b := NewReplaceValueBinder(ctx, nil, nil, tableDef)

	// v + 1 == DEFAULT(v) + 1 == 6 (constant-foldable).
	astExpr := &tree.BinaryExpr{
		Op:    tree.PLUS,
		Left:  tree.NewUnresolvedColName("v"),
		Right: tree.NewNumVal(int64(1), "1", false, tree.P_int64),
	}
	expr, err := b.BindExpr(astExpr, 0, true)
	require.NoError(t, err)
	require.NotNil(t, expr)
}

func TestReplaceValueBinder_UnsupportedBindings(t *testing.T) {
	ctx := context.Background()
	tableDef := newReplaceBinderTestTableDef()
	b := NewReplaceValueBinder(ctx, nil, nil, tableDef)

	_, err := b.BindAggFunc("sum", nil, 0, true)
	require.Error(t, err)

	_, err = b.BindWinFunc("rank", nil, 0, true)
	require.Error(t, err)

	_, err = b.BindSubquery(nil, true)
	require.Error(t, err)

	_, err = b.BindTimeWindowFunc("tumble", nil, 0, true)
	require.Error(t, err)
}
