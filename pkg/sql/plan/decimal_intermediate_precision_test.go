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
	"context"
	"testing"

	"github.com/gogo/protobuf/proto"
	"github.com/matrixorigin/matrixone/pkg/container/batch"
	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/container/vector"
	planpb "github.com/matrixorigin/matrixone/pkg/pb/plan"
	"github.com/matrixorigin/matrixone/pkg/sql/colexec"
	"github.com/stretchr/testify/require"
)

func TestDecimalRevenuePrecisionFromLiteral(t *testing.T) {
	ctx := context.Background()
	decimalType := types.New(types.T_decimal64, 15, 2)
	literal := makePlan2Int64ConstExprWithType(1)
	before := proto.Clone(literal)
	discount, err := BindFuncExprImplByPlanExpr(ctx, "-", []*planpb.Expr{literal, decimalArithmeticTestColumn(decimalType, 0)})
	require.NoError(t, err)
	require.Equal(t, int32(types.T_decimal128), discount.Typ.Id)
	require.Equal(t, int32(16), discount.Typ.Width)
	require.True(t, proto.Equal(before, literal), "lookup refinement mutated a shared literal")
	revenue, err := BindFuncExprImplByPlanExpr(ctx, "*", []*planpb.Expr{decimalArithmeticTestColumn(decimalType, 1), discount})
	require.NoError(t, err)
	require.Equal(t, int32(types.T_decimal128), revenue.Typ.Id)
	require.Equal(t, int32(31), revenue.Typ.Width)
	require.Equal(t, int32(4), revenue.Typ.Scale)
}

func TestCTASDecimalArithmeticPublishesDerivedPrecision(t *testing.T) {
	for _, tc := range []struct {
		name, expr   string
		width, scale int32
	}{
		{"discounted revenue", "l_extendedprice * (1 - l_discount)", 31, 4},
		{"direct product", "l_extendedprice * l_discount", 30, 4},
	} {
		t.Run(tc.name, func(t *testing.T) {
			built, err := buildSingleStmt(NewMockOptimizer(false, newPlanTestProcess(t)), t, "create table derived_precision as select "+tc.expr+" as amount from lineitem")
			require.NoError(t, err)
			col := built.GetDdl().GetCreateTable().GetTableDef().Cols[0]
			require.Equal(t, int32(types.T_decimal128), col.Typ.Id)
			require.Equal(t, tc.width, col.Typ.Width)
			require.Equal(t, tc.scale, col.Typ.Scale)
		})
	}
}

func TestDecimalPrecisionRetainsFullIntegerValues(t *testing.T) {
	for _, tc := range []struct{ name, sql, want string }{
		{"signed bigint", "select cast(9223372036854775807 as signed) * cast('9999999999999.99' as decimal(15,2))", "92233720368547665836279631452241.93"},
		{"unsigned bigint", "select cast(18446744073709551615 as unsigned) * cast('1.00' as decimal(15,2))", "18446744073709551615.00"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			proc := newPlanTestProcess(t)
			built, err := runOneExprStmt(NewMockOptimizer(false, proc), t, tc.sql)
			require.NoError(t, err)
			expr := built.GetQuery().Nodes[1].ProjectList[0]
			executor, err := colexec.NewExpressionExecutor(proc, expr)
			require.NoError(t, err)
			defer executor.Free()
			result, err := executor.Eval(proc, []*batch.Batch{batch.EmptyForConstFoldBatch}, nil)
			require.NoError(t, err)
			require.Equal(t, int32(types.T_decimal128), expr.Typ.Id)
			value := vector.GetFixedAtWithTypeCheck[types.Decimal128](result, 0)
			require.Equal(t, tc.want, value.Format(expr.Typ.Scale))
		})
	}
}
