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
	"strings"
	"testing"

	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/container/vector"
	planpb "github.com/matrixorigin/matrixone/pkg/pb/plan"
	"github.com/matrixorigin/matrixone/pkg/sql/colexec"
	"github.com/matrixorigin/matrixone/pkg/sql/parsers/dialect/mysql"
	"github.com/matrixorigin/matrixone/pkg/sql/plan/function"
	"github.com/matrixorigin/matrixone/pkg/testutil"
	"github.com/matrixorigin/matrixone/pkg/vm/process"
	"github.com/stretchr/testify/require"
)

type divPrecisionCompilerContext struct {
	CompilerContext
	increment int64
}

func (c divPrecisionCompilerContext) ResolveVariable(name string, system, global bool) (any, error) {
	if name == "div_precision_increment" && system && !global {
		return c.increment, nil
	}
	return c.CompilerContext.ResolveVariable(name, system, global)
}

func TestQueryBuilderCarriesDivPrecisionIncrementIntoBinding(t *testing.T) {
	decimalType := types.New(types.T_decimal64, 10, 2)
	newColumn := func(position int32) *Expr {
		return &Expr{
			Typ: makePlan2Type(&decimalType),
			Expr: &planpb.Expr_Col{Col: &planpb.ColRef{
				RelPos: 0,
				ColPos: position,
			}},
		}
	}

	for _, test := range []struct {
		increment int64
		want      types.Type
	}{
		{increment: 0, want: types.New(types.T_decimal128, 12, 2)},
		{increment: 4, want: types.New(types.T_decimal128, 16, 6)},
		{increment: 10, want: types.New(types.T_decimal128, 22, 12)},
		{increment: 30, want: types.New(types.T_decimal256, 42, 30)},
	} {
		t.Run(test.want.String(), func(t *testing.T) {
			compiler := divPrecisionCompilerContext{
				CompilerContext: NewMockCompilerContext(true),
				increment:       test.increment,
			}
			builder := NewQueryBuilder(planpb.Query_SELECT, compiler, false, true)
			expr, err := BindFuncExprImplByPlanExpr(
				builder.GetContext(), "/", []*Expr{newColumn(0), newColumn(1)})
			require.NoError(t, err)
			require.Equal(t, test.want, makeTypeByPlan2Expr(expr))
		})
	}
}

func TestPreparedDivisionSpecializationUsesPrecisionIncrementContext(t *testing.T) {
	prepared, err := runOneStmt(NewMockOptimizer(false), t,
		`prepare div_precision from 'select ? / 2 as q'`)
	require.NoError(t, err)

	ctx := function.WithDivPrecisionIncrement(context.Background(), 10)
	bound, specialized, err := FillValuesOfParamsInPlanWithSpecialization(
		ctx,
		prepared.GetDcl().GetPrepare().Plan,
		[]any{ParamValue{
			Value:         "1.00",
			SourceType:    types.New(types.T_decimal64, 10, 2),
			HasSourceType: true,
		}},
	)
	require.NoError(t, err)
	require.True(t, specialized)
	division := findPlanFunctionExpr(bound, "/")
	require.NotNil(t, division)
	require.Equal(t, types.New(types.T_decimal128, 20, 12), makeTypeByPlan2Expr(division))
}

func TestDivisionSQLBoundaries(t *testing.T) {
	for _, test := range []struct {
		name      string
		increment int64
		sql       string
		width     int32
		scale     int32
		want      string
		wantError bool
	}{
		{
			name:      "shifted numerator",
			increment: 30,
			sql: "select cast('3" + strings.Repeat("0", 46) + "' as decimal(47,0)) / " +
				"cast('1" + strings.Repeat("0", 46) + "' as decimal(47,0))",
			width: 65, scale: 30, want: "3." + strings.Repeat("0", 30),
		},
		{
			name:      "divisor alignment",
			increment: 30,
			sql: "select cast('28" + strings.Repeat("0", 45) + "' as decimal(47,0)) / " +
				"cast('1" + strings.Repeat("0", 46) + "' as decimal(47,0))",
			width: 65, scale: 30, want: "2.8" + strings.Repeat("0", 29),
		},
		{
			name:      "highest valid precision",
			increment: 0,
			sql: "select cast('" + strings.Repeat("9", 38) + "' as decimal(38,0)) / " +
				"cast('0." + strings.Repeat("0", 26) + "1' as decimal(38,27))",
			width: 65, scale: 0, want: strings.Repeat("9", 38) + strings.Repeat("0", 27),
		},
		{
			name:      "declared precision overflow",
			increment: 0,
			sql: "select cast('1" + strings.Repeat("0", 37) + "' as decimal(38,0)) / " +
				"cast('0." + strings.Repeat("0", 29) + "1' as decimal(38,30))",
			wantError: true,
		},
		{
			name:      "high input scale",
			increment: 0,
			sql:       "select cast('0.1' as decimal(38,37)) / cast('2' as decimal(1,0))",
			width:     38, scale: 30, want: "0.05" + strings.Repeat("0", 28),
		},
	} {
		t.Run(test.name, func(t *testing.T) {
			proc := testutil.NewProcess(t)
			mock := NewMockCompilerContext(true)
			mock.GetProcessFunc = func() *process.Process { return proc }
			ctx := divPrecisionCompilerContext{CompilerContext: mock, increment: test.increment}
			statement, err := mysql.ParseOne(t.Context(), test.sql, 1)
			require.NoError(t, err)
			defer statement.Free()
			plan, err := BuildPlan(ctx, statement, false)
			require.NoError(t, err)
			query := plan.GetQuery()
			expr := query.Nodes[query.Steps[len(query.Steps)-1]].ProjectList[0]
			result, free, err := colexec.GetReadonlyResultFromNoColumnExpression(proc, expr)
			if free != nil {
				defer free()
			}
			if test.wantError {
				require.Error(t, err)
				return
			}
			require.NoError(t, err)
			require.Equal(t, test.width, result.GetType().Width)
			require.Equal(t, test.scale, result.GetType().Scale)
			if result.GetType().Oid == types.T_decimal128 {
				require.Equal(t, test.want, vector.MustFixedColWithTypeCheck[types.Decimal128](result)[0].Format(test.scale))
			} else {
				require.Equal(t, test.want, vector.MustFixedColWithTypeCheck[types.Decimal256](result)[0].Format(test.scale))
			}
		})
	}
}
