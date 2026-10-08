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

package function

import (
	"context"
	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/pb/plan"
	"github.com/stretchr/testify/require"
	"testing"
)

func TestZoneMapCapabilityRegistryCoverage(t *testing.T) {
	// A newly flagged family must acquire an explicit semantic domain review.
	// The flag is inventory, not permission for paired endpoint execution.
	expected := []int32{EQUAL, GREAT_THAN, GREAT_EQUAL, LESS_THAN, LESS_EQUAL, BETWEEN,
		AND, OR, IN, ISNULL, ISNOTNULL, PLUS, MINUS, MULTI, DIV, INTEGER_DIV, UNARY_PLUS, UNARY_MINUS,
		PREFIX_EQ, PREFIX_BETWEEN, PREFIX_IN_RANGE, PREFIX_IN, IN_RANGE, CEIL, FLOOR, ROUND, PI,
		TS_TO_TIME, CURRENT_TIMESTAMP, LOCALTIME, DATE, DATE_SUB, FROM_UNIXTIME, YEAR, DATE_TRUNC}
	families := make(map[int32]bool, len(expected))
	for _, id := range expected {
		require.True(t, allSupportedFunctions[id].testFlag(plan.Function_ZONEMAPPABLE))
		families[id] = true
	}
	count := 0
	for _, f := range allSupportedFunctions {
		if f.testFlag(plan.Function_ZONEMAPPABLE) {
			count++
			require.True(t, families[int32(f.functionId)], "review new zonemappable family %d", f.functionId)
		}
	}
	require.Equal(t, len(expected), count)
}

func TestZoneMapStatementConstantPolicy(t *testing.T) {
	literal := &plan.Expr{Typ: plan.Type{Id: int32(types.T_int64)}, Expr: &plan.Expr_Lit{Lit: &plan.Literal{Value: &plan.Literal_I64Val{I64Val: 1}}}}
	for _, tc := range []struct {
		name                string
		expr                *plan.Expr
		statement, prepared bool
	}{
		{"literal", literal, true, true},
		{"point column", &plan.Expr{Typ: literal.Typ, Expr: &plan.Expr_Col{Col: &plan.ColRef{}}}, false, false},
		{"unbound parameter", &plan.Expr{Typ: literal.Typ, Expr: &plan.Expr_P{P: &plan.ParamRef{}}}, false, true},
		{"unbound variable", &plan.Expr{Typ: literal.Typ, Expr: &plan.Expr_V{V: &plan.VarRef{}}}, false, true},
		{"scalar Fold provenance", &plan.Expr{Typ: literal.Typ, Expr: &plan.Expr_Fold{Fold: &plan.FoldVal{IsConst: true}}}, true, true},
		{"list Fold", &plan.Expr{Typ: literal.Typ, Expr: &plan.Expr_Fold{Fold: &plan.FoldVal{}}}, false, false},
	} {
		t.Run(tc.name, func(t *testing.T) {
			require.Equal(t, tc.statement, IsStatementConstant(tc.expr))
			require.Equal(t, tc.prepared, IsConstant(tc.expr, true))
			require.Equal(t, tc.prepared, IsRuntimeConstant(tc.expr))
		})
	}
	for _, name := range []string{"current_timestamp", "localtime", "rand"} {
		f, err := GetFunctionByName(context.Background(), name, nil)
		require.NoError(t, err)
		expr := &plan.Expr{Expr: &plan.Expr_F{F: &plan.Function{Func: &plan.ObjectRef{Obj: f.GetEncodedOverloadID(), ObjName: name}}}}
		require.False(t, IsConstant(expr, false), "prepare-time fold must retain statement time and volatility")
		require.Equal(t, name != "rand", IsStatementConstant(expr))
		if name == "rand" {
			require.Equal(t, ZoneMapUnsupported, GetZoneMapEvaluation(expr.GetF()))
		} else {
			require.Equal(t, ZoneMapConstant, GetZoneMapEvaluation(expr.GetF()))
		}
	}
}

func TestRuntimeConstantKeepsLazyFoldingPolicy(t *testing.T) {
	literal := &plan.Expr{Typ: plan.Type{Id: int32(types.T_int64)}, Expr: &plan.Expr_Lit{Lit: &plan.Literal{Value: &plan.Literal_I64Val{I64Val: 1}}}}
	wrap := func(id int32, args ...*plan.Expr) *plan.Expr {
		return &plan.Expr{Typ: literal.Typ, Expr: &plan.Expr_F{F: &plan.Function{Func: &plan.ObjectRef{Obj: EncodeOverloadID(id, 0)}, Args: args}}}
	}
	parameter := &plan.Expr{Typ: literal.Typ, Expr: &plan.Expr_P{P: &plan.ParamRef{}}}
	column := &plan.Expr{Typ: literal.Typ, Expr: &plan.Expr_Col{Col: &plan.ColRef{}}}
	condition := &plan.Expr{Typ: plan.Type{Id: int32(types.T_bool)}, Expr: &plan.Expr_Lit{Lit: &plan.Literal{Value: &plan.Literal_Bval{Bval: true}}}}
	selector := wrap(CASE, condition, literal, literal)
	for _, tc := range []struct {
		name    string
		expr    *plan.Expr
		runtime bool
	}{
		{"constant case", selector, true},
		{"nested case", wrap(ABS, selector), true},
		{"parameter case", wrap(CASE, condition, parameter, literal), true},
		{"column case", wrap(CASE, condition, column, literal), false},
		{"volatile inactive branch", wrap(CASE, condition, literal, wrap(RANDOM)), false},
		{"list case", &plan.Expr{Expr: &plan.Expr_List{List: &plan.ExprList{List: []*plan.Expr{selector}}}}, true},
		{"aggregate", wrap(SUM, literal), false},
		{"window", wrap(ROW_NUMBER), false},
		{"invalid identity", wrap(-1), false},
		{"nil child", wrap(CASE, condition, nil, literal), false},
	} {
		t.Run(tc.name, func(t *testing.T) {
			require.Equal(t, tc.runtime, IsRuntimeConstant(tc.expr))
			if tc.runtime {
				require.False(t, IsConstant(tc.expr, true))
				require.False(t, IsStatementConstant(tc.expr))
			}
		})
	}
}

func TestZoneMapArithmeticScaleDomain(t *testing.T) {
	for _, oid := range []types.T{types.T_int64, types.T_uint64, types.T_float64, types.T_decimal64, types.T_decimal128} {
		for _, id := range []int32{PLUS, MINUS, MULTI} {
			left := &plan.Expr{Typ: plan.Type{Id: int32(oid), Scale: 0}, Expr: &plan.Expr_Col{Col: &plan.ColRef{}}}
			right := &plan.Expr{Typ: plan.Type{Id: int32(oid), Scale: 1}, Expr: &plan.Expr_Col{Col: &plan.ColRef{ColPos: 1}}}
			fn := &plan.Function{Func: &plan.ObjectRef{Obj: encodeOverloadID(id, 0)}, Args: []*plan.Expr{left, right}}
			expected := ZoneMapIndex
			if oid.IsDecimal() {
				expected = ZoneMapUnsupported
			}
			require.Equal(t, expected, GetZoneMapEvaluation(fn), "%s arithmetic %d scale annotations", oid, id)
			right.Typ.Scale = left.Typ.Scale
			require.Equal(t, ZoneMapIndex, GetZoneMapEvaluation(fn))
		}
	}
}
