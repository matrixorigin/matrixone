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
	"math"
	"testing"

	"github.com/matrixorigin/matrixone/pkg/pb/plan"
	"github.com/stretchr/testify/require"
)

func TestGetExprNdvModulo(t *testing.T) {
	col := &plan.Expr{Expr: &plan.Expr_Col{Col: &plan.ColRef{RelPos: 0, ColPos: 0, Name: "value"}}}
	literal := func(value *plan.Literal) *plan.Expr {
		return &plan.Expr{Expr: &plan.Expr_Lit{Lit: value}}
	}
	expr := func(name string, args ...*plan.Expr) *plan.Expr {
		return &plan.Expr{Expr: &plan.Expr_F{F: &plan.Function{Func: &plan.ObjectRef{ObjName: name}, Args: args}}}
	}

	integerLiterals := []struct {
		name string
		lit  *plan.Literal
	}{
		{"int8", &plan.Literal{Value: &plan.Literal_I8Val{I8Val: 7}}},
		{"int16", &plan.Literal{Value: &plan.Literal_I16Val{I16Val: 7}}},
		{"int32", &plan.Literal{Value: &plan.Literal_I32Val{I32Val: 7}}},
		{"int64", &plan.Literal{Value: &plan.Literal_I64Val{I64Val: 7}}},
		{"uint8", &plan.Literal{Value: &plan.Literal_U8Val{U8Val: 7}}},
		{"uint16", &plan.Literal{Value: &plan.Literal_U16Val{U16Val: 7}}},
		{"uint32", &plan.Literal{Value: &plan.Literal_U32Val{U32Val: 7}}},
		{"uint64", &plan.Literal{Value: &plan.Literal_U64Val{U64Val: 7}}},
	}
	for _, tc := range integerLiterals {
		for _, op := range []string{"%", "mod"} {
			t.Run(op+"/"+tc.name, func(t *testing.T) {
				builder := newStatsTestBuilderWithNDV("value", 100)
				require.Equal(t, 7.0, getExprNdv(expr(op, col, literal(tc.lit)), builder))
			})
		}
	}

	for _, tc := range []struct {
		name string
		ndv  float64
		want float64
	}{
		{"column smaller than modulus", 3, 3},
		{"column equals modulus", 7, 7},
		{"zero column NDV", 0, 0},
		{"unknown column NDV", -1, -1},
	} {
		t.Run(tc.name, func(t *testing.T) {
			builder := newStatsTestBuilderWithNDV("value", tc.ndv)
			require.Equal(t, tc.want, getExprNdv(expr("%", col, literal(integerLiterals[3].lit)), builder))
		})
	}

	for _, tc := range []struct {
		name string
		arg  *plan.Expr
	}{
		{"signed zero", literal(&plan.Literal{Value: &plan.Literal_I64Val{I64Val: 0}})},
		{"unsigned zero", literal(&plan.Literal{Value: &plan.Literal_U64Val{U64Val: 0}})},
		{"negative", literal(&plan.Literal{Value: &plan.Literal_I64Val{I64Val: -5}})},
		{"uint64 overflow", literal(&plan.Literal{Value: &plan.Literal_U64Val{U64Val: math.MaxUint64}})},
		{"non-integer literal", literal(&plan.Literal{Value: &plan.Literal_Dval{Dval: 7}})},
		{"non-literal", col},
	} {
		t.Run(tc.name, func(t *testing.T) {
			builder := newStatsTestBuilderWithNDV("value", 100)
			require.Equal(t, 100.0, getExprNdv(expr("mod", col, tc.arg), builder))
		})
	}

	t.Run("uint64 maximum accepted value", func(t *testing.T) {
		builder := newStatsTestBuilderWithNDV("value", 2e19)
		max := literal(&plan.Literal{Value: &plan.Literal_U64Val{U64Val: math.MaxInt64}})
		require.Equal(t, float64(math.MaxInt64), getExprNdv(expr("%", col, max), builder))
	})

	t.Run("missing second argument", func(t *testing.T) {
		builder := newStatsTestBuilderWithNDV("value", 100)
		require.Equal(t, 100.0, getExprNdv(expr("%", col), builder))
	})
}
