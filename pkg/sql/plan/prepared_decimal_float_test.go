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
	"math"
	"testing"

	"github.com/matrixorigin/matrixone/pkg/container/types"
	planpb "github.com/matrixorigin/matrixone/pkg/pb/plan"
	"github.com/stretchr/testify/require"
)

func TestPreparedDecimalFloatProofTypedValues(t *testing.T) {
	for _, tc := range []struct {
		name  string
		typ   types.T
		value any
		want  float64
		valid bool
	}{
		{"signed", types.T_int64, "16001", 16001, true},
		{"signed maximum", types.T_int64, "9223372036854775807", float64(int64(math.MaxInt64)), true},
		{"signed minimum", types.T_int64, "-9223372036854775808", float64(int64(math.MinInt64)), true},
		{"unsigned maximum", types.T_uint64, "18446744073709551615", float64(uint64(math.MaxUint64)), true},
		{"unsigned native", types.T_uint64, uint64(1<<63) + 1025, float64(uint64(1<<63) + 1025), true},
		{"materialized", types.T_int64, ParamValue{Value: "raw", MaterializedValue: "16001"}, 16001, true},
		{"narrow signed maximum", types.T_int8, "127", 127, true},
		{"narrow overflow", types.T_int8, "128", 0, false},
		{"signed overflow", types.T_int64, "18446744073709551615", 0, false},
		{"negative unsigned", types.T_uint64, "-1", 0, false},
		{"NULL", types.T_int64, nil, 0, false},
		{"NULL stale spelling", types.T_int64, ParamValue{MaterializedValue: "16001"}, 0, false},
		{"fraction", types.T_int64, "1.5", 0, false},
		{"floating source", types.T_float64, "16001", 0, false},
		{"floating value", types.T_int64, float64(16001), 0, false},
	} {
		t.Run(tc.name, func(t *testing.T) {
			value, ok := preparedIntegerFloatValue(tc.value, tc.typ)
			require.Equal(t, tc.valid, ok)
			if ok {
				require.Equal(t, tc.want, value)
			}
		})
	}
}

func TestPreparedDecimalFloatProofClassesAndOwnership(t *testing.T) {
	for _, dependent := range []bool{false, true} {
		ctx := withPreparedSourceBindings(context.Background(), []PreparedSourceBinding{{Position: 0, Type: types.T_int64.ToType()}}, []any{int64(7)})
		state := preparedBindingState(ctx)
		state.selectStatement, state.valueDependent = true, dependent
		decimal := types.New(types.T_decimal128, 18, 2)
		column := &Expr{Typ: makePlan2Type(&decimal), Expr: &planpb.Expr_Col{Col: &planpb.ColRef{}}}
		param := &Expr{Typ: makeSimplePlan2Type(types.T_int64), Expr: &planpb.Expr_P{P: &planpb.ParamRef{Pos: 0}}}
		peer, err := makePlan2CastExpr(ctx, param, makeSimplePlan2Type(types.T_float64))
		require.NoError(t, err)
		args, admitted, err := bindPreparedDecimalFloatArguments(ctx, "=", []*Expr{column, peer})
		require.NoError(t, err)
		require.True(t, admitted)
		require.Equal(t, column.Typ, args[1].Typ)
		require.Equal(t, int32(types.T_float64), peer.Typ.Id, "source and sibling retain DOUBLE")
		require.Equal(t, dependent, state.valueDependent, "a sibling's dependency must not be cleared")
		require.Len(t, state.decimalFloatProofs, 1)
		proof := state.decimalFloatProofs[0]
		for _, tc := range []struct {
			value any
			same  bool
		}{{"9", true}, {"1152921504606846976", false}, {nil, false}, {"7", true}} {
			require.Equal(t, tc.same, MatchPreparedDecimalFloatProofs([]PreparedDecimalFloatProof{proof}, state.bindings, []any{tc.value}))
			proof.expectedUnique = !proof.expectedUnique
			if tc.value != nil {
				require.Equal(t, !tc.same, MatchPreparedDecimalFloatProofs([]PreparedDecimalFloatProof{proof}, state.bindings, []any{tc.value}))
			}
			proof.expectedUnique = !proof.expectedUnique
		}
		require.False(t, MatchPreparedDecimalFloatProofs(state.decimalFloatProofs, nil, []any{"7"}))
		require.False(t, MatchPreparedDecimalFloatProofs(state.decimalFloatProofs, state.bindings, nil))
		require.False(t, MatchPreparedDecimalFloatProofs(state.decimalFloatProofs, []PreparedSourceBinding{{Type: types.T_float64.ToType()}}, []any{"7"}))
		// Independent comparisons have independent proof obligations. A hit
		// on position zero cannot authorize a stale second comparison.
		both := []PreparedDecimalFloatProof{proof, {
			position: 1, source: types.T_uint64, column: types.New(types.T_decimal64, 12, 2), expectedUnique: false,
		}}
		bindings := []PreparedSourceBinding{{Position: 0, Type: types.T_int64.ToType()}, {Position: 1, Type: types.T_uint64.ToType()}}
		values := []any{"7", "1152921504606846976"}
		require.True(t, MatchPreparedDecimalFloatProofs(both, bindings, values))
		require.False(t, MatchPreparedDecimalFloatProofs(both, bindings, []any{"7", "8"}), "the second class changed")
		require.False(t, MatchPreparedDecimalFloatProofs(both, bindings[:1], values), "the second binding is missing")
		require.False(t, MatchPreparedDecimalFloatProofs(both, bindings, values[:1]), "the second value is missing")
		state.selectStatement = false
		_, admitted, err = bindPreparedDecimalFloatArguments(ctx, "=", []*Expr{column, peer})
		require.NoError(t, err)
		require.False(t, admitted, "DML keeps its existing domain")
		state.selectStatement = true
		column.Typ.Id = int32(types.T_decimal256)
		_, admitted, err = bindPreparedDecimalFloatArguments(ctx, "=", []*Expr{column, peer})
		require.NoError(t, err)
		require.False(t, admitted, "unsupported DECIMAL256 does not acquire an always-false guard")
	}
}
