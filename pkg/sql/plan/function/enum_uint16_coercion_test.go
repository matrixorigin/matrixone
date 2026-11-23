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

package function

import (
	"testing"

	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/stretchr/testify/require"
)

// TestEnumSmallintUnsignedNumericContextBind is the coercion-level regression
// for the issue #31901 repro:
//
//	create table t(e enum('a','b','c'), s smallint unsigned, ...);
//	insert into t values ('b', 2, ...);
//	SELECT e=s / WHERE e=s / WHERE s=e / SELECT e<s / e+s / s*e;
//
// fixedTypeCastRule1 evaluates an ENUM operand by its uint16 ordinal, so an
// ENUM paired with a SMALLINT UNSIGNED operand landed on the equal-type
// diagonal, which carries no cast rule. The rule then reported "no cast
// needed" while the ENUM operand still had to be cast from its original
// T_enum type to execute: = and <=> bound the mismatched physical vectors and
// panicked in the comparison kernel ("casting SMALLINT UNSIGNED vector to
// []types.Enum"), while < <= > >= and + - * % rejected the pair at bind time
// ("bad value [ENUM SMALLINT UNSIGNED]"). The mixed pair must now select
// uint16 targets in either operand order, with the ENUM side cast from
// T_enum.
func TestEnumSmallintUnsignedNumericContextBind(t *testing.T) {
	enum := types.T_enum.ToType()
	smallintUnsigned := types.T_uint16.ToType()

	comparisons := []string{"=", "<=>", "<>", "<", "<=", ">", ">="}
	for _, name := range comparisons {
		for order, operands := range [][]types.Type{
			{enum, smallintUnsigned},
			{smallintUnsigned, enum},
		} {
			r, ok := GetFunctionByNameWithoutError(name, operands)
			require.True(t, ok, "%s must bind for ENUM/SMALLINT UNSIGNED order %d", name, order)
			require.True(t, r.needCast,
				"%s must cast the ENUM operand for order %d", name, order)
			require.Equal(t, types.T_uint16, r.targetTypes[0].Oid,
				"%s left target for order %d", name, order)
			require.Equal(t, types.T_uint16, r.targetTypes[1].Oid,
				"%s right target for order %d", name, order)
			require.Equal(t, types.T_bool, r.retType.Oid,
				"%s result type for order %d", name, order)
		}
	}

	arithmetic := []string{"+", "-", "*", "%"}
	for _, name := range arithmetic {
		for order, operands := range [][]types.Type{
			{enum, smallintUnsigned},
			{smallintUnsigned, enum},
		} {
			r, ok := GetFunctionByNameWithoutError(name, operands)
			require.True(t, ok, "%s must bind for ENUM/SMALLINT UNSIGNED order %d", name, order)
			require.True(t, r.needCast,
				"%s must cast the ENUM operand for order %d", name, order)
			require.Equal(t, types.T_uint16, r.targetTypes[0].Oid,
				"%s left target for order %d", name, order)
			require.Equal(t, types.T_uint16, r.targetTypes[1].Oid,
				"%s right target for order %d", name, order)
		}
	}

	// The arithmetic ordinal result keeps the uint16 domain. MINUS and MULTI
	// overload 0 return their first target parameter for non-decimal,
	// non-temporal domains (their retType closures route decimals to the
	// decimal add / multiply return types, and route date/datetime/year
	// (MINUS) or year (MULTI) to int64, returning parameters[0] otherwise),
	// so all four arithmetic operators contract to the uint16 target as the
	// result domain.
	for _, name := range []string{"+", "-", "*", "%"} {
		r, ok := GetFunctionByNameWithoutError(name, []types.Type{enum, smallintUnsigned})
		require.True(t, ok, "%s must bind", name)
		require.Equal(t, types.T_uint16, r.retType.Oid, "%s result type", name)
	}
}

// TestEnumNumericCoercionNeighborhoodUnchanged pins the neighboring contracts
// the fix must not disturb: the equal-type diagonals stay cast-free, and
// every other mix keeps its pre-existing fixedBinaryCastRule1 table route.
// These follow the same routes before and after the fix.
func TestEnumNumericCoercionNeighborhoodUnchanged(t *testing.T) {
	enum := types.T_enum.ToType()
	u16 := types.T_uint16.ToType()
	u8 := types.T_uint8.ToType()
	u32 := types.T_uint32.ToType()
	i16 := types.T_int16.ToType()
	f32 := types.T_float32.ToType()
	any := types.T_any.ToType()
	bf16 := types.T_bf16.ToType()

	// A plain uint16/uint16 pair is the equal-type diagonal: no useless cast.
	r, ok := GetFunctionByNameWithoutError("=", []types.Type{u16, u16})
	require.True(t, ok)
	require.False(t, r.needCast)

	// ENUM/ENUM compares directly on the shared identity; the check layer
	// distinguishes member definitions only through column metadata, not the Oid.
	r, ok = GetFunctionByNameWithoutError("=", []types.Type{enum, enum})
	require.True(t, ok)
	require.False(t, r.needCast)

	// Working neighbors keep their table routes and cast domains.
	for _, tc := range []struct {
		name        string
		args        []types.Type
		left, right types.T
	}{
		{"=", []types.Type{enum, u8}, types.T_uint16, types.T_uint16},
		{"=", []types.Type{u8, enum}, types.T_uint16, types.T_uint16},
		{"<", []types.Type{enum, u32}, types.T_uint32, types.T_uint32},
		{"<", []types.Type{u32, enum}, types.T_uint32, types.T_uint32},
		{"<", []types.Type{enum, i16}, types.T_int32, types.T_int32},
		{"<", []types.Type{i16, enum}, types.T_int32, types.T_int32},
		{"<", []types.Type{enum, f32}, types.T_float64, types.T_float64},
	} {
		r, ok := GetFunctionByNameWithoutError(tc.name, tc.args)
		require.True(t, ok, "%s %v must keep binding", tc.name, tc.args)
		require.True(t, r.needCast, "%s %v must keep casting", tc.name, tc.args)
		require.Equal(t, tc.left, r.targetTypes[0].Oid, "%s %v left target", tc.name, tc.args)
		require.Equal(t, tc.right, r.targetTypes[1].Oid, "%s %v right target", tc.name, tc.args)
	}

	// NULL-typed operands keep the existing T_any table routes that already
	// coerced NULL/ENUM pairs: cast both sides to the normalized uint16 domain.
	for _, args := range [][]types.Type{{any, enum}, {enum, any}} {
		r, ok := GetFunctionByNameWithoutError("=", args)
		require.True(t, ok, "\"=\" must bind for %v", args)
		require.True(t, r.needCast)
		require.Equal(t, types.T_uint16, r.targetTypes[0].Oid, "%v left target", args)
		require.Equal(t, types.T_uint16, r.targetTypes[1].Oid, "%v right target", args)
	}

	// The float32 low-precision diagonal keeps forcing its cast.
	r, ok = GetFunctionByNameWithoutError("<", []types.Type{bf16, bf16})
	require.True(t, ok)
	require.True(t, r.needCast)
	require.Equal(t, types.T_float32, r.targetTypes[0].Oid)
	require.Equal(t, types.T_float32, r.targetTypes[1].Oid)
}
