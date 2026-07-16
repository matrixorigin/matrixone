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

func TestResolveNumericBinaryTypesContextPriority(t *testing.T) {
	decimalTarget := types.New(types.T_decimal128, 30, 0)
	tests := []struct {
		name       string
		left       types.Type
		right      types.Type
		outer      *types.Type
		wantInputs types.T
		wantWidth  int32
		wantScale  int32
	}{
		{
			name:       "no context defaults to double",
			left:       types.T_any.ToType(),
			right:      types.T_any.ToType(),
			wantInputs: types.T_float64,
		},
		{
			name:       "exact outer context resolves parameters",
			left:       types.T_any.ToType(),
			right:      types.T_any.ToType(),
			outer:      &decimalTarget,
			wantInputs: types.T_decimal128,
		},
		{
			name:       "decimal sibling overrides outer context",
			left:       types.T_any.ToType(),
			right:      types.New(types.T_decimal64, 12, 2),
			outer:      typePtr(types.T_int64.ToType()),
			wantInputs: types.T_decimal64,
		},
		{
			name:       "double sibling overrides exact outer context",
			left:       types.T_any.ToType(),
			right:      types.T_float64.ToType(),
			outer:      &decimalTarget,
			wantInputs: types.T_float64,
		},
		{
			name:       "exact outer context overrides integer sibling",
			left:       types.T_uint64.ToType(),
			right:      types.T_any.ToType(),
			outer:      &decimalTarget,
			wantInputs: types.T_decimal128,
		},
		{
			name:       "integer outer context overrides narrower integer sibling",
			left:       types.T_uint32.ToType(),
			right:      types.T_any.ToType(),
			outer:      typePtr(types.T_int64.ToType()),
			wantInputs: types.T_int64,
		},
		{
			name:       "integer sibling preserves integer domain without outer context",
			left:       types.T_uint64.ToType(),
			right:      types.T_any.ToType(),
			wantInputs: types.T_uint64,
		},
		{
			name:       "wider integer sibling wins independent of traversal order",
			left:       types.T_int32.ToType(),
			right:      types.T_int64.ToType(),
			wantInputs: types.T_int64,
		},
		{
			name:       "wider integer sibling wins in reverse order",
			left:       types.T_int64.ToType(),
			right:      types.T_int32.ToType(),
			wantInputs: types.T_int64,
		},
		{
			name:       "signed and unsigned bigint use exact decimal domain",
			left:       types.T_int64.ToType(),
			right:      types.T_uint64.ToType(),
			wantInputs: types.T_decimal128,
			wantWidth:  38,
		},
		{
			name:       "mixed integers widen to a signed type that contains both",
			left:       types.T_int16.ToType(),
			right:      types.T_uint32.ToType(),
			wantInputs: types.T_int64,
		},
		{
			name:       "small signed plus unsigned bigint uses exact decimal domain",
			left:       types.T_int8.ToType(),
			right:      types.T_uint64.ToType(),
			wantInputs: types.T_decimal128,
			wantWidth:  38,
		},
		{
			name:       "decimal siblings merge precision and scale",
			left:       types.New(types.T_decimal64, 10, 2),
			right:      types.New(types.T_decimal128, 30, 10),
			wantInputs: types.T_decimal128,
			wantWidth:  30,
			wantScale:  10,
		},
		{
			name:       "decimal siblings merge precision and scale in reverse order",
			left:       types.New(types.T_decimal128, 30, 10),
			right:      types.New(types.T_decimal64, 10, 2),
			wantInputs: types.T_decimal128,
			wantWidth:  30,
			wantScale:  10,
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			got, ok := resolveNumericBinaryTypes(numericOpAdd, test.left, test.right, test.outer)
			require.True(t, ok)
			if test.left.Oid == types.T_any || test.right.Oid == types.T_any {
				require.Equal(t, test.wantInputs, got.left.Oid)
				require.Equal(t, test.wantInputs, got.right.Oid)
			}
			paramType, ok := InferNumericParameterType([]types.Type{test.left, test.right}, test.outer)
			require.True(t, ok)
			require.Equal(t, test.wantInputs, paramType.Oid)
			if test.wantWidth != 0 {
				require.Equal(t, test.wantWidth, paramType.Width)
			}
			if test.wantScale != 0 {
				require.Equal(t, test.wantScale, paramType.Scale)
			}
		})
	}
}

func TestResolveNumericBinaryTypesResult(t *testing.T) {
	tests := []struct {
		name      string
		op        numericBinaryOp
		left      types.Type
		right     types.Type
		wantType  types.T
		wantWidth int32
		wantScale int32
	}{
		{
			name:      "decimal add uses maximum scale",
			op:        numericOpAdd,
			left:      types.New(types.T_decimal64, 10, 2),
			right:     types.New(types.T_decimal64, 12, 4),
			wantType:  types.T_decimal64,
			wantWidth: 18,
			wantScale: 4,
		},
		{
			name:      "decimal subtract uses maximum scale",
			op:        numericOpSub,
			left:      types.New(types.T_decimal128, 20, 3),
			right:     types.New(types.T_decimal128, 20, 5),
			wantType:  types.T_decimal128,
			wantWidth: 38,
			wantScale: 5,
		},
		{
			name:      "decimal multiply widens storage",
			op:        numericOpMul,
			left:      types.New(types.T_decimal64, 10, 2),
			right:     types.New(types.T_decimal64, 10, 3),
			wantType:  types.T_decimal128,
			wantWidth: 38,
			wantScale: 5,
		},
		{
			name:      "decimal division adds fractional capacity",
			op:        numericOpDiv,
			left:      types.New(types.T_decimal128, 30, 4),
			right:     types.New(types.T_decimal128, 30, 2),
			wantType:  types.T_decimal128,
			wantWidth: 38,
			wantScale: 10,
		},
		{
			name:      "integer div returns signed bigint",
			op:        numericOpIntegerDiv,
			left:      types.T_uint64.ToType(),
			right:     types.T_uint64.ToType(),
			wantType:  types.T_int64,
			wantWidth: types.T_int64.ToType().Width,
		},
		{
			name:      "decimal mod keeps maximum scale",
			op:        numericOpMod,
			left:      types.New(types.T_decimal128, 20, 2),
			right:     types.New(types.T_decimal128, 20, 6),
			wantType:  types.T_decimal128,
			wantWidth: 20,
			wantScale: 6,
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			got, ok := resolveNumericBinaryTypes(test.op, test.left, test.right, nil)
			require.True(t, ok)
			require.Equal(t, test.wantType, got.result.Oid)
			require.Equal(t, test.wantWidth, got.result.Width)
			require.Equal(t, test.wantScale, got.result.Scale)
		})
	}
}

func TestResolveNumericBinaryTypesRejectsNonNumericInput(t *testing.T) {
	_, ok := resolveNumericBinaryTypes(
		numericOpAdd,
		types.T_varchar.ToType(),
		types.T_any.ToType(),
		nil,
	)
	require.False(t, ok)
}

func TestInferNumericParameterTypeRejectsUnrepresentableDecimalDomain(t *testing.T) {
	_, ok := InferNumericParameterType([]types.Type{
		types.New(types.T_decimal256, 76, 0),
		types.New(types.T_decimal256, 76, 76),
	}, nil)
	require.False(t, ok)
}

func TestNumericIntegerTypeHelpers(t *testing.T) {
	for _, test := range []struct {
		minBits int
		want    types.T
	}{
		{minBits: 1, want: types.T_int8},
		{minBits: 9, want: types.T_int16},
		{minBits: 17, want: types.T_int32},
		{minBits: 33, want: types.T_int64},
	} {
		require.Equal(t, test.want, signedIntegerType(test.minBits).Oid)
	}
	for _, test := range []struct {
		minBits int
		want    types.T
	}{
		{minBits: 1, want: types.T_uint8},
		{minBits: 9, want: types.T_uint16},
		{minBits: 17, want: types.T_uint32},
		{minBits: 33, want: types.T_uint64},
	} {
		require.Equal(t, test.want, unsignedIntegerType(test.minBits).Oid)
	}

	for _, oid := range []types.T{
		types.T_int8, types.T_int16, types.T_int32, types.T_int64,
		types.T_uint8, types.T_uint16, types.T_uint32, types.T_uint64,
	} {
		bits, _ := integerTypeBits(oid)
		require.NotZero(t, bits)
	}
	bits, signed := integerTypeBits(types.T_float64)
	require.Zero(t, bits)
	require.False(t, signed)
}

func TestResolveNumericBinaryTypesByName(t *testing.T) {
	tests := []struct {
		name       string
		wantResult types.T
		wantOK     bool
	}{
		{name: "+", wantResult: types.T_int64, wantOK: true},
		{name: "-", wantResult: types.T_int64, wantOK: true},
		{name: "*", wantResult: types.T_int64, wantOK: true},
		{name: "/", wantResult: types.T_float64, wantOK: true},
		{name: "DIV", wantResult: types.T_int64, wantOK: true},
		{name: "%", wantResult: types.T_int64, wantOK: true},
		{name: "MOD", wantResult: types.T_int64, wantOK: true},
		{name: "abs", wantOK: false},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			left, right, result, ok := ResolveNumericBinaryTypes(
				test.name,
				types.T_int64.ToType(),
				types.T_int64.ToType(),
				nil,
			)
			require.Equal(t, test.wantOK, ok)
			if !test.wantOK {
				return
			}
			require.Equal(t, test.wantResult, result.Oid)
			require.NotEqual(t, types.T_any, left.Oid)
			require.NotEqual(t, types.T_any, right.Oid)
		})
	}
}

func typePtr(typ types.Type) *types.Type {
	return &typ
}
