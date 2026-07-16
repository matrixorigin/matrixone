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
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			got, ok := resolveNumericBinaryTypes(numericOpAdd, test.left, test.right, test.outer)
			require.True(t, ok)
			require.Equal(t, test.wantInputs, got.left.Oid)
			require.Equal(t, test.wantInputs, got.right.Oid)
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
