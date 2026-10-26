// Copyright 2026 Matrix Origin
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
// http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package function

import (
	"context"
	"testing"

	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/testutil"
	"github.com/stretchr/testify/require"
)

// MySQL treats TRUE/FALSE as 1/0 next to a DECIMAL, the same way it already
// works next to an integer: arithmetic and comparison must bind, casting the
// BOOL operand to the DECIMAL side.
func TestDecimalBoolArithmeticAndComparisonBind(t *testing.T) {
	ctx := context.Background()
	boolType := types.T_bool.ToType()
	for _, dec := range []types.Type{
		types.New(types.T_decimal64, 10, 2),
		types.New(types.T_decimal128, 30, 4),
		types.New(types.T_decimal256, 60, 4),
	} {
		for _, name := range []string{"+", "-", "*", "/", "%", "div", "=", "<>", ">", "<", ">=", "<="} {
			if name == "div" && dec.Oid == types.T_decimal256 {
				continue // INT64 DIV DECIMAL256 is not bound either.
			}
			for _, args := range [][]types.Type{{dec, boolType}, {boolType, dec}} {
				if (name == "/" || name == "div") && dec.Oid == types.T_decimal256 && args[0].Oid == types.T_bool {
					continue // INT64 / DECIMAL256 is not bound either.
				}
				resolved, err := GetFunctionByName(ctx, name, args)
				require.NoError(t, err, "%s %s", dec.Oid, name)
				require.True(t, resolved.needCast, "%s %s", dec.Oid, name)
				require.True(t, resolved.targetTypes[0].Oid.IsDecimal(), "%s %s", dec.Oid, name)
				require.Equal(t, resolved.targetTypes[0].Oid, resolved.targetTypes[1].Oid, "%s %s", dec.Oid, name)
			}
		}
	}
}

func TestBoolToDecimalCast(t *testing.T) {
	proc := testutil.NewProcess(t)
	d64 := types.New(types.T_decimal64, 10, 2)
	one64, _ := types.Decimal64(1).Scale(2)
	d128 := types.New(types.T_decimal128, 30, 4)
	one128, _ := types.Decimal128{B0_63: 1}.Scale(4)
	d256 := types.New(types.T_decimal256, 60, 4)
	one256, _ := types.Decimal256FromDecimal128(types.Decimal128{B0_63: 1}).Scale(4)
	for _, tc := range []struct {
		name   string
		to     types.Type
		target any
		want   any
	}{
		{"decimal64", d64, []types.Decimal64{}, []types.Decimal64{one64, 0, 0}},
		{"decimal128", d128, []types.Decimal128{}, []types.Decimal128{one128, {}, {}}},
		{"decimal256", d256, []types.Decimal256{}, []types.Decimal256{one256, {}, {}}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			fc := NewFunctionTestCase(proc,
				[]FunctionTestInput{
					NewFunctionTestInput(types.T_bool.ToType(), []bool{true, false, true}, []bool{false, false, true}),
					NewFunctionTestInput(tc.to, tc.target, nil),
				},
				NewFunctionTestResult(tc.to, false, tc.want, []bool{false, false, true}),
				NewCast)
			ok, info := fc.RunAndFree()
			require.True(t, ok, info)
		})
	}
}

// CAST(TRUE AS DECIMAL(1,1)) clamps to the column maximum like CAST(1 AS ...).
func TestBoolToDecimalExplicitCastClamps(t *testing.T) {
	proc := testutil.NewProcess(t)
	d := types.New(types.T_decimal64, 1, 1)
	fc := NewFunctionTestCase(proc,
		[]FunctionTestInput{
			NewFunctionTestInput(types.T_bool.ToType(), []bool{true, false}, nil),
			NewFunctionTestInput(d, []types.Decimal64{}, nil),
		},
		NewFunctionTestResult(d, false, []types.Decimal64{9, 0}, nil),
		NewExplicitCast)
	ok, info := fc.RunAndFree()
	require.True(t, ok, info)
}
