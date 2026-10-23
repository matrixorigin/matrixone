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
	"fmt"
	"math/big"
	"strings"
	"testing"

	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/testutil"
	"github.com/stretchr/testify/require"
)

func TestDecimalMultiplyOperandRange(t *testing.T) {
	for _, tc := range []struct {
		width, scale int32
		want         decimalOperandRange
	}{
		{0, 0, decimalRangeUnknown}, {-1, 0, decimalRangeUnknown},
		{9, 0, decimalRangeInt32}, {9, 9, decimalRangeInt32},
		{10, 0, decimalRangeInt64}, {18, 18, decimalRangeInt64},
		{19, 0, decimalRangeUnknown}, {65, 0, decimalRangeUnknown},
		{9, -1, decimalRangeUnknown}, {9, 10, decimalRangeUnknown},
	} {
		require.Equal(t, tc.want, decimal256OperandRange(types.New(types.T_decimal256, tc.width, tc.scale)), "precision %d, scale %d", tc.width, tc.scale)
	}
	require.Equal(t, decimalRangeUnknown, decimal256OperandRange(types.New(types.T_decimal128, 9, 0)))
}

func TestDecimalMultiplyRangePreservesCoefficientAndMetadata(t *testing.T) {
	for _, width := range []int32{9, 10, 18, 19} {
		for _, scale := range []int32{0, min(width, 18)} {
			for _, shape := range []string{"vector", "left_constant", "right_constant", "null", "selection"} {
				t.Run(fmt.Sprintf("precision_%d/scale_%d/%s", width, scale, shape), func(t *testing.T) {
					proc := testutil.NewProcess(t)
					t.Cleanup(proc.Free)
					parse := func(s string) types.Decimal256 {
						v, err := types.ParseDecimal256(s, width, 0)
						require.NoError(t, err)
						return v
					}
					coefficient := strings.Repeat("9", int(width))
					leftText, rightText := []string{coefficient, "-" + coefficient, "0"}, []string{"3", "-3", "1"}
					if shape == "left_constant" {
						leftText = leftText[:1]
					}
					if shape == "right_constant" {
						rightText = rightText[:1]
					}
					left, right := make([]types.Decimal256, len(leftText)), make([]types.Decimal256, len(rightText))
					for i, s := range leftText {
						left[i] = parse(s)
					}
					for i, s := range rightText {
						right[i] = parse(s)
					}
					desiredScale := min(max(scale, int32(12)), 2*scale)
					want := make([]types.Decimal256, 3)
					// Independent integer multiplication and half-away-from-zero
					// rounding; the production narrow kernels are not the oracle.
					for i := range want {
						a, _ := new(big.Int).SetString(leftText[i%len(leftText)], 10)
						b, _ := new(big.Int).SetString(rightText[i%len(rightText)], 10)
						p := new(big.Int).Mul(a, b)
						negative := p.Sign() < 0
						p.Abs(p)
						if reduction := 2*scale - desiredScale; reduction > 0 {
							d := new(big.Int).Exp(big.NewInt(10), big.NewInt(int64(reduction)), nil)
							q, rem := new(big.Int), new(big.Int)
							q.QuoRem(p, d, rem)
							if rem.Lsh(rem, 1).Cmp(d) >= 0 {
								q.Add(q, big.NewInt(1))
							}
							p = q
						}
						if negative {
							p.Neg(p)
						}
						var err error
						want[i], err = types.ParseDecimal256(p.String(), 76, 0)
						require.NoError(t, err)
					}
					typ := types.New(types.T_decimal256, width, scale)
					l, r := NewFunctionTestInput(typ, left, nil), NewFunctionTestInput(typ, right, nil)
					l.isConst, r.isConst = shape == "left_constant", shape == "right_constant"
					var masked []bool
					var selection *FunctionSelectList
					if shape == "null" {
						l.nullList = []bool{false, true, false}
						masked = []bool{false, true, false}
					}
					if shape == "selection" {
						selection = &FunctionSelectList{AnyNull: true, SelectList: []bool{true, false, true}}
						masked = []bool{false, true, false}
					}
					resultType := types.New(types.T_decimal256, 2*width, desiredScale)
					fc := NewFunctionTestCase(proc, []FunctionTestInput{l, r}, NewFunctionTestResult(resultType, false, want, masked), multiFn).WithSelectList(selection)
					t.Cleanup(fc.Free)
					fc.fnLength = 3
					ok, info := fc.Run()
					require.True(t, ok, info)
					require.Equal(t, resultType, *fc.GetResultVectorDirectly().GetType())
				})
			}
		}
	}
}

func BenchmarkDecimalMultiplyRange(b *testing.B) {
	for _, tc := range []struct {
		name        string
		width       int32
		coefficient string
		rightWidth  int32
	}{
		{"proven32", 9, "12345", 9}, {"proven64", 18, "123456789012345", 18},
		{"unknown_small", 38, "12345", 38}, {"wide_fallback", 38, "123456789012345678901234567890", 38},
		{"q1_operand_domains", 15, "123456789012345", 38},
	} {
		b.Run(tc.name, func(b *testing.B) {
			proc := testutil.NewProcess(b)
			defer proc.Free()
			v, err := types.ParseDecimal256(tc.coefficient, tc.width, 0)
			require.NoError(b, err)
			product, _ := new(big.Int).SetString(tc.coefficient, 10)
			product.Mul(product, new(big.Int).Set(product))
			p, err := types.ParseDecimal256(product.String(), 76, 0)
			require.NoError(b, err)
			values, wanted := make([]types.Decimal256, 256), make([]types.Decimal256, 256)
			for i := range values {
				values[i], wanted[i] = v, p
			}
			typ := types.New(types.T_decimal256, tc.width, 0)
			fc := NewFunctionTestCase(proc, []FunctionTestInput{NewFunctionTestInput(typ, values, nil), NewFunctionTestInput(types.New(types.T_decimal256, tc.rightWidth, 0), values, nil)}, NewFunctionTestResult(types.New(types.T_decimal256, min(tc.width+tc.rightWidth, 65), 0), false, wanted, nil), multiFn)
			defer fc.Free()
			fc.Benchmark(b)
		})
	}
}
