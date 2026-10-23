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
	"testing"

	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/testutil"
	"github.com/stretchr/testify/require"
)

func TestDecimalMultiplyConstantBroadcastAndReuse(t *testing.T) {
	const length = 17
	proc := testutil.NewProcess(t)
	t.Cleanup(proc.Free)
	typ := types.New(types.T_decimal256, 9, 0)
	left, right := NewFunctionTestInput(typ, []types.Decimal256{types.Decimal256FromInt64(43)}, nil), NewFunctionTestInput(typ, []types.Decimal256{types.Decimal256FromInt64(23)}, nil)
	left.isConst, right.isConst = true, true
	want := make([]types.Decimal256, length)
	mask := make([]bool, length)
	selected := make([]bool, length)
	for i := range want {
		want[i] = types.Decimal256FromInt64(989)
		selected[i] = i != 0 && i != 6
		mask[i] = !selected[i]
	}
	fc := NewFunctionTestCase(proc, []FunctionTestInput{left, right}, NewFunctionTestResult(types.New(types.T_decimal256, 18, 0), false, want, mask), multiFn).WithSelectList(&FunctionSelectList{AnyNull: true, SelectList: selected})
	t.Cleanup(fc.Free)
	fc.fnLength = length
	ok, info := fc.Run()
	require.True(t, ok, info)
	require.True(t, fc.parameters[0].GetNulls().IsEmpty())
	require.True(t, fc.parameters[1].GetNulls().IsEmpty())
	fc.selectList = nil
	fc.expected.nullList = nil
	// Only physical row zero is meaningful for a non-NULL constant. Bits
	// outside that row must not become NULLs in the broadcast result.
	fc.parameters[0].GetNulls().Add(1)
	fc.parameters[1].GetNulls().Add(2)
	ok, info = fc.Run()
	require.True(t, ok, info)
}

func TestDecimalMultiplyNoEvaluableRows(t *testing.T) {
	for _, shape := range []string{"empty", "constant_null", "all_masked"} {
		t.Run(shape, func(t *testing.T) {
			proc := testutil.NewProcess(t)
			t.Cleanup(proc.Free)
			typ := types.New(types.T_decimal256, 65, 0)
			huge, err := types.ParseDecimal256("99999999999999999999999999999999999999999999999999999999999999999", 65, 0)
			require.NoError(t, err)
			length := 3
			leftVals, rightVals := []types.Decimal256{huge}, []types.Decimal256{huge}
			var leftNulls []bool
			if shape == "empty" {
				length = 0
				leftVals = nil
				rightVals = nil
			}
			if shape == "constant_null" {
				leftNulls = []bool{true}
			}
			left, right := NewFunctionTestInput(typ, leftVals, leftNulls), NewFunctionTestInput(typ, rightVals, nil)
			left.isConst, right.isConst = shape != "empty", shape != "empty"
			mask := make([]bool, length)
			for i := range mask {
				mask[i] = true
			}
			fc := NewFunctionTestCase(proc, []FunctionTestInput{left, right}, NewFunctionTestResult(typ, false, make([]types.Decimal256, length), mask), multiFn)
			t.Cleanup(fc.Free)
			fc.fnLength = length
			if shape == "all_masked" {
				fc.selectList = &FunctionSelectList{AnyNull: true, SelectList: make([]bool, length)}
			}
			ok, info := fc.Run()
			require.True(t, ok, info)
		})
	}
}

// Sibling controls guard extraction of the common Decimal256 precision check.
func BenchmarkDecimal256SiblingArithmetic(b *testing.B) {
	for _, op := range []struct {
		name string
		fn   executeLogicOfOverload
		want int64
	}{{"add", plusFn, 22}, {"sub", minusFn, 18}, {"div", divFn, 10}} {
		for _, length := range []int{0, 1, 2, 16, 256, 257, 8192} {
			b.Run(fmt.Sprintf("%s/%d", op.name, length), func(b *testing.B) {
				proc := testutil.NewProcess(b)
				defer proc.Free()
				a, c, want := make([]types.Decimal256, length), make([]types.Decimal256, length), make([]types.Decimal256, length)
				for i := range a {
					a[i] = types.Decimal256FromInt64(20)
					c[i] = types.Decimal256FromInt64(2)
					want[i] = types.Decimal256FromInt64(op.want)
				}
				typ := types.New(types.T_decimal256, 38, 0)
				fc := NewFunctionTestCase(proc, []FunctionTestInput{NewFunctionTestInput(typ, a, nil), NewFunctionTestInput(typ, c, nil)}, NewFunctionTestResult(types.New(types.T_decimal256, 65, 0), false, want, nil), op.fn)
				defer fc.Free()
				fc.Benchmark(b)
			})
		}
	}
}

func BenchmarkDecimalMultiplyDispatch(b *testing.B) {
	for _, domain := range []struct {
		name        string
		width       int32
		coefficient string
	}{{"p9", 9, "12345"}, {"p18", 18, "123456789012345"}, {"unknown_small", 38, "12345"}, {"wide", 38, "123456789012345678901234567890"}} {
		for _, shape := range []string{"one", "large", "left_constant", "right_constant", "both_constant", "allnull", "empty", "tiny2", "tiny8", "tiny16", "tail255", "tail257"} {
			b.Run(fmt.Sprintf("%s/%s", domain.name, shape), func(b *testing.B) {
				n := 256
				switch shape {
				case "empty":
					n = 0
				case "tiny2":
					n = 2
				case "tiny8":
					n = 8
				case "tiny16":
					n = 16
				case "tail255":
					n = 255
				case "tail257":
					n = 257
				}
				if shape == "one" || shape == "both_constant" {
					n = 1
				}
				if shape == "large" {
					n = 8192
				}
				v, err := types.ParseDecimal256(domain.coefficient, domain.width, 0)
				if err != nil {
					b.Fatal(err)
				}
				product, _ := new(big.Int).SetString(domain.coefficient, 10)
				product.Mul(product, new(big.Int).Set(product))
				p, err := types.ParseDecimal256(product.String(), 76, 0)
				if err != nil {
					b.Fatal(err)
				}
				makeValues := func(count int, value types.Decimal256) []types.Decimal256 {
					a := make([]types.Decimal256, count)
					for i := range a {
						a[i] = value
					}
					return a
				}
				leftN, rightN := n, n
				if shape == "left_constant" || shape == "both_constant" {
					leftN = 1
				}
				if shape == "right_constant" || shape == "both_constant" {
					rightN = 1
				}
				var mask []bool
				if shape == "allnull" {
					mask = make([]bool, n)
					for i := range mask {
						mask[i] = true
					}
				}
				typ := types.New(types.T_decimal256, domain.width, 0)
				left, right := NewFunctionTestInput(typ, makeValues(leftN, v), mask), NewFunctionTestInput(typ, makeValues(rightN, v), nil)
				left.isConst = leftN == 1 && shape != "one"
				right.isConst = rightN == 1 && shape != "one"
				proc := testutil.NewProcess(b)
				defer proc.Free()
				fc := NewFunctionTestCase(proc, []FunctionTestInput{left, right}, NewFunctionTestResult(types.New(types.T_decimal256, min(2*domain.width, 65), 0), false, makeValues(n, p), mask), multiFn)
				defer fc.Free()
				fc.fnLength = n
				fc.Benchmark(b)
			})
		}
	}
}
