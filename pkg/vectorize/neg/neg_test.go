// Copyright 2021 Matrix Origin
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

package neg

import (
	"fmt"
	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/stretchr/testify/require"
	"testing"
)

func makeIbuffer(l int) []int64 {
	buf := make([]int64, l)
	for i := range buf {
		buf[i] = int64(i)
	}
	return buf
}

func makeFbuffer(l int) []float32 {
	buf := make([]float32, l)
	for i := range buf {
		buf[i] = float32(i)
	}
	return buf
}

func TestF64Sum(t *testing.T) {
	xs := makeFbuffer(100)
	rs := make([]float32, 100)
	fmt.Printf("float neg: %v\n", Float32Neg(xs, rs))
	fmt.Printf("pure float neg: %v\n", NumericNeg(xs, rs))
}

func TestI64Sum(t *testing.T) {
	xs := makeIbuffer(100)
	rs := make([]int64, 100)
	fmt.Printf("int neg: %v\n", Int64Neg(xs, rs))
	fmt.Printf("pure int neg: %v\n", NumericNeg(xs, rs))
}

func TestDecimal64Neg(t *testing.T) {
	xs := []types.Decimal64{123, 234, 345, 0, -234}
	rs := make([]types.Decimal64, len(xs))
	rs = Decimal64Neg(xs, rs)
	expectedResult := []types.Decimal64{-123, -234, -345, 0, 234}
	require.Equal(t, expectedResult, rs)
}

func TestDecimal128Neg(t *testing.T) {
	xs := make([]types.Decimal128, 5)
	xs[0], _ = types.ParseStringToDecimal128("123456.789", 20, 5)
	xs[1], _ = types.ParseStringToDecimal128("120000.789", 20, 5)
	xs[2], _ = types.ParseStringToDecimal128("-123456.789", 20, 5)
	xs[3], _ = types.ParseStringToDecimal128("0", 20, 5)
	xs[4], _ = types.ParseStringToDecimal128("-123", 20, 5)
	rs := make([]types.Decimal128, 5)
	rs = Decimal128Neg(xs, rs)

	require.Equal(t, "-123456.78900", string(rs[0].Decimal128ToString(5)))

	require.Equal(t, "-120000.78900", string(rs[1].Decimal128ToString(5)))

	require.Equal(t, "123456.78900", string(rs[2].Decimal128ToString(5)))

	require.Equal(t, "0", string(rs[3].Decimal128ToString(5)))

	require.Equal(t, "123.00000", string(rs[4].Decimal128ToString(5)))
}
