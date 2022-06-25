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

package decimal128s

import (
	"fmt"
	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"testing"
)

const Num = 10

func generate() ([]types.Decimal128, []int64) {
	os := make([]int64, Num)
	xs := make([]types.Decimal128, Num)
	{
		for i := 0; i < Num; i++ {
			os[i] = int64(i)
		}
		xs[0], _ = types.ParseStringToDecimal128("12.34", 38, 3)
		xs[1], _ = types.ParseStringToDecimal128("12.33", 38, 3)
		xs[2], _ = types.ParseStringToDecimal128("-12.33", 38, 3)
		xs[3], _ = types.ParseStringToDecimal128("0", 38, 3)
		xs[4], _ = types.ParseStringToDecimal128("5", 38, 3)
		xs[5], _ = types.ParseStringToDecimal128("12345.678", 38, 3)
		xs[6], _ = types.ParseStringToDecimal128("12345.678", 38, 3)
		xs[7], _ = types.ParseStringToDecimal128("123456.78", 38, 3)
		xs[8], _ = types.ParseStringToDecimal128("-123456.78", 38, 3)
		xs[9], _ = types.ParseStringToDecimal128("-123456.77", 38, 3)
	}
	return xs, os
}

func TestSort(t *testing.T) {
	vs, os := generate()
	for i := 1; i < len(os); i++ {
		fmt.Println(string(vs[os[i]].Decimal128ToString(3)))
	}
	fmt.Println("decimal128 after sorting")
	Sort(vs, os)
	for i := 1; i < len(os); i++ {
		fmt.Println(string(vs[os[i]].Decimal128ToString(3)))
	}
}

func TestHeapSort(t *testing.T) {
	vs, os := generate()
	heapSort(vs, os, 0, len(vs))
	for i := 1; i < len(os); i++ {
		require.GreaterOrEqual(t, types.CompareDecimal128Decimal128Aligned(vs[os[i]], vs[os[i-1]]), int64(0))
	}
}

func TestMedianOfThree(t *testing.T) {
	vs, os := generate()
	medianOfThree(vs, os, 0, 1, 2)
	assert.True(t, (types.CompareDecimal128Decimal128Aligned(vs[os[0]], vs[os[1]]) >= 0 && types.CompareDecimal128Decimal128Aligned(vs[os[0]], vs[os[2]]) <= 0) || (types.CompareDecimal128Decimal128Aligned(vs[os[0]], vs[os[1]]) <= 0 && types.CompareDecimal128Decimal128Aligned(vs[os[0]], vs[os[2]]) >= 0))
	medianOfThree(vs, os, 5, 6, 7)
	assert.True(t, (types.CompareDecimal128Decimal128Aligned(vs[os[5]], vs[os[6]]) >= 0 && types.CompareDecimal128Decimal128Aligned(vs[os[5]], vs[os[7]]) <= 0) || (types.CompareDecimal128Decimal128Aligned(vs[os[5]], vs[os[6]]) <= 0 && types.CompareDecimal128Decimal128Aligned(vs[os[5]], vs[os[7]]) >= 0))
}

func TestSwapRange(t *testing.T) {
	vs, os := generate()
	osOriginal := make([]int64, len(os))
	copy(osOriginal, os)
	swapRange(vs, os, 0, 5, 5)
	require.Equal(t, osOriginal[:5], os[5:10])
}
