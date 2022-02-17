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

package float64s

import (
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"math/rand"
	"testing"
)

const (
	Num  = 50
	Frac = 100
)

func generate() ([]float64, []int64) {
	os := make([]int64, Num)
	xs := make([]float64, Num)
	{
		for i := 0; i < Num; i++ {
			os[i] = int64(i)
			xs[i] = rand.Float64() * Frac
		}
	}
	return xs, os
}

func TestSort(t *testing.T) {
	vs, os := generate()
	Sort(vs, os)
	for i := 1; i < len(os); i++ {
		require.GreaterOrEqual(t, vs[os[i]], vs[os[i-1]])
	}
}

func TestHeapSort(t *testing.T) {
	vs, os := generate()
	heapSort(vs, os, 0, len(vs))
	for i := 1; i < len(os); i++ {
		require.GreaterOrEqual(t, vs[os[i]], vs[os[i-1]])
	}
}

func TestMedianOfThree(t *testing.T) {
	vs, os := generate()
	medianOfThree(vs, os, 0, 1, 2)
	assert.True(t, (vs[os[0]] >= vs[os[1]] && vs[os[0]] <= vs[os[2]]) || (vs[os[0]] <= vs[os[1]] && vs[os[0]] >= vs[os[2]]))
	medianOfThree(vs, os, 5, 6, 7)
	assert.True(t, (vs[os[5]] >= vs[os[6]] && vs[os[5]] <= vs[os[7]]) || (vs[os[5]] <= vs[os[6]] && vs[os[5]] >= vs[os[7]]))
}

func TestSwapRange(t *testing.T) {
	vs, os := generate()
	osOriginal := make([]int64, len(os))
	copy(osOriginal, os)
	swapRange(vs, os, 0, 10, 10)
	require.Equal(t, osOriginal[:10], os[10:20])
}
