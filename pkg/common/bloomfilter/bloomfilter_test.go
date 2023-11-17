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

package bloomfilter

import (
	"math"
	"testing"

	"github.com/bits-and-blooms/bloom"
	"github.com/matrixorigin/matrixone/pkg/common/mpool"
	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/testutil"
)

const TEST_COUNT = 10000000
const TEST_RATE = 0.00001

func TestBloomFilter(t *testing.T) {
	mp := mpool.MustNewZero()
	vec := testutil.NewVector(TEST_COUNT, types.New(types.T_int64, 0, 0), mp, false, nil)

	boom := New(TEST_COUNT, TEST_RATE)
	boom.TestAndAddForVector(vec, func(_ bool, _ int) {})
}

func BenchmarkBloomFiltrer(b *testing.B) {
	mp := mpool.MustNewZero()
	vec := testutil.NewVector(TEST_COUNT, types.New(types.T_int64, 0, 0), mp, false, nil)

	for i := 0; i < b.N; i++ {
		boom := New(TEST_COUNT, TEST_RATE)
		boom.TestAndAddForVector(vec, func(_ bool, _ int) {})
	}
}

func BenchmarkBloom(b *testing.B) {
	mp := mpool.MustNewZero()
	vec := testutil.NewVector(TEST_COUNT, types.New(types.T_int64, 0, 0), mp, false, nil)
	k := 3
	n := float64(TEST_COUNT)
	p := TEST_RATE
	e := -float64(k) * math.Ceil(1.001*n) / math.Log(1-math.Pow(p, 1.0/float64(k)))
	m := uint(math.Ceil(e))

	for i := 0; i < b.N; i++ {
		filter := bloom.New(m, 3)
		for i := 0; i < TEST_COUNT; i++ {
			var bytes = vec.GetRawBytesAt(i)
			filter.TestAndAdd(bytes)
		}
	}
}
