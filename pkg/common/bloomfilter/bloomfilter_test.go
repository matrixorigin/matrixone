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
	"testing"

	"github.com/matrixorigin/matrixone/pkg/common/mpool"
	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/container/vector"
	"github.com/matrixorigin/matrixone/pkg/testutil"
	"github.com/stretchr/testify/require"
)

const testCount = 20000
const testRate = 0.00001
const vecCount = 10

func TestBloomFilter(t *testing.T) {
	mp := mpool.MustNewZero()
	vecs := make([]*vector.Vector, vecCount)
	for i := 0; i < vecCount; i++ {
		vecs[i] = testutil.NewVector(testCount/vecCount, types.New(types.T_int64, 0, 0), mp, false, nil)
	}

	boom := New(testCount, testRate)
	for j := 0; j < vecCount; j++ {
		boom.TestAndAdd(vecs[j], func(_ bool, _ int) {})
	}
	for j := 0; j < vecCount; j++ {
		vecs[j].Free(mp)
	}

	testVec := testutil.NewVector(testCount/vecCount, types.New(types.T_int64, 0, 0), mp, false, nil)
	allAdd := true
	boom.Test(testVec, func(exits bool, _ int) {
		allAdd = allAdd && exits
	})
	require.Equal(t, allAdd, true)

	testVec = testutil.NewVector(testCount*1.2, types.New(types.T_int64, 0, 0), mp, false, nil)
	allAdd = true
	boom.Test(testVec, func(exits bool, _ int) {
		allAdd = allAdd && exits
	})
	require.Equal(t, allAdd, false)
}

func BenchmarkBloomFiltrerAdd(b *testing.B) {
	mp := mpool.MustNewZero()
	vecs := make([]*vector.Vector, vecCount)
	for i := 0; i < vecCount; i++ {
		vecs[i] = testutil.NewVector(testCount/vecCount, types.New(types.T_int64, 0, 0), mp, false, nil)
	}
	var boom *BloomFilter
	for i := 0; i < b.N; i++ {
		boom = New(testCount, testRate)
		for j := 0; j < vecCount; j++ {
			boom.Add(vecs[j])
		}
	}
}

func BenchmarkBloomFiltrerTestAndAdd(b *testing.B) {
	mp := mpool.MustNewZero()
	vecs := make([]*vector.Vector, vecCount)
	for i := 0; i < vecCount; i++ {
		vecs[i] = testutil.NewVector(testCount/vecCount, types.New(types.T_int64, 0, 0), mp, false, nil)
	}
	var boom *BloomFilter
	for i := 0; i < b.N; i++ {
		boom = New(testCount, testRate)
		for j := 0; j < vecCount; j++ {
			boom.TestAndAdd(vecs[j], func(_ bool, _ int) {})
		}
	}
}
