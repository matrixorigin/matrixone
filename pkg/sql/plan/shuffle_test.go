// Copyright 2022 Matrix Origin
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

package plan

import (
	"bytes"
	"math/rand"
	"testing"
	"unsafe"

	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/stretchr/testify/require"
)

var area = make([]byte, 0, 10000)

func TestRangeShuffle(t *testing.T) {
	require.Equal(t, GetRangeShuffleIndexUnsigned(0, 1000000, 299999, 10), uint64(2))
	require.Equal(t, GetRangeShuffleIndexUnsigned(0, 1000000, 888, 10), uint64(0))
	require.Equal(t, GetRangeShuffleIndexUnsigned(0, 1000000, 100000000, 10), uint64(9))
	require.Equal(t, GetRangeShuffleIndexSigned(0, 1000000, 299999, 10), uint64(2))
	require.Equal(t, GetRangeShuffleIndexSigned(0, 1000000, 888, 10), uint64(0))
	require.Equal(t, GetRangeShuffleIndexSigned(0, 1000000, 100000000, 10), uint64(9))
	require.Equal(t, GetRangeShuffleIndexSigned(0, 1000000, -2, 10), uint64(0))
	require.Equal(t, GetRangeShuffleIndexSigned(0, 1000000, 999000, 10), uint64(9))
	require.Equal(t, GetRangeShuffleIndexSigned(0, 1000000, 99999, 10), uint64(0))
	require.Equal(t, GetRangeShuffleIndexSigned(0, 1000000, 100000, 10), uint64(1))
	require.Equal(t, GetRangeShuffleIndexSigned(0, 1000000, 100001, 10), uint64(1))
	require.Equal(t, GetRangeShuffleIndexSigned(0, 1000000, 199999, 10), uint64(1))
	require.Equal(t, GetRangeShuffleIndexSigned(0, 1000000, 200000, 10), uint64(2))
	require.Equal(t, GetRangeShuffleIndexSigned(0, 1000000, 999999, 10), uint64(9))
	require.Equal(t, GetRangeShuffleIndexSigned(0, 1000000, 899999, 10), uint64(8))
	require.Equal(t, GetRangeShuffleIndexSigned(0, 1000000, 900000, 10), uint64(9))
	require.Equal(t, GetRangeShuffleIndexSigned(0, 1000000, 1000000, 10), uint64(9))
}

func buildVarlenaFromByteSlice(bs []byte) *types.Varlena {
	var v types.Varlena
	vlen := len(bs)
	if vlen <= types.VarlenaInlineSize {
		// first clear varlena to 0
		p1 := v.UnsafePtr()
		*(*int64)(p1) = 0
		*(*int64)(unsafe.Add(p1, 8)) = 0
		*(*int64)(unsafe.Add(p1, 16)) = 0
		v[0] = byte(vlen)
		copy(v[1:1+vlen], bs)
		return &v
	} else {
		voff := len(area)
		area = append(area, bs...)
		v.SetOffsetLen(uint32(voff), uint32(vlen))
	}
	return &v
}

// The result will be 0 if a == b, -1 if a < b, and +1 if a > b.
func compareUint64(a, b uint64) int {
	if a == b {
		return 0
	} else if a < b {
		return -1
	} else {
		return 1
	}
}

func TestStringToUint64(t *testing.T) {
	s1 := []byte("abc")
	u1 := VarlenaToUint64Inline(buildVarlenaFromByteSlice(s1))
	require.Equal(t, u1, ByteSliceToUint64(s1))
	s2 := []byte("abcde")
	u2 := VarlenaToUint64Inline(buildVarlenaFromByteSlice(s2))
	require.Equal(t, u2, ByteSliceToUint64(s2))
	s3 := []byte("abcdeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeee")
	u3 := VarlenaToUint64(buildVarlenaFromByteSlice(s3), area)
	require.Equal(t, u3, ByteSliceToUint64(s3))
	s4 := []byte("a")
	u4 := VarlenaToUint64(buildVarlenaFromByteSlice(s4), area)
	require.Equal(t, u4, ByteSliceToUint64(s4))
	s5 := []byte("")
	u5 := VarlenaToUint64(buildVarlenaFromByteSlice(s5), area)
	require.Equal(t, u5, ByteSliceToUint64(s5))
	s6 := []byte("A")
	u6 := VarlenaToUint64(buildVarlenaFromByteSlice(s6), area)
	require.Equal(t, u6, ByteSliceToUint64(s6))
	s7 := []byte("AAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAA")
	u7 := VarlenaToUint64(buildVarlenaFromByteSlice(s7), area)
	require.Equal(t, u7, ByteSliceToUint64(s7))

	require.Equal(t, bytes.Compare(s1, s2), compareUint64(u1, u2))
	require.Equal(t, bytes.Compare(s1, s3), compareUint64(u1, u3))
	require.Equal(t, bytes.Compare(s1, s4), compareUint64(u1, u4))
	require.Equal(t, bytes.Compare(s1, s5), compareUint64(u1, u5))
	require.Equal(t, bytes.Compare(s1, s6), compareUint64(u1, u6))
	require.Equal(t, bytes.Compare(s1, s7), compareUint64(u1, u7))
	require.Equal(t, bytes.Compare(s2, s3), compareUint64(u2, u3))
	require.Equal(t, bytes.Compare(s2, s4), compareUint64(u2, u4))
	require.Equal(t, bytes.Compare(s2, s5), compareUint64(u2, u5))
	require.Equal(t, bytes.Compare(s2, s6), compareUint64(u2, u6))
	require.Equal(t, bytes.Compare(s2, s7), compareUint64(u2, u7))
	require.Equal(t, bytes.Compare(s3, s4), compareUint64(u3, u4))
	require.Equal(t, bytes.Compare(s3, s5), compareUint64(u3, u5))
	require.Equal(t, bytes.Compare(s3, s6), compareUint64(u3, u6))
	require.Equal(t, bytes.Compare(s3, s7), compareUint64(u3, u7))
	require.Equal(t, bytes.Compare(s4, s5), compareUint64(u4, u5))
	require.Equal(t, bytes.Compare(s4, s6), compareUint64(u4, u6))
	require.Equal(t, bytes.Compare(s4, s7), compareUint64(u4, u7))
	require.Equal(t, bytes.Compare(s5, s6), compareUint64(u5, u6))
	require.Equal(t, bytes.Compare(s5, s7), compareUint64(u5, u7))
	require.Equal(t, bytes.Compare(s6, s7), compareUint64(u6, u7))
}

type ShuffleFloatTestCase struct {
	min           []float64
	max           []float64
	expectoverlap float64
	bucket        int
}

func TestShuffleRange(t *testing.T) {
	testcase := make([]ShuffleFloatTestCase, 0)
	testcase = append(testcase, ShuffleFloatTestCase{
		min:           []float64{},
		max:           []float64{},
		expectoverlap: 0.25,
		bucket:        64,
	})
	testcase[0].min = append(testcase[0].min, 0)
	testcase[0].max = append(testcase[0].max, 10000)
	for i := 1; i < 100000; i++ {
		testcase[0].min = append(testcase[0].min, testcase[0].max[i-1]+float64(rand.Int()%10000))
		testcase[0].max = append(testcase[0].max, testcase[0].min[i]+float64(rand.Int()%10000))
	}
	testcase[0].min = append(testcase[0].min, testcase[0].max[99999]/2)
	testcase[0].max = append(testcase[0].max, testcase[0].min[100000]+10000)
	for i := 100001; i <= 200000; i++ {
		testcase[0].min = append(testcase[0].min, testcase[0].max[i-1]+float64(rand.Int()%10000))
		testcase[0].max = append(testcase[0].max, testcase[0].min[i]+float64(rand.Int()%10000))
	}

	testcase = append(testcase, ShuffleFloatTestCase{
		min:           []float64{},
		max:           []float64{},
		expectoverlap: 0.999,
		bucket:        2,
	})
	for i := 0; i <= 100000; i++ {
		testcase[1].min = append(testcase[1].min, float64(rand.Int()))
		testcase[1].max = append(testcase[1].max, testcase[1].min[i]+float64(rand.Int()))
	}

	testcase = append(testcase, ShuffleFloatTestCase{
		min:           []float64{},
		max:           []float64{},
		expectoverlap: 0.002,
		bucket:        64,
	})
	testcase[2].min = append(testcase[2].min, 0)
	testcase[2].max = append(testcase[2].max, 10000)
	for i := 1; i < 100000; i++ {
		testcase[2].min = append(testcase[2].min, testcase[2].max[i-1]-10)
		testcase[2].max = append(testcase[2].max, testcase[2].min[i]+10000)
	}

	leng := len(testcase)

	for i := 0; i < leng; i++ {
		shufflerange := NewShuffleRange()
		for j := 0; j < len(testcase[i].min); j++ {
			shufflerange.Update(testcase[i].min[j], testcase[i].max[j])
		}

		shufflerange.Eval(testcase[i].bucket)
		var k float64
		if testcase[i].expectoverlap >= 0.1 {
			k = (shufflerange.Overlap - testcase[i].expectoverlap) / testcase[i].expectoverlap
		} else {
			k = (shufflerange.Overlap - testcase[i].expectoverlap) / 0.1
		}
		require.Equal(t, k >= -0.2 && k <= 0.1, true)
	}
}
