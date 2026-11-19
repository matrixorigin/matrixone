// Copyright 2024 Matrix Origin
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

package aggexec

import (
	"fmt"
	"testing"

	"github.com/matrixorigin/matrixone/pkg/common/moerr"
	"github.com/matrixorigin/matrixone/pkg/common/mpool"
	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/container/vector"
	"github.com/stretchr/testify/require"
)

func initAggResultWithBytesTypeResult(
	mp *mpool.MPool,
	resultType types.Type,
	setEmptyGroupToNull bool, initialValue string, hasDistinct bool) aggResultWithBytesType {
	res := aggResultWithBytesType{}
	res.init(mp, resultType, setEmptyGroupToNull, hasDistinct)
	res.InitialValue = []byte(initialValue)
	return res
}

func TestPreallocatedResultGrowthAcrossChunks(t *testing.T) {
	for _, tc := range []struct {
		name                string
		initial, additional int
	}{
		{"first_chunk", 0, 3},
		{"two_full_chunks", 0, 8},
		{"three_full_chunks", 0, 12},
		{"partial_third_chunk", 0, 9},
		{"partial_then_full", 2, 10},
		{"full_then_full", 4, 8},
	} {
		for _, preallocate := range []bool{false, true} {
			for _, withEmpty := range []bool{false, true} {
				t.Run(fmt.Sprintf("%s/preallocate=%t/empty=%t", tc.name, preallocate, withEmpty), func(t *testing.T) {
					mp := mpool.MustNewZero()
					ret := initAggResultWithFixedTypeResult[int64](mp, types.T_int64.ToType(), withEmpty, 0, false)
					t.Cleanup(func() { ret.free(); require.Zero(t, mp.CurrNB()) })
					ret.optInformation.chunkSize = 4
					if tc.initial > 0 {
						require.NoError(t, ret.grows(tc.initial))
					}
					if preallocate {
						require.NoError(t, ret.preExtend(tc.additional))
						require.Equal(t, tc.initial, ret.resultList[0].Length())
						for _, chunk := range ret.resultList[1:] {
							require.Zero(t, chunk.Length())
						}
					}
					require.NoError(t, ret.grows(tc.additional))
					count := tc.initial + tc.additional
					require.Len(t, ret.values, (count+3)/4)
					// A subsequent grow must continue at the last occupied chunk.
					require.NoError(t, ret.grows(1))
					count++
					for i := 0; i < count; i++ {
						ret.values[i/4][i%4] = int64(i + 1)
						if withEmpty {
							ret.bsFromEmptyList[i/4][i%4] = i%3 == 0
						}
					}
					output := ret.flushAll()
					t.Cleanup(func() {
						for _, chunk := range output {
							chunk.Free(mp)
						}
					})
					require.Len(t, output, (count+3)/4)
					for i, chunk := range output {
						require.Equal(t, min(4, count-i*4), chunk.Length())
						for j, value := range vector.MustFixedColNoTypeCheck[int64](chunk) {
							index := i*4 + j
							require.Equal(t, int64(index+1), value)
							require.Equal(t, withEmpty && index%3 == 0, chunk.IsNull(uint64(j)))
						}
					}
				})
			}
		}
	}
}

func TestResultGrowthDoesNotExtendFullChunk(t *testing.T) {
	mp := mpool.MustNewZero()
	ret := initAggResultWithFixedTypeResult[int64](mp, types.T_int64.ToType(), true, 0, false)
	t.Cleanup(func() { ret.free(); require.Zero(t, mp.CurrNB()) })
	ret.optInformation.chunkSize = AggBatchSize
	require.NoError(t, ret.grows(AggBatchSize))
	resultBytes, emptyBytes := ret.resultList[0].Allocated(), ret.emptyList[0].Allocated()
	require.NoError(t, ret.grows(0))
	require.Equal(t, resultBytes, ret.resultList[0].Allocated())
	require.Equal(t, emptyBytes, ret.emptyList[0].Allocated())
	require.NoError(t, ret.grows(1))
	require.Equal(t, resultBytes, ret.resultList[0].Allocated())
	require.Equal(t, emptyBytes, ret.emptyList[0].Allocated())
	checkRowDistribution(t, []int{AggBatchSize, 1}, ret.resultList)
}

func TestResultGrowthFailureCleansPartialChunks(t *testing.T) {
	for _, tc := range []struct {
		name            string
		freeBelow       int64
		completed       int
		allocatedResult bool
	}{
		{"result", 65536, 0, false},
		{"empty", 73728, 0, true},
		{"after_one_chunk", 110000, 1, false},
	} {
		t.Run(tc.name, func(t *testing.T) {
			const limit = int64(1 << 20)
			mp, err := mpool.NewMPool("result-growth-failure", limit, mpool.NoFixed)
			require.NoError(t, err)
			ret := initAggResultWithFixedTypeResult[int64](mp, types.T_int64.ToType(), true, 0, false)
			var ballast [][]byte
			t.Cleanup(func() {
				ret.free()
				for _, b := range ballast {
					mp.Free(b)
				}
				require.Zero(t, mp.CurrNB())
				mpool.DeleteMPool(mp)
			})
			ret.optInformation.chunkSize = AggBatchSize
			require.NoError(t, ret.grows(AggBatchSize))
			resultBytes, emptyBytes := ret.resultList[0].Allocated(), ret.emptyList[0].Allocated()
			for i := 0; i < 300 && limit-mp.CurrNB() >= tc.freeBelow; i++ {
				b, allocErr := mp.Alloc(4096, true)
				require.NoError(t, allocErr)
				ballast = append(ballast, b)
			}
			err = ret.grows(2 * AggBatchSize)
			require.Truef(t, moerr.IsMoErrCode(err, moerr.ErrMPoolCapacity), "error: %v", err)
			require.Equal(t, resultBytes, ret.resultList[0].Allocated())
			require.Equal(t, emptyBytes, ret.emptyList[0].Allocated())
			require.Len(t, ret.resultList, 2+tc.completed)
			if tc.completed != 0 {
				require.Equal(t, AggBatchSize, ret.resultList[1].Length())
			}
			if tc.allocatedResult {
				require.Positive(t, ret.resultList[1].Allocated())
				require.Zero(t, ret.emptyList[1].Allocated())
			}
			require.Zero(t, ret.resultList[len(ret.resultList)-1].Length())
		})
	}
}

// there is very important to check the result's extendResultPurely first.
//
// we do test for the following three cases:
// 1. the using block is enough to save all the data.
// 2. the pre-allocated block is enough to save all the data.
// 3. the unused space is not enough, and we need to append new block.
func TestExtendResultPurely(t *testing.T) {
	blockLimitation := 100

	mg := mpool.MustNewZeroNoFixed()
	{
		osr := optSplitResult{}
		osr.init(mg, types.T_bool.ToType(), false, false)
		osr.optInformation.chunkSize = blockLimitation

		// pre extendResultPurely 130 rows.
		require.NoError(t, osr.preExtend(130))
		checkRowDistribution(t, []int{0, 0}, osr.resultList)
		checkCapSituation(t, []int{100, 30}, osr.resultList, osr.optInformation.chunkSize)

		// case 1 : extendResultPurely 50 only use the first block.
		require.NoError(t, osr.extendResultPurely(50))
		checkRowDistribution(t, []int{50, 0}, osr.resultList)
		checkCapSituation(t, []int{100, 30}, osr.resultList, osr.optInformation.chunkSize)

		// case 2 : extendResultPurely 75 will full the first block and set 1 row to the second block.
		require.NoError(t, osr.extendResultPurely(75))
		checkRowDistribution(t, []int{100, 25}, osr.resultList)
		checkCapSituation(t, []int{100, 30}, osr.resultList, osr.optInformation.chunkSize)

		// case 3 : extendResultPurely 200 will full the last block and append 2 more blocks.
		require.NoError(t, osr.extendResultPurely(200))
		checkRowDistribution(t, []int{100, 100, 100, 25}, osr.resultList)
		checkCapSituation(t, []int{100, 100, 100, 25}, osr.resultList, osr.optInformation.chunkSize)
	}
}

func TestFlushAll(t *testing.T) {
	blockLimitation := 100

	mg := mpool.MustNewZeroNoFixed()
	{
		osr := optSplitResult{}
		osr.init(mg, types.T_bool.ToType(), false, false)
		osr.optInformation.chunkSize = blockLimitation

		require.NoError(t, osr.preExtend(130))
		vs := osr.flushAll()
		require.True(t, len(vs) == 0 || (len(vs) == 1 && vs[0].Length() == 0))
		for i := range vs {
			vs[i].Free(mg)
		}

		osr.free()
		require.Equal(t, int64(0), mg.CurrNB())
	}

	{
		osr := optSplitResult{}
		osr.init(mg, types.T_bool.ToType(), false, false)
		osr.optInformation.chunkSize = blockLimitation

		require.NoError(t, osr.extendResultPurely(201))
		vs := osr.flushAll()
		require.Equal(t, 3, len(vs))
		for i := range vs {
			vs[i].Free(mg)
		}

		osr.free()
		require.Equal(t, int64(0), mg.CurrNB())
	}
}

func checkRowDistribution(t *testing.T, expected []int, src []*vector.Vector) {
	require.Equal(t, len(expected), len(src))

	for i := range src {
		require.NotNil(t, src[i])
	}
	for i := range expected {
		require.Equal(t, expected[i], src[i].Length())
	}
}

func checkCapSituation(t *testing.T, expected []int, src []*vector.Vector, ourLimitation int) {
	require.Equal(t, len(expected), len(src))

	for i := range src {
		require.NotNil(t, src[i])
	}
	for i := range expected {
		require.LessOrEqual(t, expected[i], min(src[i].Capacity(), ourLimitation))
	}
}

func TestResultSize(t *testing.T) {
	mp := mpool.MustNewZeroNoFixed()
	chunkSize := 10

	// Test aggResultWithFixedType
	{
		before := mp.CurrNB()
		r := initAggResultWithFixedTypeResult[int64](mp, types.T_int64.ToType(), true, 0, false)
		r.optInformation.chunkSize = chunkSize

		initialSize := r.Size()
		require.Greater(t, initialSize, int64(0))

		// grow and check size increase
		err := r.grows(chunkSize*2 + 1)
		require.NoError(t, err)
		grownSize := r.Size()
		require.Greater(t, grownSize, initialSize)

		// free and check memory release
		r.free()
		require.Equal(t, before, mp.CurrNB())
	}

	// Test aggResultWithBytesType
	{
		before := mp.CurrNB()
		r := initAggResultWithBytesTypeResult(mp, types.T_varchar.ToType(), true, "", false)
		r.optInformation.chunkSize = chunkSize

		initialSize := r.Size()
		require.Greater(t, initialSize, int64(0))

		// grow and check size increase
		err := r.grows(chunkSize*2 + 1)
		require.NoError(t, err)
		grownSize := r.Size()
		require.Greater(t, grownSize, initialSize)

		// populate with data and check size increase again
		for i := range r.resultList {
			for j := 0; j < r.resultList[i].Length(); j++ {
				err = vector.SetBytesAt(r.resultList[i], j, []byte("test_data"), mp)
				require.NoError(t, err)
			}
		}
		populatedSize := r.Size()
		require.GreaterOrEqual(t, populatedSize, grownSize)

		// free and check memory release
		r.free()
		require.Equal(t, before, mp.CurrNB())
	}
}
