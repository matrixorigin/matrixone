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
	"github.com/matrixorigin/matrixone/pkg/common/mpool"
	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/container/vector"
	"github.com/stretchr/testify/require"
	"testing"
)

// there is very important to check the result's extendResultPurely first.
//
// we do test for the following three cases:
// 1. the using block is enough to save all the data.
// 2. the pre-allocated block is enough to save all the data.
// 3. the unused space is not enough, and we need to append new block.
func TestExtendResultPurely(t *testing.T) {
	blockLimitation := 100

	mg := SimpleAggMemoryManager{mp: mpool.MustNewZeroNoFixed()}
	{
		osr := optSplitResult{}
		osr.init(mg, types.T_bool.ToType(), false)
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

func TestResultSerialization(t *testing.T) {
	proc := hackAggMemoryManager()
	{
		before := proc.Mp().CurrNB()
		r1 := aggResultWithFixedType[int64]{}
		r1.optSplitResult.optInformation.chunkSize = 2
		r1.optSplitResult.optInformation.doesThisNeedEmptyList = true
		r1.init(proc, types.T_int64.ToType(), true)

		_, _, _, _, err := r1.resExtend(7)
		require.NoError(t, err)

		data1, data2, err := r1.marshalToBytes()
		require.NoError(t, err)

		r2 := aggResultWithFixedType[int64]{}
		r2.init(proc, types.T_int64.ToType(), true)
		r2.optSplitResult.optInformation.chunkSize = 2
		r2.optSplitResult.optInformation.doesThisNeedEmptyList = true

		require.NoError(t, r2.unmarshalFromBytes(data1, data2))

		require.Equal(t, len(r1.resultList), len(r2.resultList))
		require.Equal(t, len(r1.emptyList), len(r2.emptyList))
		require.Equal(t, r1.nowIdx1, r2.nowIdx1)

		r1.free()
		r2.free()
		require.Equal(t, before, proc.Mp().CurrNB())
	}

	{
		before := proc.Mp().CurrNB()
		r1 := aggResultWithBytesType{}
		r1.optSplitResult.optInformation.chunkSize = 2
		r1.optSplitResult.optInformation.doesThisNeedEmptyList = true
		r1.init(proc, types.T_varchar.ToType(), true)

		_, _, _, _, err := r1.resExtend(15)
		require.NoError(t, err)

		data1, data2, err := r1.marshalToBytes()
		require.NoError(t, err)

		r2 := aggResultWithBytesType{}
		r2.init(proc, types.T_varchar.ToType(), true)
		r2.optSplitResult.optInformation.chunkSize = 2
		r2.optSplitResult.optInformation.doesThisNeedEmptyList = true

		require.NoError(t, r2.unmarshalFromBytes(data1, data2))

		require.Equal(t, len(r1.resultList), len(r2.resultList))
		require.Equal(t, len(r1.emptyList), len(r2.emptyList))
		require.Equal(t, r1.nowIdx1, r2.nowIdx1)

		r1.free()
		r2.free()
		require.Equal(t, before, proc.Mp().CurrNB())
	}
}
