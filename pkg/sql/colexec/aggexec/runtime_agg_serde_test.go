// Copyright 2026 Matrix Origin
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
	"bytes"
	"testing"

	"github.com/matrixorigin/matrixone/pkg/common/mpool"
	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/container/vector"
	"github.com/stretchr/testify/require"
)

type fakeSerdeGroup string

func (g fakeSerdeGroup) MarshalBinary() ([]byte, error) {
	return []byte(g), nil
}

func TestRuntimeAggSerdeMarshalAndUnmarshal(t *testing.T) {
	mp := mpool.MustNewZero()
	ret := initAggResultWithFixedTypeResult[int64](mp, types.T_int64.ToType(), true, 0, false)
	ret.optInformation.chunkSize = 4
	require.NoError(t, ret.grows(2))
	ret.values[0][0] = 11
	ret.values[0][1] = 22
	ret.bsFromEmptyList[0][0] = false
	ret.bsFromEmptyList[0][1] = false

	var buf bytes.Buffer
	require.NoError(t, marshalRetAndGroupsToBuffer(2, [][]uint8{{1, 1}}, &buf, &ret.optSplitResult, []fakeSerdeGroup{"g0", "g1"}, [][]byte{[]byte("extra")}))

	reader := bytes.NewReader(buf.Bytes())
	cnt, err := types.ReadInt64(reader)
	require.NoError(t, err)
	require.Equal(t, int64(2), cnt)

	restored := initAggResultWithFixedTypeResult[int64](mp, types.T_int64.ToType(), true, 0, false)
	require.NoError(t, restored.unmarshalFromReader(reader))
	require.Equal(t, 2, restored.resultList[0].Length())
	require.Equal(t, []int64{11, 22}, vector.MustFixedColNoTypeCheck[int64](restored.resultList[0]))

	groupCnt, err := types.ReadInt64(reader)
	require.NoError(t, err)
	require.Equal(t, int64(2), groupCnt)
	_, g0, err := types.ReadSizeBytes(reader)
	require.NoError(t, err)
	_, g1, err := types.ReadSizeBytes(reader)
	require.NoError(t, err)
	require.Equal(t, "g0", string(g0))
	require.Equal(t, "g1", string(g1))

	extraCnt, err := types.ReadInt64(reader)
	require.NoError(t, err)
	require.Equal(t, int64(1), extraCnt)
	_, extra, err := types.ReadSizeBytes(reader)
	require.NoError(t, err)
	require.Equal(t, "extra", string(extra))
}

func TestRuntimeAggSerdeChunkAndNoGroupPaths(t *testing.T) {
	mp := mpool.MustNewZero()
	ret := initAggResultWithBytesTypeResult(mp, types.T_varchar.ToType(), true, "", false)
	ret.optInformation.chunkSize = 2
	require.NoError(t, ret.grows(3))
	require.NoError(t, vector.SetBytesAt(ret.resultList[0], 0, []byte("a"), mp))
	require.NoError(t, vector.SetBytesAt(ret.resultList[0], 1, []byte("b"), mp))
	require.NoError(t, vector.SetBytesAt(ret.resultList[1], 0, []byte("c"), mp))
	ret.bsFromEmptyList[0][0] = false
	ret.bsFromEmptyList[0][1] = false
	ret.bsFromEmptyList[1][0] = false

	var zero bytes.Buffer
	require.NoError(t, marshalRetAndGroupsToBuffer[fakeSerdeGroup](0, nil, &zero, &ret.optSplitResult, nil, nil))
	require.Equal(t, 8, zero.Len())

	var chunkBuf bytes.Buffer
	require.NoError(t, marshalChunkToBuffer(0, &chunkBuf, &ret.optSplitResult, []fakeSerdeGroup{"x", "y", "z"}, [][]byte{[]byte("meta")}))
	require.Greater(t, chunkBuf.Len(), 0)

	var noGroup bytes.Buffer
	types.WriteInt64(&noGroup, int64(ret.optInformation.chunkSize))
	require.NoError(t, ret.marshalToBuffers([][]uint8{{1, 1}, {1}}, &noGroup))

	restored := initAggResultWithBytesTypeResult(mp, types.T_varchar.ToType(), true, "", false)
	require.NoError(t, unmarshalFromReaderNoGroup(bytes.NewReader(noGroup.Bytes()), &restored.optSplitResult))
	require.Equal(t, ret.optInformation.chunkSize, restored.optInformation.chunkSize)
	require.Len(t, restored.resultList, 1)
	require.Equal(t, 3, restored.resultList[0].Length())
	require.Equal(t, "a", string(restored.resultList[0].GetBytesAt(0)))
	require.Equal(t, "c", string(restored.resultList[0].GetBytesAt(2)))

	empty := initAggResultWithBytesTypeResult(mp, types.T_varchar.ToType(), true, "", false)
	err := unmarshalFromReaderNoGroup(bytes.NewReader([]byte{1, 2, 3}), &empty.optSplitResult)
	require.Error(t, err)
}
