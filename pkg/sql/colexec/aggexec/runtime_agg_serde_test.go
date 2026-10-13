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
	"io"
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

func TestDistinctHashSerdeRoundTrip(t *testing.T) {
	mp := mpool.MustNewZero()
	defer func() { require.Zero(t, mp.CurrNB()) }()

	values := buildFixedVec(
		t, mp, types.T_int64.ToType(), []int64{11, 22})
	defer values.Free(mp)

	source := newDistinctHash(mp)
	require.NoError(t, source.grows(2))
	inserted, err := source.fill(0, []*vector.Vector{values}, 0)
	require.NoError(t, err)
	require.True(t, inserted)
	inserted, err = source.fill(1, []*vector.Vector{values}, 1)
	require.NoError(t, err)
	require.True(t, inserted)

	var encoded bytes.Buffer
	require.NoError(t, source.marshalToBuffers(nil, &encoded))
	restored := newDistinctHash(mp)
	require.NoError(t, restored.unmarshalFromReader(
		bytes.NewReader(encoded.Bytes()), mp))
	require.Len(t, restored.maps, 2)
	inserted, err = restored.fill(0, []*vector.Vector{values}, 0)
	require.NoError(t, err)
	require.False(t, inserted)
	inserted, err = restored.fill(1, []*vector.Vector{values}, 1)
	require.NoError(t, err)
	require.False(t, inserted)

	source.free()
	restored.free()
}

func TestDistinctHashRejectsPayloadBeyondBoundedReader(t *testing.T) {
	mp := mpool.MustNewZero()
	defer func() { require.Zero(t, mp.CurrNB()) }()

	var encoded bytes.Buffer
	require.NoError(t, types.WriteUint64(&encoded, 1))
	require.NoError(t, types.WriteUint64(&encoded, 1<<30))
	restored := newDistinctHash(mp)
	require.ErrorIs(t, restored.unmarshalFromReader(
		bytes.NewReader(encoded.Bytes()), mp), io.ErrUnexpectedEOF)
	restored.free()
}

func TestOptSplitResultDistinctSerdeFlattensSelectedChunks(t *testing.T) {
	mp := mpool.MustNewZero()
	defer func() { require.Zero(t, mp.CurrNB()) }()

	values := buildFixedVec(
		t, mp, types.T_int64.ToType(), []int64{11, 22, 33})
	defer values.Free(mp)
	source := initAggResultWithFixedTypeResult[int64](
		mp, types.T_int64.ToType(), false, 0, true)
	source.optInformation.chunkSize = 2
	require.NoError(t, source.grows(3))
	source.values[0][0], source.values[0][1], source.values[1][0] = 11, 22, 33
	for row := 0; row < 3; row++ {
		chunk, local := row/2, row%2
		inserted, err := source.distinct[chunk].fill(
			local, []*vector.Vector{values}, row)
		require.NoError(t, err)
		require.True(t, inserted)
	}

	var record bytes.Buffer
	require.NoError(t, types.WriteInt64(&record, 2))
	require.NoError(t, source.optSplitResult.marshalToBuffers(
		[][]uint8{{0, 1}, {1}}, &record))

	target := initAggResultWithFixedTypeResult[int64](
		mp, types.T_int64.ToType(), false, 0, true)
	rows, err := unmarshalFromReaderNoGroup(
		bytes.NewReader(record.Bytes()), &target.optSplitResult)
	require.NoError(t, err)
	require.Equal(t, 2, rows)
	require.Len(t, target.resultList, 1)
	require.Equal(t, []int64{22, 33},
		vector.MustFixedColNoTypeCheck[int64](target.resultList[0]))
	require.Len(t, target.distinct, 1)
	require.Len(t, target.distinct[0].maps, 2)
	for group, row := range []int{1, 2} {
		inserted, err := target.distinct[0].fill(
			group, []*vector.Vector{values}, row)
		require.NoError(t, err)
		require.False(t, inserted)
	}

	var chunkRecord bytes.Buffer
	require.NoError(t, marshalChunkToBuffer(
		0, &chunkRecord, &source.optSplitResult,
		[]fakeSerdeGroup{"g0", "g1", "g2"}, nil))
	chunkTarget := initAggResultWithFixedTypeResult[int64](
		mp, types.T_int64.ToType(), false, 0, true)
	chunkRows, err := unmarshalFromReaderNoGroup(
		bytes.NewReader(chunkRecord.Bytes()), &chunkTarget.optSplitResult)
	require.NoError(t, err)
	require.Equal(t, 2, chunkRows)
	require.Len(t, chunkTarget.distinct, 1)
	require.Len(t, chunkTarget.distinct[0].maps, 2)

	source.free()
	target.free()
	chunkTarget.free()
}

func TestOptSplitResultReadsLegacyDistinctMapCountFrames(t *testing.T) {
	mp := mpool.MustNewZero()
	defer func() { require.Zero(t, mp.CurrNB()) }()

	values := buildFixedVec(
		t, mp, types.T_int64.ToType(), []int64{11, 22, 33})
	defer values.Free(mp)
	source := initAggResultWithFixedTypeResult[int64](
		mp, types.T_int64.ToType(), false, 0, true)
	source.optInformation.chunkSize = 2
	require.NoError(t, source.grows(3))
	source.values[0][0], source.values[0][1], source.values[1][0] = 11, 22, 33
	for row := range 3 {
		chunk, local := row/2, row%2
		inserted, err := source.distinct[chunk].fill(
			local, []*vector.Vector{values}, row)
		require.NoError(t, err)
		require.True(t, inserted)
	}

	// Reproduce the pre-M3 wire exactly: the outer field was the selected map
	// count, while each physical chunk still emitted its own count-prefixed
	// distinctHash frame.
	combined := vector.NewOffHeapVecWithType(types.T_int64.ToType())
	defer combined.Free(mp)
	require.NoError(t, combined.UnionBatch(
		source.resultList[0], 0, 2, nil, mp))
	require.NoError(t, combined.UnionBatch(
		source.resultList[1], 0, 1, nil, mp))
	var legacy bytes.Buffer
	require.NoError(t, types.WriteInt64(&legacy, 3))
	require.NoError(t, combined.MarshalBinaryTo(&legacy))
	require.NoError(t, types.WriteInt64(&legacy, 0))
	require.NoError(t, types.WriteInt64(&legacy, 3))
	require.NoError(t, source.distinct[0].marshalToBuffers(nil, &legacy))
	require.NoError(t, source.distinct[1].marshalToBuffers(nil, &legacy))

	target := initAggResultWithFixedTypeResult[int64](
		mp, types.T_int64.ToType(), false, 0, true)
	rows, err := unmarshalFromReaderNoGroup(
		bytes.NewReader(legacy.Bytes()), &target.optSplitResult)
	require.NoError(t, err)
	require.Equal(t, 3, rows)
	require.Len(t, target.distinct, 1)
	require.Len(t, target.distinct[0].maps, 3)
	for row := range 3 {
		inserted, err := target.distinct[0].fill(
			row, []*vector.Vector{values}, row)
		require.NoError(t, err)
		require.False(t, inserted)
	}

	source.free()
	target.free()
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

func TestReadAggregateExtraRejectsInvalidFramingWithoutAllocatingPayload(t *testing.T) {
	t.Run("negative size", func(t *testing.T) {
		var record bytes.Buffer
		require.NoError(t, types.WriteInt64(&record, 1))
		require.NoError(t, types.WriteInt32(&record, -1))
		require.ErrorContains(t,
			readAggregateExtra(bytes.NewReader(record.Bytes())),
			"invalid aggregate extra state size")
	})
	t.Run("declared payload exceeds reader", func(t *testing.T) {
		var record bytes.Buffer
		require.NoError(t, types.WriteInt64(&record, 1))
		require.NoError(t, types.WriteInt32(&record, 1<<30))
		require.ErrorIs(t,
			readAggregateExtra(bytes.NewReader(record.Bytes())),
			io.EOF)
	})
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
	types.WriteInt64(&noGroup, 3)
	require.NoError(t, ret.marshalToBuffers([][]uint8{{1, 1}, {1}}, &noGroup))

	restored := initAggResultWithBytesTypeResult(mp, types.T_varchar.ToType(), true, "", false)
	decodedRows, err := unmarshalFromReaderNoGroup(
		bytes.NewReader(noGroup.Bytes()), &restored.optSplitResult)
	require.NoError(t, err)
	require.Equal(t, 3, decodedRows)
	require.Equal(t, 3, restored.optInformation.chunkSize)
	require.Len(t, restored.resultList, 1)
	require.Equal(t, 3, restored.resultList[0].Length())
	require.Equal(t, "a", string(restored.resultList[0].GetBytesAt(0)))
	require.Equal(t, "c", string(restored.resultList[0].GetBytesAt(2)))

	empty := initAggResultWithBytesTypeResult(mp, types.T_varchar.ToType(), true, "", false)
	_, err = unmarshalFromReaderNoGroup(
		bytes.NewReader([]byte{1, 2, 3}), &empty.optSplitResult)
	require.Error(t, err)
}

func TestRuntimeAggSerdeEmptyChunkIsOneCompleteRecord(t *testing.T) {
	mp := mpool.MustNewZero()
	defer func() { require.Zero(t, mp.CurrNB()) }()

	ret := initAggResultWithFixedTypeResult[int64](
		mp, types.T_int64.ToType(), true, 0, false)
	ret.optInformation.chunkSize = 2
	ret.resultList = append(
		ret.resultList,
		vector.NewOffHeapVecWithType(types.T_int64.ToType()),
	)
	ret.emptyList = append(
		ret.emptyList,
		vector.NewOffHeapVecWithType(types.T_bool.ToType()),
	)
	ret.bsFromEmptyList = append(ret.bsFromEmptyList, nil)

	var record bytes.Buffer
	require.NoError(t, marshalChunkToBuffer(
		1, &record, &ret.optSplitResult,
		[]fakeSerdeGroup{"g0", "g1"}, [][]byte{[]byte("trailer")},
	))
	require.Len(t, record.Bytes(), 8)
	const sentinel = int64(0x1122334455667788)
	require.NoError(t, types.WriteInt64(&record, sentinel))

	target := initAggResultWithFixedTypeResult[int64](
		mp, types.T_int64.ToType(), true, 0, false)
	reader := bytes.NewReader(record.Bytes())
	rows, err := unmarshalFromReaderNoGroup(reader, &target.optSplitResult)
	require.NoError(t, err)
	require.Zero(t, rows)
	next, err := types.ReadInt64(reader)
	require.NoError(t, err)
	require.Equal(t, sentinel, next)
	require.Zero(t, reader.Len())

	ret.free()
	target.free()
}

func TestUnmarshalFromReaderNoGroupValidatesDeclaredRows(t *testing.T) {
	mp := mpool.MustNewZero()
	defer func() { require.Zero(t, mp.CurrNB()) }()

	source := initAggResultWithFixedTypeResult[int64](
		mp, types.T_int64.ToType(), true, 0, false)
	require.NoError(t, source.grows(1))
	var payload bytes.Buffer
	require.NoError(t, source.optSplitResult.marshalToBuffers(
		[][]uint8{{1}}, &payload))

	t.Run("negative", func(t *testing.T) {
		var record bytes.Buffer
		require.NoError(t, types.WriteInt64(&record, -1))
		target := initAggResultWithFixedTypeResult[int64](
			mp, types.T_int64.ToType(), true, 0, false)
		_, err := unmarshalFromReaderNoGroup(
			bytes.NewReader(record.Bytes()), &target.optSplitResult)
		require.ErrorContains(t, err, "invalid aggregate result row count")
		target.free()
	})

	t.Run("result shape mismatch", func(t *testing.T) {
		var record bytes.Buffer
		require.NoError(t, types.WriteInt64(&record, 2))
		_, err := record.Write(payload.Bytes())
		require.NoError(t, err)
		target := initAggResultWithFixedTypeResult[int64](
			mp, types.T_int64.ToType(), true, 0, false)
		_, err = unmarshalFromReaderNoGroup(
			bytes.NewReader(record.Bytes()), &target.optSplitResult)
		require.ErrorContains(t, err, "row count 1 does not match 2")
		require.Empty(t, target.resultList)
		target.free()
	})

	t.Run("empty replacement", func(t *testing.T) {
		var record bytes.Buffer
		require.NoError(t, types.WriteInt64(&record, 0))
		target := initAggResultWithFixedTypeResult[int64](
			mp, types.T_int64.ToType(), true, 0, false)
		require.NoError(t, target.grows(1))
		rows, err := unmarshalFromReaderNoGroup(
			bytes.NewReader(record.Bytes()), &target.optSplitResult)
		require.NoError(t, err)
		require.Zero(t, rows)
		target.setupT()
		require.Len(t, target.resultList, 1)
		require.Zero(t, target.resultList[0].Length())
		require.NoError(t, target.grows(1))
		target.free()
	})

	t.Run("missing empty state", func(t *testing.T) {
		withoutEmpty := initAggResultWithFixedTypeResult[int64](
			mp, types.T_int64.ToType(), false, 0, false)
		require.NoError(t, withoutEmpty.grows(1))
		var payload bytes.Buffer
		require.NoError(t, types.WriteInt64(&payload, 1))
		require.NoError(t, withoutEmpty.optSplitResult.marshalToBuffers(
			[][]uint8{{1}}, &payload))

		target := initAggResultWithFixedTypeResult[int64](
			mp, types.T_int64.ToType(), true, 0, false)
		_, err := unmarshalFromReaderNoGroup(
			bytes.NewReader(payload.Bytes()), &target.optSplitResult)
		require.ErrorContains(t, err, "empty-state presence")
		withoutEmpty.free()
		target.free()
	})

	t.Run("unexpected empty state", func(t *testing.T) {
		var record bytes.Buffer
		require.NoError(t, types.WriteInt64(&record, 1))
		require.NoError(t, source.optSplitResult.marshalToBuffers(
			[][]uint8{{1}}, &record))
		target := initAggResultWithFixedTypeResult[int64](
			mp, types.T_int64.ToType(), false, 0, false)
		_, err := unmarshalFromReaderNoGroup(
			bytes.NewReader(record.Bytes()), &target.optSplitResult)
		require.ErrorContains(t, err, "empty-state presence")
		target.free()
	})

	t.Run("missing distinct state", func(t *testing.T) {
		withoutDistinct := initAggResultWithFixedTypeResult[int64](
			mp, types.T_int64.ToType(), false, 0, false)
		require.NoError(t, withoutDistinct.grows(1))
		var payload bytes.Buffer
		require.NoError(t, types.WriteInt64(&payload, 1))
		require.NoError(t, withoutDistinct.optSplitResult.marshalToBuffers(
			[][]uint8{{1}}, &payload))

		target := initAggResultWithFixedTypeResult[int64](
			mp, types.T_int64.ToType(), false, 0, true)
		_, err := unmarshalFromReaderNoGroup(
			bytes.NewReader(payload.Bytes()), &target.optSplitResult)
		require.ErrorContains(t, err, "distinct-state presence")
		withoutDistinct.free()
		target.free()
	})

	t.Run("unexpected distinct state", func(t *testing.T) {
		withDistinct := initAggResultWithFixedTypeResult[int64](
			mp, types.T_int64.ToType(), false, 0, true)
		require.NoError(t, withDistinct.grows(1))
		var payload bytes.Buffer
		require.NoError(t, types.WriteInt64(&payload, 1))
		require.NoError(t, withDistinct.optSplitResult.marshalToBuffers(
			[][]uint8{{1}}, &payload))

		target := initAggResultWithFixedTypeResult[int64](
			mp, types.T_int64.ToType(), false, 0, false)
		_, err := unmarshalFromReaderNoGroup(
			bytes.NewReader(payload.Bytes()), &target.optSplitResult)
		require.ErrorContains(t, err, "distinct-state presence")
		withDistinct.free()
		target.free()
	})

	source.free()
}

func TestRuntimeAggSerdeSparseChunkSelectionSkipsOmittedGroups(t *testing.T) {
	mp := mpool.MustNewZero()
	ret := initAggResultWithFixedTypeResult[int64](
		mp, types.T_int64.ToType(), true, 0, false)
	ret.optInformation.chunkSize = 2
	require.NoError(t, ret.grows(3))
	ret.values[0][0], ret.values[0][1], ret.values[1][0] = 11, 22, 33
	ret.bsFromEmptyList[0][0] = false
	ret.bsFromEmptyList[0][1] = false
	ret.bsFromEmptyList[1][0] = false

	var encoded bytes.Buffer
	require.NoError(t, marshalRetAndGroupsToBuffer(
		1,
		[][]uint8{nil, {1}},
		&encoded,
		&ret.optSplitResult,
		[]fakeSerdeGroup{"g0", "g1", "g2"},
		nil,
	))
	reader := bytes.NewReader(encoded.Bytes())
	cnt, err := types.ReadInt64(reader)
	require.NoError(t, err)
	require.Equal(t, int64(1), cnt)

	restored := initAggResultWithFixedTypeResult[int64](
		mp, types.T_int64.ToType(), true, 0, false)
	require.NoError(t, restored.unmarshalFromReader(reader))
	require.Equal(t, []int64{33},
		vector.MustFixedColNoTypeCheck[int64](restored.resultList[0]))
	groupCnt, err := types.ReadInt64(reader)
	require.NoError(t, err)
	require.Equal(t, int64(1), groupCnt)
	_, group, err := types.ReadSizeBytes(reader)
	require.NoError(t, err)
	require.Equal(t, "g2", string(group))
	extraCnt, err := types.ReadInt64(reader)
	require.NoError(t, err)
	require.Zero(t, extraCnt)
	require.Zero(t, reader.Len())

	ret.free()
	restored.free()
	require.Zero(t, mp.CurrNB())
}

func TestOptSplitResultRejectsCorruptCountsBeforeAllocation(t *testing.T) {
	mp := mpool.MustNewZero()
	source := vector.NewOffHeapVecWithType(types.T_int64.ToType())
	defer func() {
		source.Free(mp)
		require.Zero(t, mp.CurrNB())
	}()

	makeRecord := func(emptyCount, distinctCount int64) []byte {
		var record bytes.Buffer
		require.NoError(t, source.MarshalBinaryTo(&record))
		require.NoError(t, types.WriteInt64(&record, emptyCount))
		require.NoError(t, types.WriteInt64(&record, distinctCount))
		return record.Bytes()
	}

	for _, tc := range []struct {
		name          string
		emptyCount    int64
		distinctCount int64
	}{
		{name: "negative-empty", emptyCount: -1},
		{name: "negative-distinct", distinctCount: -1},
		{name: "impossible-distinct", distinctCount: 1 << 40},
	} {
		t.Run(tc.name, func(t *testing.T) {
			target := optSplitResult{mp: mp, resultType: types.T_int64.ToType()}
			err := target.unmarshalFromReader(bytes.NewReader(
				makeRecord(tc.emptyCount, tc.distinctCount)))
			require.Error(t, err)
			require.Empty(t, target.resultList)
			require.Empty(t, target.emptyList)
			require.Empty(t, target.distinct)
		})
	}
}
