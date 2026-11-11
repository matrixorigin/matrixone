// Copyright 2026 Matrix Origin
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
// http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package batch

import (
	"bytes"
	"strings"
	"testing"

	"github.com/matrixorigin/matrixone/pkg/common/mpool"
	"github.com/matrixorigin/matrixone/pkg/container/nulls"
	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/container/vector"
	"github.com/stretchr/testify/require"
)

func flatCopyDictionary(t *testing.T, source *vector.Vector, mp *mpool.MPool, allocation *vector.AllocationAccountSelection) *vector.Vector {
	t.Helper()
	base, err := vector.FreezeVector(source, mp, allocation)
	require.NoError(t, err)
	defer base.Release()
	rows, err := vector.NewRowSelection([]int64{2, 0, 2, 1}, 3, mp, allocation)
	require.NoError(t, err)
	defer rows.Release()
	result, err := vector.NewDictionaryVector(base, rows, mp, allocation)
	require.NoError(t, err)
	return result
}

func TestFlatCopyPreservesLogicalBatchAndCodecBoundaries(t *testing.T) {
	mp := mpool.MustNewZero()
	state := newTestBatchAllocationAccount(t, 128)
	t.Cleanup(func() { finalizeTestBatchAllocationAccount(t, state); require.Zero(t, mp.CurrNB()) })
	ints := vector.NewVec(types.T_int64.ToType())
	t.Cleanup(func() { ints.Free(mp) })
	require.NoError(t, vector.AppendFixedList(ints, []int64{10, 20, 30}, nil, mp))
	nulls.Add(ints.GetNulls(), 1)
	nulls.Add(ints.GetGrouping(), 2)
	ints.SetIsBin(true)
	ints.SetPrepareParamType(types.T_int64)
	text := vector.NewVec(types.T_varchar.ToType())
	t.Cleanup(func() { text.Free(mp) })
	long := strings.Repeat("selected", 8)
	for _, value := range []string{"short", "null", long} {
		require.NoError(t, vector.AppendBytes(text, []byte(value), false, mp))
	}
	source := NewWithSize(3)
	t.Cleanup(func() { source.Clean(mp) })
	source.Vecs[0] = flatCopyDictionary(t, ints, mp, nil)
	source.Vecs[1] = flatCopyDictionary(t, text, mp, nil)
	var err error
	source.Vecs[2], err = vector.NewConstFixed(types.T_int32.ToType(), int32(7), 4, mp)
	require.NoError(t, err)
	source.Attrs = []string{"number", "text", "constant"}
	source.Recursive, source.ShuffleIDX = 1, 3
	source.ExtraBuf = []byte("auxiliary")
	source.SetRowCount(4)
	require.True(t, source.HasDictionary())
	_, err = source.MarshalBinary()
	require.Error(t, err)
	_, err = BatchToProtoBatch(source)
	require.Error(t, err)
	output, err := source.FlatCopy(mp, state.selection)
	require.NoError(t, err)
	t.Cleanup(func() { output.Clean(mp) })
	require.False(t, output.HasDictionary())
	require.Equal(t, source.Attrs, output.Attrs)
	require.Equal(t, 4, output.RowCount())
	require.Equal(t, source.Recursive, output.Recursive)
	require.Equal(t, source.ShuffleIDX, output.ShuffleIDX)
	require.Equal(t, source.ExtraBuf, output.ExtraBuf)
	require.True(t, output.HasAccountedExtraBuffer())
	require.Equal(t, []int64{30, 10, 30}, vector.MustFixedColNoTypeCheck[int64](output.Vecs[0])[:3])
	require.True(t, output.Vecs[0].IsNull(3))
	require.True(t, output.Vecs[0].GetGrouping().Contains(0))
	require.True(t, output.Vecs[0].GetGrouping().Contains(2))
	require.True(t, output.Vecs[0].GetIsBin())
	require.Equal(t, types.T_int64, output.Vecs[0].GetPrepareParamType())
	require.True(t, output.Vecs[2].IsConst())
	for row, want := range []string{long, "short", long, "null"} {
		require.Equal(t, want, string(output.Vecs[1].GetBytesAt(row)))
	}
	// The spill format preserves grouping; ordinary and protobuf formats keep
	// their pre-existing metadata contracts. All receive explicit flat data.
	var spill bytes.Buffer
	require.NoError(t, output.MarshalBinaryWithGroupingTo(&spill))
	decoded := NewWithSize(0)
	t.Cleanup(func() { decoded.Clean(mp) })
	require.NoError(t, decoded.UnmarshalFromReaderWithGrouping(bytes.NewReader(spill.Bytes()), mp))
	require.Equal(t, int64(30), vector.GetFixedAtNoTypeCheck[int64](decoded.Vecs[0], 2))
	require.True(t, decoded.Vecs[0].GetGrouping().Contains(2))
	var legacy bytes.Buffer
	require.NoError(t, output.Vecs[0].MarshalBinaryWithBufferV1(&legacy))
	encoded, err := output.MarshalBinary()
	require.NoError(t, err)
	require.NoError(t, decoded.UnmarshalBinaryWithAnyMp(encoded, mp))
	require.Equal(t, 4, decoded.RowCount())
	proto, err := BatchToProtoBatch(output)
	require.NoError(t, err)
	restored, err := ProtoBatchToBatch(proto)
	require.NoError(t, err)
	t.Cleanup(func() { restored.Clean(mp) })
	require.Equal(t, int32(7), vector.GetFixedAtNoTypeCheck[int32](restored.Vecs[2], 3))
	// Independent destination storage, including headers and batch metadata.
	output.Attrs[0] = "changed"
	output.ExtraBuf[0] = '!'
	require.NoError(t, vector.SetFixedAtNoTypeCheck(output.Vecs[0], 0, int64(99)))
	require.Equal(t, "number", source.Attrs[0])
	require.Equal(t, byte('a'), source.ExtraBuf[0])
	require.Equal(t, int64(30), vector.GetFixedAtNoTypeCheck[int64](source.Vecs[0], 0))
	require.True(t, source.HasDictionary())
}

func TestFlatCopyFailureReleasesPartialPublication(t *testing.T) {
	mp := mpool.MustNewZero()
	registry, err := mpool.NewAllocationAccountRegistry(1, 32)
	require.NoError(t, err)
	controller := &rejectNextBatchAllocation{}
	account, err := registry.OpenWithController(1<<20, controller)
	require.NoError(t, err)
	allocation, err := vector.NewAllocationAccountSelection(account, 1, 1, 2, 3, 4)
	require.NoError(t, err)
	state := testBatchAllocationAccount{registry: registry, account: account, selection: allocation}
	t.Cleanup(func() {
		finalizeTestBatchAllocationAccount(t, state)
		require.Zero(t, controller.used)
		require.Zero(t, mp.CurrNB())
	})
	source := NewWithSize(2)
	t.Cleanup(func() { source.Clean(mp) })
	for i := range source.Vecs {
		source.Vecs[i] = vector.NewVec(types.T_int64.ToType())
		require.NoError(t, vector.AppendFixedList(source.Vecs[i], []int64{3, 4}, nil, mp))
	}
	source.SetRowCount(2)
	// Fail the second column and then ExtraBuf, after prior columns succeeded.
	source.ExtraBuf = []byte("extra")
	for _, failAt := range []int{2, 3} {
		controller.calls, controller.failAt = 0, failAt
		output, err := source.FlatCopy(mp, allocation)
		require.ErrorIs(t, err, mpool.ErrAllocationAccountCapacity)
		require.Nil(t, output)
		require.Zero(t, account.Snapshot().Used)
		require.Zero(t, controller.used)
		require.Equal(t, []int64{3, 4}, vector.MustFixedColNoTypeCheck[int64](source.Vecs[0]))
	}
	controller.failAt = 0
	output, err := source.FlatCopy(mp, allocation)
	require.NoError(t, err)
	output.Clean(mp)
}

func TestFlatCopyConstantNullAndEmpty(t *testing.T) {
	mp := mpool.MustNewZero()
	t.Cleanup(func() { require.Zero(t, mp.CurrNB()) })
	for _, rows := range []int{0, 4} {
		source := NewWithSize(1)
		source.Vecs[0] = vector.NewConstNull(types.T_int64.ToType(), rows, mp)
		source.SetRowCount(rows)
		func() {
			defer source.Clean(mp)
			output, err := source.FlatCopy(mp, nil)
			require.NoError(t, err)
			defer output.Clean(mp)
			require.True(t, output.Vecs[0].IsConstNull())
			require.Equal(t, rows, output.RowCount())
			proto, err := BatchToProtoBatch(output)
			require.NoError(t, err)
			require.Empty(t, proto.Vecs[0].Data)
			decoded, err := ProtoBatchToBatch(proto)
			require.NoError(t, err)
			defer decoded.Clean(mp)
			require.True(t, decoded.Vecs[0].IsConstNull())
		}()
	}
	var missing *Batch
	require.False(t, missing.HasDictionary())
	_, err := missing.FlatCopy(mp, nil)
	require.Error(t, err)
}
