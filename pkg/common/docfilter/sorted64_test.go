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

package docfilter

import (
	"encoding/binary"
	"math"
	"testing"

	"github.com/matrixorigin/matrixone/pkg/common/mpool"
	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/container/vector"
	"github.com/stretchr/testify/require"
)

func TestSorted64Filter(t *testing.T) {
	mp := mpool.MustNewZero()
	present := buildIntVec(t, mp, types.T_int64.ToType(),
		[]int64{9, -1, 3, 9, math.MinInt64}, map[int]bool{2: true})
	defer present.Free(mp)

	payload, err := BuildSorted64Bytes(present)
	require.NoError(t, err)
	// NULL is omitted and duplicate 9 is canonicalized.
	require.Equal(t, uint64(3), binary.LittleEndian.Uint64(payload[:8]))
	require.Len(t, payload, 8+3*8)

	f, err := NewSorted64Filter(payload)
	require.NoError(t, err)
	require.True(t, f.Exact())
	require.True(t, f.Valid())
	for _, row := range []int{0, 1, 3, 4} {
		require.True(t, f.Test(present.GetRawBytesAt(row)))
	}
	type callbackResult struct {
		exists bool
		null   bool
		row    int
	}
	var callbacks []callbackResult
	require.Equal(t, []uint8{1, 1, 0, 1, 1}, f.TestVector(
		present,
		func(exists, null bool, row int) {
			callbacks = append(callbacks, callbackResult{exists, null, row})
		},
	))
	require.Equal(t, []callbackResult{
		{true, false, 0},
		{true, false, 1},
		{false, true, 2},
		{true, false, 3},
		{true, false, 4},
	}, callbacks)
	absent := buildIntVec(t, mp, types.T_int64.ToType(), []int64{0, 3, 8, 10}, nil)
	defer absent.Free(mp)
	require.Equal(t, []uint8{0, 0, 0, 0}, f.TestVector(absent, nil))

	shared := f.Share()
	f.Free()
	require.True(t, shared.Valid())
	require.True(t, shared.Test(present.GetRawBytesAt(0)))
	shared.Free()
	require.False(t, f.Valid())
}

func TestSorted64ConstVector(t *testing.T) {
	runConstVectorFilterTest(t, func(t *testing.T, v *vector.Vector) constProbeFilter {
		data, err := BuildSorted64Bytes(v)
		require.NoError(t, err)
		f, err := NewSorted64Filter(data)
		require.NoError(t, err)
		return f
	})
}

func TestSorted64TestVectorZeroLengthNonNullConst(t *testing.T) {
	mp := mpool.MustNewZero()
	typ := types.T_int64.ToType()
	source, err := vector.NewConstFixed[int64](typ, 77, 1, mp)
	require.NoError(t, err)
	defer source.Free(mp)
	source.SetLength(0)

	payload, err := BuildSorted64Bytes(source)
	require.NoError(t, err)
	filter, err := NewSorted64Filter(payload)
	require.NoError(t, err)
	defer filter.Free()

	probe, err := vector.NewConstFixed[int64](typ, 77, 2, mp)
	require.NoError(t, err)
	defer probe.Free(mp)
	require.NotPanics(t, func() {
		require.Equal(t, []uint8{0, 0}, filter.TestVector(probe, nil))
	})
}

func TestSorted64RejectsNonCanonicalPayload(t *testing.T) {
	encode := func(values ...uint64) []byte {
		data := make([]byte, 8+len(values)*8)
		binary.LittleEndian.PutUint64(data[:8], uint64(len(values)))
		for i, value := range values {
			binary.LittleEndian.PutUint64(data[8+i*8:], value)
		}
		return data
	}

	for _, data := range [][]byte{
		nil,
		make([]byte, 9),
		append([]byte{2, 0, 0, 0, 0, 0, 0, 0}, make([]byte, 8)...),
		encode(2, 1),
		encode(1, 1),
	} {
		f, err := NewSorted64Filter(data)
		require.Error(t, err)
		require.Nil(t, f)
	}

	empty, err := NewSorted64Filter(make([]byte, 8))
	require.NoError(t, err)
	require.True(t, empty.Valid())
	empty.Free()
}

func TestIntegerFilterSelectionUsesRepresentationCost(t *testing.T) {
	mp := mpool.MustNewZero()
	dense := buildIntVec(t, mp, types.T_uint64.ToType(), []uint64{100, 101, 102}, nil)
	defer dense.Free(mp)
	tag, _, err := BuildIntegerFilter(dense)
	require.NoError(t, err)
	require.Equal(t, TagCbitmap, tag)

	sparse := buildIntVec(t, mp, types.T_uint64.ToType(), []uint64{0, 64}, nil)
	defer sparse.Free(mp)
	tag, _, err = BuildIntegerFilter(sparse)
	require.NoError(t, err)
	require.Equal(t, TagSorted64, tag)
}
