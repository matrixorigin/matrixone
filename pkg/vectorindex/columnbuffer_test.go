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

package vectorindex

import (
	"encoding/binary"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/matrixorigin/matrixone/pkg/common/mpool"
	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/container/vector"
)

// fixedBuf lays out n little-endian values of the given width, the ColumnBuffer
// fixed-width layout.
func fixedBuf(t types.T, width int, vals []uint64) *ColumnBuffer {
	d := make([]byte, len(vals)*width)
	for i, v := range vals {
		switch width {
		case 1:
			d[i] = byte(v)
		case 2:
			binary.LittleEndian.PutUint16(d[i*2:], uint16(v))
		case 4:
			binary.LittleEndian.PutUint32(d[i*4:], uint32(v))
		case 8:
			binary.LittleEndian.PutUint64(d[i*8:], v)
		}
	}
	return &ColumnBuffer{Type: t, Data: d, N: len(vals)}
}

// varlenaBuf lays out n [u32 len][content] entries, the ColumnBuffer varlena layout.
func varlenaBuf(t types.T, vals []string) *ColumnBuffer {
	var d []byte
	for _, v := range vals {
		var hdr [4]byte
		binary.LittleEndian.PutUint32(hdr[:], uint32(len(v)))
		d = append(d, hdr[:]...)
		d = append(d, v...)
	}
	return &ColumnBuffer{Type: t, Data: d, N: len(vals)}
}

// Every supported type decodes into a vector of the same type with the right
// element count.
func TestAppendColumnBuffer_AllTypes(t *testing.T) {
	mp := mpool.MustNewZero()

	for _, c := range []struct {
		name string
		buf  *ColumnBuffer
	}{
		{"int64", fixedBuf(types.T_int64, 8, []uint64{1, 2, 3})},
		{"uint64", fixedBuf(types.T_uint64, 8, []uint64{1, 2, 3})},
		{"bit", fixedBuf(types.T_bit, 8, []uint64{1, 2, 3})},
		{"int32", fixedBuf(types.T_int32, 4, []uint64{1, 2, 3})},
		{"uint32", fixedBuf(types.T_uint32, 4, []uint64{1, 2, 3})},
		{"int16", fixedBuf(types.T_int16, 2, []uint64{1, 2, 3})},
		{"uint16", fixedBuf(types.T_uint16, 2, []uint64{1, 2, 3})},
		{"int8", fixedBuf(types.T_int8, 1, []uint64{1, 2, 3})},
		{"uint8", fixedBuf(types.T_uint8, 1, []uint64{1, 2, 3})},
		{"date", fixedBuf(types.T_date, 4, []uint64{1, 2, 3})},
		{"datetime", fixedBuf(types.T_datetime, 8, []uint64{1, 2, 3})},
		{"time", fixedBuf(types.T_time, 8, []uint64{1, 2, 3})},
		{"timestamp", fixedBuf(types.T_timestamp, 8, []uint64{1, 2, 3})},
		{"decimal64", fixedBuf(types.T_decimal64, 8, []uint64{1, 2, 3})},
		{"decimal128", fixedBuf(types.T_decimal128, 16, []uint64{1, 2, 3, 4, 5, 6})},
		{"varchar", varlenaBuf(types.T_varchar, []string{"a", "bb", "ccc"})},
		{"char", varlenaBuf(types.T_char, []string{"a", "bb", "ccc"})},
		{"text", varlenaBuf(types.T_text, []string{"a", "bb", "ccc"})},
		{"datalink", varlenaBuf(types.T_datalink, []string{"a", "bb", "ccc"})},
		{"binary", varlenaBuf(types.T_binary, []string{"a", "bb", "ccc"})},
		{"varbinary", varlenaBuf(types.T_varbinary, []string{"a", "bb", "ccc"})},
		{"blob", varlenaBuf(types.T_blob, []string{"a", "bb", "ccc"})},
		{"json", varlenaBuf(types.T_json, []string{"a", "bb", "ccc"})},
		{"uuid", varlenaBuf(types.T_uuid, []string{
			"00000000-0000-0000-0000-000000000001",
			"00000000-0000-0000-0000-000000000002",
			"00000000-0000-0000-0000-000000000003",
		})},
	} {
		t.Run(c.name, func(t *testing.T) {
			if c.name == "decimal128" {
				c.buf.N = 3 // six uint64 halves make three decimal128 values
			}
			vec := vector.NewVec(types.New(c.buf.Type, 0, 0))
			defer vec.Free(mp)
			require.NoError(t, AppendColumnBuffer(c.buf, vec, mp))
			require.Equal(t, 3, vec.Length())
		})
	}
}

func TestAppendColumnBuffer_UnsupportedType(t *testing.T) {
	mp := mpool.MustNewZero()
	vec := vector.NewVec(types.New(types.T_any, 0, 0))
	defer vec.Free(mp)
	require.Error(t, AppendColumnBuffer(&ColumnBuffer{Type: types.T_any, N: 1}, vec, mp))
}

// AppendColumnBufferRange decodes [start, start+n). A varlena column walks its
// length prefixes to reach start.
func TestAppendColumnBufferRange(t *testing.T) {
	mp := mpool.MustNewZero()

	t.Run("fixed", func(t *testing.T) {
		buf := fixedBuf(types.T_int64, 8, []uint64{10, 20, 30, 40})
		vec := vector.NewVec(types.New(types.T_int64, 0, 0))
		defer vec.Free(mp)
		require.NoError(t, AppendColumnBufferRange(buf, vec, 1, 2, mp))
		require.Equal(t, []int64{20, 30}, vector.MustFixedColNoTypeCheck[int64](vec))
	})

	t.Run("varlena", func(t *testing.T) {
		buf := varlenaBuf(types.T_varchar, []string{"a", "bb", "ccc", "dddd"})
		vec := vector.NewVec(types.New(types.T_varchar, 0, 0))
		defer vec.Free(mp)
		require.NoError(t, AppendColumnBufferRange(buf, vec, 2, 2, mp))
		require.Equal(t, 2, vec.Length())
		require.Equal(t, "ccc", vec.GetStringAt(0))
		require.Equal(t, "dddd", vec.GetStringAt(1))
	})

	// A negative start reads from 0; an over-long range stops at N.
	t.Run("clamped", func(t *testing.T) {
		buf := fixedBuf(types.T_int64, 8, []uint64{10, 20})
		vec := vector.NewVec(types.New(types.T_int64, 0, 0))
		defer vec.Free(mp)
		require.NoError(t, AppendColumnBufferRange(buf, vec, -5, 100, mp))
		require.Equal(t, []int64{10, 20}, vector.MustFixedColNoTypeCheck[int64](vec))
	})

	t.Run("empty range", func(t *testing.T) {
		buf := fixedBuf(types.T_int64, 8, []uint64{10, 20})
		vec := vector.NewVec(types.New(types.T_int64, 0, 0))
		defer vec.Free(mp)
		require.NoError(t, AppendColumnBufferRange(buf, vec, 5, 1, mp))
		require.Equal(t, 0, vec.Length())
	})
}

// A NULL element occupies a placeholder in Data; the following element still decodes.
func TestAppendColumnBuffer_Nulls(t *testing.T) {
	mp := mpool.MustNewZero()

	t.Run("fixed", func(t *testing.T) {
		buf := fixedBuf(types.T_int64, 8, []uint64{10, 0, 30})
		buf.Nulls = []bool{false, true, false}
		vec := vector.NewVec(types.New(types.T_int64, 0, 0))
		defer vec.Free(mp)
		require.NoError(t, AppendColumnBuffer(buf, vec, mp))
		require.Equal(t, 3, vec.Length())
		require.True(t, vec.IsNull(1))
		require.False(t, vec.IsNull(2))
		require.Equal(t, int64(30), vector.MustFixedColNoTypeCheck[int64](vec)[2])
	})

	t.Run("varlena", func(t *testing.T) {
		buf := varlenaBuf(types.T_varchar, []string{"a", "", "ccc"})
		buf.Nulls = []bool{false, true, false}
		vec := vector.NewVec(types.New(types.T_varchar, 0, 0))
		defer vec.Free(mp)
		require.NoError(t, AppendColumnBuffer(buf, vec, mp))
		require.True(t, vec.IsNull(1))
		require.Equal(t, "ccc", vec.GetStringAt(2))
	})

	t.Run("uuid", func(t *testing.T) {
		buf := varlenaBuf(types.T_uuid, []string{
			"00000000-0000-0000-0000-000000000001", "",
		})
		buf.Nulls = []bool{false, true}
		vec := vector.NewVec(types.New(types.T_uuid, 0, 0))
		defer vec.Free(mp)
		require.NoError(t, AppendColumnBuffer(buf, vec, mp))
		require.True(t, vec.IsNull(1))
	})

	// A malformed uuid returns the parse error.
	t.Run("bad uuid", func(t *testing.T) {
		vec := vector.NewVec(types.New(types.T_uuid, 0, 0))
		defer vec.Free(mp)
		require.Error(t, AppendColumnBuffer(varlenaBuf(types.T_uuid, []string{"nope"}), vec, mp))
	})
}

// Reset zeroes N and truncates Data/Nulls, keeping their capacity.
func TestColumnBufferReset(t *testing.T) {
	buf := fixedBuf(types.T_int64, 8, []uint64{1, 2, 3})
	buf.Nulls = []bool{false, false, false}
	dataCap, nullsCap := cap(buf.Data), cap(buf.Nulls)

	buf.Reset()
	require.Equal(t, 0, buf.N)
	require.Empty(t, buf.Data)
	require.Empty(t, buf.Nulls)
	require.Equal(t, dataCap, cap(buf.Data), "the data buffer is reused, not dropped")
	require.Equal(t, nullsCap, cap(buf.Nulls))

	// A pk column leaves Nulls nil; Reset must not materialize one.
	pk := fixedBuf(types.T_int64, 8, []uint64{1})
	pk.Reset()
	require.Nil(t, pk.Nulls)
}
