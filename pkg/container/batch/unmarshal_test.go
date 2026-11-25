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

package batch

import (
	"bytes"
	"testing"

	"github.com/matrixorigin/matrixone/pkg/common/mpool"
	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/container/vector"
	"github.com/stretchr/testify/require"
)

// newTwoColumnTestBatch builds a 32-row (int64, varchar) batch allocated from
// mp. The strings are long enough to live outside the varlena header.
func newTwoColumnTestBatch(t *testing.T, mp *mpool.MPool) *Batch {
	t.Helper()
	bat := NewWithSize(2)
	bat.Vecs[0] = vector.NewVec(types.T_int64.ToType(), mp)
	bat.Vecs[1] = vector.NewVec(types.T_varchar.ToType(), mp)
	for i := 0; i < 32; i++ {
		require.NoError(t, vector.AppendFixed(bat.Vecs[0], int64(i), false, mp))
		require.NoError(t, vector.AppendBytes(
			bat.Vecs[1], []byte("batch test payload that is not inline"), false, mp,
		))
	}
	bat.SetRowCount(32)
	return bat
}

func TestUnmarshalFromReaderAcceptsBitmapCapacityBeyondLogicalRows(t *testing.T) {
	mp := mpool.MustNewZero()
	source := newTwoColumnTestBatch(t, mp)
	source.Vecs[0].GetNulls().Add(31)
	require.NoError(t, source.Shrink([]int64{0, 1, 2, 3, 4}, false, mp))

	var encoded bytes.Buffer
	require.NoError(t, source.MarshalBinaryTo(&encoded))
	decoded := NewWithSize(0)
	require.NoError(t, decoded.UnmarshalFromReader(&encoded, mp))
	require.Equal(t, 5, decoded.RowCount())
	require.Equal(t, int64(0), vector.GetFixedAtWithTypeCheck[int64](decoded.Vecs[0], 0))

	decoded.Clean(mp)
	source.Clean(mp)
	require.Zero(t, mp.CurrNB())
}

func TestUnmarshalFromReaderPreservesRowsWhenVectorCountChanges(t *testing.T) {
	mp := mpool.MustNewZero()
	source := newTwoColumnTestBatch(t, mp)

	var encoded bytes.Buffer
	require.NoError(t, source.MarshalBinaryTo(&encoded))
	decoded := NewWithSize(1)
	decoded.Vecs[0] = vector.NewVec(types.T_int64.ToType(), mp)
	decoded.SetRowCount(7)
	require.NoError(t, decoded.UnmarshalFromReader(&encoded, mp))
	require.Equal(t, source.RowCount(), decoded.RowCount())
	require.Len(t, decoded.Vecs, 2)
	require.Equal(t, int64(31), vector.GetFixedAtWithTypeCheck[int64](decoded.Vecs[0], 31))

	decoded.Clean(mp)
	source.Clean(mp)
	require.Zero(t, mp.CurrNB())
}

func TestUnmarshalFromReaderRejectsInvalidLengthsBeforeAllocation(t *testing.T) {
	encode := func(values ...[]byte) []byte {
		return bytes.Join(values, nil)
	}
	zeroRows := int64(0)
	zeroCount := int32(0)
	negative := int32(-1)
	huge := int32(1<<20 + 1)
	oversized := int32(1 << 30)

	for _, test := range []struct {
		name string
		data []byte
	}{
		{
			name: "negative vector count",
			data: encode(types.EncodeInt64(&zeroRows), types.EncodeInt32(&negative)),
		},
		{
			name: "huge vector count",
			data: encode(types.EncodeInt64(&zeroRows), types.EncodeInt32(&huge)),
		},
		{
			name: "negative extra buffer size",
			data: encode(types.EncodeInt64(&zeroRows), types.EncodeInt32(&zeroCount), types.EncodeInt32(&negative)),
		},
		{
			name: "extra buffer larger than the input",
			data: encode(
				types.EncodeInt64(&zeroRows),
				types.EncodeInt32(&zeroCount),
				types.EncodeInt32(&oversized),
			),
		},
	} {
		t.Run(test.name, func(t *testing.T) {
			mp := mpool.MustNewZero()
			decoded := NewWithSize(0)
			require.NotPanics(t, func() {
				require.Error(t, decoded.UnmarshalFromReader(bytes.NewReader(test.data), mp))
			})
			require.Zero(t, mp.CurrNB(), "nothing is allocated for a rejected length")
			decoded.Clean(mp)
		})
	}
}

// A receiver vector owned by another pool is not the decoder's to free.
func TestUnmarshalRejectsForeignReceiver(t *testing.T) {
	mp := mpool.MustNewZero()
	other := mpool.MustNewZero()
	source := newTwoColumnTestBatch(t, mp)
	data, err := source.MarshalBinary()
	require.NoError(t, err)

	foreign := newTwoColumnTestBatch(t, other)
	require.Error(t, foreign.UnmarshalBinaryWithAnyMp(data, mp))
	foreign.Clean(other)
	require.Zero(t, other.CurrNB())

	// A receiver from the decoding pool is reused.
	own := newTwoColumnTestBatch(t, mp)
	require.NoError(t, own.UnmarshalBinaryWithAnyMp(data, mp))
	require.Equal(t, source.RowCount(), own.RowCount())
	require.Equal(t, source.Vecs[1].GetStringAt(31), own.Vecs[1].GetStringAt(31))

	own.Clean(mp)
	source.Clean(mp)
	require.Zero(t, mp.CurrNB())
}
