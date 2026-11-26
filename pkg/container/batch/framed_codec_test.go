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

// A framed record is the batch with an int64 length in front; a file of
// records reads back one batch at a time, the extra buffer included.
func TestFramedRecordsRoundTrip(t *testing.T) {
	mp := mpool.MustNewZero()
	source := newTwoColumnTestBatch(t, mp)
	require.NoError(t, source.SetExtraBuf(mp, bytes.Repeat([]byte("x"), 1<<10)))

	framedSize, err := source.MarshalFramedSize()
	require.NoError(t, err)
	size, err := source.MarshalBinarySize()
	require.NoError(t, err)
	require.Equal(t, size+8, framedSize)

	var encoded bytes.Buffer
	require.NoError(t, source.MarshalFramedTo(&encoded))
	require.Equal(t, framedSize, encoded.Len())
	second := newTwoColumnTestBatch(t, mp)
	require.NoError(t, second.Shrink([]int64{0, 1, 2}, false, mp))
	require.NoError(t, second.MarshalFramedTo(&encoded))

	decoded := NewWithSize(0)
	require.NoError(t, decoded.UnmarshalFramedFrom(&encoded, mp))
	require.Equal(t, source.ExtraBuf(), decoded.ExtraBuf())
	require.Equal(t, source.RowCount(), decoded.RowCount())
	require.Equal(t,
		vector.MustFixedColWithTypeCheck[int64](source.Vecs[0]),
		vector.MustFixedColWithTypeCheck[int64](decoded.Vecs[0]))
	require.Equal(t, source.Vecs[1].GetBytesAt(31), decoded.Vecs[1].GetBytesAt(31))

	// Reusing the decoded batch for the next record overwrites it in place.
	require.NoError(t, decoded.UnmarshalFramedFrom(&encoded, mp))
	require.Equal(t, second.RowCount(), decoded.RowCount())
	require.Empty(t, decoded.ExtraBuf())
	require.Zero(t, encoded.Len(), "each record is consumed whole")

	decoded.Clean(mp)
	second.Clean(mp)
	source.Clean(mp)
	require.Zero(t, mp.CurrNB())
}

// A record whose length disagrees with its batch is rejected: too short
// before the decoder runs past it, too long by the bytes left over.
func TestFramedRecordRejectsWrongLength(t *testing.T) {
	mp := mpool.MustNewZero()
	source := newTwoColumnTestBatch(t, mp)
	payload, err := source.MarshalBinary()
	require.NoError(t, err)
	payload = append([]byte(nil), payload...)
	source.Clean(mp)

	for name, size := range map[string]int64{
		"short":    int64(len(payload) - 1),
		"long":     int64(len(payload) + 1),
		"negative": -1,
	} {
		t.Run(name, func(t *testing.T) {
			var encoded bytes.Buffer
			encoded.Write(types.EncodeInt64(&size))
			encoded.Write(payload)
			encoded.WriteByte(0)
			decoded := NewWithSize(0)
			require.Error(t, decoded.UnmarshalFramedFrom(&encoded, mp))
			decoded.Clean(mp)
			require.Zero(t, mp.CurrNB())
		})
	}
}

func TestFramedRecordRejectsMismatchedRowCount(t *testing.T) {
	mp := mpool.MustNewZero()
	source := NewWithSize(1)
	source.Vecs[0] = vector.NewVec(types.T_int64.ToType(), mp)
	require.NoError(t, vector.AppendFixed(source.Vecs[0], int64(1), false, mp))
	source.SetRowCount(2)

	var encoded bytes.Buffer
	require.NoError(t, source.MarshalFramedTo(&encoded))
	decoded := NewWithSize(0)
	require.ErrorContains(t,
		decoded.UnmarshalFramedFrom(&encoded, mp),
		"vector length does not match row count",
	)

	decoded.Clean(mp)
	source.Clean(mp)
	require.Zero(t, mp.CurrNB())
}
