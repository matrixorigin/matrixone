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

func TestSpillCodecRoundTripDropsExtraBuf(t *testing.T) {
	mp := mpool.MustNewZero()
	source := newTwoColumnTestBatch(t, mp)
	require.NoError(t, source.SetExtraBuf(mp, bytes.Repeat([]byte("x"), 1<<20)))

	spillSize, err := source.MarshalBinaryForSpillSize()
	require.NoError(t, err)
	stableSize, err := source.MarshalBinarySize()
	require.NoError(t, err)
	require.Greater(t, stableSize-spillSize, len(source.ExtraBuf())/2)

	var encoded bytes.Buffer
	require.NoError(t, source.MarshalBinaryForSpillTo(&encoded))
	decoded := NewWithSize(0)
	require.NoError(t, decoded.UnmarshalFromReaderForSpill(&encoded, mp))
	require.Empty(t, decoded.ExtraBuf())
	require.Equal(t, source.RowCount(), decoded.RowCount())
	require.Len(t, decoded.Vecs, len(source.Vecs))
	require.Equal(t,
		vector.MustFixedColWithTypeCheck[int64](source.Vecs[0]),
		vector.MustFixedColWithTypeCheck[int64](decoded.Vecs[0]))
	require.Equal(t, source.Vecs[1].GetBytesAt(31), decoded.Vecs[1].GetBytesAt(31))
	require.Zero(t, encoded.Len(), "spill codec must consume its whole frame")

	// Reusing the decoded batch for a second frame overwrites it in place.
	second := newTwoColumnTestBatch(t, mp)
	require.NoError(t, second.Shrink([]int64{0, 1, 2}, false, mp))
	require.NoError(t, second.MarshalBinaryForSpillTo(&encoded))
	require.NoError(t, decoded.UnmarshalFromReaderForSpill(&encoded, mp))
	require.Equal(t, second.RowCount(), decoded.RowCount())

	decoded.Clean(mp)
	second.Clean(mp)
	source.Clean(mp)
	require.Zero(t, mp.CurrNB())
}

// A spill frame carries no names or extra buffer. A frame that has them is
// rejected before the decoder allocates for them.
func TestSpillCodecRejectsStableMetadataBeforePayloadAllocation(t *testing.T) {
	for _, test := range []struct {
		name  string
		attrs []string
		extra []byte
		want  string
	}{
		{
			name:  "attributes",
			attrs: []string{string(bytes.Repeat([]byte("a"), 1<<20))},
			want:  "attributes are not allowed",
		},
		{
			name:  "extra buffer",
			extra: bytes.Repeat([]byte("x"), 1<<20),
			want:  "extra buffer is not allowed",
		},
	} {
		t.Run(test.name, func(t *testing.T) {
			mp := mpool.MustNewZero()
			source := NewWithSize(0)
			if test.extra != nil {
				require.NoError(t, source.SetExtraBuf(mp, test.extra))
			}
			source.Attrs = test.attrs
			var payload bytes.Buffer
			require.NoError(t, source.MarshalBinaryTo(&payload))
			source.Clean(mp)
			var encoded bytes.Buffer
			payloadSize := int64(payload.Len())
			encoded.Write(types.EncodeInt64(&payloadSize))
			encoded.Write(payload.Bytes())

			decoded := NewWithSize(0)
			require.ErrorContains(t, decoded.UnmarshalFromReaderForSpill(&encoded, mp), test.want)
			require.Zero(t, mp.CurrNB())
			decoded.Clean(mp)
		})
	}
}

func TestSpillCodecRejectsMismatchedRowCount(t *testing.T) {
	mp := mpool.MustNewZero()
	source := NewWithSize(1)
	source.Vecs[0] = vector.NewVec(types.T_int64.ToType(), mp)
	require.NoError(t, vector.AppendFixed(source.Vecs[0], int64(1), false, mp))
	source.SetRowCount(2)

	var encoded bytes.Buffer
	require.NoError(t, source.MarshalBinaryForSpillTo(&encoded))
	decoded := NewWithSize(0)
	require.ErrorContains(t,
		decoded.UnmarshalFromReaderForSpill(&encoded, mp),
		"vector length does not match row count",
	)

	decoded.Clean(mp)
	source.Clean(mp)
	require.Zero(t, mp.CurrNB())
}
