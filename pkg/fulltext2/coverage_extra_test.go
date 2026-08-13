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

package fulltext2

import (
	"bytes"
	"testing"

	"github.com/matrixorigin/matrixone/pkg/container/batch"
	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/util/executor"
	"github.com/stretchr/testify/require"
)

// TestResultScalarInt64EmptyAndNil covers the two fall-through branches of the generation
// scalar reader: an empty result and a nil batch both yield 0 (never panic on Vecs[0]).
func TestResultScalarInt64EmptyAndNil(t *testing.T) {
	require.Equal(t, int64(0), resultScalarInt64(executor.Result{}))                             // no batches
	require.Equal(t, int64(0), resultScalarInt64(executor.Result{Batches: []*batch.Batch{nil}})) // nil batch skipped
}

// TestCdcReadLenGuards covers the corrupt-input guards of the CDC length-prefixed readers: a
// header too short for the uint32 length, and a length that overruns the remaining bytes. Both
// are defense-in-depth on already-CRC'd data that BVT never exercises.
func TestCdcReadLenGuards(t *testing.T) {
	// bytes: only 2 bytes present, uint32 length read fails.
	_, err := cdcReadLenBytes(bytes.NewReader([]byte{0x01, 0x02}), int32(types.T_int64))
	require.Error(t, err)
	_, err = cdcReadLenString(bytes.NewReader([]byte{0x01, 0x02}))
	require.Error(t, err)

	// length=0xFFFF but no payload bytes remain → "truncated".
	truncated := []byte{0xFF, 0xFF, 0x00, 0x00}
	_, err = cdcReadLenBytes(bytes.NewReader(truncated), int32(types.T_int64))
	require.ErrorContains(t, err, "truncated")
	_, err = cdcReadLenString(bytes.NewReader(truncated))
	require.ErrorContains(t, err, "truncated")
}

// TestDecodePostingsGuards covers the ranking-blob format guards: empty (clears the segment),
// too short for the 1-byte format tag + 8-byte header, and an unsupported format byte. The
// happy V6 path is exercised too so the assignment line is covered.
func TestDecodePostingsGuards(t *testing.T) {
	s := &Segment{}
	require.NoError(t, s.decodePostings(nil, nil, nil)) // empty → nil postings
	require.Nil(t, s.ranking)

	require.ErrorContains(t, s.decodePostings([]byte{1, 2, 3}, nil, nil), "too short")

	bad := append([]byte{0xFF}, make([]byte, 8)...) // 9 bytes, wrong format tag
	require.ErrorContains(t, s.decodePostings(bad, nil, nil), "unsupported postings format")

	ok := append([]byte{postingsFormatV1}, make([]byte, 8)...)
	require.NoError(t, s.decodePostings(ok, []byte{1}, []byte{2}))
	require.Equal(t, ok, s.ranking)
}

// TestDecodeTermEntryCorrupt covers the out-of-bounds / bad-uvarint guards in the lazy term
// directory decoder — a corrupt FST value must return (nil,false), never panic or index past
// the ranking slice.
func TestDecodeTermEntryCorrupt(t *testing.T) {
	// off past the end of the (empty) ranking.
	tp, ok := (&Segment{}).decodeTermEntry(0)
	require.False(t, ok)
	require.Nil(t, tp)

	// leading uvarint overflows (10 continuation bytes) → first read fails.
	overflow := bytes.Repeat([]byte{0xFF}, 10)
	_, ok = (&Segment{ranking: overflow}).decodeTermEntry(0)
	require.False(t, ok)

	// one valid uvarint then EOF → the second read fails.
	_, ok = (&Segment{ranking: []byte{0x01}}).decodeTermEntry(0)
	require.False(t, ok)

	// four valid uvarints then nothing → the termMaxTf byte read is out of bounds.
	_, ok = (&Segment{ranking: []byte{1, 1, 1, 1}}).decodeTermEntry(0)
	require.False(t, ok)
}

// TestFrameSegmentRoundTrip covers the insert-frame codec: FrameSegment → UnframeSegment recovers
// the same doc count, UnframeTail dispatches the insert frame to the segment (not deletes) arm, and
// a corrupt frame is rejected.
func TestFrameSegmentRoundTrip(t *testing.T) {
	b := NewBuilder("f", int32(types.T_int64))
	feed(t, b, int64(0), "quick", "brown")
	feed(t, b, int64(1), "quick", "fox")
	seg, err := b.Finish()
	require.NoError(t, err)

	framed, err := FrameSegment(seg)
	require.NoError(t, err)
	require.NotEmpty(t, framed)

	seg2, err := UnframeSegment("f", framed)
	require.NoError(t, err)
	require.Equal(t, seg.numDocs(), seg2.numDocs())

	// UnframeTail on the same insert frame → segment set, no deletes.
	s3, dels, err := UnframeTail("f", framed)
	require.NoError(t, err)
	require.NotNil(t, s3)
	require.Nil(t, dels)

	// corrupt frame → error (UnframeCdcChunk rejects it).
	_, err = UnframeSegment("f", []byte{0x00, 0x01, 0x02})
	require.Error(t, err)
}

// TestAsBytes covers the pk raw-bytes coercion: []byte and string pass through, everything else
// (a boxed fixed-width pk) returns nil so callers fall back to typed encoding.
func TestAsBytes(t *testing.T) {
	require.Equal(t, []byte("raw"), asBytes([]byte("raw")))
	require.Equal(t, []byte("str"), asBytes("str"))
	require.Nil(t, asBytes(int64(5)))
	require.Nil(t, asBytes(nil))
}
