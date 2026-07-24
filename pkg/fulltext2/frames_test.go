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
	"fmt"
	"testing"

	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/monlp/tokenizer"
	"github.com/matrixorigin/matrixone/pkg/vectorindex"
	"github.com/stretchr/testify/require"
)

// bigSegment builds an n-doc segment plus one doc with a unique needle token, so
// its serialization is large enough to span multiple storage chunks.
func bigSegment(t *testing.T, n int) (*Segment, int64) {
	t.Helper()
	docs := make([]Doc, 0, n+1)
	for i := 1; i <= n; i++ {
		docs = append(docs, Doc{int64(i), []byte(fmt.Sprintf("word%d alpha beta", i))})
	}
	needle := int64(n + 1)
	docs = append(docs, Doc{needle, []byte("uniqueneedletoken")})
	s, err := BuildSegmentFromDocs("big", int32(types.T_int64), docs, tokenizer.NewSimpleTokenizer())
	require.NoError(t, err)
	return s, needle
}

// TestFramesMultiChunkRoundtrip: a large segment framed, split into chunks,
// shuffled, re-ordered O(n), reassembled, and unframed back to a queryable
// segment. Exercises the multi-chunk path.
func TestFramesMultiChunkRoundtrip(t *testing.T) {
	seg, needle := bigSegment(t, 4000)
	framed, err := FrameSegment(seg)
	require.NoError(t, err)

	chunks := splitFrameChunks(0, framed)
	if len(framed) > vectorindex.MaxChunkSize {
		require.Greater(t, len(chunks), 1, "a segment larger than MaxChunkSize must span multiple chunks")
	}

	// storage returns rows in arbitrary order → reverse, then O(n) place.
	shuffled := make([]TailChunk, len(chunks))
	for i := range chunks {
		shuffled[i] = chunks[len(chunks)-1-i]
	}
	ordered, err := orderTailChunks(shuffled)
	require.NoError(t, err)
	frames, err := reassembleFrames(ordered)
	require.NoError(t, err)
	require.Len(t, frames, 1)
	require.EqualValues(t, 0, frames[0].ChunkId)

	loaded, err := UnframeSegment("big", frames[0].Data)
	require.NoError(t, err)
	t.Cleanup(func() { _ = loaded.dict.Close() })

	idx := NewIndex([]*Segment{loaded}, nil)
	require.Equal(t, int64(4001), idx.NumDocs())
	require.Equal(t, []any{needle}, iquery(t, idx, "uniqueneedletoken"))
}

// TestFramesSingleChunk: a small segment yields one chunk at its start chunk_id.
func TestFramesSingleChunk(t *testing.T) {
	s, err := BuildSegmentFromDocs("s", int32(types.T_int64),
		[]Doc{{int64(1), []byte("quick brown fox")}}, tokenizer.NewSimpleTokenizer())
	require.NoError(t, err)

	framed, err := FrameSegment(s)
	require.NoError(t, err)
	chunks := splitFrameChunks(5, framed)
	require.Len(t, chunks, 1)
	require.EqualValues(t, 5, chunks[0].ChunkId)

	frames, err := reassembleFrames(chunks)
	require.NoError(t, err)
	loaded, err := UnframeSegment("s", frames[0].Data)
	require.NoError(t, err)
	t.Cleanup(func() { _ = loaded.dict.Close() })
	require.Equal(t, int64(1), loaded.N)
}

// TestOrderTailChunksGap: a missing/duplicate chunk_id is corruption, reported.
func TestOrderTailChunksGap(t *testing.T) {
	_, err := orderTailChunks([]TailChunk{{ChunkId: 0}, {ChunkId: 2}}) // gap at 1
	require.Error(t, err)

	ordered, err := orderTailChunks([]TailChunk{{ChunkId: 7}, {ChunkId: 5}, {ChunkId: 6}})
	require.NoError(t, err)
	require.EqualValues(t, 5, ordered[0].ChunkId)
	require.EqualValues(t, 6, ordered[1].ChunkId)
	require.EqualValues(t, 7, ordered[2].ChunkId)
}
