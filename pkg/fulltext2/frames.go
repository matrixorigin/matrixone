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
	"fmt"

	"github.com/matrixorigin/matrixone/pkg/common/moerr"
	"github.com/matrixorigin/matrixone/pkg/vectorindex"
	cuvscdc "github.com/matrixorigin/matrixone/pkg/vectorindex/cuvs"
)

// CDC framing for the tag=1 tail. Fulltext v2 reuses cuVS's payload-agnostic
// FrameCdcChunk envelope (magic + version + op-counts + crc32) — exactly what
// bm25 does — carrying a serialized positional Segment as the insert payload. A
// frame larger than the storage data column is split across consecutive chunks
// at ascending chunk_ids; the load path reassembles them by each frame's
// self-describing header length. The chunk_id is the recency key, assigned by the
// storage layer, never stored inside the frame.

// FrameSegment serializes seg and wraps it as one insert-segment frame. nInserts
// carries the doc count so a header-only scan can sum delta docs without decoding.
func FrameSegment(seg *Segment) ([]byte, error) {
	blob, err := seg.Serialize()
	if err != nil {
		return nil, err
	}
	return cuvscdc.FrameCdcChunk(blob, nil, uint32(seg.N), 0, 0), nil
}

// UnframeSegment decodes an insert-segment frame back into a loaded Segment
// (id set to id).
func UnframeSegment(id string, framed []byte) (*Segment, error) {
	records, _, nInserts, _, _, err := cuvscdc.UnframeCdcChunk(framed)
	if err != nil {
		return nil, err
	}
	if nInserts == 0 {
		return nil, moerr.NewInternalErrorNoCtx("fulltext2: frame carries no insert segment")
	}
	return Deserialize(id, bytes.NewReader(records))
}

// TailChunk is one raw tag=1 storage row: a MaxChunkSize-bounded piece of a frame
// at its chunk_id.
type TailChunk struct {
	ChunkId int64
	Data    []byte
}

// TailFrame is a reassembled complete frame plus the chunk_id of its first chunk
// (its recency key).
type TailFrame struct {
	ChunkId int64
	Data    []byte
}

// splitFrameChunks splits a complete frame into MaxChunkSize-bounded chunks at
// consecutive chunk_ids from startChunkId. A frame <= MaxChunkSize yields a single
// chunk.
func splitFrameChunks(startChunkId int64, framed []byte) []TailChunk {
	out := make([]TailChunk, 0, (len(framed)+vectorindex.MaxChunkSize-1)/vectorindex.MaxChunkSize)
	cid := startChunkId
	for off := 0; off < len(framed); off += vectorindex.MaxChunkSize {
		end := off + vectorindex.MaxChunkSize
		if end > len(framed) {
			end = len(framed)
		}
		out = append(out, TailChunk{ChunkId: cid, Data: framed[off:end]})
		cid++
	}
	return out
}

// orderTailChunks returns the chunks ordered by chunk_id WITHOUT a comparison
// sort: it places each at index (chunk_id - min) in a preallocated slice (O(n)).
// tag=1 chunk_ids are a gapless run (the writer appends consecutive ids, and
// compaction deletes a whole low prefix — never a hole), so [min..max] must span
// exactly len(chunks) ids; a mismatch means a missing or duplicate chunk
// (corruption), reported rather than silently mis-assembled.
func orderTailChunks(chunks []TailChunk) ([]TailChunk, error) {
	if len(chunks) == 0 {
		return nil, nil
	}
	minC, maxC := chunks[0].ChunkId, chunks[0].ChunkId
	for _, c := range chunks[1:] {
		if c.ChunkId < minC {
			minC = c.ChunkId
		}
		if c.ChunkId > maxC {
			maxC = c.ChunkId
		}
	}
	span := maxC - minC + 1
	if span != int64(len(chunks)) {
		return nil, moerr.NewInternalErrorNoCtx(fmt.Sprintf(
			"fulltext2 tail: chunk_id range [%d..%d] spans %d but got %d rows (gap or duplicate)",
			minC, maxC, span, len(chunks)))
	}
	ordered := make([]TailChunk, span)
	for _, c := range chunks {
		ordered[c.ChunkId-minC] = c
	}
	return ordered, nil
}

// reassembleFrames groups chunk_id-ordered rows back into complete frames using
// each frame's self-describing header length (CdcFrameLen): a frame occupies
// consecutive chunks whose bytes sum to that length, and its ordering key is the
// first chunk's chunk_id. Chunks MUST be pre-sorted by chunk_id ascending.
func reassembleFrames(chunks []TailChunk) ([]TailFrame, error) {
	var frames []TailFrame
	i := 0
	for i < len(chunks) {
		total, err := cuvscdc.CdcFrameLen(chunks[i].Data)
		if err != nil {
			return nil, err
		}
		firstChunkId := chunks[i].ChunkId
		buf := make([]byte, 0, total)
		for len(buf) < total && i < len(chunks) {
			buf = append(buf, chunks[i].Data...)
			i++
		}
		if len(buf) < total {
			return nil, moerr.NewInternalErrorNoCtx("fulltext2 tail: truncated frame (missing chunk rows)")
		}
		frames = append(frames, TailFrame{ChunkId: firstChunkId, Data: buf[:total]})
	}
	return frames, nil
}
