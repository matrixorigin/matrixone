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

package wand

import (
	"bytes"
	"fmt"
	"io"

	"github.com/matrixorigin/matrixone/pkg/common/moerr"
	"github.com/matrixorigin/matrixone/pkg/vectorindex"
	cuvscdc "github.com/matrixorigin/matrixone/pkg/vectorindex/cuvs"
)

// tag=1 CdcTail frames. WAND reuses cuVS's payload-agnostic FrameCdcChunk
// envelope (magic+version+op-counts+crc32); the payload is WAND-specific — a
// serialized WandModel for an insert segment, or an EncodeDeleteLog blob for a
// delete batch. The two are told apart by the frame's op counts (nInserts>0 is a
// segment, nDeletes>0 is a delete log), which are mutually exclusive for WAND
// frames. The frame's chunk_id — its append position in the single tag=1 CdcTail
// log — is assigned by the storage layer at load, never stored in the frame.
// See fulltext_wand.md "single CdcTail log, chunk_id-ordered".

// FrameSegment serializes m and wraps it as one insert-segment frame. nInserts is
// set to the segment's doc count so the idxcron tag=1-growth gate can sum delta
// docs from the frame header without deserializing.
func FrameSegment(m *WandModel) ([]byte, error) {
	blob, err := m.Serialize()
	if err != nil {
		return nil, err
	}
	return cuvscdc.FrameCdcChunk(blob, nil, uint32(m.N), 0, 0), nil
}

// FrameDeletes wraps one CDC delete batch (pks only) as a delete-log frame;
// nDeletes is the record count.
func FrameDeletes(pkType int32, recs []DeleteRecord) ([]byte, error) {
	blob, err := EncodeDeleteLog(pkType, recs)
	if err != nil {
		return nil, err
	}
	return cuvscdc.FrameCdcChunk(blob, nil, 0, uint32(len(recs)), 0), nil
}

// TailFrame is one tag=1 CdcTail entry: the framed bytes carried at its chunk_id.
type TailFrame struct {
	ChunkId int64
	Data    []byte
}

// AssembleFrames decodes tag=1 CdcTail frames — which MUST be pre-sorted by
// ChunkId ascending — into the ordered insert-segment list and the folded
// pk -> max-delete-chunk_id map, ready for ComputeLiveness + SearchSegmentsLive.
// Each returned segment's ChunkId is set to its frame's chunk_id (the recency
// key). The caller owns the returned segments and must Free() them; on any
// framing/decode error the partially-built segments are freed before returning.
func AssembleFrames(frames []TailFrame) (segs []*WandModel, deletes map[any]int64, err error) {
	for _, f := range frames {
		segs, deletes, err = applyTailFrame(f.Data, f.ChunkId, segs, deletes)
		if err != nil {
			freeSegs(segs)
			return nil, nil, err
		}
	}
	return segs, deletes, nil
}

// applyTailFrame decodes one framed tag=1 blob carried at chunkId and folds it into
// (segs, deletes): an insert frame → a deserialized segment (its ChunkId set to
// chunkId) appended to segs; a delete frame → folded into the pk→max-delete-chunk_id
// map. Shared by AssembleFrames (in-memory frames) and assembleFramesAt (streaming
// file). On error the caller owns freeing the partial segs.
func applyTailFrame(data []byte, chunkId int64, segs []*WandModel, deletes map[any]int64) ([]*WandModel, map[any]int64, error) {
	records, _, nInserts, nDeletes, _, uerr := cuvscdc.UnframeCdcChunk(data)
	if uerr != nil {
		return segs, deletes, uerr
	}
	switch {
	case nInserts > 0:
		m, derr := Deserialize(fmt.Sprintf("tail-%d", chunkId), bytes.NewReader(records))
		if derr != nil {
			return segs, deletes, derr
		}
		m.ChunkId = chunkId
		segs = append(segs, m)
	case nDeletes > 0:
		recs, derr := DecodeDeleteLog(records)
		if derr != nil {
			return segs, deletes, derr
		}
		deletes = FoldDeleteFrame(deletes, recs, chunkId)
	default:
		return segs, deletes, moerr.NewInternalErrorNoCtx("wand tail frame: empty (neither inserts nor deletes)")
	}
	return segs, deletes, nil
}

// assembleFramesAt walks the tag=1 frames from a chunk-placed source (each chunk at
// slot*MaxChunkSize, slot = chunk_id - minChunk; span slots) and decodes each frame
// straight into (segs, deletes) — the STREAMING assembler. It reads only one frame
// at a time (the header to learn the length, then the frame bytes, freed before the
// next), so the whole tail is never resident: peak transient is one frame, not the
// delta. r is the streaming loader's temp file (or a bytes.Reader in tests).
func assembleFramesAt(r io.ReaderAt, minChunk, span int64) (segs []*WandModel, deletes map[any]int64, err error) {
	hdr := make([]byte, cuvscdc.CdcHeaderSize)
	for slot := int64(0); slot < span; {
		off := slot * int64(vectorindex.MaxChunkSize)
		if _, e := r.ReadAt(hdr, off); e != nil {
			freeSegs(segs)
			return nil, nil, e
		}
		total, e := cuvscdc.CdcFrameLen(hdr)
		if e != nil {
			freeSegs(segs)
			return nil, nil, e
		}
		buf := make([]byte, total)
		if _, e := r.ReadAt(buf, off); e != nil {
			freeSegs(segs)
			return nil, nil, e
		}
		segs, deletes, e = applyTailFrame(buf, minChunk+slot, segs, deletes)
		buf = nil // release before the next frame
		if e != nil {
			freeSegs(segs)
			return nil, nil, e
		}
		slot += int64((total + vectorindex.MaxChunkSize - 1) / vectorindex.MaxChunkSize)
	}
	return segs, deletes, nil
}

// freeSegs releases the C-backed buffers of every segment (idempotent).
func freeSegs(segs []*WandModel) {
	for _, s := range segs {
		s.Free()
	}
}

// TailChunk is one raw tag=1 CdcTail storage row (one MaxChunkSize-bounded piece
// of a frame). A frame larger than the store's data column is split across
// several consecutive chunks; the load path reassembles them.
type TailChunk struct {
	ChunkId int64
	Data    []byte
}

// splitFrameChunks splits a complete frame into MaxChunkSize-bounded storage
// chunks at consecutive chunk_ids from startChunkId. A frame <= MaxChunkSize
// yields a single chunk. (Frames are never empty — a valid frame is >= the
// 44-byte overhead.)
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
// sort: it places each at index (chunk_id - min) in a preallocated slice (O(n)) —
// the same position-not-sort approach the tag=0 loader uses (streamChunksToFile
// WriteAt by offset). This lets loadTailFrames drop `ORDER BY chunk_id`, which
// would force a SQL Sort (full materialization / possible spill) on the load path.
//
// tag=1 chunk_ids are a GAPLESS run — the writer appends consecutive ids and
// compaction deletes a whole low prefix (never a hole) — so [min..max] must span
// exactly len(chunks) ids; a span mismatch means a missing or duplicate chunk
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
			"wand tail: chunk_id range [%d..%d] spans %d but got %d rows (gap or duplicate)",
			minC, maxC, span, len(chunks)))
	}
	ordered := make([]TailChunk, span)
	for _, c := range chunks {
		ordered[c.ChunkId-minC] = c
	}
	return ordered, nil
}

// reassembleFrames groups chunk_id-ordered storage rows back into complete
// frames using each frame's self-describing header length (cuVS CdcFrameLen): a
// frame occupies consecutive chunks whose bytes sum to that length, and its
// ordering key is the first chunk's chunk_id. Chunks MUST be pre-sorted by
// ChunkId ascending.
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
			return nil, moerr.NewInternalErrorNoCtx("wand tail: truncated frame (missing chunk rows)")
		}
		frames = append(frames, TailFrame{ChunkId: firstChunkId, Data: buf[:total]})
	}
	return frames, nil
}
