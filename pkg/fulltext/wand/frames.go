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

	"github.com/matrixorigin/matrixone/pkg/common/moerr"
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
		records, _, nInserts, nDeletes, _, uerr := cuvscdc.UnframeCdcChunk(f.Data)
		if uerr != nil {
			freeSegs(segs)
			return nil, nil, uerr
		}
		switch {
		case nInserts > 0:
			m, derr := Deserialize(fmt.Sprintf("tail-%d", f.ChunkId), bytes.NewReader(records))
			if derr != nil {
				freeSegs(segs)
				return nil, nil, derr
			}
			m.ChunkId = f.ChunkId
			segs = append(segs, m)
		case nDeletes > 0:
			recs, derr := DecodeDeleteLog(records)
			if derr != nil {
				freeSegs(segs)
				return nil, nil, derr
			}
			deletes = FoldDeleteFrame(deletes, recs, f.ChunkId)
		default:
			freeSegs(segs)
			return nil, nil, moerr.NewInternalErrorNoCtx("wand tail frame: empty (neither inserts nor deletes)")
		}
	}
	return segs, deletes, nil
}

// freeSegs releases the C-backed buffers of every segment (idempotent).
func freeSegs(segs []*WandModel) {
	for _, s := range segs {
		s.Free()
	}
}
