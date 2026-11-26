// Copyright 2021 Matrix Origin
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
	"io"

	"github.com/matrixorigin/matrixone/pkg/common/moerr"
	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/container/vector"
	"github.com/matrixorigin/matrixone/pkg/pb/api"
)

// MarshalVectors encodes vecs into a new slice, like EncodeVectors but
// copying the vectors' buffers.
func MarshalVectors(vecs []*vector.Vector) ([]byte, error) {
	bat := Batch{Vecs: vecs}
	if len(vecs) > 0 {
		bat.rowCount = vecs[0].Length()
	}
	return bat.MarshalBinary()
}

// EncodeVectors encodes vecs as one batch whose row count is the length of
// the first vector. Only the vectors are encoded: no extra buffer, and a
// zero Recursive and ShuffleIDX. The result borrows the vectors' buffers;
// see Encode.
func EncodeVectors(vecs []*vector.Vector) (api.EncodedBatch, error) {
	bat := Batch{Vecs: vecs}
	if len(vecs) > 0 {
		bat.rowCount = vecs[0].Length()
	}
	return Encode(&bat)
}

// Encode returns bat's MarshalBinary encoding as an api.EncodedBatch that
// borrows its vectors' data and area buffers and its extra buffer: only the
// headers and null bitmaps are copied. bat must stay unchanged until the
// result is marshaled or dropped.
func Encode(bat *Batch) (api.EncodedBatch, error) {
	var w partsWriter
	if err := bat.MarshalBinaryTo(&w); err != nil {
		return api.EncodedBatch{}, err
	}
	return api.NewEncodedBatch(w.finish()...), nil
}

// minBorrowedPart is the smallest buffer partsWriter keeps by reference;
// copying a smaller one costs less than another part.
const minBorrowedPart = 256

// partsWriter collects an encoding as parts. It copies ordinary writes into
// its own buffer and keeps borrowed buffers by reference.
type partsWriter struct {
	binaryAppender
	segments []partSegment
	// owned is where the owned bytes not yet in a segment begin.
	owned int
}

// partSegment is a borrowed buffer, or owned bytes [start, end) when
// borrowed is nil.
type partSegment struct {
	borrowed   []byte
	start, end int
}

// EnsureCapacity ignores the encoding's total size: most of it is
// borrowed, not copied.
func (w *partsWriter) EnsureCapacity(int) error { return nil }

func (w *partsWriter) WriteBorrowed(data []byte) error {
	if len(data) < minBorrowedPart {
		_, err := w.Write(data)
		return err
	}
	w.closeOwned()
	w.segments = append(w.segments, partSegment{borrowed: data})
	return nil
}

func (w *partsWriter) closeOwned() {
	if end := len(w.buf); end > w.owned {
		w.segments = append(w.segments, partSegment{start: w.owned, end: end})
		w.owned = end
	}
}

// finish returns the parts in order. Owned segments are resolved only
// now, after the owned buffer has stopped growing.
func (w *partsWriter) finish() [][]byte {
	w.closeOwned()
	parts := make([][]byte, len(w.segments))
	for i, segment := range w.segments {
		if segment.borrowed != nil {
			parts[i] = segment.borrowed
		} else {
			parts[i] = w.buf[segment.start:segment.end:segment.end]
		}
	}
	return parts
}

// ViewBinary decodes data, a MarshalBinary encoding, into a new batch whose
// vectors and extra buffer are read-only views of data. Every vector must
// hold the batch's row count.
func ViewBinary(data []byte) (*Batch, error) {
	bat := NewWithSize(0)
	if err := bat.UnmarshalBinary(data); err != nil {
		return nil, err
	}
	if err := bat.CheckLength(); err != nil {
		return nil, moerr.NewInvalidInputNoCtx("batch vector length does not match row count")
	}
	return bat, nil
}

// EncodedRowCount returns the row count of a MarshalBinary encoding without
// decoding its vectors.
func EncodedRowCount(data []byte) (int, error) {
	if len(data) < 8 {
		return 0, io.ErrUnexpectedEOF
	}
	rows := types.DecodeInt64(data[:8])
	if rows < 0 || int64(int(rows)) != rows {
		return 0, moerr.NewInvalidInputNoCtx("invalid batch row count")
	}
	return int(rows), nil
}
