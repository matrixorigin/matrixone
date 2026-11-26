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
//
// bat must be valid, as an engine's batches are: Encode sends each varlen
// area as is, without the codec's linear check of its descriptors.
func Encode(bat *Batch) (api.EncodedBatch, error) {
	var w partsWriter
	if err := bat.marshalBinaryTo(&w, true); err != nil {
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
	parts [][]byte
	// owned is where the owned bytes not yet in a part begin.
	owned int
}

// reserve sizes the owned buffer and the part list for bat, so the encoding
// allocates each once.
func (w *partsWriter) reserve(bat *Batch, plans []vector.MarshalBinaryPlan) {
	copied := 8 + 4 + 4 + 4 + 4
	for _, plan := range plans {
		copied += 4 + plan.CopiedSize(minBorrowedPart)
	}
	if len(bat.extraBuf) < minBorrowedPart {
		copied += len(bat.extraBuf)
	}
	w.buf = make([]byte, 0, copied)
	// Each vector borrows at most its data and area, each followed by owned
	// bytes; the extra buffer adds one more pair.
	w.parts = make([][]byte, 0, 4*len(plans)+3)
}

func (w *partsWriter) WriteBorrowed(data []byte) error {
	if len(data) < minBorrowedPart {
		_, err := w.Write(data)
		return err
	}
	w.closeOwned()
	w.parts = append(w.parts, data)
	return nil
}

// closeOwned ends the current owned part. The part stays valid if the owned
// buffer grows later: its bytes are final and stay in the old array.
func (w *partsWriter) closeOwned() {
	if end := len(w.buf); end > w.owned {
		w.parts = append(w.parts, w.buf[w.owned:end:end])
		w.owned = end
	}
}

func (w *partsWriter) finish() [][]byte {
	w.closeOwned()
	return w.parts
}

// ViewBinary decodes data, a MarshalBinary encoding, into a new batch whose
// vectors and extra buffer are read-only views of data. Every vector must
// hold the batch's row count.
func ViewBinary(data []byte) (*Batch, error) {
	return viewBinary(data, true)
}

// ViewTrustedBinary is ViewBinary for data that Encode produced from a valid
// batch, such as an api.Entry from a peer of the same cluster. It checks the
// framing, sizes and row counts, but not the null bitmaps or varlen payloads.
func ViewTrustedBinary(data []byte) (*Batch, error) {
	return viewBinary(data, false)
}

func viewBinary(data []byte, validateValues bool) (*Batch, error) {
	bat := NewWithSize(0)
	// The vectors are views, so they can share one allocation.
	if len(data) >= 12 {
		if n := int(types.DecodeInt32(data[8:12])); n > 0 && n <= (len(data)-12)/4 {
			views := make([]vector.Vector, n)
			bat.Vecs = make([]*vector.Vector, n)
			for i := range views {
				bat.Vecs[i] = &views[i]
			}
		}
	}
	if err := bat.unmarshalBinary(data, nil, validateValues); err != nil {
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
