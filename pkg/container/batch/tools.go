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
// zero Recursive and ShuffleIDX. It allocates the batch header the result
// borrows; see Encode.
func EncodeVectors(vecs []*vector.Vector) (api.EncodedBatch, error) {
	bat := &Batch{Vecs: vecs}
	if len(vecs) > 0 {
		bat.rowCount = vecs[0].Length()
	}
	return Encode(bat)
}

// Encode returns bat's MarshalBinary encoding as an api.EncodedBatch that
// borrows bat and writes it straight into the message buffer when the Entry
// is marshaled. It allocates nothing and copies nothing. bat, its vectors
// and their buffers must stay unchanged until the result is marshaled or
// dropped; marshaling fails if bat no longer has the size it had here.
//
// bat must be valid, as an engine's batches are: Encode sends each varlen
// area as is, without the codec's linear check of its descriptors.
func Encode(bat *Batch) (api.EncodedBatch, error) {
	size, err := bat.marshalTrustedSize()
	if err != nil {
		return api.EncodedBatch{}, err
	}
	return api.NewBorrowedEncodedBatch((*trustedEncoder)(bat), size), nil
}

// trustedEncoder is the api.BatchEncoder of a valid batch. Being a pointer
// type, it is stored in the interface without an allocation.
type trustedEncoder Batch

func (e *trustedEncoder) EncodeTo(dst []byte) error {
	return (*Batch)(e).marshalTrustedTo(dst)
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

// ViewTrustedBinaryInto is ViewTrustedBinary decoding into bat, which must
// be a view from an earlier decode or a new batch. It reuses bat's vectors,
// so decoding an encoding with as many vectors as the last allocates
// nothing. The previous views are replaced.
func ViewTrustedBinaryInto(bat *Batch, data []byte) error {
	if bat.mp != nil || bat.selection != nil {
		return moerr.NewInvalidInputNoCtx("batch is not a view")
	}
	if err := bat.unmarshalBinary(data, nil, false); err != nil {
		return err
	}
	if err := bat.CheckLength(); err != nil {
		return moerr.NewInvalidInputNoCtx("batch vector length does not match row count")
	}
	return nil
}

// DropViews drops the references of bat, a batch from ViewTrustedBinaryInto,
// to its encoding, and keeps its vectors for the next decode.
func DropViews(bat *Batch) {
	for _, vec := range bat.Vecs {
		vec.Free(nil)
	}
	bat.extraBuf = nil
	bat.rowCount = 0
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
