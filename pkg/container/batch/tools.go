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
)

// MarshalVectors encodes vecs as one batch whose row count is the length of
// the first vector. Only the vectors are encoded: no extra buffer, and a
// zero Recursive and ShuffleIDX.
func MarshalVectors(vecs []*vector.Vector) ([]byte, error) {
	bat := Batch{Vecs: vecs}
	if len(vecs) > 0 {
		bat.rowCount = vecs[0].Length()
	}
	return bat.MarshalBinary()
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
