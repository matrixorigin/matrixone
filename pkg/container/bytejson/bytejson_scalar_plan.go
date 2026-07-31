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

package bytejson

import (
	"encoding/binary"
	"math"
	"unicode/utf8"

	"github.com/matrixorigin/matrixone/pkg/common/moerr"
)

// StringDataEncoder writes a binary-JSON string from caller-owned bytes.
// The source must remain valid until EncodeDataInto returns.
type StringDataEncoder struct {
	value    []byte
	dataSize uint32
}

func NewStringDataEncoder(value []byte) (*StringDataEncoder, error) {
	if !utf8.Valid(value) {
		return nil, moerr.NewInvalidArgNoCtx("JSON string", "invalid UTF-8")
	}
	var lengthBuffer [binary.MaxVarintLen64]byte
	lengthSize := binary.PutUvarint(lengthBuffer[:], uint64(len(value)))
	total := uint64(lengthSize) + uint64(len(value))
	if total > math.MaxUint32 {
		return nil, moerr.NewInvalidArgNoCtx("JSON string", "value is too large")
	}
	return &StringDataEncoder{
		value:    value,
		dataSize: uint32(total),
	}, nil
}

func (e *StringDataEncoder) TypeCode() TpCode {
	return TpCodeString
}

func (e *StringDataEncoder) DataSize() uint32 {
	if e == nil {
		return 0
	}
	return e.dataSize
}

func (e *StringDataEncoder) EncodeDataInto(dst []byte) (int, error) {
	if e == nil || uint64(len(dst)) != uint64(e.dataSize) {
		return 0, moerr.NewInvalidArgNoCtx("JSON string", "result size mismatch")
	}
	written := binary.PutUvarint(dst, uint64(len(e.value)))
	written += copy(dst[written:], e.value)
	return written, nil
}
