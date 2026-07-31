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

	"github.com/matrixorigin/matrixone/pkg/common/moerr"
)

// ObjectKeysArrayEncoder exposes an object's sorted keys as a JSON array
// without materializing []any or a second binary-JSON payload.
type ObjectKeysArrayEncoder struct {
	object   ByteJson
	dataSize uint32
}

func NewObjectKeysArrayEncoder(object ByteJson) (*ObjectKeysArrayEncoder, error) {
	if object.Type != TpCodeObject {
		return nil, moerr.NewInvalidArgNoCtx("json_keys", "JSON value is not an object")
	}
	count := object.GetElemCnt()
	total := uint64(headerSize) + uint64(count)*uint64(valEntrySize)
	var lengthBuffer [binary.MaxVarintLen64]byte
	for idx := 0; idx < count; idx++ {
		key := object.GetObjectKey(idx)
		lengthSize := binary.PutUvarint(lengthBuffer[:], uint64(len(key)))
		total += uint64(lengthSize) + uint64(len(key))
		if total > math.MaxUint32 {
			return nil, moerr.NewInvalidArgNoCtx("json_keys", "JSON result is too large")
		}
	}
	return &ObjectKeysArrayEncoder{
		object:   object,
		dataSize: uint32(total),
	}, nil
}

func (e *ObjectKeysArrayEncoder) TypeCode() TpCode {
	return TpCodeArray
}

func (e *ObjectKeysArrayEncoder) DataSize() uint32 {
	if e == nil {
		return 0
	}
	return e.dataSize
}

func (e *ObjectKeysArrayEncoder) EncodeDataInto(dst []byte) (int, error) {
	if e == nil || uint64(len(dst)) != uint64(e.dataSize) {
		return 0, moerr.NewInvalidArgNoCtx("json_keys", "JSON result size mismatch")
	}
	count := e.object.GetElemCnt()
	headerEnd := headerSize + count*valEntrySize
	clear(dst[:headerEnd])
	endian.PutUint32(dst, uint32(count))
	endian.PutUint32(dst[docSizeOff:], e.dataSize)
	payloadOffset := headerEnd
	for idx := 0; idx < count; idx++ {
		entryOffset := headerSize + idx*valEntrySize
		dst[entryOffset] = byte(TpCodeString)
		endian.PutUint32(dst[entryOffset+valTypeSize:], uint32(payloadOffset))
		key := e.object.GetObjectKey(idx)
		payloadOffset += binary.PutUvarint(dst[payloadOffset:], uint64(len(key)))
		payloadOffset += copy(dst[payloadOffset:], key)
	}
	if payloadOffset != len(dst) {
		return 0, moerr.NewInvalidArgNoCtx("json_keys", "JSON result size mismatch")
	}
	return payloadOffset, nil
}
