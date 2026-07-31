// Copyright 2026 Matrix Origin
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package bytejson

import (
	"bytes"
	"encoding/base64"
	"encoding/binary"
	"math"
	"slices"

	"github.com/matrixorigin/matrixone/pkg/common/moerr"
)

type fixedDataEncoder struct {
	typeCode TpCode
	data     [numberSize]byte
	size     uint32
}

func NewLiteralDataEncoder(literal byte) ByteJsonDataEncoder {
	return &fixedDataEncoder{typeCode: TpCodeLiteral, data: [numberSize]byte{literal}, size: 1}
}

func NewInt64DataEncoder(value int64) ByteJsonDataEncoder {
	encoder := &fixedDataEncoder{typeCode: TpCodeInt64, size: numberSize}
	endian.PutUint64(encoder.data[:], uint64(value))
	return encoder
}

func NewUint64DataEncoder(value uint64) ByteJsonDataEncoder {
	encoder := &fixedDataEncoder{typeCode: TpCodeUint64, size: numberSize}
	endian.PutUint64(encoder.data[:], value)
	return encoder
}

func NewFloat64DataEncoder(value float64) ByteJsonDataEncoder {
	encoder := &fixedDataEncoder{typeCode: TpCodeFloat64, size: numberSize}
	endian.PutUint64(encoder.data[:], math.Float64bits(value))
	return encoder
}

func (e *fixedDataEncoder) TypeCode() TpCode { return e.typeCode }
func (e *fixedDataEncoder) DataSize() uint32 { return e.size }
func (e *fixedDataEncoder) EncodeDataInto(dst []byte) (int, error) {
	if e == nil || len(dst) != int(e.size) {
		return 0, moerr.NewInvalidArgNoCtx("JSON scalar", "result size mismatch")
	}
	return copy(dst, e.data[:e.size]), nil
}

type rawDataEncoder struct {
	value ByteJson
}

// NewRawDataEncoder references an already storage-compatible value.
func NewRawDataEncoder(value ByteJson) (ByteJsonDataEncoder, error) {
	if value.requiresLegacyBinaryEncoding() {
		return nil, moerr.NewInvalidArgNoCtx(
			"JSON value",
			"value is not storage compatible",
		)
	}
	if uint64(len(value.Data)) > math.MaxUint32 {
		return nil, moerr.NewInvalidArgNoCtx("JSON value", "value is too large")
	}
	return &rawDataEncoder{value: value}, nil
}

func (e *rawDataEncoder) TypeCode() TpCode { return e.value.Type }
func (e *rawDataEncoder) DataSize() uint32 { return uint32(len(e.value.Data)) }
func (e *rawDataEncoder) EncodeDataInto(dst []byte) (int, error) {
	if e == nil || len(dst) != len(e.value.Data) {
		return 0, moerr.NewInvalidArgNoCtx("JSON value", "result size mismatch")
	}
	return copy(dst, e.value.Data), nil
}

type typedStringDataEncoder struct {
	typeCode TpCode
	value    []byte
	dataSize uint32
}

func NewTypedStringDataEncoder(tp TpCode, value []byte) (ByteJsonDataEncoder, error) {
	switch tp {
	case TpCodeString, TpCodeDecimal, TpCodeDate, TpCodeTime, TpCodeDatetime, TpCodeBlob:
	default:
		return nil, moerr.NewInvalidArgNoCtx("JSON string", "invalid type code")
	}
	dataSize, err := binaryStringDataSize(len(value))
	if err != nil {
		return nil, err
	}
	return &typedStringDataEncoder{typeCode: tp, value: value, dataSize: dataSize}, nil
}

func (e *typedStringDataEncoder) TypeCode() TpCode { return e.typeCode }
func (e *typedStringDataEncoder) DataSize() uint32 { return e.dataSize }
func (e *typedStringDataEncoder) EncodeDataInto(dst []byte) (int, error) {
	if e == nil || len(dst) != int(e.dataSize) {
		return 0, moerr.NewInvalidArgNoCtx("JSON string", "result size mismatch")
	}
	written := binary.PutUvarint(dst, uint64(len(e.value)))
	written += copy(dst[written:], e.value)
	return written, nil
}

type binaryDataEncoder struct {
	value    []byte
	prefix   string
	dataSize uint32
}

func NewOpaqueDataEncoder(value []byte) (ByteJsonDataEncoder, error) {
	return newBinaryDataEncoder(value, "")
}

func NewBitDataEncoder(value []byte) (ByteJsonDataEncoder, error) {
	return newBinaryDataEncoder(value, persistedBitPrefix)
}

func newBinaryDataEncoder(value []byte, prefix string) (ByteJsonDataEncoder, error) {
	encodedLength := uint64(base64.StdEncoding.EncodedLen(len(value))) + uint64(len(prefix))
	if encodedLength > math.MaxInt {
		return nil, moerr.NewInvalidArgNoCtx("JSON binary", "value is too large")
	}
	dataSize, err := binaryStringDataSize(int(encodedLength))
	if err != nil {
		return nil, err
	}
	return &binaryDataEncoder{value: value, prefix: prefix, dataSize: dataSize}, nil
}

func (e *binaryDataEncoder) TypeCode() TpCode { return TpCodeBlob }
func (e *binaryDataEncoder) DataSize() uint32 { return e.dataSize }
func (e *binaryDataEncoder) EncodeDataInto(dst []byte) (int, error) {
	if e == nil || len(dst) != int(e.dataSize) {
		return 0, moerr.NewInvalidArgNoCtx("JSON binary", "result size mismatch")
	}
	encodedLength := base64.StdEncoding.EncodedLen(len(e.value)) + len(e.prefix)
	written := binary.PutUvarint(dst, uint64(encodedLength))
	written += copy(dst[written:], e.prefix)
	base64.StdEncoding.Encode(dst[written:], e.value)
	return written + base64.StdEncoding.EncodedLen(len(e.value)), nil
}

func binaryStringDataSize(length int) (uint32, error) {
	if length < 0 {
		return 0, moerr.NewInvalidArgNoCtx("JSON string", "invalid length")
	}
	var lengthBuffer [binary.MaxVarintLen64]byte
	lengthSize := binary.PutUvarint(lengthBuffer[:], uint64(length))
	total := uint64(lengthSize) + uint64(length)
	if total > math.MaxUint32 {
		return 0, moerr.NewInvalidArgNoCtx("JSON string", "value is too large")
	}
	return uint32(total), nil
}

type ArrayDataEncoder struct {
	values   []ByteJsonDataEncoder
	dataSize uint32
}

func NewArrayDataEncoder(values []ByteJsonDataEncoder) (*ArrayDataEncoder, error) {
	total := uint64(headerSize) + uint64(len(values))*valEntrySize
	for _, value := range values {
		if value == nil {
			return nil, moerr.NewInvalidArgNoCtx("JSON array", "nil value encoder")
		}
		if value.TypeCode() != TpCodeLiteral {
			total += uint64(value.DataSize())
		}
		if total > math.MaxUint32 {
			return nil, moerr.NewInvalidArgNoCtx("JSON array", "result is too large")
		}
	}
	return &ArrayDataEncoder{values: values, dataSize: uint32(total)}, nil
}

func (e *ArrayDataEncoder) TypeCode() TpCode { return TpCodeArray }
func (e *ArrayDataEncoder) DataSize() uint32 { return e.dataSize }
func (e *ArrayDataEncoder) EncodeDataInto(dst []byte) (int, error) {
	if e == nil || len(dst) != int(e.dataSize) {
		return 0, moerr.NewInvalidArgNoCtx("JSON array", "result size mismatch")
	}
	headerEnd := headerSize + len(e.values)*valEntrySize
	clear(dst[:headerEnd])
	endian.PutUint32(dst, uint32(len(e.values)))
	endian.PutUint32(dst[docSizeOff:], e.dataSize)
	payloadOffset := headerEnd
	for idx, value := range e.values {
		entryOffset := headerSize + idx*valEntrySize
		dst[entryOffset] = byte(value.TypeCode())
		if value.TypeCode() == TpCodeLiteral {
			if _, err := value.EncodeDataInto(dst[entryOffset+valTypeSize : entryOffset+valTypeSize+1]); err != nil {
				return 0, err
			}
			continue
		}
		endian.PutUint32(dst[entryOffset+valTypeSize:], uint32(payloadOffset))
		size := int(value.DataSize())
		written, err := value.EncodeDataInto(dst[payloadOffset : payloadOffset+size])
		if err != nil {
			return 0, err
		}
		if written != size {
			return 0, moerr.NewInvalidArgNoCtx("JSON array", "value size mismatch")
		}
		payloadOffset += size
	}
	return payloadOffset, nil
}

type IndexedFloatArrayDataEncoder struct {
	count    int
	valueAt  func(int) float64
	dataSize uint32
}

func NewIndexedFloatArrayDataEncoder(
	count int,
	valueAt func(int) float64,
) (*IndexedFloatArrayDataEncoder, error) {
	if count < 0 || valueAt == nil {
		return nil, moerr.NewInvalidArgNoCtx("JSON array", "invalid value accessor")
	}
	total := uint64(headerSize) + uint64(count)*(valEntrySize+numberSize)
	if total > math.MaxUint32 {
		return nil, moerr.NewInvalidArgNoCtx("JSON array", "result is too large")
	}
	return &IndexedFloatArrayDataEncoder{
		count: count, valueAt: valueAt, dataSize: uint32(total),
	}, nil
}

func (e *IndexedFloatArrayDataEncoder) TypeCode() TpCode { return TpCodeArray }
func (e *IndexedFloatArrayDataEncoder) DataSize() uint32 { return e.dataSize }
func (e *IndexedFloatArrayDataEncoder) EncodeDataInto(dst []byte) (int, error) {
	if e == nil || len(dst) != int(e.dataSize) {
		return 0, moerr.NewInvalidArgNoCtx("JSON array", "result size mismatch")
	}
	headerEnd := headerSize + e.count*valEntrySize
	clear(dst[:headerEnd])
	endian.PutUint32(dst, uint32(e.count))
	endian.PutUint32(dst[docSizeOff:], e.dataSize)
	payloadOffset := headerEnd
	for idx := 0; idx < e.count; idx++ {
		entryOffset := headerSize + idx*valEntrySize
		dst[entryOffset] = byte(TpCodeFloat64)
		endian.PutUint32(dst[entryOffset+valTypeSize:], uint32(payloadOffset))
		endian.PutUint64(dst[payloadOffset:], math.Float64bits(e.valueAt(idx)))
		payloadOffset += numberSize
	}
	return payloadOffset, nil
}

type ObjectDataEncoderEntry struct {
	Key   []byte
	Value ByteJsonDataEncoder
	order int
}

type ObjectDataEncoder struct {
	entries  []ObjectDataEncoderEntry
	dataSize uint32
}

func NewObjectDataEncoder(entries []ObjectDataEncoderEntry) (*ObjectDataEncoder, error) {
	for idx := range entries {
		entries[idx].order = idx
		if entries[idx].Value == nil || len(entries[idx].Key) > math.MaxUint16 {
			return nil, moerr.NewInvalidArgNoCtx("JSON object", "invalid entry")
		}
	}
	slices.SortFunc(entries, func(left, right ObjectDataEncoderEntry) int {
		if order := bytes.Compare(left.Key, right.Key); order != 0 {
			return order
		}
		return left.order - right.order
	})
	unique := entries[:0]
	for _, entry := range entries {
		if len(unique) > 0 && bytes.Equal(unique[len(unique)-1].Key, entry.Key) {
			unique[len(unique)-1] = entry
		} else {
			unique = append(unique, entry)
		}
	}
	entries = unique
	total := uint64(headerSize) + uint64(len(entries))*(keyEntrySize+valEntrySize)
	for _, entry := range entries {
		total += uint64(len(entry.Key))
		if entry.Value.TypeCode() != TpCodeLiteral {
			total += uint64(entry.Value.DataSize())
		}
		if total > math.MaxUint32 {
			return nil, moerr.NewInvalidArgNoCtx("JSON object", "result is too large")
		}
	}
	return &ObjectDataEncoder{entries: entries, dataSize: uint32(total)}, nil
}

func (e *ObjectDataEncoder) TypeCode() TpCode { return TpCodeObject }
func (e *ObjectDataEncoder) DataSize() uint32 { return e.dataSize }
func (e *ObjectDataEncoder) EncodeDataInto(dst []byte) (int, error) {
	if e == nil || len(dst) != int(e.dataSize) {
		return 0, moerr.NewInvalidArgNoCtx("JSON object", "result size mismatch")
	}
	count := len(e.entries)
	keyEntryBegin := headerSize
	valueEntryBegin := keyEntryBegin + count*keyEntrySize
	payloadOffset := valueEntryBegin + count*valEntrySize
	clear(dst[:payloadOffset])
	endian.PutUint32(dst, uint32(count))
	endian.PutUint32(dst[docSizeOff:], e.dataSize)
	for idx, entry := range e.entries {
		entryOffset := keyEntryBegin + idx*keyEntrySize
		endian.PutUint32(dst[entryOffset:], uint32(payloadOffset))
		endian.PutUint16(dst[entryOffset+keyOriginOff:], uint16(len(entry.Key)))
		payloadOffset += copy(dst[payloadOffset:], entry.Key)
	}
	for idx, entry := range e.entries {
		value := entry.Value
		entryOffset := valueEntryBegin + idx*valEntrySize
		dst[entryOffset] = byte(value.TypeCode())
		if value.TypeCode() == TpCodeLiteral {
			if _, err := value.EncodeDataInto(dst[entryOffset+valTypeSize : entryOffset+valTypeSize+1]); err != nil {
				return 0, err
			}
			continue
		}
		endian.PutUint32(dst[entryOffset+valTypeSize:], uint32(payloadOffset))
		size := int(value.DataSize())
		written, err := value.EncodeDataInto(dst[payloadOffset : payloadOffset+size])
		if err != nil {
			return 0, err
		}
		if written != size {
			return 0, moerr.NewInvalidArgNoCtx("JSON object", "value size mismatch")
		}
		payloadOffset += size
	}
	return payloadOffset, nil
}
