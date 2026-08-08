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

package lifecycle

import (
	"context"
	"crypto/sha256"
	"encoding/binary"
	"hash"
	"math"

	"github.com/matrixorigin/matrixone/pkg/common/moerr"
	"github.com/matrixorigin/matrixone/pkg/container/batch"
	"github.com/matrixorigin/matrixone/pkg/container/nulls"
	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/container/vector"
)

const canonicalEncoderVersion uint16 = 1

const (
	canonicalDatasetBegin byte = 0xd1
	canonicalRowBegin     byte = 0xf0
	canonicalRowEnd       byte = 0xf1
	canonicalColumnBegin  byte = 0xc0
)

type CanonicalCell struct {
	Type  types.Type
	Value any
	Null  bool
}

type CanonicalValueEncoder struct {
	hash         hash.Hash
	rowCount     uint64
	logicalBytes uint64
}

type CanonicalBatchEncoder struct {
	*CanonicalValueEncoder
}

func NewCanonicalValueEncoder(schemaDigest [32]byte) *CanonicalValueEncoder {
	encoder := &CanonicalValueEncoder{hash: sha256.New()}
	encoder.writeByte(canonicalDatasetBegin)
	encoder.writeUint16(canonicalEncoderVersion)
	encoder.writeBytes(schemaDigest[:])
	return encoder
}

func NewCanonicalBatchEncoder(schemaDigest [32]byte) *CanonicalBatchEncoder {
	return &CanonicalBatchEncoder{
		CanonicalValueEncoder: NewCanonicalValueEncoder(schemaDigest),
	}
}

func (encoder *CanonicalValueEncoder) WriteRow(
	ctx context.Context,
	cells []CanonicalCell,
) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	for _, cell := range cells {
		if !isCanonicalTypeSupported(cell.Type.Oid) {
			return moerr.NewNotSupportedf(ctx, "Lifecycle canonical type %s", cell.Type.Oid)
		}
	}

	encoder.writeByte(canonicalRowBegin)
	encoder.writeUint32(uint32(len(cells)))
	for ordinal, cell := range cells {
		encoder.writeByte(canonicalColumnBegin)
		encoder.writeUint32(uint32(ordinal))
		encoder.writeUint16(uint16(cell.Type.Oid))
		encoder.writeUint32(uint32(cell.Type.Width))
		encoder.writeUint32(uint32(cell.Type.Scale))
		if cell.Null {
			encoder.writeByte(0)
			encoder.writeUint64(0)
			continue
		}
		value, err := encodeCanonicalValue(cell.Type, cell.Value)
		if err != nil {
			return moerr.NewInvalidInputf(
				ctx,
				"cannot encode Lifecycle column %d (%s): %v",
				ordinal,
				cell.Type.Oid,
				err,
			)
		}
		encoder.writeByte(1)
		encoder.writeUint64(uint64(len(value)))
		encoder.writeBytes(value)
	}
	encoder.writeByte(canonicalRowEnd)
	encoder.rowCount++
	return nil
}

// WriteBatch writes rows in their existing physical order. If selected is not
// nil, only rows whose bit is set are encoded; callers use this for E rows.
func (encoder *CanonicalBatchEncoder) WriteBatch(
	ctx context.Context,
	value *batch.Batch,
	selected *nulls.Nulls,
) error {
	if value == nil {
		return nil
	}
	for _, vec := range value.Vecs {
		if vec == nil {
			return moerr.NewInvalidInput(ctx, "Lifecycle batch contains a nil vector")
		}
		if !isCanonicalTypeSupported(vec.GetType().Oid) {
			return moerr.NewNotSupportedf(
				ctx,
				"Lifecycle canonical type %s",
				vec.GetType().Oid,
			)
		}
	}
	for row := 0; row < value.RowCount(); row++ {
		if err := ctx.Err(); err != nil {
			return err
		}
		if selected != nil && !selected.Contains(uint64(row)) {
			continue
		}
		cells := make([]CanonicalCell, len(value.Vecs))
		for column, vec := range value.Vecs {
			cell := CanonicalCell{
				Type: *vec.GetType(),
				Null: vec.GetNulls().Contains(uint64(row)),
			}
			if !cell.Null {
				var err error
				cell.Value, err = canonicalValueFromVector(vec, row)
				if err != nil {
					return err
				}
			}
			cells[column] = cell
		}
		if err := encoder.WriteRow(ctx, cells); err != nil {
			return err
		}
	}
	return nil
}

func (encoder *CanonicalValueEncoder) Sum() [32]byte {
	var result [32]byte
	copy(result[:], encoder.hash.Sum(nil))
	return result
}

func (encoder *CanonicalValueEncoder) RowCount() uint64 {
	return encoder.rowCount
}

func (encoder *CanonicalValueEncoder) LogicalBytes() uint64 {
	return encoder.logicalBytes
}

func (encoder *CanonicalValueEncoder) writeByte(value byte) {
	var data [1]byte
	data[0] = value
	_, _ = encoder.hash.Write(data[:])
	encoder.logicalBytes++
}

func (encoder *CanonicalValueEncoder) writeUint16(value uint16) {
	var data [2]byte
	binary.BigEndian.PutUint16(data[:], value)
	encoder.writeBytes(data[:])
}

func (encoder *CanonicalValueEncoder) writeUint32(value uint32) {
	var data [4]byte
	binary.BigEndian.PutUint32(data[:], value)
	encoder.writeBytes(data[:])
}

func (encoder *CanonicalValueEncoder) writeUint64(value uint64) {
	var data [8]byte
	binary.BigEndian.PutUint64(data[:], value)
	encoder.writeBytes(data[:])
}

func (encoder *CanonicalValueEncoder) writeBytes(value []byte) {
	_, _ = encoder.hash.Write(value)
	encoder.logicalBytes += uint64(len(value))
}

func isCanonicalTypeSupported(oid types.T) bool {
	switch oid {
	case types.T_bool,
		types.T_bit,
		types.T_int8,
		types.T_int16,
		types.T_int32,
		types.T_int64,
		types.T_uint8,
		types.T_uint16,
		types.T_uint32,
		types.T_uint64,
		types.T_float32,
		types.T_float64,
		types.T_char,
		types.T_varchar,
		types.T_binary,
		types.T_varbinary,
		types.T_blob,
		types.T_text,
		types.T_json,
		types.T_date,
		types.T_datetime,
		types.T_timestamp,
		types.T_time,
		types.T_decimal64,
		types.T_decimal128,
		types.T_decimal256,
		types.T_uuid,
		types.T_enum:
		return true
	default:
		return false
	}
}

func canonicalValueFromVector(vec *vector.Vector, row int) (any, error) {
	switch vec.GetType().Oid {
	case types.T_bool:
		return vector.GetFixedAtNoTypeCheck[bool](vec, row), nil
	case types.T_bit, types.T_uint64:
		return vector.GetFixedAtNoTypeCheck[uint64](vec, row), nil
	case types.T_int8:
		return vector.GetFixedAtNoTypeCheck[int8](vec, row), nil
	case types.T_int16:
		return vector.GetFixedAtNoTypeCheck[int16](vec, row), nil
	case types.T_int32:
		return vector.GetFixedAtNoTypeCheck[int32](vec, row), nil
	case types.T_int64:
		return vector.GetFixedAtNoTypeCheck[int64](vec, row), nil
	case types.T_uint8:
		return vector.GetFixedAtNoTypeCheck[uint8](vec, row), nil
	case types.T_uint16:
		return vector.GetFixedAtNoTypeCheck[uint16](vec, row), nil
	case types.T_uint32:
		return vector.GetFixedAtNoTypeCheck[uint32](vec, row), nil
	case types.T_float32:
		return vector.GetFixedAtNoTypeCheck[float32](vec, row), nil
	case types.T_float64:
		return vector.GetFixedAtNoTypeCheck[float64](vec, row), nil
	case types.T_char, types.T_varchar, types.T_binary, types.T_varbinary,
		types.T_blob, types.T_text:
		return vec.GetBytesAt(row), nil
	case types.T_json:
		return []byte(types.DecodeJson(vec.GetBytesAt(row)).String()), nil
	case types.T_date:
		return vector.GetFixedAtNoTypeCheck[types.Date](vec, row), nil
	case types.T_datetime:
		return vector.GetFixedAtNoTypeCheck[types.Datetime](vec, row), nil
	case types.T_timestamp:
		return vector.GetFixedAtNoTypeCheck[types.Timestamp](vec, row), nil
	case types.T_time:
		return vector.GetFixedAtNoTypeCheck[types.Time](vec, row), nil
	case types.T_decimal64:
		return vector.GetFixedAtNoTypeCheck[types.Decimal64](vec, row), nil
	case types.T_decimal128:
		return vector.GetFixedAtNoTypeCheck[types.Decimal128](vec, row), nil
	case types.T_decimal256:
		return vector.GetFixedAtNoTypeCheck[types.Decimal256](vec, row), nil
	case types.T_uuid:
		return vector.GetFixedAtNoTypeCheck[types.Uuid](vec, row), nil
	case types.T_enum:
		return vector.GetFixedAtNoTypeCheck[types.Enum](vec, row), nil
	default:
		return nil, moerr.NewNotSupportedf(
			context.Background(),
			"Lifecycle canonical type %s",
			vec.GetType().Oid,
		)
	}
}

func encodeCanonicalValue(typ types.Type, value any) ([]byte, error) {
	switch typ.Oid {
	case types.T_bool:
		if typed, ok := value.(bool); ok {
			if typed {
				return []byte{1}, nil
			}
			return []byte{0}, nil
		}
	case types.T_int8:
		if typed, ok := value.(int8); ok {
			return []byte{byte(typed)}, nil
		}
	case types.T_uint8:
		if typed, ok := value.(uint8); ok {
			return []byte{typed}, nil
		}
	case types.T_int16:
		if typed, ok := value.(int16); ok {
			return encodeUint16(uint16(typed)), nil
		}
	case types.T_uint16:
		if typed, ok := value.(uint16); ok {
			return encodeUint16(typed), nil
		}
	case types.T_enum:
		if typed, ok := value.(types.Enum); ok {
			return encodeUint16(uint16(typed)), nil
		}
	case types.T_int32:
		if typed, ok := value.(int32); ok {
			return encodeUint32(uint32(typed)), nil
		}
	case types.T_uint32:
		if typed, ok := value.(uint32); ok {
			return encodeUint32(typed), nil
		}
	case types.T_date:
		if typed, ok := value.(types.Date); ok {
			return encodeUint32(uint32(typed)), nil
		}
	case types.T_int64:
		if typed, ok := value.(int64); ok {
			return encodeUint64(uint64(typed)), nil
		}
	case types.T_bit, types.T_uint64:
		if typed, ok := value.(uint64); ok {
			return encodeUint64(typed), nil
		}
	case types.T_datetime:
		if typed, ok := value.(types.Datetime); ok {
			return encodeUint64(uint64(typed)), nil
		}
	case types.T_timestamp:
		if typed, ok := value.(types.Timestamp); ok {
			return encodeUint64(uint64(typed)), nil
		}
	case types.T_time:
		if typed, ok := value.(types.Time); ok {
			return encodeUint64(uint64(typed)), nil
		}
	case types.T_float32:
		if typed, ok := value.(float32); ok {
			bits := math.Float32bits(typed)
			if math.IsNaN(float64(typed)) {
				bits = 0x7fc00000
			} else if typed == 0 {
				bits = 0
			}
			return encodeUint32(bits), nil
		}
	case types.T_float64:
		if typed, ok := value.(float64); ok {
			bits := math.Float64bits(typed)
			if math.IsNaN(typed) {
				bits = 0x7ff8000000000000
			} else if typed == 0 {
				bits = 0
			}
			return encodeUint64(bits), nil
		}
	case types.T_char, types.T_varchar, types.T_binary, types.T_varbinary,
		types.T_blob, types.T_text, types.T_json:
		if typed, ok := value.([]byte); ok {
			return typed, nil
		}
		if typed, ok := value.(string); ok {
			return []byte(typed), nil
		}
	case types.T_decimal64:
		if typed, ok := value.(types.Decimal64); ok {
			return encodeUint64(uint64(typed)), nil
		}
	case types.T_decimal128:
		if typed, ok := value.(types.Decimal128); ok {
			result := make([]byte, 16)
			binary.BigEndian.PutUint64(result[0:8], typed.B64_127)
			binary.BigEndian.PutUint64(result[8:16], typed.B0_63)
			return result, nil
		}
	case types.T_decimal256:
		if typed, ok := value.(types.Decimal256); ok {
			result := make([]byte, 32)
			binary.BigEndian.PutUint64(result[0:8], typed.B192_255)
			binary.BigEndian.PutUint64(result[8:16], typed.B128_191)
			binary.BigEndian.PutUint64(result[16:24], typed.B64_127)
			binary.BigEndian.PutUint64(result[24:32], typed.B0_63)
			return result, nil
		}
	case types.T_uuid:
		if typed, ok := value.(types.Uuid); ok {
			return typed[:], nil
		}
	}
	return nil, moerr.NewInternalErrorNoCtxf("unexpected Go value %T for %s", value, typ.Oid)
}

func encodeUint16(value uint16) []byte {
	var result [2]byte
	binary.BigEndian.PutUint16(result[:], value)
	return result[:]
}

func encodeUint32(value uint32) []byte {
	var result [4]byte
	binary.BigEndian.PutUint32(result[:], value)
	return result[:]
}

func encodeUint64(value uint64) []byte {
	var result [8]byte
	binary.BigEndian.PutUint64(result[:], value)
	return result[:]
}
