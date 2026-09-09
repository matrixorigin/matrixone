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

package python

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/binary"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"math"
	"reflect"
	"unicode/utf8"

	"github.com/apache/arrow-go/v18/arrow"
	"github.com/apache/arrow-go/v18/arrow/array"
	"github.com/apache/arrow-go/v18/arrow/decimal128"
	"github.com/apache/arrow-go/v18/arrow/flight"
	"github.com/apache/arrow-go/v18/arrow/ipc"
	"github.com/apache/arrow-go/v18/arrow/memory"
	"github.com/matrixorigin/matrixone/pkg/common/mpool"
	"github.com/matrixorigin/matrixone/pkg/container/arrowipc"
	"github.com/matrixorigin/matrixone/pkg/container/bytejson"
	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/container/vector"
)

const (
	TypeMetadataKey        = "mo.udf.type"
	TypeFingerprintKey     = "mo.udf.type_fingerprint"
	DefaultMaxBatchBytes   = 16 << 20
	DefaultMaxBatchRows    = 65536
	DefaultMaxControlBytes = 1 << 20
)

type TypeDescriptor struct {
	TypeID           int32  `json:"type_id"`
	Width            int32  `json:"width,omitempty"`
	Scale            int32  `json:"scale,omitempty"`
	Charset          uint8  `json:"charset,omitempty"`
	OffsetWidth      int32  `json:"offset_width,omitempty"`
	JSONEncoding     string `json:"json_encoding,omitempty"`
	TemporalEncoding string `json:"temporal_encoding,omitempty"`
}

func NewTypeDescriptor(typ types.Type) (TypeDescriptor, error) {
	d := TypeDescriptor{TypeID: int32(typ.Oid), Width: typ.Width, Scale: typ.Scale, Charset: typ.Charset, OffsetWidth: 32}
	switch typ.Oid {
	case types.T_json:
		d.JSONEncoding = "canonical_text"
	case types.T_date, types.T_datetime, types.T_timestamp:
		d.TemporalEncoding = "sql_zero_struct"
	case types.T_bool, types.T_int8, types.T_int16, types.T_int32, types.T_int64,
		types.T_uint8, types.T_uint16, types.T_uint32, types.T_uint64,
		types.T_float32, types.T_float64, types.T_decimal64, types.T_decimal128,
		types.T_char, types.T_varchar, types.T_text, types.T_uuid, types.T_time,
		types.T_binary, types.T_varbinary, types.T_blob,
		types.T_array_float32, types.T_array_float64:
	default:
		return TypeDescriptor{}, fmt.Errorf("python udf does not support MatrixOne type %s", typ.String())
	}
	if typ.Oid == types.T_array_float32 || typ.Oid == types.T_array_float64 {
		if typ.Width <= 0 {
			return TypeDescriptor{}, fmt.Errorf("python udf requires a positive vector dimension for %s", typ.String())
		}
		d.OffsetWidth = 0
	}
	if typ.Oid == types.T_uuid {
		d.OffsetWidth = 0
	}
	return d, nil
}

func (d TypeDescriptor) Type() types.Type {
	return types.NewWithCharset(types.T(d.TypeID), d.Width, d.Scale, d.Charset)
}
func (d TypeDescriptor) canonical() ([]byte, error) { return json.Marshal(d) }
func (d TypeDescriptor) Fingerprint() (string, error) {
	canonical, err := d.canonical()
	if err != nil {
		return "", err
	}
	physical, err := d.arrowType()
	if err != nil {
		return "", err
	}
	physicalFingerprint := physical.Fingerprint()
	if types.T(d.TypeID) == types.T_uuid {
		// Arrow-Go's FixedSizeBinary fingerprint omits ByteWidth.  Keep the
		// UUID width in the cross-language fingerprint as part of the frozen
		// physical contract instead of relying only on the local type check.
		physicalFingerprint = "@P[16]"
	}
	h := sha256.New()
	h.Write([]byte("matrixone-python-udf-type\x00"))
	h.Write(canonical)
	h.Write([]byte{0})
	h.Write([]byte(physicalFingerprint))
	return hex.EncodeToString(h.Sum(nil)), nil
}

func (d TypeDescriptor) arrowType() (arrow.DataType, error) {
	switch types.T(d.TypeID) {
	case types.T_bool:
		return arrow.FixedWidthTypes.Boolean, nil
	case types.T_int8:
		return arrow.PrimitiveTypes.Int8, nil
	case types.T_int16:
		return arrow.PrimitiveTypes.Int16, nil
	case types.T_int32:
		return arrow.PrimitiveTypes.Int32, nil
	case types.T_int64:
		return arrow.PrimitiveTypes.Int64, nil
	case types.T_uint8:
		return arrow.PrimitiveTypes.Uint8, nil
	case types.T_uint16:
		return arrow.PrimitiveTypes.Uint16, nil
	case types.T_uint32:
		return arrow.PrimitiveTypes.Uint32, nil
	case types.T_uint64:
		return arrow.PrimitiveTypes.Uint64, nil
	case types.T_float32:
		return arrow.PrimitiveTypes.Float32, nil
	case types.T_float64:
		return arrow.PrimitiveTypes.Float64, nil
	case types.T_decimal64, types.T_decimal128:
		precision := d.Width
		if precision == 0 {
			precision = 18
			if types.T(d.TypeID) == types.T_decimal128 {
				precision = 38
			}
		}
		if precision < 1 || precision > decimal128.MaxPrecision {
			return nil, fmt.Errorf("decimal precision %d is outside Arrow's supported range", precision)
		}
		return &arrow.Decimal128Type{Precision: precision, Scale: d.Scale}, nil
	case types.T_char, types.T_varchar, types.T_text, types.T_json:
		if d.OffsetWidth == 64 {
			return arrow.BinaryTypes.LargeString, nil
		}
		return arrow.BinaryTypes.String, nil
	case types.T_binary, types.T_varbinary, types.T_blob:
		if d.OffsetWidth == 64 {
			return arrow.BinaryTypes.LargeBinary, nil
		}
		return arrow.BinaryTypes.Binary, nil
	case types.T_uuid:
		return &arrow.FixedSizeBinaryType{ByteWidth: 16}, nil
	case types.T_time:
		return &arrow.DurationType{Unit: arrow.Microsecond}, nil
	case types.T_array_float32:
		if d.Width <= 0 {
			return nil, fmt.Errorf("invalid vecf32 dimension %d", d.Width)
		}
		return arrow.FixedSizeListOf(d.Width, arrow.PrimitiveTypes.Float32), nil
	case types.T_array_float64:
		if d.Width <= 0 {
			return nil, fmt.Errorf("invalid vecf64 dimension %d", d.Width)
		}
		return arrow.FixedSizeListOf(d.Width, arrow.PrimitiveTypes.Float64), nil
	case types.T_date:
		return sqlTemporalType(arrow.PrimitiveTypes.Date32), nil
	case types.T_datetime:
		return sqlTemporalType(&arrow.TimestampType{Unit: arrow.Microsecond}), nil
	case types.T_timestamp:
		return sqlTemporalType(&arrow.TimestampType{Unit: arrow.Microsecond, TimeZone: "UTC"}), nil
	default:
		return nil, fmt.Errorf("unsupported MatrixOne type id %d", d.TypeID)
	}
}
func sqlTemporalType(value arrow.DataType) *arrow.StructType {
	return arrow.StructOf(arrow.Field{Name: "is_zero", Type: arrow.FixedWidthTypes.Boolean, Nullable: false}, arrow.Field{Name: "value", Type: value, Nullable: false})
}
func (d TypeDescriptor) Field(name string) (arrow.Field, error) {
	typ, err := d.arrowType()
	if err != nil {
		return arrow.Field{}, err
	}
	fingerprint, err := d.Fingerprint()
	if err != nil {
		return arrow.Field{}, err
	}
	descriptor, err := d.canonical()
	if err != nil {
		return arrow.Field{}, err
	}
	return arrow.Field{Name: name, Type: typ, Nullable: true, Metadata: arrow.NewMetadata([]string{TypeMetadataKey, TypeFingerprintKey}, []string{string(descriptor), fingerprint})}, nil
}
func (d TypeDescriptor) ValidateField(field arrow.Field) error {
	want, err := d.Field(field.Name)
	if err != nil {
		return err
	}
	if field.Type == nil || !sameArrowType(field.Type, want.Type) || field.Nullable != want.Nullable {
		return fmt.Errorf("TYPE_CONTRACT: Arrow type %s does not match %s", field.Type, want.Type)
	}
	if !field.Metadata.Equal(want.Metadata) {
		return fmt.Errorf("TYPE_CONTRACT: Arrow logical metadata does not match the frozen descriptor")
	}
	return nil
}

// sameArrowType intentionally compares the complete concrete Arrow type.
// Arrow-Go's Fingerprint is a physical layout fingerprint and, for some
// FixedSizeBinary versions, omits ByteWidth.  A UUID contract must reject
// fixed_size_binary(1) even when that fingerprint collides with the expected
// fixed_size_binary(16).
func sameArrowType(left, right arrow.DataType) bool {
	if left == nil || right == nil {
		return left == right
	}
	return reflect.DeepEqual(left, right)
}

type ArrowFrame struct {
	Header []byte
	Body   []byte
}

type encodedRecordBatch struct {
	Frames []ArrowFrame
	Rows   int64
}

var errArrowBatchTooLarge = errors.New("Arrow record batch exceeds the configured limit")

func EncodeRecordBatch(record arrow.RecordBatch, maxBytes int64) ([]ArrowFrame, error) {
	if record == nil || record.Schema() == nil || maxBytes <= 0 {
		return nil, fmt.Errorf("invalid Arrow record batch")
	}
	collector := &flightFrameCollector{maxBytes: maxBytes}
	writer := flight.NewRecordWriter(collector, ipc.WithSchema(record.Schema()), ipc.WithAllocator(memory.NewGoAllocator()))
	if err := writer.Write(record); err != nil {
		return nil, fmt.Errorf("encode Arrow record batch: %w", err)
	}
	if err := writer.Close(); err != nil {
		return nil, fmt.Errorf("close Arrow record batch: %w", err)
	}
	if len(collector.frames) < 2 {
		return nil, fmt.Errorf("Arrow stream did not contain schema and record batch")
	}
	return collector.frames, nil
}

// flightFrameCollector preserves the exact Flight representation: DataHeader
// is the unframed IPC FlatBuffer and DataBody contains the body plus its IPC
// alignment padding.  A stream-file frame cannot be copied directly into
// FlightData because its eight-byte continuation prefix is not part of
// FlightData.DataHeader.
type flightFrameCollector struct {
	frames   []ArrowFrame
	maxBytes int64
	total    int64
}

func (c *flightFrameCollector) Send(data *flight.FlightData) error {
	if data == nil || len(data.DataHeader) == 0 {
		return fmt.Errorf("invalid Arrow Flight frame")
	}
	if data.FlightDescriptor != nil || len(data.AppMetadata) != 0 {
		return fmt.Errorf("unexpected metadata on Arrow payload frame")
	}
	header := append([]byte(nil), data.DataHeader...)
	body := append([]byte(nil), data.DataBody...)
	c.total += int64(len(header) + len(body))
	if c.total > c.maxBytes {
		return fmt.Errorf("%w: stream is %d bytes, limit is %d", errArrowBatchTooLarge, c.total, c.maxBytes)
	}
	c.frames = append(c.frames, ArrowFrame{Header: header, Body: body})
	return nil
}

func BuildInputRecord(inputs []*vector.Vector, args []types.Type, length int) (arrow.RecordBatch, *arrow.Schema, error) {
	return BuildInputRecordRange(inputs, args, 0, length)
}

func BuildInputRecordRange(inputs []*vector.Vector, args []types.Type, start, length int) (arrow.RecordBatch, *arrow.Schema, error) {
	if start < 0 || length <= 0 || len(inputs) != len(args) {
		return nil, nil, fmt.Errorf("invalid Python UDF input shape")
	}
	fields := make([]arrow.Field, len(args))
	for i, typ := range args {
		descriptor, err := NewTypeDescriptor(typ)
		if err != nil {
			return nil, nil, err
		}
		fields[i], err = descriptor.Field(fmt.Sprintf("arg_%d", i))
		if err != nil {
			return nil, nil, err
		}
		if inputs[i] == nil || (!inputs[i].IsConst() && inputs[i].Length() < start+length) {
			return nil, nil, fmt.Errorf("input column %d is shorter than batch range", i)
		}
	}
	schema := arrow.NewSchema(fields, nil)
	if len(fields) == 0 {
		return array.NewRecordBatch(schema, nil, int64(length)), schema, nil
	}
	builder := array.NewRecordBuilder(memory.NewGoAllocator(), schema)
	defer builder.Release()
	for column, typ := range args {
		for row := 0; row < length; row++ {
			if err := appendInputValue(builder.Field(column), inputs[column], typ, start, row); err != nil {
				return nil, nil, fmt.Errorf("input column %d row %d: %w", column, row, err)
			}
		}
	}
	return builder.NewRecordBatch(), schema, nil
}
func sourceRow(v *vector.Vector, start, row int) int {
	if v.IsConst() {
		return 0
	}
	return start + row
}
func appendInputValue(builder array.Builder, v *vector.Vector, typ types.Type, start, row int) error {
	index := sourceRow(v, start, row)
	null := v.IsNull(uint64(index))
	if typ.Oid == types.T_date || typ.Oid == types.T_datetime || typ.Oid == types.T_timestamp {
		return appendTemporalInput(builder.(*array.StructBuilder), v, typ, index, null)
	}
	if typ.Oid == types.T_time {
		b := builder.(*array.DurationBuilder)
		if null {
			b.AppendNull()
		} else {
			b.Append(arrow.Duration(vector.GetFixedAtNoTypeCheck[types.Time](v, index)))
		}
		return nil
	}
	switch typ.Oid {
	case types.T_bool:
		b := builder.(*array.BooleanBuilder)
		if null {
			b.AppendNull()
		} else {
			b.Append(vector.GetFixedAtNoTypeCheck[bool](v, index))
		}
	case types.T_int8:
		b := builder.(*array.Int8Builder)
		if null {
			b.AppendNull()
		} else {
			b.Append(vector.GetFixedAtNoTypeCheck[int8](v, index))
		}
	case types.T_int16:
		b := builder.(*array.Int16Builder)
		if null {
			b.AppendNull()
		} else {
			b.Append(vector.GetFixedAtNoTypeCheck[int16](v, index))
		}
	case types.T_int32:
		b := builder.(*array.Int32Builder)
		if null {
			b.AppendNull()
		} else {
			b.Append(vector.GetFixedAtNoTypeCheck[int32](v, index))
		}
	case types.T_int64:
		b := builder.(*array.Int64Builder)
		if null {
			b.AppendNull()
		} else {
			b.Append(vector.GetFixedAtNoTypeCheck[int64](v, index))
		}
	case types.T_uint8:
		b := builder.(*array.Uint8Builder)
		if null {
			b.AppendNull()
		} else {
			b.Append(vector.GetFixedAtNoTypeCheck[uint8](v, index))
		}
	case types.T_uint16:
		b := builder.(*array.Uint16Builder)
		if null {
			b.AppendNull()
		} else {
			b.Append(vector.GetFixedAtNoTypeCheck[uint16](v, index))
		}
	case types.T_uint32:
		b := builder.(*array.Uint32Builder)
		if null {
			b.AppendNull()
		} else {
			b.Append(vector.GetFixedAtNoTypeCheck[uint32](v, index))
		}
	case types.T_uint64:
		b := builder.(*array.Uint64Builder)
		if null {
			b.AppendNull()
		} else {
			b.Append(vector.GetFixedAtNoTypeCheck[uint64](v, index))
		}
	case types.T_float32:
		b := builder.(*array.Float32Builder)
		if null {
			b.AppendNull()
		} else {
			b.Append(vector.GetFixedAtNoTypeCheck[float32](v, index))
		}
	case types.T_float64:
		b := builder.(*array.Float64Builder)
		if null {
			b.AppendNull()
		} else {
			b.Append(vector.GetFixedAtNoTypeCheck[float64](v, index))
		}
	case types.T_decimal64:
		b := builder.(*array.Decimal128Builder)
		if null {
			b.AppendNull()
		} else {
			b.Append(decimal64ToArrow(vector.GetFixedAtNoTypeCheck[types.Decimal64](v, index)))
		}
	case types.T_decimal128:
		b := builder.(*array.Decimal128Builder)
		if null {
			b.AppendNull()
		} else {
			value := vector.GetFixedAtNoTypeCheck[types.Decimal128](v, index)
			b.Append(decimal128.New(int64(value.B64_127), value.B0_63))
		}
	case types.T_char, types.T_varchar, types.T_text, types.T_json:
		b := builder.(*array.StringBuilder)
		if null {
			b.AppendNull()
		} else {
			p := vector.GenerateFunctionStrParameter(v)
			value, _ := p.GetStrValue(uint64(index))
			if typ.Oid == types.T_json {
				value = []byte(types.DecodeJson(value).String())
			}
			b.Append(string(value))
		}
	case types.T_binary, types.T_varbinary, types.T_blob:
		b := builder.(*array.BinaryBuilder)
		if null {
			b.AppendNull()
		} else {
			p := vector.GenerateFunctionStrParameter(v)
			value, _ := p.GetStrValue(uint64(index))
			b.Append(value)
		}
	case types.T_uuid:
		b := builder.(*array.FixedSizeBinaryBuilder)
		if null {
			b.AppendNull()
		} else {
			value := vector.GetFixedAtNoTypeCheck[types.Uuid](v, index)
			b.Append(value[:])
		}
	case types.T_array_float32:
		b := builder.(*array.FixedSizeListBuilder)
		if null {
			b.AppendNull()
		} else {
			b.Append(true)
			b.ValueBuilder().(*array.Float32Builder).AppendValues(vector.GetArrayAt[float32](v, index), nil)
		}
	case types.T_array_float64:
		b := builder.(*array.FixedSizeListBuilder)
		if null {
			b.AppendNull()
		} else {
			b.Append(true)
			b.ValueBuilder().(*array.Float64Builder).AppendValues(vector.GetArrayAt[float64](v, index), nil)
		}
	default:
		return fmt.Errorf("unsupported input type %s", typ.String())
	}
	return nil
}
func appendTemporalInput(builder *array.StructBuilder, v *vector.Vector, typ types.Type, index int, null bool) error {
	zero := false
	var raw int64
	switch typ.Oid {
	case types.T_date:
		if !null {
			value := vector.GetFixedAtNoTypeCheck[types.Date](v, index)
			zero = value == types.ZeroDate
			if !zero {
				raw = int64(value.DaysSinceUnixEpoch())
			}
		}
		builder.Append(!null)
		builder.FieldBuilder(0).(*array.BooleanBuilder).Append(zero)
		builder.FieldBuilder(1).(*array.Date32Builder).Append(arrow.Date32(raw))
	case types.T_datetime:
		if !null {
			value := vector.GetFixedAtNoTypeCheck[types.Datetime](v, index)
			zero = value == types.ZeroDatetime
			if !zero {
				raw = int64(value) - types.GetUnixEpochSecs()
			}
		}
		builder.Append(!null)
		builder.FieldBuilder(0).(*array.BooleanBuilder).Append(zero)
		builder.FieldBuilder(1).(*array.TimestampBuilder).Append(arrow.Timestamp(raw))
	case types.T_timestamp:
		if !null {
			value := vector.GetFixedAtNoTypeCheck[types.Timestamp](v, index)
			zero = value == types.ZeroTimestamp
			if !zero {
				raw = int64(value) - types.GetUnixEpochSecs()
			}
		}
		builder.Append(!null)
		builder.FieldBuilder(0).(*array.BooleanBuilder).Append(zero)
		builder.FieldBuilder(1).(*array.TimestampBuilder).Append(arrow.Timestamp(raw))
	default:
		return fmt.Errorf("unsupported temporal type %s", typ.String())
	}
	return nil
}
func decimal64ToArrow(value types.Decimal64) decimal128.Num {
	if value.Sign() {
		return decimal128.New(-1, uint64(value))
	}
	return decimal128.New(0, uint64(value))
}

func DecodeRecordBatch(schemaFrame, batchFrame ArrowFrame, maxBytes int64) (arrow.RecordBatch, error) {
	if len(schemaFrame.Header) == 0 || len(batchFrame.Header) == 0 || maxBytes <= 0 {
		return nil, fmt.Errorf("invalid Arrow output frames")
	}
	if _, err := arrowipc.InspectMessage(context.Background(), schemaFrame.Header, arrowipc.ValidationOptions{MaxMetadataBytes: arrowipc.DefaultMaxMetadataBytes, MaxBodyBytes: 0, BodyEnvelopeBytes: 0, MaxDecodedRecordBytes: 1}); err != nil {
		return nil, err
	}
	if _, err := arrowipc.InspectMessage(context.Background(), batchFrame.Header, arrowipc.ValidationOptions{MaxMetadataBytes: arrowipc.DefaultMaxMetadataBytes, MaxBodyBytes: maxBytes, BodyEnvelopeBytes: int64(len(batchFrame.Body)), Body: batchFrame.Body, ValidateBody: true, MaxDecodedRecordBytes: maxBytes}); err != nil {
		return nil, err
	}
	stream := make([]byte, 0, len(schemaFrame.Header)+len(batchFrame.Header)+len(batchFrame.Body)+32)
	stream = appendIPCFrame(stream, schemaFrame.Header, schemaFrame.Body)
	stream = appendIPCFrame(stream, batchFrame.Header, batchFrame.Body)
	stream = append(stream, 0, 0, 0, 0, 0, 0, 0, 0)
	reader, err := ipc.NewReader(bytes.NewReader(stream), ipc.WithAllocator(memory.NewGoAllocator()))
	if err != nil {
		return nil, fmt.Errorf("decode Arrow output: %w", err)
	}
	defer reader.Release()
	if !reader.Next() {
		return nil, fmt.Errorf("decode Arrow output: %v", reader.Err())
	}
	record := reader.RecordBatch()
	if record == nil {
		return nil, fmt.Errorf("decode Arrow output: missing record batch")
	}
	record.Retain()
	return record, nil
}
func appendIPCFrame(dst, header, body []byte) []byte {
	metadataLength := (len(header) + 7) &^ 7
	var prefix [8]byte
	binary.LittleEndian.PutUint32(prefix[:4], math.MaxUint32)
	binary.LittleEndian.PutUint32(prefix[4:], uint32(metadataLength))
	dst = append(dst, prefix[:]...)
	dst = append(dst, header...)
	for len(dst)%8 != 0 {
		dst = append(dst, 0)
	}
	dst = append(dst, body...)
	for len(dst)%8 != 0 {
		dst = append(dst, 0)
	}
	return dst
}

func AppendArrowResult(descriptor TypeDescriptor, input arrow.Array, result vector.FunctionResultWrapper, mp *mpool.MPool) error {
	if input == nil || result == nil || input.Len() == 0 {
		return nil
	}
	if mp == nil {
		return fmt.Errorf("python udf: missing memory pool for Arrow result")
	}
	if err := validateArrayType(descriptor, input); err != nil {
		return err
	}
	if err := validateArrowValueDomain(descriptor, input); err != nil {
		return err
	}
	result.GetResultVector().SetTypeScale(descriptor.Scale)
	typ := types.T(descriptor.TypeID)
	for i := 0; i < input.Len(); i++ {
		null := input.IsNull(i)
		switch typ {
		case types.T_bool:
			if err := vector.MustFunctionResult[bool](result).Append(input.(*array.Boolean).Value(i), null); err != nil {
				return err
			}
		case types.T_int8:
			if err := vector.MustFunctionResult[int8](result).Append(input.(*array.Int8).Value(i), null); err != nil {
				return err
			}
		case types.T_int16:
			if err := vector.MustFunctionResult[int16](result).Append(input.(*array.Int16).Value(i), null); err != nil {
				return err
			}
		case types.T_int32:
			if err := vector.MustFunctionResult[int32](result).Append(input.(*array.Int32).Value(i), null); err != nil {
				return err
			}
		case types.T_int64:
			if err := vector.MustFunctionResult[int64](result).Append(input.(*array.Int64).Value(i), null); err != nil {
				return err
			}
		case types.T_uint8:
			if err := vector.MustFunctionResult[uint8](result).Append(input.(*array.Uint8).Value(i), null); err != nil {
				return err
			}
		case types.T_uint16:
			if err := vector.MustFunctionResult[uint16](result).Append(input.(*array.Uint16).Value(i), null); err != nil {
				return err
			}
		case types.T_uint32:
			if err := vector.MustFunctionResult[uint32](result).Append(input.(*array.Uint32).Value(i), null); err != nil {
				return err
			}
		case types.T_uint64:
			if err := vector.MustFunctionResult[uint64](result).Append(input.(*array.Uint64).Value(i), null); err != nil {
				return err
			}
		case types.T_float32:
			if err := vector.MustFunctionResult[float32](result).Append(input.(*array.Float32).Value(i), null); err != nil {
				return err
			}
		case types.T_float64:
			if err := vector.MustFunctionResult[float64](result).Append(input.(*array.Float64).Value(i), null); err != nil {
				return err
			}
		case types.T_decimal64:
			value, err := decimalFromArray(input.(*array.Decimal128).Value(i), true)
			if err != nil {
				return err
			}
			if err = vector.MustFunctionResult[types.Decimal64](result).Append(value.(types.Decimal64), null); err != nil {
				return err
			}
		case types.T_decimal128:
			value, err := decimalFromArray(input.(*array.Decimal128).Value(i), false)
			if err != nil {
				return err
			}
			if err = vector.MustFunctionResult[types.Decimal128](result).Append(value.(types.Decimal128), null); err != nil {
				return err
			}
		case types.T_char, types.T_varchar, types.T_text:
			if err := vector.MustFunctionResult[types.Varlena](result).AppendBytes([]byte(input.(*array.String).Value(i)), null); err != nil {
				return err
			}
		case types.T_json:
			if null {
				if err := vector.MustFunctionResult[types.Varlena](result).AppendBytes(nil, true); err != nil {
					return err
				}
				continue
			}
			encoded, err := bytejson.ParseJsonByteFromString(input.(*array.String).Value(i))
			if err != nil {
				return fmt.Errorf("TYPE_CONTRACT: invalid JSON result: %w", err)
			}
			if err = vector.MustFunctionResult[types.Varlena](result).AppendBytes(encoded, false); err != nil {
				return err
			}
		case types.T_binary, types.T_varbinary, types.T_blob:
			if err := vector.MustFunctionResult[types.Varlena](result).AppendBytes(input.(*array.Binary).Value(i), null); err != nil {
				return err
			}
		case types.T_uuid:
			var value types.Uuid
			if !null {
				copy(value[:], input.(*array.FixedSizeBinary).Value(i))
			}
			if err := vector.MustFunctionResult[types.Uuid](result).Append(value, null); err != nil {
				return err
			}
		case types.T_time:
			if err := vector.MustFunctionResult[types.Time](result).Append(types.Time(input.(*array.Duration).Value(i)), null); err != nil {
				return err
			}
		case types.T_array_float32:
			if err := appendFloat32ArrayResult(input.(*array.FixedSizeList), i, null, result, mp); err != nil {
				return err
			}
		case types.T_array_float64:
			if err := appendFloat64ArrayResult(input.(*array.FixedSizeList), i, null, result, mp); err != nil {
				return err
			}
		case types.T_date, types.T_datetime, types.T_timestamp:
			if err := appendTemporalResult(input.(*array.Struct), typ, i, result); err != nil {
				return err
			}
		default:
			return fmt.Errorf("unsupported output type %s", typ.String())
		}
	}
	return nil
}

func appendFloat32ArrayResult(input *array.FixedSizeList, row int, null bool, result vector.FunctionResultWrapper, mp *mpool.MPool) error {
	if null {
		return vector.AppendArray[float32](result.GetResultVector(), nil, true, mp)
	}
	start, end := input.ValueOffsets(row)
	values := input.ListValues().(*array.Float32)
	items := make([]float32, end-start)
	for i := start; i < end; i++ {
		if values.IsNull(int(i)) {
			return fmt.Errorf("TYPE_CONTRACT: vector child at row %d is null", row)
		}
		items[i-start] = values.Value(int(i))
	}
	return vector.AppendArray(result.GetResultVector(), items, false, mp)
}

func appendFloat64ArrayResult(input *array.FixedSizeList, row int, null bool, result vector.FunctionResultWrapper, mp *mpool.MPool) error {
	if null {
		return vector.AppendArray[float64](result.GetResultVector(), nil, true, mp)
	}
	start, end := input.ValueOffsets(row)
	values := input.ListValues().(*array.Float64)
	items := make([]float64, end-start)
	for i := start; i < end; i++ {
		if values.IsNull(int(i)) {
			return fmt.Errorf("TYPE_CONTRACT: vector child at row %d is null", row)
		}
		items[i-start] = values.Value(int(i))
	}
	return vector.AppendArray(result.GetResultVector(), items, false, mp)
}
func validateArrayType(descriptor TypeDescriptor, input arrow.Array) error {
	want, err := descriptor.arrowType()
	if err != nil {
		return err
	}
	if !sameArrowType(input.DataType(), want) {
		return fmt.Errorf("TYPE_CONTRACT: output Arrow type %s does not match %s", input.DataType(), want)
	}
	return nil
}

func validateArrowValueDomain(descriptor TypeDescriptor, input arrow.Array) error {
	if input == nil {
		return fmt.Errorf("TYPE_CONTRACT: missing Arrow result")
	}
	for row := 0; row < input.Len(); row++ {
		if input.IsNull(row) {
			continue
		}
		switch types.T(descriptor.TypeID) {
		case types.T_char, types.T_varchar, types.T_text:
			if descriptor.Width > 0 && int32(utf8.RuneCountInString(input.(*array.String).Value(row))) > descriptor.Width {
				return fmt.Errorf("TYPE_CONTRACT: string row %d exceeds width %d", row, descriptor.Width)
			}
		case types.T_binary, types.T_varbinary, types.T_blob:
			if descriptor.Width > 0 && int32(len(input.(*array.Binary).Value(row))) > descriptor.Width {
				return fmt.Errorf("TYPE_CONTRACT: binary row %d exceeds width %d", row, descriptor.Width)
			}
		case types.T_time:
			value := types.Time(input.(*array.Duration).Value(row))
			if !types.IsMySQLTime(value) || !hasExactMicrosecondScale(int64(value), descriptor.Scale) {
				return fmt.Errorf("TYPE_CONTRACT: TIME row %d is outside the declared SQL domain", row)
			}
		case types.T_decimal64, types.T_decimal128:
			precision := descriptor.Width
			if precision == 0 {
				precision = 18
				if types.T(descriptor.TypeID) == types.T_decimal128 {
					precision = 38
				}
			}
			if !input.(*array.Decimal128).Value(row).FitsInPrecision(precision) {
				return fmt.Errorf("TYPE_CONTRACT: decimal row %d exceeds precision %d", row, precision)
			}
		case types.T_array_float32:
			list := input.(*array.FixedSizeList)
			values := list.ListValues().(*array.Float32)
			start, end := list.ValueOffsets(row)
			for child := start; child < end; child++ {
				if values.IsNull(int(child)) {
					return fmt.Errorf("TYPE_CONTRACT: vector child at row %d is null", row)
				}
			}
		case types.T_array_float64:
			list := input.(*array.FixedSizeList)
			values := list.ListValues().(*array.Float64)
			start, end := list.ValueOffsets(row)
			for child := start; child < end; child++ {
				if values.IsNull(int(child)) {
					return fmt.Errorf("TYPE_CONTRACT: vector child at row %d is null", row)
				}
			}
		case types.T_date, types.T_datetime, types.T_timestamp:
			structValue := input.(*array.Struct)
			zeroField, valueField := structValue.Field(0), structValue.Field(1)
			if zeroField.IsNull(row) || valueField.IsNull(row) {
				return fmt.Errorf("TYPE_CONTRACT: temporal row %d has a null child", row)
			}
			zero := zeroField.(*array.Boolean).Value(row)
			switch types.T(descriptor.TypeID) {
			case types.T_date:
				value := valueField.(*array.Date32).Value(row)
				if zero && value != 0 {
					return fmt.Errorf("TYPE_CONTRACT: DATE zero row %d has a non-zero placeholder", row)
				}
				if !zero {
					date := types.DaysFromUnixEpochToDate(int32(value))
					year, month, day, _ := date.Calendar(true)
					if !types.ValidDate(year, month, day) {
						return fmt.Errorf("TYPE_CONTRACT: DATE row %d is outside the SQL domain", row)
					}
				}
			case types.T_datetime:
				value := valueField.(*array.Timestamp).Value(row)
				if zero && value != 0 {
					return fmt.Errorf("TYPE_CONTRACT: DATETIME zero row %d has a non-zero placeholder", row)
				}
				if !zero {
					absolute, ok := addUnixEpoch(int64(value))
					if !ok || !hasExactMicrosecondScale(absolute, descriptor.Scale) {
						return fmt.Errorf("TYPE_CONTRACT: DATETIME row %d is outside the declared SQL domain", row)
					}
					datetime := types.Datetime(absolute)
					year, month, day, _ := datetime.ToDate().Calendar(true)
					if !types.ValidDatetime(year, month, day) {
						return fmt.Errorf("TYPE_CONTRACT: DATETIME row %d is outside the SQL domain", row)
					}
				}
			case types.T_timestamp:
				value := valueField.(*array.Timestamp).Value(row)
				if zero && value != 0 {
					return fmt.Errorf("TYPE_CONTRACT: TIMESTAMP zero row %d has a non-zero placeholder", row)
				}
				if !zero {
					absolute, ok := addUnixEpoch(int64(value))
					if !ok || absolute < int64(types.TimestampMinValue) || absolute > int64(types.TimestampMaxValue) || !hasExactMicrosecondScale(absolute, descriptor.Scale) {
						return fmt.Errorf("TYPE_CONTRACT: TIMESTAMP row %d is outside the declared SQL domain", row)
					}
				}
			}
		}
	}
	return nil
}

func addUnixEpoch(value int64) (int64, bool) {
	epoch := types.GetUnixEpochSecs()
	if value > math.MaxInt64-epoch || value < math.MinInt64+epoch {
		return 0, false
	}
	return value + epoch, true
}

func hasExactMicrosecondScale(value int64, scale int32) bool {
	if scale < 0 || scale > 6 {
		return false
	}
	quantum := int64(1)
	for i := scale; i < 6; i++ {
		quantum *= 10
	}
	return value%quantum == 0
}
func decimalFromArray(value decimal128.Num, narrow bool) (any, error) {
	high, low := uint64(value.HighBits()), value.LowBits()
	if narrow && high != 0 && high != math.MaxUint64 {
		return nil, fmt.Errorf("TYPE_CONTRACT: Decimal128 result does not fit Decimal64")
	}
	if narrow {
		return types.Decimal64(low), nil
	}
	return types.Decimal128{B0_63: low, B64_127: high}, nil
}
func appendTemporalResult(input *array.Struct, typ types.T, index int, result vector.FunctionResultWrapper) error {
	if input.IsNull(index) {
		return appendTemporalNull(typ, result)
	}
	zero := input.Field(0).(*array.Boolean).Value(index)
	if typ == types.T_date {
		value := types.ZeroDate
		if !zero {
			value = types.DaysFromUnixEpochToDate(int32(input.Field(1).(*array.Date32).Value(index)))
		}
		return vector.MustFunctionResult[types.Date](result).Append(value, false)
	}
	if typ == types.T_datetime {
		value := types.ZeroDatetime
		if !zero {
			value = types.Datetime(input.Field(1).(*array.Timestamp).Value(index)) + types.Datetime(types.GetUnixEpochSecs())
		}
		return vector.MustFunctionResult[types.Datetime](result).Append(value, false)
	}
	value := types.ZeroTimestamp
	if !zero {
		value = types.Timestamp(input.Field(1).(*array.Timestamp).Value(index)) + types.Timestamp(types.GetUnixEpochSecs())
	}
	return vector.MustFunctionResult[types.Timestamp](result).Append(value, false)
}
func appendTemporalNull(typ types.T, result vector.FunctionResultWrapper) error {
	switch typ {
	case types.T_date:
		return vector.MustFunctionResult[types.Date](result).Append(0, true)
	case types.T_datetime:
		return vector.MustFunctionResult[types.Datetime](result).Append(0, true)
	default:
		return vector.MustFunctionResult[types.Timestamp](result).Append(0, true)
	}
}
