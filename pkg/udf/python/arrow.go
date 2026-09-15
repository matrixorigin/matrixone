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
	"io"
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
	commonutil "github.com/matrixorigin/matrixone/pkg/common/util"
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
	// Bulk append is only used for a large fixed-width primitive column.  The
	// small-batch path avoids allocating a temporary validity slice and keeps
	// the ordinary per-value path for constants and special SQL encodings.
	bulkInputMinRows = 64
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
	// Planner types use scale=-1 as the sentinel for types whose SQL
	// definition has no scale (for example INT and BINARY).  That sentinel is
	// an internal planner representation and cannot cross the Python ABI:
	// descriptors are canonical and use zero for every non-applicable field.
	// Likewise, display widths on numeric types are not part of their value
	// contract.  Normalize those fields here so SQL planner metadata and the
	// persisted/Arrow descriptor have one stable representation.
	width := typ.Width
	scale := typ.Scale
	switch typ.Oid {
	case types.T_decimal64, types.T_decimal128:
		if scale < 0 {
			scale = 0
		}
	case types.T_time, types.T_datetime, types.T_timestamp:
		width = 0
		if scale < 0 {
			scale = 0
		}
	case types.T_char, types.T_varchar, types.T_binary, types.T_varbinary,
		types.T_array_float32, types.T_array_float64:
		scale = 0
	default:
		width = 0
		scale = 0
	}
	d := TypeDescriptor{TypeID: int32(typ.Oid), Width: width, Scale: scale, Charset: typ.Charset, OffsetWidth: 32}
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
	if err := d.Validate(); err != nil {
		return "", err
	}
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

// Validate checks the semantic fields that Arrow's physical type does not
// carry.  Keeping this at the descriptor boundary makes manually constructed
// catalog or wire descriptors obey the same contract as descriptors produced
// from a SQL type.
func (d TypeDescriptor) Validate() error {
	oid := types.T(d.TypeID)
	if d.Width < 0 {
		return fmt.Errorf("TYPE_CONTRACT: descriptor width must be non-negative")
	}
	if d.Scale < 0 {
		return fmt.Errorf("TYPE_CONTRACT: descriptor scale must be non-negative")
	}
	expectedOffsetWidth := int32(32)
	if oid == types.T_uuid || oid == types.T_array_float32 || oid == types.T_array_float64 {
		expectedOffsetWidth = 0
	}
	if d.OffsetWidth != expectedOffsetWidth {
		return fmt.Errorf("TYPE_CONTRACT: descriptor offset_width must be %d for %s", expectedOffsetWidth, oid.String())
	}
	if d.JSONEncoding != "" && (oid != types.T_json || d.JSONEncoding != "canonical_text") {
		return fmt.Errorf("TYPE_CONTRACT: unsupported JSON encoding")
	}
	if oid == types.T_json && d.JSONEncoding != "canonical_text" {
		return fmt.Errorf("TYPE_CONTRACT: JSON descriptor must use canonical_text encoding")
	}
	if d.TemporalEncoding != "" && (oid != types.T_date && oid != types.T_datetime && oid != types.T_timestamp || d.TemporalEncoding != "sql_zero_struct") {
		return fmt.Errorf("TYPE_CONTRACT: unsupported temporal encoding")
	}
	if (oid == types.T_date || oid == types.T_datetime || oid == types.T_timestamp) && d.TemporalEncoding != "sql_zero_struct" {
		return fmt.Errorf("TYPE_CONTRACT: temporal descriptor must use sql_zero_struct encoding")
	}

	switch oid {
	case types.T_bool, types.T_int8, types.T_int16, types.T_int32, types.T_int64,
		types.T_uint8, types.T_uint16, types.T_uint32, types.T_uint64,
		types.T_float32, types.T_float64, types.T_json:
		if d.Width != 0 || d.Scale != 0 || d.Charset != types.CharsetLegacy {
			return fmt.Errorf("TYPE_CONTRACT: descriptor carries unused fields for %s", oid.String())
		}
	case types.T_decimal64, types.T_decimal128:
		maxPrecision := int32(38)
		if oid == types.T_decimal64 {
			maxPrecision = 18
		}
		if d.Width < 1 || d.Width > maxPrecision {
			return fmt.Errorf("TYPE_CONTRACT: decimal precision %d is outside %s range", d.Width, oid.String())
		}
		if d.Scale > d.Width {
			return fmt.Errorf("TYPE_CONTRACT: decimal scale %d exceeds precision %d", d.Scale, d.Width)
		}
		if d.Charset != types.CharsetLegacy {
			return fmt.Errorf("TYPE_CONTRACT: decimal descriptor carries a charset")
		}
	case types.T_date:
		if d.Width != 0 || d.Scale != 0 || d.Charset != types.CharsetLegacy {
			return fmt.Errorf("TYPE_CONTRACT: DATE descriptor has unused or invalid fields")
		}
	case types.T_time, types.T_datetime, types.T_timestamp:
		if d.Width != 0 || d.Scale > 6 || d.Charset != types.CharsetLegacy {
			return fmt.Errorf("TYPE_CONTRACT: temporal scale or charset is outside the supported range")
		}
	case types.T_char, types.T_varchar, types.T_text:
		if d.Scale != 0 {
			return fmt.Errorf("TYPE_CONTRACT: text descriptor carries an unused scale")
		}
		switch d.Charset {
		case types.CharsetLegacy, types.CharsetUTF8MB4Bin, types.CharsetUTF8:
		default:
			return fmt.Errorf("TYPE_CONTRACT: unsupported text charset %d", d.Charset)
		}
	case types.T_binary, types.T_varbinary, types.T_blob:
		if d.Scale != 0 || d.Charset != types.CharsetBinary {
			return fmt.Errorf("TYPE_CONTRACT: binary descriptor must use the binary charset")
		}
	case types.T_uuid:
		if d.Width != 0 || d.Scale != 0 || d.Charset != types.CharsetLegacy {
			return fmt.Errorf("TYPE_CONTRACT: UUID descriptor carries unused fields")
		}
	case types.T_array_float32, types.T_array_float64:
		if d.Width < 1 || d.Width > types.MaxArrayDimension || d.Scale != 0 || d.Charset != types.CharsetLegacy {
			return fmt.Errorf("TYPE_CONTRACT: vector dimension or metadata is outside the supported range")
		}
	default:
		return fmt.Errorf("TYPE_CONTRACT: unsupported MatrixOne type id %d", d.TypeID)
	}
	return nil
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

func (c *flightFrameCollector) reset(maxBytes, retainedBytes int64) {
	// Do not reuse the frame slice backing array. A successful candidate can be
	// retained by the size search while the next candidate is encoded.
	c.frames = nil
	c.maxBytes = maxBytes
	c.total = retainedBytes
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

// inputBatchWireEncoder keeps one Arrow IPC writer alive for an invocation.
// The writer emits the schema once; later candidates and batches only encode a
// record message. The cached schema is still counted in every candidate's
// limit, because MaxBatchBytes describes the complete Flight payload that the
// receiver would have to admit for that batch.
type inputBatchWireEncoder struct {
	schema      *arrow.Schema
	collector   *flightFrameCollector
	writer      *flight.Writer
	schemaFrame ArrowFrame
	schemaBytes int64
	schemaReady bool
	closed      bool
}

func (e *inputBatchWireEncoder) encode(record arrow.RecordBatch, maxBytes int64) ([]ArrowFrame, error) {
	if e == nil || e.closed || record == nil || maxBytes <= 0 {
		return nil, fmt.Errorf("invalid Arrow record batch encoder")
	}
	if e.schema == nil || !record.Schema().Equal(e.schema) {
		return nil, fmt.Errorf("Arrow record batch schema does not match the invocation schema")
	}
	if e.writer == nil {
		e.collector = &flightFrameCollector{}
		e.writer = flight.NewRecordWriter(
			e.collector,
			ipc.WithSchema(e.schema),
			ipc.WithAllocator(memory.NewGoAllocator()),
		)
	}
	writerHadSchema := e.schemaReady
	e.collector.reset(maxBytes, e.schemaBytes)
	if err := e.writer.Write(record); err != nil {
		e.captureSchema()
		return nil, fmt.Errorf("encode Arrow record batch: %w", err)
	}
	e.captureSchema()
	if writerHadSchema {
		if len(e.collector.frames) != 1 {
			return nil, fmt.Errorf("Arrow writer did not emit one record frame")
		}
		return []ArrowFrame{e.schemaFrame, e.collector.frames[0]}, nil
	}
	if len(e.collector.frames) != 2 || !e.schemaReady {
		return nil, fmt.Errorf("Arrow stream did not contain schema and record batch")
	}
	return append([]ArrowFrame(nil), e.collector.frames...), nil
}

func (e *inputBatchWireEncoder) captureSchema() {
	if e == nil || e.schemaReady || e.collector == nil || len(e.collector.frames) == 0 {
		return
	}
	frame := e.collector.frames[0]
	e.schemaFrame = ArrowFrame{
		Header: append([]byte(nil), frame.Header...),
		Body:   append([]byte(nil), frame.Body...),
	}
	e.schemaBytes = int64(len(e.schemaFrame.Header) + len(e.schemaFrame.Body))
	e.schemaReady = true
}

func (e *inputBatchWireEncoder) close() error {
	if e == nil || e.closed {
		return nil
	}
	if e.writer == nil {
		e.closed = true
		return nil
	}
	if err := e.writer.Close(); err != nil {
		return fmt.Errorf("close Arrow record batch encoder: %w", err)
	}
	// Commit the closed state only after the writer has released its resources.
	// A failed close remains retryable for the owning invocation cleanup path.
	e.closed = true
	return nil
}

func BuildInputRecord(inputs []*vector.Vector, args []types.Type, length int) (arrow.RecordBatch, *arrow.Schema, error) {
	return BuildInputRecordRange(inputs, args, 0, length)
}

func BuildInputRecordRange(inputs []*vector.Vector, args []types.Type, start, length int) (arrow.RecordBatch, *arrow.Schema, error) {
	encoder, err := newInputBatchEncoder(inputs, args)
	if err != nil {
		return nil, nil, err
	}
	return encoder.build(start, length)
}

// inputBatchEncoder owns the immutable descriptor/schema portion of the
// Gateway input boundary for one invocation.  Building it once avoids
// re-creating TypeDescriptor, Field metadata, and the Arrow Schema for every
// physical batch and every size probe.  The record builder and encoded bytes
// remain per batch, so the encoder does not retain input or output buffers.
type inputBatchEncoder struct {
	inputs            []*vector.Vector
	args              []types.Type
	descriptors       []TypeDescriptor
	varlenaParameters []vector.FunctionParameterWrapper[types.Varlena]
	schema            *arrow.Schema
	fixedWidth        bool
	wire              *inputBatchWireEncoder
}

func newInputBatchEncoder(inputs []*vector.Vector, args []types.Type) (*inputBatchEncoder, error) {
	if len(inputs) != len(args) {
		return nil, fmt.Errorf("invalid Python UDF input shape")
	}
	fields := make([]arrow.Field, len(args))
	descriptors := make([]TypeDescriptor, len(args))
	varlenaParameters := make([]vector.FunctionParameterWrapper[types.Varlena], len(args))
	fixedWidth := true
	for i, typ := range args {
		descriptor, err := NewTypeDescriptor(typ)
		if err != nil {
			return nil, err
		}
		descriptors[i] = descriptor
		fields[i], err = descriptor.Field(fmt.Sprintf("arg_%d", i))
		if err != nil {
			return nil, err
		}
		if typ.IsVarlen() {
			fixedWidth = false
		}
		if inputs[i] == nil {
			return nil, fmt.Errorf("input column %d is missing", i)
		}
		actualType := inputs[i].GetType()
		if actualType == nil {
			return nil, fmt.Errorf("input column %d has no type", i)
		}
		if !actualType.Eq(typ) {
			return nil, fmt.Errorf(
				"input column %d type %s does not match the frozen argument type %s",
				i,
				actualType.DescString(),
				typ.DescString(),
			)
		}
		if inputs[i].Length() > 0 && isVarlenaInputType(typ.Oid) {
			varlenaParameters[i] = vector.GenerateFunctionStrParameter(inputs[i])
		}
	}
	schema := arrow.NewSchema(fields, nil)
	return &inputBatchEncoder{
		inputs:            inputs,
		args:              args,
		descriptors:       descriptors,
		varlenaParameters: varlenaParameters,
		schema:            schema,
		fixedWidth:        fixedWidth,
	}, nil
}

func (e *inputBatchEncoder) encode(record arrow.RecordBatch, maxBytes int64) ([]ArrowFrame, error) {
	if e == nil {
		return nil, fmt.Errorf("invalid Python UDF input encoder")
	}
	if e.wire == nil {
		e.wire = &inputBatchWireEncoder{schema: e.schema}
	}
	return e.wire.encode(record, maxBytes)
}

func (e *inputBatchEncoder) close() error {
	if e == nil || e.wire == nil {
		return nil
	}
	return e.wire.close()
}

func (e *inputBatchEncoder) build(start, length int) (arrow.RecordBatch, *arrow.Schema, error) {
	if e == nil || start < 0 || length <= 0 {
		return nil, nil, fmt.Errorf("invalid Python UDF input shape")
	}
	for i, input := range e.inputs {
		if input.Length() == 0 || (!input.IsConst() && input.Length() < start+length) {
			return nil, nil, fmt.Errorf("input column %d is shorter than batch range", i)
		}
	}
	if len(e.args) == 0 {
		return array.NewRecordBatch(e.schema, nil, int64(length)), e.schema, nil
	}
	builder := array.NewRecordBuilder(memory.NewGoAllocator(), e.schema)
	defer builder.Release()
	for column, typ := range e.args {
		bulk, err := appendBulkFixedInputValue(
			builder.Field(column), e.inputs[column], typ, start, length,
		)
		if err != nil {
			return nil, nil, fmt.Errorf("input column %d: %w", column, err)
		}
		if bulk {
			continue
		}
		for row := 0; row < length; row++ {
			if err := appendInputValue(
				builder.Field(column), e.inputs[column], typ, start, row,
				e.varlenaParameters[column],
			); err != nil {
				return nil, nil, fmt.Errorf("input column %d row %d: %w", column, row, err)
			}
		}
	}
	return builder.NewRecordBatch(), e.schema, nil
}

// appendBulkFixedInputValue uses Arrow's typed AppendValues APIs for the
// primitive SQL types whose MatrixOne storage already has the same physical
// representation.  It copies only the values that Arrow must own; the source
// vector remains the ownership boundary, and NULL positions are supplied as a
// separate validity slice.  The return value says whether the type was
// handled by this path.
func appendBulkFixedInputValue(
	builder array.Builder, v *vector.Vector, typ types.Type, start, length int,
) (bool, error) {
	if length < bulkInputMinRows {
		return false, nil
	}
	validity := func() []bool {
		if v.IsConstNull() {
			return make([]bool, length)
		}
		if v.GetNulls() == nil || v.GetNulls().IsEmpty() {
			return nil
		}
		valid := make([]bool, length)
		for row := range valid {
			valid[row] = !v.IsNull(uint64(sourceRow(v, start, row)))
		}
		return valid
	}
	if builder == nil || v == nil {
		return false, fmt.Errorf("input column is missing")
	}

	switch typ.Oid {
	case types.T_bool:
		values, err := bulkFixedInputWindow[bool](v, start, length)
		if err != nil {
			return false, err
		}
		builder.(*array.BooleanBuilder).AppendValues(values, validity())
	case types.T_int8:
		values, err := bulkFixedInputWindow[int8](v, start, length)
		if err != nil {
			return false, err
		}
		builder.(*array.Int8Builder).AppendValues(values, validity())
	case types.T_int16:
		values, err := bulkFixedInputWindow[int16](v, start, length)
		if err != nil {
			return false, err
		}
		builder.(*array.Int16Builder).AppendValues(values, validity())
	case types.T_int32:
		values, err := bulkFixedInputWindow[int32](v, start, length)
		if err != nil {
			return false, err
		}
		builder.(*array.Int32Builder).AppendValues(values, validity())
	case types.T_int64:
		values, err := bulkFixedInputWindow[int64](v, start, length)
		if err != nil {
			return false, err
		}
		builder.(*array.Int64Builder).AppendValues(values, validity())
	case types.T_uint8:
		values, err := bulkFixedInputWindow[uint8](v, start, length)
		if err != nil {
			return false, err
		}
		builder.(*array.Uint8Builder).AppendValues(values, validity())
	case types.T_uint16:
		values, err := bulkFixedInputWindow[uint16](v, start, length)
		if err != nil {
			return false, err
		}
		builder.(*array.Uint16Builder).AppendValues(values, validity())
	case types.T_uint32:
		values, err := bulkFixedInputWindow[uint32](v, start, length)
		if err != nil {
			return false, err
		}
		builder.(*array.Uint32Builder).AppendValues(values, validity())
	case types.T_uint64:
		values, err := bulkFixedInputWindow[uint64](v, start, length)
		if err != nil {
			return false, err
		}
		builder.(*array.Uint64Builder).AppendValues(values, validity())
	case types.T_float32:
		values, err := bulkFixedInputWindow[float32](v, start, length)
		if err != nil {
			return false, err
		}
		builder.(*array.Float32Builder).AppendValues(values, validity())
	case types.T_float64:
		values, err := bulkFixedInputWindow[float64](v, start, length)
		if err != nil {
			return false, err
		}
		builder.(*array.Float64Builder).AppendValues(values, validity())
	default:
		return false, nil
	}
	return true, nil
}

func bulkFixedInputWindow[T any](v *vector.Vector, start, length int) ([]T, error) {
	if v.IsConstNull() {
		// A constant NULL intentionally has no physical value slot.  Arrow
		// still needs one placeholder per logical row so its validity bitmap
		// preserves the batch shape.
		return make([]T, length), nil
	}
	values := vector.MustFixedColNoTypeCheck[T](v)
	if v.IsConst() {
		if len(values) == 0 {
			return nil, fmt.Errorf("constant input column has no value")
		}
		expanded := make([]T, length)
		for row := range expanded {
			expanded[row] = values[0]
		}
		return expanded, nil
	}
	if start < 0 || length <= 0 || start > len(values) || length > len(values)-start {
		return nil, fmt.Errorf("input column is shorter than batch range")
	}
	return values[start : start+length], nil
}

// canBuildFullBatch cheaply decides whether a variable-width full candidate
// is worth materializing.  It uses the source values already owned by the
// SQL vector and a deliberately inflated bound.  If the bound is not clearly
// below the wire limit, the bounded probe path is retained so a very wide
// input cannot create an oversized temporary Arrow record just to discover
// that it does not fit.
func (e *inputBatchEncoder) canBuildFullBatch(start, length, maxBytes int64) bool {
	if e == nil || e.fixedWidth || length <= 0 || maxBytes <= 0 {
		return false
	}
	const safetyNumerator = int64(2)
	const safetyDenominator = int64(1)
	const perColumnOverhead = int64(4096)
	const perRowOverhead = int64(16)
	const maxEstimate = int64(^uint64(0) >> 1)
	add := func(total, value int64) int64 {
		if value < 0 || total > maxEstimate-value {
			return maxEstimate
		}
		return total + value
	}
	multiply := func(left, right int64) int64 {
		if left < 0 || right < 0 || (left != 0 && right > maxEstimate/left) {
			return maxEstimate
		}
		return left * right
	}
	estimate := int64(128)
	for column, typ := range e.args {
		if !typ.IsVarlen() {
			return false
		}
		if elementWidth, ok := arrayElementByteWidth(typ.Oid); ok {
			// Vector arguments use a fixed-size-list physical type.  They are
			// varlen in MatrixOne's storage, but do not have a Varlena wrapper
			// or Arrow string/binary offsets to inspect.  Account for every
			// child value and the parent validity bitmap instead of treating
			// the vector as a byte string.
			valueBytes := multiply(int64(typ.Width), elementWidth)
			estimate = add(estimate, multiply(valueBytes, length))
			estimate = add(estimate, add(length, 7)/8)
			estimate = add(estimate, multiply(length, perRowOverhead))
			continue
		}
		if !isVarlenaInputType(typ.Oid) {
			// An unsupported variable-width physical type must use the
			// ordinary bounded probe path.  In particular, never call a nil
			// Varlena wrapper while estimating a batch.
			return false
		}
		descriptor := e.descriptors[column]
		offsetWidth := int64(descriptor.OffsetWidth)
		if offsetWidth == 0 {
			offsetWidth = 32
		}
		offsetWidth /= 8
		if offsetWidth != 4 && offsetWidth != 8 {
			return false
		}
		estimate = add(estimate, perColumnOverhead)
		estimate = add(estimate, multiply(add(length, 1), offsetWidth))
		estimate = add(estimate, add(length, 7)/8)
		parameter := e.varlenaParameters[column]
		for row := int64(0); row < length; row++ {
			index := sourceRow(e.inputs[column], int(start), int(row))
			if e.inputs[column].IsNull(uint64(index)) {
				continue
			}
			value, _ := parameter.GetStrValue(uint64(index))
			valueBytes := int64(len(value))
			// JSON canonicalization can add escaping and separators. The
			// factor is intentionally conservative; a bound that is not
			// clearly safe falls back to the ordinary bounded probes.
			if typ.Oid == types.T_json {
				valueBytes = add(valueBytes, valueBytes)
			}
			estimate = add(estimate, valueBytes)
			estimate = add(estimate, perRowOverhead)
		}
	}
	if estimate > maxEstimate/safetyNumerator {
		return false
	}
	return estimate*safetyNumerator <= maxBytes*safetyDenominator
}

func arrayElementByteWidth(oid types.T) (int64, bool) {
	switch oid {
	case types.T_array_float32:
		return 4, true
	case types.T_array_float64:
		return 8, true
	default:
		return 0, false
	}
}

func sourceRow(v *vector.Vector, start, row int) int {
	if v.IsConst() {
		return 0
	}
	return start + row
}

func isVarlenaInputType(oid types.T) bool {
	switch oid {
	case types.T_char, types.T_varchar, types.T_text, types.T_json,
		types.T_binary, types.T_varbinary, types.T_blob:
		return true
	default:
		return false
	}
}

func appendInputValue(
	builder array.Builder,
	v *vector.Vector,
	typ types.Type,
	start, row int,
	varlenaParameter vector.FunctionParameterWrapper[types.Varlena],
) error {
	index := sourceRow(v, start, row)
	null := v.IsNull(uint64(index))
	if !null && isVarlenaInputType(typ.Oid) && varlenaParameter == nil {
		return fmt.Errorf("missing cached varlena input parameter")
	}
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
			value, _ := varlenaParameter.GetStrValue(uint64(index))
			if typ.Oid == types.T_json {
				canonical, err := canonicalJSONInput(value)
				if err != nil {
					return err
				}
				value = canonical
			}
			b.Append(string(value))
		}
	case types.T_binary, types.T_varbinary, types.T_blob:
		b := builder.(*array.BinaryBuilder)
		if null {
			b.AppendNull()
		} else {
			value, _ := varlenaParameter.GetStrValue(uint64(index))
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

// canonicalJSONInput converts MatrixOne's binary JSON rendering into the
// compact UTF-8 text required by the Python ABI.  ByteJson.String is intended
// for SQL display and includes object/array whitespace; passing it directly
// would make a valid object fail the worker's canonical-text check even
// though scalar JSON values happen to work.
func canonicalJSONInput(value []byte) ([]byte, error) {
	text := []byte(types.DecodeJson(value).String())
	canonical, err := canonicalJSONTextBytes(text)
	if err != nil {
		return nil, fmt.Errorf("TYPE_CONTRACT: invalid JSON input: %w", err)
	}
	return canonical, nil
}

func canonicalJSONText(value string) (string, error) {
	canonical, err := canonicalJSONTextBytes([]byte(value))
	if err != nil {
		return "", err
	}
	return string(canonical), nil
}

func canonicalJSONTextBytes(value []byte) ([]byte, error) {
	var compact bytes.Buffer
	if err := json.Compact(&compact, value); err != nil {
		return nil, err
	}
	return compact.Bytes(), nil
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
	if len(schemaFrame.Body) != 0 {
		return nil, fmt.Errorf("invalid Arrow schema frame body")
	}
	schemaInfo, err := arrowipc.InspectMessage(context.Background(), schemaFrame.Header, arrowipc.ValidationOptions{MaxMetadataBytes: arrowipc.DefaultMaxMetadataBytes, MaxBodyBytes: 0, BodyEnvelopeBytes: 0, MaxDecodedRecordBytes: 1})
	if err != nil {
		return nil, err
	}
	if schemaInfo.HeaderType != arrowipc.MessageHeaderSchema {
		return nil, fmt.Errorf("invalid Arrow schema frame header %d", schemaInfo.HeaderType)
	}
	batchInfo, err := arrowipc.InspectMessage(context.Background(), batchFrame.Header, arrowipc.ValidationOptions{MaxMetadataBytes: arrowipc.DefaultMaxMetadataBytes, MaxBodyBytes: maxBytes, BodyEnvelopeBytes: int64(len(batchFrame.Body)), Body: batchFrame.Body, ValidateBody: true, MaxDecodedRecordBytes: maxBytes})
	if err != nil {
		return nil, err
	}
	if batchInfo.HeaderType != arrowipc.MessageHeaderRecordBatch {
		return nil, fmt.Errorf("invalid Arrow record frame header %d", batchInfo.HeaderType)
	}
	// The snapshot is already the immutable validation/publication backing.
	// Feed that backing directly to Arrow's stream reader instead of assembling
	// another stream-sized byte slice around it.  The small IPC continuation
	// prefixes and alignment padding are separate readers; the body remains
	// owned by the validated snapshot until Arrow releases the record.
	stream := newIPCStreamReader(schemaFrame, batchFrame)
	reader, err := ipc.NewReader(stream, ipc.WithAllocator(memory.NewGoAllocator()))
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

const (
	inlineIPCFrameCount = 2
	inlineIPCPartCount  = inlineIPCFrameCount*5 + 1
)

var ipcZeroPadding [8]byte

func newIPCStreamReader(frames ...ArrowFrame) io.Reader {
	reader := &ipcStreamReader{}
	if len(frames) <= inlineIPCFrameCount {
		reader.parts = reader.inlineParts[:0]
		for index, frame := range frames {
			metadataLength := (len(frame.Header) + 7) &^ 7
			bodyLength := (len(frame.Body) + 7) &^ 7
			binary.LittleEndian.PutUint32(reader.prefixes[index][:4], math.MaxUint32)
			binary.LittleEndian.PutUint32(reader.prefixes[index][4:], uint32(metadataLength))
			reader.parts = append(reader.parts,
				reader.prefixes[index][:],
				frame.Header,
				ipcZeroPadding[:metadataLength-len(frame.Header)],
				frame.Body,
				ipcZeroPadding[:bodyLength-len(frame.Body)],
			)
		}
		// Arrow's stream reader uses an eight-byte zero continuation marker to
		// observe the end of the stream after the one record batch.
		reader.parts = append(reader.parts, ipcZeroPadding[:])
		return reader
	}

	// Keep the helper correct for future callers that need more than the
	// schema-plus-record pair. The production result path stays on the inline
	// storage above, so this fallback does not add a per-batch allocation to
	// the current protocol.
	parts := make([][]byte, 0, len(frames)*5+1)
	for _, frame := range frames {
		metadataLength := (len(frame.Header) + 7) &^ 7
		bodyLength := (len(frame.Body) + 7) &^ 7
		var prefix [8]byte
		binary.LittleEndian.PutUint32(prefix[:4], math.MaxUint32)
		binary.LittleEndian.PutUint32(prefix[4:], uint32(metadataLength))
		parts = append(parts,
			prefix[:],
			frame.Header,
			ipcZeroPadding[:metadataLength-len(frame.Header)],
			frame.Body,
			ipcZeroPadding[:bodyLength-len(frame.Body)],
		)
	}
	parts = append(parts, ipcZeroPadding[:])
	return &ipcStreamReader{parts: parts}
}

type ipcStreamReader struct {
	parts       [][]byte
	inlineParts [inlineIPCPartCount][]byte
	prefixes    [inlineIPCFrameCount][8]byte
	part        int
	offset      int
}

func (r *ipcStreamReader) Read(dst []byte) (int, error) {
	if len(dst) == 0 {
		return 0, nil
	}
	read := 0
	for read < len(dst) && r.part < len(r.parts) {
		source := r.parts[r.part]
		if r.offset >= len(source) {
			r.part++
			r.offset = 0
			continue
		}
		copied := copy(dst[read:], source[r.offset:])
		r.offset += copied
		read += copied
	}
	if read != 0 {
		return read, nil
	}
	return 0, io.EOF
}

func AppendArrowResult(descriptor TypeDescriptor, input arrow.Array, result vector.FunctionResultWrapper, mp *mpool.MPool) error {
	if input == nil {
		return fmt.Errorf("TYPE_CONTRACT: missing Arrow result")
	}
	if result == nil {
		return fmt.Errorf("python udf: missing result wrapper")
	}
	if err := validateArrayType(descriptor, input); err != nil {
		return err
	}
	if err := validateArrowValueDomain(descriptor, input); err != nil {
		return err
	}
	if input.Len() == 0 {
		return nil
	}
	if mp == nil {
		return fmt.Errorf("python udf: missing memory pool for Arrow result")
	}
	result.GetResultVector().SetTypeScale(descriptor.Scale)
	typ := types.T(descriptor.TypeID)
	switch typ {
	case types.T_bool:
		return appendBoolArrowResult(input.(*array.Boolean), result)
	case types.T_int8:
		return appendFixedArrowValues(input.(*array.Int8).Int8Values(), input, result)
	case types.T_int16:
		return appendFixedArrowValues(input.(*array.Int16).Int16Values(), input, result)
	case types.T_int32:
		return appendFixedArrowValues(input.(*array.Int32).Int32Values(), input, result)
	case types.T_int64:
		return appendFixedArrowValues(input.(*array.Int64).Int64Values(), input, result)
	case types.T_uint8:
		return appendFixedArrowValues(input.(*array.Uint8).Uint8Values(), input, result)
	case types.T_uint16:
		return appendFixedArrowValues(input.(*array.Uint16).Uint16Values(), input, result)
	case types.T_uint32:
		return appendFixedArrowValues(input.(*array.Uint32).Uint32Values(), input, result)
	case types.T_uint64:
		return appendFixedArrowValues(input.(*array.Uint64).Uint64Values(), input, result)
	case types.T_float32:
		return appendFixedArrowValues(input.(*array.Float32).Float32Values(), input, result)
	case types.T_float64:
		return appendFixedArrowValues(input.(*array.Float64).Float64Values(), input, result)
	case types.T_char, types.T_varchar, types.T_text:
		return appendStringArrowResult(input.(*array.String), result)
	case types.T_json:
		return appendJSONArrowResult(input.(*array.String), result)
	case types.T_binary, types.T_varbinary, types.T_blob:
		return appendBinaryArrowResult(input.(*array.Binary), result)
	}
	for i := 0; i < input.Len(); i++ {
		null := input.IsNull(i)
		switch typ {
		case types.T_decimal64:
			var value types.Decimal64
			if !null {
				converted, err := decimalFromArray(input.(*array.Decimal128).Value(i), true)
				if err != nil {
					return err
				}
				value = converted.(types.Decimal64)
			}
			if err := vector.MustFunctionResult[types.Decimal64](result).Append(value, null); err != nil {
				return err
			}
		case types.T_decimal128:
			var value types.Decimal128
			if !null {
				converted, err := decimalFromArray(input.(*array.Decimal128).Value(i), false)
				if err != nil {
					return err
				}
				value = converted.(types.Decimal128)
			}
			if err := vector.MustFunctionResult[types.Decimal128](result).Append(value, null); err != nil {
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

func appendStringArrowResult(input *array.String, result vector.FunctionResultWrapper) error {
	output := vector.MustFunctionResult[types.Varlena](result)
	for i := 0; i < input.Len(); i++ {
		// AppendBytes copies synchronously into the MO-owned area. The unsafe
		// view therefore never escapes the call and does not retain Arrow's
		// backing buffer after the decoded record is released.
		if err := output.AppendBytes(
			commonutil.UnsafeStringToBytes(input.Value(i)), input.IsNull(i),
		); err != nil {
			return err
		}
	}
	return nil
}

func appendJSONArrowResult(input *array.String, result vector.FunctionResultWrapper) error {
	output := vector.MustFunctionResult[types.Varlena](result)
	for i := 0; i < input.Len(); i++ {
		if input.IsNull(i) {
			if err := output.AppendBytes(nil, true); err != nil {
				return err
			}
			continue
		}
		encoded, err := bytejson.ParseJsonByteFromString(input.Value(i))
		if err != nil {
			return fmt.Errorf("TYPE_CONTRACT: invalid JSON result: %w", err)
		}
		if err = output.AppendBytes(encoded, false); err != nil {
			return err
		}
	}
	return nil
}

func appendBinaryArrowResult(input *array.Binary, result vector.FunctionResultWrapper) error {
	output := vector.MustFunctionResult[types.Varlena](result)
	for i := 0; i < input.Len(); i++ {
		if err := output.AppendBytes(input.Value(i), input.IsNull(i)); err != nil {
			return err
		}
	}
	return nil
}

type nullableArrowArray interface {
	NullN() int
	IsNull(int) bool
}

func appendBoolArrowResult(input *array.Boolean, result vector.FunctionResultWrapper) error {
	output := vector.MustFunctionResult[bool](result)
	if input.NullN() == 0 {
		for i := 0; i < input.Len(); i++ {
			output.AppendMustValue(input.Value(i))
		}
		return nil
	}
	for i := 0; i < input.Len(); i++ {
		if input.IsNull(i) {
			output.AppendMustNull()
		} else {
			output.AppendMustValue(input.Value(i))
		}
	}
	return nil
}

func appendFixedArrowValues[T types.FixedSizeT](values []T, input nullableArrowArray, result vector.FunctionResultWrapper) error {
	output := vector.MustFunctionResult[T](result)
	if input.NullN() == 0 {
		for _, value := range values {
			output.AppendMustValue(value)
		}
		return nil
	}
	for i, value := range values {
		if input.IsNull(i) {
			output.AppendMustNull()
		} else {
			output.AppendMustValue(value)
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
		case types.T_json:
			value := input.(*array.String).Value(row)
			canonical, err := canonicalJSONText(value)
			if err != nil {
				return fmt.Errorf("TYPE_CONTRACT: JSON row %d is not valid canonical text: %w", row, err)
			}
			if canonical != value {
				return fmt.Errorf("TYPE_CONTRACT: JSON row %d is not canonical text", row)
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
	if narrow && !decimal128FitsDecimal64(high, low) {
		return nil, fmt.Errorf("TYPE_CONTRACT: Decimal128 result does not fit Decimal64")
	}
	if narrow {
		return types.Decimal64(low), nil
	}
	return types.Decimal128{B0_63: low, B64_127: high}, nil
}

func decimal128FitsDecimal64(high, low uint64) bool {
	// A signed 128-bit value fits in signed 64 bits only when the high word is
	// a proper sign extension of the low word. Checking the high word alone
	// would turn +2^63 into the negative Decimal64 minimum.
	return (high == 0 && low <= math.MaxInt64) ||
		(high == math.MaxUint64 && low >= uint64(1)<<63)
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
