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

package sidecarflight

import (
	"encoding/binary"
	"math"

	"github.com/matrixorigin/matrixone/pkg/common/mpool"
	"github.com/matrixorigin/matrixone/pkg/container/batch"
	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/container/vector"
	planpb "github.com/matrixorigin/matrixone/pkg/pb/plan"
)

const (
	arrowHeaderSchema      = byte(1)
	arrowHeaderRecordBatch = byte(3)
	maxArrowMetadataBytes  = 1 << 20

	arrowTypeInt           = byte(2)
	arrowTypeFloatingPoint = byte(3)
	arrowTypeUTF8          = byte(5)
	arrowTypeBool          = byte(6)
	arrowTypeDecimal       = byte(7)
	arrowTypeDate          = byte(8)
)

type arrowField struct {
	name        string
	nullable    bool
	arrowType   byte
	bitWidth    int32
	isSigned    bool
	precision   int32
	scale       int32
	floatKind   int16
	dateUnit    int16
	expected    planpb.Type
	bufferCount int
}

// Schema is the validated flat result shape shared by FlightInfo and every
// FlightData message in one execution.
type Schema struct {
	fields   []arrowField
	headings []string
}

// ParseSchema decodes the Arrow IPC schema without bringing an Arrow runtime
// into MatrixOne. Only the flat, dictionary-free types in the negotiated TPC-H
// capability are accepted.
func ParseSchema(wire []byte, expected []planpb.Type, headings []string) (*Schema, error) {
	if len(wire) == 0 || len(wire) > maxArrowMetadataBytes {
		return nil, internalErrorf("Arrow schema metadata exceeds the supported bound")
	}
	if len(expected) == 0 || len(headings) != len(expected) {
		return nil, internalErrorf("MatrixOne result schema is empty or inconsistent")
	}
	metadata, err := ipcMetadata(wire)
	if err != nil {
		return nil, err
	}
	message, err := rootTable(metadata)
	if err != nil {
		return nil, internalErrorf("Arrow schema message: %w", err)
	}
	version, err := message.byteField(0, 0)
	if err != nil || version != 4 {
		return nil, internalErrorf("Arrow schema message has unsupported metadata version %d", version)
	}
	headerType, err := message.byteField(1, 0)
	if err != nil || headerType != arrowHeaderSchema {
		return nil, internalErrorf("Arrow schema message has header type %d", headerType)
	}
	if bodyLength, bodyErr := message.int64Field(3, 0); bodyErr != nil || bodyLength != 0 {
		return nil, internalErrorf("Arrow schema message has an invalid body length")
	}
	schemaTable, ok, err := message.tableField(2)
	if err != nil {
		return nil, internalErrorf("Arrow schema message is missing its schema: %w", err)
	}
	if !ok {
		return nil, internalErrorf("Arrow schema message is missing its schema")
	}
	endianness, err := schemaTable.int16Field(0, 0)
	if err != nil || endianness != 0 {
		return nil, internalErrorf("Arrow schema is not little-endian")
	}
	_, featureCount, _, err := schemaTable.vector(3, 8)
	if err != nil || featureCount != 0 {
		return nil, internalErrorf("Arrow schema uses unsupported features")
	}
	_, fieldCount, fieldsPresent, err := schemaTable.vector(1, 4)
	if err != nil {
		return nil, internalErrorf("Arrow schema fields: %w", err)
	}
	if !fieldsPresent {
		fieldCount = 0
	}
	if fieldCount != len(expected) || len(headings) != len(expected) {
		return nil, internalErrorf("Arrow schema has %d fields; MatrixOne expects %d", fieldCount, len(expected))
	}
	fieldTables, err := schemaTable.tableVector(1)
	if err != nil {
		return nil, internalErrorf("Arrow schema fields: %w", err)
	}
	result := &Schema{fields: make([]arrowField, len(fieldTables)), headings: append([]string(nil), headings...)}
	for i, table := range fieldTables {
		field, parseErr := parseArrowField(table, expected[i])
		if parseErr != nil {
			return nil, internalErrorf("Arrow field %d: %w", i, parseErr)
		}
		if field.name != headings[i] {
			return nil, internalErrorf("Arrow field %d is named %q; MatrixOne expects %q", i, field.name, headings[i])
		}
		result.fields[i] = field
	}
	return result, nil
}

func parseArrowField(table flatTable, expected planpb.Type) (arrowField, error) {
	name, ok, err := table.stringField(0)
	if err != nil {
		return arrowField{}, internalErrorf("missing name: %w", err)
	}
	if !ok {
		return arrowField{}, internalErrorf("missing name")
	}
	if dictionary, present, dictionaryErr := table.tableField(4); dictionaryErr != nil {
		return arrowField{}, dictionaryErr
	} else if present || dictionary.data != nil {
		return arrowField{}, internalErrorf("dictionary encoding is not supported")
	}
	_, childCount, _, err := table.vector(5, 4)
	if err != nil {
		return arrowField{}, err
	}
	if childCount != 0 {
		return arrowField{}, internalErrorf("nested Arrow fields are not supported")
	}
	nullable, err := table.boolField(1, false)
	if err != nil {
		return arrowField{}, err
	}
	typeID, err := table.byteField(2, 0)
	if err != nil {
		return arrowField{}, err
	}
	typeTable, ok, err := table.tableField(3)
	if err != nil {
		return arrowField{}, internalErrorf("missing Arrow type metadata: %w", err)
	}
	if !ok {
		return arrowField{}, internalErrorf("missing Arrow type metadata")
	}
	field := arrowField{name: name, nullable: nullable, arrowType: typeID, expected: expected, bufferCount: 2}
	moType := types.T(expected.Id)
	switch typeID {
	case arrowTypeBool:
		if moType != types.T_bool {
			return arrowField{}, typeMismatch(typeID, moType)
		}
		field.bitWidth = 1
	case arrowTypeInt:
		field.bitWidth, err = typeTable.int32Field(0, 0)
		if err != nil {
			return arrowField{}, err
		}
		field.isSigned, err = typeTable.boolField(1, false)
		if err != nil {
			return arrowField{}, err
		}
		wantWidth, wantSigned := expectedIntegerShape(moType)
		if moType == types.T_uint32 {
			// Sirius transports EXTRACT's unsigned MO result as signed i64.
			wantWidth, wantSigned = 64, true
		}
		if field.bitWidth != wantWidth || field.isSigned != wantSigned {
			return arrowField{}, typeMismatch(typeID, moType)
		}
	case arrowTypeFloatingPoint:
		field.floatKind, err = typeTable.int16Field(0, 0)
		if err != nil {
			return arrowField{}, err
		}
		if (moType == types.T_float32 && field.floatKind != 1) || (moType == types.T_float64 && field.floatKind != 2) ||
			(moType != types.T_float32 && moType != types.T_float64) {
			return arrowField{}, typeMismatch(typeID, moType)
		}
		field.bitWidth = map[types.T]int32{types.T_float32: 32, types.T_float64: 64}[moType]
	case arrowTypeUTF8:
		if moType != types.T_char && moType != types.T_varchar {
			return arrowField{}, typeMismatch(typeID, moType)
		}
		field.bufferCount = 3
	case arrowTypeDecimal:
		field.precision, err = typeTable.int32Field(0, 0)
		if err != nil {
			return arrowField{}, err
		}
		field.scale, err = typeTable.int32Field(1, 0)
		if err != nil {
			return arrowField{}, err
		}
		field.bitWidth, err = typeTable.int32Field(2, 128)
		if err != nil {
			return arrowField{}, err
		}
		if (moType != types.T_decimal64 && moType != types.T_decimal128) || field.precision != expected.Width ||
			field.scale != expected.Scale || field.bitWidth != 128 {
			return arrowField{}, typeMismatch(typeID, moType)
		}
	case arrowTypeDate:
		field.dateUnit, err = typeTable.int16Field(0, 0)
		if err != nil {
			return arrowField{}, err
		}
		if moType != types.T_date || field.dateUnit != 0 {
			return arrowField{}, typeMismatch(typeID, moType)
		}
		field.bitWidth = 32
	default:
		return arrowField{}, internalErrorf("unsupported Arrow type id %d", typeID)
	}
	return field, nil
}

func expectedIntegerShape(t types.T) (int32, bool) {
	switch t {
	case types.T_int8:
		return 8, true
	case types.T_int16:
		return 16, true
	case types.T_int32:
		return 32, true
	case types.T_int64:
		return 64, true
	default:
		return 0, false
	}
}

func typeMismatch(arrowType byte, moType types.T) error {
	return internalErrorf("Arrow type %d does not match MatrixOne type %s", arrowType, moType.String())
}

func (s *Schema) matches(other *Schema) bool {
	if s == nil || other == nil || len(s.fields) != len(other.fields) {
		return false
	}
	for i := range s.fields {
		left, right := s.fields[i], other.fields[i]
		if left.name != right.name || left.nullable != right.nullable || left.arrowType != right.arrowType || left.bitWidth != right.bitWidth ||
			left.isSigned != right.isSigned || left.precision != right.precision || left.scale != right.scale ||
			left.floatKind != right.floatKind || left.dateUnit != right.dateUnit ||
			left.expected.Id != right.expected.Id || left.expected.Width != right.expected.Width ||
			left.expected.Scale != right.expected.Scale || left.expected.NotNullable != right.expected.NotNullable ||
			left.expected.Charset != right.expected.Charset {
			return false
		}
	}
	return true
}

func (s *Schema) validateStreamSchema(header []byte) error {
	expected := make([]planpb.Type, len(s.fields))
	for i := range s.fields {
		expected[i] = s.fields[i].expected
	}
	streamSchema, err := ParseSchema(header, expected, s.headings)
	if err != nil {
		return err
	}
	if !s.matches(streamSchema) {
		return internalErrorf("Arrow stream schema differs from FlightInfo schema")
	}
	return nil
}

type arrowNode struct {
	length    int64
	nullCount int64
}

type arrowBuffer struct {
	offset int64
	length int64
}

// decodeRecordBatch converts exactly one flat Arrow record batch into MO
// vectors. The returned batch owns its memory and must be cleaned by the
// synchronous consumer before the next Flight message is requested.
func (s *Schema) decodeRecordBatch(header, body []byte, maxDecodedBytes uint64, mp *mpool.MPool) (result *batch.Batch, err error) {
	if s == nil || mp == nil || maxDecodedBytes == 0 {
		return nil, internalErrorf("sidecar flight: missing schema or memory pool")
	}
	if len(header) == 0 || len(header) > maxArrowMetadataBytes {
		return nil, internalErrorf("Arrow record batch metadata exceeds the supported bound")
	}
	metadata, err := ipcMetadata(header)
	if err != nil {
		return nil, err
	}
	message, err := rootTable(metadata)
	if err != nil {
		return nil, err
	}
	version, err := message.byteField(0, 0)
	if err != nil || version != 4 {
		return nil, internalErrorf("Arrow record batch has unsupported metadata version %d", version)
	}
	headerType, err := message.byteField(1, 0)
	if err != nil || headerType != arrowHeaderRecordBatch {
		return nil, internalErrorf("Arrow message has unsupported header type %d", headerType)
	}
	bodyLength, err := message.int64Field(3, 0)
	if err != nil || bodyLength < 0 || bodyLength != int64(len(body)) {
		return nil, internalErrorf("Arrow record batch body length mismatch")
	}
	record, ok, err := message.tableField(2)
	if err != nil {
		return nil, internalErrorf("Arrow record batch metadata is missing: %w", err)
	}
	if !ok {
		return nil, internalErrorf("Arrow record batch metadata is missing")
	}
	rows, err := record.int64Field(0, 0)
	if err != nil || rows < 0 || rows > int64(maxInt()) {
		return nil, internalErrorf("Arrow record batch row count is invalid")
	}
	if compression, present, compressionErr := record.tableField(3); compressionErr != nil {
		return nil, compressionErr
	} else if present || compression.data != nil {
		return nil, internalErrorf("compressed Arrow batches are not supported")
	}
	_, variadicCount, _, err := record.vector(4, 8)
	if err != nil || variadicCount != 0 {
		return nil, internalErrorf("variadic Arrow buffers are not supported")
	}
	nodeBytes, nodeCount, err := record.structVector(1, 16)
	if err != nil || nodeCount != len(s.fields) {
		return nil, internalErrorf("Arrow record batch has %d field nodes; expected %d", nodeCount, len(s.fields))
	}
	nodes := make([]arrowNode, nodeCount)
	for i := range nodes {
		nodes[i] = arrowNode{length: int64(binary.LittleEndian.Uint64(nodeBytes[i*16:])), nullCount: int64(binary.LittleEndian.Uint64(nodeBytes[i*16+8:]))}
		if nodes[i].length != rows || nodes[i].nullCount < 0 || nodes[i].nullCount > rows {
			return nil, internalErrorf("Arrow field %d has invalid node metadata", i)
		}
	}
	bufferBytes, bufferCount, err := record.structVector(2, 16)
	wantBuffers := 0
	for _, field := range s.fields {
		wantBuffers += field.bufferCount
	}
	if err != nil || bufferCount != wantBuffers {
		return nil, internalErrorf("Arrow record batch has %d buffers; expected %d", bufferCount, wantBuffers)
	}
	buffers := make([]arrowBuffer, bufferCount)
	for i := range buffers {
		buffers[i] = arrowBuffer{offset: int64(binary.LittleEndian.Uint64(bufferBytes[i*16:])), length: int64(binary.LittleEndian.Uint64(bufferBytes[i*16+8:]))}
		if buffers[i].offset < 0 || buffers[i].length < 0 || buffers[i].offset > int64(len(body)) || buffers[i].length > int64(len(body))-buffers[i].offset {
			return nil, internalErrorf("Arrow buffer %d is outside the record body", i)
		}
	}
	decodedBytes := uint64(0)
	bufferIndex := 0
	for _, field := range s.fields {
		rowBytes := uint64(types.New(types.T(field.expected.Id), field.expected.Width, field.expected.Scale).TypeSize())
		if rowBytes == 0 || uint64(rows) > (maxDecodedBytes-decodedBytes)/rowBytes {
			return nil, internalErrorf("Arrow record batch exceeds the decoded-memory budget")
		}
		decodedBytes += uint64(rows) * rowBytes
		nullBytes := uint64(bitmapBytes(rows))
		if nullBytes > maxDecodedBytes-decodedBytes {
			return nil, internalErrorf("Arrow record batch exceeds the decoded-memory budget")
		}
		decodedBytes += nullBytes
		if field.arrowType == arrowTypeUTF8 {
			dataBytes := uint64(buffers[bufferIndex+2].length)
			if dataBytes > maxDecodedBytes-decodedBytes {
				return nil, internalErrorf("Arrow record batch exceeds the decoded-memory budget")
			}
			decodedBytes += dataBytes
		}
		bufferIndex += field.bufferCount
	}
	result = batch.NewWithSize(len(s.fields))
	result.Attrs = append([]string(nil), s.headings...)
	defer func() {
		if err != nil && result != nil {
			result.Clean(mp)
			result = nil
		}
	}()
	bufferIndex = 0
	for i, field := range s.fields {
		moType := types.New(types.T(field.expected.Id), field.expected.Width, field.expected.Scale)
		moType.SetNotNull(field.expected.NotNullable)
		result.Vecs[i] = vector.NewVec(moType)
		columnBuffers := buffers[bufferIndex : bufferIndex+field.bufferCount]
		bufferIndex += field.bufferCount
		if err = decodeColumn(result.Vecs[i], field, nodes[i], columnBuffers, body, mp); err != nil {
			return nil, internalErrorf("Arrow field %d: %w", i, err)
		}
	}
	result.SetRowCount(int(rows))
	return result, nil
}

func decodeColumn(vec *vector.Vector, field arrowField, node arrowNode, buffers []arrowBuffer, body []byte, mp *mpool.MPool) error {
	if (!field.nullable || field.expected.NotNullable) && node.nullCount != 0 {
		return internalErrorf("required field contains nulls")
	}
	validity := sliceBuffer(body, buffers[0])
	if node.nullCount == 0 {
		if len(validity) != 0 && int64(len(validity)) < bitmapBytes(node.length) {
			return internalErrorf("validity buffer is too short")
		}
	} else if int64(len(validity)) < bitmapBytes(node.length) {
		return internalErrorf("validity buffer is too short")
	}
	isNull := func(row int64) bool {
		return node.nullCount != 0 && validity[row>>3]&(1<<uint(row&7)) == 0
	}
	if node.nullCount != 0 {
		actualNulls := int64(0)
		for row := int64(0); row < node.length; row++ {
			if isNull(row) {
				actualNulls++
			}
		}
		if actualNulls != node.nullCount {
			return internalErrorf("validity bitmap does not match the declared null count")
		}
	}
	values := sliceBuffer(body, buffers[1])
	if field.arrowType == arrowTypeUTF8 {
		offsetsBytes := values
		data := sliceBuffer(body, buffers[2])
		if node.length > math.MaxInt64/4-1 || int64(len(offsetsBytes)) != (node.length+1)*4 {
			return internalErrorf("UTF8 offsets have an invalid length")
		}
		previous := int32(0)
		for row := int64(0); row < node.length; row++ {
			start := int32(binary.LittleEndian.Uint32(offsetsBytes[row*4:]))
			end := int32(binary.LittleEndian.Uint32(offsetsBytes[(row+1)*4:]))
			if start != previous || end < start || end < 0 || int64(end) > int64(len(data)) {
				return internalErrorf("UTF8 offsets are invalid")
			}
			if err := vector.AppendBytes(vec, data[start:end], isNull(row), mp); err != nil {
				return err
			}
			previous = end
		}
		if int(previous) != len(data) {
			return internalErrorf("UTF8 data has trailing bytes")
		}
		return nil
	}
	width := int64(field.bitWidth / 8)
	if field.arrowType == arrowTypeBool {
		width = 0
		if int64(len(values)) < bitmapBytes(node.length) {
			return internalErrorf("boolean values buffer is too short")
		}
	} else if width <= 0 || node.length > math.MaxInt64/width || int64(len(values)) != node.length*width {
		return internalErrorf("fixed-width values have an invalid length")
	}
	for row := int64(0); row < node.length; row++ {
		null := isNull(row)
		offset := row * width
		var err error
		switch field.arrowType {
		case arrowTypeBool:
			err = vector.AppendFixed(vec, values[row>>3]&(1<<uint(row&7)) != 0, null, mp)
		case arrowTypeInt:
			switch types.T(field.expected.Id) {
			case types.T_int8:
				err = vector.AppendFixed(vec, int8(values[offset]), null, mp)
			case types.T_int16:
				err = vector.AppendFixed(vec, int16(binary.LittleEndian.Uint16(values[offset:])), null, mp)
			case types.T_int32:
				err = vector.AppendFixed(vec, int32(binary.LittleEndian.Uint32(values[offset:])), null, mp)
			case types.T_int64:
				err = vector.AppendFixed(vec, int64(binary.LittleEndian.Uint64(values[offset:])), null, mp)
			case types.T_uint32:
				value := int64(binary.LittleEndian.Uint64(values[offset:]))
				if !null && (value < 0 || value > math.MaxUint32) {
					return internalErrorf("signed i64 value %d overflows MatrixOne uint32", value)
				}
				err = vector.AppendFixed(vec, uint32(value), null, mp)
			default:
				return internalErrorf("unsupported MatrixOne integer type")
			}
		case arrowTypeFloatingPoint:
			if types.T(field.expected.Id) == types.T_float32 {
				err = vector.AppendFixed(vec, math.Float32frombits(binary.LittleEndian.Uint32(values[offset:])), null, mp)
			} else {
				err = vector.AppendFixed(vec, math.Float64frombits(binary.LittleEndian.Uint64(values[offset:])), null, mp)
			}
		case arrowTypeDate:
			err = vector.AppendFixed(vec, types.DaysFromUnixEpochToDate(int32(binary.LittleEndian.Uint32(values[offset:]))), null, mp)
		case arrowTypeDecimal:
			low := binary.LittleEndian.Uint64(values[offset:])
			high := binary.LittleEndian.Uint64(values[offset+8:])
			if types.T(field.expected.Id) == types.T_decimal64 {
				signExtension := uint64(0)
				if low>>63 != 0 {
					signExtension = math.MaxUint64
				}
				if !null && high != signExtension {
					return internalErrorf("decimal128 value does not fit MatrixOne decimal64")
				}
				err = vector.AppendFixed(vec, types.Decimal64(low), null, mp)
			} else {
				err = vector.AppendFixed(vec, types.Decimal128{B0_63: low, B64_127: high}, null, mp)
			}
		default:
			return internalErrorf("unsupported Arrow field type")
		}
		if err != nil {
			return err
		}
	}
	return nil
}

func bitmapBytes(rows int64) int64 {
	result := rows / 8
	if rows%8 != 0 {
		result++
	}
	return result
}

func sliceBuffer(body []byte, buffer arrowBuffer) []byte {
	return body[int(buffer.offset):int(buffer.offset+buffer.length)]
}

// ipcMetadata accepts both raw Message flatbuffers and stream-framed IPC
// metadata (continuation marker plus size, or the legacy size prefix).
func ipcMetadata(wire []byte) ([]byte, error) {
	if len(wire) < 4 {
		return nil, internalErrorf("Arrow IPC metadata is truncated")
	}
	if binary.LittleEndian.Uint32(wire[:4]) == math.MaxUint32 {
		if len(wire) < 8 {
			return nil, internalErrorf("Arrow IPC continuation header is truncated")
		}
		length := uint64(binary.LittleEndian.Uint32(wire[4:8]))
		if length == 0 || length > uint64(len(wire)-8) {
			return nil, internalErrorf("Arrow IPC metadata length is invalid")
		}
		return wire[8 : 8+length], nil
	}
	length := uint64(binary.LittleEndian.Uint32(wire[:4]))
	if length != 0 && length == uint64(len(wire)-4) {
		return wire[4:], nil
	}
	return wire, nil
}

type flatTable struct {
	data  []byte
	start uint64
}

func rootTable(data []byte) (flatTable, error) {
	if len(data) < 4 {
		return flatTable{}, internalErrorf("flatbuffer root is truncated")
	}
	start := uint64(binary.LittleEndian.Uint32(data[:4]))
	table := flatTable{data: data, start: start}
	if start < 4 {
		return flatTable{}, internalErrorf("flatbuffer root offset is invalid")
	}
	if _, _, err := table.vtable(); err != nil {
		return flatTable{}, err
	}
	return table, nil
}

func (t flatTable) vtable() (uint64, uint16, error) {
	if t.start+4 > uint64(len(t.data)) {
		return 0, 0, internalErrorf("flatbuffer table is outside metadata")
	}
	back := int64(int32(binary.LittleEndian.Uint32(t.data[t.start:])))
	vtablePosition := int64(t.start) - back
	if vtablePosition < 0 || uint64(vtablePosition) > uint64(len(t.data))-4 {
		return 0, 0, internalErrorf("flatbuffer vtable offset is invalid")
	}
	start := uint64(vtablePosition)
	length := binary.LittleEndian.Uint16(t.data[start:])
	objectLength := binary.LittleEndian.Uint16(t.data[start+2:])
	if length < 4 || start+uint64(length) > uint64(len(t.data)) || objectLength < 4 || t.start+uint64(objectLength) > uint64(len(t.data)) {
		return 0, 0, internalErrorf("flatbuffer vtable has invalid bounds")
	}
	return start, length, nil
}

func (t flatTable) field(index int, width uint64) (uint64, bool, error) {
	vtable, length, err := t.vtable()
	if err != nil {
		return 0, false, err
	}
	entry := uint64(4 + index*2)
	if entry+2 > uint64(length) {
		return 0, false, nil
	}
	offset := uint64(binary.LittleEndian.Uint16(t.data[vtable+entry:]))
	if offset == 0 {
		return 0, false, nil
	}
	objectLength := uint64(binary.LittleEndian.Uint16(t.data[vtable+2:]))
	if offset < 4 || offset > objectLength || width > objectLength-offset {
		return 0, false, internalErrorf("flatbuffer field is outside its table")
	}
	position := t.start + offset
	if position+width > uint64(len(t.data)) {
		return 0, false, internalErrorf("flatbuffer field is truncated")
	}
	return position, true, nil
}

func (t flatTable) byteField(index int, defaultValue byte) (byte, error) {
	position, ok, err := t.field(index, 1)
	if err != nil || !ok {
		return defaultValue, err
	}
	return t.data[position], nil
}

func (t flatTable) boolField(index int, defaultValue bool) (bool, error) {
	value, err := t.byteField(index, map[bool]byte{false: 0, true: 1}[defaultValue])
	if err != nil || value > 1 {
		return false, internalErrorf("flatbuffer boolean is invalid")
	}
	return value != 0, nil
}

func (t flatTable) int16Field(index int, defaultValue int16) (int16, error) {
	position, ok, err := t.field(index, 2)
	if err != nil || !ok {
		return defaultValue, err
	}
	return int16(binary.LittleEndian.Uint16(t.data[position:])), nil
}

func (t flatTable) int32Field(index int, defaultValue int32) (int32, error) {
	position, ok, err := t.field(index, 4)
	if err != nil || !ok {
		return defaultValue, err
	}
	return int32(binary.LittleEndian.Uint32(t.data[position:])), nil
}

func (t flatTable) int64Field(index int, defaultValue int64) (int64, error) {
	position, ok, err := t.field(index, 8)
	if err != nil || !ok {
		return defaultValue, err
	}
	return int64(binary.LittleEndian.Uint64(t.data[position:])), nil
}

func (t flatTable) indirect(position uint64) (uint64, error) {
	if position+4 > uint64(len(t.data)) {
		return 0, internalErrorf("flatbuffer offset is truncated")
	}
	offset := uint64(binary.LittleEndian.Uint32(t.data[position:]))
	if offset == 0 || offset > uint64(len(t.data))-position {
		return 0, internalErrorf("flatbuffer offset is invalid")
	}
	return position + offset, nil
}

func (t flatTable) tableField(index int) (flatTable, bool, error) {
	position, ok, err := t.field(index, 4)
	if err != nil || !ok {
		return flatTable{}, false, err
	}
	start, err := t.indirect(position)
	if err != nil {
		return flatTable{}, false, err
	}
	result := flatTable{data: t.data, start: start}
	if _, _, err = result.vtable(); err != nil {
		return flatTable{}, false, err
	}
	return result, true, nil
}

func (t flatTable) vector(index int, elementWidth uint64) (uint64, int, bool, error) {
	position, ok, err := t.field(index, 4)
	if err != nil || !ok {
		return 0, 0, ok, err
	}
	start, err := t.indirect(position)
	if err != nil || start+4 > uint64(len(t.data)) {
		return 0, 0, true, internalErrorf("flatbuffer vector is truncated")
	}
	length := uint64(binary.LittleEndian.Uint32(t.data[start:]))
	dataStart := start + 4
	if elementWidth != 0 && length > (uint64(len(t.data))-dataStart)/elementWidth {
		return 0, 0, true, internalErrorf("flatbuffer vector has invalid bounds")
	}
	if length > uint64(maxInt()) {
		return 0, 0, true, internalErrorf("flatbuffer vector is too large")
	}
	return dataStart, int(length), true, nil
}

func (t flatTable) stringField(index int) (string, bool, error) {
	start, length, ok, err := t.vector(index, 1)
	if err != nil || !ok {
		return "", ok, err
	}
	if start+uint64(length) >= uint64(len(t.data)) || t.data[start+uint64(length)] != 0 {
		return "", true, internalErrorf("flatbuffer string is not terminated")
	}
	return string(t.data[start : start+uint64(length)]), true, nil
}

func (t flatTable) tableVector(index int) ([]flatTable, error) {
	start, length, ok, err := t.vector(index, 4)
	if err != nil || !ok {
		return nil, err
	}
	result := make([]flatTable, length)
	for i := range result {
		element := start + uint64(i*4)
		tableStart, indirectErr := t.indirect(element)
		if indirectErr != nil {
			return nil, indirectErr
		}
		result[i] = flatTable{data: t.data, start: tableStart}
		if _, _, indirectErr = result[i].vtable(); indirectErr != nil {
			return nil, indirectErr
		}
	}
	return result, nil
}

func (t flatTable) structVector(index int, width uint64) ([]byte, int, error) {
	start, length, ok, err := t.vector(index, width)
	if err != nil || !ok {
		return nil, 0, err
	}
	return t.data[start : start+uint64(length)*width], length, nil
}
