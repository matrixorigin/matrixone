// Copyright 2024 Matrix Origin
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
// http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package frontend

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"strconv"
	"strings"
	"time"

	"github.com/apache/arrow/go/v18/arrow"
	"github.com/apache/arrow/go/v18/arrow/array"
	"github.com/apache/arrow/go/v18/arrow/memory"
	"github.com/apache/arrow/go/v18/parquet"
	"github.com/apache/arrow/go/v18/parquet/pqarrow"
	"github.com/matrixorigin/matrixone/pkg/common/moerr"
	"github.com/matrixorigin/matrixone/pkg/common/mpool"
	"github.com/matrixorigin/matrixone/pkg/container/batch"
	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/container/vector"
	"github.com/matrixorigin/matrixone/pkg/defines"
)

// ParquetWriter handles writing data to Parquet format using Apache Arrow
type ParquetWriter struct {
	ctx           context.Context
	buf           *bytes.Buffer
	schema        *arrow.Schema
	pool          memory.Allocator
	pqWriter      *pqarrow.FileWriter
	columnNames   []string
	columnTypes   []defines.MysqlType
	columnSchemas []string // parquet schema strings for complex types
}

// NewParquetWriter creates a new ParquetWriter
func NewParquetWriter(ctx context.Context, ses FeSession, mrs *MysqlResultSet) (*ParquetWriter, error) {
	if mrs == nil || len(mrs.Columns) == 0 {
		return nil, moerr.NewInternalError(ctx, "no columns for parquet export")
	}

	// Collect tables and query schemas
	schemaMap := make(map[string]map[string]string) // db.table -> columnName -> schema
	if ses != nil {
		tableColumns := collectTableColumns(mrs)
		for key := range tableColumns {
			dbName, tableName := splitDbTable(key)
			schemas, err := QueryParquetSchemas(ctx, ses, dbName, tableName)
			if err == nil && len(schemas) > 0 {
				schemaMap[key] = schemas
			}
		}
	}

	columnNames := make([]string, len(mrs.Columns))
	columnTypes := make([]defines.MysqlType, len(mrs.Columns))
	columnSchemas := make([]string, len(mrs.Columns))

	// Build Arrow schema from column definitions
	arrowFields := make([]arrow.Field, len(mrs.Columns))
	for i, col := range mrs.Columns {
		columnNames[i] = col.Name()
		mysqlCol, ok := col.(*MysqlColumn)
		if !ok {
			return nil, moerr.NewInternalError(ctx, "invalid column type")
		}
		columnTypes[i] = mysqlCol.ColumnType()

		// Try to get parquet schema for this column (complex types)
		var arrowType arrow.DataType
		dbName := mysqlCol.Schema()
		tableName := mysqlCol.OrgTable()
		columnName := mysqlCol.OrgName()

		if dbName != "" && tableName != "" && columnName != "" {
			key := dbName + "." + tableName
			if tableSchemas, ok := schemaMap[key]; ok {
				if schemaStr, ok := tableSchemas[columnName]; ok {
					complexType, err := schemaStringToArrowType(schemaStr)
					if err == nil {
						arrowType = complexType
						columnSchemas[i] = schemaStr
					}
				}
			}
		}

		if arrowType == nil {
			arrowType = mysqlTypeToArrowType(columnTypes[i], mysqlCol.Flag())
		}
		arrowFields[i] = arrow.Field{Name: columnNames[i], Type: arrowType, Nullable: true}
	}

	schema := arrow.NewSchema(arrowFields, nil)
	pool := memory.NewGoAllocator()
	buf := &bytes.Buffer{}

	// Use Required root repetition for compatibility with parquet-go reader.
	// The default is Repeated, which causes "unexpected EOF" errors when reading.
	writerProps := parquet.NewWriterProperties(
		parquet.WithRootRepetition(parquet.Repetitions.Required),
	)
	pqWriter, err := pqarrow.NewFileWriter(schema, buf, writerProps, pqarrow.DefaultWriterProps())
	if err != nil {
		return nil, moerr.NewInternalErrorf(ctx, "failed to create parquet writer: %v", err)
	}

	return &ParquetWriter{
		ctx:           ctx,
		buf:           buf,
		schema:        schema,
		pool:          pool,
		pqWriter:      pqWriter,
		columnNames:   columnNames,
		columnTypes:   columnTypes,
		columnSchemas: columnSchemas,
	}, nil
}

// mysqlTypeToArrowType converts MySQL type to Arrow DataType
func mysqlTypeToArrowType(typ defines.MysqlType, flag uint16) arrow.DataType {
	isUnsigned := flag&uint16(defines.UNSIGNED_FLAG) != 0
	switch typ {
	case defines.MYSQL_TYPE_BOOL:
		return arrow.FixedWidthTypes.Boolean
	case defines.MYSQL_TYPE_TINY:
		if isUnsigned {
			return arrow.PrimitiveTypes.Uint8
		}
		return arrow.PrimitiveTypes.Int8
	case defines.MYSQL_TYPE_SHORT:
		if isUnsigned {
			return arrow.PrimitiveTypes.Uint16
		}
		return arrow.PrimitiveTypes.Int16
	case defines.MYSQL_TYPE_INT24, defines.MYSQL_TYPE_LONG:
		if isUnsigned {
			return arrow.PrimitiveTypes.Uint32
		}
		return arrow.PrimitiveTypes.Int32
	case defines.MYSQL_TYPE_LONGLONG:
		if isUnsigned {
			return arrow.PrimitiveTypes.Uint64
		}
		return arrow.PrimitiveTypes.Int64
	case defines.MYSQL_TYPE_BIT:
		return arrow.PrimitiveTypes.Uint64
	case defines.MYSQL_TYPE_FLOAT:
		return arrow.PrimitiveTypes.Float32
	case defines.MYSQL_TYPE_DOUBLE:
		return arrow.PrimitiveTypes.Float64
	case defines.MYSQL_TYPE_BLOB:
		return arrow.BinaryTypes.Binary
	default:
		// String for date/time, decimal, varchar, json, uuid, text, etc.
		return arrow.BinaryTypes.String
	}
}

// WriteBatch writes a batch of data to the parquet writer using Arrow
func (pw *ParquetWriter) WriteBatch(bat *batch.Batch, mp *mpool.MPool, timeZone *time.Location) error {
	if bat == nil || bat.RowCount() == 0 {
		return nil
	}

	// Create a RecordBuilder for this batch
	recordBuilder := array.NewRecordBuilder(pw.pool, pw.schema)
	defer recordBuilder.Release()

	// Convert each column to Arrow array
	for colIdx, vec := range bat.Vecs {
		schemaStr := pw.columnSchemas[colIdx]
		fieldBuilder := recordBuilder.Field(colIdx)

		if err := buildArrowColumn(fieldBuilder, vec, schemaStr, timeZone); err != nil {
			return err
		}
	}

	// Build the record and write to parquet
	record := recordBuilder.NewRecord()
	defer record.Release()

	return pw.pqWriter.WriteBuffered(record)
}

// vectorValueToParquet converts a vector value to a parquet-compatible Go value
func vectorValueToParquet(vec *vector.Vector, i int, timeZone *time.Location) (any, error) {
	switch vec.GetType().Oid {
	case types.T_bool:
		return vector.GetFixedAtNoTypeCheck[bool](vec, i), nil
	case types.T_int8:
		return int32(vector.GetFixedAtNoTypeCheck[int8](vec, i)), nil
	case types.T_int16:
		return int32(vector.GetFixedAtNoTypeCheck[int16](vec, i)), nil
	case types.T_int32:
		return vector.GetFixedAtNoTypeCheck[int32](vec, i), nil
	case types.T_int64:
		return vector.GetFixedAtNoTypeCheck[int64](vec, i), nil
	case types.T_uint8:
		return int32(vector.GetFixedAtNoTypeCheck[uint8](vec, i)), nil
	case types.T_uint16:
		return int32(vector.GetFixedAtNoTypeCheck[uint16](vec, i)), nil
	case types.T_uint32:
		// Use int64 to avoid overflow (uint32 max > int32 max)
		return int64(vector.GetFixedAtNoTypeCheck[uint32](vec, i)), nil
	case types.T_uint64:
		return int64(vector.GetFixedAtNoTypeCheck[uint64](vec, i)), nil
	case types.T_bit:
		return int64(vector.GetFixedAtNoTypeCheck[uint64](vec, i)), nil
	case types.T_float32:
		return vector.GetFixedAtNoTypeCheck[float32](vec, i), nil
	case types.T_float64:
		return vector.GetFixedAtNoTypeCheck[float64](vec, i), nil
	case types.T_char, types.T_varchar, types.T_text:
		return string(vec.GetBytesAt(i)), nil
	case types.T_binary, types.T_varbinary, types.T_blob:
		return vec.GetBytesAt(i), nil
	case types.T_json:
		val := types.DecodeJson(vec.GetBytesAt(i))
		return val.String(), nil
	case types.T_date:
		val := vector.GetFixedAtNoTypeCheck[types.Date](vec, i)
		return val.String(), nil
	case types.T_datetime:
		scale := vec.GetType().Scale
		val := vector.GetFixedAtNoTypeCheck[types.Datetime](vec, i).String2(scale)
		return val, nil
	case types.T_time:
		scale := vec.GetType().Scale
		val := vector.GetFixedAtNoTypeCheck[types.Time](vec, i).String2(scale)
		return val, nil
	case types.T_timestamp:
		if timeZone == nil {
			timeZone = time.UTC
		}
		scale := vec.GetType().Scale
		val := vector.GetFixedAtNoTypeCheck[types.Timestamp](vec, i).String2(timeZone, scale)
		return val, nil
	case types.T_decimal64:
		scale := vec.GetType().Scale
		val := vector.GetFixedAtNoTypeCheck[types.Decimal64](vec, i).Format(scale)
		return val, nil
	case types.T_decimal128:
		scale := vec.GetType().Scale
		val := vector.GetFixedAtNoTypeCheck[types.Decimal128](vec, i).Format(scale)
		return val, nil
	case types.T_uuid:
		val := vector.GetFixedAtNoTypeCheck[types.Uuid](vec, i).String()
		return val, nil
	case types.T_enum:
		val := vector.GetFixedAtNoTypeCheck[types.Enum](vec, i).String()
		return val, nil
	default:
		return nil, moerr.NewInternalErrorf(context.Background(), "unsupported type for parquet export: %v", vec.GetType().Oid)
	}
}

// Close closes the parquet writer and returns the complete parquet data
func (pw *ParquetWriter) Close() ([]byte, error) {
	if err := pw.pqWriter.Close(); err != nil {
		return nil, err
	}
	return pw.buf.Bytes(), nil
}

// Reset resets the writer for a new file
func (pw *ParquetWriter) Reset() error {
	pw.buf.Reset()
	var err error
	pw.pqWriter, err = pqarrow.NewFileWriter(pw.schema, pw.buf, nil, pqarrow.DefaultWriterProps())
	return err
}

// Size returns the current buffer size in bytes
func (pw *ParquetWriter) Size() int {
	return pw.buf.Len()
}

// schemaStringToArrowType converts schema string to Arrow DataType
func schemaStringToArrowType(schemaStr string) (arrow.DataType, error) {
	schemaStr = strings.TrimSpace(schemaStr)

	if strings.HasPrefix(schemaStr, "list<") {
		inner := schemaStr[5 : len(schemaStr)-1]
		elemType, err := schemaStringToArrowType(inner)
		if err != nil {
			return nil, err
		}
		return arrow.ListOf(elemType), nil
	}

	if strings.HasPrefix(schemaStr, "map<") {
		inner := schemaStr[4 : len(schemaStr)-1]
		keyType, valueType := splitMapTypesForConvert(inner)
		keyArrow, err := schemaStringToArrowType(keyType)
		if err != nil {
			return nil, err
		}
		valueArrow, err := schemaStringToArrowType(valueType)
		if err != nil {
			return nil, err
		}
		return arrow.MapOf(keyArrow, valueArrow), nil
	}

	if strings.HasPrefix(schemaStr, "struct<") {
		inner := schemaStr[7 : len(schemaStr)-1]
		fields := splitStructFieldsForConvert(inner)
		arrowFields := make([]arrow.Field, 0, len(fields))
		for _, f := range fields {
			name, typeStr := splitFieldNameTypeForConvert(f)
			if name == "" {
				continue
			}
			fieldType, err := schemaStringToArrowType(typeStr)
			if err != nil {
				return nil, err
			}
			arrowFields = append(arrowFields, arrow.Field{
				Name: name, Type: fieldType, Nullable: true,
			})
		}
		return arrow.StructOf(arrowFields...), nil
	}

	// Basic types
	return basicTypeStringToArrow(schemaStr), nil
}

// basicTypeStringToArrow converts basic type string to Arrow DataType
func basicTypeStringToArrow(typeStr string) arrow.DataType {
	typeStr = strings.ToLower(strings.TrimSpace(typeStr))
	switch typeStr {
	case "boolean", "bool":
		return arrow.FixedWidthTypes.Boolean
	case "int8":
		return arrow.PrimitiveTypes.Int8
	case "int16":
		return arrow.PrimitiveTypes.Int16
	case "int32", "int":
		return arrow.PrimitiveTypes.Int32
	case "int64":
		return arrow.PrimitiveTypes.Int64
	case "uint8":
		return arrow.PrimitiveTypes.Uint8
	case "uint16":
		return arrow.PrimitiveTypes.Uint16
	case "uint32":
		return arrow.PrimitiveTypes.Uint32
	case "uint64":
		return arrow.PrimitiveTypes.Uint64
	case "float", "float32":
		return arrow.PrimitiveTypes.Float32
	case "double", "float64":
		return arrow.PrimitiveTypes.Float64
	case "binary":
		return arrow.BinaryTypes.Binary
	default:
		return arrow.BinaryTypes.String
	}
}

// buildArrowColumn converts MO Vector to Arrow array using the builder
func buildArrowColumn(builder array.Builder, vec *vector.Vector, schemaStr string, timeZone *time.Location) error {
	rowCount := vec.Length()

	// Handle complex types (list, map, struct)
	if schemaStr != "" {
		return buildComplexArrowColumn(builder, vec, schemaStr)
	}

	// Handle simple types
	return buildSimpleArrowColumn(builder, vec, timeZone, rowCount)
}

// buildSimpleArrowColumn builds Arrow array for simple types
func buildSimpleArrowColumn(builder array.Builder, vec *vector.Vector, timeZone *time.Location, rowCount int) error {
	for i := 0; i < rowCount; i++ {
		if vec.GetNulls().Contains(uint64(i)) {
			builder.AppendNull()
			continue
		}

		switch b := builder.(type) {
		case *array.BooleanBuilder:
			b.Append(vector.GetFixedAtNoTypeCheck[bool](vec, i))
		case *array.Int8Builder:
			b.Append(vector.GetFixedAtNoTypeCheck[int8](vec, i))
		case *array.Int16Builder:
			b.Append(vector.GetFixedAtNoTypeCheck[int16](vec, i))
		case *array.Int32Builder:
			b.Append(vector.GetFixedAtNoTypeCheck[int32](vec, i))
		case *array.Int64Builder:
			b.Append(vector.GetFixedAtNoTypeCheck[int64](vec, i))
		case *array.Uint8Builder:
			b.Append(vector.GetFixedAtNoTypeCheck[uint8](vec, i))
		case *array.Uint16Builder:
			b.Append(vector.GetFixedAtNoTypeCheck[uint16](vec, i))
		case *array.Uint32Builder:
			b.Append(vector.GetFixedAtNoTypeCheck[uint32](vec, i))
		case *array.Uint64Builder:
			b.Append(vector.GetFixedAtNoTypeCheck[uint64](vec, i))
		case *array.Float32Builder:
			b.Append(vector.GetFixedAtNoTypeCheck[float32](vec, i))
		case *array.Float64Builder:
			b.Append(vector.GetFixedAtNoTypeCheck[float64](vec, i))
		case *array.StringBuilder:
			val, err := vectorValueToString(vec, i, timeZone)
			if err != nil {
				return err
			}
			b.Append(val)
		case *array.BinaryBuilder:
			b.Append(vec.GetBytesAt(i))
		default:
			// Fallback to string
			val, err := vectorValueToString(vec, i, timeZone)
			if err != nil {
				return err
			}
			if sb, ok := builder.(*array.StringBuilder); ok {
				sb.Append(val)
			}
		}
	}
	return nil
}

// vectorValueToString converts vector value to string
func vectorValueToString(vec *vector.Vector, i int, timeZone *time.Location) (string, error) {
	switch vec.GetType().Oid {
	case types.T_bool:
		if vector.GetFixedAtNoTypeCheck[bool](vec, i) {
			return "true", nil
		}
		return "false", nil
	case types.T_char, types.T_varchar, types.T_text:
		return string(vec.GetBytesAt(i)), nil
	case types.T_json:
		val := types.DecodeJson(vec.GetBytesAt(i))
		return val.String(), nil
	case types.T_date:
		return vector.GetFixedAtNoTypeCheck[types.Date](vec, i).String(), nil
	case types.T_datetime:
		scale := vec.GetType().Scale
		return vector.GetFixedAtNoTypeCheck[types.Datetime](vec, i).String2(scale), nil
	case types.T_time:
		scale := vec.GetType().Scale
		return vector.GetFixedAtNoTypeCheck[types.Time](vec, i).String2(scale), nil
	case types.T_timestamp:
		if timeZone == nil {
			timeZone = time.UTC
		}
		scale := vec.GetType().Scale
		return vector.GetFixedAtNoTypeCheck[types.Timestamp](vec, i).String2(timeZone, scale), nil
	case types.T_decimal64:
		scale := vec.GetType().Scale
		return vector.GetFixedAtNoTypeCheck[types.Decimal64](vec, i).Format(scale), nil
	case types.T_decimal128:
		scale := vec.GetType().Scale
		return vector.GetFixedAtNoTypeCheck[types.Decimal128](vec, i).Format(scale), nil
	case types.T_uuid:
		return vector.GetFixedAtNoTypeCheck[types.Uuid](vec, i).String(), nil
	case types.T_enum:
		return vector.GetFixedAtNoTypeCheck[types.Enum](vec, i).String(), nil
	default:
		return fmt.Sprintf("%v", vec.GetBytesAt(i)), nil
	}
}

// buildComplexArrowColumn builds Arrow array for complex types (list, map, struct)
func buildComplexArrowColumn(builder array.Builder, vec *vector.Vector, schemaStr string) error {
	rowCount := vec.Length()

	for i := 0; i < rowCount; i++ {
		if vec.GetNulls().Contains(uint64(i)) {
			builder.AppendNull()
			continue
		}

		// Get JSON string from vector
		var jsonStr string
		switch vec.GetType().Oid {
		case types.T_json:
			val := types.DecodeJson(vec.GetBytesAt(i))
			jsonStr = val.String()
		case types.T_char, types.T_varchar, types.T_text:
			jsonStr = string(vec.GetBytesAt(i))
		default:
			builder.AppendNull()
			continue
		}

		// Build Arrow value based on schema type
		if err := appendComplexValue(builder, jsonStr, schemaStr); err != nil {
			return err
		}
	}
	return nil
}

// appendComplexValue appends a complex value to the Arrow builder
func appendComplexValue(builder array.Builder, jsonStr, schemaStr string) error {
	schemaStr = strings.TrimSpace(schemaStr)

	if strings.HasPrefix(schemaStr, "list<") {
		return appendListValue(builder, jsonStr, schemaStr)
	}
	if strings.HasPrefix(schemaStr, "map<") {
		return appendMapValue(builder, jsonStr, schemaStr)
	}
	if strings.HasPrefix(schemaStr, "struct<") {
		return appendStructValue(builder, jsonStr, schemaStr)
	}

	// Basic type - append as string
	if sb, ok := builder.(*array.StringBuilder); ok {
		sb.Append(jsonStr)
	}
	return nil
}

// appendListValue appends a list value to the Arrow ListBuilder
func appendListValue(builder array.Builder, jsonStr, schemaStr string) error {
	listBuilder, ok := builder.(*array.ListBuilder)
	if !ok {
		return moerr.NewInternalErrorf(context.Background(), "expected ListBuilder")
	}

	var arr []any
	if err := unmarshalJSONWithNumber(jsonStr, &arr); err != nil {
		return err
	}

	elementType := schemaStr[5 : len(schemaStr)-1] // remove "list<" and ">"
	listBuilder.Append(true)
	valueBuilder := listBuilder.ValueBuilder()

	for _, v := range arr {
		if v == nil {
			valueBuilder.AppendNull()
			continue
		}
		if err := appendValue(valueBuilder, v, elementType); err != nil {
			return err
		}
	}
	return nil
}

// appendMapValue appends a map value to the Arrow MapBuilder
func appendMapValue(builder array.Builder, jsonStr, schemaStr string) error {
	mapBuilder, ok := builder.(*array.MapBuilder)
	if !ok {
		return moerr.NewInternalErrorf(context.Background(), "expected MapBuilder")
	}

	var m map[string]any
	if err := json.Unmarshal([]byte(jsonStr), &m); err != nil {
		return err
	}

	// Handle empty map
	if len(m) == 0 {
		mapBuilder.Append(true)
		return nil
	}

	inner := schemaStr[4 : len(schemaStr)-1]
	keyType, valueType := splitMapTypesForConvert(inner)

	mapBuilder.Append(true)
	keyBuilder := mapBuilder.KeyBuilder()
	itemBuilder := mapBuilder.ItemBuilder()

	for k, v := range m {
		// Append key with proper type conversion
		if err := appendValue(keyBuilder, k, keyType); err != nil {
			return err
		}
		// Append value
		if v == nil {
			itemBuilder.AppendNull()
		} else {
			if err := appendValue(itemBuilder, v, valueType); err != nil {
				return err
			}
		}
	}
	return nil
}

// appendStructValue appends a struct value to the Arrow StructBuilder
func appendStructValue(builder array.Builder, jsonStr, schemaStr string) error {
	structBuilder, ok := builder.(*array.StructBuilder)
	if !ok {
		return moerr.NewInternalErrorf(context.Background(), "expected StructBuilder")
	}

	var m map[string]any
	if err := json.Unmarshal([]byte(jsonStr), &m); err != nil {
		return err
	}

	inner := schemaStr[7 : len(schemaStr)-1] // remove "struct<" and ">"
	fields := splitStructFieldsForConvert(inner)

	structBuilder.Append(true)
	for i, field := range fields {
		name, typeStr := splitFieldNameTypeForConvert(field)
		if name == "" {
			continue
		}
		fieldBuilder := structBuilder.FieldBuilder(i)
		v, exists := m[name]
		if !exists || v == nil {
			fieldBuilder.AppendNull()
		} else {
			if err := appendValue(fieldBuilder, v, typeStr); err != nil {
				return err
			}
		}
	}
	return nil
}

// appendValue appends a value to the Arrow builder, handling both basic and nested types
func appendValue(builder array.Builder, v any, typeStr string) error {
	typeStr = strings.TrimSpace(typeStr)

	// Check if this is a nested type that needs recursive handling
	if strings.HasPrefix(typeStr, "list<") || strings.HasPrefix(typeStr, "map<") || strings.HasPrefix(typeStr, "struct<") {
		// For nested types, we need to convert v back to JSON and recurse
		jsonBytes, err := json.Marshal(v)
		if err != nil {
			return err
		}
		return appendComplexValue(builder, string(jsonBytes), typeStr)
	}

	// Handle basic types
	appendBasicValue(builder, v, typeStr)
	return nil
}

// appendBasicValue appends a basic value to the Arrow builder
func appendBasicValue(builder array.Builder, v any, typeStr string) {
	switch b := builder.(type) {
	case *array.BooleanBuilder:
		if bv, ok := v.(bool); ok {
			b.Append(bv)
		} else {
			b.AppendNull()
		}
	case *array.Int8Builder:
		if f, ok := toFloat64(v); ok {
			b.Append(int8(f))
		} else if s, ok := v.(string); ok {
			// Handle string to int conversion for map keys
			if i, err := parseInt64(s); err == nil {
				b.Append(int8(i))
			} else {
				b.AppendNull()
			}
		} else {
			b.AppendNull()
		}
	case *array.Int16Builder:
		if f, ok := toFloat64(v); ok {
			b.Append(int16(f))
		} else if s, ok := v.(string); ok {
			if i, err := parseInt64(s); err == nil {
				b.Append(int16(i))
			} else {
				b.AppendNull()
			}
		} else {
			b.AppendNull()
		}
	case *array.Int32Builder:
		if f, ok := toFloat64(v); ok {
			b.Append(int32(f))
		} else if s, ok := v.(string); ok {
			if i, err := parseInt64(s); err == nil {
				b.Append(int32(i))
			} else {
				b.AppendNull()
			}
		} else {
			b.AppendNull()
		}
	case *array.Int64Builder:
		if i, ok := toInt64(v); ok {
			b.Append(i)
		} else if s, ok := v.(string); ok {
			if i, err := parseInt64(s); err == nil {
				b.Append(i)
			} else {
				b.AppendNull()
			}
		} else {
			b.AppendNull()
		}
	case *array.Uint8Builder:
		if f, ok := toFloat64(v); ok {
			b.Append(uint8(f))
		} else if s, ok := v.(string); ok {
			if i, err := parseUint64(s); err == nil {
				b.Append(uint8(i))
			} else {
				b.AppendNull()
			}
		} else {
			b.AppendNull()
		}
	case *array.Uint16Builder:
		if f, ok := toFloat64(v); ok {
			b.Append(uint16(f))
		} else if s, ok := v.(string); ok {
			if i, err := parseUint64(s); err == nil {
				b.Append(uint16(i))
			} else {
				b.AppendNull()
			}
		} else {
			b.AppendNull()
		}
	case *array.Uint32Builder:
		if f, ok := toFloat64(v); ok {
			b.Append(uint32(f))
		} else if s, ok := v.(string); ok {
			if i, err := parseUint64(s); err == nil {
				b.Append(uint32(i))
			} else {
				b.AppendNull()
			}
		} else {
			b.AppendNull()
		}
	case *array.Uint64Builder:
		if i, ok := toUint64(v); ok {
			b.Append(i)
		} else if s, ok := v.(string); ok {
			if i, err := parseUint64(s); err == nil {
				b.Append(i)
			} else {
				b.AppendNull()
			}
		} else {
			b.AppendNull()
		}
	case *array.Float32Builder:
		if f, ok := toFloat64(v); ok {
			b.Append(float32(f))
		} else {
			b.AppendNull()
		}
	case *array.Float64Builder:
		if f, ok := toFloat64(v); ok {
			b.Append(f)
		} else {
			b.AppendNull()
		}
	case *array.StringBuilder:
		if s, ok := v.(string); ok {
			b.Append(s)
		} else {
			b.Append(fmt.Sprintf("%v", v))
		}
	case *array.BinaryBuilder:
		if s, ok := v.(string); ok {
			b.Append([]byte(s))
		} else {
			b.AppendNull()
		}
	default:
		// For unknown builder types, try string
		if sb, ok := builder.(*array.StringBuilder); ok {
			sb.Append(fmt.Sprintf("%v", v))
		} else {
			builder.AppendNull()
		}
	}
}

// toFloat64 converts a value to float64, handling json.Number for precision
func toFloat64(v any) (float64, bool) {
	switch n := v.(type) {
	case float64:
		return n, true
	case float32:
		return float64(n), true
	case int:
		return float64(n), true
	case int64:
		return float64(n), true
	case json.Number:
		if f, err := n.Float64(); err == nil {
			return f, true
		}
	}
	return 0, false
}

// toInt64 converts a value to int64, handling json.Number for large integers
func toInt64(v any) (int64, bool) {
	switch n := v.(type) {
	case float64:
		return int64(n), true
	case int:
		return int64(n), true
	case int64:
		return n, true
	case json.Number:
		if i, err := n.Int64(); err == nil {
			return i, true
		}
	}
	return 0, false
}

// toUint64 converts a value to uint64, handling json.Number for large integers
func toUint64(v any) (uint64, bool) {
	switch n := v.(type) {
	case float64:
		return uint64(n), true
	case int:
		return uint64(n), true
	case int64:
		return uint64(n), true
	case uint64:
		return n, true
	case json.Number:
		if i, err := n.Int64(); err == nil {
			return uint64(i), true
		}
	}
	return 0, false
}

// parseInt64 parses a string to int64
func parseInt64(s string) (int64, error) {
	return strconv.ParseInt(s, 10, 64)
}

// parseUint64 parses a string to uint64
func parseUint64(s string) (uint64, error) {
	return strconv.ParseUint(s, 10, 64)
}

// Helper functions for type conversion
func splitMapTypesForConvert(s string) (string, string) {
	depth := 0
	for i, c := range s {
		switch c {
		case '<':
			depth++
		case '>':
			depth--
		case ',':
			if depth == 0 {
				return strings.TrimSpace(s[:i]), strings.TrimSpace(s[i+1:])
			}
		}
	}
	return s, "string"
}

func splitStructFieldsForConvert(s string) []string {
	var fields []string
	depth := 0
	start := 0
	for i, c := range s {
		switch c {
		case '<':
			depth++
		case '>':
			depth--
		case ',':
			if depth == 0 {
				fields = append(fields, strings.TrimSpace(s[start:i]))
				start = i + 1
			}
		}
	}
	if start < len(s) {
		fields = append(fields, strings.TrimSpace(s[start:]))
	}
	return fields
}

func splitFieldNameTypeForConvert(field string) (string, string) {
	idx := strings.Index(field, ":")
	if idx < 0 {
		return field, "string"
	}
	return strings.TrimSpace(field[:idx]), strings.TrimSpace(field[idx+1:])
}

// unmarshalJSONWithNumber unmarshals JSON using json.Number to preserve int64 precision
func unmarshalJSONWithNumber(jsonStr string, v any) error {
	dec := json.NewDecoder(strings.NewReader(jsonStr))
	dec.UseNumber()
	return dec.Decode(v)
}
