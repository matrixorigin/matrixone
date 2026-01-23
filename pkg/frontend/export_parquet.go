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

// vectorValueToComplexType converts a JSON vector value to complex type structure
func vectorValueToComplexType(vec *vector.Vector, i int, schemaStr string) (any, error) {
	// Get JSON string from vector
	var jsonStr string
	switch vec.GetType().Oid {
	case types.T_json:
		val := types.DecodeJson(vec.GetBytesAt(i))
		jsonStr = val.String()
	case types.T_char, types.T_varchar, types.T_text:
		jsonStr = string(vec.GetBytesAt(i))
	default:
		return nil, moerr.NewInternalErrorf(context.Background(),
			"unsupported type for complex parquet export: %v", vec.GetType().Oid)
	}

	// Parse JSON and convert to appropriate Go type based on schema
	return convertJSONToParquetType(jsonStr, schemaStr)
}

// convertJSONToParquetType converts JSON string to Go type matching parquet schema
func convertJSONToParquetType(jsonStr, schemaStr string) (any, error) {
	schemaStr = strings.TrimSpace(schemaStr)

	if strings.HasPrefix(schemaStr, "list<") {
		return convertJSONToList(jsonStr, schemaStr)
	}
	if strings.HasPrefix(schemaStr, "map<") {
		return convertJSONToMap(jsonStr, schemaStr)
	}
	if strings.HasPrefix(schemaStr, "struct<") {
		return convertJSONToStruct(jsonStr, schemaStr)
	}

	// Basic type - just parse as-is
	var result any
	if err := json.Unmarshal([]byte(jsonStr), &result); err != nil {
		return nil, err
	}
	return result, nil
}

// convertJSONToList converts JSON array to Go slice
func convertJSONToList(jsonStr, schemaStr string) (any, error) {
	var arr []any
	if err := json.Unmarshal([]byte(jsonStr), &arr); err != nil {
		return nil, err
	}

	// Extract element type
	elementType := schemaStr[5 : len(schemaStr)-1] // remove "list<" and ">"

	// Convert elements based on element type
	switch strings.ToLower(strings.TrimSpace(elementType)) {
	case "string":
		result := make([]string, len(arr))
		for i, v := range arr {
			if v == nil {
				result[i] = ""
			} else if s, ok := v.(string); ok {
				result[i] = s
			} else {
				result[i] = fmt.Sprintf("%v", v)
			}
		}
		return result, nil
	case "int32", "int":
		result := make([]int32, len(arr))
		for i, v := range arr {
			if v == nil {
				result[i] = 0
			} else if f, ok := v.(float64); ok {
				result[i] = int32(f)
			}
		}
		return result, nil
	case "int64":
		result := make([]int64, len(arr))
		for i, v := range arr {
			if v == nil {
				result[i] = 0
			} else if f, ok := v.(float64); ok {
				result[i] = int64(f)
			}
		}
		return result, nil
	case "float", "float32":
		result := make([]float32, len(arr))
		for i, v := range arr {
			if v == nil {
				result[i] = 0
			} else if f, ok := v.(float64); ok {
				result[i] = float32(f)
			}
		}
		return result, nil
	case "double", "float64":
		result := make([]float64, len(arr))
		for i, v := range arr {
			if v == nil {
				result[i] = 0
			} else if f, ok := v.(float64); ok {
				result[i] = f
			}
		}
		return result, nil
	default:
		// For complex nested types, return as-is
		return arr, nil
	}
}

// convertJSONToMap converts JSON object to Go map
func convertJSONToMap(jsonStr, schemaStr string) (any, error) {
	var m map[string]any
	if err := json.Unmarshal([]byte(jsonStr), &m); err != nil {
		return nil, err
	}

	// Extract key and value types: map<keyType,valueType>
	inner := schemaStr[4 : len(schemaStr)-1]
	_, valueType := splitMapTypesForConvert(inner)

	// Convert map values based on value type
	switch strings.ToLower(strings.TrimSpace(valueType)) {
	case "string":
		result := make(map[string]string)
		for k, v := range m {
			if v == nil {
				result[k] = ""
			} else if s, ok := v.(string); ok {
				result[k] = s
			} else {
				result[k] = fmt.Sprintf("%v", v)
			}
		}
		return result, nil
	case "int32", "int":
		result := make(map[string]int32)
		for k, v := range m {
			if v == nil {
				result[k] = 0
			} else if f, ok := v.(float64); ok {
				result[k] = int32(f)
			}
		}
		return result, nil
	case "int64":
		result := make(map[string]int64)
		for k, v := range m {
			if v == nil {
				result[k] = 0
			} else if f, ok := v.(float64); ok {
				result[k] = int64(f)
			}
		}
		return result, nil
	default:
		// For complex nested types, return as-is
		return m, nil
	}
}

// convertJSONToStruct converts JSON object to Go map for struct
func convertJSONToStruct(jsonStr, schemaStr string) (any, error) {
	var m map[string]any
	if err := json.Unmarshal([]byte(jsonStr), &m); err != nil {
		return nil, err
	}

	// For struct, parquet-go expects map[string]any with correct value types
	// Convert values to appropriate types based on field types
	inner := schemaStr[7 : len(schemaStr)-1] // remove "struct<" and ">"
	fields := splitStructFieldsForConvert(inner)

	// Check if all fields are string type - if so, return map[string]string
	allString := true
	for _, field := range fields {
		_, typeStr := splitFieldNameTypeForConvert(field)
		if strings.ToLower(strings.TrimSpace(typeStr)) != "string" {
			allString = false
			break
		}
	}

	if allString {
		result := make(map[string]string)
		for _, field := range fields {
			name, _ := splitFieldNameTypeForConvert(field)
			if name == "" {
				continue
			}
			v, exists := m[name]
			if !exists || v == nil {
				result[name] = ""
				continue
			}
			if s, ok := v.(string); ok {
				result[name] = s
			} else {
				result[name] = fmt.Sprintf("%v", v)
			}
		}
		return result, nil
	}

	// Mixed types - return map[string]any with converted values
	result := make(map[string]any)
	for _, field := range fields {
		name, typeStr := splitFieldNameTypeForConvert(field)
		if name == "" {
			continue
		}
		v, exists := m[name]
		if !exists {
			continue
		}
		result[name] = convertValueToType(v, typeStr)
	}
	return result, nil
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

func convertValueToType(v any, typeStr string) any {
	if v == nil {
		return nil
	}
	typeStr = strings.ToLower(strings.TrimSpace(typeStr))
	switch typeStr {
	case "string":
		if s, ok := v.(string); ok {
			return s
		}
		return fmt.Sprintf("%v", v)
	case "int32", "int":
		if f, ok := v.(float64); ok {
			return int32(f)
		}
		return v
	case "int64":
		if f, ok := v.(float64); ok {
			return int64(f)
		}
		return v
	case "float", "float32":
		if f, ok := v.(float64); ok {
			return float32(f)
		}
		return v
	case "double", "float64":
		if f, ok := v.(float64); ok {
			return f
		}
		return v
	case "boolean", "bool":
		if b, ok := v.(bool); ok {
			return b
		}
		return v
	default:
		return v
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
	if err := json.Unmarshal([]byte(jsonStr), &arr); err != nil {
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
		appendBasicValue(valueBuilder, v, elementType)
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

	inner := schemaStr[4 : len(schemaStr)-1]
	_, valueType := splitMapTypesForConvert(inner)

	mapBuilder.Append(true)
	keyBuilder := mapBuilder.KeyBuilder()
	itemBuilder := mapBuilder.ItemBuilder()

	for k, v := range m {
		appendBasicValue(keyBuilder, k, "string")
		if v == nil {
			itemBuilder.AppendNull()
		} else {
			appendBasicValue(itemBuilder, v, valueType)
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
			appendBasicValue(fieldBuilder, v, typeStr)
		}
	}
	return nil
}

// appendBasicValue appends a basic value to the Arrow builder
func appendBasicValue(builder array.Builder, v any, typeStr string) {
	typeStr = strings.ToLower(strings.TrimSpace(typeStr))

	switch b := builder.(type) {
	case *array.BooleanBuilder:
		if bv, ok := v.(bool); ok {
			b.Append(bv)
		} else {
			b.AppendNull()
		}
	case *array.Int8Builder:
		if f, ok := v.(float64); ok {
			b.Append(int8(f))
		} else {
			b.AppendNull()
		}
	case *array.Int16Builder:
		if f, ok := v.(float64); ok {
			b.Append(int16(f))
		} else {
			b.AppendNull()
		}
	case *array.Int32Builder:
		if f, ok := v.(float64); ok {
			b.Append(int32(f))
		} else {
			b.AppendNull()
		}
	case *array.Int64Builder:
		if f, ok := v.(float64); ok {
			b.Append(int64(f))
		} else {
			b.AppendNull()
		}
	case *array.Uint8Builder:
		if f, ok := v.(float64); ok {
			b.Append(uint8(f))
		} else {
			b.AppendNull()
		}
	case *array.Uint16Builder:
		if f, ok := v.(float64); ok {
			b.Append(uint16(f))
		} else {
			b.AppendNull()
		}
	case *array.Uint32Builder:
		if f, ok := v.(float64); ok {
			b.Append(uint32(f))
		} else {
			b.AppendNull()
		}
	case *array.Uint64Builder:
		if f, ok := v.(float64); ok {
			b.Append(uint64(f))
		} else {
			b.AppendNull()
		}
	case *array.Float32Builder:
		if f, ok := v.(float64); ok {
			b.Append(float32(f))
		} else {
			b.AppendNull()
		}
	case *array.Float64Builder:
		if f, ok := v.(float64); ok {
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
