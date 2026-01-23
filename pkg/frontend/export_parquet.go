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

	"github.com/matrixorigin/matrixone/pkg/common/moerr"
	"github.com/matrixorigin/matrixone/pkg/common/mpool"
	"github.com/matrixorigin/matrixone/pkg/container/batch"
	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/container/vector"
	"github.com/matrixorigin/matrixone/pkg/defines"
	"github.com/parquet-go/parquet-go"
)

// ParquetWriter handles writing data to Parquet format
type ParquetWriter struct {
	ctx           context.Context
	buf           *bytes.Buffer
	writer        *parquet.GenericWriter[any]
	schema        *parquet.Schema
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

	// Build parquet schema from column definitions using Group (map[string]Node)
	group := make(parquet.Group)
	for i, col := range mrs.Columns {
		columnNames[i] = col.Name()
		// Get the column type from MysqlColumn
		mysqlCol, ok := col.(*MysqlColumn)
		if !ok {
			return nil, moerr.NewInternalError(ctx, "invalid column type")
		}
		columnTypes[i] = mysqlCol.ColumnType()

		// Try to get parquet schema for this column
		var node parquet.Node
		dbName := mysqlCol.Schema()
		tableName := mysqlCol.OrgTable()
		columnName := mysqlCol.OrgName()

		if dbName != "" && tableName != "" && columnName != "" {
			key := dbName + "." + tableName
			if tableSchemas, ok := schemaMap[key]; ok {
				if schemaStr, ok := tableSchemas[columnName]; ok {
					complexNode, err := BuildComplexParquetNode(schemaStr)
					if err == nil {
						node = complexNode
						columnSchemas[i] = schemaStr // Store schema string
					}
				}
			}
		}

		if node == nil {
			node = buildParquetNode(columnTypes[i], mysqlCol.Flag())
		}
		group[columnNames[i]] = node
	}

	schema := parquet.NewSchema("export", group)
	buf := &bytes.Buffer{}
	writer := parquet.NewGenericWriter[any](buf, schema)

	return &ParquetWriter{
		ctx:           ctx,
		buf:           buf,
		writer:        writer,
		schema:        schema,
		columnNames:   columnNames,
		columnTypes:   columnTypes,
		columnSchemas: columnSchemas,
	}, nil
}

// buildParquetNode creates a parquet node from MySQL type
func buildParquetNode(typ defines.MysqlType, flag uint16) parquet.Node {
	isUnsigned := flag&uint16(defines.UNSIGNED_FLAG) != 0
	// All fields are optional (nullable) by default
	switch typ {
	case defines.MYSQL_TYPE_BOOL:
		return parquet.Optional(parquet.Leaf(parquet.BooleanType))
	case defines.MYSQL_TYPE_TINY, defines.MYSQL_TYPE_SHORT, defines.MYSQL_TYPE_INT24:
		return parquet.Optional(parquet.Leaf(parquet.Int32Type))
	case defines.MYSQL_TYPE_LONG:
		// For unsigned int32, use Int64 to avoid overflow
		if isUnsigned {
			return parquet.Optional(parquet.Leaf(parquet.Int64Type))
		}
		return parquet.Optional(parquet.Leaf(parquet.Int32Type))
	case defines.MYSQL_TYPE_BIT:
		return parquet.Optional(parquet.Leaf(parquet.Int64Type))
	case defines.MYSQL_TYPE_LONGLONG:
		return parquet.Optional(parquet.Leaf(parquet.Int64Type))
	case defines.MYSQL_TYPE_FLOAT:
		return parquet.Optional(parquet.Leaf(parquet.FloatType))
	case defines.MYSQL_TYPE_DOUBLE:
		return parquet.Optional(parquet.Leaf(parquet.DoubleType))
	case defines.MYSQL_TYPE_DATE, defines.MYSQL_TYPE_DATETIME, defines.MYSQL_TYPE_TIMESTAMP, defines.MYSQL_TYPE_TIME:
		// Use string representation for date/time types for simplicity and compatibility
		return parquet.Optional(parquet.String())
	case defines.MYSQL_TYPE_DECIMAL:
		// Use string representation for decimals
		return parquet.Optional(parquet.String())
	case defines.MYSQL_TYPE_VARCHAR, defines.MYSQL_TYPE_VAR_STRING, defines.MYSQL_TYPE_STRING,
		defines.MYSQL_TYPE_JSON, defines.MYSQL_TYPE_UUID, defines.MYSQL_TYPE_TEXT:
		return parquet.Optional(parquet.String())
	case defines.MYSQL_TYPE_BLOB:
		return parquet.Optional(parquet.Leaf(parquet.ByteArrayType))
	default:
		// Default to string for unknown types
		return parquet.Optional(parquet.String())
	}
}

// WriteBatch writes a batch of data to the parquet writer
func (pw *ParquetWriter) WriteBatch(bat *batch.Batch, mp *mpool.MPool, timeZone *time.Location) error {
	if bat == nil || bat.RowCount() == 0 {
		return nil
	}

	rows := make([]any, bat.RowCount())
	for i := 0; i < bat.RowCount(); i++ {
		row := make(map[string]any)
		rows[i] = row
	}

	// Convert each column
	for colIdx, vec := range bat.Vecs {
		colName := pw.columnNames[colIdx]
		schemaStr := pw.columnSchemas[colIdx]

		for rowIdx := 0; rowIdx < bat.RowCount(); rowIdx++ {
			row := rows[rowIdx].(map[string]any)
			if vec.GetNulls().Contains(uint64(rowIdx)) {
				row[colName] = nil
				continue
			}

			// If column has complex type schema, convert JSON to complex type
			if schemaStr != "" {
				val, err := vectorValueToComplexType(vec, rowIdx, schemaStr)
				if err != nil {
					return err
				}
				row[colName] = val
			} else {
				val, err := vectorValueToParquet(vec, rowIdx, timeZone)
				if err != nil {
					return err
				}
				row[colName] = val
			}
		}
	}

	_, err := pw.writer.Write(rows)
	return err
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
	if err := pw.writer.Close(); err != nil {
		return nil, err
	}
	return pw.buf.Bytes(), nil
}

// Reset resets the writer for a new file
func (pw *ParquetWriter) Reset() {
	pw.buf.Reset()
	pw.writer.Reset(pw.buf)
}

// Size returns the current buffer size in bytes
func (pw *ParquetWriter) Size() int {
	return pw.buf.Len()
}
