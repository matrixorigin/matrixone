// Copyright 2024 Matrix Origin
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

package external

import (
	"fmt"
	"strings"

	"github.com/matrixorigin/matrixone/pkg/catalog"
	"github.com/matrixorigin/matrixone/pkg/vm/process"
	"github.com/parquet-go/parquet-go"
)

// GenerateSchemaString generates schema string from parquet column
// Returns empty string if column is not a nested type
func GenerateSchemaString(col *parquet.Column) string {
	if col == nil || col.Leaf() {
		return ""
	}

	logicalType := col.Type().LogicalType()
	if logicalType != nil {
		if logicalType.List != nil {
			return generateListSchema(col)
		}
		if logicalType.Map != nil {
			return generateMapSchema(col)
		}
	}

	// Check for Parquet 3-level List pattern
	if isParquetListPattern(col) {
		return generateListSchema(col)
	}

	// Check for Parquet Map pattern
	if isParquetMapPattern(col) {
		return generateMapSchema(col)
	}

	// Default to struct
	return generateStructSchema(col)
}

// generateListSchema generates schema string for List type
func generateListSchema(col *parquet.Column) string {
	children := col.Columns()
	if len(children) == 0 {
		return "list<unknown>"
	}

	// List structure: group -> list (repeated) -> element
	listChild := children[0]
	var elementCol *parquet.Column

	if len(listChild.Columns()) > 0 {
		elementCol = listChild.Columns()[0]
	} else if listChild.Leaf() {
		// Simple repeated field
		return fmt.Sprintf("list<%s>", getBasicTypeString(listChild))
	}

	if elementCol == nil {
		return "list<unknown>"
	}

	if elementCol.Leaf() {
		return fmt.Sprintf("list<%s>", getBasicTypeString(elementCol))
	}

	// Nested element
	nestedSchema := GenerateSchemaString(elementCol)
	if nestedSchema == "" {
		return "list<unknown>"
	}
	return fmt.Sprintf("list<%s>", nestedSchema)
}

// generateMapSchema generates schema string for Map type
func generateMapSchema(col *parquet.Column) string {
	children := col.Columns()
	if len(children) == 0 {
		return "map<string,unknown>"
	}

	// Map structure: group -> key_value (repeated) -> key, value
	kvChild := children[0]
	kvChildren := kvChild.Columns()

	var keyType, valueType string = "string", "unknown"

	for _, child := range kvChildren {
		if child.Name() == "key" {
			if child.Leaf() {
				keyType = getBasicTypeString(child)
			}
		} else if child.Name() == "value" {
			if child.Leaf() {
				valueType = getBasicTypeString(child)
			} else {
				nestedSchema := GenerateSchemaString(child)
				if nestedSchema != "" {
					valueType = nestedSchema
				}
			}
		}
	}

	return fmt.Sprintf("map<%s,%s>", keyType, valueType)
}

// generateStructSchema generates schema string for Struct type
func generateStructSchema(col *parquet.Column) string {
	children := col.Columns()
	if len(children) == 0 {
		return "struct<>"
	}

	var fields []string
	for _, child := range children {
		fieldName := escapeFieldName(child.Name())
		var fieldType string

		if child.Leaf() {
			fieldType = getBasicTypeString(child)
		} else {
			nestedSchema := GenerateSchemaString(child)
			if nestedSchema != "" {
				fieldType = nestedSchema
			} else {
				fieldType = "unknown"
			}
		}

		fields = append(fields, fmt.Sprintf("%s:%s", fieldName, fieldType))
	}

	return fmt.Sprintf("struct<%s>", strings.Join(fields, ","))
}

// getBasicTypeString returns basic type string from parquet column
func getBasicTypeString(col *parquet.Column) string {
	if col == nil {
		return "unknown"
	}

	pt := col.Type()
	if pt == nil {
		return "unknown"
	}

	logicalType := pt.LogicalType()

	// Check physical type first, then refine with logical type.
	// This prevents Arrow-created parquet files from being misidentified
	// (e.g., Int32 with String logical type incorrectly becoming string).
	switch pt.Kind() {
	case parquet.Boolean:
		return "boolean"

	case parquet.Int32:
		// Check logical type for more specific integer types
		if logicalType != nil && logicalType.Integer != nil {
			bitWidth := logicalType.Integer.BitWidth
			isSigned := logicalType.Integer.IsSigned
			if isSigned {
				switch bitWidth {
				case 8:
					return "int8"
				case 16:
					return "int16"
				}
			} else {
				switch bitWidth {
				case 8:
					return "uint8"
				case 16:
					return "uint16"
				case 32:
					return "uint32"
				}
			}
		}
		if logicalType != nil {
			if logicalType.Date != nil {
				return "date"
			}
			if logicalType.Time != nil {
				return "time"
			}
			if logicalType.Decimal != nil {
				return fmt.Sprintf("decimal(%d,%d)",
					logicalType.Decimal.Precision,
					logicalType.Decimal.Scale)
			}
		}
		return "int32"

	case parquet.Int64:
		// Check logical type for more specific types
		if logicalType != nil && logicalType.Integer != nil {
			if !logicalType.Integer.IsSigned && logicalType.Integer.BitWidth == 64 {
				return "uint64"
			}
		}
		if logicalType != nil {
			if logicalType.Timestamp != nil {
				return "timestamp"
			}
			if logicalType.Time != nil {
				return "time"
			}
			if logicalType.Decimal != nil {
				return fmt.Sprintf("decimal(%d,%d)",
					logicalType.Decimal.Precision,
					logicalType.Decimal.Scale)
			}
		}
		return "int64"

	case parquet.Int96:
		return "int96"

	case parquet.Float:
		return "float"

	case parquet.Double:
		return "double"

	case parquet.ByteArray:
		// Only ByteArray should consider String logical type
		if logicalType != nil {
			if logicalType.String != nil {
				return "string"
			}
			if logicalType.Json != nil {
				return "json"
			}
			if logicalType.UUID != nil {
				return "uuid"
			}
		}
		return "binary"

	case parquet.FixedLenByteArray:
		if logicalType != nil {
			if logicalType.UUID != nil {
				return "uuid"
			}
			if logicalType.Decimal != nil {
				return fmt.Sprintf("decimal(%d,%d)",
					logicalType.Decimal.Precision,
					logicalType.Decimal.Scale)
			}
		}
		return fmt.Sprintf("fixed_len_byte_array(%d)", pt.Length())

	default:
		return "unknown"
	}
}

// escapeFieldName escapes field name if it contains special characters
func escapeFieldName(name string) string {
	if strings.ContainsAny(name, "<>,:`") {
		escaped := strings.ReplaceAll(name, "`", "``")
		return fmt.Sprintf("`%s`", escaped)
	}
	return name
}

// SaveParquetSchema saves parquet schema to mo_parquet_schema table
func SaveParquetSchema(
	proc *process.Process,
	dbName, tableName, columnName, schemaStr string,
) {
	if schemaStr == "" {
		return
	}

	// Escape single quotes in values
	dbName = strings.ReplaceAll(dbName, "'", "''")
	tableName = strings.ReplaceAll(tableName, "'", "''")
	columnName = strings.ReplaceAll(columnName, "'", "''")
	schemaStr = strings.ReplaceAll(schemaStr, "'", "''")

	sql := fmt.Sprintf(
		`INSERT IGNORE INTO %s.%s (database_name, table_name, column_name, parquet_schema, schema_version) VALUES ('%s','%s','%s','%s',1)`,
		catalog.MO_CATALOG, catalog.MO_PARQUET_SCHEMA,
		dbName, tableName, columnName, schemaStr,
	)

	proc.Base.PostDmlSqlList.Append(sql)
}

// saveNestedSchemas saves schema for all nested columns
func (h *ParquetHandler) saveNestedSchemas(param *ExternalParam, proc *process.Process) {
	if param.TableDef == nil {
		return
	}

	dbName := param.TableDef.DbName
	tableName := param.TableDef.Name
	if dbName == "" || tableName == "" {
		return
	}

	for _, attr := range param.Attrs {
		col := h.cols[attr.ColIndex]
		if col == nil || col.Leaf() {
			continue
		}

		schemaStr := GenerateSchemaString(col)
		if schemaStr != "" {
			SaveParquetSchema(proc, dbName, tableName, attr.ColName, schemaStr)
		}
	}
}
