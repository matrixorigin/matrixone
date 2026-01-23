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

package frontend

import (
	"context"
	"fmt"
	"strings"

	"github.com/matrixorigin/matrixone/pkg/catalog"
	"github.com/parquet-go/parquet-go"
)

// QueryParquetSchemas queries parquet schemas for a table
// Returns map[columnName]schemaString
func QueryParquetSchemas(ctx context.Context, ses FeSession, dbName, tableName string) (map[string]string, error) {
	result := make(map[string]string)

	sql := fmt.Sprintf(
		`SELECT column_name, parquet_schema FROM %s.%s WHERE database_name = '%s' AND table_name = '%s'`,
		catalog.MO_CATALOG, catalog.MO_PARQUET_SCHEMA,
		strings.ReplaceAll(dbName, "'", "''"),
		strings.ReplaceAll(tableName, "'", "''"),
	)

	bh := ses.GetShareTxnBackgroundExec(ctx, false)
	defer bh.Close()

	bh.ClearExecResultSet()
	err := bh.Exec(ctx, sql)
	if err != nil {
		// Table might not exist yet, return empty result
		return result, nil
	}

	erArray, err := getResultSet(ctx, bh)
	if err != nil {
		return result, nil
	}

	if len(erArray) == 0 {
		return result, nil
	}

	mrs := erArray[0].(*MysqlResultSet)
	for _, row := range mrs.Data {
		if len(row) >= 2 {
			colName, ok1 := row[0].(string)
			schemaStr, ok2 := row[1].(string)
			if ok1 && ok2 {
				result[colName] = schemaStr
			}
		}
	}

	return result, nil
}

// BuildComplexParquetNode builds a parquet node from schema string
func BuildComplexParquetNode(schemaStr string) (parquet.Node, error) {
	schemaStr = strings.TrimSpace(schemaStr)

	if strings.HasPrefix(schemaStr, "list<") {
		return parseListSchema(schemaStr)
	}
	if strings.HasPrefix(schemaStr, "map<") {
		return parseMapSchema(schemaStr)
	}
	if strings.HasPrefix(schemaStr, "struct<") {
		return parseStructSchema(schemaStr)
	}

	// Basic type
	return parseBasicTypeNode(schemaStr), nil
}

// parseListSchema parses list<elementType> schema
func parseListSchema(schemaStr string) (parquet.Node, error) {
	// Extract element type: list<elementType>
	inner := schemaStr[5 : len(schemaStr)-1] // remove "list<" and ">"
	elementNode, err := BuildComplexParquetNode(inner)
	if err != nil {
		return nil, err
	}
	return parquet.List(elementNode), nil
}

// parseMapSchema parses map<keyType,valueType> schema
func parseMapSchema(schemaStr string) (parquet.Node, error) {
	// Extract key and value types: map<keyType,valueType>
	inner := schemaStr[4 : len(schemaStr)-1] // remove "map<" and ">"
	keyType, valueType := splitMapTypes(inner)

	keyNode, err := BuildComplexParquetNode(keyType)
	if err != nil {
		return nil, err
	}
	valueNode, err := BuildComplexParquetNode(valueType)
	if err != nil {
		return nil, err
	}
	return parquet.Map(keyNode, valueNode), nil
}

// parseStructSchema parses struct<field1:type1,field2:type2> schema
func parseStructSchema(schemaStr string) (parquet.Node, error) {
	// Extract fields: struct<field1:type1,field2:type2>
	inner := schemaStr[7 : len(schemaStr)-1] // remove "struct<" and ">"
	fields := splitStructFields(inner)

	group := make(parquet.Group)
	for _, field := range fields {
		name, typeStr := splitFieldNameType(field)
		if name == "" {
			continue
		}
		node, err := BuildComplexParquetNode(typeStr)
		if err != nil {
			return nil, err
		}
		group[name] = node
	}
	return group, nil
}

// splitMapTypes splits "keyType,valueType" considering nested types
func splitMapTypes(s string) (string, string) {
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

// splitStructFields splits struct fields considering nested types
func splitStructFields(s string) []string {
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

// splitFieldNameType splits "fieldName:fieldType"
func splitFieldNameType(field string) (string, string) {
	idx := strings.Index(field, ":")
	if idx < 0 {
		return field, "string"
	}
	return strings.TrimSpace(field[:idx]), strings.TrimSpace(field[idx+1:])
}

// parseBasicTypeNode creates a parquet node for basic types
func parseBasicTypeNode(typeStr string) parquet.Node {
	typeStr = strings.ToLower(strings.TrimSpace(typeStr))
	switch typeStr {
	case "boolean", "bool":
		return parquet.Optional(parquet.Leaf(parquet.BooleanType))
	case "int8", "int16", "int32":
		return parquet.Optional(parquet.Leaf(parquet.Int32Type))
	case "int64":
		return parquet.Optional(parquet.Leaf(parquet.Int64Type))
	case "uint8", "uint16", "uint32":
		return parquet.Optional(parquet.Leaf(parquet.Int32Type))
	case "uint64":
		return parquet.Optional(parquet.Leaf(parquet.Int64Type))
	case "float":
		return parquet.Optional(parquet.Leaf(parquet.FloatType))
	case "double":
		return parquet.Optional(parquet.Leaf(parquet.DoubleType))
	case "binary":
		return parquet.Optional(parquet.Leaf(parquet.ByteArrayType))
	default:
		// Default to string for unknown types
		return parquet.Optional(parquet.String())
	}
}

// collectTableColumns collects all tables and columns from result set
func collectTableColumns(mrs *MysqlResultSet) map[string]map[string]struct{} {
	result := make(map[string]map[string]struct{})
	for _, col := range mrs.Columns {
		mysqlCol, ok := col.(*MysqlColumn)
		if !ok {
			continue
		}
		dbName := mysqlCol.Schema()
		tableName := mysqlCol.OrgTable()
		if dbName != "" && tableName != "" {
			key := dbName + "." + tableName
			if result[key] == nil {
				result[key] = make(map[string]struct{})
			}
			result[key][mysqlCol.OrgName()] = struct{}{}
		}
	}
	return result
}

// splitDbTable splits "db.table" into db and table
func splitDbTable(key string) (string, string) {
	idx := strings.Index(key, ".")
	if idx < 0 {
		return "", key
	}
	return key[:idx], key[idx+1:]
}
