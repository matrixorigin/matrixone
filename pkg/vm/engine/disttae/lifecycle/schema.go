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
	"encoding/json"
	"fmt"
	"strings"

	"github.com/matrixorigin/matrixone/pkg/common/moerr"
	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/pb/plan"
)

const schemaDescriptorFormatVersion uint16 = 1

type SchemaDescriptor struct {
	FormatVersion      uint16         `json:"format_version"`
	SourceTableID      uint64         `json:"source_table_id"`
	SourceTableVersion uint32         `json:"source_table_version"`
	SourceDatabaseName string         `json:"source_database_name"`
	SourceTableName    string         `json:"source_table_name"`
	Columns            []SchemaColumn `json:"columns"`
}

type SchemaColumn struct {
	Ordinal           uint32 `json:"ordinal"`
	SourceColumnID    uint64 `json:"source_column_id"`
	Name              string `json:"name"`
	TypeID            int32  `json:"type_id"`
	Width             int32  `json:"width"`
	Scale             int32  `json:"scale"`
	EnumValues        string `json:"enum_values,omitempty"`
	NotNull           bool   `json:"not_null"`
	AutoIncrement     bool   `json:"auto_increment"`
	DefaultExpression string `json:"default_expression,omitempty"`
}

func BuildSchemaDescriptor(
	ctx context.Context,
	table *plan.TableDef,
) (SchemaDescriptor, [32]byte, error) {
	if table == nil {
		return SchemaDescriptor{}, [32]byte{}, moerr.NewInvalidInput(ctx, "nil Lifecycle table schema")
	}
	descriptor := SchemaDescriptor{
		FormatVersion:      schemaDescriptorFormatVersion,
		SourceTableID:      table.TblId,
		SourceTableVersion: table.Version,
		SourceDatabaseName: table.DbName,
		SourceTableName:    table.Name,
		Columns:            make([]SchemaColumn, 0, len(table.Cols)),
	}
	for _, column := range table.Cols {
		if column == nil || column.Hidden {
			continue
		}
		oid := types.T(column.Typ.Id)
		// MO encodes ENUM directly and encodes SET/typed ARRAY semantics in
		// Enumvalues while reusing UINT64/JSON OIDs. Phase 1 cannot round-trip
		// those SQL types as independent restore-table DDL, so fail closed while
		// binding instead of silently restoring them as another type.
		if !isPhase1ArchiveColumnSupported(oid, column.Typ.Enumvalues) {
			return SchemaDescriptor{}, [32]byte{}, moerr.NewNotSupportedf(
				ctx,
				"Lifecycle archive column %s encoded SQL type %s",
				column.Name,
				oid,
			)
		}
		if column.GeneratedCol != nil {
			return SchemaDescriptor{}, [32]byte{}, moerr.NewNotSupportedf(
				ctx,
				"Lifecycle archive generated column %s",
				column.Name,
			)
		}
		if column.OnUpdate != nil {
			return SchemaDescriptor{}, [32]byte{}, moerr.NewNotSupportedf(
				ctx,
				"Lifecycle archive ON UPDATE column %s",
				column.Name,
			)
		}
		schemaColumn := SchemaColumn{
			Ordinal:        uint32(len(descriptor.Columns)),
			SourceColumnID: column.ColId,
			Name:           column.Name,
			TypeID:         column.Typ.Id,
			Width:          column.Typ.Width,
			Scale:          column.Typ.Scale,
			EnumValues:     column.Typ.Enumvalues,
			NotNull:        column.NotNull || column.Typ.NotNullable,
			AutoIncrement:  column.Typ.AutoIncr,
		}
		if column.Default != nil {
			schemaColumn.DefaultExpression = column.Default.OriginString
		}
		descriptor.Columns = append(descriptor.Columns, schemaColumn)
	}
	if len(descriptor.Columns) == 0 {
		return SchemaDescriptor{}, [32]byte{}, moerr.NewInvalidInput(
			ctx,
			"Lifecycle archive schema has no user columns",
		)
	}
	encoded, err := json.Marshal(descriptor)
	if err != nil {
		return SchemaDescriptor{}, [32]byte{}, err
	}
	return descriptor, sha256.Sum256(encoded), nil
}

func isPhase1ArchiveColumnSupported(oid types.T, enumValues string) bool {
	return oid != types.T_enum && enumValues == "" && isCanonicalTypeSupported(oid)
}

func (descriptor SchemaDescriptor) Digest() ([32]byte, error) {
	encoded, err := json.Marshal(descriptor)
	if err != nil {
		return [32]byte{}, err
	}
	return sha256.Sum256(encoded), nil
}

// BuildRestoreCreateTableSQL restores only the historical logical columns.
// SourceColumnID is lineage metadata and is deliberately not emitted: normal
// MO DDL allocates new IDs for the independent restore table.
func (descriptor SchemaDescriptor) BuildRestoreCreateTableSQL(
	ctx context.Context,
	databaseName string,
	tableName string,
) (string, error) {
	if descriptor.FormatVersion != schemaDescriptorFormatVersion {
		return "", moerr.NewNotSupportedf(
			ctx,
			"Lifecycle schema descriptor version %d",
			descriptor.FormatVersion,
		)
	}
	if len(descriptor.Columns) == 0 {
		return "", moerr.NewInvalidInput(ctx, "Lifecycle schema descriptor has no columns")
	}
	var sql strings.Builder
	sql.WriteString("create table ")
	sql.WriteString(quoteLifecycleIdentifier(databaseName))
	sql.WriteByte('.')
	sql.WriteString(quoteLifecycleIdentifier(tableName))
	sql.WriteString(" (")
	for index, column := range descriptor.Columns {
		if column.Ordinal != uint32(index) {
			return "", moerr.NewInvalidInput(ctx, "Lifecycle schema column ordinals are not continuous")
		}
		typeSQL, err := lifecycleColumnTypeSQL(column)
		if err != nil {
			return "", err
		}
		if index > 0 {
			sql.WriteByte(',')
		}
		sql.WriteString(quoteLifecycleIdentifier(column.Name))
		sql.WriteByte(' ')
		sql.WriteString(typeSQL)
		if column.NotNull {
			sql.WriteString(" NOT NULL")
		}
		if column.AutoIncrement {
			sql.WriteString(" AUTO_INCREMENT")
		}
		if column.DefaultExpression != "" {
			sql.WriteString(" DEFAULT ")
			sql.WriteString(column.DefaultExpression)
		}
	}
	sql.WriteByte(')')
	return sql.String(), nil
}

func lifecycleColumnTypeSQL(column SchemaColumn) (string, error) {
	oid := types.T(column.TypeID)
	switch oid {
	case types.T_bool:
		return "BOOL", nil
	case types.T_bit:
		width := column.Width
		if width <= 0 {
			width = 64
		}
		return fmt.Sprintf("BIT(%d)", width), nil
	case types.T_int8:
		return "TINYINT", nil
	case types.T_int16:
		return "SMALLINT", nil
	case types.T_int32:
		return "INT", nil
	case types.T_int64:
		return "BIGINT", nil
	case types.T_uint8:
		return "TINYINT UNSIGNED", nil
	case types.T_uint16:
		return "SMALLINT UNSIGNED", nil
	case types.T_uint32:
		return "INT UNSIGNED", nil
	case types.T_uint64:
		return "BIGINT UNSIGNED", nil
	case types.T_float32:
		return "FLOAT", nil
	case types.T_float64:
		return "DOUBLE", nil
	case types.T_char:
		return fmt.Sprintf("CHAR(%d)", max(column.Width, 1)), nil
	case types.T_varchar:
		return fmt.Sprintf("VARCHAR(%d)", max(column.Width, 1)), nil
	case types.T_binary:
		return fmt.Sprintf("BINARY(%d)", max(column.Width, 1)), nil
	case types.T_varbinary:
		return fmt.Sprintf("VARBINARY(%d)", max(column.Width, 1)), nil
	case types.T_blob:
		return "BLOB", nil
	case types.T_text:
		return "TEXT", nil
	case types.T_json:
		return "JSON", nil
	case types.T_date:
		return "DATE", nil
	case types.T_datetime:
		return temporalTypeSQL("DATETIME", column.Scale), nil
	case types.T_timestamp:
		return temporalTypeSQL("TIMESTAMP", column.Scale), nil
	case types.T_time:
		return temporalTypeSQL("TIME", column.Scale), nil
	case types.T_decimal64, types.T_decimal128, types.T_decimal256:
		width := column.Width
		if width <= 0 {
			width = 38
		}
		return fmt.Sprintf("DECIMAL(%d,%d)", width, column.Scale), nil
	case types.T_uuid:
		return "UUID", nil
	case types.T_enum:
		if column.EnumValues == "" {
			return "", fmt.Errorf("enum column %s has no values", column.Name)
		}
		return "ENUM(" + column.EnumValues + ")", nil
	default:
		return "", fmt.Errorf("unsupported Lifecycle restore type %s", oid)
	}
}

func temporalTypeSQL(name string, scale int32) string {
	if scale <= 0 {
		return name
	}
	return fmt.Sprintf("%s(%d)", name, scale)
}

func quoteLifecycleIdentifier(identifier string) string {
	return "`" + strings.ReplaceAll(identifier, "`", "``") + "`"
}
