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
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/pb/plan"
)

func TestBuildSchemaDescriptorPreservesRestoreSemantics(t *testing.T) {
	table := &plan.TableDef{
		TblId:   42,
		Name:    "events",
		DbName:  "db",
		Version: 7,
		Cols: []*plan.ColDef{
			{
				ColId:   11,
				Name:    "id",
				NotNull: true,
				Typ: plan.Type{
					Id:          int32(types.T_uint64),
					NotNullable: true,
					AutoIncr:    true,
				},
			},
			{
				ColId: 12,
				Name:  "event_time",
				Typ: plan.Type{
					Id:    int32(types.T_timestamp),
					Scale: 6,
				},
				Default: &plan.Default{OriginString: "current_timestamp"},
			},
		},
	}

	descriptor, digest, err := BuildSchemaDescriptor(context.Background(), table)
	require.NoError(t, err)
	require.Equal(t, uint16(1), descriptor.FormatVersion)
	require.Equal(t, "events", descriptor.SourceTableName)
	require.Len(t, descriptor.Columns, 2)
	require.Equal(t, uint64(11), descriptor.Columns[0].SourceColumnID)
	require.True(t, descriptor.Columns[0].AutoIncrement)
	require.Equal(t, int32(6), descriptor.Columns[1].Scale)
	require.Equal(t, "current_timestamp", descriptor.Columns[1].DefaultExpression)
	require.NotEqual(t, [32]byte{}, digest)

	again, againDigest, err := BuildSchemaDescriptor(context.Background(), table)
	require.NoError(t, err)
	require.Equal(t, descriptor, again)
	require.Equal(t, digest, againDigest)
}

func TestBuildSchemaDescriptorRejectsUnsupportedColumn(t *testing.T) {
	table := &plan.TableDef{
		Name: "unsupported",
		Cols: []*plan.ColDef{{
			ColId: 1,
			Name:  "embedding",
			Typ:   plan.Type{Id: int32(types.T_array_float32)},
		}},
	}
	_, _, err := BuildSchemaDescriptor(context.Background(), table)
	require.Error(t, err)
}

func TestBuildSchemaDescriptorRejectsOnUpdateColumn(t *testing.T) {
	table := &plan.TableDef{
		Name: "on_update",
		Cols: []*plan.ColDef{{
			ColId:    1,
			Name:     "updated_at",
			Typ:      plan.Type{Id: int32(types.T_timestamp)},
			OnUpdate: &plan.OnUpdate{OriginString: "current_timestamp"},
		}},
	}
	_, _, err := BuildSchemaDescriptor(context.Background(), table)
	require.ErrorContains(t, err, "ON UPDATE")
}

func TestBuildSchemaDescriptorRejectsEncodedSQLTypes(t *testing.T) {
	for _, test := range []struct {
		name string
		typ  plan.Type
	}{
		{
			name: "enum",
			typ: plan.Type{
				Id:         int32(types.T_enum),
				Enumvalues: "red,green",
			},
		},
		{
			name: "set encoded as uint64",
			typ: plan.Type{
				Id:         int32(types.T_uint64),
				Enumvalues: "read,write",
			},
		},
		{
			name: "typed array encoded as json",
			typ: plan.Type{
				Id:         int32(types.T_json),
				Enumvalues: "array(varchar(20))",
			},
		},
	} {
		t.Run(test.name, func(t *testing.T) {
			table := &plan.TableDef{
				Name: "encoded_type",
				Cols: []*plan.ColDef{{
					ColId: 1,
					Name:  "value",
					Typ:   test.typ,
				}},
			}
			_, _, err := BuildSchemaDescriptor(context.Background(), table)
			require.ErrorContains(t, err, "encoded SQL type")
		})
	}
}

func TestSchemaDescriptorRestoreDDLDoesNotReuseSourceColumnIDs(t *testing.T) {
	descriptor := SchemaDescriptor{
		FormatVersion:   1,
		SourceTableName: "events",
		Columns: []SchemaColumn{
			{
				Ordinal:        0,
				SourceColumnID: 99,
				Name:           "id",
				TypeID:         int32(types.T_int64),
				NotNull:        true,
			},
		},
	}
	ddl, err := descriptor.BuildRestoreCreateTableSQL(
		context.Background(),
		"db",
		"__mo_lifecycle_restore_1",
	)
	require.NoError(t, err)
	require.Contains(t, ddl, "`id` BIGINT NOT NULL")
	require.NotContains(t, ddl, "99")
}

func TestLifecycleRestoreColumnTypeSQLCoversPhaseOneTypes(t *testing.T) {
	tests := []struct {
		name     string
		column   SchemaColumn
		expected string
	}{
		{"bool", SchemaColumn{TypeID: int32(types.T_bool)}, "BOOL"},
		{"bit default width", SchemaColumn{TypeID: int32(types.T_bit)}, "BIT(64)"},
		{"int8", SchemaColumn{TypeID: int32(types.T_int8)}, "TINYINT"},
		{"int16", SchemaColumn{TypeID: int32(types.T_int16)}, "SMALLINT"},
		{"int32", SchemaColumn{TypeID: int32(types.T_int32)}, "INT"},
		{"int64", SchemaColumn{TypeID: int32(types.T_int64)}, "BIGINT"},
		{"uint8", SchemaColumn{TypeID: int32(types.T_uint8)}, "TINYINT UNSIGNED"},
		{"uint16", SchemaColumn{TypeID: int32(types.T_uint16)}, "SMALLINT UNSIGNED"},
		{"uint32", SchemaColumn{TypeID: int32(types.T_uint32)}, "INT UNSIGNED"},
		{"uint64", SchemaColumn{TypeID: int32(types.T_uint64)}, "BIGINT UNSIGNED"},
		{"float32", SchemaColumn{TypeID: int32(types.T_float32)}, "FLOAT"},
		{"float64", SchemaColumn{TypeID: int32(types.T_float64)}, "DOUBLE"},
		{"char", SchemaColumn{TypeID: int32(types.T_char), Width: 12}, "CHAR(12)"},
		{"varchar minimum width", SchemaColumn{TypeID: int32(types.T_varchar)}, "VARCHAR(1)"},
		{"binary", SchemaColumn{TypeID: int32(types.T_binary), Width: 8}, "BINARY(8)"},
		{"varbinary", SchemaColumn{TypeID: int32(types.T_varbinary), Width: 16}, "VARBINARY(16)"},
		{"blob", SchemaColumn{TypeID: int32(types.T_blob)}, "BLOB"},
		{"text", SchemaColumn{TypeID: int32(types.T_text)}, "TEXT"},
		{"json", SchemaColumn{TypeID: int32(types.T_json)}, "JSON"},
		{"date", SchemaColumn{TypeID: int32(types.T_date)}, "DATE"},
		{"datetime", SchemaColumn{TypeID: int32(types.T_datetime), Scale: 6}, "DATETIME(6)"},
		{"timestamp no scale", SchemaColumn{TypeID: int32(types.T_timestamp)}, "TIMESTAMP"},
		{"time", SchemaColumn{TypeID: int32(types.T_time), Scale: 3}, "TIME(3)"},
		{"decimal default width", SchemaColumn{TypeID: int32(types.T_decimal128), Scale: 4}, "DECIMAL(38,4)"},
		{"uuid", SchemaColumn{TypeID: int32(types.T_uuid)}, "UUID"},
		{"enum", SchemaColumn{TypeID: int32(types.T_enum), EnumValues: "'red','green'"}, "ENUM('red','green')"},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			actual, err := lifecycleColumnTypeSQL(test.column)
			require.NoError(t, err)
			require.Equal(t, test.expected, actual)
		})
	}

	_, err := lifecycleColumnTypeSQL(SchemaColumn{
		Name:   "status",
		TypeID: int32(types.T_enum),
	})
	require.ErrorContains(t, err, "has no values")
	_, err = lifecycleColumnTypeSQL(SchemaColumn{TypeID: int32(types.T_array_float32)})
	require.ErrorContains(t, err, "unsupported")
}
