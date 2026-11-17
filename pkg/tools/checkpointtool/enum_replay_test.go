// Copyright 2026 Matrix Origin
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

package checkpointtool

import (
	"context"
	"testing"
	"time"

	"github.com/matrixorigin/matrixone/pkg/catalog"
	"github.com/matrixorigin/matrixone/pkg/common/collation"
	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/pb/api"
	"github.com/stretchr/testify/require"
)

func TestCheckpointRestoreCollation(t *testing.T) {
	numeric := types.T_int32.ToType()
	base := &TableSchema{TableName: "t", DatabaseName: "d", SchemaExtra: &api.SchemaExtra{DefaultCharset: 3},
		Columns: []TableColumn{{Name: "id", SQLType: "INT", NativeType: &numeric}}}
	for _, tc := range []struct {
		name   string
		change func(*TableSchema)
		want   string
	}{
		{"general", func(s *TableSchema) {}, "utf8mb4_general_ci"},
		{"binary", func(s *TableSchema) { s.SchemaExtra.DefaultCharset = 2 }, "utf8mb4_bin"},
		{"unicode", func(s *TableSchema) { s.SchemaExtra.DefaultCharset = 10; s.SchemaExtra.CollationVersion = 1 }, "utf8mb4_unicode_ci"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			s := cloneTableSchema(base)
			tc.change(s)
			name, err := RestoreTableCollation(context.Background(), s, false)
			require.NoError(t, err)
			require.Equal(t, tc.want, name)
		})
	}
	for _, tc := range []struct {
		name   string
		change func(*TableSchema)
	}{
		{"missing defaults", func(s *TableSchema) { s.SchemaExtra = nil }},
		{"legacy default", func(s *TableSchema) { s.SchemaExtra.DefaultCharset = 0 }},
		{"unknown fields", func(s *TableSchema) { s.SchemaExtra.XXX_unrecognized = []byte{0xf8, 0x07, 0x01} }},
		{"unknown identity", func(s *TableSchema) { s.SchemaExtra.DefaultCharset = 255 }},
		{"marker without ENUM", func(s *TableSchema) { s.SchemaExtra.EnumUnsupportedDefault = "utf8mb4_0900_ai_ci" }},
		{"unknown native", func(s *TableSchema) { s.Columns[0].NativeType = nil }},
		{"default expression", func(s *TableSchema) { s.Columns[0].HasDefault = true; s.Columns[0].Default = "1" }},
		{"generated expression", func(s *TableSchema) { s.Columns[0].Generated = "1" }},
		{"format", func(s *TableSchema) { s.SchemaExtra.KeyFormat = 1 }},
		{"mismatched SQL type", func(s *TableSchema) { s.Columns[0].SQLType = "VARCHAR(9)" }},
		{"text default mismatch", func(s *TableSchema) {
			v := types.NewWithCharset(types.T_varchar, 9, 0, types.CharsetUTF8MB4Bin)
			s.Columns[0].NativeType = &v
			s.Columns[0].SQLType = sqlTypeFromMOType(v)
		}},
		{"textual ENUM", func(s *TableSchema) { s.Columns[0].NativeType = nil; s.Columns[0].SQLType = " enum\t('a')" }},
	} {
		t.Run(tc.name, func(t *testing.T) {
			s := cloneTableSchema(base)
			tc.change(s)
			_, err := RestoreTableCollation(context.Background(), s, false)
			require.Error(t, err)
		})
	}
	for _, identity := range []uint32{0, 2, 3, 10} {
		s := cloneTableSchema(base)
		typ := types.NewWithCharset(types.T_enum, 0, 0, uint8(identity))
		s.Columns[0].NativeType = &typ
		s.Columns[0].SQLType = "ENUM"
		_, err := RestoreTableCollation(context.Background(), s, false)
		require.ErrorContains(t, err, "ENUM identity")
	}
	s := cloneTableSchema(base)
	s.SchemaExtra.DefaultCharset = 0
	name, err := RestoreTableCollation(context.Background(), s, true)
	require.NoError(t, err)
	require.Empty(t, name, "read-only external cannot acquire ENUM by ALTER")
	s.SchemaExtra.EnumUnsupportedDefault = "utf8mb4_0900_ai_ci"
	_, err = RestoreTableCollation(context.Background(), s, true)
	require.Error(t, err)
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	_, err = RestoreTableCollation(ctx, base, false)
	require.ErrorIs(t, err, context.Canceled)
	ctx, cancel = context.WithDeadline(context.Background(), time.Now().Add(-time.Second))
	defer cancel()
	_, err = RestoreTableCollation(ctx, base, false)
	require.ErrorIs(t, err, context.DeadlineExceeded)
}

func TestCheckpointRestoreMetadataProvenance(t *testing.T) {
	extra, err := (&api.SchemaExtra{DefaultCharset: 3}).Marshal()
	require.NoError(t, err)
	typ := types.T_int32.ToType()
	columns := &LogicalTableView{
		Headers: []string{"object", "block", "row", catalog.SystemColAttr_RelID, catalog.SystemColAttr_Name, catalog.SystemColAttr_Type, "attnum", "attr_seqnum", "att_is_hidden"},
		Rows:    [][]string{{"o", "0", "0", "42", "id", encodedSQLType(t, typ), "1", "0", "0"}},
	}
	tables := &LogicalTableView{
		Headers: []string{"object", "block", "row", "rel_id", "relname", "reldatabase", "extra_info"},
		Rows:    [][]string{{"o", "0", "0", "42", "t", "d", string(extra)}},
	}
	schema := buildTableSchemaFromCatalogViews(42, tables, columns)
	name, err := RestoreTableCollation(context.Background(), schema, false)
	require.NoError(t, err)
	require.Equal(t, "utf8mb4_general_ci", name)
	grouped := buildTableSchemasForIDs([]uint64{42}, tables, columns)[42]
	require.NotNil(t, grouped.SchemaExtra)
	require.Equal(t, uint32(3), grouped.SchemaExtra.DefaultCharset)
	clone := cloneTableSchema(grouped)
	clone.SchemaExtra.DefaultCharset = 2
	clone.Columns[0].NativeType.Oid = types.T_int64
	require.Equal(t, uint32(3), grouped.SchemaExtra.DefaultCharset)
	require.Equal(t, types.T_int32, grouped.Columns[0].NativeType.Oid)

	for _, raw := range []string{"", "NULL", "\xff", "\x80"} {
		copyView := *tables
		copyView.Rows = [][]string{{"o", "0", "0", "42", "t", "d", raw}}
		s := buildTableSchemaFromCatalogViews(42, &copyView, columns)
		_, err = RestoreTableCollation(context.Background(), s, false)
		require.Error(t, err)
	}
	for _, headers := range [][]string{
		{"object", "block", "row", "rel_id", "relname", "reldatabase"},
		{"object", "block", "row", "rel_id", "relname", "reldatabase", "col_6"},
		{"object", "block", "row", "rel_id", "relname", "reldatabase", "extra_info", "extra_info"},
		{"object", "block", "row", "rel_id", "relname", "reldatabase", "extra_info", "rel_id"},
	} {
		copyView := *tables
		copyView.Headers = headers
		s := buildTableSchemaFromCatalogViews(42, &copyView, columns)
		_, err = RestoreTableCollation(context.Background(), s, false)
		require.Error(t, err)
	}
	duplicate := *columns
	duplicate.Headers = append(append([]string(nil), columns.Headers...), catalog.SystemColAttr_Type)
	s := buildTableSchemaFromCatalogViews(42, tables, &duplicate)
	_, err = RestoreTableCollation(context.Background(), s, false)
	require.Error(t, err)
	short := *tables
	short.Rows = [][]string{{"o", "0", "0", "42", "t", "d"}}
	s = buildTableSchemaFromCatalogViews(42, &short, columns)
	_, err = RestoreTableCollation(context.Background(), s, false)
	require.Error(t, err)

	// Native ENUM provenance survives decoding and cloning even though SQL-script
	// export deliberately refuses it; raw CSV keeps using this same schema.
	enum := types.NewWithCharset(types.T_enum, 0, 0, uint8(types.EnumCollationUTF8Bin))
	enumColumns := *columns
	enumColumns.Rows = [][]string{{"o", "0", "0", "42", "e", encodedSQLType(t, enum), "1", "0", "0"}}
	s = cloneTableSchema(buildTableSchemaFromCatalogViews(42, tables, &enumColumns))
	require.NotNil(t, s.Columns[0].NativeType)
	require.Equal(t, types.EnumCollationUTF8Bin, s.Columns[0].NativeType.GetEnumCollation())
	_, err = RestoreTableCollation(context.Background(), s, false)
	require.ErrorContains(t, err, "ENUM identity")

	text := types.NewWithCharset(types.T_varchar, 9, 0, types.CharsetUTF8)
	s = cloneTableSchema(schema)
	s.Columns[0].NativeType = &text
	s.Columns[0].SQLType = sqlTypeFromMOType(text)
	_, err = RestoreTableCollation(context.Background(), s, false)
	require.NoError(t, err)
	s.SchemaExtra.CollationVersion = uint32(collation.RevisionV1)
	_, err = RestoreTableCollation(context.Background(), s, false)
	require.Error(t, err)
}
