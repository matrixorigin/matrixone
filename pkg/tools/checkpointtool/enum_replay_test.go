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
	"fmt"
	"testing"
	"time"

	"github.com/matrixorigin/matrixone/pkg/catalog"
	"github.com/matrixorigin/matrixone/pkg/common/collation"
	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/pb/api"
	"github.com/matrixorigin/matrixone/pkg/pb/plan"
	"github.com/stretchr/testify/require"
)

func TestCheckpointRestoreCollation(t *testing.T) {
	numeric := types.T_int32.ToType()
	base := &TableSchema{TableName: "t", DatabaseName: "d", SchemaExtra: &api.SchemaExtra{DefaultCharset: 3},
		Columns: []TableColumn{{Name: "id", SQLType: "INT", NativeType: &numeric, expressionReplayProved: true}}}
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
		Headers: []string{"object", "block", "row", catalog.SystemColAttr_RelID, catalog.SystemColAttr_Name, catalog.SystemColAttr_Type, "attnum", "attr_seqnum", "att_is_hidden", catalog.SystemColAttr_NullAbility, catalog.SystemColAttr_HasExpr, catalog.SystemColAttr_DefaultExpr, catalog.SystemColAttr_HasUpdate, catalog.SystemColAttr_Update, catalog.SystemColAttr_HasGenerated, catalog.SystemColAttr_Generated},
		Rows:    [][]string{{"o", "0", "0", "42", "id", encodedSQLType(t, typ), "1", "0", "0", "0", "0", "", "0", "", "0", ""}},
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

func TestCheckpointImplicitNullableDefaultUsesTypedCatalogProof(t *testing.T) {
	implicit, err := (&plan.Default{NullAbility: true}).Marshal()
	require.NoError(t, err)
	extra, err := (&api.SchemaExtra{DefaultCharset: 2}).Marshal()
	require.NoError(t, err)
	tables := &LogicalTableView{
		Headers: []string{"object", "block", "row", "rel_id", "relname", "reldatabase", "extra_info"},
		Rows:    [][]string{{"o", "0", "0", "42", "nullable_table", "restore_db", string(extra)}},
	}
	set := func(view *LogicalTableView, name, value string) {
		t.Helper()
		for i, header := range view.Headers {
			if header == name {
				view.Rows[0][i] = value
				return
			}
		}
		t.Fatalf("fixture has no catalog column %s", name)
	}
	makeColumns := func() *LogicalTableView {
		v := &LogicalTableView{
			Headers: append([]string{"object", "block", "row"}, catalog.MoColumnsSchema...),
			Rows:    [][]string{make([]string, 3+len(catalog.MoColumnsSchema))},
		}
		set(v, catalog.SystemColAttr_RelID, "42")
		set(v, catalog.SystemColAttr_Name, "id")
		set(v, catalog.SystemColAttr_Type, encodedSQLType(t, types.T_int32.ToType()))
		set(v, catalog.SystemColAttr_Num, "1")
		set(v, catalog.SystemColAttr_NullAbility, "0")
		set(v, catalog.SystemColAttr_HasExpr, "1")
		set(v, catalog.SystemColAttr_DefaultExpr, string(implicit))
		set(v, catalog.SystemColAttr_HasUpdate, "0")
		set(v, catalog.SystemColAttr_HasGenerated, "0")
		return v
	}
	checkAccepted := func(schema *TableSchema) {
		t.Helper()
		require.NotNil(t, schema)
		require.Len(t, schema.Columns, 1)
		require.True(t, schema.Columns[0].HasDefault)
		require.True(t, schema.Columns[0].expressionReplayProved)
		name, err := RestoreTableCollation(t.Context(), schema, false)
		require.NoError(t, err)
		ddl := RenderCreateTableDDLFromSchemaWithCollation(schema, name)
		require.Contains(t, ddl, "DEFAULT NULL")
		require.Contains(t, ddl, "utf8mb4_bin")
	}
	v := makeColumns()
	checkAccepted(buildTableSchemaFromCatalogViews(42, tables, v))
	grouped := buildTableSchemasForIDs([]uint64{42}, tables, v)[42]
	checkAccepted(grouped)
	checkAccepted(cloneTableSchema(grouped))

	defaultBytes := func(def *plan.Default) string {
		t.Helper()
		data, err := def.Marshal()
		require.NoError(t, err)
		return string(data)
	}
	for _, tc := range []struct {
		name   string
		change func(*LogicalTableView)
	}{
		{"empty bytes", func(v *LogicalTableView) { set(v, catalog.SystemColAttr_DefaultExpr, "") }},
		{"catalog NULL", func(v *LogicalTableView) { set(v, catalog.SystemColAttr_DefaultExpr, "NULL") }},
		{"malformed default", func(v *LogicalTableView) { set(v, catalog.SystemColAttr_DefaultExpr, "\\xff") }},
		{"textual null", func(v *LogicalTableView) { set(v, catalog.SystemColAttr_DefaultExpr, "null") }},
		{"explicit null expression", func(v *LogicalTableView) {
			set(v, catalog.SystemColAttr_DefaultExpr, defaultBytes(&plan.Default{NullAbility: true, Expr: &plan.Expr{Expr: &plan.Expr_Lit{Lit: &plan.Literal{Isnull: true}}}}))
		}},
		{"unknown default field", func(v *LogicalTableView) {
			set(v, catalog.SystemColAttr_DefaultExpr, defaultBytes(&plan.Default{NullAbility: true, XXX_unrecognized: []byte{0xa0, 0x06, 0x01}}))
		}},
		{"default nullable disagreement", func(v *LogicalTableView) {
			set(v, catalog.SystemColAttr_DefaultExpr, defaultBytes(&plan.Default{}))
		}},
		{"not null column", func(v *LogicalTableView) { set(v, catalog.SystemColAttr_NullAbility, "1") }},
		{"unknown nullable flag", func(v *LogicalTableView) { set(v, catalog.SystemColAttr_NullAbility, "NULL") }},
		{"hidden on-update", func(v *LogicalTableView) { set(v, catalog.SystemColAttr_HasUpdate, "1") }},
		{"hidden generated", func(v *LogicalTableView) { set(v, catalog.SystemColAttr_HasGenerated, "1") }},
		{"invalid update flag", func(v *LogicalTableView) { set(v, catalog.SystemColAttr_HasUpdate, "2") }},
		{"missing update flag", func(v *LogicalTableView) {
			v.Headers[3+catalogColumnIndex(catalog.SystemColAttr_HasUpdate)] = "unknown_update"
		}},
		{"duplicate generated flag", func(v *LogicalTableView) {
			v.Headers = append(v.Headers, catalog.SystemColAttr_HasGenerated)
			v.Rows[0] = append(v.Rows[0], "0")
		}},
		{"shifted default slot", func(v *LogicalTableView) {
			i := 3 + catalogColumnIndex(catalog.SystemColAttr_DefaultExpr)
			j := 3 + catalogColumnIndex(catalog.SystemColAttr_HasUpdate)
			v.Headers[i], v.Headers[j] = v.Headers[j], v.Headers[i]
		}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			v := makeColumns()
			tc.change(v)
			schema := buildTableSchemaFromCatalogViews(42, tables, v)
			require.NotNil(t, schema)
			require.False(t, schema.Columns[0].expressionReplayProved)
			_, err := RestoreTableCollation(t.Context(), schema, false)
			require.Error(t, err)
		})
	}
	// Exercise the absent-default route too: an empty display cannot prove that
	// generated/on-update bindings were absent. External rendering shares this
	// proof instead of bypassing it.
	for _, hasDefault := range []bool{false, true} {
		for _, tc := range []struct {
			name   string
			change func(*LogicalTableView)
		}{
			{"valid", func(v *LogicalTableView) {}},
			{"opaque update", func(v *LogicalTableView) {
				set(v, catalog.SystemColAttr_HasUpdate, "1")
				set(v, catalog.SystemColAttr_Update, "\xff")
			}},
			{"empty-origin generated", func(v *LogicalTableView) {
				set(v, catalog.SystemColAttr_HasGenerated, "1")
				b, err := (&plan.GeneratedCol{Expr: &plan.Expr{Expr: &plan.Expr_Lit{Lit: &plan.Literal{}}}}).Marshal()
				require.NoError(t, err)
				set(v, catalog.SystemColAttr_Generated, string(b))
			}},
			{"unflagged update bytes", func(v *LogicalTableView) { set(v, catalog.SystemColAttr_Update, "\xff") }},
			{"unflagged generated bytes", func(v *LogicalTableView) { set(v, catalog.SystemColAttr_Generated, "\xff") }},
			{"invalid default flag", func(v *LogicalTableView) { set(v, catalog.SystemColAttr_HasExpr, "2") }},
			{"missing default flag", func(v *LogicalTableView) {
				v.Headers[3+catalogColumnIndex(catalog.SystemColAttr_HasExpr)] = "missing"
			}},
			{"duplicate nullable flag", func(v *LogicalTableView) {
				v.Headers = append(v.Headers, catalog.SystemColAttr_NullAbility)
				v.Rows[0] = append(v.Rows[0], "0")
			}},
			{"missing generated bytes", func(v *LogicalTableView) {
				v.Headers[3+catalogColumnIndex(catalog.SystemColAttr_Generated)] = "missing"
			}},
			{"duplicate update bytes", func(v *LogicalTableView) {
				v.Headers = append(v.Headers, catalog.SystemColAttr_Update)
				v.Rows[0] = append(v.Rows[0], "")
			}},
		} {
			t.Run(fmt.Sprintf("default=%t/%s", hasDefault, tc.name), func(t *testing.T) {
				v := makeColumns()
				if !hasDefault {
					set(v, catalog.SystemColAttr_HasExpr, "false")
					set(v, catalog.SystemColAttr_DefaultExpr, "")
				}
				tc.change(v)
				schema := cloneTableSchema(buildTableSchemasForIDs([]uint64{42}, tables, v)[42])
				for _, external := range []bool{false, true} {
					_, err := RestoreTableCollation(t.Context(), schema, external)
					if tc.name == "valid" {
						require.NoError(t, err)
					} else {
						require.ErrorContains(t, err, "stored expression replay")
					}
				}
			})
		}
	}
	v = makeColumns()
	set(v, catalog.SystemColAttr_HasExpr, "0")
	_, err = RestoreTableCollation(t.Context(), buildTableSchemaFromCatalogViews(42, tables, v), false)
	require.ErrorContains(t, err, "stored expression replay", "unflagged default bytes are contradictory")

}

func catalogColumnIndex(name string) int {
	for i, column := range catalog.MoColumnsSchema {
		if column == name {
			return i
		}
	}
	return -1
}
