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

package checkpointtool

import (
	"context"
	"fmt"
	"strconv"
	"testing"

	"github.com/matrixorigin/matrixone/pkg/catalog"
	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/pb/api"
	"github.com/stretchr/testify/require"
)

func enumReplayViews(t *testing.T, collation uint32, members []string, extra *api.SchemaExtra) (*LogicalTableView, *LogicalTableView) {
	t.Helper()
	typ := types.T_enum.ToType()
	payload, encoding, err := types.EncodeEnumMembers(members)
	require.NoError(t, err)
	typ.SetEnumEncoding(encoding)
	typ.SetEnumCollation(collation)
	table := moTablesCatalogTestRow(t, "101", "colors", "app", "10", "r", "0", "")
	if extra != nil {
		raw, marshalErr := extra.Marshal()
		require.NoError(t, marshalErr)
		setCatalogCell(t, catalog.MoTablesSchema, table[logicalViewMetaCols:], catalog.SystemRelAttr_ExtraInfo, string(raw))
	}
	tables := &LogicalTableView{Headers: append([]string{"object", "block", "row"}, catalog.MoTablesSchema...), Rows: [][]string{table}, NullsKnown: true, RowNulls: [][]uint64{make([]uint64, (len(catalog.MoTablesSchema)+63)/64)}}
	columns := &LogicalTableView{Headers: append([]string{"object", "block", "row"}, catalog.MoColumnsSchema...), Rows: [][]string{
		moColumnsTestRow(t, "101", "colors", "choice", encodedSQLType(t, typ), "1", "0", "0", catalog.SystemColAttr_HasExpr, "1", catalog.SystemColAttr_DefaultExpr, encodedDefault(t, "", true), catalog.SystemColAttr_EnumValues, payload),
	}, NullsKnown: true, RowNulls: [][]uint64{make([]uint64, (len(catalog.MoColumnsSchema)+63)/64)}}
	return tables, columns
}

func TestCheckpointEnumCollationCatalogToDDL(t *testing.T) {
	for _, tc := range []struct {
		name       string
		collation  uint32
		members    []string
		extra      api.SchemaExtra
		wantColumn string
		wantTable  string
	}{
		{"binary column and CI table", types.EnumCollationUTF8Bin, []string{"A", "a"}, api.SchemaExtra{DefaultCharset: types.EnumCollationGeneralCI}, "ENUM('A','a') CHARACTER SET utf8mb4 COLLATE utf8mb4_bin", " COLLATE=utf8mb4_general_ci"},
		{"framed CI column and binary table", types.EnumCollationGeneralCI, []string{"a,b", ""}, api.SchemaExtra{DefaultCharset: types.EnumCollationUTF8Bin}, "ENUM('a,b','') CHARACTER SET utf8mb4 COLLATE utf8mb4_general_ci", " COLLATE=utf8mb4_bin"},
		{"legacy column", types.EnumCollationLegacy, []string{"A", "a"}, api.SchemaExtra{}, "ENUM('A','a')", ""},
		{"raw empty member", types.EnumCollationUTF8Bin, []string{"A", "", "b"}, api.SchemaExtra{}, "ENUM('A','','b') CHARACTER SET utf8mb4 COLLATE utf8mb4_bin", ""},
		{"raw spaces and quote", types.EnumCollationUTF8Bin, []string{" leading", "trailing ", "quote's"}, api.SchemaExtra{}, "ENUM(' leading','trailing ','quote''s') CHARACTER SET utf8mb4 COLLATE utf8mb4_bin", ""},
		{"unsupported table default", types.EnumCollationUTF8Bin, []string{"A", "a"}, api.SchemaExtra{EnumUnsupportedDefault: "utf8mb4_unicode_ci"}, "ENUM('A','a') CHARACTER SET utf8mb4 COLLATE utf8mb4_bin", " COLLATE=`utf8mb4_unicode_ci`"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			tables, columns := enumReplayViews(t, tc.collation, tc.members, &tc.extra)
			schema := buildTableSchemaFromCatalogViews(101, tables, columns)
			require.Len(t, schema.Columns, 1)
			require.Equal(t, tc.collation, schema.Columns[0].EnumCollation)
			clone := cloneTableSchema(schema)
			require.NoError(t, ValidateEnumReplaySchema(clone))
			ddl := RenderCreateTableDDLFromSchema(clone)
			require.Contains(t, ddl, tc.wantColumn)
			if tc.wantTable != "" {
				require.Contains(t, ddl, tc.wantTable)
			}
			reader := &CheckpointReader{}
			reader.SetGetLogicalViewForTest(func(_ *CheckpointReader, tableID uint64) (*LogicalTableView, error) {
				switch tableID {
				case moTablesID:
					return tables, nil
				case moColumnsID:
					return columns, nil
				default:
					return nil, fmt.Errorf("no catalog fixture for table %d", tableID)
				}
			})
			shown, err := reader.ShowCreateTable(context.Background(), 101, types.BuildTS(1, 0))
			require.NoError(t, err)
			require.Equal(t, ddl, shown)
		})
	}
}

func TestCheckpointEnumDefaultWithoutEnumAndUnreplayableMetadata(t *testing.T) {
	for _, tc := range []struct {
		name   string
		schema TableSchema
		want   string
	}{
		{"binary table", TableSchema{DefaultCharset: types.EnumCollationUTF8Bin}, " COLLATE=utf8mb4_bin"},
		{"unsupported table", TableSchema{EnumUnsupportedDefault: "utf8mb4_unicode_ci"}, " COLLATE=`utf8mb4_unicode_ci`"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			tc.schema.TableName = "t"
			tc.schema.Columns = []TableColumn{{Name: "id", SQLType: "INT"}}
			require.Contains(t, RenderCreateTableDDLFromSchema(cloneTableSchema(&tc.schema)), tc.want)
		})
	}
	for _, schema := range []TableSchema{
		{DefaultCharset: 99},
		{DefaultCharset: types.EnumCollationUTF8Bin, EnumUnsupportedDefault: "utf8mb4_unicode_ci"},
		{EnumUnsupportedDefault: "utf8mb4_bin"},
		{DefaultCharset: types.EnumCollationUTF8Bin, Columns: []TableColumn{{Name: "e", SQLType: "ENUM", EnumValues: "A,a"}}},
		{EnumUnsupportedDefault: "utf8mb4_unicode_ci", Columns: []TableColumn{{Name: "e", SQLType: "ENUM", EnumValues: "A,a"}}},
		{Columns: []TableColumn{{Name: "e", SQLType: "ENUM", EnumValues: "A,a", EnumCollation: 99}}},
		{enumDefaultDecodeError: "malformed extra_info"},
	} {
		schema.TableName = "t"
		if len(schema.Columns) == 0 {
			schema.Columns = []TableColumn{{Name: "id", SQLType: "INT"}}
		}
		require.Error(t, ValidateEnumReplaySchema(&schema))
		require.Empty(t, RenderCreateTableDDLFromSchema(&schema))
	}
}

func TestCheckpointEnumMalformedExtraInfoFailsReplay(t *testing.T) {
	tables, columns := enumReplayViews(t, types.EnumCollationUTF8Bin, []string{"A", "a"}, nil)
	setCatalogCell(t, catalog.MoTablesSchema, tables.Rows[0][logicalViewMetaCols:], catalog.SystemRelAttr_ExtraInfo, "\xff")
	schema := buildTableSchemaFromCatalogViews(101, tables, columns)
	require.ErrorContains(t, ValidateEnumReplaySchema(schema), "invalid checkpoint table ENUM default")
	require.Empty(t, RenderCreateTableDDLFromSchema(schema))
}

func TestCheckpointEnumHistoricalAndUnknownTableLayouts(t *testing.T) {
	oldColumns := preCPKLayout.moTablesSchema
	oldRow := make([]string, len(oldColumns))
	setCatalogCell(t, oldColumns, oldRow, catalog.SystemRelAttr_ID, "101")
	setCatalogCell(t, oldColumns, oldRow, catalog.SystemRelAttr_Name, "old_table")
	oldView := &LogicalTableView{Headers: append([]string{"object", "block", "row"}, oldColumns...), Rows: [][]string{append([]string{"obj", "0", "1"}, oldRow...)}}
	require.Equal(t, -1, checkpointExtraInfoIndex(oldView))
	oldSchema := findTableSchemaFromMoTables(oldView, 101)
	require.NotNil(t, oldSchema)
	require.NoError(t, ValidateEnumReplaySchema(oldSchema))
	require.Zero(t, oldSchema.DefaultCharset)

	unknown := &LogicalTableView{
		Headers: []string{"object", "block", "row", catalog.SystemRelAttr_ID, catalog.SystemRelAttr_Name},
		Rows:    [][]string{{"obj", "0", "1", "101", "partial"}},
	}
	require.Equal(t, -2, checkpointExtraInfoIndex(unknown))
	schema := findTableSchemaFromMoTables(unknown, 101)
	require.NotNil(t, schema)
	require.ErrorContains(t, ValidateEnumReplaySchema(schema), "unrecognized mo_tables extra_info layout")
}

func TestCheckpointEnumLoadReplayMatchesLabelFirstLookup(t *testing.T) {
	domains := [][]string{
		{"A", "a"}, {"2", "x"}, {"1", "2"}, {"0", "x"},
		{"02", "x"}, {" 2", "2 "}, {"２", "x"}, {"", "2"},
		{"1", "1"}, {"x", "1"}, {"a,b", "2"},
	}
	for _, identity := range []uint32{types.EnumCollationLegacy, types.EnumCollationUTF8Bin, types.EnumCollationGeneralCI} {
		for _, domain := range domains {
			payload, encoding, err := types.EncodeEnumMembers(domain)
			require.NoError(t, err)
			schema := &TableSchema{Columns: []TableColumn{{Name: "e", SQLType: "ENUM", EnumValues: payload, EnumEncoding: encoding, EnumCollation: identity, EnumValuesAreRaw: true}}}
			ambiguous := false
			for ordinal := 0; ordinal <= len(domain); ordinal++ {
				matched, matchErr := types.EnumMembers(domain).ParseCollatedName(strconv.Itoa(ordinal), identity)
				require.NoError(t, matchErr)
				if matched != 0 && int(matched) != ordinal {
					ambiguous = true
				}
			}
			if ambiguous {
				require.ErrorContains(t, ValidateEnumLoadReplay(schema), "cannot replay checkpoint ENUM", "identity %d domain %q", identity, domain)
			} else {
				require.NoError(t, ValidateEnumLoadReplay(schema), "identity %d domain %q", identity, domain)
			}
		}
	}
	schema := &TableSchema{Columns: []TableColumn{{Name: "e", SQLType: "ENUM", EnumValues: "2,x", EnumValuesAreRaw: true, Generated: "1"}}}
	require.NoError(t, ValidateEnumLoadReplay(schema), "generated columns are not loaded from CSV")
	schema.Columns[0].Generated = ""
	schema.Columns[0].EnumValuesAreRaw = false
	require.ErrorContains(t, ValidateEnumLoadReplay(schema), "cannot verify checkpoint ENUM domain")
}

func TestCheckpointEnumLoadReplayPhysicalOrdinalRange(t *testing.T) {
	for _, identity := range []uint32{types.EnumCollationLegacy, types.EnumCollationUTF8Bin, types.EnumCollationGeneralCI} {
		for _, tc := range []struct {
			name        string
			members     []string
			wantOrdinal string
		}{
			{"out of domain", []string{"3", "x"}, "3"},
			{"largest uint16", []string{"65535", "x"}, "65535"},
			{"beyond uint16", []string{"65536", "x"}, ""},
			{"leading zero", []string{"03", "x"}, ""},
			{"first duplicate is safe", []string{"1", "1"}, ""},
			{"first duplicate differs", []string{"x", "1", "1"}, "1"},
			{"zero label", []string{"0", "x"}, "0"},
		} {
			t.Run(fmt.Sprintf("identity_%d_%s", identity, tc.name), func(t *testing.T) {
				payload, encoding, err := types.EncodeEnumMembers(tc.members)
				require.NoError(t, err)
				schema := &TableSchema{Columns: []TableColumn{{Name: "e", SQLType: "ENUM", EnumValues: payload, EnumEncoding: encoding, EnumCollation: identity, EnumValuesAreRaw: true}}}
				err = ValidateEnumLoadReplay(schema)
				if tc.wantOrdinal == "" {
					require.NoError(t, err)
				} else {
					require.ErrorContains(t, err, "decimal ordinal "+tc.wantOrdinal+" matches member")
				}
			})
		}
	}
	t.Run("first match state is per column", func(t *testing.T) {
		schema := &TableSchema{Columns: []TableColumn{
			{Name: "safe", SQLType: "ENUM", EnumValues: "1,x", EnumValuesAreRaw: true},
			{Name: "unsafe", SQLType: "ENUM", EnumValues: "x,1", EnumValuesAreRaw: true},
		}}
		require.ErrorContains(t, ValidateEnumLoadReplay(schema), `column "unsafe": decimal ordinal 1 matches member 2`)
	})
	t.Run("general ci full width digits", func(t *testing.T) {
		schema := &TableSchema{Columns: []TableColumn{{Name: "e", SQLType: "ENUM", EnumValues: "３,x", EnumCollation: types.EnumCollationGeneralCI, EnumValuesAreRaw: true}}}
		matched, err := types.EnumMembers{"３", "x"}.ParseCollatedName("3", types.EnumCollationGeneralCI)
		require.NoError(t, err)
		require.Zero(t, matched)
		require.NoError(t, ValidateEnumLoadReplay(schema))
	})
}

func TestCheckpointEnumDomainRequiresPresentNonNullCell(t *testing.T) {
	for _, tc := range []struct {
		name   string
		mutate func(*LogicalTableView, int)
	}{
		{"short row", func(view *LogicalTableView, idx int) { view.Rows[0] = view.Rows[0][:logicalViewDataOffset(view)+idx] }},
		{"missing named column", func(view *LogicalTableView, idx int) {
			at := logicalViewDataOffset(view) + idx
			view.Headers = append(view.Headers[:at], view.Headers[at+1:]...)
			view.Rows[0] = append(view.Rows[0][:at], view.Rows[0][at+1:]...)
		}},
		{"null cell", func(view *LogicalTableView, idx int) { view.RowNulls[0][idx/64] |= uint64(1) << uint(idx%64) }},
		{"unknown validity", func(view *LogicalTableView, _ int) { view.NullsKnown = false; view.RowNulls = nil }},
	} {
		t.Run(tc.name, func(t *testing.T) {
			tables, columns := enumReplayViews(t, types.EnumCollationUTF8Bin, []string{"A", "a"}, nil)
			idx := columns.columnDataIndex(catalog.SystemColAttr_EnumValues)
			require.GreaterOrEqual(t, idx, 0)
			tc.mutate(columns, idx)
			schema := buildTableSchemaFromCatalogViews(101, tables, columns)
			require.Len(t, schema.Columns, 1)
			require.ErrorContains(t, ValidateEnumReplaySchema(schema), "cannot verify checkpoint ENUM metadata")
			require.Empty(t, RenderCreateTableDDLFromSchema(schema))
		})
	}
	// A non-NULL empty payload is known metadata, but CREATE would reauthor
	// its legacy representation as framed and change planner behavior.
	tables, columns := enumReplayViews(t, types.EnumCollationUTF8Bin, []string{"A"}, nil)
	idx := columns.columnDataIndex(catalog.SystemColAttr_EnumValues)
	columns.Rows[0][logicalViewDataOffset(columns)+idx] = ""
	schema := buildTableSchemaFromCatalogViews(101, tables, columns)
	require.False(t, schema.Columns[0].enumDomainUnverified)
	require.ErrorContains(t, ValidateEnumReplaySchema(schema), "cannot replay legacy empty ENUM")
	require.Empty(t, RenderCreateTableDDLFromSchema(schema))
}

func TestCheckpointEnumDomainValidityFollowsBatchRows(t *testing.T) {
	_, columns := enumReplayViews(t, types.EnumCollationUTF8Bin, []string{"A", "a"}, nil)
	other := append([]string(nil), columns.Rows[0]...)
	id := columns.columnDataIndex(catalog.SystemColAttr_RelID)
	other[logicalViewDataOffset(columns)+id] = "202"
	columns.Rows = append(columns.Rows, other)
	bits := make([]uint64, len(columns.RowNulls[0]))
	idx := columns.columnDataIndex(catalog.SystemColAttr_EnumValues)
	bits[idx/64] |= uint64(1) << uint(idx%64)
	columns.RowNulls = append(columns.RowNulls, bits)
	groups := groupCatalogRowsByUintColumn(columns, catalog.SystemColAttr_RelID)
	good := buildColumnsFromMoColumnsRows(logicalViewWithRows(columns, groups[101]), 101)
	bad := buildColumnsFromMoColumnsRows(logicalViewWithRows(columns, groups[202]), 202)
	require.Len(t, good, 1)
	require.Len(t, bad, 1)
	require.False(t, good[0].enumDomainUnverified)
	require.True(t, bad[0].enumDomainUnverified)
}

func TestCheckpointTableDefaultAndViewDefinitionValidity(t *testing.T) {
	tables, columns := enumReplayViews(t, types.EnumCollationUTF8Bin, []string{"A", "a"}, nil)
	viewIdx := tables.columnDataIndex(catalog.SystemRelAttr_ViewDef)
	extraIdx := tables.columnDataIndex(catalog.SystemRelAttr_ExtraInfo)
	require.GreaterOrEqual(t, viewIdx, 0)
	require.GreaterOrEqual(t, extraIdx, 0)
	tables.Rows[0][logicalViewDataOffset(tables)+viewIdx] = `{"Stmt":"CREATE VIEW v AS SELECT 1","DefaultDatabase":"db"}`
	schema := buildTableSchemaFromCatalogViews(101, tables, columns)
	require.True(t, schema.ViewDefinitionKnown)
	require.Equal(t, schema.ViewDefinition, cloneTableSchema(schema).ViewDefinition)
	require.NoError(t, ValidateEnumReplaySchema(schema))
	tables.RowNulls[0][extraIdx/64] |= uint64(1) << uint(extraIdx%64)
	schema = buildTableSchemaFromCatalogViews(101, tables, columns)
	require.ErrorContains(t, ValidateEnumReplaySchema(schema), "missing or NULL extra_info")
	tables.RowNulls[0][extraIdx/64] &^= uint64(1) << uint(extraIdx%64)
	tables.RowNulls[0][viewIdx/64] |= uint64(1) << uint(viewIdx%64)
	schema = buildTableSchemaFromCatalogViews(101, tables, columns)
	require.False(t, schema.ViewDefinitionKnown)
}

func TestCheckpointCatalogDoesNotGuessPartialOrAmbiguousLayout(t *testing.T) {
	t.Run("partial named default cell", func(t *testing.T) {
		tables, columns := enumReplayViews(t, types.EnumCollationUTF8Bin, []string{"A", "a"}, nil)
		idx := columns.columnDataIndex(catalog.SystemColAttr_DefaultExpr)
		columns.Headers[logicalViewDataOffset(columns)+idx] = "unknown_default"
		schema := buildTableSchemaFromCatalogViews(101, tables, columns)
		require.Len(t, schema.Columns, 1)
		require.ErrorContains(t, ValidateEnumReplaySchema(schema), "cannot verify checkpoint ENUM default")
	})
	t.Run("partial named table extra", func(t *testing.T) {
		tables, _ := enumReplayViews(t, types.EnumCollationUTF8Bin, []string{"A"}, nil)
		idx := tables.columnDataIndex(catalog.SystemRelAttr_ExtraInfo)
		tables.Headers[logicalViewDataOffset(tables)+idx] = "unknown_extra"
		require.Equal(t, -2, checkpointExtraInfoIndex(tables))
	})
	t.Run("ambiguous positional grouping", func(t *testing.T) {
		found := false
		for width := 1; width <= len(catalog.MoColumnsSchema)+2; width++ {
			first, ambiguous := -2, false
			for _, match := range catalogLayoutMatches(width, moColumnsID) {
				if len(schemaForLayout(match.layout, moColumnsID))+match.offset != width {
					continue
				}
				idx := catalogColIndexForLayout(match.layout, moColumnsID, catalog.SystemColAttr_RelID, match.offset)
				if first != -2 && first != idx {
					ambiguous = true
				}
				first = idx
			}
			if !ambiguous {
				continue
			}
			headers := []string{"object", "block", "row"}
			for i := 0; i < width; i++ {
				headers = append(headers, fmt.Sprintf("col_%d", i))
			}
			row := make([]string, len(headers))
			for i := logicalViewMetaCols; i < len(row); i++ {
				row[i] = "101"
			}
			view := &LogicalTableView{Headers: headers, Rows: [][]string{row}}
			require.Equal(t, -1, checkpointCatalogColumnIndex(view, moColumnsID, catalog.SystemColAttr_RelID))
			require.Empty(t, groupCatalogRowsByUintColumn(view, catalog.SystemColAttr_RelID))
			found = true
			break
		}
		require.True(t, found, "test must exercise two disagreeing exact historical layouts")
	})
}
