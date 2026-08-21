// Copyright 2026 Matrix Origin
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package plan

import (
	"context"
	"testing"

	"github.com/matrixorigin/matrixone/pkg/catalog"
	"github.com/matrixorigin/matrixone/pkg/container/types"
	planpb "github.com/matrixorigin/matrixone/pkg/pb/plan"
	"github.com/stretchr/testify/require"
)

func TestRecoverLegacyTinyTextFromCreateSQL(t *testing.T) {
	tableDef := &planpb.TableDef{
		TblId:     10,
		LogicalId: 10,
		DbName:    "upgrade_db",
		Name:      "legacy_t",
		TableType: catalog.SystemOrdinaryRel,
		Createsql: "CREATE TABLE `legacy_t` (`id` INT, `payload` TINYTEXT, `body` TEXT)",
		Cols: []*planpb.ColDef{
			{Name: "id", OriginName: "id", Seqnum: 0, Typ: planpb.Type{Id: int32(types.T_int32)}},
			{Name: "payload", OriginName: "payload", Seqnum: 1, Typ: planpb.Type{Id: int32(types.T_text)}},
			{Name: "body", OriginName: "body", Seqnum: 2, Typ: planpb.Type{Id: int32(types.T_text)}},
		},
	}
	originalLegacyColumn := tableDef.Cols[1]

	require.NoError(t, RecoverLegacyTinyTextFromCreateSQL(t.Context(), tableDef))
	require.Equal(t, int32(types.MaxTinyTextLen), tableDef.Cols[1].Typ.Width)
	require.Equal(t, int32(0), tableDef.Cols[2].Typ.Width)
	require.NotSame(t, originalLegacyColumn, tableDef.Cols[1])
	require.Equal(t, int32(0), originalLegacyColumn.Typ.Width)

	// Recovery is idempotent for definitions written by a fixed binary.
	require.NoError(t, RecoverLegacyTinyTextFromCreateSQL(t.Context(), tableDef))
	require.Equal(t, int32(types.MaxTinyTextLen), tableDef.Cols[1].Typ.Width)
}

func TestLegacyLikeColumnsTreatNoneAndTextLiteralFormsAsEquivalent(t *testing.T) {
	makeColumn := func(literalType types.Type, form planpb.StringLiteralForm) *planpb.ColDef {
		return &planpb.ColDef{
			Name: "payload", Typ: planpb.Type{Id: int32(types.T_text)},
			Default: &planpb.Default{Expr: &planpb.Expr{
				Typ: planpb.Type{Id: int32(literalType.Oid), Charset: uint32(literalType.Charset)},
				Expr: &planpb.Expr_Lit{Lit: &planpb.Literal{
					Value: &planpb.Literal_Sval{Sval: "x"}, LiteralForm: form,
				}},
			}},
		}
	}
	require.True(t, legacyLikeColumnsCompatible(
		makeColumn(types.T_varchar.ToType(), planpb.StringLiteralForm_STRING_LITERAL_NONE),
		makeColumn(types.T_varchar.ToType(), planpb.StringLiteralForm_STRING_LITERAL_TEXT)))
	require.False(t, legacyLikeColumnsCompatible(
		makeColumn(types.T_varbinary.ToType(), planpb.StringLiteralForm_STRING_LITERAL_NONE),
		makeColumn(types.T_varbinary.ToType(), planpb.StringLiteralForm_STRING_LITERAL_TEXT)))
}

func TestLegacyLikeColumnsTreatNoneAndTextLiteralFormsAsEquivalent(t *testing.T) {
	makeColumn := func(literalType types.Type, form planpb.StringLiteralForm) *planpb.ColDef {
		return &planpb.ColDef{
			Name: "payload", Typ: planpb.Type{Id: int32(types.T_text)},
			Default: &planpb.Default{Expr: &planpb.Expr{
				Typ: planpb.Type{Id: int32(literalType.Oid), Charset: uint32(literalType.Charset)},
				Expr: &planpb.Expr_Lit{Lit: &planpb.Literal{
					Value: &planpb.Literal_Sval{Sval: "x"}, LiteralForm: form,
				}},
			}},
		}
	}
	require.True(t, legacyLikeColumnsCompatible(
		makeColumn(types.T_varchar.ToType(), planpb.StringLiteralForm_STRING_LITERAL_NONE),
		makeColumn(types.T_varchar.ToType(), planpb.StringLiteralForm_STRING_LITERAL_TEXT)))
	require.False(t, legacyLikeColumnsCompatible(
		makeColumn(types.T_varbinary.ToType(), planpb.StringLiteralForm_STRING_LITERAL_NONE),
		makeColumn(types.T_varbinary.ToType(), planpb.StringLiteralForm_STRING_LITERAL_TEXT)))
}

func TestRecoverLegacyTinyTextDoesNotOverrideAlteredSchemas(t *testing.T) {
	for _, test := range []struct {
		name      string
		version   uint32
		tableID   uint64
		logicalID uint64
		column    *planpb.ColDef
	}{
		{
			name:      "tinytext modified to text",
			version:   0,
			tableID:   11,
			logicalID: 10,
			column:    &planpb.ColDef{Name: "payload", OriginName: "payload", Seqnum: 1, Typ: planpb.Type{Id: int32(types.T_text)}},
		},
		{
			name:      "tinytext dropped and text re-added",
			version:   0,
			tableID:   12,
			logicalID: 10,
			column:    &planpb.ColDef{Name: "payload", OriginName: "payload", Seqnum: 2, Typ: planpb.Type{Id: int32(types.T_text)}},
		},
		{
			name:      "catalog identity unavailable",
			version:   0,
			tableID:   0,
			logicalID: 0,
			column:    &planpb.ColDef{Name: "payload", OriginName: "payload", Seqnum: 1, Typ: planpb.Type{Id: int32(types.T_text)}},
		},
	} {
		t.Run(test.name, func(t *testing.T) {
			tableDef := &planpb.TableDef{
				TblId:     test.tableID,
				LogicalId: test.logicalID,
				DbName:    "upgrade_db",
				Name:      "altered_t",
				TableType: catalog.SystemOrdinaryRel,
				Createsql: "CREATE TABLE altered_t (id INT, payload TINYTEXT)",
				Version:   test.version,
				Cols: []*planpb.ColDef{
					{Name: "id", OriginName: "id", Seqnum: 0, Typ: planpb.Type{Id: int32(types.T_int32)}},
					test.column,
				},
			}

			require.NoError(t, RecoverLegacyTinyTextFromCreateSQL(t.Context(), tableDef))
			require.Zero(t, tableDef.Cols[1].Typ.Width)
		})
	}
}

func TestRecoverLegacyTinyTextPreservesInPlaceRename(t *testing.T) {
	tableDef := &planpb.TableDef{
		TblId:     10,
		LogicalId: 10,
		DbName:    "upgrade_db",
		Name:      "renamed_t",
		TableType: catalog.SystemOrdinaryRel,
		Createsql: "CREATE TABLE renamed_t (id INT, payload TINYTEXT)",
		Version:   1,
		Cols: []*planpb.ColDef{
			{Name: "id", OriginName: "id", Seqnum: 0, Typ: planpb.Type{Id: int32(types.T_int32)}},
			{Name: "renamed_payload", OriginName: "renamed_payload", Seqnum: 1, Typ: planpb.Type{Id: int32(types.T_text)}},
		},
	}

	require.NoError(t, RecoverLegacyTinyTextFromCreateSQL(t.Context(), tableDef))
	require.Equal(t, int32(types.MaxTinyTextLen), tableDef.Cols[1].Typ.Width)
}

func TestRecoverLegacyTinyTextRequiresStableSeqnum(t *testing.T) {
	tableDef := &planpb.TableDef{
		TblId:     10,
		LogicalId: 10,
		DbName:    "upgrade_db",
		Name:      "readded_t",
		TableType: catalog.SystemOrdinaryRel,
		Createsql: "CREATE TABLE readded_t (id INT, payload TINYTEXT)",
		Cols: []*planpb.ColDef{
			{Name: "id", OriginName: "id", Seqnum: 0, Typ: planpb.Type{Id: int32(types.T_int32)}},
			{Name: "payload", OriginName: "payload", Seqnum: 2, Typ: planpb.Type{Id: int32(types.T_text)}},
		},
	}

	require.NoError(t, RecoverLegacyTinyTextFromCreateSQL(t.Context(), tableDef))
	require.Zero(t, tableDef.Cols[1].Typ.Width)
}

func TestRecoverLegacyTinyTextFollowsCreateLikeLineage(t *testing.T) {
	legacyType := planpb.Type{Id: int32(types.T_text)}
	tableDef := &planpb.TableDef{
		TblId:     20,
		LogicalId: 20,
		DbName:    "upgrade_db",
		Name:      "legacy_clone",
		TableType: catalog.SystemOrdinaryRel,
		Createsql: "CREATE TABLE legacy_clone LIKE legacy_source",
		Cols: []*planpb.ColDef{
			{Name: "id", OriginName: "id", Seqnum: 0, Typ: planpb.Type{Id: int32(types.T_int32)}},
			{
				Name: "payload", OriginName: "payload", Seqnum: 1, Typ: legacyType,
				Default: &planpb.Default{
					NullAbility:  true,
					OriginString: "null",
					Expr: &planpb.Expr{
						Typ:  planpb.Type{Id: int32(types.T_text), Width: types.MaxTinyTextLen},
						Expr: &planpb.Expr_Lit{Lit: &planpb.Literal{Isnull: true}},
					},
				},
			},
		},
	}
	sourceDef := &planpb.TableDef{
		TblId:     10,
		LogicalId: 10,
		DbName:    "upgrade_db",
		Name:      "legacy_source",
		TableType: catalog.SystemOrdinaryRel,
		Createsql: "CREATE TABLE legacy_source (id INT, payload TINYTEXT)",
		Cols: []*planpb.ColDef{
			{Name: "id", OriginName: "id", Seqnum: 0, Typ: planpb.Type{Id: int32(types.T_int32)}},
			{
				Name: "payload", OriginName: "payload", Seqnum: 1, Typ: legacyType,
				Default: &planpb.Default{
					NullAbility: true,
				},
			},
		},
	}

	require.NoError(t, RecoverLegacyTinyText(t.Context(), tableDef, func(
		_ context.Context,
		databaseName string,
		tableName string,
	) (*planpb.TableDef, error) {
		require.Equal(t, "upgrade_db", databaseName)
		require.Equal(t, "legacy_source", tableName)
		return sourceDef, nil
	}))
	require.Equal(t, int32(types.MaxTinyTextLen), tableDef.Cols[1].Typ.Width)
	// Both source and target are normalized on planner-owned clones.
	require.Zero(t, sourceDef.Cols[1].Typ.Width)
}

func TestRecoverLegacyTinyTextRejectsStaleCreateLikeLineage(t *testing.T) {
	tableDef := &planpb.TableDef{
		TblId:     20,
		LogicalId: 20,
		DbName:    "upgrade_db",
		Name:      "legacy_clone",
		TableType: catalog.SystemOrdinaryRel,
		Createsql: "CREATE TABLE legacy_clone LIKE legacy_source",
		Cols: []*planpb.ColDef{{
			Name: "payload", OriginName: "payload", Typ: planpb.Type{Id: int32(types.T_text)},
		}},
	}
	sourceDef := &planpb.TableDef{
		TblId:     10,
		LogicalId: 10,
		DbName:    "upgrade_db",
		Name:      "legacy_source",
		TableType: catalog.SystemOrdinaryRel,
		Createsql: "CREATE TABLE legacy_source (payload TINYTEXT NOT NULL)",
		Cols: []*planpb.ColDef{{
			Name: "payload", OriginName: "payload", NotNull: true,
			Typ: planpb.Type{Id: int32(types.T_text)},
		}},
	}

	require.NoError(t, RecoverLegacyTinyText(t.Context(), tableDef, func(
		context.Context, string, string,
	) (*planpb.TableDef, error) {
		return sourceDef, nil
	}))
	require.Zero(t, tableDef.Cols[0].Typ.Width)
}

func TestRecoverLegacyTinyTextFromCreateSQLIgnoresUnrelatedText(t *testing.T) {
	tableDef := &planpb.TableDef{
		DbName:    "upgrade_db",
		Name:      "plain_text_t",
		TableType: catalog.SystemOrdinaryRel,
		Createsql: "CREATE TABLE plain_text_t (payload TEXT COMMENT 'not TINYTEXT data')",
		Cols: []*planpb.ColDef{
			{Name: "payload", OriginName: "payload", Typ: planpb.Type{Id: int32(types.T_text)}},
		},
	}

	require.NoError(t, RecoverLegacyTinyTextFromCreateSQL(t.Context(), tableDef))
	require.Equal(t, int32(0), tableDef.Cols[0].Typ.Width)
}

func TestRecoverLegacyTinyTextFromCreateSQLIgnoresViews(t *testing.T) {
	tableDef := &planpb.TableDef{
		DbName:    "upgrade_db",
		Name:      "legacy_v",
		TableType: catalog.SystemViewRel,
		Createsql: "CREATE VIEW legacy_v AS SELECT 'tinytext' AS payload",
		Cols: []*planpb.ColDef{
			{Name: "payload", OriginName: "payload", Typ: planpb.Type{Id: int32(types.T_text)}},
		},
	}

	require.NoError(t, RecoverLegacyTinyTextFromCreateSQL(t.Context(), tableDef))
	require.Equal(t, int32(0), tableDef.Cols[0].Typ.Width)
}
