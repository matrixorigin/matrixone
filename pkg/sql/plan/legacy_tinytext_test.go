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

func TestRecoverLegacyTinyTextFromCreateSQLSurvivesColumnRename(t *testing.T) {
	tableDef := &planpb.TableDef{
		DbName:    "upgrade_db",
		Name:      "renamed_t",
		TableType: catalog.SystemOrdinaryRel,
		Createsql: "CREATE TABLE renamed_t (id INT, payload TINYTEXT)",
		Cols: []*planpb.ColDef{
			{Name: "id", OriginName: "id", Seqnum: 0, Typ: planpb.Type{Id: int32(types.T_int32)}},
			{Name: "renamed_payload", OriginName: "renamed_payload", Seqnum: 1, Typ: planpb.Type{Id: int32(types.T_text)}},
		},
	}

	require.NoError(t, RecoverLegacyTinyTextFromCreateSQL(t.Context(), tableDef))
	require.Equal(t, int32(types.MaxTinyTextLen), tableDef.Cols[1].Typ.Width)
}

func TestRecoverLegacyTinyTextFollowsCreateLikeLineage(t *testing.T) {
	legacyType := planpb.Type{Id: int32(types.T_text)}
	tableDef := &planpb.TableDef{
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
		DbName:    "upgrade_db",
		Name:      "legacy_clone",
		TableType: catalog.SystemOrdinaryRel,
		Createsql: "CREATE TABLE legacy_clone LIKE legacy_source",
		Cols: []*planpb.ColDef{{
			Name: "payload", OriginName: "payload", Typ: planpb.Type{Id: int32(types.T_text)},
		}},
	}
	sourceDef := &planpb.TableDef{
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
