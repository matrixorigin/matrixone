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
			{Name: "id", OriginName: "id", Typ: planpb.Type{Id: int32(types.T_int32)}},
			{Name: "payload", OriginName: "payload", Typ: planpb.Type{Id: int32(types.T_text)}},
			{Name: "body", OriginName: "body", Typ: planpb.Type{Id: int32(types.T_text)}},
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

func TestRecoverLegacyTinyTextFromCreateSQLValidatesBeforeMutation(t *testing.T) {
	tableDef := &planpb.TableDef{
		DbName:    "upgrade_db",
		Name:      "broken_t",
		TableType: catalog.SystemOrdinaryRel,
		Createsql: "CREATE TABLE broken_t (first TINYTEXT, second TINYTEXT)",
		Cols: []*planpb.ColDef{
			{Name: "first", OriginName: "first", Typ: planpb.Type{Id: int32(types.T_text)}},
			{Name: "wrong_name", OriginName: "wrong_name", Typ: planpb.Type{Id: int32(types.T_text)}},
		},
	}

	err := RecoverLegacyTinyTextFromCreateSQL(t.Context(), tableDef)
	require.ErrorContains(t, err, "catalog column name does not match")
	require.Equal(t, int32(0), tableDef.Cols[0].Typ.Width)
	require.Equal(t, int32(0), tableDef.Cols[1].Typ.Width)
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
