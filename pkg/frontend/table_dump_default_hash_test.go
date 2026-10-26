// Copyright 2026 Matrix Origin
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//	http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package frontend

import (
	"testing"

	"github.com/gogo/protobuf/proto"
	"github.com/stretchr/testify/require"

	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/pb/plan"
	sqlplan "github.com/matrixorigin/matrixone/pkg/sql/plan"
)

// paddedBinaryDefaultTableDef mirrors the DUMP fingerprint input for a table
// whose binary(8) column default was bound from `DEFAULT 'a'`: the binder
// assignment-pads the bound literal (Expr Sval) while the stored default SQL
// text (OriginString) keeps the user spelling.
func paddedBinaryDefaultTableDef(origin string) *plan.TableDef {
	return &plan.TableDef{
		Name: "source",
		Cols: []*plan.ColDef{
			{Name: "id", OriginName: "id", Typ: plan.Type{Id: int32(types.T_int32)},
				Default: &plan.Default{NullAbility: true}},
			{Name: "a", OriginName: "a",
				Typ: plan.Type{Id: int32(types.T_binary), Width: 8},
				Default: &plan.Default{OriginString: origin, NullAbility: true,
					Expr: &plan.Expr{Expr: &plan.Expr_Lit{Lit: &plan.Literal{
						Value: &plan.Literal_Sval{Sval: "a\x00\x00\x00\x00\x00\x00\x00"},
					}}}}},
		},
	}
}

// TestTableDumpPaddedBinaryDefaultReplayHashMatchesManifest keeps the LOAD
// gate open across a SHOW CREATE replay rebuild of a padded binary default
// (the #31066 review regression): the replay DDL context-free consumers
// (dump, CDC, publication) print must carry the stored default text rather
// than the padded bound literal, so the recreated target's recomputed schema
// hash equals the manifest's fingerprint and tableDumpRelationSchemaMatches
// accepts it.
func TestTableDumpPaddedBinaryDefaultReplayHashMatchesManifest(t *testing.T) {
	def := paddedBinaryDefaultTableDef("'a'")

	replaySQL, parsed, err := sqlplan.ConstructCreateTableSQL(nil, def, nil, false, nil)
	require.NoError(t, err)
	if parsed != nil {
		parsed.Free()
	}
	require.Contains(t, replaySQL, "DEFAULT 'a'")
	require.NotContains(t, replaySQL, "a\x00", "the raw padded literal must not print")
	require.NotContains(t, replaySQL, `\0`, "the escaped padded spelling must not print")

	// The candidate target a replay of that output rebuilds keeps the same
	// stored default text, so its fingerprint is the manifest's and the LOAD
	// gate accepts it.
	replay := proto.Clone(def).(*plan.TableDef)
	sourceHash, err := tableSchemaHash(def)
	require.NoError(t, err)
	replayHash, err := tableSchemaHash(replay)
	require.NoError(t, err)
	require.Equal(t, sourceHash, replayHash)
	require.True(t, tableDumpRelationSchemaMatches(
		tableDumpRelation{SchemaHash: replayHash},
		tableDumpRelation{SchemaHash: sourceHash}),
		"a target replayed from SHOW CREATE output must load against its archive")
}

// TestTableDumpPaddedBinaryDefaultRejectsPaddedSpelling keeps the gate
// discriminating on the other side: a replayed table whose stored default
// text is the padded spelling (what the superseded re-quote rendering bound)
// binds the same default value but is a genuinely different schema, so its
// hash differs and LOAD rejects it.
func TestTableDumpPaddedBinaryDefaultRejectsPaddedSpelling(t *testing.T) {
	source := paddedBinaryDefaultTableDef("'a'")
	sourceHash, err := tableSchemaHash(source)
	require.NoError(t, err)

	// Same bound bytes, different stored text: the value is preserved, but
	// the schema fingerprint diverges.
	corrupted := paddedBinaryDefaultTableDef("'a" + "\x00\x00\x00\x00\x00\x00\x00" + "'")
	require.Equal(t,
		paddedBinaryDefaultSval(source), paddedBinaryDefaultSval(corrupted))
	corruptedHash, err := tableSchemaHash(corrupted)
	require.NoError(t, err)
	require.NotEqual(t, sourceHash, corruptedHash)
	require.False(t, tableDumpRelationSchemaMatches(
		tableDumpRelation{SchemaHash: corruptedHash},
		tableDumpRelation{SchemaHash: sourceHash}),
		"a target storing the padding-spelling default must not load against the original archive")
}

func paddedBinaryDefaultSval(def *plan.TableDef) string {
	return def.Cols[1].Default.Expr.GetLit().GetSval()
}
