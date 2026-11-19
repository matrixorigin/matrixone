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

package frontend

import (
	"testing"

	"github.com/matrixorigin/matrixone/pkg/catalog"
	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/pb/plan"
	sqlplan "github.com/matrixorigin/matrixone/pkg/sql/plan"
	"github.com/stretchr/testify/require"
)

// backslashCommentTableDef is the fixed shape whose stored column and table
// comments contain literal backslashes: the exact case #31068 fixes for the
// SHOW CREATE TABLE display. The definitions hand-build plan.TableDef so the
// fingerprint inputs stay identical across versions.
func backslashCommentTableDef() *plan.TableDef {
	return &plan.TableDef{
		TblId: 7, LogicalId: 7,
		Name:      "fingerprint_source",
		TableType: catalog.SystemOrdinaryRel,
		Cols: []*plan.ColDef{
			{
				Name: "a", Seqnum: 0,
				Typ:     plan.Type{Id: int32(types.T_int32)},
				Comment: `C:\dir\new`,
				Default: &plan.Default{NullAbility: true},
			},
			{
				Name: "b", Seqnum: 1,
				Typ:     plan.Type{Id: int32(types.T_int32)},
				Comment: `it''s`,
				Default: &plan.Default{NullAbility: true},
			},
		},
		Defs: []*plan.TableDef_DefType{{
			Def: &plan.TableDef_DefType_Properties{
				Properties: &plan.PropertiesDef{Properties: []*plan.Property{{
					Key: catalog.SystemRelAttr_Comment, Value: `x\ty''z`,
				}}},
			},
		}},
	}
}

// legacyBackslashCommentFingerprint is the schema fingerprint that base
// commit 2f8011faec produced for backslashCommentTableDef with its historical
// raw comment spelling:
//
//	COMMENT 'C:\dir\new', COMMENT 'it''s' ... COMMENT='x\ty''z'
//
// The value is frozen from the base tree (probe captured in
// state/artifacts/issue-31068/evidence/probe-hash-fixture-basetree.log); the
// fingerprint is persisted in dump manifests, so it must not drift with the
// SHOW CREATE TABLE comment-escaping fix (#31068).
const legacyBackslashCommentFingerprint = "d0f7fd53926c805dd05af3d5dcd2f4f148d8861d539db9a7490aa77e07b061bd"

// DUMP/LOAD schema fingerprints are persisted in dump manifests and compared
// across versions, so the canonical DDL they hash must keep the historical
// comment spelling byte for byte while the SHOW display escapes backslashes.
func TestDumpSchemaHashKeepsLegacyBackslashCommentSpelling(t *testing.T) {
	def := backslashCommentTableDef()
	hash, err := tableSchemaHash(def)
	require.NoError(t, err)
	require.Equal(t, legacyBackslashCommentFingerprint, hash)

	logical, err := tableLogicalSchemaHashWithResolver(t.Context(), def, nil)
	require.NoError(t, err)
	require.Equal(t, legacyBackslashCommentFingerprint, logical)

	// Only the SHOW CREATE TABLE display escaped the backslashes; the show
	// DDL remains replay-safe and rejects none of the fix's own contract.
	showDDL, _, err := sqlplan.ConstructCreateTableSQL(nil, def, nil, false, nil)
	require.NoError(t, err)
	require.Contains(t, showDDL, `COMMENT 'C:\\dir\\new'`)
	require.Contains(t, showDDL, `COMMENT 'it''s'`)
	require.Contains(t, showDDL, `COMMENT='x\\ty''z'`)
}

// A dump manifest written by an older version (raw comment spelling) must
// still match an unchanged table after the escaping fix, while the same
// comparison keeps rejecting genuinely different schemas.
func TestOldDumpManifestMatchesUnchangedTableAfterCommentEscapingFix(t *testing.T) {
	def := backslashCommentTableDef()
	hashNow, err := tableSchemaHash(def)
	require.NoError(t, err)
	require.Equal(t, legacyBackslashCommentFingerprint, hashNow)

	target := tableDumpRelation{SchemaHash: hashNow, LogicalSchemaHash: hashNow}
	legacyManifest := tableDumpRelation{SchemaHash: legacyBackslashCommentFingerprint}
	require.True(t, tableDumpRelationSchemaMatches(target, legacyManifest))

	// Genuine mismatch control: the corrupted comment the base SHOW output
	// produced on replay (`\d` was eaten, `\n` became LF) stays a mismatch.
	corruptedComment := backslashCommentTableDef()
	corruptedComment.Cols[0].Comment = "C:dir\nNewline missing backslashes"
	corruptedHash, err := tableSchemaHash(corruptedComment)
	require.NoError(t, err)
	require.NotEqual(t, hashNow, corruptedHash)
	require.False(t, tableDumpRelationSchemaMatches(
		target, tableDumpRelation{SchemaHash: corruptedHash}))

	// Genuine mismatch control: a different column type also stays a
	// mismatch even though its comments are identical.
	alteredType := backslashCommentTableDef()
	alteredType.Cols[0].Typ.Id = int32(types.T_int64)
	alteredHash, err := tableSchemaHash(alteredType)
	require.NoError(t, err)
	require.NotEqual(t, hashNow, alteredHash)
	require.False(t, tableDumpRelationSchemaMatches(
		target, tableDumpRelation{SchemaHash: alteredHash}))
}
