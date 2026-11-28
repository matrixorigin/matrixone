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
	"encoding/hex"
	"testing"

	"github.com/matrixorigin/matrixone/pkg/catalog"
	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/pb/plan"
	sqlplan "github.com/matrixorigin/matrixone/pkg/sql/plan"
	"github.com/stretchr/testify/require"
)

// apostropheIdentTableDef is the fixed shape whose CHECK constraint and
// IVFFLAT/FULLTEXT2 INCLUDE column names contain apostrophes. The definitions
// hand-build plan.TableDef so the fingerprint inputs stay identical across
// versions.
func apostropheIdentTableDef() *plan.TableDef {
	return &plan.TableDef{
		TblId: 11, LogicalId: 11,
		Name:      "apostrophe_fingerprint_source",
		TableType: catalog.SystemOrdinaryRel,
		Cols: []*plan.ColDef{
			{Name: "id", Seqnum: 0, Typ: plan.Type{Id: int32(types.T_int32)}, Default: &plan.Default{NullAbility: true}},
			{Name: "v", Seqnum: 1, Typ: plan.Type{Id: int32(types.T_int32)}, Default: &plan.Default{NullAbility: true}},
			{Name: "pay'load", Seqnum: 2, Typ: plan.Type{Id: int32(types.T_int32)}, Default: &plan.Default{NullAbility: true}},
			{Name: "sta'tus", Seqnum: 3, Typ: plan.Type{Id: int32(types.T_int32)}, Default: &plan.Default{NullAbility: true}},
			{Name: "body", Seqnum: 4, Typ: plan.Type{Id: int32(types.T_varchar), Width: 256}, Default: &plan.Default{NullAbility: true}},
		},
		Checks: []*plan.CheckDef{{
			Name:      "c'k",
			OriginSql: "v >= 0",
		}},
		Indexes: []*plan.IndexDef{
			{
				IndexName: "iv", Parts: []string{"v"},
				IndexAlgo:       catalog.MoIndexIvfFlatAlgo.ToString(),
				IndexAlgoParams: `{"lists":"1","op_type":"vector_l2_ops"}`,
				IncludedColumns: []string{"pay'load"},
			},
			{
				IndexName: "ftx", Parts: []string{"body"},
				IndexAlgo:       catalog.MoIndexFullText2Algo.ToString(),
				IndexAlgoParams: `{"parser":"ngram"}`,
				IncludedColumns: []string{"sta'tus"},
			},
		},
	}
}

// Historically the persisted dump hash input rendered CHECK and INCLUDE
// identifiers with formatStr, which doubles apostrophes inside backticks.
// These goldens were captured from the upstream renderer at base
// 226f575cfe360b14e6d24cb26fc97de037d940ea over the same fixture
// (raw capture: intake189-33356-33357-preserve-dump-hash-and-sql-oracles-glm2/
// raw/capture2-pristine.log). They are persisted dump-manifest fingerprints and
// must not drift with the identifier-quoting fix for CHECK/INCLUDE replay.
const legacyApostropheIdentFingerprint = "0b560858917ab964da83da7b8ee21d219ae0a12d2030269255c570946ad38df8"

const legacyApostropheIdentLogicalFingerprint = "26410328c0a5e66a5411dd043693419ca0fdb5c9db1262682c0476f893ffd2dd"

const historicalApostropheCanonicalHex = "435245415445205441424c4520605f5f7461626c655f64756d705f7461726765745f5f6020280a20206069646020696e742044454641554c54204e554c4c2c0a202060766020696e742044454641554c54204e554c4c2c0a202060706179276c6f61646020696e742044454641554c54204e554c4c2c0a202060737461277475736020696e742044454641554c54204e554c4c2c0a202060626f647960207661726368617228323536292044454641554c54204e554c4c2c0a20204b45592060697660205553494e4720697666666c6174202860766029206c69737473203d203120206f705f747970652027766563746f725f6c325f6f7073272020494e434c55444520286070617927276c6f616460292c0a2046554c4c54455854322060667478602860626f6479602920494e434c55444520286073746127277475736029205749544820504152534552206e6772616d2c0a2020434f4e53545241494e5420606327276b6020434845434b202876203e3d2030290a2920434f4c4c4154453d757466386d62345f62696e"

// DUMP/LOAD schema fingerprints are persisted in dump manifests and compared
// across versions, so the canonical DDL they hash must keep the historical
// apostrophe-doubled CHECK/INCLUDE spelling byte for byte while SHOW CREATE
// and executable replay keep verbatim identifier quoting.
func TestDumpSchemaHashKeepsLegacyApostropheIdentSpelling(t *testing.T) {
	def := apostropheIdentTableDef()
	hash, err := tableSchemaHash(def)
	require.NoError(t, err)
	require.Equal(t, legacyApostropheIdentFingerprint, hash)

	logical, err := tableLogicalSchemaHashWithResolver(t.Context(), def, nil)
	require.NoError(t, err)
	require.Equal(t, legacyApostropheIdentLogicalFingerprint, logical)

	clone := *def
	clone.Name = "__table_dump_target__"
	clone.DbName = ""
	clone.Createsql = ""
	canonical, err := sqlplan.ConstructCreateTableSQLForSchemaHash(&clone)
	require.NoError(t, err)
	require.Equal(t, historicalApostropheCanonicalHex, hex.EncodeToString([]byte(canonical)))

	raw, err := hex.DecodeString(historicalApostropheCanonicalHex)
	require.NoError(t, err)
	require.Contains(t, string(raw), "INCLUDE (`pay''load`)")
	require.Contains(t, string(raw), "INCLUDE (`sta''tus`)")
	require.Contains(t, string(raw), "CONSTRAINT `c''k` CHECK (v >= 0)")

	// Only the replay/display quoting changed; the show DDL renders the
	// authored identifiers verbatim and stays replay-safe.
	showDDL, _, err := sqlplan.ConstructCreateTableSQL(nil, def, nil, false, nil)
	require.NoError(t, err)
	require.Contains(t, showDDL, "CONSTRAINT `c'k` CHECK (v >= 0)")
	require.Contains(t, showDDL, "INCLUDE (`pay'load`)")
	require.Contains(t, showDDL, "INCLUDE (`sta'tus`)")
	require.NotContains(t, showDDL, "c''k")
	require.NotContains(t, showDDL, "pay''load")
	require.NotContains(t, showDDL, "sta''tus")
}

// A dump manifest written before the identifier-quoting fix (apostrophe-doubled
// hash bytes) must still match an unchanged table after the fix, while the
// same comparison keeps rejecting genuinely different schemas.
func TestOldDumpManifestMatchesUnchangedTableAfterIdentFix(t *testing.T) {
	def := apostropheIdentTableDef()
	hashNow, err := tableSchemaHash(def)
	require.NoError(t, err)
	require.Equal(t, legacyApostropheIdentFingerprint, hashNow)

	logicalNow, err := tableLogicalSchemaHashWithResolver(t.Context(), def, nil)
	require.NoError(t, err)
	require.Equal(t, legacyApostropheIdentLogicalFingerprint, logicalNow)

	target := tableDumpRelation{SchemaHash: hashNow, LogicalSchemaHash: logicalNow}
	legacyManifest := tableDumpRelation{SchemaHash: legacyApostropheIdentFingerprint, LogicalSchemaHash: legacyApostropheIdentLogicalFingerprint}
	require.True(t, tableDumpRelationSchemaMatches(target, legacyManifest))

	// Genuine mismatch control: a different column type stays a mismatch.
	alteredType := apostropheIdentTableDef()
	alteredType.Cols[0].Typ.Id = int32(types.T_int64)
	alteredHash, err := tableSchemaHash(alteredType)
	require.NoError(t, err)
	require.NotEqual(t, hashNow, alteredHash)
	require.False(t, tableDumpRelationSchemaMatches(
		target, tableDumpRelation{SchemaHash: alteredHash}))

	// Genuine mismatch control: a renamed CHECK (the doubled apostrophe
	// identity the pre-fix replay produced) is a different schema, not the
	// authored one.
	renamedCheck := apostropheIdentTableDef()
	renamedCheck.Checks[0].Name = "c''k"
	renamedCheckHash, err := tableSchemaHash(renamedCheck)
	require.NoError(t, err)
	require.NotEqual(t, hashNow, renamedCheckHash)
	require.False(t, tableDumpRelationSchemaMatches(
		target, tableDumpRelation{SchemaHash: renamedCheckHash}))
}
