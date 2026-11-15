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

package plan

import (
	"context"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/matrixorigin/matrixone/pkg/common/moerr"
	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/pb/plan"
	"github.com/matrixorigin/matrixone/pkg/sql/features"
	"github.com/matrixorigin/matrixone/pkg/sql/parsers"
	"github.com/matrixorigin/matrixone/pkg/sql/parsers/dialect"
	"github.com/matrixorigin/matrixone/pkg/sql/parsers/tree"
)

// Issue #32211 — consumer-valid RawSQL oracle for the partitioned
// CREATE INDEX → ALTER ADD INDEX delegation.
//
// The delegated plan's RawSQL is a real consumer input: Scope.AlterTable
// reparses it to propagate each physical partition child
// (compile/alter.go alterPartitionTables), so every qualified form must
// survive the full serialize → ParseOne → rewrite-child-table →
// serialize → ParseOne loop with the exact index AST bytes and no
// semantic loss, including quoted identifiers, explicit btree, prefix,
// order, comment and visibility options. The dedicated CREATE INDEX
// families excluded from the delegation (vector/plugin/regular-other
// algos, explicit-using unique, fulltext) must keep their original
// planning route or their original admission error — never the ALTER
// route.

// newIssue32211RoundtripTableDef mirrors the engine-resolved partition
// state (Partition defs + durable features.Partitioned flag) plus the
// legal-but-tricky identifier surface the roundtrip must preserve.
func newIssue32211RoundtripTableDef(name string, partitioned bool) *TableDef {
	def := &TableDef{
		Name: name,
		Cols: []*ColDef{
			{
				ColId:   1,
				Name:    "c1",
				NotNull: true,
				Typ:     plan.Type{Id: int32(types.T_int32)},
				Default: &plan.Default{NullAbility: false},
			},
			{
				ColId:   2,
				Name:    "nm",
				NotNull: false,
				Typ:     plan.Type{Id: int32(types.T_varchar), Width: 64},
				Default: &plan.Default{NullAbility: true},
			},
			{
				ColId:   3,
				Name:    "my col",
				NotNull: false,
				Typ:     plan.Type{Id: int32(types.T_varchar), Width: 64},
				Default: &plan.Default{NullAbility: true},
			},
			{
				ColId:   4,
				Name:    "select",
				NotNull: false,
				Typ:     plan.Type{Id: int32(types.T_varchar), Width: 64},
				Default: &plan.Default{NullAbility: true},
			},
		},
	}
	def.Pkey = &plan.PrimaryKeyDef{
		Names:       []string{"c1"},
		PkeyColName: "c1",
	}
	if partitioned {
		def.Partition = &plan.Partition{
			PartitionDefs: []*plan.PartitionDef{
				{Def: &plan.Expr{}},
				{Def: &plan.Expr{}},
			},
		}
		def.FeatureFlag |= features.Partitioned
	}
	return def
}

func newIssue32211RoundtripContext(t testing.TB) *MockCompilerContext {
	t.Helper()
	ctx := NewMockCompilerContext(true, newPlanTestProcess(t))
	ctx.SetContext(context.Background())
	registerIssue32211Table(ctx, newIssue32211RoundtripTableDef("pp", true))
	registerIssue32211Table(ctx, newIssue32211RoundtripTableDef("p q", true))
	registerIssue32211Table(ctx, newIssue32211RoundtripTableDef("nn", false))
	return ctx
}

type issue32211ExpectedKeyPart struct {
	col       string
	direction tree.Direction
	length    int
}

type issue32211RoundtripCase struct {
	name      string
	sql       string
	unique    bool
	indexName string
	keyType   tree.IndexType
	keyParts  []issue32211ExpectedKeyPart
	comment   string
	visible   tree.VisibleType
}

func parseIssue32211AlterSQL(t testing.TB, sql string, caseSensitive int64, assertMsg string) *tree.AlterTable {
	t.Helper()
	stmt, err := parsers.ParseOne(context.Background(), dialect.MYSQL, sql, caseSensitive)
	require.NoError(t, err, "%s: delegated RawSQL %q must reparse", assertMsg, sql)
	alter, ok := stmt.(*tree.AlterTable)
	require.True(t, ok, "%s: reparsed statement must stay ALTER TABLE, got %T", assertMsg, stmt)
	return alter
}

// requireIssue32211IndexAST asserts the exact parser-produced index
// definition inside an AlterOptionAdd.
func requireIssue32211IndexAST(
	t testing.TB,
	tc issue32211RoundtripCase,
	def tree.TableDef,
	assertMsg string,
) {
	t.Helper()
	if tc.unique {
		ui, ok := def.(*tree.UniqueIndex)
		require.True(t, ok, "%s: expected a unique index def, got %T", assertMsg, def)
		require.Equal(t, tc.indexName, ui.Name, "%s: index name loss", assertMsg)
		require.True(t, ui.Empty,
			"%s: the default unique form must stay parser-typed with Empty=true (no USING part)", assertMsg)
		requireIssues32211KeyParts(t, tc, ui.KeyParts, assertMsg)
		requireIssue32211Options(t, tc, ui.IndexOption, assertMsg)
		require.Empty(t, ui.ConstraintSymbol, "%s", assertMsg)
		return
	}
	ix, ok := def.(*tree.Index)
	require.True(t, ok, "%s: expected an ordinary index def, got %T", assertMsg, def)
	require.Equal(t, tc.indexName, ix.Name, "%s: index name loss", assertMsg)
	require.Equal(t, tc.keyType, ix.KeyType,
		"%s: index key type loss (default INVALID / explicit BTREE)", assertMsg)
	require.False(t, ix.IfNotExists, "%s", assertMsg)
	requireIssues32211KeyParts(t, tc, ix.KeyParts, assertMsg)
	requireIssue32211Options(t, tc, ix.IndexOption, assertMsg)
}

func requireIssues32211KeyParts(
	t testing.TB,
	tc issue32211RoundtripCase,
	keyParts []*tree.KeyPart,
	assertMsg string,
) {
	t.Helper()
	require.Len(t, keyParts, len(tc.keyParts), "%s: key part count loss", assertMsg)
	for i, expect := range tc.keyParts {
		got := keyParts[i]
		require.NotNil(t, got.ColName, "%s: key part %d lost its column", assertMsg, i)
		require.Equal(t, expect.col, got.ColName.ColNameOrigin(),
			"%s: key part %d column loss", assertMsg, i)
		require.Equal(t, expect.direction, got.Direction,
			"%s: key part %d direction loss", assertMsg, i)
		require.Equal(t, expect.length, got.Length,
			"%s: key part %d prefix length loss", assertMsg, i)
	}
}

func requireIssue32211Options(
	t testing.TB,
	tc issue32211RoundtripCase,
	option *tree.IndexOption,
	assertMsg string,
) {
	t.Helper()
	if option == nil {
		require.Empty(t, tc.comment, "%s: comment lost (IndexOption nil)", assertMsg)
		require.Equal(t, tree.VISIBLE_TYPE_INVALID, tc.visible,
			"%s: visibility lost (IndexOption nil)", assertMsg)
		return
	}
	require.Equal(t, tc.comment, option.Comment, "%s: comment loss", assertMsg)
	require.Equal(t, tc.visible, option.Visible, "%s: visibility loss", assertMsg)
}

// issue32211AlterAddIndexDefOf extracts the single AlterOptionAdd def.
func issue32211AlterAddIndexDefOf(
	t testing.TB,
	alter *tree.AlterTable,
	assertMsg string,
) tree.TableDef {
	t.Helper()
	require.Empty(t, alter.PartitionOption, "%s: no partition option expected", assertMsg)
	require.Len(t, alter.Options, 1, "%s: exactly one alter option expected", assertMsg)
	add, ok := alter.Options[0].(*tree.AlterOptionAdd)
	require.True(t, ok, "%s: expected AlterOptionAdd, got %T", assertMsg, alter.Options[0])
	return add.Def
}

func runIssue32211RoundtripCase(t *testing.T, ctx *MockCompilerContext, tc issue32211RoundtripCase) {
	t.Helper()
	p, err := runIssue32211Stmt(t, ctx, tc.sql)
	require.NoError(t, err, "%s: creating a qualified index on a partitioned table must stay a supported operation", tc.name)

	alter := p.GetDdl().GetAlterTable()
	require.NotNil(t, alter, "%s: qualified partitioned CREATE INDEX must keep delegating to the ALTER route, got DdlType %v", tc.name, p.GetDdl().DdlType)
	require.Equal(t, plan.AlterTable_INPLACE, alter.GetAlgorithmType(), "%s", tc.name)
	require.NotEmpty(t, alter.GetRawSQL(), "%s: child propagation needs RawSQL", tc.name)

	// Plan-level payload: the published index def named and typed exactly
	// as requested.
	var addIndex *plan.AlterTableAddIndex
	for _, action := range alter.GetActions() {
		if candidate := action.GetAddIndex(); candidate != nil {
			require.Nil(t, addIndex, "%s: exactly one AddIndex action expected", tc.name)
			addIndex = candidate
		}
	}
	require.NotNil(t, addIndex, "%s: AddIndex action missing", tc.name)
	indexDefs := addIndex.GetIndexInfo().GetTableDef().GetIndexes()
	require.Len(t, indexDefs, 1, "%s", tc.name)
	require.Equal(t, tc.indexName, indexDefs[0].GetIndexName(), "%s: plan def name loss", tc.name)
	require.Equal(t, tc.unique, indexDefs[0].GetUnique(), "%s: plan def unique loss", tc.name)

	// Real consumer closure #1: Scope.AlterTable reparses RawSQL.
	alter1 := parseIssue32211AlterSQL(t, alter.GetRawSQL(), 1, tc.name)
	require.Equal(t, alter.GetTableDef().GetName(), string(alter1.Table.Name()),
		"%s: reparse must keep the bound table", tc.name)
	requireIssue32211IndexAST(t, tc, issue32211AlterAddIndexDefOf(t, alter1, tc.name), tc.name)

	// Serialize(consumer AST) must be byte-identical (idempotence).
	second := tree.StringWithOpts(
		alter1,
		dialect.MYSQL,
		tree.WithQuoteIdentifier(),
		tree.WithSingleQuoteString(),
	)
	require.Equal(t, alter.GetRawSQL(), second,
		"%s: serialize∘parse must be idempotent: %q vs %q", tc.name, alter.GetRawSQL(), second)

	// Real consumer closure #2: each physical partition child gets the
	// same ADD INDEX under the child table name (mirrors
	// alterPartitionTables' table rewrite + re-serialization).
	const childTable = "part p0"
	alter1.PartitionOption = nil
	alter1.Table = tree.NewTableName(
		tree.Identifier(childTable),
		alter1.Table.ObjectNamePrefix,
		alter1.Table.AtTsExpr,
	)
	childSQL := tree.StringWithOpts(
		alter1,
		dialect.MYSQL,
		tree.WithQuoteIdentifier(),
		tree.WithSingleQuoteString(),
	)
	alter2 := parseIssue32211AlterSQL(t, childSQL, 1, tc.name+"/child")
	require.Equal(t, childTable, string(alter2.Table.Name()),
		"%s/child: quoted child table name loss: %q", tc.name, childSQL)
	requireIssue32211IndexAST(t, tc, issue32211AlterAddIndexDefOf(t, alter2, tc.name+"/child"), tc.name+"/child")
}

func TestIssue32211CreateIndexRawSQLRoundtrip(t *testing.T) {
	ctx := newIssue32211RoundtripContext(t)

	cases := []issue32211RoundtripCase{
		{
			name:      "ordinary default using",
			sql:       "create index idx_def on pp (c1)",
			indexName: "idx_def",
			keyType:   tree.INDEX_TYPE_INVALID,
			keyParts:  []issue32211ExpectedKeyPart{{col: "c1"}},
		},
		{
			name:      "ordinary explicit btree",
			sql:       "create index idx_bt on pp (c1) using btree",
			indexName: "idx_bt",
			keyType:   tree.INDEX_TYPE_BTREE,
			keyParts:  []issue32211ExpectedKeyPart{{col: "c1"}},
		},
		{
			name:      "composite with order and prefix",
			sql:       "create index idx_cpx on pp (c1 desc, nm (8))",
			indexName: "idx_cpx",
			keyType:   tree.INDEX_TYPE_INVALID,
			keyParts: []issue32211ExpectedKeyPart{
				{col: "c1", direction: tree.Descending},
				{col: "nm", length: 8},
			},
		},
		{
			name:      "reserved word index and column names",
			sql:       "create index `select` on pp (`select`)",
			indexName: "select",
			keyType:   tree.INDEX_TYPE_INVALID,
			keyParts:  []issue32211ExpectedKeyPart{{col: "select"}},
		},
		{
			name:      "spaced index, table and column names",
			sql:       "create index `my idx` on `p q` (`my col`)",
			indexName: "my idx",
			keyType:   tree.INDEX_TYPE_INVALID,
			keyParts:  []issue32211ExpectedKeyPart{{col: "my col"}},
		},
		{
			name:      "embedded backtick index name",
			sql:       "create index `a``b` on pp (c1)",
			indexName: "a`b",
			keyType:   tree.INDEX_TYPE_INVALID,
			keyParts:  []issue32211ExpectedKeyPart{{col: "c1"}},
		},
		{
			name:      "default unique index",
			sql:       "create unique index uidx_def on pp (nm)",
			unique:    true,
			indexName: "uidx_def",
			keyParts:  []issue32211ExpectedKeyPart{{col: "nm"}},
		},
		{
			name:      "comment option with spaces and quotes",
			sql:       "create index idx_cmt on pp (c1) comment 'my note ''q'''",
			indexName: "idx_cmt",
			keyType:   tree.INDEX_TYPE_INVALID,
			keyParts:  []issue32211ExpectedKeyPart{{col: "c1"}},
			comment:   "my note 'q'",
		},
		{
			// value bytes: a backslash b space c — the delegated RawSQL must
			// backslash-escape so the consumer's backslash-aware ParseOne
			// restores exactly these bytes (issue #32211 review R2).
			name:      "comment with literal backslash survives reparse",
			sql:       "create index idx_bs on pp (c1) comment 'a\\\\b c'",
			indexName: "idx_bs",
			keyType:   tree.INDEX_TYPE_INVALID,
			keyParts:  []issue32211ExpectedKeyPart{{col: "c1"}},
			comment:   "a\\b c",
		},
		{
			name:      "comment with real newline survives reparse",
			sql:       "create index idx_nl on pp (c1) comment 'line1\nline2'",
			indexName: "idx_nl",
			keyType:   tree.INDEX_TYPE_INVALID,
			keyParts:  []issue32211ExpectedKeyPart{{col: "c1"}},
			comment:   "line1\nline2",
		},
		{
			name:      "utf8 comment survives reparse",
			sql:       "create index idx_u8 on pp (c1) comment '数据库 ✓ ok'",
			indexName: "idx_u8",
			keyType:   tree.INDEX_TYPE_INVALID,
			keyParts:  []issue32211ExpectedKeyPart{{col: "c1"}},
			comment:   "数据库 ✓ ok",
		},
		{
			name:      "invisible option",
			sql:       "create index idx_inv on pp (c1) invisible",
			indexName: "idx_inv",
			keyType:   tree.INDEX_TYPE_INVALID,
			keyParts:  []issue32211ExpectedKeyPart{{col: "c1"}},
			visible:   tree.VISIBLE_TYPE_INVISIBLE,
		},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			runIssue32211RoundtripCase(t, ctx, tc)
		})
	}

	t.Run("quoted identifiers survive case-insensitive reparse mode", func(t *testing.T) {
		p, err := runIssue32211Stmt(t, ctx, "create index `iSel` on pp (`selEct`)")
		require.NoError(t, err)
		alter := p.GetDdl().GetAlterTable()
		require.NotNil(t, alter)
		// c.getLower() sessions reparse without case sensitivity; the
		// quoted serialization must preserve exact identifier bytes.
		parsed := parseIssue32211AlterSQL(t, alter.GetRawSQL(), 0, "case-insensitive mode")
		require.Equal(t, "pp", string(parsed.Table.Name()), "table must stay case-exact")
		ix, ok := issue32211AlterAddIndexDefOf(t, parsed, "case-insensitive mode").(*tree.Index)
		require.True(t, ok)
		require.Equal(t, "iSel", ix.Name)
		require.Equal(t, "selEct", ix.KeyParts[0].ColName.ColNameOrigin())
	})
}

// TestIssue32211CreateIndexFamilyBoundaries proves the delegation is
// bounded to the default/BTREE ordinary forms and the lossless default
// unique form. Every other family keeps its original planning route or
// its original admission error — never the ALTER route.
func TestIssue32211CreateIndexFamilyBoundaries(t *testing.T) {
	ctx := newIssue32211RoundtripContext(t)

	boundarySQL := map[string]string{
		"ivfflat":               "create index ivf_idx on pp (nm) using ivfflat",
		"master":                "create index mst_idx on pp (c1) using master",
		"hash":                  "create index hsh_idx on pp (c1) using hash",
		"zonemap":               "create index zmp_idx on pp (c1) using zonemap",
		"unique explicit using": "create unique index uh_idx on pp (nm) using hash",
	}
	for family, sql := range boundarySQL {
		t.Run(family+" stays on its dedicated path", func(t *testing.T) {
			p, err := runIssue32211Stmt(t, ctx, sql)
			if err != nil {
				// The family keeps its original admission error.
				return
			}
			require.Nil(t, p.GetDdl().GetAlterTable(),
				"%s must not be redirected into the ALTER ADD INDEX route", family)
			require.NotNil(t, p.GetDdl().GetCreateIndex(),
				"%s must keep the dedicated CREATE INDEX plan", family)
		})
	}
}

// TestIssue32211ExplicitBtreeUniqueDelegatesIdentically proves review R3:
// the explicit-BTREE UNIQUE form belongs to this issue's qualified family.
// buildUniqueIndexTable builds the same unique ordered table/IndexDef for
// the default and explicit BTREE kinds, so the delegation normalizes both
// into the same typed ALTER shape: identical serialized AST, identical plan
// payload, identical reparsed ALTER AST.
func TestIssue32211ExplicitBtreeUniqueDelegatesIdentically(t *testing.T) {
	newAlter := func(sql string) *plan.AlterTable {
		ctx := newIssue32211RoundtripContext(t)
		p, err := runIssue32211Stmt(t, ctx, sql)
		require.NoError(t, err)
		alter := p.GetDdl().GetAlterTable()
		require.NotNil(t, alter, "%s must delegate to the ALTER route", sql)
		require.Equal(t, plan.AlterTable_INPLACE, alter.GetAlgorithmType(), "%s", sql)
		return alter
	}
	altDefault := newAlter("create unique index ux on pp (nm)")
	altBtree := newAlter("create unique index ux on pp (nm) using btree")

	// Same normalized typed ALTER form: byte-identical serialized AST.
	require.Equal(t, altDefault.GetRawSQL(), altBtree.GetRawSQL())

	// Same built unique index def. The physical index-table name embeds a
	// fresh per-plan UUID (buildUniqueIndexTable mints one at every build),
	// so semantic identity compares every structural field and pins the
	// generated-name family instead of the random identity.
	var addA, addB *plan.AlterTableAddIndex
	for _, action := range altDefault.GetActions() {
		if c := action.GetAddIndex(); c != nil {
			addA = c
		}
	}
	for _, action := range altBtree.GetActions() {
		if c := action.GetAddIndex(); c != nil {
			addB = c
		}
	}
	require.NotNil(t, addA)
	require.NotNil(t, addB)
	require.Equal(t, addA.GetDbName(), addB.GetDbName())
	require.Equal(t, addA.GetTableName(), addB.GetTableName())
	require.Equal(t, addA.GetOriginTablePrimaryKey(), addB.GetOriginTablePrimaryKey())
	require.Equal(t, addA.GetIndexTableExist(), addB.GetIndexTableExist())
	defsA := addA.GetIndexInfo().GetTableDef().GetIndexes()
	defsB := addB.GetIndexInfo().GetTableDef().GetIndexes()
	require.Len(t, defsB, len(defsA))
	for i := range defsA {
		require.Equal(t, defsA[i].GetIndexName(), defsB[i].GetIndexName())
		require.Equal(t, defsA[i].GetUnique(), defsB[i].GetUnique())
		require.Equal(t, defsA[i].GetParts(), defsB[i].GetParts())
		require.Equal(t, defsA[i].GetIndexAlgo(), defsB[i].GetIndexAlgo())
		require.Equal(t, defsA[i].GetIndexAlgoTableType(), defsB[i].GetIndexAlgoTableType())
		require.Equal(t, defsA[i].GetIndexAlgoParams(), defsB[i].GetIndexAlgoParams())
		require.Contains(t, defsA[i].GetIndexTableName(), "__mo_index_unique_")
		require.Contains(t, defsB[i].GetIndexTableName(), "__mo_index_unique_")
	}
	// Same physical unique-index table schema: identical column types.
	colsA := addA.GetIndexInfo().GetTableDef().GetCols()
	colsB := addB.GetIndexInfo().GetTableDef().GetCols()
	require.Len(t, colsB, len(colsA))
	for i := range colsA {
		require.Equal(t, colsA[i].GetTyp(), colsB[i].GetTyp())
	}
	require.Len(t, addB.GetIndexInfo().GetIndexTables(), len(addA.GetIndexInfo().GetIndexTables()))
	for i, tbl := range addA.GetIndexInfo().GetIndexTables() {
		require.Contains(t, tbl.GetName(), "__mo_index_unique_")
		require.Contains(t, addB.GetIndexInfo().GetIndexTables()[i].GetName(), "__mo_index_unique_")
		require.Len(t, addB.GetIndexInfo().GetIndexTables()[i].GetCols(), len(tbl.GetCols()))
	}

	// Real consumer loop: both reparse to the identical ALTER AST.
	parsedDefault := parseIssue32211AlterSQL(t, altDefault.GetRawSQL(), 1, "default unique")
	parsedBtree := parseIssue32211AlterSQL(t, altBtree.GetRawSQL(), 1, "explicit btree unique")
	require.Equal(t,
		issue32211AlterAddIndexDefOf(t, parsedDefault, "default unique"),
		issue32211AlterAddIndexDefOf(t, parsedBtree, "explicit btree unique"))
}

// TestIssue32211CreateIndexRoundtripAdmissionControls keeps the CREATE
// INDEX admission contract in front of the delegation.
func TestIssue32211CreateIndexRoundtripAdmissionControls(t *testing.T) {
	ctx := newIssue32211RoundtripContext(t)

	t.Run("duplicate index name keeps CREATE INDEX admission error", func(t *testing.T) {
		table := ctx.tables["pp"]
		table.Indexes = append(table.Indexes, &plan.IndexDef{
			IndexName: "dup",
			Parts:     []string{"c1"},
		})
		_, err := runIssue32211Stmt(t, ctx, "create index dup on pp (c1)")
		require.Error(t, err)
		require.True(t, moerr.IsMoErrCode(err, moerr.ErrDuplicateKey),
			"duplicate name must keep the CREATE INDEX DuplicateKey admission, got %v", err)
	})

	t.Run("non-partitioned table keeps CREATE INDEX plan", func(t *testing.T) {
		p, err := runIssue32211Stmt(t, ctx, "create index nn_idx on nn (c1)")
		require.NoError(t, err)
		require.Nil(t, p.GetDdl().GetAlterTable())
		require.NotNil(t, p.GetDdl().GetCreateIndex())
	})
}
