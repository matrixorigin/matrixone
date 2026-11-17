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

	"github.com/gogo/protobuf/proto"

	"github.com/matrixorigin/matrixone/pkg/container/types"
	planpb "github.com/matrixorigin/matrixone/pkg/pb/plan"
	"github.com/stretchr/testify/require"
)

func TestEnumIndexIdentityDependencies(t *testing.T) {
	spec := enumIndexTestSpec()
	table, index := spec.Source, spec.Index
	index.Parts = []string{"other"}
	uses, err := EnumIndexUsesNewIdentity(table, index)
	require.NoError(t, err)
	require.False(t, uses, "an unrelated legacy index must stay on its old path")
	index.Parts = []string{"e"}
	uses, err = EnumIndexUsesNewIdentity(table, index)
	require.NoError(t, err)
	require.True(t, uses)
	index.Parts = []string{"other"}
	index.IncludedColumns = []string{"e"}
	uses, err = EnumIndexUsesNewIdentity(table, index)
	require.NoError(t, err)
	require.True(t, uses, "INCLUDE is an index dependency even if the key is not ENUM")
	index.IncludedColumns = nil
	index.IndexAlgoParams = `{"included_columns":"e"}`
	uses, err = EnumIndexUsesNewIdentity(table, index)
	require.NoError(t, err)
	require.True(t, uses, "catalog-loaded INCLUDE metadata must also be checked")
	index.IndexAlgoParams = ""
	index.Parts = []string{"generated"}
	table.Cols = append(table.Cols, &planpb.ColDef{
		Name: "generated", Typ: planpb.Type{Id: int32(types.T_int64)},
		GeneratedCol: &planpb.GeneratedCol{Expr: &planpb.Expr{Expr: &planpb.Expr_Col{Col: &planpb.ColRef{RelPos: 0, ColPos: 1}}}},
	})
	uses, err = EnumIndexUsesNewIdentity(table, index)
	require.NoError(t, err)
	require.True(t, uses, "a generated index key inherits its source dependency")
	table.Cols[len(table.Cols)-1].GeneratedCol.Expr.GetCol().ColPos = 99
	_, err = EnumIndexUsesNewIdentity(table, index)
	require.Error(t, err)
	table.Cols[len(table.Cols)-1].GeneratedCol.Expr.GetCol().ColPos = int32(len(table.Cols) - 1)
	_, err = EnumIndexUsesNewIdentity(table, index)
	require.Error(t, err, "a cyclic dependency cannot be classified")
	index.Parts = []string{"other"}
	table.Pkey.PkeyColName = "e"
	table.Pkey.Names = []string{"e"}
	uses, err = EnumIndexUsesNewIdentity(table, index)
	require.NoError(t, err)
	require.True(t, uses, "the hidden primary-key payload also fixes the index format")
}

func TestEnumIndexIdentityInGeneratedIntermediate(t *testing.T) {
	spec := enumIndexTestSpec()
	table, index := spec.Source, spec.Index
	table.Cols[1].Typ.EnumCollation = 0
	table.Cols = append(table.Cols, &planpb.ColDef{
		Name: "generated", Typ: planpb.Type{Id: int32(types.T_int64)},
		GeneratedCol: &planpb.GeneratedCol{Expr: &planpb.Expr{
			Typ:  planpb.Type{Id: int32(types.T_enum), Enumvalues: "A,a", EnumCollation: 2},
			Expr: &planpb.Expr_Lit{Lit: &planpb.Literal{}},
		}},
	})
	index.Parts = []string{"generated"}
	uses, err := EnumIndexUsesNewIdentity(table, index)
	require.NoError(t, err)
	require.True(t, uses)
	index.Parts = []string{"other"}
	uses, err = EnumIndexUsesNewIdentity(table, index)
	require.NoError(t, err)
	require.False(t, uses, "an unrelated legacy index retains its format")
}

func TestNewEnumIndexRejectsMixedLegacyPrefix(t *testing.T) {
	spec := enumIndexTestSpec()
	table, index := spec.Source, spec.Index
	index.Parts = []string{"other", "e"}
	index.IndexAlgoParams = `{"prefix_lengths":"other:1"}`
	require.ErrorContains(t, validateNewEnumIndexSafety(table, []*planpb.IndexDef{index}), "ENUM prefix index")
	index.Unique = false
	require.Error(t, validateNewEnumIndexSafety(table, []*planpb.IndexDef{index}))

	// A legacy-only prefix index remains valid on a table that also has an
	// unrelated new-identity ENUM column.
	index.Parts = []string{"other"}
	require.NoError(t, validateNewEnumIndexSafety(table, []*planpb.IndexDef{index}))
	// A prefix of a regular text column with a new ENUM remains supported.
	table.Cols = append(table.Cols, &planpb.ColDef{Name: "txt", Typ: planpb.Type{Id: int32(types.T_varchar)}})
	index.Parts = []string{"txt", "e"}
	index.IndexAlgoParams = `{"prefix_lengths":"txt:1"}`
	require.NoError(t, validateNewEnumIndexSafety(table, []*planpb.IndexDef{index}))
	// Malformed prefix metadata is not treated as absence of a prefix.
	index.IndexAlgoParams = `{"prefix_lengths":"other:0"}`
	require.Error(t, validateNewEnumIndexSafety(table, []*planpb.IndexDef{index}))
}

func TestNewEnumIndexRejectsUnverifiedAlgorithms(t *testing.T) {
	spec := enumIndexTestSpec()
	table, index := spec.Source, spec.Index
	index.IndexAlgo = "ivfflat"
	index.Parts = []string{"other"}
	index.IncludedColumns = []string{"other"}
	index.IndexAlgoParams = `{"included_columns":"e"}`
	require.ErrorContains(t, validateNewEnumIndexSafety(table, []*planpb.IndexDef{index}), "index algorithm")
	index.IndexAlgoParams = ""
	index.IncludedColumns = []string{"e"}
	require.ErrorContains(t, validateNewEnumIndexSafety(table, []*planpb.IndexDef{index}), "index algorithm")
	index.IncludedColumns = nil
	require.NoError(t, validateNewEnumIndexSafety(table, []*planpb.IndexDef{index}), "an unrelated legacy plugin index is retained")
	index.IndexAlgoParams = `{"async":"true","included_columns":"e"}`
	require.ErrorContains(t, validateNewEnumIndexSafety(table, []*planpb.IndexDef{index}), "index algorithm", "async plugin indexes are rejected before background dispatch")
	index.IndexAlgoParams = ""
	index.Parts = []string{"e"}
	require.Error(t, validateNewEnumIndexSafety(table, []*planpb.IndexDef{index}))
	index.IndexAlgo = "btree"
	require.NoError(t, validateNewEnumIndexSafety(table, []*planpb.IndexDef{index}), "ordinary proven indexes remain supported")
}

func enumColumnSourceMap(table *planpb.TableDef) map[string]selectExpr {
	sources := make(map[string]selectExpr, len(table.Cols))
	for _, col := range table.Cols {
		if col == nil || col.Hidden {
			continue
		}
		sources[col.Name] = selectExpr{sexprType: exprColumnName, sexprStr: col.Name}
	}
	return sources
}

func TestRemapAlterColumnSourceTracksOriginalSource(t *testing.T) {
	ctx := &AlterTableContext{alterColMap: map[string]selectExpr{
		"e": {sexprType: exprColumnName, sexprStr: "e"},
	}}
	remapAlterColumnSource(ctx, "e", "f")
	remapAlterColumnSource(ctx, "f", "g")
	require.Equal(t, selectExpr{sexprType: exprColumnName, sexprStr: "e"}, ctx.alterColMap["g"])
	require.NotContains(t, ctx.alterColMap, "f")

	ctx.alterColMap["added"] = selectExpr{sexprType: exprConstValue, sexprStr: "NULL"}
	remapAlterColumnSource(ctx, "added", "renamed_added")
	require.Equal(t, selectExpr{sexprType: exprConstValue, sexprStr: "NULL"}, ctx.alterColMap["renamed_added"])

	remapAlterColumnSource(ctx, "missing", "new")
	require.NotContains(t, ctx.alterColMap, "new")
}

func TestRejectUnsafeEnumIdentityDowngrade(t *testing.T) {
	spec := enumIndexTestSpec()
	oldTable := proto.Clone(spec.Source).(*planpb.TableDef)
	oldIndex := proto.Clone(spec.Index).(*planpb.IndexDef)
	oldTable.Indexes = []*planpb.IndexDef{oldIndex}
	sources := enumColumnSourceMap(oldTable)

	newTable := proto.Clone(oldTable).(*planpb.TableDef)
	newTable.Cols[1].Typ.EnumCollation = 0
	require.ErrorContains(t, rejectUnsafeEnumIdentityDowngrade(oldTable, newTable, sources), "legacy ENUM")

	varcharTable := proto.Clone(newTable).(*planpb.TableDef)
	varcharTable.Cols[1].Typ.Id = int32(types.T_varchar)
	require.NoError(t, rejectUnsafeEnumIdentityDowngrade(oldTable, varcharTable, sources), "ENUM to VARCHAR is a distinct conversion")

	withoutIndex := proto.Clone(newTable).(*planpb.TableDef)
	withoutIndex.Indexes = nil
	require.NoError(t, rejectUnsafeEnumIdentityDowngrade(oldTable, withoutIndex, sources), "removing the affected index avoids a downgrade")

	unrelatedIndex := proto.Clone(oldTable).(*planpb.TableDef)
	unrelatedIndex.Indexes[0].Parts = []string{"other"}
	unrelatedTarget := proto.Clone(unrelatedIndex).(*planpb.TableDef)
	unrelatedTarget.Cols[1].Typ.EnumCollation = 0
	require.NoError(t, rejectUnsafeEnumIdentityDowngrade(unrelatedIndex, unrelatedTarget, enumColumnSourceMap(unrelatedIndex)), "an unrelated legacy-only index remains eligible")

	depends, err := indexReferencesColumn(oldTable, oldIndex, "new_column")
	require.NoError(t, err)
	require.False(t, depends, "an added column has no old-index dependency")

	generated := proto.Clone(oldTable).(*planpb.TableDef)
	generated.Cols = append(generated.Cols, &planpb.ColDef{
		Name: "generated", Typ: planpb.Type{Id: int32(types.T_int64)},
		GeneratedCol: &planpb.GeneratedCol{Expr: &planpb.Expr{Expr: &planpb.Expr_Col{
			Col: &planpb.ColRef{RelPos: 0, ColPos: 1},
		}}},
	})
	generated.Indexes[0].Parts = []string{"generated"}
	generatedTarget := proto.Clone(generated).(*planpb.TableDef)
	generatedTarget.Cols[1].Typ.EnumCollation = 0
	require.ErrorContains(t, rejectUnsafeEnumIdentityDowngrade(generated, generatedTarget, enumColumnSourceMap(generated)), "legacy ENUM")

	primaryOnly := proto.Clone(oldTable).(*planpb.TableDef)
	primaryOnly.Indexes = nil
	primaryOnly.Pkey = &planpb.PrimaryKeyDef{PkeyColName: "e", Names: []string{"e"}}
	primaryTarget := proto.Clone(primaryOnly).(*planpb.TableDef)
	primaryTarget.Cols[1].Typ.EnumCollation = types.EnumCollationLegacy
	require.ErrorContains(t, rejectUnsafeEnumIdentityDowngrade(primaryOnly, primaryTarget, enumColumnSourceMap(primaryOnly)), "legacy ENUM")
}

func TestAlterCopyDowngradeRejectsDuplicateColIDAndNameReuse(t *testing.T) {
	mock := newMySQLSpecialOrderMock()
	table := mock.ctxt.tables["enum_order_t"]
	for i, col := range table.Cols {
		col.ColId = 77
		col.Seqnum = uint32(i)
	}
	table.Cols[1].Typ.EnumCollation = types.EnumCollationUTF8Bin
	table.Pkey = &planpb.PrimaryKeyDef{PkeyColName: "id", Names: []string{"id"}}
	table.Indexes = []*planpb.IndexDef{{
		IndexName: "ix_e", Parts: []string{"e", "id"},
	}}
	_, err := runOneStmt(mock, t,
		"alter table enum_order_t change column e f enum('low','mid','high'), change column v e varchar(32)")
	require.ErrorContains(t, err, "ALTER from ENUM collation identity to legacy ENUM with a dependent index")
}

func TestRejectUnsafeEnumIdentityDowngradeAfterRename(t *testing.T) {
	spec := enumIndexTestSpec()
	oldTable := proto.Clone(spec.Source).(*planpb.TableDef)
	for i, col := range oldTable.Cols {
		col.Seqnum = uint32(i)
	}
	oldTable.Cols[0].ColId = 7
	oldTable.Cols[1].ColId = 7
	oldTable.Cols[2].ColId = 7
	oldIndex := proto.Clone(spec.Index).(*planpb.IndexDef)
	oldIndex.IndexName = "e_idx"
	oldTable.Indexes = []*planpb.IndexDef{oldIndex}

	newTable := proto.Clone(oldTable).(*planpb.TableDef)
	newTable.Cols[1].Name = "f"
	newTable.Cols[1].Typ.EnumCollation = types.EnumCollationLegacy
	newTable.Indexes[0].Parts = []string{"f"}
	ctx := &AlterTableContext{alterColMap: enumColumnSourceMap(oldTable)}
	remapAlterColumnSource(ctx, "e", "f")
	require.ErrorContains(t, rejectUnsafeEnumIdentityDowngrade(oldTable, newTable, ctx.alterColMap), "legacy ENUM")

	// Two sequential renames must retain the original COPY source.
	newTable.Cols[1].Name = "g"
	newTable.Indexes[0].Parts = []string{"g"}
	remapAlterColumnSource(ctx, "f", "g")
	require.ErrorContains(t, rejectUnsafeEnumIdentityDowngrade(oldTable, newTable, ctx.alterColMap), "legacy ENUM")

	// Reusing the old name for a different column must not hide the renamed,
	// downgraded ENUM still referenced by the existing index.
	oldTable.Cols[2].Typ.Id = int32(types.T_varchar)
	oldTable.Cols[2].Typ.Enumvalues = ""
	newTable = proto.Clone(oldTable).(*planpb.TableDef)
	newTable.Cols[1].Name = "f"
	newTable.Cols[1].Typ.EnumCollation = types.EnumCollationLegacy
	newTable.Cols[2].Name = "e"
	newTable.Indexes[0].Parts = []string{"f"}
	ctx = &AlterTableContext{alterColMap: enumColumnSourceMap(oldTable)}
	remapAlterColumnSource(ctx, "e", "f")
	remapAlterColumnSource(ctx, "other", "e")
	require.ErrorContains(t, rejectUnsafeEnumIdentityDowngrade(oldTable, newTable, ctx.alterColMap), "legacy ENUM")

	// Dropping and re-adding the same name has no source-column mapping.
	replacement := proto.Clone(oldTable).(*planpb.TableDef)
	replacement.Cols[1].ColId = 99
	replacement.Cols[1].Typ.EnumCollation = types.EnumCollationLegacy
	replacementSources := enumColumnSourceMap(oldTable)
	delete(replacementSources, "e")
	replacementSources["e"] = selectExpr{sexprType: exprConstValue, sexprStr: "NULL"}
	require.NoError(t, rejectUnsafeEnumIdentityDowngrade(oldTable, replacement, replacementSources))
}

func TestDropAlterCopyUniqueDedupForAffectedIndexes(t *testing.T) {
	opt := &planpb.AlterCopyOpt{
		SkipUniqueIdxDedup: map[string]bool{"affected": true, "unaffected": true},
		SkipIndexesCopy:    map[string]bool{"affected": false, "unaffected": true},
	}
	dropAlterCopyUniqueDedupForAffectedIndexes(opt)
	require.NotContains(t, opt.SkipUniqueIdxDedup, "affected")
	require.True(t, opt.SkipUniqueIdxDedup["unaffected"])
}
