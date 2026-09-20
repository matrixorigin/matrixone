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
	"strings"
	"testing"

	"github.com/gogo/protobuf/proto"
	"github.com/matrixorigin/matrixone/pkg/catalog"
	"github.com/matrixorigin/matrixone/pkg/defines"
	planpb "github.com/matrixorigin/matrixone/pkg/pb/plan"
	"github.com/matrixorigin/matrixone/pkg/sql/parsers/dialect/mysql"
	"github.com/matrixorigin/matrixone/pkg/sql/parsers/tree"
	"github.com/stretchr/testify/require"
)

func TestAlterCopyIndexRenameLineage(t *testing.T) {
	original := &TableDef{Indexes: []*IndexDef{
		{IndexName: "a", IndexTableName: "physical_a", Parts: []string{"v"}, Unique: true},
		{IndexName: "b", IndexTableName: "physical_b1", IndexAlgo: "hnsw", IndexAlgoTableType: "metadata"},
		{IndexName: "b", IndexTableName: "physical_b2", IndexAlgo: "hnsw", IndexAlgoTableType: "storage", IndexAlgoParams: `{"session_vars":{"x":1}}`},
	}}
	copied := DeepCopyTableDef(original, true)
	for _, pair := range [][2]string{{"A", "tmp"}, {"b", "a"}, {"tmp", "b"}} {
		require.NoError(t, renameCopyIndex(context.Background(), copied, &tree.AlterTableRenameIndexClause{OldName: pair[0], NewName: pair[1]}))
	}
	mapping, err := AlterCopyIndexRenames(original, copied)
	require.NoError(t, err)
	require.Equal(t, map[string]string{"a": "b", "b": "a"}, mapping)
	require.Equal(t, "a", original.Indexes[0].IndexName)
	require.Equal(t, "b", original.Indexes[2].IndexName)
	for _, mutate := range []func(*TableDef){
		func(d *TableDef) { d.Indexes = d.Indexes[:2] },
		func(d *TableDef) { d.Indexes[2].IndexName = "split" },
		func(d *TableDef) { d.Indexes[0].IndexName = "a" },
		func(d *TableDef) { d.Indexes[2].IndexAlgoParams = "changed" },
		func(d *TableDef) { d.Indexes[2] = nil },
	} {
		bad := DeepCopyTableDef(copied, true)
		mutate(bad)
		_, err := AlterCopyIndexRenames(original, bad)
		require.Error(t, err)
	}
	for _, other := range []*TableDef{
		{}, {Indexes: original.Indexes[:1]},
		{Indexes: []*IndexDef{{IndexName: "new", IndexTableName: "new_physical"}}},
		DeepCopyTableDef(original, true),
	} {
		mapping, err := AlterCopyIndexRenames(original, other)
		require.NoError(t, err)
		require.Nil(t, mapping)
	}
	for _, target := range []string{"a", "A"} {
		copy := DeepCopyTableDef(original, true)
		require.NoError(t, renameCopyIndex(context.Background(), copy, &tree.AlterTableRenameIndexClause{OldName: "a", NewName: target}))
		mapping, err := AlterCopyIndexRenames(original, copy)
		require.NoError(t, err)
		if target == "a" {
			require.Nil(t, mapping)
		} else {
			require.Equal(t, map[string]string{"a": "A"}, mapping)
		}
	}
}

func TestRenameCopyIndexErrors(t *testing.T) {
	for _, pair := range [][2]string{{"missing", "x"}, {"a", "B"}, {"PRIMARY", "x"}, {"a", "primary"}, {"a", ""}, {"a", "bad "}, {"a", strings.Repeat("x", 65)}} {
		table := &TableDef{Indexes: []*IndexDef{{IndexName: "a"}, {IndexName: "b"}}}
		before := proto.Clone(table).(*TableDef)
		require.Error(t, renameCopyIndex(context.Background(), table, &tree.AlterTableRenameIndexClause{OldName: pair[0], NewName: pair[1]}))
		require.True(t, proto.Equal(before, table), "source definition mutated")
	}
	// Parser normalization uses lower-case, not Unicode simple folding: final
	// sigma and ordinary sigma are distinct existing index names.
	table := &TableDef{Indexes: []*IndexDef{{IndexName: "σ"}, {IndexName: "ς"}}}
	require.NoError(t, renameCopyIndex(context.Background(), table, &tree.AlterTableRenameIndexClause{OldName: "Σ", NewName: "renamed"}))
	require.Equal(t, "renamed", table.Indexes[0].IndexName)
	require.Equal(t, "ς", table.Indexes[1].IndexName)
}

func TestRenameIndexCopyPlan(t *testing.T) {
	for _, suffix := range []string{"", ", ALGORITHM=COPY, LOCK=SHARED", ", RENAME KEY renamed TO idx"} {
		mock := newAutoIncrementAlterOptimizer()
		table := mock.ctxt.tables["auto_incr_t"]
		// Reuse the fixture with fixed-visible system metadata, avoiding catalog IO.
		table.DbName = "mo_catalog"
		table.Indexes = []*IndexDef{{IndexName: "idx", IndexTableName: "physical_idx", Parts: []string{"v"}, Unique: true, TableExist: true}, {IndexName: "other", IndexTableName: "physical_other", Parts: []string{"id"}, TableExist: true}}
		before := proto.Clone(table).(*TableDef)
		p, err := buildSingleStmt(mock, t, "ALTER TABLE mo_catalog.auto_incr_t RENAME INDEX idx TO renamed"+suffix)
		require.NoError(t, err)
		alter := p.GetDdl().GetAlterTable()
		require.Equal(t, planpb.AlterTable_COPY, alter.AlgorithmType)
		require.Empty(t, alter.Options.SkipIndexesCopy)
		require.Empty(t, alter.Options.SkipUniqueIdxDedup)
		require.Contains(t, alter.AffectedCols, "other")
		require.Contains(t, alter.AffectedCols, alter.CopyTableDef.Indexes[0].IndexName)
		require.True(t, proto.Equal(before, table), "source definition mutated")
		_, err = AlterCopyIndexRenames(alter.TableDef, alter.CopyTableDef)
		require.NoError(t, err)
	}
	for _, suffix := range []string{", ADD COLUMN x INT", ", DROP INDEX idx", ", RENAME TO another", ", ALGORITHM=INPLACE", ", ALGORITHM=INSTANT", ", LOCK=NONE"} {
		stmt, err := mysql.ParseOne(context.Background(), "ALTER TABLE t RENAME INDEX idx TO renamed"+suffix, 1)
		require.NoError(t, err)
		options := stmt.(*tree.AlterTable).Options
		algorithm, err := ResolveAlterTableAlgorithm(context.Background(), options, &TableDef{})
		if err == nil {
			err = resolveAndValidateLock(context.Background(), options, algorithm)
		}
		require.Error(t, err)
		stmt.Free()
	}
}

func TestCopyCreatePreservesIndexSessionVars(t *testing.T) {
	source := &TableDef{Indexes: []*IndexDef{{
		IndexName: "renamed", IndexTableName: "old_physical",
		IndexAlgoParams: `{"session_vars":{"cfg":{"probe_limit":5},"enabled":true}}`,
	}}}
	mock := newAutoIncrementAlterOptimizer()
	mock.ctxt.SetContext(context.WithValue(mock.ctxt.GetContext(), defines.AlterCopySourceTableKey{}, source))
	p, err := buildSingleStmt(mock, t, "CREATE TABLE copy_target (id int primary key, v varchar(10), KEY renamed(v), KEY untouched(id))")
	require.NoError(t, err)
	indexes := p.GetDdl().GetCreateTable().GetTableDef().Indexes
	require.Len(t, indexes, 2)
	vars, err := catalog.IndexParamsSessionVars(indexes[0].IndexAlgoParams)
	require.NoError(t, err)
	expected, err := catalog.IndexParamsSessionVars(source.Indexes[0].IndexAlgoParams)
	require.NoError(t, err)
	require.Equal(t, expected, vars)
	require.NotEqual(t, "old_physical", indexes[0].IndexTableName)
	vars, err = catalog.IndexParamsSessionVars(indexes[1].IndexAlgoParams)
	require.NoError(t, err)
	require.Empty(t, vars)
	require.Equal(t, `{"session_vars":{"cfg":{"probe_limit":5},"enabled":true}}`, source.Indexes[0].IndexAlgoParams)
	// Invalid captured metadata fails planning, before catalog/plugin execution.
	source.Indexes[0].IndexAlgoParams = `{"session_vars":`
	_, err = buildSingleStmt(mock, t, "CREATE TABLE another_copy (id int primary key, v varchar(10), KEY renamed(v))")
	require.Error(t, err)
}
