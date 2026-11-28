// Copyright 2026 Matrix Origin
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
// http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package mysql

import (
	"context"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/matrixorigin/matrixone/pkg/sql/parsers/dialect"
	"github.com/matrixorigin/matrixone/pkg/sql/parsers/tree"
)

// Review 6094104086 (P3): the partition name lists of PARTITION BY KEY,
// RANGE COLUMNS and LIST COLUMNS must accept the same unquoted END column
// spelling this PR's bare-END compatibility already admits at declaration
// positions - confining the extension to the partition lists, without
// widening the shared column_name_list used by privilege/INCLUDE contexts -
// while ordinary and quoted partition identifiers and the compound-statement
// END delimiter keep their contracts.
func TestReview6094BareEndPartitionNameLists(t *testing.T) {
	ctx := context.Background()
	mustCreate := func(t *testing.T, sql string) {
		t.Helper()
		_, err := ParseOne(ctx, sql, 1)
		require.NoErrorf(t, err, "the bare-END partition shape must parse: %s", sql)
	}
	mustReject := func(t *testing.T, sql string, why string) {
		t.Helper()
		_, err := ParseOne(ctx, sql, 1)
		require.Errorf(t, err, "%s: %s", why, sql)
	}
	// The three valid public SQL table shapes with the unquoted END key.
	mustCreate(t, "create table t_key_end (end int) partition by key(end) partitions 2")
	mustCreate(t, "create table t_range_end (end int) partition by range columns(end) (partition p0 values less than (2), partition p1 values less than (100))")
	mustCreate(t, "create table t_list_end (end int) partition by list columns(end) (partition pe values in (1), partition pd values in (2))")
	// Ordinary and quoted partition identifiers keep their established form.
	mustCreate(t, "create table t_key_plain (v int) partition by key(v) partitions 2")
	mustCreate(t, "create table t_key_quoted (end int) partition by key(`end`) partitions 2")
	// The compound-statement END delimiter is unaffected by the admitted
	// identifier position: the partitioned statement parses on its own and
	// alongside a trailing statement without leaking the END keyword.
	stmts, err := Parse(ctx, "create table t_comp (end int) partition by key(end) partitions 2", 1)
	require.NoError(t, err)
	require.Len(t, stmts, 1)
	_, err = Parse(ctx, "begin create table t_comp2 (end int) partition by key(end) partitions 2; end;", 1)
	require.NoError(t, err, "the compound block keeps END as its delimiter beside the partitioned create")
	// A nested pair of compound blocks reusing the established nested
	// grammar spelling: the partitioned bare-END CREATE lives in the
	// inner block, an ordinary trailing statement stays separate, and both
	// block END delimiters remain attached to block boundaries.
	nested, nerr := Parse(ctx, "begin begin create table t_nest (end int) partition by key(end) partitions 2; end; end; select 7", 1)
	require.NoError(t, nerr, "the nested compound blocks must accept the partitioned bare-END create")
	require.Len(t, nested, 2, "the trailing ordinary statement must remain a separate statement")
	_, isSelect := nested[1].(*tree.Select)
	require.Truef(t, isSelect, "the trailing ordinary statement must stay an ordinary select, not block content")
	// Missing END delimiters are rejected explicitly: one end cannot close
	// two begins, a trailing begin must not be left unclosed, and an inner
	// block without any end must not swallow the partitioned statement.
	mustReject(t, "begin begin create table t_miss_outer (end int) partition by key(end) partitions 2; end;", "a missing outer END must be rejected")
	mustReject(t, "begin begin create table t_miss_final (end int) partition by key(end) partitions 2; end; begin select 1;", "a missing final END must be rejected")
	mustReject(t, "begin begin create table t_miss_inner (end int) partition by key(end) partitions 2;", "a missing inner END must be rejected")
	// The admitted identifier position stays confined: END remains a
	// keyword at table names. (`select end ...` is not a confinement
	// control here: this PR's established bare-END compatibility already
	// admits the ordinary column-reference spelling tree-wide, and the
	// empty-key spelling is likewise admitted by unrelated pre-existing
	// grammar alternatives; both predate and are untouched by the
	// partition-list extension this review addressed.)
	mustReject(t, "create table end (v int)", "END must not be admitted as a table name")

	// Exact AST key identity: the bare END partition key must resolve to
	// the same column name as its backtick-quoted twin on the same table
	// shape, rendered by the canonical partition formatter - not merely
	// share a substring with the table body.
	partitionFragment := func(t *testing.T, sql string) string {
		t.Helper()
		stmt, perr := ParseOne(ctx, sql, 1)
		require.NoError(t, perr)
		part := stmt.(*tree.CreateTable).PartitionOption
		require.NotNil(t, part, "the partitioned create must carry a PartitionBy")
		fc := tree.NewFmtCtx(dialect.MYSQL)
		part.PartBy.PType.Format(fc)
		return fc.String()
	}
	twins := []struct {
		label, bare, quoted, canonical string
	}{
		{"KEY",
			"create table t_tw (end int) partition by key(end) partitions 2",
			"create table t_tw (end int) partition by key(`end`) partitions 2",
			"key algorithm = 2 (end)"},
		{"RANGE COLUMNS",
			"create table t_tw (end int) partition by range columns(end) (partition p0 values less than (2), partition p1 values less than (100))",
			"create table t_tw (end int) partition by range columns(`end`) (partition p0 values less than (2), partition p1 values less than (100))",
			"range columns (end)"},
		{"LIST COLUMNS",
			"create table t_tw (end int) partition by list columns(end) (partition pe values in (1), partition pd values in (2))",
			"create table t_tw (end int) partition by list columns(`end`) (partition pe values in (1), partition pd values in (2))",
			"list columns (end)"},
	}
	for _, tw := range twins {
		fragBare := partitionFragment(t, tw.bare)
		fragQuoted := partitionFragment(t, tw.quoted)
		require.Equalf(t, tw.canonical, fragBare, "%s bare-END partition key must render its exact canonical identity", tw.label)
		require.Equalf(t, fragBare, fragQuoted, "%s bare-END and quoted twins must carry the identical resolved AST key", tw.label)
	}
}
