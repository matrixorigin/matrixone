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

package frontend

import (
	"context"
	"strings"
	"testing"

	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/sql/parsers"
	"github.com/matrixorigin/matrixone/pkg/sql/parsers/dialect"
	"github.com/matrixorigin/matrixone/pkg/util/trace/impl/motrace/statistic"
	"github.com/stretchr/testify/require"
)

func TestRebaseTextLiteral(t *testing.T) {
	for _, tc := range []struct{ input, want string }{
		{"", "cast(unhex('') as char)"},
		{"a\\b'c", "cast(unhex('615c622763') as char)"},
		{"\u4e2d", "cast(unhex('e4b8ad') as char)"},
	} {
		require.Equal(t, tc.want, rebaseTextLiteral(tc.input))
	}
}

func TestRebaseDeltaProbeSQL(t *testing.T) {
	reads := [4]rebaseSQLRead{{"bf", 1}, {"f", 2}, {"bu", 3}, {"u", 4}}
	columns := []rebaseSQLColumn{
		{"a", types.T_int64.ToType(), true},
		{"b", types.T_int32.ToType(), true},
		{"v", types.T_varchar.ToType(), false},
	}
	query, err := buildRebaseDeltaProbeSQL(t.Context(), "t", reads, columns, [][]string{{"1", "2"}, {"3", "4"}})
	require.NoError(t, err)
	require.Equal(t, 3, strings.Count(query, "select * from "))
	require.Equal(t, 3, strings.Count(query, " where "))
	for _, alias := range []string{"bf", "f", "u"} {
		require.Contains(t, query, "`"+alias+"`.`t`{MO_TS=")
	}
	require.NotContains(t, query, "`bu`.`t`") // Bases were checked before delta probing.
	require.Contains(t, query, "serial(f.`v`)")
	require.Contains(t, query, "coalesce(f.`a`, u.`a`)")
	stmts, err := parsers.Parse(t.Context(), dialect.MYSQL, query, 0)
	require.NoError(t, err)
	require.Len(t, stmts, 1)
	for _, stmt := range stmts {
		stmt.Free()
	}
	for _, keys := range [][][]string{nil, {{"1"}}, {{"1", ""}}} {
		_, err = buildRebaseDeltaProbeSQL(t.Context(), "t", reads, columns, keys)
		require.Error(t, err)
	}
	_, err = buildRebaseDeltaProbeSQL(t.Context(), "t", reads, columns[2:], [][]string{{"1"}})
	require.Error(t, err)
}

func TestRebaseApplyQueries(t *testing.T) {
	plan := rebaseTableSQL{deleteRows: "delete", insertRows: "insert"}
	for _, tc := range []struct {
		name                   string
		insert, update, delete uint64
		want                   []string
	}{
		{"unchanged", 0, 0, 0, []string{}},
		{"insert", 1, 0, 0, []string{"insert"}},
		{"delete", 0, 0, 1, []string{"delete"}},
		{"update", 0, 1, 0, []string{"delete", "insert"}},
		{"mixed", 1, 1, 1, []string{"delete", "insert"}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			counts := []uint64{99, 99, 0, tc.insert, tc.update, tc.delete}
			require.Equal(t, tc.want, rebaseApplyQueries(plan, counts))
		})
	}
}

func TestRebaseSameTableSnapshot(t *testing.T) {
	left := rebaseSQLRead{"left", 11}
	right := rebaseSQLRead{"right", 11}
	require.True(t, rebaseSameTableSnapshot(1, 1, left, right))
	require.False(t, rebaseSameTableSnapshot(1, 2, left, right))
	require.False(t, rebaseSameTableSnapshot(0, 0, left, right))
	right.timestamp++
	require.False(t, rebaseSameTableSnapshot(1, 1, left, right))
	left.timestamp, right.timestamp = 0, 0
	require.False(t, rebaseSameTableSnapshot(1, 1, left, right))
}

func TestRebasePhaseObserverLifetime(t *testing.T) {
	var observations []RebasePhaseObservation
	restore := SetRebasePhaseHookForTest(func(event RebasePhaseObservation) {
		observations = append(observations, event)
	})
	defer restore()
	finish := beginRebasePhase("base_check", "t", "select 1")
	finish(nil)
	finish(context.Canceled)
	require.Len(t, observations, 1)
	require.Equal(t, "t", observations[0].Table)
	require.False(t, observations[0].Failed)
	beginRebasePhase("changes", "t", "select 2")(context.Canceled)
	require.Len(t, observations, 2)
	require.True(t, observations[1].Failed)
	restore()
	beginRebasePhase("disabled", "", "")(nil)
	require.Len(t, observations, 2)
}

type rebaseStatsOnlyExec struct {
	BackgroundExec
	stats statistic.StatsArray
	reads int
}

func (e *rebaseStatsOnlyExec) GetExecStatsArray() statistic.StatsArray {
	e.reads++
	return e.stats
}

func TestRebasePhaseStatsSnapshots(t *testing.T) {
	var got RebasePhaseObservation
	restore := SetRebasePhaseHookForTest(func(event RebasePhaseObservation) { got = event })
	defer restore()
	before := *statistic.NewStatsArray().WithMemorySize(100).WithS3ReadBytes(40)
	after := *statistic.NewStatsArray().WithMemorySize(100).WithS3ReadBytes(65)
	exec := &rebaseStatsOnlyExec{stats: before}
	finish := beginRebaseStatementPhase(exec, "changes", "t", "select 1")
	exec.stats = after
	finish(nil)
	finish(nil)
	require.Equal(t, 2, exec.reads)
	require.Equal(t, before, *got.StatsBefore)
	require.Equal(t, after, *got.StatsAfter)
	exec.stats = statistic.StatsArray{}
	require.Equal(t, after, *got.StatsAfter, "observations own value snapshots")
	restore()
	beginRebaseStatementPhase(exec, "disabled", "", "")(nil)
	require.Equal(t, 2, exec.reads, "disabled diagnostics must not read resource counters")
}

func TestRebaseFourBagMismatchLowering(t *testing.T) {
	reads := []rebaseSQLRead{{"b", 11}, {"f", 22}, {"x", 33}, {"u", 44}}
	for _, typ := range []types.T{types.T_varchar, types.T_int64, types.T_float64, types.T_decimal128} {
		columns := []rebaseSQLColumn{{"n0", typ.ToType(), false}}
		query, err := buildRebaseBagsMismatchSQL(t.Context(), "t`1", reads, columns)
		require.NoError(t, err)
		stmt, err := parsers.ParseOne(t.Context(), dialect.MYSQL, query, 1)
		require.NoError(t, err)
		stmt.Free()
		require.Equal(t, 3, strings.Count(query, " union all "))
		require.Equal(t, 4, strings.Count(query, "{MO_TS="))
		require.Contains(t, query, "having sum(n0) <> 0 or sum(n1) <> 0 or sum(n2) <> 0")
		require.NotContains(t, query, "join")
	}
	_, err := buildRebaseBagsMismatchSQL(t.Context(), "t", reads[:3], []rebaseSQLColumn{{"v", types.T_int64.ToType(), false}})
	require.Error(t, err)
	reads[3].timestamp = 0
	_, err = buildRebaseBagsMismatchSQL(t.Context(), "t", reads, []rebaseSQLColumn{{"v", types.T_int64.ToType(), false}})
	require.Error(t, err)
}

func TestRebaseSQLLowering(t *testing.T) {
	reads := [4]rebaseSQLRead{{"feature_base", 11}, {"feature", 22}, {"upstream_base", 33}, {"upstream", 44}}
	for _, pkType := range []types.T{types.T_int64, types.T_varchar, types.T_float32, types.T_float64} {
		for _, policy := range []rebaseConflictPolicy{rebaseFail, rebaseKeepUpstream, rebaseKeepBranch} {
			columns := []rebaseSQLColumn{{"id", pkType.ToType(), true}, {"part", types.T_int32.ToType(), true}, {"value", types.T_varchar.ToType(), false}}
			plan, err := buildRebaseTableSQL(t.Context(), "t`1", "new`db", reads, columns, policy)
			require.NoError(t, err)
			for _, sql := range []string{plan.baseMismatch, plan.changes, plan.deleteRows, plan.insertRows} {
				stmt, err := parsers.ParseOne(context.Background(), dialect.MYSQL, sql, 1)
				require.NoError(t, err, sql)
				stmt.Free()
				require.Contains(t, sql, "{MO_TS=11}")
			}
			require.Contains(t, plan.baseMismatch, "{MO_TS=33}")
			require.NotContains(t, plan.baseMismatch, "{MO_TS=22}")
			require.NotContains(t, plan.baseMismatch, "{MO_TS=44}")
			for _, sql := range []string{plan.changes, plan.deleteRows, plan.insertRows} {
				require.Contains(t, sql, "{MO_TS=22}")
				require.Contains(t, sql, "{MO_TS=44}")
				require.NotContains(t, sql, "{MO_TS=33}", "the separate correspondence proof must precede replay")
			}
			require.Contains(t, plan.changes, "b.`id` is null and f.`id` is null")
			require.Contains(t, plan.changes, "serial(f.`value`) <=> serial(u.`value`)")
			require.Contains(t, plan.deleteRows, "`new``db`.`t``1`")
			require.Contains(t, plan.insertRows, "(`id`, `part`, `value`)")
			require.Contains(t, plan.insertRows, "f.`id` is not null")
			if pkType == types.T_float32 || pkType == types.T_float64 {
				require.Contains(t, plan.baseMismatch, "serial(b.`id`) = serial(x.`id`)")
				require.Contains(t, plan.changes, "serial(coalesce(b.`id`, f.`id`)) = serial(u.`id`)")
				require.Contains(t, plan.deleteRows, "serial(d.`id`) = serial(r.k0)")
			}
		}
	}
}

func TestRebaseSQLTextValueLowering(t *testing.T) {
	reads := [4]rebaseSQLRead{{"b", 1}, {"f", 2}, {"x", 3}, {"u", 4}}
	for _, typ := range []types.T{types.T_char, types.T_varchar, types.T_text, types.T_binary, types.T_varbinary, types.T_blob} {
		columns := []rebaseSQLColumn{{"id", typ.ToType(), true}, {"v", typ.ToType(), false}}
		plan, err := buildRebaseTableSQL(t.Context(), "t", "dst", reads, columns, rebaseFail)
		require.NoError(t, err)
		require.Contains(t, plan.changes, "b.`id` = f.`id`")
		require.Contains(t, plan.changes, "serial(b.`id`) <=> serial(f.`id`)")
		require.Contains(t, plan.changes, "serial(f.`v`) <=> serial(u.`v`)")
		bag, err := buildRebaseBagMismatchSQL(t.Context(), "t", reads[0], reads[1], columns[1:])
		require.NoError(t, err)
		require.Contains(t, bag, "serial(`v`) as c0")
		require.Contains(t, bag, "group by serial(`v`)")
		for _, sql := range []string{plan.changes, bag} {
			stmt, err := parsers.ParseOne(context.Background(), dialect.MYSQL, sql, 1)
			require.NoError(t, err, sql)
			stmt.Free()
		}
	}
}

func TestRebaseSQLFloatValueLowering(t *testing.T) {
	reads := [4]rebaseSQLRead{{"b", 1}, {"f", 2}, {"x", 3}, {"u", 4}}
	for _, typ := range []types.T{types.T_float32, types.T_float64} {
		columns := []rebaseSQLColumn{{"id", types.T_int64.ToType(), true}, {"v", typ.ToType(), false}}
		plan, err := buildRebaseTableSQL(t.Context(), "t", "dst", reads, columns, rebaseFail)
		require.NoError(t, err)
		require.Contains(t, plan.changes, "serial(f.`v`) <=> serial(u.`v`)")
		require.Contains(t, plan.baseMismatch, "serial(b.`v`) <=> serial(x.`v`)")
		bag, err := buildRebaseBagMismatchSQL(t.Context(), "t", reads[0], reads[1], columns[1:])
		require.NoError(t, err)
		require.Contains(t, bag, "serial(`v`) as c0")
		require.Contains(t, bag, "group by serial(`v`)")
		for _, sql := range []string{plan.changes, bag} {
			stmt, err := parsers.ParseOne(context.Background(), dialect.MYSQL, sql, 1)
			require.NoError(t, err, sql)
			stmt.Free()
		}
	}
}

func TestRebaseSQLRejectsInvalidLayout(t *testing.T) {
	reads := [4]rebaseSQLRead{{"b", 1}, {"f", 2}, {"x", 3}, {"u", 4}}
	columns := []rebaseSQLColumn{{"id", types.T_int64.ToType(), true}}
	for _, tc := range []struct {
		name, table, target string
		reads               [4]rebaseSQLRead
		columns             []rebaseSQLColumn
		policy              rebaseConflictPolicy
	}{
		{name: "empty table", target: "dst", reads: reads, columns: columns},
		{name: "empty target", table: "t", reads: reads, columns: columns},
		{name: "source overwrite", table: "t", target: "F", reads: reads, columns: columns},
		{name: "no columns", table: "t", target: "dst", reads: reads},
		{name: "no key", table: "t", target: "dst", reads: reads, columns: []rebaseSQLColumn{{"v", types.T_int64.ToType(), false}}},
		{name: "duplicate column", table: "t", target: "dst", reads: reads, columns: append(append([]rebaseSQLColumn{}, columns...), rebaseSQLColumn{"ID", types.T_int64.ToType(), false})},
		{name: "unfixed source", table: "t", target: "dst", reads: [4]rebaseSQLRead{{"b", 1}, {"f", 0}, {"x", 3}, {"u", 4}}, columns: columns},
		{name: "invalid policy", table: "t", target: "dst", reads: reads, columns: columns, policy: 99},
		{name: "NUL identifier", table: "t\x00", target: "dst", reads: reads, columns: columns},
	} {
		t.Run(tc.name, func(t *testing.T) {
			plan, err := buildRebaseTableSQL(t.Context(), tc.table, tc.target, tc.reads, tc.columns, tc.policy)
			require.Error(t, err)
			require.Equal(t, rebaseTableSQL{}, plan)
		})
	}
}

func TestRebaseSQLConflictPolicies(t *testing.T) {
	reads := [4]rebaseSQLRead{{"b", 1}, {"f", 2}, {"x", 3}, {"u", 4}}
	columns := []rebaseSQLColumn{{"id", types.T_int64.ToType(), true}}
	fail, err := buildRebaseTableSQL(t.Context(), "t", "dst", reads, columns, rebaseFail)
	require.NoError(t, err)
	branch, err := buildRebaseTableSQL(t.Context(), "t", "dst", reads, columns, rebaseKeepBranch)
	require.NoError(t, err)
	upstream, err := buildRebaseTableSQL(t.Context(), "t", "dst", reads, columns, rebaseKeepUpstream)
	require.NoError(t, err)
	require.Equal(t, fail.deleteRows, branch.deleteRows, "FAIL differs by mandatory pre-apply conflict rejection")
	require.Equal(t, fail.insertRows, branch.insertRows)
	require.Greater(t, strings.Count(upstream.deleteRows, "and not"), strings.Count(branch.deleteRows, "and not"))
	require.Greater(t, strings.Count(upstream.insertRows, "and not"), strings.Count(branch.insertRows, "and not"))
}
