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

package compile

import (
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/container/vector"
	"github.com/matrixorigin/matrixone/pkg/pb/plan"
	"github.com/matrixorigin/matrixone/pkg/sql/parsers"
	"github.com/matrixorigin/matrixone/pkg/sql/parsers/dialect"
	"github.com/matrixorigin/matrixone/pkg/sql/parsers/tree"
	plan2 "github.com/matrixorigin/matrixone/pkg/sql/plan"
	"github.com/matrixorigin/matrixone/pkg/testutil"
)

func parseAlterSequence(t *testing.T, sql string) *tree.AlterSequence {
	t.Helper()
	stmt, err := parsers.ParseOne(testutil.NewProc(t).Ctx, dialect.MYSQL, sql, 1)
	require.NoError(t, err)
	// The statement comes from the reuse pool; callers consume the AST
	// immediately, so it must not be returned to the pool here.
	alter, ok := stmt.(*tree.AlterSequence)
	require.True(t, ok, "%q did not parse as AlterSequence", sql)
	return alter
}

type sequenceTestInteger interface {
	int16 | int32 | int64 | uint16 | uint32 | uint64
}

// sequenceRow builds the single sequence-table row the way
// Scope.AlterSequence reads it via SELECT *.
func sequenceRow[T sequenceTestInteger](lastSeq, minV, maxV, startV T, incr int64, cycle, isCalled bool) []interface{} {
	return []interface{}{lastSeq, minV, maxV, startV, incr, cycle, isCalled}
}

// Issue #30371: ALTER SEQUENCE from a session that never called NEXTVAL (no
// curval cache) must not reset is_called to false; otherwise the next NEXTVAL
// replays the last issued value and sequence-backed default inserts hit
// duplicate-key errors.
func TestMakeAlterSequenceParamPreservesIssuedStateWithoutCurval(t *testing.T) {
	ctx := testutil.NewProc(t).Ctx

	t.Run("cross-session alter preserves called state and last value", func(t *testing.T) {
		stmt := parseAlterSequence(t, "ALTER SEQUENCE s INCREMENT BY 2")
		// stored row: last=1, increment changed to 2, sequence already issued
		row := sequenceRow[int64](1, 1, 100, 1, 1, false, true)

		last, incr, _, _, _, _, isCalled, err := makeAlterSequenceParam[int64](ctx, stmt, row, "")
		require.NoError(t, err)
		require.Equal(t, int64(2), incr)
		require.Equal(t, int64(1), last, "last value must not be pre-advanced without a curval cache")
		require.True(t, isCalled, "is_called must be preserved so NEXTVAL advances from the last issued value")
	})

	t.Run("cross-session alter on a never-issued sequence stays not-called", func(t *testing.T) {
		stmt := parseAlterSequence(t, "ALTER SEQUENCE s INCREMENT BY 2")
		// stored row: fresh sequence, nothing issued yet
		row := sequenceRow[int64](1, 1, 100, 1, 1, false, false)

		last, _, _, _, _, _, isCalled, err := makeAlterSequenceParam[int64](ctx, stmt, row, "")
		require.NoError(t, err)
		require.Equal(t, int64(1), last)
		require.False(t, isCalled, "a fresh sequence must keep is_called=false so the first NEXTVAL returns the start value")
	})

	t.Run("same-session alter keeps the pre-advance behavior", func(t *testing.T) {
		stmt := parseAlterSequence(t, "ALTER SEQUENCE s INCREMENT BY 2")
		row := sequenceRow[int64](1, 1, 100, 1, 1, false, true)

		last, incr, _, _, _, _, isCalled, err := makeAlterSequenceParam[int64](ctx, stmt, row, "1")
		require.NoError(t, err)
		require.Equal(t, int64(2), incr)
		require.Equal(t, int64(3), last, "the curval path still pre-advances the stored last value")
		require.False(t, isCalled, "the curval path keeps writing is_called=false as before")
	})
}

// The full sequence-backed insert from #30371: connection A issues value 1,
// connection B alters the increment to 2 with no prior NEXTVAL in B, and the
// next NEXTVAL must return 3 (last issued value 1 + new increment 2), not
// replay 1.
func TestAlterSequenceParamMatchesIssue30371Scenario(t *testing.T) {
	ctx := testutil.NewProc(t).Ctx
	stmt := parseAlterSequence(t, "ALTER SEQUENCE s INCREMENT BY 2")

	// after connection A: sequence row (last=1, incr=1, is_called=true)
	row := sequenceRow[int64](1, 1, 9223372036854775807, 1, 1, false, true)
	last, incr, _, _, _, _, isCalled, err := makeAlterSequenceParam[int64](ctx, stmt, row, "")
	require.NoError(t, err)
	require.Equal(t, int64(1), last)
	require.Equal(t, int64(2), incr)
	require.True(t, isCalled)
	// next NEXTVAL over this state advances with the new increment: 1+2=3,
	// which no existing row holds, so the default insert succeeds.
}

// Exercise the batch owner, not just its parameter helper: is_called must reach
// the persisted vector for every supported signed and unsigned sequence type.
func TestSequenceAlterBatchPreservesIssuedState(t *testing.T) {
	t.Run("int16", func(t *testing.T) { testSequenceAlterBatch[int16](t, types.T_int16) })
	t.Run("int32", func(t *testing.T) { testSequenceAlterBatch[int32](t, types.T_int32) })
	t.Run("int64", func(t *testing.T) { testSequenceAlterBatch[int64](t, types.T_int64) })
	t.Run("uint16", func(t *testing.T) { testSequenceAlterBatch[uint16](t, types.T_uint16) })
	t.Run("uint32", func(t *testing.T) { testSequenceAlterBatch[uint32](t, types.T_uint32) })
	t.Run("uint64", func(t *testing.T) { testSequenceAlterBatch[uint64](t, types.T_uint64) })
}

func testSequenceAlterBatch[T sequenceTestInteger](t *testing.T, oid types.T) {
	t.Helper()
	for _, tc := range []struct {
		name, curval string
		storedCalled bool
		last         T
		called       bool
	}{
		{"cross-session issued", "", true, 1, true},
		{"cross-session fresh", "", false, 1, false},
		{"same-session issued", "1", true, 3, false},
	} {
		t.Run(tc.name, func(t *testing.T) {
			proc := testutil.NewProcess(t)
			defer proc.Free()
			baseline := proc.Mp().CurrNB()
			stmt := parseAlterSequence(t, "ALTER SEQUENCE s INCREMENT BY 2")
			row := sequenceRow[T](1, 1, 100, 1, 1, true, tc.storedCalled)
			table := &plan.TableDef{Cols: []*plan.ColDef{{Typ: plan.Type{Id: int32(oid)}}}}
			bat, _, err := makeSequenceAlterBatch(proc.Ctx, stmt, table, proc, row, tc.curval)
			require.NoError(t, err)
			require.NotNil(t, bat)
			defer func() {
				bat.Clean(proc.Mp())
				require.Equal(t, baseline, proc.Mp().CurrNB(), "all batch vectors must be released")
			}()
			require.Equal(t, 1, bat.RowCount())
			require.Equal(t, []string{"last_seq_num", "min_value", "max_value", "start_value", "increment_value", "cycle", "is_called"}, bat.Attrs)
			require.Len(t, bat.Vecs, 7)
			for i, want := range []T{tc.last, 1, 100, 1} {
				require.Equal(t, oid, bat.Vecs[i].GetType().Oid)
				require.Equal(t, []T{want}, vector.MustFixedColWithTypeCheck[T](bat.Vecs[i]))
			}
			require.Equal(t, []int64{2}, vector.MustFixedColWithTypeCheck[int64](bat.Vecs[4]))
			require.Equal(t, []bool{true}, vector.MustFixedColWithTypeCheck[bool](bat.Vecs[5]))
			require.Equal(t, []bool{tc.called}, vector.MustFixedColWithTypeCheck[bool](bat.Vecs[6]))
			require.Equal(t, sequenceRow[T](1, 1, 100, 1, 1, true, tc.storedCalled), row, "building a batch must not mutate its input row")
		})
	}
}

func TestSequenceAlterBatchRejectsInvalidValues(t *testing.T) {
	for _, tc := range []struct {
		name, sql, message string
	}{
		{"zero increment", "ALTER SEQUENCE s INCREMENT BY 0", "must not be 0"},
		{"unsigned increment", "ALTER SEQUENCE s INCREMENT BY 9223372036854775808", "data type is int64"},
		{"invalid bounds", "ALTER SEQUENCE s MINVALUE 100", "must be bigger than MINVALUE"},
		{"invalid start", "ALTER SEQUENCE s START WITH 101", "must between MINVALUE"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			proc := testutil.NewProcess(t)
			defer proc.Free()
			baseline := proc.Mp().CurrNB()
			row := sequenceRow[int64](1, 1, 100, 1, 1, false, true)
			table := &plan.TableDef{Cols: []*plan.ColDef{{Typ: plan.Type{Id: int32(types.T_int64)}}}}
			bat, _, err := makeSequenceAlterBatch(proc.Ctx, parseAlterSequence(t, tc.sql), table, proc, row, "")
			require.ErrorContains(t, err, tc.message)
			require.Nil(t, bat)
			require.Equal(t, baseline, proc.Mp().CurrNB(), "rejected input must not retain a partial batch")
			require.Equal(t, sequenceRow[int64](1, 1, 100, 1, 1, false, true), row)
		})
	}
}

// Issue #30679: a START WITH clause restarts the sequence only when it is
// strictly ahead of the stored position in the direction of the effective
// increment (ascending n > f, descending n < f); the restart persists
// (last=n, is_called=false, start_value=n) so every session's next NEXTVAL
// returns exactly n. A start at or behind the position keeps
// (last_seq_num, is_called) and only records the direction-aware frontier
// (ascending max(n, f), descending min(n, f)) as start_value: clamped-behind
// starts stay accepted but never re-enter a consumed range. Matrix:
// {ascending, descending} x {called, uncalled} x {ahead, equal, behind}.
func TestMakeAlterSequenceParamStartWithRestartMatrix(t *testing.T) {
	ctx := testutil.NewProc(t).Ctx

	for _, tc := range []struct {
		name   string
		sql    string
		row    []interface{}
		curval string
		last   int64
		start  int64
		called bool
	}{
		// ascending, increment +2, stored position last=10
		{"ascending called ahead restarts", "ALTER SEQUENCE s START WITH 20", sequenceRow[int64](10, 1, 100, 1, 2, false, true), "", 20, 20, false},
		{"ascending called equal keeps position", "ALTER SEQUENCE s START WITH 10", sequenceRow[int64](10, 1, 100, 1, 2, false, true), "", 10, 10, true},
		{"ascending called behind keeps position", "ALTER SEQUENCE s START WITH 5", sequenceRow[int64](10, 1, 100, 1, 2, false, true), "", 10, 10, true},
		{"ascending uncalled ahead restarts", "ALTER SEQUENCE s START WITH 20", sequenceRow[int64](10, 1, 100, 1, 2, false, false), "", 20, 20, false},
		{"ascending uncalled equal keeps position", "ALTER SEQUENCE s START WITH 10", sequenceRow[int64](10, 1, 100, 1, 2, false, false), "", 10, 10, false},
		{"ascending uncalled behind keeps position and records the declared start", "ALTER SEQUENCE s START WITH 5", sequenceRow[int64](10, 1, 100, 1, 2, false, false), "", 10, 5, false},
		// descending, increment -2, stored position last=10
		{"descending called ahead restarts", "ALTER SEQUENCE s START WITH 5", sequenceRow[int64](10, -100, 100, 100, -2, false, true), "", 5, 5, false},
		{"descending called equal keeps position", "ALTER SEQUENCE s START WITH 10", sequenceRow[int64](10, -100, 100, 100, -2, false, true), "", 10, 10, true},
		{"descending called behind keeps position", "ALTER SEQUENCE s START WITH 15", sequenceRow[int64](10, -100, 100, 100, -2, false, true), "", 10, 10, true},
		{"descending uncalled ahead restarts", "ALTER SEQUENCE s START WITH 5", sequenceRow[int64](10, -100, 100, 100, -2, false, false), "", 5, 5, false},
		{"descending uncalled equal keeps position", "ALTER SEQUENCE s START WITH 10", sequenceRow[int64](10, -100, 100, 100, -2, false, false), "", 10, 10, false},
		{"descending uncalled behind keeps position and records the declared start", "ALTER SEQUENCE s START WITH 15", sequenceRow[int64](10, -100, 100, 100, -2, false, false), "", 10, 15, false},
		// the restart lives in the row, so it applies in every session; the
		// altering session's curval cache must not pre-advance past it
		{"restart ignores the altering session's curval", "ALTER SEQUENCE s START WITH 20", sequenceRow[int64](10, 1, 100, 1, 2, false, true), "10", 20, 20, false},
	} {
		t.Run(tc.name, func(t *testing.T) {
			stmt := parseAlterSequence(t, tc.sql)
			row := append([]interface{}(nil), tc.row...)
			last, _, _, _, start, _, isCalled, err := makeAlterSequenceParam[int64](ctx, stmt, row, tc.curval)
			require.NoError(t, err)
			require.Equal(t, tc.last, last)
			require.Equal(t, tc.start, start)
			require.Equal(t, tc.called, isCalled)
			require.Equal(t, tc.row, row, "computing the row must not mutate the stored state")
		})
	}
}

// Issue #30679: a combined clause takes its direction from the NEW increment
// of the same statement, so a flip of the increment direction makes the same
// START WITH value take the other branch of the restart rule.
func TestMakeAlterSequenceParamStartWithIncrementDirectionFlip(t *testing.T) {
	ctx := testutil.NewProc(t).Ctx

	t.Run("ascending sequence flips to descending and restarts downward", func(t *testing.T) {
		// stored state still belongs to the ascending generation
		row := sequenceRow[int64](10, -100, 100, 1, 2, false, true)
		stmt := parseAlterSequence(t, "ALTER SEQUENCE s INCREMENT BY -2 START WITH 5")
		last, incr, _, _, start, _, isCalled, err := makeAlterSequenceParam[int64](ctx, stmt, row, "")
		require.NoError(t, err)
		require.Equal(t, int64(-2), incr)
		require.Equal(t, int64(5), last, "5 is strictly ahead of 10 for the new descending direction")
		require.Equal(t, int64(5), start)
		require.False(t, isCalled, "the next NEXTVAL must return exactly 5")
	})

	t.Run("descending sequence flips to ascending and restarts upward", func(t *testing.T) {
		row := sequenceRow[int64](10, 1, 100, 100, -2, false, true)
		stmt := parseAlterSequence(t, "ALTER SEQUENCE s INCREMENT BY 3 START WITH 40")
		last, incr, _, _, start, _, isCalled, err := makeAlterSequenceParam[int64](ctx, stmt, row, "")
		require.NoError(t, err)
		require.Equal(t, int64(3), incr)
		require.Equal(t, int64(40), last)
		require.Equal(t, int64(40), start)
		require.False(t, isCalled)
	})

	// Issue #27049 (clone_database_sequence.sql): a never-issued sequence has
	// an empty consumed range, so a combined flip + range rebase + START WITH
	// records the declared start unclamped and passes M2 against the new
	// bounds instead of spuriously rejecting on the stale one. The pending
	// position itself is out of the new bounds (1 < MINVALUE 3 for the new
	// descending direction), so the #30359 uniform uncalled arm restarts the
	// sequence at the declared start: the next NEXTVAL emits 9, then 7.
	t.Run("never-issued sequence accepts a wholesale flip and rebase", func(t *testing.T) {
		row := sequenceRow[int64](1, 1, 9223372036854775807, 1, 1, false, false)
		stmt := parseAlterSequence(t, "ALTER SEQUENCE s INCREMENT BY -2 MINVALUE 3 MAXVALUE 9 START WITH 9 CYCLE")
		last, incr, minV, maxV, start, cycle, isCalled, err := makeAlterSequenceParam[int64](ctx, stmt, row, "")
		require.NoError(t, err)
		require.Equal(t, int64(-2), incr)
		require.Equal(t, int64(3), minV)
		require.Equal(t, int64(9), maxV)
		require.Equal(t, int64(9), last, "the pending 1 is outside the rebased bounds; the sequence restarts at the declared start")
		require.Equal(t, int64(9), start, "the declared start is recorded as the boundary of the empty consumed range")
		require.True(t, cycle)
		require.False(t, isCalled)
	})
}

// Issue #30679 / M2: the FINAL effective definition — after the restart/clamp
// decision and all combined clauses — must pass the CREATE-equivalence
// validation before the replacement row is built, so a combined START+MIN/MAX
// that pushes the effective start out of bounds rejects the whole ALTER
// atomically and the stored state stands.
func TestSequenceAlterBatchRejectsInvalidFinalState(t *testing.T) {
	for _, tc := range []struct {
		name, sql, message string
		row                []interface{}
	}{
		{
			// ascending last=100: START WITH 50 is behind and clamps to the
			// frontier 100, which the new MAXVALUE 60 puts out of bounds.
			"ascending maxvalue below clamped frontier",
			"ALTER SEQUENCE s MAXVALUE 60 START WITH 50",
			"STARTVALUE (100) for sequence must between MINVALUE (1) and MAXVALUE (60)",
			sequenceRow[int64](100, 1, 1000, 1, 1, false, true),
		},
		{
			// descending last=100: START WITH 150 is behind and clamps to
			// 100, below the new MINVALUE 120.
			"descending minvalue above clamped frontier",
			"ALTER SEQUENCE s MINVALUE 120 START WITH 150",
			"STARTVALUE (100) for sequence must between MINVALUE (120) and MAXVALUE (1000)",
			sequenceRow[int64](100, -1000, 1000, 100, -1, false, true),
		},
		{
			// a real restart is validated too: 2000 is above MAXVALUE.
			"restart start beyond maxvalue",
			"ALTER SEQUENCE s START WITH 2000",
			"STARTVALUE (2000)",
			sequenceRow[int64](100, 1, 1000, 1, 1, false, true),
		},
		{
			// never-issued: the recorded start is the declared one, so the
			// M2 message names the user value (5), not a clamped position.
			"uncalled recorded start below new minvalue",
			"ALTER SEQUENCE s MINVALUE 20 START WITH 5",
			"STARTVALUE (5) for sequence must between MINVALUE (20) and MAXVALUE (100)",
			sequenceRow[int64](10, 1, 100, 1, 1, false, false),
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			proc := testutil.NewProcess(t)
			defer proc.Free()
			baseline := proc.Mp().CurrNB()
			row := append([]interface{}(nil), tc.row...)
			table := &plan.TableDef{Cols: []*plan.ColDef{{Typ: plan.Type{Id: int32(types.T_int64)}}}}
			bat, _, err := makeSequenceAlterBatch(proc.Ctx, parseAlterSequence(t, tc.sql), table, proc, row, "")
			require.ErrorContains(t, err, tc.message)
			require.Nil(t, bat)
			require.Equal(t, baseline, proc.Mp().CurrNB(), "rejected input must not retain a partial batch")
			require.Equal(t, tc.row, row, "an atomic rejection must leave the stored sequence row unchanged")
		})
	}
}

// Issue #30679 / v12-review F3: a never-issued sequence whose kept pending
// value lies outside the final bounds must not emit it — there is no wrap
// re-entry into the sequence's own range. The ALTER then restarts at the
// declared start (itself validated against the final bounds), uniformly for
// CYCLE and NO CYCLE definitions (v17, E1: the one authoritative uncalled
// START WITH rule's single exception; the CYCLE subtest below pins it).
// In-bounds pending values still keep the position on a behind-start; an
// uncalled emission has no wrap, so no re-entry can smuggle the pending
// value out.
func TestMakeAlterSequenceParamUncalledOutOfBoundsRestart(t *testing.T) {
	ctx := testutil.NewProc(t).Ctx

	t.Run("ascending no-cycle restarts when the pending value exceeds the new max", func(t *testing.T) {
		// F3: last=100 pending, uncalled; ALTER MAXVALUE 20 START WITH 10.
		row := sequenceRow[int64](100, 1, 1000, 1, 1, false, false)
		stmt := parseAlterSequence(t, "ALTER SEQUENCE s MAXVALUE 20 START WITH 10")
		last, _, minV, maxV, start, cycle, isCalled, err := makeAlterSequenceParam[int64](ctx, stmt, row, "")
		require.NoError(t, err)
		require.Equal(t, int64(10), last, "the out-of-bounds pending value is replaced by the declared start")
		require.Equal(t, int64(10), start)
		require.False(t, isCalled)
		require.Equal(t, int64(1), minV)
		require.Equal(t, int64(20), maxV)
		require.False(t, cycle)
	})

	t.Run("descending no-cycle restarts when the pending value falls below the new min", func(t *testing.T) {
		row := sequenceRow[int64](5, 1, 1000, 5, -1, false, false)
		stmt := parseAlterSequence(t, "ALTER SEQUENCE s MINVALUE 500 START WITH 900")
		last, _, minV, maxV, start, cycle, isCalled, err := makeAlterSequenceParam[int64](ctx, stmt, row, "")
		require.NoError(t, err)
		require.Equal(t, int64(900), last)
		require.Equal(t, int64(900), start)
		require.False(t, isCalled)
		require.Equal(t, int64(500), minV)
		require.Equal(t, int64(1000), maxV)
		require.False(t, cycle)
	})

	// #30359 review: the never-issued out-of-bounds arm is uniform across
	// CYCLE and NO CYCLE. An uncalled emission has no wrap; keeping the
	// pending 100 under the new max 20 would emit a value outside the
	// final bounds, so the sequence restarts at the declared start.
	t.Run("cycle restarts the out-of-bounds pending value at the declared start", func(t *testing.T) {
		row := sequenceRow[int64](100, 1, 1000, 1, 1, false, false)
		stmt := parseAlterSequence(t, "ALTER SEQUENCE s MAXVALUE 20 START WITH 10 CYCLE")
		last, _, _, _, start, cycle, isCalled, err := makeAlterSequenceParam[int64](ctx, stmt, row, "")
		require.NoError(t, err)
		require.Equal(t, int64(10), last, "the out-of-bounds pending value can never escape the new bounds")
		require.Equal(t, int64(10), start)
		require.True(t, cycle)
		require.False(t, isCalled)
	})

	t.Run("in-bounds pending value still keeps the position", func(t *testing.T) {
		row := sequenceRow[int64](10, 1, 100, 1, 2, false, false)
		stmt := parseAlterSequence(t, "ALTER SEQUENCE s START WITH 5")
		last, _, _, _, start, _, isCalled, err := makeAlterSequenceParam[int64](ctx, stmt, row, "")
		require.NoError(t, err)
		require.Equal(t, int64(10), last)
		require.Equal(t, int64(5), start)
		require.False(t, isCalled)
	})

	t.Run("an out-of-bounds declared start still rejects atomically", func(t *testing.T) {
		proc := testutil.NewProcess(t)
		defer proc.Free()
		row := sequenceRow[int64](100, 1, 1000, 1, 1, false, false)
		table := &plan.TableDef{Cols: []*plan.ColDef{{Typ: plan.Type{Id: int32(types.T_int64)}}}}
		bat, _, err := makeSequenceAlterBatch(proc.Ctx,
			parseAlterSequence(t, "ALTER SEQUENCE s MAXVALUE 20 START WITH 50"), table, proc, row, "")
		require.ErrorContains(t, err, "STARTVALUE (50) for sequence must between MINVALUE (1) and MAXVALUE (20)")
		require.Nil(t, bat)
		require.Equal(t, sequenceRow[int64](100, 1, 1000, 1, 1, false, false), row,
			"the rejection leaves the stored sequence state untouched")
	})
}

// Issue #30679 / v12-review F2: the catalog CREATE text comes from the
// validated effective execution-time definition — the same computation that
// builds the persisted row — so it carries every clause (type, increment,
// bounds, start, cycle) even when the ALTER named only one, and serializes
// effective values rather than requested ones.
func TestSequenceAlterEffectiveDefinitionForCatalog(t *testing.T) {
	for _, tc := range []struct {
		name string
		sql  string
		oid  types.T
		row  []interface{}
		want plan2.AlterSequenceEffectiveDefinition
	}{
		{
			name: "start-with-only ALTER carries the complete definition",
			sql:  "ALTER SEQUENCE s START WITH 55",
			oid:  types.T_int64,
			row:  sequenceRow[int64](3, 1, 1000, 1, 4, false, true),
			want: plan2.AlterSequenceEffectiveDefinition{
				TypeSQL: "bigint", Increment: 4, MinValue: "1", MaxValue: "1000", Start: "55", Cycle: false,
			},
		},
		{
			name: "unsigned type keyword and domain formatting",
			sql:  "ALTER SEQUENCE s START WITH 9",
			oid:  types.T_uint32,
			row:  sequenceRow[uint32](3, 1, 4000000000, 1, 2, true, true),
			want: plan2.AlterSequenceEffectiveDefinition{
				TypeSQL: "int unsigned", Increment: 2, MinValue: "1", MaxValue: "4000000000", Start: "9", Cycle: true,
			},
		},
		{
			name: "clamped start serializes the effective frontier, not the request",
			sql:  "ALTER SEQUENCE s MAXVALUE 500 START WITH 5",
			oid:  types.T_int64,
			row:  sequenceRow[int64](100, 1, 1000, 1, 1, false, true),
			want: plan2.AlterSequenceEffectiveDefinition{
				TypeSQL: "bigint", Increment: 1, MinValue: "1", MaxValue: "500", Start: "100", Cycle: false,
			},
		},
		{
			name: "descending sequence keeps its negative increment",
			sql:  "ALTER SEQUENCE s START WITH 60",
			oid:  types.T_int32,
			row:  sequenceRow[int32](80, 1, 100, 80, -2, false, true),
			want: plan2.AlterSequenceEffectiveDefinition{
				TypeSQL: "int", Increment: -2, MinValue: "1", MaxValue: "100", Start: "60", Cycle: false,
			},
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			proc := testutil.NewProcess(t)
			defer proc.Free()
			baseline := proc.Mp().CurrNB()
			row := append([]interface{}(nil), tc.row...)
			table := &plan.TableDef{Cols: []*plan.ColDef{{Typ: plan.Type{Id: int32(tc.oid)}}}}
			bat, def, err := makeSequenceAlterBatch(proc.Ctx, parseAlterSequence(t, tc.sql), table, proc, row, "")
			require.NoError(t, err)
			require.NotNil(t, bat)
			defer func() {
				bat.Clean(proc.Mp())
				require.Equal(t, baseline, proc.Mp().CurrNB(), "all batch vectors must be released")
			}()
			require.Equal(t, tc.want, def)

			// The catalog text must encode exactly the persisted definition
			// columns (min/max/start/increment/cycle come from one
			// computation).
			require.Equal(t, def.Increment, vector.MustFixedColWithTypeCheck[int64](bat.Vecs[4])[0])
			require.Equal(t, def.Cycle, vector.MustFixedColWithTypeCheck[bool](bat.Vecs[5])[0])

			// The synthesized catalog text replays: it parses as a CREATE
			// SEQUENCE and carries the whole effective definition.
			ddl := plan2.AlterSequenceEffectiveCreateSQL("db1", "s", def)
			replayed, err := parsers.ParseOne(testutil.NewProc(t).Ctx, dialect.MYSQL, ddl, 1)
			require.NoError(t, err, "the synthesized catalog text must parse: %q", ddl)
			_, ok := replayed.(*tree.CreateSequence)
			require.True(t, ok, "the synthesized catalog text must be a CREATE SEQUENCE: %q", ddl)
			require.Contains(t, ddl, "create sequence if not exists `db1`.`s` as "+def.TypeSQL)
			require.Contains(t, ddl, "minvalue "+def.MinValue)
			require.Contains(t, ddl, "maxvalue "+def.MaxValue)
			require.Contains(t, ddl, "start with "+def.Start)
		})
	}

	t.Run("exact canonical text", func(t *testing.T) {
		ddl := plan2.AlterSequenceEffectiveCreateSQL("alter_seq_start_db", "asw_replay",
			plan2.AlterSequenceEffectiveDefinition{
				TypeSQL: "bigint", Increment: 1, MinValue: "1", MaxValue: "100000", Start: "50000",
			})
		require.Equal(t,
			"create sequence if not exists `alter_seq_start_db`.`asw_replay` as bigint increment by 1 minvalue 1 maxvalue 100000 start with 50000 no cycle",
			ddl)
	})
}

// Issue #30679: rel_createsql and SHOW CREATE must never carry the raw ALTER
// text; the persisted value is a canonical CREATE SEQUENCE that replays.
func TestAlterSequenceCreateSQLSynthesis(t *testing.T) {
	for _, tc := range []struct {
		name, sql, db, table, want string
	}{
		{
			"start with only",
			"ALTER SEQUENCE seqw.s START WITH 500", "seqw", "s",
			"create sequence if not exists `seqw`.`s` start with 500",
		},
		{
			"combined clauses in canonical order",
			"ALTER SEQUENCE s INCREMENT BY -3 MINVALUE -100 MAXVALUE 100 START WITH 5 NO CYCLE", "mo", "s",
			"create sequence if not exists `mo`.`s` increment by -3 minvalue -100 maxvalue 100 start with 5 no cycle",
		},
		{
			"non-start alter still synthesizes a replayable create",
			"ALTER SEQUENCE s INCREMENT BY 2", "mo", "s",
			"create sequence if not exists `mo`.`s` increment by 2",
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			stmt := parseAlterSequence(t, tc.sql)
			got := plan2.AlterSequenceCreateSQLForCatalog(stmt, tc.db, tc.table)
			require.Equal(t, tc.want, got)
			replayed, err := parsers.ParseOne(testutil.NewProc(t).Ctx, dialect.MYSQL, got, 1)
			require.NoError(t, err, "the synthesized statement must parse as valid SQL")
			_, ok := replayed.(*tree.CreateSequence)
			require.True(t, ok, "the synthesized statement must be a CREATE SEQUENCE: %q", got)
		})
	}
}

// CYCLE pending-emission oracle (#30359 review): a never-issued sequence
// ALTERed with tightened bounds and a start at-or-behind the stored position
// kept its pending value, and the uncalled first NEXTVAL emits the stored
// value as-is — outside the final bounds — even for CYCLE (the wrap applies
// only after that first emission). The out-of-bounds kept pending must
// restart at the declared start for CYCLE too, while the declared start
// itself remains validated by the final-state check (atomic rejection).
func TestMakeAlterSequenceParamCycleOutOfBoundsPendingRestarts(t *testing.T) {
	ctx := testutil.NewProc(t).Ctx

	t.Run("ascending cycle restart at declared start", func(t *testing.T) {
		stmt := parseAlterSequence(t, "ALTER SEQUENCE s MAXVALUE 30 START WITH 10")
		// never-issued CYCLE sequence with pending 100 and final bounds [1,30]
		row := sequenceRow[int64](100, 1, 1000, 100, 1, true, false)
		last, incr, minV, maxV, start, cycle, isCalled, err := makeAlterSequenceParam[int64](ctx, stmt, row, "")
		require.NoError(t, err)
		require.Equal(t, int64(10), last, "the out-of-bounds pending value restarts at the declared start")
		require.False(t, isCalled, "the restart is pending: the first NEXTVAL emits the declared start 10")
		require.Equal(t, int64(10), start)
		require.Equal(t, int64(30), maxV)
		require.Equal(t, int64(1), minV)
		require.Equal(t, int64(1), incr)
		require.True(t, cycle)
	})

	t.Run("descending cycle restart at declared start", func(t *testing.T) {
		stmt := parseAlterSequence(t, "ALTER SEQUENCE s MINVALUE 950 START WITH 980")
		// never-issued descending CYCLE: pending 900 below final min 950
		row := sequenceRow[int64](900, 1, 1000, 900, -1, true, false)
		last, incr, minV, maxV, start, cycle, isCalled, err := makeAlterSequenceParam[int64](ctx, stmt, row, "")
		require.NoError(t, err)
		require.Equal(t, int64(980), last, "the out-of-bounds pending value restarts at the declared start")
		require.False(t, isCalled, "the restart is pending: the first NEXTVAL emits the declared start 980")
		require.Equal(t, int64(980), start)
		require.Equal(t, int64(950), minV)
		require.Equal(t, int64(1000), maxV)
		require.Equal(t, int64(-1), incr)
		require.True(t, cycle)
	})

	t.Run("in-bounds ascending cycle pending is kept", func(t *testing.T) {
		// Control: a pending value inside the final bounds is untouched.
		stmt := parseAlterSequence(t, "ALTER SEQUENCE s MAXVALUE 300 START WITH 50")
		row := sequenceRow[int64](100, 1, 1000, 100, 1, true, false)
		last, _, _, maxV, start, cycle, isCalled, err := makeAlterSequenceParam[int64](ctx, stmt, row, "")
		require.NoError(t, err)
		require.Equal(t, int64(100), last, "an in-bounds pending value stays pending")
		require.Equal(t, int64(50), start)
		require.False(t, isCalled)
		require.Equal(t, int64(300), maxV)
		require.True(t, cycle)
	})

	t.Run("no-cycle restart arm is unchanged", func(t *testing.T) {
		stmt := parseAlterSequence(t, "ALTER SEQUENCE s MAXVALUE 20 START WITH 10")
		row := sequenceRow[int64](100, 1, 1000, 100, 1, false, false)
		last, _, _, maxV, start, cycle, isCalled, err := makeAlterSequenceParam[int64](ctx, stmt, row, "")
		require.NoError(t, err)
		require.Equal(t, int64(10), last)
		require.Equal(t, int64(10), start)
		require.Equal(t, int64(20), maxV)
		require.False(t, isCalled)
		require.False(t, cycle)
	})
}
