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
			bat, err := makeSequenceAlterBatch(proc.Ctx, stmt, table, proc, row, tc.curval)
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
			bat, err := makeSequenceAlterBatch(proc.Ctx, parseAlterSequence(t, tc.sql), table, proc, row, "")
			require.ErrorContains(t, err, tc.message)
			require.Nil(t, bat)
			require.Equal(t, baseline, proc.Mp().CurrNB(), "rejected input must not retain a partial batch")
			require.Equal(t, sequenceRow[int64](1, 1, 100, 1, 1, false, true), row)
		})
	}
}
