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

// sequenceRow builds the single sequence-table row the way
// Scope.AlterSequence reads it via SELECT *.
func sequenceRow[T interface {
	int16 | int32 | int64 | uint16 | uint32 | uint64
}](lastSeq, minV, maxV, startV T, incr int64, cycle, isCalled bool) []interface{} {
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
