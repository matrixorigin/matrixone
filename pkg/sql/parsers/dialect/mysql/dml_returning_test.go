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

package mysql

import (
	"context"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/matrixorigin/matrixone/pkg/sql/parsers/dialect"
	"github.com/matrixorigin/matrixone/pkg/sql/parsers/tree"
)

func TestDMLReturningSyntax(t *testing.T) {
	for _, sql := range []string{
		"insert into t values (1) returning *",
		"insert into t select a from s returning t.a as inserted_a, ?",
		"update t as x set a = 2 where b = 1 order by a limit 2 returning x.*, x.a + 1 as next_a",
		"delete from t where a > 0 order by a limit 1 returning a, null as n",
	} {
		t.Run(sql, func(t *testing.T) {
			stmt, err := ParseOne(context.Background(), sql, 1)
			require.NoError(t, err)
			require.Equal(t, tree.RESP_DEFERRED_RESULT_ROW, stmt.StmtKind().RespType())
			formatted := tree.String(stmt, dialect.MYSQL)
			roundTrip, err := ParseOne(context.Background(), formatted, 1)
			require.NoError(t, err)
			require.Equal(t, formatted, tree.String(roundTrip, dialect.MYSQL))
			roundTrip.Free()
			stmt.Free()
		})
	}
}

func TestReturningRemainsNonReservedIdentifier(t *testing.T) {
	for _, sql := range []string{
		"create table t(returning int)",
		"select returning from t",
		"select 1 as returning",
		"insert into t(returning) select returning from s",
		"update t set returning = returning + 1",
		"delete returning from returning",
	} {
		stmt, err := ParseOne(context.Background(), sql, 1)
		require.NoError(t, err, sql)
		stmt.Free()
	}
}

func TestDMLReturningListMustNotBeEmpty(t *testing.T) {
	_, err := ParseOne(context.Background(), "delete from t returning", 1)
	require.Error(t, err)
}

func TestDMLReturningPreservesRejectedSyntaxShape(t *testing.T) {
	stmt, err := ParseOne(context.Background(), "delete from t partition(p0) returning *", 1)
	require.NoError(t, err)
	deleteStmt := stmt.(*tree.Delete)
	require.Equal(t, tree.IdentifierList{tree.Identifier("p0")}, deleteStmt.PartitionNames)
	require.Contains(t, tree.String(deleteStmt, dialect.MYSQL), "partition(p0)")
	stmt.Free()

	stmt, err = ParseOne(context.Background(), "update t, s set t.a = s.a returning t.a", 1)
	require.NoError(t, err)
	require.True(t, stmt.(*tree.Update).MultiTable)
	stmt.Free()
}

func TestRejectedDMLReturningSyntaxRoundTrips(t *testing.T) {
	for _, sql := range []string{
		"insert ignore into t values (1) returning *",
		"update low_priority t set a = 1 returning a",
		"update high_priority ignore t set a = 1 returning a",
		"delete low_priority quick ignore from t returning a",
		"replace into t values (1) returning *",
		"merge into t using s on t.a = s.a when matched then delete returning *",
	} {
		stmt, err := ParseOne(context.Background(), sql, 1)
		require.NoError(t, err, sql)
		formatted := tree.String(stmt, dialect.MYSQL)
		roundTrip, err := ParseOne(context.Background(), formatted, 1)
		require.NoError(t, err, formatted)
		require.Equal(t, formatted, tree.String(roundTrip, dialect.MYSQL))
		switch original := stmt.(type) {
		case *tree.Insert:
			reparsed := roundTrip.(*tree.Insert)
			require.Equal(t, len(original.OnDuplicateUpdate), len(reparsed.OnDuplicateUpdate))
			require.Equal(t, original.OnDuplicateUpdate[0] == nil, reparsed.OnDuplicateUpdate[0] == nil)
		case *tree.Update:
			reparsed := roundTrip.(*tree.Update)
			require.Equal(t, original.Priority, reparsed.Priority)
			require.Equal(t, original.Ignore, reparsed.Ignore)
		case *tree.Delete:
			reparsed := roundTrip.(*tree.Delete)
			require.Equal(t, original.Priority, reparsed.Priority)
			require.Equal(t, original.Quick, reparsed.Quick)
			require.Equal(t, original.Ignore, reparsed.Ignore)
		}
		roundTrip.Free()
		stmt.Free()
	}
}
