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
	"testing"

	"github.com/matrixorigin/matrixone/pkg/common/moerr"
	"github.com/matrixorigin/matrixone/pkg/sql/parsers/dialect/mysql"
	"github.com/stretchr/testify/require"
)

// TestJoinOnForwardReferenceErrorClass walks the three forward-referencing
// JOIN ON variants of the official join_outer.test "--error 1054" family
// through the real mysql.Parse + BuildPlan path: the ON predicate of the
// first join references alias t3, which only the later join defines. The
// reference stays rejected; MySQL classifies the failure as
// ER_BAD_FIELD_ERROR 1054 (42S22) "Unknown column 't3.a' in 'on clause'"
// instead of a Postgres-style missing FROM entry.
func TestJoinOnForwardReferenceErrorClass(t *testing.T) {
	mock := NewMockOptimizer(false, newPlanTestProcess(t))

	forwardCases := []struct {
		name string
		sql  string
	}{
		{
			name: "forward alias binds as the left ON operand",
			sql: `
				select t1.a, t2.a, t3.a
				from bvt_test1.t1 as t1
				left join bvt_test2.t2 as t2 on (t3.a = t2.a)
				left join bvt_test1.t1 as t3 on (t1.a = t3.a)`,
		},
		{
			name: "forward alias binds as the right ON operand",
			sql: `
				select t1.a, t2.a, t3.a
				from bvt_test1.t1 as t1
				left join bvt_test2.t2 as t2 on (t2.a = t3.a)
				left join bvt_test1.t1 as t3 on (t1.a = t3.a)`,
		},
		{
			name: "forward alias binds beside an earlier column",
			sql: `
				select t1.a, t2.a, t3.a
				from bvt_test1.t1 as t1
				left join bvt_test2.t2 as t2 on (t2.b = t3.a)
				left join bvt_test1.t1 as t3 on (t1.a = t3.a)`,
		},
	}

	for _, tc := range forwardCases {
		t.Run(tc.name, func(t *testing.T) {
			queryPlan, err := runOneStmt(mock, t, tc.sql)
			require.Error(t, err, "forward JOIN ON references stay rejected")
			require.True(t, moerr.IsMoErrCode(err, moerr.ErrBadFieldError), "actual: %v", err)
			var moErr *moerr.Error
			require.ErrorAs(t, err, &moErr)
			require.Equal(t, uint16(moerr.ErrBadFieldError), moErr.ErrorCode(),
				"internal MO code is 20309, not the wire code")
			require.Equal(t, uint16(moerr.ER_BAD_FIELD_ERROR), moErr.MySQLCode(),
				"wire code is 1054, not the internal code")
			require.Equal(t, "42S22", moErr.SqlState())
			require.Equal(t, "Unknown column 't3.a' in 'on clause'", moErr.Error())
			require.Nil(t, queryPlan, "an out-of-scope ON reference never becomes legal")
		})
	}

	t.Run("a later valid plan still builds on the same process", func(t *testing.T) {
		_, err := runOneStmt(mock, t, `select a from bvt_test2.t2`)
		require.NoError(t, err)
	})
}

// TestJoinOnForwardReferenceControls pins the surrounding contract: prior
// aliases, qualified names, ON subqueries and outer correlation keep binding,
// while unrelated failures keep their own error classes.
func TestJoinOnForwardReferenceControls(t *testing.T) {
	mock := NewMockOptimizer(false, newPlanTestProcess(t))

	noErrorCases := []struct {
		name string
		sql  string
	}{
		{
			name: "left join over a prior alias",
			sql:  `select t1.a from bvt_test1.t1 as t1 left join bvt_test2.t2 as t2 on t1.a = t2.a`,
		},
		{
			name: "inner join over a prior alias",
			sql:  `select t1.a from bvt_test1.t1 as t1 join bvt_test2.t2 as t2 on t1.a = t2.a`,
		},
		{
			name: "db-qualified prior tables",
			sql: `select bvt_test1.t1.a from bvt_test1.t1
			      left join bvt_test2.t2 on bvt_test1.t1.a = bvt_test2.t2.a`,
		},
		{
			name: "on subquery scoped by its own FROM list",
			sql: `select t1.a from bvt_test1.t1 as t1
			      left join bvt_test2.t2 as t2 on t1.a = t2.a and t2.b in (select s.b from bvt_test2.t2 as s)`,
		},
		{
			name: "outer correlation resolved from a join on",
			sql: `select t3.a from bvt_test2.t3 as t3
			      where t3.a in (select t2.a from bvt_test2.t2 as t2
			                     join bvt_test2.t3 as i3 on i3.a = t3.a)`,
		},
	}
	for _, tc := range noErrorCases {
		t.Run(tc.name, func(t *testing.T) {
			_, err := runOneStmt(mock, t, tc.sql)
			require.NoError(t, err, tc.sql)
		})
	}

	t.Run("where forward reference keeps the missing FROM class", func(t *testing.T) {
		_, err := runOneStmt(mock, t, `select a from bvt_test2.t2 where t3.a = 1`)
		require.Error(t, err)
		require.True(t, moerr.IsMoErrCode(err, moerr.ErrInvalidInput), "actual: %v", err)
		require.Equal(t, "invalid input: missing FROM-clause entry for table 't3'", err.Error())
	})

	t.Run("missing column of a visible ON table keeps its diagnostic", func(t *testing.T) {
		_, err := runOneStmt(mock, t, `select t1.a from bvt_test1.t1 as t1 left join bvt_test2.t2 as t2 on t1.nosuch = t2.a`)
		require.Error(t, err)
		require.True(t, moerr.IsMoErrCode(err, moerr.ErrBadFieldError), "actual: %v", err)
		require.Contains(t, err.Error(), "does not exist")
	})

	t.Run("unqualified ambiguous ON column keeps its class", func(t *testing.T) {
		_, err := runOneStmt(mock, t, `select t1.a from bvt_test1.t1 as t1 left join bvt_test2.t2 as t2 on b = t2.b`)
		require.Error(t, err)
		require.True(t, moerr.IsMoErrCode(err, moerr.ErrInvalidInput), "actual: %v", err)
	})

	t.Run("missing base table keeps its own error", func(t *testing.T) {
		_, err := runOneStmt(mock, t, `select t1.a from bvt_test1.t1 as t1 left join bvt_test2.nosuch as s on t1.a = s.a`)
		require.Error(t, err)
		require.False(t, moerr.IsMoErrCode(err, moerr.ErrBadFieldError), "a missing base table is not an on-clause column: %v", err)
	})

	t.Run("syntax error keeps its parse diagnostic", func(t *testing.T) {
		stmts, err := mysql.Parse(context.Background(), `selec a from bvt_test2.t2`, 1)
		if err == nil {
			for _, stmt := range stmts {
				stmt.Free()
			}
		}
		require.Error(t, err)
	})
}

// TestJoinOnNestedScopeResolution pins the error class of an invalid forward
// JOIN ON reference inside a subquery: the failure stays owned by the
// innermost ON clause even when the delegated outer binding also cannot
// resolve the same qualifier, while a legal same-name local alias keeps
// resolving against its innermost scope.
func TestJoinOnNestedScopeResolution(t *testing.T) {
	mock := NewMockOptimizer(false, newPlanTestProcess(t))

	t.Run("nested invalid forward ON reference keeps the on-clause class", func(t *testing.T) {
		queryPlan, err := runOneStmt(mock, t, `
			select o.a
			from bvt_test1.t1 as o
			where exists (
				select t2.a
				from bvt_test2.t2 as t2
				left join bvt_test1.t1 as t4 on (t3.a = t2.a)
				left join bvt_test1.t1 as t3 on (t3.a = t4.a)
			)`)
		require.Error(t, err, "the forward reference stays rejected in every scope")
		require.True(t, moerr.IsMoErrCode(err, moerr.ErrBadFieldError), "actual: %v", err)
		var moErr *moerr.Error
		require.ErrorAs(t, err, &moErr)
		require.Equal(t, uint16(moerr.ErrBadFieldError), moErr.ErrorCode(),
			"internal MO code is 20309, not the wire code")
		require.Equal(t, uint16(moerr.ER_BAD_FIELD_ERROR), moErr.MySQLCode(),
			"wire code is 1054, not the internal code")
		require.Equal(t, "42S22", moErr.SqlState())
		require.Equal(t, "Unknown column 't3.a' in 'on clause'", moErr.Error())
		require.Nil(t, queryPlan, "an out-of-scope ON reference never becomes legal")
	})

	t.Run("local alias shadows an outer same-name alias", func(t *testing.T) {
		// s1.c resolves only against the inner scope's s1 (bvt_test1.t1 has
		// c); the outer s1 (bvt_test2.t3) has no c, so success proves the
		// innermost binding owns the reference.
		_, err := runOneStmt(mock, t, `
			select o.a
			from bvt_test1.t1 as o
			left join bvt_test2.t3 as s1 on o.a = s1.a
			where exists (select s1.c from bvt_test1.t1 as s1 where s1.b > 0)`)
		require.NoError(t, err)
	})
}

// TestJoinOnParentDiagnosticsOwnership pins the boundary of the on-clause
// error class: a delegated outer failure keeps the parent's own diagnostic
// whenever an outer binding context actually owns the qualified name. A
// duplicate-named derived column keeps its ambiguity diagnosis inside and
// outside a join ON, and a missing column of an existing outer alias keeps
// its does-not-exist bad-field diagnosis.
func TestJoinOnParentDiagnosticsOwnership(t *testing.T) {
	mock := NewMockOptimizer(false, newPlanTestProcess(t))

	t.Run("derived duplicate column keeps the ambiguity diagnostic", func(t *testing.T) {
		queryPlan, err := runOneStmt(mock, t, `SELECT dup.x FROM (SELECT a, b FROM bvt_test2.t2) AS dup (x, x)`)
		require.Error(t, err, "a duplicate-named column cannot become resolvable")
		require.True(t, moerr.IsMoErrCode(err, moerr.ErrInvalidInput), "actual: %v", err)
		require.Contains(t, err.Error(), "ambiguous column reference")
		require.Nil(t, queryPlan)
	})

	t.Run("nested ON ambiguity of a parent-owned name keeps the parent diagnostic", func(t *testing.T) {
		queryPlan, err := runOneStmt(mock, t, `
			SELECT 1
			FROM (SELECT a, b FROM bvt_test2.t2) AS dup (x, x)
			WHERE EXISTS (
				SELECT t1.a
				FROM bvt_test1.t1 AS t1
				JOIN bvt_test2.t2 AS t2 ON (dup.x = t2.a)
			)`)
		require.Error(t, err, "the duplicate-named parent column stays unresolvable")
		require.True(t, moerr.IsMoErrCode(err, moerr.ErrInvalidInput), "actual: %v", err)
		require.Contains(t, err.Error(), "ambiguous column reference")
		require.False(t, moerr.IsMoErrCode(err, moerr.ErrBadFieldError),
			"a parent-owned ambiguity must not become an on-clause unknown column: %v", err)
		require.Nil(t, queryPlan, "an ambiguous reference never becomes legal")
	})

	t.Run("nested ON no-such-column of an existing outer alias keeps does-not-exist", func(t *testing.T) {
		_, err := runOneStmt(mock, t, `
			SELECT 1
			FROM bvt_test1.t1 AS o
			WHERE EXISTS (
				SELECT t2.a
				FROM bvt_test2.t2 AS t2
				JOIN bvt_test2.t3 AS t3 ON (o.zzz = t2.a)
			)`)
		require.Error(t, err)
		require.True(t, moerr.IsMoErrCode(err, moerr.ErrBadFieldError), "actual: %v", err)
		require.Contains(t, err.Error(), "'o.zzz' does not exist")
	})
}

// TestJoinOnQualifiedNamespaceErrorClass locks the on-clause class for
// qualified references that no binding context owns — the forms exercised
// by the lowercase and decimal BVT cases: a real table of another schema
// and a database-qualified name that exists nowhere.
func TestJoinOnQualifiedNamespaceErrorClass(t *testing.T) {
	mock := NewMockOptimizer(false, newPlanTestProcess(t))

	t.Run("table elsewhere in the catalog stays rejected with the on-clause class", func(t *testing.T) {
		queryPlan, err := runOneStmt(mock, t, `
			SELECT t1.a
			FROM bvt_test1.t1 AS t1
			LEFT JOIN bvt_test2.t2 AS t2 ON (bvt_test2.t3.a = t2.a)`)
		require.Error(t, err, "a table outside the query scope stays rejected")
		require.True(t, moerr.IsMoErrCode(err, moerr.ErrBadFieldError), "actual: %v", err)
		var moErr *moerr.Error
		require.ErrorAs(t, err, &moErr)
		require.Equal(t, uint16(moerr.ErrBadFieldError), moErr.ErrorCode(),
			"internal MO code is 20309, not the wire code")
		require.Equal(t, uint16(moerr.ER_BAD_FIELD_ERROR), moErr.MySQLCode(),
			"wire code is 1054, not the internal code")
		require.Equal(t, "42S22", moErr.SqlState())
		require.Equal(t, "Unknown column 'bvt_test2.t3.a' in 'on clause'", moErr.Error())
		require.Nil(t, queryPlan, "an out-of-scope ON reference never becomes legal")
	})

	t.Run("database-qualified name existing nowhere keeps its full spelling", func(t *testing.T) {
		_, err := runOneStmt(mock, t, `
			SELECT t2.a
			FROM bvt_test2.t2 AS t2
			LEFT JOIN bvt_test2.t3 AS t3 ON (t2.b = nosuchdb.t4.a)`)
		require.Error(t, err)
		require.True(t, moerr.IsMoErrCode(err, moerr.ErrBadFieldError), "actual: %v", err)
		require.Equal(t, "Unknown column 'nosuchdb.t4.a' in 'on clause'", err.Error())
	})
}
