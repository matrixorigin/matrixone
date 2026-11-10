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

package issues

import (
	"context"
	"database/sql"
	"fmt"
	"testing"
	"time"

	"github.com/go-sql-driver/mysql"
	"github.com/matrixorigin/matrixone/pkg/embed"
	"github.com/stretchr/testify/require"
)

// TestIssue31882And31913ValuesSubqueryNullAndUnionDateTime runs both issue
// scenes on one shared two-CN generation. RunBaseClusterTests keeps a passing
// callback admitted and reuses the generation, so this parent owns exactly
// one admission: each scene registers its own bounded database cleanup
// immediately after its create database succeeds, and the shared fixture is
// released only after RunBaseClusterTests returns (closing it inside the
// callback would wait on its own admission).
func TestIssue31882And31913ValuesSubqueryNullAndUnionDateTime(t *testing.T) {
	defer func() {
		if err := embed.CloseBaseClusterTests(); err != nil {
			t.Errorf("release shared base-cluster fixture: %v", err)
		}
	}()
	embed.RunBaseClusterTests(t, func(c embed.Cluster) {
		ctx, cancel := context.WithTimeout(t.Context(), 10*time.Minute)
		defer cancel()

		cn, err := c.GetCNService(0)
		require.NoError(t, err)
		db, err := sql.Open("mysql", fmt.Sprintf("dump:111@tcp(127.0.0.1:%d)/", cn.GetServiceConfig().CN.Frontend.Port))
		require.NoError(t, err)
		defer db.Close()
		conn, err := db.Conn(ctx)
		require.NoError(t, err)
		defer conn.Close()

		t.Run("issue_31882_values_subquery_and_null", func(t *testing.T) {
			issue31882ValuesSubqueryAndNull(t, conn)
		})
		t.Run("issue_31913_union_date_and_time", func(t *testing.T) {
			issue31913UnionDateAndTime(t, conn)
		})
	})
}

// dropOwnedDatabase drops one scene-owned database with an independent
// bounded context. It is registered immediately after the scene's create
// database succeeds so a FailNow inside the scene still leaves no owned
// catalog state, and it runs at scene scope exit, after the scene's own
// later-deferred rows and statements are closed. A cleanup failure is
// reported without masking the primary scene failure.
func dropOwnedDatabase(t *testing.T, conn *sql.Conn, name string) {
	t.Helper()
	dropCtx, cancel := context.WithTimeout(context.Background(), time.Minute)
	defer cancel()
	if _, err := conn.ExecContext(dropCtx, "drop database if exists "+name); err != nil {
		t.Errorf("drop owned database %s: %v", name, err)
	}
}

// issue31882ValuesSubqueryAndNull covers #31882: INSERT/REPLACE VALUES with a
// scalar subquery lowers to UNION ALL branches, and a bare NULL in the same
// VALUES list must reach the destination column as a typed NULL value
// instead of an untyped vector that panics in pSpool. Covers both row orders,
// a NULL in the same row as a subquery, a NULL behind and before typed
// literal rows, a single-subquery-row statement, prepared execution, a real
// vector roundtrip, and NOT NULL enforcement with type-checked rejection
// and statement reuse.
func issue31882ValuesSubqueryAndNull(t *testing.T, conn *sql.Conn) {
	t.Helper()
	const dbName = "issue_31882_values_subquery_null"
	ctx, cancel := context.WithTimeout(t.Context(), 5*time.Minute)
	defer cancel()

	_, err := conn.ExecContext(ctx, "create database if not exists "+dbName)
	require.NoError(t, err)
	defer dropOwnedDatabase(t, conn, dbName)
	_, err = conn.ExecContext(ctx, "use "+dbName)
	require.NoError(t, err)
	_, err = conn.ExecContext(ctx, "drop table if exists t_values_subquery_null")
	require.NoError(t, err)
	_, err = conn.ExecContext(ctx,
		`create table t_values_subquery_null (
				id int primary key,
				a int,
				d decimal(10,2),
				v varchar(32),
				vc vecf32(3)
			)`)
	require.NoError(t, err)

	// Bare NULL behind the subquery row: the NULL sits in the literal row
	// whose first row determines the branch type.
	_, err = conn.ExecContext(ctx,
		`insert into t_values_subquery_null values
				(1, (select 3), 1.10, 'a', '[1,2,3]'),
				(2, null, null, null, null)`)
	require.NoError(t, err)
	// Bare NULL before the subquery row: reversed row order.
	_, err = conn.ExecContext(ctx,
		`insert into t_values_subquery_null values
				(-2, null, null, null, null),
				(-1, (select 4), 4.40, 'b', '[4,5,6]')`)
	require.NoError(t, err)
	// Bare NULL in the same row as another column's subquery, plus a
	// NULL-only literal row behind a typed literal row.
	_, err = conn.ExecContext(ctx,
		`insert into t_values_subquery_null values
				(3, (select 30), null, 'c', null),
				(-3, 5, 5.50, 'd', '[7,8,9]'),
				(-4, null, null, null, null)`)
	require.NoError(t, err)
	// Single-row statement: the branch projection has no UNION ALL.
	_, err = conn.ExecContext(ctx,
		`insert into t_values_subquery_null values (6, (select 60), 6.60, 'e', null)`)
	require.NoError(t, err)

	// Every inserted row must be stored: the supplied values round-trip
	// and every bare NULL is stored as SQL NULL.
	type stored struct {
		id     int64
		a      sql.NullInt64
		d      sql.NullString
		v      sql.NullString
		vcNull bool
	}
	rows, err := conn.QueryContext(ctx,
		`select id, a, d, v, vc is null from t_values_subquery_null order by id`)
	require.NoError(t, err)
	defer rows.Close()
	got := make([]stored, 0, 8)
	for rows.Next() {
		var r stored
		require.NoError(t, rows.Scan(&r.id, &r.a, &r.d, &r.v, &r.vcNull))
		got = append(got, r)
	}
	require.NoError(t, rows.Err())
	nullInt := func(v int64) sql.NullInt64 { return sql.NullInt64{Int64: v, Valid: true} }
	nullStr := func(v string) sql.NullString { return sql.NullString{String: v, Valid: true} }
	require.Equal(t, []stored{
		{-4, sql.NullInt64{}, sql.NullString{}, sql.NullString{}, true},
		{-3, nullInt(5), nullStr("5.50"), nullStr("d"), false},
		{-2, sql.NullInt64{}, sql.NullString{}, sql.NullString{}, true},
		{-1, nullInt(4), nullStr("4.40"), nullStr("b"), false},
		{1, nullInt(3), nullStr("1.10"), nullStr("a"), false},
		{2, sql.NullInt64{}, sql.NullString{}, sql.NullString{}, true},
		{3, nullInt(30), sql.NullString{}, nullStr("c"), true},
		{6, nullInt(60), nullStr("6.60"), nullStr("e"), true},
	}, got)

	var nullCount int64
	require.NoError(t, conn.QueryRowContext(ctx,
		`select count(*) from t_values_subquery_null where a is null or d is null or v is null or vc is null`).
		Scan(&nullCount))
	require.Equal(t, int64(5), nullCount, "rows carrying at least one NULL")

	// The stored vector content must round-trip its literal: only the
	// identical stored value has distance zero to it.
	var dist float64
	require.NoError(t, conn.QueryRowContext(ctx,
		`select l2_distance(vc, '[1,2,3]') from t_values_subquery_null where id = 1`).Scan(&dist))
	require.Equal(t, float64(0), dist, "the vecf32 value must round-trip its literal content")

	// Prepared execution: parameters keep working in a VALUES statement
	// that also contains a scalar subquery, and a NULL parameter stores
	// NULL.
	_, err = conn.ExecContext(ctx, "drop table if exists p_values_subquery_null")
	require.NoError(t, err)
	_, err = conn.ExecContext(ctx, `create table p_values_subquery_null (id int primary key, a int, v varchar(8))`)
	require.NoError(t, err)
	stmt, err := conn.PrepareContext(ctx, `insert into p_values_subquery_null values (?, (select 41), ?)`)
	require.NoError(t, err)
	defer stmt.Close()
	_, err = stmt.ExecContext(ctx, 10, nil)
	require.NoError(t, err)
	_, err = stmt.ExecContext(ctx, 11, "x")
	require.NoError(t, err)
	pRows, err := conn.QueryContext(ctx, `select id, a, v is null from p_values_subquery_null`)
	require.NoError(t, err)
	defer pRows.Close()
	prepared := map[int64]struct {
		a     int64
		vNull bool
	}{}
	for pRows.Next() {
		var id, a int64
		var vNull bool
		require.NoError(t, pRows.Scan(&id, &a, &vNull))
		prepared[id] = struct {
			a     int64
			vNull bool
		}{a: a, vNull: vNull}
	}
	require.NoError(t, pRows.Err())
	require.Len(t, prepared, 2, "exactly the two prepared rows")
	row10, ok10 := prepared[10]
	require.True(t, ok10, "prepared row 10 must exist")
	require.Equal(t, int64(41), row10.a)
	require.True(t, row10.vNull, "the NULL parameter must store NULL")
	row11, ok11 := prepared[11]
	require.True(t, ok11, "prepared row 11 must exist")
	require.Equal(t, int64(41), row11.a)
	require.False(t, row11.vNull, "the non-NULL parameter must store its value")

	// NOT NULL enforcement: a bare NULL into a NOT NULL column still
	// errors, and the same connection keeps working afterwards.
	_, err = conn.ExecContext(ctx, "drop table if exists nn_values_subquery_null")
	require.NoError(t, err)
	_, err = conn.ExecContext(ctx, `create table nn_values_subquery_null (id int primary key, a int not null, v int)`)
	require.NoError(t, err)
	_, err = conn.ExecContext(ctx, `insert into nn_values_subquery_null values (1, (select 1), null)`)
	require.NoError(t, err)
	_, err = conn.ExecContext(ctx, `insert into nn_values_subquery_null values (2, null, (select 1))`)
	require.Error(t, err, "NOT NULL must reject a bare NULL even when another column carries a subquery")
	// The checked-out producer is moerr.NewConstraintViolation
	// (pkg/sql/colexec/multi_update/insert.go and
	// pkg/sql/colexec/constraint_util.go: "Column '%s' cannot be null");
	// moerr.ErrConstraintViolation (20304) maps to MySQL errno 3819
	// (ER_CHECK_CONSTRAINT_VIOLATED) on the wire, and the driver
	// surfaces it as a typed MySQL error.
	var mysqlErr *mysql.MySQLError
	require.ErrorAs(t, err, &mysqlErr,
		"the rejection must surface as the driver's typed MySQL error")
	require.Equal(t, uint16(3819), mysqlErr.Number,
		"moerr.ErrConstraintViolation maps to ER_CHECK_CONSTRAINT_VIOLATED")
	require.ErrorContains(t, err, "Column 'a' cannot be null")
	_, err = conn.ExecContext(ctx, `insert into nn_values_subquery_null values (3, (select 3), null)`)
	require.NoError(t, err, "the same connection and statement path must stay usable after the rejection")
	var kept int64
	require.NoError(t, conn.QueryRowContext(ctx, `select count(*) from nn_values_subquery_null`).Scan(&kept))
	require.Equal(t, int64(2), kept)

	// REPLACE lowers through the same branch machinery and must store
	// the NULL and the subquery result the same way.
	_, err = conn.ExecContext(ctx, "drop table if exists r_values_subquery_null")
	require.NoError(t, err)
	_, err = conn.ExecContext(ctx, `create table r_values_subquery_null (id int primary key, a int)`)
	require.NoError(t, err)
	_, err = conn.ExecContext(ctx, `insert into r_values_subquery_null values (2, (select 2))`)
	require.NoError(t, err)
	_, err = conn.ExecContext(ctx, `replace into r_values_subquery_null values (2, null)`)
	require.NoError(t, err)
	var rA sql.NullInt64
	require.NoError(t, conn.QueryRowContext(ctx, `select a from r_values_subquery_null where id = 2`).Scan(&rA))
	require.False(t, rA.Valid, "REPLACE must store the NULL")
	_, err = conn.ExecContext(ctx, `replace into r_values_subquery_null values (2, (select 22))`)
	require.NoError(t, err)
	require.NoError(t, conn.QueryRowContext(ctx, `select a from r_values_subquery_null where id = 2`).Scan(&rA))
	require.True(t, rA.Valid)
	require.Equal(t, int64(22), rA.Int64)

	// Public-SQL BIT carrier witness (PR32350 review P2): a raw BIT payload
	// is never a parsed vector carrier, so the statement must surface the
	// checked string-to-array conversion (Error 20301 "malformed vector
	// input" from the array parser), and the same connection must keep
	// accepting genuine vector literals afterwards.
	_, err = conn.ExecContext(ctx,
		`insert into t_values_subquery_null values (7, (select 7), 7.70, 'g', b'1')`)
	require.Error(t, err, "the raw BIT payload must not pass the vector assignment unchecked")
	require.ErrorAs(t, err, &mysqlErr,
		"the BIT rejection must surface as the driver's typed MySQL error")
	require.Equal(t, uint16(20301), mysqlErr.Number,
		"the checked vector conversion error is moerr.ErrInvalidInput malformed vector input")
	require.ErrorContains(t, err, "malformed vector input")
	_, err = conn.ExecContext(ctx,
		`insert into t_values_subquery_null values (8, (select 8), 8.80, 'h', '[1,2,3]')`)
	require.NoError(t, err, "the same connection must stay usable after the BIT rejection")
	require.NoError(t, conn.QueryRowContext(ctx,
		`select l2_distance(vc, '[1,2,3]') from t_values_subquery_null where id = 8`).Scan(&dist))
	require.Equal(t, float64(0), dist, "the genuine vecf32 literal must round-trip after the BIT rejection")
}
