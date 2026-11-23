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
	"strings"
	"testing"
	"time"

	"github.com/go-sql-driver/mysql"
	"github.com/matrixorigin/matrixone/pkg/embed"
	"github.com/stretchr/testify/require"
)

// TestIssue31901EnumSmallintUnsignedNumericCoercion covers #31901 over the
// real wire: an ENUM column compared with a SMALLINT UNSIGNED column must
// bind in the uint16 numeric domain (member ordinal), with three-valued NULL
// semantics preserved: SELECT projections cover all five operators (=, <=>,
// <, +, *) on both operand orders; WHERE filters cover = and < on both
// operand orders. It uses the shared base-cluster
// fixture with exactly one admission; CloseBaseClusterTests runs only after
// RunBaseClusterTests returns, and the scene database cleanup is registered
// immediately after its create database succeeds so a FailNow still leaves
// no owned catalog state.
func TestIssue31901EnumSmallintUnsignedNumericCoercion(t *testing.T) {
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

		t.Run("issue_31901_enum_smallint_unsigned", func(t *testing.T) {
			issue31901EnumSmallintUnsignedCoercion(t, conn)
		})
	})
}

// dropIssue31901Database drops the scene-owned database with an independent
// bounded context. It is registered immediately after create database
// succeeds so a FailNow inside the scene still leaves no owned catalog
// state, and a cleanup failure is reported without masking the primary
// scene failure.
func dropIssue31901Database(t *testing.T, conn *sql.Conn, name string) {
	t.Helper()
	dropCtx, cancel := context.WithTimeout(context.Background(), time.Minute)
	defer cancel()
	if _, err := conn.ExecContext(dropCtx, "drop database if exists "+name); err != nil {
		t.Errorf("drop owned database %s: %v", name, err)
	}
}

// issue31901EnumSmallintUnsignedCoercion exercises the #31901 contract on a
// persistent table with rows covering equal and unequal ordinal pairs plus
// ENUM-NULL, u16-NULL and both-NULL rows. Assertions are by primary-key id
// sets and full ordered projections, never by WHERE result ordering.
// Ordinals: a=1, b=2, c=3.
func issue31901EnumSmallintUnsignedCoercion(t *testing.T, conn *sql.Conn) {
	t.Helper()
	const dbName = "issue_31901_enum_u16"
	ctx, cancel := context.WithTimeout(t.Context(), 5*time.Minute)
	defer cancel()

	_, err := conn.ExecContext(ctx, "create database if not exists "+dbName)
	require.NoError(t, err)
	defer dropIssue31901Database(t, conn, dbName)
	_, err = conn.ExecContext(ctx, "use "+dbName)
	require.NoError(t, err)
	_, err = conn.ExecContext(ctx, "drop table if exists t_enum_u16, t_enum_label")
	require.NoError(t, err)
	_, err = conn.ExecContext(ctx,
		`create table t_enum_u16 (
				id int primary key,
				e enum('a','b','c'),
				s smallint unsigned
			)`)
	require.NoError(t, err)
	_, err = conn.ExecContext(ctx,
		`insert into t_enum_u16 values
				(1, 'b', 2),
				(2, 'c', 2),
				(3, 'a', 3),
				(4, null, 2),
				(5, 'b', null),
				(6, null, null)`)
	require.NoError(t, err)

	// Full projection, both operand orders: uint16-domain comparison and
	// arithmetic, three-valued NULL preserved for every operator.
	type projection struct {
		id           int64
		eqES, eqSE   sql.NullBool
		nseES, nseSE sql.NullBool
		ltES, ltSE   sql.NullBool
		addES, addSE sql.NullInt64
		mulES, mulSE sql.NullInt64
	}
	nb := func(v bool) sql.NullBool { return sql.NullBool{Bool: v, Valid: true} }
	nn := func(v int64) sql.NullInt64 { return sql.NullInt64{Int64: v, Valid: true} }
	rows, err := conn.QueryContext(ctx,
		`select id, e = s, s = e, e <=> s, s <=> e, e < s, s < e,
				e + s, s + e, e * s, s * e
				from t_enum_u16 order by id`)
	require.NoError(t, err)
	defer rows.Close()
	got := make([]projection, 0, 6)
	for rows.Next() {
		var p projection
		require.NoError(t, rows.Scan(&p.id, &p.eqES, &p.eqSE, &p.nseES, &p.nseSE,
			&p.ltES, &p.ltSE, &p.addES, &p.addSE, &p.mulES, &p.mulSE))
		got = append(got, p)
	}
	require.NoError(t, rows.Err())
	require.Equal(t, []projection{
		{1, nb(true), nb(true), nb(true), nb(true), nb(false), nb(false), nn(4), nn(4), nn(4), nn(4)},
		{2, nb(false), nb(false), nb(false), nb(false), nb(false), nb(true), nn(5), nn(5), nn(6), nn(6)},
		{3, nb(false), nb(false), nb(false), nb(false), nb(true), nb(false), nn(4), nn(4), nn(3), nn(3)},
		{4, sql.NullBool{}, sql.NullBool{}, nb(false), nb(false), sql.NullBool{}, sql.NullBool{}, sql.NullInt64{}, sql.NullInt64{}, sql.NullInt64{}, sql.NullInt64{}},
		{5, sql.NullBool{}, sql.NullBool{}, nb(false), nb(false), sql.NullBool{}, sql.NullBool{}, sql.NullInt64{}, sql.NullInt64{}, sql.NullInt64{}, sql.NullInt64{}},
		{6, sql.NullBool{}, sql.NullBool{}, nb(true), nb(true), sql.NullBool{}, sql.NullBool{}, sql.NullInt64{}, sql.NullInt64{}, sql.NullInt64{}, sql.NullInt64{}},
	}, got)

	// WHERE filters cover = and < on both operand orders; assert the
	// returned primary-key id sets, ordered by id, never guessed from
	// result ordering.
	idsWhere := func(cond string) []int64 {
		wRows, err := conn.QueryContext(ctx,
			`select id from t_enum_u16 where `+cond+` order by id`)
		require.NoError(t, err)
		defer wRows.Close()
		var ids []int64
		for wRows.Next() {
			var id int64
			require.NoError(t, wRows.Scan(&id))
			ids = append(ids, id)
		}
		require.NoError(t, wRows.Err())
		return ids
	}
	require.Equal(t, []int64{1}, idsWhere(`e = s`), "e = s keeps only the equal-ordinal row")
	require.Equal(t, []int64{1}, idsWhere(`s = e`), "s = e is the reversed equal-ordinal row")
	require.Equal(t, []int64{3}, idsWhere(`e < s`), "e < s keeps a=1 below 3")
	require.Equal(t, []int64{2}, idsWhere(`s < e`), "s < e keeps 2 below c=3")

	// The ENUM column still displays its member label, not its ordinal.
	var label string
	require.NoError(t, conn.QueryRowContext(ctx,
		`select e from t_enum_u16 where id = 1`).Scan(&label))
	require.Equal(t, "b", label)

	// Label-domain controls: ENUM vs ENUM with a different member order and
	// ENUM vs varchar compare by label, not by ordinal position.
	_, err = conn.ExecContext(ctx,
		`create table t_enum_label (
				id int primary key,
				e1 enum('a','b','c'),
				e2 enum('c','b','a'),
				v varchar(8)
			)`)
	require.NoError(t, err)
	_, err = conn.ExecContext(ctx,
		`insert into t_enum_label values (1, 'a', 'a', 'a'), (2, 'b', 'a', 'b')`)
	require.NoError(t, err)
	type labelRow struct {
		id            int64
		e1eqE2, e1eqV bool
		e2eqV         bool
	}
	lRows, err := conn.QueryContext(ctx,
		`select id, e1 = e2, e1 = v, e2 = v from t_enum_label order by id`)
	require.NoError(t, err)
	defer lRows.Close()
	var labels []labelRow
	for lRows.Next() {
		var r labelRow
		require.NoError(t, lRows.Scan(&r.id, &r.e1eqE2, &r.e1eqV, &r.e2eqV))
		labels = append(labels, r)
	}
	require.NoError(t, lRows.Err())
	require.Equal(t, []labelRow{
		{1, true, true, true},
		{2, false, true, false},
	}, labels, "row 1: a=a across ordinals 1 vs 3; row 2: b<>a by label, b=b vs varchar")

	// Real COM_STMT_PREPARE/EXECUTE on the same connection: the table-column
	// comparison stays inside the prepared statement; parameters only select
	// ids. Repeated execution includes NULL and ordinary parameters.
	stmtEq, err := conn.PrepareContext(ctx, `select e = s from t_enum_u16 where id = ?`)
	require.NoError(t, err)
	defer stmtEq.Close()
	var eq1 bool
	require.NoError(t, stmtEq.QueryRowContext(ctx, 1).Scan(&eq1))
	require.True(t, eq1, "id 1: b ordinal 2 equals s 2")
	var eq2 bool
	require.NoError(t, stmtEq.QueryRowContext(ctx, 2).Scan(&eq2))
	require.False(t, eq2, "id 2: c ordinal 3 differs from s 2")
	var eqNull sql.NullBool
	require.NoError(t, stmtEq.QueryRowContext(ctx, 4).Scan(&eqNull))
	require.False(t, eqNull.Valid, "id 4: ENUM NULL keeps the comparison NULL")
	require.ErrorIs(t, stmtEq.QueryRowContext(ctx, nil).Scan(&eq1), sql.ErrNoRows,
		"a NULL id parameter matches no row")
	require.NoError(t, stmtEq.QueryRowContext(ctx, 1).Scan(&eq1))
	require.True(t, eq1, "the prepared statement stays reusable")

	stmtAdd, err := conn.PrepareContext(ctx,
		`select id, e + s from t_enum_u16 where id in (?, ?) order by id`)
	require.NoError(t, err)
	defer stmtAdd.Close()
	collect := func(a, b any) map[int64]int64 {
		pRows, err := stmtAdd.QueryContext(ctx, a, b)
		require.NoError(t, err)
		defer pRows.Close()
		out := map[int64]int64{}
		for pRows.Next() {
			var id, sum int64
			require.NoError(t, pRows.Scan(&id, &sum))
			out[id] = sum
		}
		require.NoError(t, pRows.Err())
		return out
	}
	require.Equal(t, map[int64]int64{1: 4, 3: 4}, collect(1, 3))
	require.Equal(t, map[int64]int64{2: 5}, collect(2, nil),
		"an IN-list NULL parameter matches no row but keeps the statement valid")
	require.Empty(t, collect(nil, nil), "two NULL parameters match nothing")

	// Pin the strict-assignment precondition on this connection instead of
	// assuming a server default: func_mo.go's allowEnumSetAdjustment is
	// IGNORE || !strict, which would downgrade the rejection to a warning.
	// Save the session sql_mode, restore it with an independent bounded
	// context at scene exit (runs before the database drop, while the
	// connection is alive and primary errors are already recorded), set
	// exactly STRICT_TRANS_TABLES and read it back before relying on the
	// truncation-as-error contract.
	var savedMode string
	require.NoError(t, conn.QueryRowContext(ctx, `select @@session.sql_mode`).Scan(&savedMode))
	defer func() {
		restoreCtx, cancel := context.WithTimeout(context.Background(), time.Minute)
		defer cancel()
		if _, err := conn.ExecContext(restoreCtx,
			"set session sql_mode = '"+strings.ReplaceAll(savedMode, "'", "''")+"'"); err != nil {
			t.Errorf("restore session sql_mode: %v", err)
		}
	}()
	_, err = conn.ExecContext(ctx, "set session sql_mode = 'STRICT_TRANS_TABLES'")
	require.NoError(t, err)
	var effectiveMode string
	require.NoError(t, conn.QueryRowContext(ctx, `select @@session.sql_mode`).Scan(&effectiveMode))
	require.Equal(t, "STRICT_TRANS_TABLES", effectiveMode,
		"the strict-assignment precondition must be pinned on this connection, not assumed")

	// Error path, pinned to the real producer: the enum assignment cast
	// (pkg/sql/plan/function/func_mo.go, CastEnumIndexValueToIndex ->
	// types.ParseEnumAssignment truncation -> recordEnumSetTruncation)
	// rejects a non-member label under strict sql_mode with
	// moerr.NewEnumSetValueTruncated (ErrEnumSetValueTruncated 20210),
	// which pkg/common/moerr/error.go maps to MySQL errno 1265
	// (WARN_DATA_TRUNCATED) with SQLSTATE 01000 and message
	// "Data truncated for ENUM value: '<label>'". The same connection and
	// the same prepared statement must keep working afterwards, and the
	// rejected row must not be stored.
	_, err = conn.ExecContext(ctx, `insert into t_enum_u16 values (7, 'zzz', 1)`)
	require.Error(t, err, "a non-member ENUM label must be rejected, not silently coerced")
	var mysqlErr *mysql.MySQLError
	require.ErrorAs(t, err, &mysqlErr,
		"the rejection must surface as the driver's typed MySQL error")
	require.Equal(t, uint16(1265), mysqlErr.Number,
		"ErrEnumSetValueTruncated maps to WARN_DATA_TRUNCATED")
	require.Equal(t, "01000", string(mysqlErr.SQLState[:]),
		"ErrEnumSetValueTruncated maps to SQLSTATE 01000 (pkg/common/moerr/error.go:535)")
	require.ErrorContains(t, err, "Data truncated for ENUM value: 'zzz'")
	var one int64
	require.NoError(t, conn.QueryRowContext(ctx, `select 1`).Scan(&one))
	require.Equal(t, int64(1), one, "the same connection must stay usable after the rejection")
	require.NoError(t, stmtEq.QueryRowContext(ctx, 1).Scan(&eq1))
	require.True(t, eq1, "the same prepared statement must stay usable after the rejection")
	var kept int64
	require.NoError(t, conn.QueryRowContext(ctx, `select count(*) from t_enum_u16`).Scan(&kept))
	require.Equal(t, int64(6), kept, "the rejected row must not be stored")
	var stray int64
	require.NoError(t, conn.QueryRowContext(ctx,
		`select count(*) from t_enum_u16 where id = 7`).Scan(&stray))
	require.Equal(t, int64(0), stray)
}
