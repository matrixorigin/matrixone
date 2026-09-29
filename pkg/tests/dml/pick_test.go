// Copyright 2021 - 2024 Matrix Origin
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

package dml

import (
	"context"
	"database/sql"
	"fmt"
	"sort"
	"strings"
	"testing"
	"time"

	_ "github.com/go-sql-driver/mysql"
	"github.com/stretchr/testify/require"

	"github.com/matrixorigin/matrixone/pkg/embed"
	"github.com/matrixorigin/matrixone/pkg/tests/testutils"
)

func TestDataBranchPick(t *testing.T) {
	embed.RunBaseClusterTests(t,
		func(c embed.Cluster) {
			ctx, cancel := context.WithTimeout(context.Background(), time.Second*360)
			defer cancel()

			cn1, err := c.GetCNService(0)
			require.NoError(t, err)

			port := cn1.GetServiceConfig().CN.Frontend.Port
			dsn := fmt.Sprintf("dump:111@tcp(127.0.0.1:%d)/", port)
			sqlDB, err := sql.Open("mysql", dsn)
			require.NoError(t, err)
			defer sqlDB.Close()
			sqlDB.SetMaxOpenConns(1)

			dbName := testutils.GetDatabaseName(t)
			defer cleanupTestDatabases(t, sqlDB, dbName)
			execSQLDB(t, ctx, sqlDB, fmt.Sprintf("create database `%s`", dbName))
			execSQLDB(t, ctx, sqlDB, fmt.Sprintf("use `%s`", dbName))

			t.Run("key_values_and_consecutive_pick", func(t *testing.T) {
				runPickByKeyValues(t, ctx, sqlDB)
			})
			t.Run("conflict_policies", func(t *testing.T) {
				runPickConflictMatrix(t, ctx, sqlDB)
			})
			t.Run("accept_mixed_update_and_insert", func(t *testing.T) {
				runPickAcceptMixedUpdateAndInsert(t, ctx, sqlDB)
			})
			t.Run("subquery_keys", func(t *testing.T) {
				runPickSubqueryKeys(t, ctx, sqlDB)
			})
			t.Run("subquery_key_coercion", func(t *testing.T) {
				runPickSubqueryKeyCoercion(t, ctx, sqlDB)
			})
			t.Run("subquery_rejects_null_key", func(t *testing.T) {
				runPickSubqueryRejectsNullKey(t, ctx, sqlDB)
			})
			t.Run("subquery_rejects_invalid_coercion", func(t *testing.T) {
				runPickSubqueryRejectsInvalidCoercion(t, ctx, sqlDB)
			})
			t.Run("varchar_primary_key", func(t *testing.T) {
				runPickVarcharPK(t, ctx, sqlDB)
			})
			t.Run("varchar_escaped_lca_conflict", func(t *testing.T) {
				runPickVarcharPKLCAEscapedDeleteUpdate(t, ctx, sqlDB)
			})
			t.Run("non_overlapping_destination_data", func(t *testing.T) {
				runPickIntoExistingData(t, ctx, sqlDB)
			})
			t.Run("mixed_insert_update_delete", func(t *testing.T) {
				runPickMixedOperations(t, ctx, sqlDB)
			})
			t.Run("rejects_destination_snapshot", func(t *testing.T) {
				runPickRejectDstSnapshot(t, ctx, sqlDB)
			})
			t.Run("rejects_explicit_transaction", func(t *testing.T) {
				runPickRejectExplicitTransaction(t, ctx, sqlDB)
			})
		})
}

// ---------------------------------------------------------------------------
// helpers
// ---------------------------------------------------------------------------

// queryIntColumn returns a sorted slice of int values from a single-column query.
func queryIntColumn(t *testing.T, ctx context.Context, db *sql.DB, q string) []int {
	t.Helper()
	rows, err := db.QueryContext(ctx, q)
	require.NoErrorf(t, err, "sql: %s", q)
	defer rows.Close()

	var vals []int
	for rows.Next() {
		var v int
		require.NoError(t, rows.Scan(&v))
		vals = append(vals, v)
	}
	require.NoErrorf(t, rows.Err(), "sql: %s", q)
	sort.Ints(vals)
	return vals
}

// queryRowCount returns the number of rows a query produces.
func queryRowCount(t *testing.T, ctx context.Context, db *sql.DB, q string) int {
	t.Helper()
	var cnt int
	err := db.QueryRowContext(ctx, q).Scan(&cnt)
	require.NoErrorf(t, err, "sql: %s", q)
	return cnt
}

// queryStringRows returns all rows as [][]string with NULL representation.
func queryStringRows(t *testing.T, ctx context.Context, db *sql.DB, q string) [][]string {
	t.Helper()
	rows, err := db.QueryContext(ctx, q)
	require.NoErrorf(t, err, "sql: %s", q)
	defer rows.Close()

	cols, err := rows.Columns()
	require.NoError(t, err)

	var result [][]string
	for rows.Next() {
		raw := make([]sql.RawBytes, len(cols))
		dest := make([]any, len(cols))
		for i := range raw {
			dest[i] = &raw[i]
		}
		require.NoError(t, rows.Scan(dest...))

		row := make([]string, len(cols))
		for i, b := range raw {
			if b == nil {
				row[i] = "NULL"
			} else {
				row[i] = string(b)
			}
		}
		result = append(result, row)
	}
	require.NoErrorf(t, rows.Err(), "sql: %s", q)
	return result
}

// execExpectError runs a statement expecting a non-nil error and returns the error message.
func execExpectError(t *testing.T, ctx context.Context, db *sql.DB, stmt string) string {
	t.Helper()
	_, err := db.ExecContext(ctx, stmt)
	require.Errorf(t, err, "expected error for: %s", stmt)
	return err.Error()
}

// cleanupPickCaseTables lets the orthogonal pick scenarios share one database
// while retaining fresh table state. Descendants are listed before their base
// tables so branch metadata is removed in dependency order.
func cleanupPickCaseTables(t *testing.T, db *sql.DB) {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	_, err := db.ExecContext(ctx,
		"drop table if exists dst, src, base, pick_keys, ckeys, csrc, cbase")
	if err != nil {
		t.Errorf("clean up shared data branch pick tables: %v", err)
	}
}

// ---------------------------------------------------------------------------
// test cases
// ---------------------------------------------------------------------------

// runPickByKeyValues covers subset selection, an already-identical key, and a
// second pick from the same source without rebuilding an equivalent fixture.
func runPickByKeyValues(t *testing.T, parentCtx context.Context, db *sql.DB) {
	t.Helper()
	ctx, cancel := context.WithTimeout(parentCtx, 90*time.Second)
	defer cancel()

	defer cleanupPickCaseTables(t, db)

	execSQLDB(t, ctx, db, "create table base (a int primary key, b int)")
	execSQLDB(t, ctx, db, "insert into base values (1,10),(2,20),(3,30)")
	execSQLDB(t, ctx, db, "data branch create table src from base")
	execSQLDB(t, ctx, db, "insert into src values (4,40),(5,50)")

	execSQLDB(t, ctx, db, "data branch pick src into base keys(2,4)")

	// base should now have: 1,2,3 (original) + 4 (picked) = 4 rows.
	// Key 2 was already in base; since values are identical (20), no conflict.
	pks := queryIntColumn(t, ctx, db, "select a from base order by a")
	require.Equal(t, []int{1, 2, 3, 4}, pks)

	// Verify picked value
	var b int
	require.NoError(t, db.QueryRowContext(ctx, "select b from base where a=4").Scan(&b))
	require.Equal(t, 40, b)

	// A second pick proves the source remains usable and completes the set. This
	// subsumes the old duplicate "pick all" and "consecutive" fixtures.
	execSQLDB(t, ctx, db, "data branch pick src into base keys(5)")
	pks = queryIntColumn(t, ctx, db, "select a from base order by a")
	require.Equal(t, []int{1, 2, 3, 4, 5}, pks)
}

// runPickConflictMatrix covers four conflict shapes against all three policies.
// The shapes use different keys in one common fixture. Each policy has its own
// sibling destination, so the matrix keeps policy histories independent while
// sharing the branch construction required by every shape.
func runPickConflictMatrix(t *testing.T, parentCtx context.Context, db *sql.DB) {
	t.Helper()
	// The parent already provides the suite's 360-second hang guard. Reuse it
	// for the whole matrix rather than giving the 12 cases a tighter aggregate
	// deadline than they had on slower CI runners.
	ctx, cancel := context.WithCancel(parentCtx)
	defer cancel()

	execSQLDB(t, ctx, db, "create table conflict_base (a int primary key, b int)")
	execSQLDB(t, ctx, db, "insert into conflict_base values (1,10),(2,20),(4,40),(5,50)")

	type conflictShape struct {
		name string
		key  int
	}

	shapes := []conflictShape{
		{name: "insert_insert", key: 3},
		{name: "update_update", key: 1},
		{name: "update_delete", key: 4},
		{name: "delete_update", key: 5},
	}

	// Create every branch from the same unmodified ancestor. In particular, do
	// not clone destinations from src: doing so would turn insert/insert into an
	// update conflict and would lose the LCA relationship being tested.
	execSQLDB(t, ctx, db, "data branch create table conflict_src from conflict_base")
	execSQLDB(t, ctx, db, "data branch create table conflict_dst_skip from conflict_base")
	execSQLDB(t, ctx, db, "data branch create table conflict_dst_accept from conflict_base")
	execSQLDB(t, ctx, db, "data branch create table conflict_dst_fail from conflict_base")

	// The source carries one representative of each conflict shape. The
	// destinations carry the opposing side of each conflict, plus the same
	// untouched key 2 sentinel.
	execSQLDB(t, ctx, db, "insert into conflict_src values (3,300)")
	execSQLDB(t, ctx, db, "update conflict_src set b=111 where a=1")
	execSQLDB(t, ctx, db, "update conflict_src set b=444 where a=4")
	execSQLDB(t, ctx, db, "delete from conflict_src where a=5")

	for _, dst := range []string{"conflict_dst_skip", "conflict_dst_accept", "conflict_dst_fail"} {
		execSQLDB(t, ctx, db, fmt.Sprintf("insert into %s values (3,999)", dst))
		execSQLDB(t, ctx, db, fmt.Sprintf("update %s set b=999 where a=1", dst))
		execSQLDB(t, ctx, db, fmt.Sprintf("delete from %s where a=4", dst))
		execSQLDB(t, ctx, db, fmt.Sprintf("update %s set b=999 where a=5", dst))
	}

	sourceRows := queryStringRows(t, ctx, db, "select * from conflict_src order by a")
	expectedSourceRows := [][]string{
		{"1", "111"},
		{"2", "20"},
		{"3", "300"},
		{"4", "444"},
	}
	require.Equal(t, expectedSourceRows, sourceRows, "source fixture must contain all four conflict shapes")
	initialDestinationRows := [][]string{
		{"1", "999"},
		{"2", "20"},
		{"3", "999"},
		{"5", "999"},
	}
	cloneRows := func(rows [][]string) [][]string {
		cloned := make([][]string, len(rows))
		for i, row := range rows {
			cloned[i] = append([]string(nil), row...)
		}
		return cloned
	}
	applyAcceptedShape := func(rows [][]string, shape string) [][]string {
		accepted := cloneRows(rows)
		switch shape {
		case "insert_insert":
			for _, row := range accepted {
				if row[0] == "3" {
					row[1] = "300"
				}
			}
		case "update_update":
			for _, row := range accepted {
				if row[0] == "1" {
					row[1] = "111"
				}
			}
		case "update_delete":
			accepted = append(accepted, []string{"4", "444"})
		case "delete_update":
			filtered := accepted[:0]
			for _, row := range accepted {
				if row[0] != "5" {
					filtered = append(filtered, row)
				}
			}
			accepted = filtered
		}
		sort.Slice(accepted, func(i, j int) bool { return accepted[i][0] < accepted[j][0] })
		return accepted
	}
	acceptedRows := cloneRows(initialDestinationRows)
	for _, dst := range []string{"conflict_dst_skip", "conflict_dst_accept", "conflict_dst_fail"} {
		require.Equal(t, initialDestinationRows,
			queryStringRows(t, ctx, db, "select * from "+dst+" order by a"),
			"all policy destinations must start from the same conflict fixture")
	}

	for _, shape := range shapes {
		t.Run(shape.name, func(t *testing.T) {
			for _, policy := range []string{"skip", "accept", "fail"} {
				t.Run(policy, func(t *testing.T) {
					dst := "conflict_dst_" + policy

					stmt := fmt.Sprintf(
						"data branch pick %s into %s keys(%d) when conflict %s",
						"conflict_src", dst, shape.key, policy)
					if policy == "fail" {
						errMsg := execExpectError(t, ctx, db, stmt)
						require.Contains(t, strings.ToLower(errMsg), "conflict")
						require.Equal(t, initialDestinationRows,
							queryStringRows(t, ctx, db, "select * from "+dst+" order by a"),
							"fail must leave the full destination unchanged")
					} else if policy == "skip" {
						execSQLDB(t, ctx, db, stmt)
						require.Equal(t, initialDestinationRows,
							queryStringRows(t, ctx, db, "select * from "+dst+" order by a"),
							"skip must retain the destination version")
					} else {
						expectedRows := applyAcceptedShape(acceptedRows, shape.name)
						execSQLDB(t, ctx, db, stmt)
						require.Equal(t, expectedRows,
							queryStringRows(t, ctx, db, "select * from "+dst+" order by a"),
							"accept must apply the source version while retaining accepted changes")
						acceptedRows = expectedRows
					}
					require.Equal(t, sourceRows, queryStringRows(t, ctx, db,
						"select * from conflict_src order by a"), "PICK must leave the source unchanged")
				})
			}
		})
	}
}

func runPickAcceptMixedUpdateAndInsert(t *testing.T, parentCtx context.Context, db *sql.DB) {
	t.Helper()
	ctx, cancel := context.WithTimeout(parentCtx, 90*time.Second)
	defer cancel()

	defer cleanupPickCaseTables(t, db)

	execSQLDB(t, ctx, db, "create table base (a int primary key, b int)")
	execSQLDB(t, ctx, db, "insert into base values (1,10),(2,20)")
	execSQLDB(t, ctx, db, "data branch create table src from base")
	execSQLDB(t, ctx, db, "data branch create table dst from base")
	execSQLDB(t, ctx, db, "update src set b=11 where a=1")
	execSQLDB(t, ctx, db, "insert into src values (3,30)")
	execSQLDB(t, ctx, db, "update dst set b=99 where a=1")

	execSQLDB(t, ctx, db, "data branch pick src into dst keys(1,3) when conflict accept")
	require.Equal(t, [][]string{{"1", "11"}, {"2", "20"}, {"3", "30"}},
		queryStringRows(t, ctx, db, "select a, b from dst order by a"))
}

// runPickSubqueryKeys: use a SELECT subquery to specify which PKs to pick.
func runPickSubqueryKeys(t *testing.T, parentCtx context.Context, db *sql.DB) {
	t.Helper()
	ctx, cancel := context.WithTimeout(parentCtx, 90*time.Second)
	defer cancel()

	defer cleanupPickCaseTables(t, db)

	execSQLDB(t, ctx, db, "create table base (a int primary key, b int)")
	execSQLDB(t, ctx, db, "insert into base values (1,10),(2,20),(3,30)")
	execSQLDB(t, ctx, db, "data branch create table src from base")
	execSQLDB(t, ctx, db,
		"insert into src select result, result * 10 from generate_series(4,20) g")

	// Create a helper table with the keys we want to pick
	execSQLDB(t, ctx, db, "create table pick_keys (k int)")
	execSQLDB(t, ctx, db, "insert into pick_keys values (5),(10),(15),(20)")

	execSQLDB(t, ctx, db,
		"data branch pick src into base keys(select k from pick_keys order by k asc)")

	pks := queryIntColumn(t, ctx, db, "select a from base order by a")
	require.Equal(t, []int{1, 2, 3, 5, 10, 15, 20}, pks)

	// Verify one of the picked values
	var b int
	require.NoError(t, db.QueryRowContext(ctx, "select b from base where a=15").Scan(&b))
	require.Equal(t, 150, b)
}

// runPickSubqueryKeyCoercion verifies subquery outputs are coerced to PK types.
func runPickSubqueryKeyCoercion(t *testing.T, parentCtx context.Context, db *sql.DB) {
	t.Helper()
	ctx, cancel := context.WithTimeout(parentCtx, 90*time.Second)
	defer cancel()

	defer cleanupPickCaseTables(t, db)

	execSQLDB(t, ctx, db, "create table base (a bigint primary key, b int)")
	execSQLDB(t, ctx, db, "insert into base values (1,10)")
	execSQLDB(t, ctx, db, "data branch create table src from base")
	execSQLDB(t, ctx, db, "insert into src values (10,100),(20,200)")
	execSQLDB(t, ctx, db, "create table pick_keys (k int)")
	execSQLDB(t, ctx, db, "insert into pick_keys values (10),(20)")
	execSQLDB(t, ctx, db,
		"data branch pick src into base keys(select k from pick_keys order by k)")

	pks := queryIntColumn(t, ctx, db, "select cast(a as signed) from base order by a")
	require.Equal(t, []int{1, 10, 20}, pks)

	execSQLDB(t, ctx, db, "create table cbase (a bigint, b varchar(16), v int, primary key(a, b))")
	execSQLDB(t, ctx, db, "insert into cbase values (1,'k1',10)")
	execSQLDB(t, ctx, db, "data branch create table csrc from cbase")
	execSQLDB(t, ctx, db, "insert into csrc values (10,'x',100),(20,'y',200)")
	execSQLDB(t, ctx, db, "create table ckeys (a int, b char(16))")
	execSQLDB(t, ctx, db, "insert into ckeys values (10,'x'),(20,'y')")
	execSQLDB(t, ctx, db,
		"data branch pick csrc into cbase keys(select a, b from ckeys order by a, b)")

	require.Equal(t, 1, queryRowCount(t, ctx, db, "select count(*) from cbase where a=10 and b='x' and v=100"))
	require.Equal(t, 1, queryRowCount(t, ctx, db, "select count(*) from cbase where a=20 and b='y' and v=200"))
}

func runPickSubqueryRejectsNullKey(t *testing.T, parentCtx context.Context, db *sql.DB) {
	t.Helper()
	ctx, cancel := context.WithTimeout(parentCtx, 90*time.Second)
	defer cancel()

	defer cleanupPickCaseTables(t, db)

	execSQLDB(t, ctx, db, "create table base (a int primary key, b int)")
	execSQLDB(t, ctx, db, "insert into base values (1,10)")
	execSQLDB(t, ctx, db, "data branch create table src from base")
	execSQLDB(t, ctx, db, "insert into src values (10,100)")
	execSQLDB(t, ctx, db, "create table pick_keys (k int)")
	execSQLDB(t, ctx, db, "insert into pick_keys values (10),(null)")

	errMsg := execExpectError(t, ctx, db,
		"data branch pick src into base keys(select k from pick_keys order by k)")
	require.Contains(t, strings.ToLower(errMsg), "cannot be null")
	require.Equal(t, []int{1}, queryIntColumn(t, ctx, db, "select a from base order by a"))
}

func runPickSubqueryRejectsInvalidCoercion(t *testing.T, parentCtx context.Context, db *sql.DB) {
	t.Helper()
	ctx, cancel := context.WithTimeout(parentCtx, 90*time.Second)
	defer cancel()

	defer cleanupPickCaseTables(t, db)

	execSQLDB(t, ctx, db, "create table base (a int primary key, b int)")
	execSQLDB(t, ctx, db, "insert into base values (1,10)")
	execSQLDB(t, ctx, db, "data branch create table src from base")
	execSQLDB(t, ctx, db, "insert into src values (10,100)")
	execSQLDB(t, ctx, db, "create table pick_keys (k varchar(20))")
	execSQLDB(t, ctx, db, "insert into pick_keys values ('oops')")

	errMsg := execExpectError(t, ctx, db,
		"data branch pick src into base keys(select k from pick_keys)")
	require.Contains(t, strings.ToLower(errMsg), "cannot be converted")
	require.Equal(t, []int{1}, queryIntColumn(t, ctx, db, "select a from base order by a"))
}

// runPickVarcharPK: pick with non-integer PK.
func runPickVarcharPK(t *testing.T, parentCtx context.Context, db *sql.DB) {
	t.Helper()
	ctx, cancel := context.WithTimeout(parentCtx, 90*time.Second)
	defer cancel()

	defer cleanupPickCaseTables(t, db)

	execSQLDB(t, ctx, db, "create table base (name varchar(64) primary key, score int)")
	execSQLDB(t, ctx, db, "insert into base values ('alice',85),('bob',90)")
	execSQLDB(t, ctx, db, "data branch create table src from base")
	execSQLDB(t, ctx, db, "insert into src values ('charlie',78),('diana',92),('eve',88)")

	execSQLDB(t, ctx, db, "data branch pick src into base keys('charlie','eve')")

	rows := queryStringRows(t, ctx, db, "select name from base order by name")
	names := make([]string, len(rows))
	for i, r := range rows {
		names[i] = r[0]
	}
	require.Equal(t, []string{"alice", "bob", "charlie", "eve"}, names)
}

func runPickVarcharPKLCAEscapedDeleteUpdate(t *testing.T, parentCtx context.Context, db *sql.DB) {
	t.Helper()
	ctx, cancel := context.WithTimeout(parentCtx, 90*time.Second)
	defer cancel()

	defer cleanupPickCaseTables(t, db)

	execSQLDB(t, ctx, db, "create table base (name varchar(64) primary key, score int)")
	execSQLDB(t, ctx, db,
		"insert into base values ('o''hara',85),('slash\\\\path',90),('plain',95)")
	execSQLDB(t, ctx, db, "data branch create table src from base")
	execSQLDB(t, ctx, db, "data branch create table dst from base")

	execSQLDB(t, ctx, db, "delete from src where name in ('o''hara','slash\\\\path')")
	execSQLDB(t, ctx, db, "update dst set score=999 where name='o''hara'")

	execSQLDB(t, ctx, db,
		"data branch pick src into dst keys('o''hara','slash\\\\path') when conflict accept")

	rows := queryStringRows(t, ctx, db, "select name, score from dst order by name")
	require.Equal(t, [][]string{{"plain", "95"}}, rows)
}

// runPickIntoExistingData: dst already has rows that don't overlap with src.
func runPickIntoExistingData(t *testing.T, parentCtx context.Context, db *sql.DB) {
	t.Helper()
	ctx, cancel := context.WithTimeout(parentCtx, 90*time.Second)
	defer cancel()

	defer cleanupPickCaseTables(t, db)

	execSQLDB(t, ctx, db, "create table base (a int primary key, b int)")
	execSQLDB(t, ctx, db, "insert into base values (1,10)")
	execSQLDB(t, ctx, db, "data branch create table src from base")
	execSQLDB(t, ctx, db, "data branch create table dst from base")

	// src adds rows
	execSQLDB(t, ctx, db, "insert into src values (10,100),(20,200)")
	// dst independently adds different rows
	execSQLDB(t, ctx, db, "insert into dst values (50,500),(60,600)")

	execSQLDB(t, ctx, db, "data branch pick src into dst keys(1,10,20)")

	pks := queryIntColumn(t, ctx, db, "select a from dst order by a")
	require.Equal(t, []int{1, 10, 20, 50, 60}, pks)
}

// runPickMixedOperations: source has INSERT + UPDATE + DELETE, pick all.
func runPickMixedOperations(t *testing.T, parentCtx context.Context, db *sql.DB) {
	t.Helper()
	ctx, cancel := context.WithTimeout(parentCtx, 90*time.Second)
	defer cancel()

	defer cleanupPickCaseTables(t, db)

	execSQLDB(t, ctx, db, "create table base (a int primary key, b int, c varchar(32))")
	execSQLDB(t, ctx, db, "insert into base values (1,10,'x'),(2,20,'y'),(3,30,'z'),(4,40,'w'),(5,50,'v')")
	execSQLDB(t, ctx, db, "data branch create table src from base")

	// Mixed operations on src:
	execSQLDB(t, ctx, db, "delete from src where a=2")                     // DELETE
	execSQLDB(t, ctx, db, "update src set b=99, c='updated' where a=3")    // UPDATE
	execSQLDB(t, ctx, db, "insert into src values (6,60,'inserted')")      // INSERT
	execSQLDB(t, ctx, db, "delete from src where a=5")                     // DELETE
	execSQLDB(t, ctx, db, "insert into src values (7,70,'also_inserted')") // INSERT

	execSQLDB(t, ctx, db, "data branch pick src into base keys(1,2,3,4,5,6,7)")

	// Expected: base should match src's current state
	// src has: 1(10,x), 3(99,updated), 4(40,w), 6(60,inserted), 7(70,also_inserted)
	pks := queryIntColumn(t, ctx, db, "select a from base order by a")
	require.Equal(t, []int{1, 3, 4, 6, 7}, pks)

	// Verify updated row
	var b int
	var c string
	require.NoError(t, db.QueryRowContext(ctx, "select b, c from base where a=3").Scan(&b, &c))
	require.Equal(t, 99, b)
	require.Equal(t, "updated", c)

	// Verify deleted rows are gone
	cnt := queryRowCount(t, ctx, db, "select count(*) from base where a in (2,5)")
	require.Equal(t, 0, cnt)

	// Verify inserted row
	require.NoError(t, db.QueryRowContext(ctx, "select b, c from base where a=6").Scan(&b, &c))
	require.Equal(t, 60, b)
	require.Equal(t, "inserted", c)
}

func runPickRejectDstSnapshot(t *testing.T, parentCtx context.Context, db *sql.DB) {
	t.Helper()
	ctx, cancel := context.WithTimeout(parentCtx, 90*time.Second)
	defer cancel()

	defer cleanupPickCaseTables(t, db)

	execSQLDB(t, ctx, db, "create table base (a int primary key, b int)")
	execSQLDB(t, ctx, db, "insert into base values (1,10)")
	execSQLDB(t, ctx, db, "data branch create table src from base")
	execSQLDB(t, ctx, db, "create table dst like base")

	errMsg := execExpectError(t, ctx, db,
		"data branch pick src into dst{snapshot=sp_dst} keys(1)")
	require.Contains(t, strings.ToLower(errMsg), "destination snapshot")
	require.Equal(t, 0, queryRowCount(t, ctx, db, "select count(*) from dst"))
}

func runPickRejectExplicitTransaction(t *testing.T, parentCtx context.Context, db *sql.DB) {
	t.Helper()
	ctx, cancel := context.WithTimeout(parentCtx, 90*time.Second)
	defer cancel()

	defer cleanupPickCaseTables(t, db)

	execSQLDB(t, ctx, db, "create table base (a int primary key, b int)")
	execSQLDB(t, ctx, db, "insert into base values (1,10)")
	execSQLDB(t, ctx, db, "data branch create table src from base")
	execSQLDB(t, ctx, db, "insert into src values (2,20)")

	execSQLDB(t, ctx, db, "begin")
	errMsg := execExpectError(t, ctx, db, "data branch pick src into base keys(2)")
	require.Contains(t, strings.ToLower(errMsg), "data branch merge/pick is not supported in transactions")
	execSQLDB(t, ctx, db, "rollback")

	require.Equal(t, []int{1}, queryIntColumn(t, ctx, db, "select a from base order by a"))
}
