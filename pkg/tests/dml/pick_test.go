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
	"strconv"
	"strings"
	"testing"
	"time"

	_ "github.com/go-sql-driver/mysql"
	"github.com/stretchr/testify/require"

	"github.com/matrixorigin/matrixone/pkg/embed"
	"github.com/matrixorigin/matrixone/pkg/tests/testutils"
)

func TestDataBranchPick(t *testing.T) {
	embed.RunBaseClusterTests(
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

			t.Log("pick specific rows by PK value list")
			runPickByKeyValues(t, ctx, sqlDB)

			t.Log("pick all rows (no KEYS clause)")
			runPickAll(t, ctx, sqlDB)

			t.Log("pick with DELETE propagation")
			runPickWithDelete(t, ctx, sqlDB)

			t.Log("pick conflict SKIP")
			runPickConflictSkip(t, ctx, sqlDB)

			t.Log("pick conflict ACCEPT")
			runPickConflictAccept(t, ctx, sqlDB)

			t.Log("pick conflict FAIL")
			runPickConflictFail(t, ctx, sqlDB)

			t.Log("pick update/update conflict SKIP")
			runPickConflictSkipUpdateUpdate(t, ctx, sqlDB)

			t.Log("pick update/update conflict ACCEPT")
			runPickConflictAcceptUpdateUpdate(t, ctx, sqlDB)

			t.Log("pick update/update conflict FAIL")
			runPickConflictFailUpdateUpdate(t, ctx, sqlDB)

			t.Log("pick update/delete conflict SKIP")
			runPickConflictSkipUpdateDelete(t, ctx, sqlDB)

			t.Log("pick update/delete conflict ACCEPT")
			runPickConflictAcceptUpdateDelete(t, ctx, sqlDB)

			t.Log("pick update/delete conflict FAIL")
			runPickConflictFailUpdateDelete(t, ctx, sqlDB)

			t.Log("pick delete/update conflict SKIP")
			runPickConflictSkipDeleteUpdate(t, ctx, sqlDB)

			t.Log("pick delete/update conflict ACCEPT")
			runPickConflictAcceptDeleteUpdate(t, ctx, sqlDB)

			t.Log("pick delete/update conflict FAIL")
			runPickConflictFailDeleteUpdate(t, ctx, sqlDB)

			t.Log("pick with subquery KEYS")
			runPickSubqueryKeys(t, ctx, sqlDB)

			t.Log("pick with subquery key coercion")
			runPickSubqueryKeyCoercion(t, ctx, sqlDB)

			t.Log("pick subquery rejects NULL keys")
			runPickSubqueryRejectsNullKey(t, ctx, sqlDB)

			t.Log("pick subquery rejects invalid key coercion")
			runPickSubqueryRejectsInvalidCoercion(t, ctx, sqlDB)

			t.Log("large-scale pick (1000 rows, pick 50)")
			runPickLargeScale(t, ctx, sqlDB)

			t.Log("pick with varchar primary key")
			runPickVarcharPK(t, ctx, sqlDB)

			t.Log("pick consecutive: two picks from same source")
			runPickConsecutive(t, ctx, sqlDB)

			t.Log("pick into table with pre-existing non-overlapping data")
			runPickIntoExistingData(t, ctx, sqlDB)

			t.Log("pick with mixed INSERT + UPDATE + DELETE in source")
			runPickMixedOperations(t, ctx, sqlDB)

			t.Log("pick rejects destination snapshots")
			runPickRejectDstSnapshot(t, ctx, sqlDB)

			t.Log("pick rejects explicit transactions")
			runPickRejectExplicitTransaction(t, ctx, sqlDB)
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

// pickDB creates a unique database and returns (dbName, cleanup).
func pickDB(t *testing.T, ctx context.Context, db *sql.DB) (string, func()) {
	t.Helper()
	dbName := testutils.GetDatabaseName(t)
	execSQLDB(t, ctx, db, fmt.Sprintf("create database `%s`", dbName))
	execSQLDB(t, ctx, db, fmt.Sprintf("use `%s`", dbName))
	return dbName, func() {
		execSQLDB(t, ctx, db, "use mo_catalog")
		execSQLDB(t, ctx, db, fmt.Sprintf("drop database if exists `%s`", dbName))
	}
}

// ---------------------------------------------------------------------------
// test cases
// ---------------------------------------------------------------------------

// runPickByKeyValues: pick 2 out of 5 inserted rows by KEYS(2,4).
func runPickByKeyValues(t *testing.T, parentCtx context.Context, db *sql.DB) {
	t.Helper()
	ctx, cancel := context.WithTimeout(parentCtx, 90*time.Second)
	defer cancel()

	_, cleanup := pickDB(t, ctx, db)
	defer cleanup()

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
}

// runPickAll: pick everything (no KEYS clause) from branch into base.
func runPickAll(t *testing.T, parentCtx context.Context, db *sql.DB) {
	t.Helper()
	ctx, cancel := context.WithTimeout(parentCtx, 90*time.Second)
	defer cancel()

	_, cleanup := pickDB(t, ctx, db)
	defer cleanup()

	execSQLDB(t, ctx, db, "create table base (a int primary key, b varchar(32))")
	execSQLDB(t, ctx, db, "insert into base values (1,'one'),(2,'two')")
	execSQLDB(t, ctx, db, "data branch create table src from base")
	execSQLDB(t, ctx, db, "insert into src values (3,'three'),(4,'four'),(5,'five')")

	execSQLDB(t, ctx, db, "data branch pick src into base keys(1,2,3,4,5)")

	cnt := queryRowCount(t, ctx, db, "select count(*) from base")
	require.Equal(t, 5, cnt)

	pks := queryIntColumn(t, ctx, db, "select a from base order by a")
	require.Equal(t, []int{1, 2, 3, 4, 5}, pks)
}

// runPickWithDelete: source branch deletes rows, pick propagates deletion.
func runPickWithDelete(t *testing.T, parentCtx context.Context, db *sql.DB) {
	t.Helper()
	ctx, cancel := context.WithTimeout(parentCtx, 90*time.Second)
	defer cancel()

	_, cleanup := pickDB(t, ctx, db)
	defer cleanup()

	execSQLDB(t, ctx, db, "create table base (a int primary key, b int)")
	execSQLDB(t, ctx, db, "insert into base values (1,10),(2,20),(3,30),(4,40),(5,50)")
	execSQLDB(t, ctx, db, "data branch create table src from base")

	// Delete rows 2 and 4 in src
	execSQLDB(t, ctx, db, "delete from src where a in (2,4)")
	// Also insert a new row
	execSQLDB(t, ctx, db, "insert into src values (6,60)")

	execSQLDB(t, ctx, db, "data branch pick src into base keys(1,2,3,4,5,6)")

	pks := queryIntColumn(t, ctx, db, "select a from base order by a")
	require.Equal(t, []int{1, 3, 5, 6}, pks)
}

// runPickConflictSkip: overlapping INSERT, SKIP keeps dst value.
func runPickConflictSkip(t *testing.T, parentCtx context.Context, db *sql.DB) {
	t.Helper()
	ctx, cancel := context.WithTimeout(parentCtx, 90*time.Second)
	defer cancel()

	_, cleanup := pickDB(t, ctx, db)
	defer cleanup()

	execSQLDB(t, ctx, db, "create table base (a int primary key, b int)")
	execSQLDB(t, ctx, db, "insert into base values (1,10),(2,20)")
	execSQLDB(t, ctx, db, "data branch create table src from base")
	execSQLDB(t, ctx, db, "data branch create table dst from base")

	// Both branches insert key 3 with different values
	execSQLDB(t, ctx, db, "insert into src values (3,300)")
	execSQLDB(t, ctx, db, "insert into dst values (3,999)")

	execSQLDB(t, ctx, db, "data branch pick src into dst keys(3) when conflict skip")

	// dst should keep its own value (999) for key 3
	var b int
	require.NoError(t, db.QueryRowContext(ctx, "select b from dst where a=3").Scan(&b))
	require.Equal(t, 999, b)
}

// runPickConflictAccept: overlapping INSERT, ACCEPT takes src value.
func runPickConflictAccept(t *testing.T, parentCtx context.Context, db *sql.DB) {
	t.Helper()
	ctx, cancel := context.WithTimeout(parentCtx, 90*time.Second)
	defer cancel()

	_, cleanup := pickDB(t, ctx, db)
	defer cleanup()

	execSQLDB(t, ctx, db, "create table base (a int primary key, b int)")
	execSQLDB(t, ctx, db, "insert into base values (1,10),(2,20)")
	execSQLDB(t, ctx, db, "data branch create table src from base")
	execSQLDB(t, ctx, db, "data branch create table dst from base")

	execSQLDB(t, ctx, db, "insert into src values (3,300)")
	execSQLDB(t, ctx, db, "insert into dst values (3,999)")

	execSQLDB(t, ctx, db, "data branch pick src into dst keys(3) when conflict accept")

	// dst should accept src's value (300) for key 3
	var b int
	require.NoError(t, db.QueryRowContext(ctx, "select b from dst where a=3").Scan(&b))
	require.Equal(t, 300, b)
}

// runPickConflictFail: overlapping INSERT, FAIL raises error.
func runPickConflictFail(t *testing.T, parentCtx context.Context, db *sql.DB) {
	t.Helper()
	ctx, cancel := context.WithTimeout(parentCtx, 90*time.Second)
	defer cancel()

	_, cleanup := pickDB(t, ctx, db)
	defer cleanup()

	execSQLDB(t, ctx, db, "create table base (a int primary key, b int)")
	execSQLDB(t, ctx, db, "insert into base values (1,10),(2,20)")
	execSQLDB(t, ctx, db, "data branch create table src from base")
	execSQLDB(t, ctx, db, "data branch create table dst from base")

	execSQLDB(t, ctx, db, "insert into src values (3,300)")
	execSQLDB(t, ctx, db, "insert into dst values (3,999)")

	errMsg := execExpectError(t, ctx, db,
		"data branch pick src into dst keys(3) when conflict fail")
	require.Contains(t, strings.ToLower(errMsg), "conflict")

	// dst should still have its original value (unchanged since FAIL aborts)
	var b int
	require.NoError(t, db.QueryRowContext(ctx, "select b from dst where a=3").Scan(&b))
	require.Equal(t, 999, b)
}

func runPickConflictSkipUpdateUpdate(t *testing.T, parentCtx context.Context, db *sql.DB) {
	t.Helper()
	ctx, cancel := context.WithTimeout(parentCtx, 90*time.Second)
	defer cancel()

	_, cleanup := pickDB(t, ctx, db)
	defer cleanup()

	execSQLDB(t, ctx, db, "create table base (a int primary key, b int)")
	execSQLDB(t, ctx, db, "insert into base values (1,10),(2,20)")
	execSQLDB(t, ctx, db, "data branch create table src from base")
	execSQLDB(t, ctx, db, "data branch create table dst from base")

	execSQLDB(t, ctx, db, "update src set b=111 where a=1")
	execSQLDB(t, ctx, db, "update dst set b=999 where a=1")

	execSQLDB(t, ctx, db, "data branch pick src into dst keys(1) when conflict skip")

	var b int
	require.NoError(t, db.QueryRowContext(ctx, "select b from dst where a=1").Scan(&b))
	require.Equal(t, 999, b)
}

func runPickConflictAcceptUpdateUpdate(t *testing.T, parentCtx context.Context, db *sql.DB) {
	t.Helper()
	ctx, cancel := context.WithTimeout(parentCtx, 90*time.Second)
	defer cancel()

	_, cleanup := pickDB(t, ctx, db)
	defer cleanup()

	execSQLDB(t, ctx, db, "create table base (a int primary key, b int)")
	execSQLDB(t, ctx, db, "insert into base values (1,10),(2,20)")
	execSQLDB(t, ctx, db, "data branch create table src from base")
	execSQLDB(t, ctx, db, "data branch create table dst from base")

	execSQLDB(t, ctx, db, "update src set b=111 where a=1")
	execSQLDB(t, ctx, db, "update dst set b=999 where a=1")

	execSQLDB(t, ctx, db, "data branch pick src into dst keys(1) when conflict accept")

	var b int
	require.NoError(t, db.QueryRowContext(ctx, "select b from dst where a=1").Scan(&b))
	require.Equal(t, 111, b)
}

func runPickConflictFailUpdateUpdate(t *testing.T, parentCtx context.Context, db *sql.DB) {
	t.Helper()
	ctx, cancel := context.WithTimeout(parentCtx, 90*time.Second)
	defer cancel()

	_, cleanup := pickDB(t, ctx, db)
	defer cleanup()

	execSQLDB(t, ctx, db, "create table base (a int primary key, b int)")
	execSQLDB(t, ctx, db, "insert into base values (1,10),(2,20)")
	execSQLDB(t, ctx, db, "data branch create table src from base")
	execSQLDB(t, ctx, db, "data branch create table dst from base")

	execSQLDB(t, ctx, db, "update src set b=111 where a=1")
	execSQLDB(t, ctx, db, "update dst set b=999 where a=1")

	errMsg := execExpectError(t, ctx, db,
		"data branch pick src into dst keys(1) when conflict fail")
	require.Contains(t, strings.ToLower(errMsg), "conflict")

	var b int
	require.NoError(t, db.QueryRowContext(ctx, "select b from dst where a=1").Scan(&b))
	require.Equal(t, 999, b)
}

// runPickConflictSkipUpdateDelete: src updates a row while dst deletes it.
func runPickConflictSkipUpdateDelete(t *testing.T, parentCtx context.Context, db *sql.DB) {
	t.Helper()
	ctx, cancel := context.WithTimeout(parentCtx, 90*time.Second)
	defer cancel()

	_, cleanup := pickDB(t, ctx, db)
	defer cleanup()

	execSQLDB(t, ctx, db, "create table base (a int primary key, b int)")
	execSQLDB(t, ctx, db, "insert into base values (1,10),(2,20)")
	execSQLDB(t, ctx, db, "data branch create table src from base")
	execSQLDB(t, ctx, db, "data branch create table dst from base")

	execSQLDB(t, ctx, db, "update src set b=111 where a=1")
	execSQLDB(t, ctx, db, "delete from dst where a=1")

	execSQLDB(t, ctx, db, "data branch pick src into dst keys(1) when conflict skip")

	require.Equal(t, 0, queryRowCount(t, ctx, db, "select count(*) from dst where a=1"))
}

func runPickConflictAcceptUpdateDelete(t *testing.T, parentCtx context.Context, db *sql.DB) {
	t.Helper()
	ctx, cancel := context.WithTimeout(parentCtx, 90*time.Second)
	defer cancel()

	_, cleanup := pickDB(t, ctx, db)
	defer cleanup()

	execSQLDB(t, ctx, db, "create table base (a int primary key, b int)")
	execSQLDB(t, ctx, db, "insert into base values (1,10),(2,20)")
	execSQLDB(t, ctx, db, "data branch create table src from base")
	execSQLDB(t, ctx, db, "data branch create table dst from base")

	execSQLDB(t, ctx, db, "update src set b=111 where a=1")
	execSQLDB(t, ctx, db, "delete from dst where a=1")

	execSQLDB(t, ctx, db, "data branch pick src into dst keys(1) when conflict accept")

	require.Equal(t, 1, queryRowCount(t, ctx, db, "select count(*) from dst where a=1 and b=111"))
}

// runPickConflictFailUpdateDelete: update/delete conflicts must not degrade to ACCEPT.
func runPickConflictFailUpdateDelete(t *testing.T, parentCtx context.Context, db *sql.DB) {
	t.Helper()
	ctx, cancel := context.WithTimeout(parentCtx, 90*time.Second)
	defer cancel()

	_, cleanup := pickDB(t, ctx, db)
	defer cleanup()

	execSQLDB(t, ctx, db, "create table base (a int primary key, b int)")
	execSQLDB(t, ctx, db, "insert into base values (1,10),(2,20)")
	execSQLDB(t, ctx, db, "data branch create table src from base")
	execSQLDB(t, ctx, db, "data branch create table dst from base")

	execSQLDB(t, ctx, db, "update src set b=111 where a=1")
	execSQLDB(t, ctx, db, "delete from dst where a=1")

	errMsg := execExpectError(t, ctx, db,
		"data branch pick src into dst keys(1) when conflict fail")
	require.Contains(t, strings.ToLower(errMsg), "conflict")
	require.Equal(t, 0, queryRowCount(t, ctx, db, "select count(*) from dst where a=1"))
}

func runPickConflictSkipDeleteUpdate(t *testing.T, parentCtx context.Context, db *sql.DB) {
	t.Helper()
	ctx, cancel := context.WithTimeout(parentCtx, 90*time.Second)
	defer cancel()

	_, cleanup := pickDB(t, ctx, db)
	defer cleanup()

	execSQLDB(t, ctx, db, "create table base (a int primary key, b int)")
	execSQLDB(t, ctx, db, "insert into base values (1,10),(2,20)")
	execSQLDB(t, ctx, db, "data branch create table src from base")
	execSQLDB(t, ctx, db, "data branch create table dst from base")

	execSQLDB(t, ctx, db, "delete from src where a=1")
	execSQLDB(t, ctx, db, "update dst set b=999 where a=1")

	execSQLDB(t, ctx, db, "data branch pick src into dst keys(1) when conflict skip")

	var b int
	require.NoError(t, db.QueryRowContext(ctx, "select b from dst where a=1").Scan(&b))
	require.Equal(t, 999, b)
}

func runPickConflictAcceptDeleteUpdate(t *testing.T, parentCtx context.Context, db *sql.DB) {
	t.Helper()
	ctx, cancel := context.WithTimeout(parentCtx, 90*time.Second)
	defer cancel()

	_, cleanup := pickDB(t, ctx, db)
	defer cleanup()

	execSQLDB(t, ctx, db, "create table base (a int primary key, b int)")
	execSQLDB(t, ctx, db, "insert into base values (1,10),(2,20)")
	execSQLDB(t, ctx, db, "data branch create table src from base")
	execSQLDB(t, ctx, db, "data branch create table dst from base")

	execSQLDB(t, ctx, db, "delete from src where a=1")
	execSQLDB(t, ctx, db, "update dst set b=999 where a=1")

	execSQLDB(t, ctx, db, "data branch pick src into dst keys(1) when conflict accept")

	require.Equal(t, 0, queryRowCount(t, ctx, db, "select count(*) from dst where a=1"))
}

func runPickConflictFailDeleteUpdate(t *testing.T, parentCtx context.Context, db *sql.DB) {
	t.Helper()
	ctx, cancel := context.WithTimeout(parentCtx, 90*time.Second)
	defer cancel()

	_, cleanup := pickDB(t, ctx, db)
	defer cleanup()

	execSQLDB(t, ctx, db, "create table base (a int primary key, b int)")
	execSQLDB(t, ctx, db, "insert into base values (1,10),(2,20)")
	execSQLDB(t, ctx, db, "data branch create table src from base")
	execSQLDB(t, ctx, db, "data branch create table dst from base")

	execSQLDB(t, ctx, db, "delete from src where a=1")
	execSQLDB(t, ctx, db, "update dst set b=999 where a=1")

	errMsg := execExpectError(t, ctx, db,
		"data branch pick src into dst keys(1) when conflict fail")
	require.Contains(t, strings.ToLower(errMsg), "conflict")

	var b int
	require.NoError(t, db.QueryRowContext(ctx, "select b from dst where a=1").Scan(&b))
	require.Equal(t, 999, b)
}

// runPickSubqueryKeys: use a SELECT subquery to specify which PKs to pick.
func runPickSubqueryKeys(t *testing.T, parentCtx context.Context, db *sql.DB) {
	t.Helper()
	ctx, cancel := context.WithTimeout(parentCtx, 90*time.Second)
	defer cancel()

	_, cleanup := pickDB(t, ctx, db)
	defer cleanup()

	execSQLDB(t, ctx, db, "create table base (a int primary key, b int)")
	execSQLDB(t, ctx, db, "insert into base values (1,10),(2,20),(3,30)")
	execSQLDB(t, ctx, db, "data branch create table src from base")
	for i := 4; i <= 20; i++ {
		execSQLDB(t, ctx, db, fmt.Sprintf("insert into src values (%d,%d)", i, i*10))
	}

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

	_, cleanup := pickDB(t, ctx, db)
	defer cleanup()

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

	_, cleanup := pickDB(t, ctx, db)
	defer cleanup()

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

	_, cleanup := pickDB(t, ctx, db)
	defer cleanup()

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

// runPickLargeScale: 1000-row table, branch inserts 500 more, pick 50 specific.
func runPickLargeScale(t *testing.T, parentCtx context.Context, db *sql.DB) {
	t.Helper()
	ctx, cancel := context.WithTimeout(parentCtx, 120*time.Second)
	defer cancel()

	_, cleanup := pickDB(t, ctx, db)
	defer cleanup()

	execSQLDB(t, ctx, db, "create table base (a int primary key, b varchar(64))")

	// Insert 1000 seed rows
	execSQLDB(t, ctx, db,
		"insert into base select result, concat('seed_', cast(result as char)) from generate_series(1,1000) g")

	execSQLDB(t, ctx, db, "data branch create table src from base")

	// Insert 500 new rows in src (1001..1500)
	execSQLDB(t, ctx, db,
		"insert into src select result, concat('new_', cast(result as char)) from generate_series(1001,1500) g")

	// Pick 50 specific new rows: 1001,1011,1021,...,1491
	keyList := make([]string, 50)
	for i := 0; i < 50; i++ {
		keyList[i] = strconv.Itoa(1001 + i*10)
	}
	keysCSV := strings.Join(keyList, ",")

	execSQLDB(t, ctx, db, fmt.Sprintf("data branch pick src into base keys(%s)", keysCSV))

	cnt := queryRowCount(t, ctx, db, "select count(*) from base")
	require.Equal(t, 1050, cnt) // 1000 seed + 50 picked

	// Spot-check: key 1001 should exist with value 'new_1001'
	var b string
	require.NoError(t, db.QueryRowContext(ctx, "select b from base where a=1001").Scan(&b))
	require.Equal(t, "new_1001", b)

	// Key 1002 should NOT exist (we didn't pick it)
	cnt = queryRowCount(t, ctx, db, "select count(*) from base where a=1002")
	require.Equal(t, 0, cnt)
}

// runPickVarcharPK: pick with non-integer PK.
func runPickVarcharPK(t *testing.T, parentCtx context.Context, db *sql.DB) {
	t.Helper()
	ctx, cancel := context.WithTimeout(parentCtx, 90*time.Second)
	defer cancel()

	_, cleanup := pickDB(t, ctx, db)
	defer cleanup()

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

// runPickConsecutive: two consecutive picks from the same source.
func runPickConsecutive(t *testing.T, parentCtx context.Context, db *sql.DB) {
	t.Helper()
	ctx, cancel := context.WithTimeout(parentCtx, 90*time.Second)
	defer cancel()

	_, cleanup := pickDB(t, ctx, db)
	defer cleanup()

	execSQLDB(t, ctx, db, "create table base (a int primary key, b int)")
	execSQLDB(t, ctx, db, "insert into base values (1,10)")
	execSQLDB(t, ctx, db, "data branch create table src from base")
	execSQLDB(t, ctx, db, "insert into src values (2,20),(3,30),(4,40),(5,50)")

	// First pick: keys 2,3
	execSQLDB(t, ctx, db, "data branch pick src into base keys(2,3)")
	pks := queryIntColumn(t, ctx, db, "select a from base order by a")
	require.Equal(t, []int{1, 2, 3}, pks)

	// Second pick: keys 4,5
	execSQLDB(t, ctx, db, "data branch pick src into base keys(4,5)")
	pks = queryIntColumn(t, ctx, db, "select a from base order by a")
	require.Equal(t, []int{1, 2, 3, 4, 5}, pks)
}

// runPickIntoExistingData: dst already has rows that don't overlap with src.
func runPickIntoExistingData(t *testing.T, parentCtx context.Context, db *sql.DB) {
	t.Helper()
	ctx, cancel := context.WithTimeout(parentCtx, 90*time.Second)
	defer cancel()

	_, cleanup := pickDB(t, ctx, db)
	defer cleanup()

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

	_, cleanup := pickDB(t, ctx, db)
	defer cleanup()

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

	_, cleanup := pickDB(t, ctx, db)
	defer cleanup()

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

	_, cleanup := pickDB(t, ctx, db)
	defer cleanup()

	execSQLDB(t, ctx, db, "create table base (a int primary key, b int)")
	execSQLDB(t, ctx, db, "insert into base values (1,10)")
	execSQLDB(t, ctx, db, "data branch create table src from base")
	execSQLDB(t, ctx, db, "insert into src values (2,20)")

	execSQLDB(t, ctx, db, "begin")
	errMsg := execExpectError(t, ctx, db, "data branch pick src into base keys(2)")
	require.Contains(t, strings.ToLower(errMsg), "explicit transactions")
	execSQLDB(t, ctx, db, "rollback")

	require.Equal(t, []int{1}, queryIntColumn(t, ctx, db, "select a from base order by a"))
}
