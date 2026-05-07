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
	"testing"
	"time"

	_ "github.com/go-sql-driver/mysql"
	"github.com/stretchr/testify/require"

	"github.com/matrixorigin/matrixone/pkg/embed"
)

func openRetestSQLDB(t *testing.T, c embed.Cluster) *sql.DB {
	t.Helper()

	cn1, err := c.GetCNService(0)
	require.NoError(t, err)

	port := cn1.GetServiceConfig().CN.Frontend.Port
	dsn := fmt.Sprintf("dump:111@tcp(127.0.0.1:%d)/", port)
	db, err := sql.Open("mysql", dsn)
	require.NoError(t, err)
	return db
}

func TestDataBranchPickRetestCompositePKChunkProbe(t *testing.T) {
	embed.RunBaseClusterTests(func(c embed.Cluster) {
		ctx, cancel := context.WithTimeout(context.Background(), 2*time.Hour)
		defer cancel()

		db := openRetestSQLDB(t, c)
		defer db.Close()
		execSQLDB(t, ctx, db, "set role moadmin")

		_, cleanup := pickDB(t, ctx, db)
		defer cleanup()

		execSQLDB(t, ctx, db, "create table base (k1 int, k2 int, val bigint, primary key (k1, k2))")
		execSQLDB(t, ctx, db,
			"insert into base select 1, result, result * 10 from generate_series(1,1000000) g")

		execSQLDB(t, ctx, db, "data branch create table src from base")
		execSQLDB(t, ctx, db, "data branch create table dst from base")
		execSQLDB(t, ctx, db, "update src set val = val + 1 where k2 between 1 and 8192")

		execSQLDB(t, ctx, db, "create table pick_keys (k1 int, k2 int)")
		execSQLDB(t, ctx, db,
			"insert into pick_keys select 1, result from generate_series(10000,18191) g")
		execSQLDB(t, ctx, db,
			"insert into pick_keys select 1, result from generate_series(1,8192) g")

		firstChunk := queryStringRows(t, ctx, db,
			"select min(k2), max(k2), count(*) from (select k1, k2 from pick_keys order by 1, 2 limit 8192) t")
		secondChunk := queryStringRows(t, ctx, db,
			"select min(k2), max(k2), count(*) from (select k1, k2 from pick_keys order by 1, 2 limit 8192 offset 8192) t")
		t.Logf("composite chunk layout after fix: first=%v second=%v", firstChunk, secondChunk)

		execSQLDB(t, ctx, db, "data branch pick src into dst keys(select k1, k2 from pick_keys)")

		updatedCnt := queryRowCount(t, ctx, db,
			"select count(*) from dst where k1 = 1 and k2 between 1 and 8192 and val = k2 * 10 + 1")
		unchangedCnt := queryRowCount(t, ctx, db,
			"select count(*) from dst where k1 = 1 and k2 between 1 and 8192 and val = k2 * 10")
		totalCnt := queryRowCount(t, ctx, db,
			"select count(*) from dst where k1 = 1 and k2 between 1 and 8192")
		t.Logf("composite retest counts: updated=%d unchanged=%d total=%d", updatedCnt, unchangedCnt, totalCnt)

		require.Equal(t, 8192, totalCnt)
		require.Equal(t, 8192, updatedCnt)
		require.Equal(t, 0, unchangedCnt)
	})
}

func TestDataBranchPickRetestNoPKSubqueryFakePK(t *testing.T) {
	embed.RunBaseClusterTests(func(c embed.Cluster) {
		ctx, cancel := context.WithTimeout(context.Background(), 10*time.Minute)
		defer cancel()

		db := openRetestSQLDB(t, c)
		defer db.Close()
		execSQLDB(t, ctx, db, "set role moadmin")

		_, cleanup := pickDB(t, ctx, db)
		defer cleanup()

		execSQLDB(t, ctx, db, "create table base (id int, payload varchar(32))")
		execSQLDB(t, ctx, db, "insert into base values (1, 'one'), (2, 'two'), (3, 'three')")
		execSQLDB(t, ctx, db, "data branch create table src from base")
		execSQLDB(t, ctx, db, "data branch create table dst from base")

		execSQLDB(t, ctx, db, "create table pick_keys (fpk bigint unsigned)")
		execSQLDB(t, ctx, db, "insert into pick_keys select `__mo_fake_pk_col` from src where id in (1, 2) order by id")

		execSQLDB(t, ctx, db, "update src set payload = 'ONE' where id = 1")
		execSQLDB(t, ctx, db, "delete from src where id = 2")
		execSQLDB(t, ctx, db, "insert into src values (4, 'four')")
		execSQLDB(t, ctx, db, "insert into pick_keys select `__mo_fake_pk_col` from src where id = 4")

		errMsg := execExpectError(t, ctx, db,
			"data branch pick src into dst keys(select fpk from pick_keys order by fpk)")
		t.Logf("no-pk retest error: %s", errMsg)
		require.Contains(t, errMsg, "requires a table with a primary key")
	})
}

func TestDataBranchPickRetestDatePKLiteral(t *testing.T) {
	embed.RunBaseClusterTests(func(c embed.Cluster) {
		ctx, cancel := context.WithTimeout(context.Background(), 5*time.Minute)
		defer cancel()

		db := openRetestSQLDB(t, c)
		defer db.Close()
		execSQLDB(t, ctx, db, "set role moadmin")

		_, cleanup := pickDB(t, ctx, db)
		defer cleanup()

		execSQLDB(t, ctx, db, "create table base (d date primary key, v int)")
		execSQLDB(t, ctx, db, "insert into base values ('2025-01-01', 10), ('2025-01-02', 20)")
		execSQLDB(t, ctx, db, "data branch create table src from base")
		execSQLDB(t, ctx, db, "data branch create table dst from base")
		execSQLDB(t, ctx, db, "insert into src values ('2025-01-03', 30)")

		execSQLDB(t, ctx, db, "data branch pick src into dst keys('2025-01-03')")

		rows := queryStringRows(t, ctx, db, "select cast(d as char), cast(v as char) from dst where d = '2025-01-03'")
		t.Logf("date-pk retest rows: %v", rows)
		require.Equal(t, [][]string{{"2025-01-03", "30"}}, rows)
	})
}
