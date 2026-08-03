// Copyright 2021 - 2026 Matrix Origin
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

package issues

import (
	"context"
	"database/sql"
	"fmt"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/matrixorigin/matrixone/pkg/embed"
	"github.com/matrixorigin/matrixone/pkg/tests/testutils"
	"github.com/stretchr/testify/require"
)

func TestIssue26095ConcurrentDataBranchDeletion(t *testing.T) {
	runAuthenticatedClusterTest(t, func(c embed.Cluster) {
		runIssue26095ConcurrentDataBranchDeletion(t, c)
	})
}

func runIssue26095ConcurrentDataBranchDeletion(t *testing.T, c embed.Cluster) {
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Minute)
	defer cancel()

	cn, err := c.GetCNService(0)
	require.NoError(t, err)
	port := cn.GetServiceConfig().CN.Frontend.Port
	rootDB := openIssue26095DB(t, ctx, fmt.Sprintf("sys#root#moadmin:111@tcp(127.0.0.1:%d)/", port))
	defer rootDB.Close()

	tenantName := fmt.Sprintf("issue26095_%d", time.Now().UnixNano())
	execSQLRequire(t, ctx, rootDB, fmt.Sprintf(
		"create account `%s` admin_name 'admin' identified by '111'", tenantName,
	))
	defer execSQLMaybe(t, ctx, rootDB, fmt.Sprintf("drop account if exists `%s`", tenantName))
	tenantID := queryIssue26095AccountID(t, ctx, rootDB, tenantName)
	require.NotZero(t, tenantID)
	tenantDB := openIssue26095DB(t, ctx, fmt.Sprintf(
		"%s#admin#accountadmin:111@tcp(127.0.0.1:%d)/", tenantName, port,
	))
	defer tenantDB.Close()
	require.Equal(t, tenantID, queryIssue26095CurrentAccountID(t, ctx, tenantDB))

	accounts := []struct {
		name       string
		id         uint32
		db         *sql.DB
		roundCount int
	}{
		{name: "sys", id: 0, db: rootDB, roundCount: 3},
		{name: "tenant", id: tenantID, db: tenantDB, roundCount: 1},
	}
	base := strings.ToLower(testutils.GetDatabaseName(t))
	for _, account := range accounts {
		t.Run(account.name, func(t *testing.T) {
			for round := 0; round < account.roundCount; round++ {
				t.Run(fmt.Sprintf("plain_drop_table_round_%d", round), func(t *testing.T) {
					dbName := fmt.Sprintf("%s_%s_plain_%d", base, account.name, round)
					defer execSQLMaybe(t, ctx, account.db, fmt.Sprintf("drop database if exists `%s`", dbName))
					createSiblingBranches(t, ctx, account.db, dbName)
					tableIDs := queryIssue26095TableIDs(t, ctx, rootDB, account.id, dbName, "b%")
					requireIssue26095ReclaimPending(t, ctx, rootDB, tableIDs)

					statements := make([]string, 4)
					for i := range statements {
						statements[i] = fmt.Sprintf("drop table `%s`.`b%d`", dbName, i)
					}
					runConcurrentStatements(t, ctx, account.db, statements)
					require.Equal(t, 0, countIssue26095Tables(t, ctx, account.db, dbName))
					requireIssue26095Reclaimed(t, ctx, rootDB, tableIDs)
				})

				t.Run(fmt.Sprintf("branch_delete_table_round_%d", round), func(t *testing.T) {
					dbName := fmt.Sprintf("%s_%s_branch_%d", base, account.name, round)
					defer execSQLMaybe(t, ctx, account.db, fmt.Sprintf("drop database if exists `%s`", dbName))
					createSiblingBranches(t, ctx, account.db, dbName)
					tableIDs := queryIssue26095TableIDs(t, ctx, rootDB, account.id, dbName, "b%")
					requireIssue26095ReclaimPending(t, ctx, rootDB, tableIDs)

					statements := make([]string, 4)
					for i := range statements {
						statements[i] = fmt.Sprintf("data branch delete table `%s`.`b%d`", dbName, i)
					}
					runConcurrentStatements(t, ctx, account.db, statements)
					require.Equal(t, 0, countIssue26095Tables(t, ctx, account.db, dbName))
					requireIssue26095Reclaimed(t, ctx, rootDB, tableIDs)
				})

				t.Run(fmt.Sprintf("branch_delete_database_round_%d", round), func(t *testing.T) {
					source := fmt.Sprintf("%s_%s_source_%d", base, account.name, round)
					left := fmt.Sprintf("%s_%s_left_%d", base, account.name, round)
					right := fmt.Sprintf("%s_%s_right_%d", base, account.name, round)
					for _, name := range []string{left, right, source} {
						defer execSQLMaybe(t, ctx, account.db, fmt.Sprintf("drop database if exists `%s`", name))
					}
					execSQLRequire(t, ctx, account.db, fmt.Sprintf("create database `%s`", source))
					execSQLRequire(t, ctx, account.db, fmt.Sprintf("create table `%s`.`t1` (id int primary key)", source))
					execSQLRequire(t, ctx, account.db, fmt.Sprintf("create table `%s`.`t2` (id bigint primary key)", source))
					execSQLRequire(t, ctx, account.db, fmt.Sprintf("data branch create database `%s` from `%s`", left, source))
					execSQLRequire(t, ctx, account.db, fmt.Sprintf("data branch create database `%s` from `%s`", right, source))
					tableIDs := append(
						queryIssue26095TableIDs(t, ctx, rootDB, account.id, left, "t%"),
						queryIssue26095TableIDs(t, ctx, rootDB, account.id, right, "t%")...,
					)
					requireIssue26095ReclaimPending(t, ctx, rootDB, tableIDs)

					runConcurrentStatements(t, ctx, account.db, []string{
						fmt.Sprintf("data branch delete database `%s`", left),
						fmt.Sprintf("data branch delete database `%s`", right),
					})
					require.Equal(t, 0, countIssue26095Databases(t, ctx, account.db, left, right))
					requireIssue26095Reclaimed(t, ctx, rootDB, tableIDs)
				})
			}
		})
	}
}

func openIssue26095DB(t *testing.T, ctx context.Context, dsn string) *sql.DB {
	t.Helper()
	db, err := sql.Open("mysql", dsn)
	require.NoError(t, err)
	db.SetMaxOpenConns(8)
	err = db.PingContext(ctx)
	if err != nil {
		_ = db.Close()
	}
	require.NoError(t, err)
	return db
}

func queryIssue26095AccountID(t *testing.T, ctx context.Context, db *sql.DB, accountName string) uint32 {
	t.Helper()
	var accountID uint32
	require.NoError(t, db.QueryRowContext(ctx,
		"select account_id from mo_catalog.mo_account where account_name = ?", accountName,
	).Scan(&accountID))
	return accountID
}

func queryIssue26095CurrentAccountID(t *testing.T, ctx context.Context, db *sql.DB) uint32 {
	t.Helper()
	var accountID uint32
	require.NoError(t, db.QueryRowContext(ctx, "select current_account_id()").Scan(&accountID))
	return accountID
}

func createSiblingBranches(t *testing.T, ctx context.Context, db *sql.DB, dbName string) {
	t.Helper()
	execSQLRequire(t, ctx, db, fmt.Sprintf("create database `%s`", dbName))
	execSQLRequire(t, ctx, db, fmt.Sprintf("create table `%s`.`root_t` (id int primary key)", dbName))
	for i := 0; i < 4; i++ {
		execSQLRequire(t, ctx, db, fmt.Sprintf(
			"data branch create table `%s`.`b%d` from `%s`.`root_t`",
			dbName, i, dbName,
		))
	}
}

func queryIssue26095TableIDs(
	t *testing.T,
	ctx context.Context,
	db *sql.DB,
	accountID uint32,
	dbName string,
	tablePattern string,
) []uint64 {
	t.Helper()
	rows, err := db.QueryContext(ctx,
		"select rel_id from mo_catalog.mo_tables "+
			"where account_id = ? and reldatabase = ? and relname like ? order by rel_id",
		accountID, dbName, tablePattern,
	)
	require.NoError(t, err)
	defer rows.Close()

	var tableIDs []uint64
	for rows.Next() {
		var tableID uint64
		require.NoError(t, rows.Scan(&tableID))
		tableIDs = append(tableIDs, tableID)
	}
	require.NoError(t, rows.Err())
	require.NotEmpty(t, tableIDs)
	return tableIDs
}

func requireIssue26095ReclaimPending(t *testing.T, ctx context.Context, db *sql.DB, tableIDs []uint64) {
	t.Helper()
	idList, snapshotNames := issue26095ReclaimKeys(tableIDs)
	require.Equal(t, len(tableIDs), queryIssue26095Count(t, ctx, db, fmt.Sprintf(
		"select count(*) from mo_catalog.mo_branch_metadata "+
			"where table_id in (%s) and table_deleted = false", idList,
	)))
	require.Equal(t, len(tableIDs), queryIssue26095Count(t, ctx, db, fmt.Sprintf(
		"select count(*) from mo_catalog.mo_snapshots "+
			"where kind = 'branch' and sname in (%s)", snapshotNames,
	)))
}

func requireIssue26095Reclaimed(t *testing.T, ctx context.Context, db *sql.DB, tableIDs []uint64) {
	t.Helper()
	idList, snapshotNames := issue26095ReclaimKeys(tableIDs)
	require.Equal(t, len(tableIDs), queryIssue26095Count(t, ctx, db, fmt.Sprintf(
		"select count(*) from mo_catalog.mo_branch_metadata "+
			"where table_id in (%s) and table_deleted = true", idList,
	)))
	require.Zero(t, queryIssue26095Count(t, ctx, db, fmt.Sprintf(
		"select count(*) from mo_catalog.mo_snapshots "+
			"where kind = 'branch' and sname in (%s)", snapshotNames,
	)))
}

func issue26095ReclaimKeys(tableIDs []uint64) (string, string) {
	ids := make([]string, len(tableIDs))
	snapshotNames := make([]string, len(tableIDs))
	for i, tableID := range tableIDs {
		ids[i] = fmt.Sprintf("%d", tableID)
		snapshotNames[i] = fmt.Sprintf("'__mo_branch_%d'", tableID)
	}
	return strings.Join(ids, ","), strings.Join(snapshotNames, ",")
}

func queryIssue26095Count(t *testing.T, ctx context.Context, db *sql.DB, query string) int {
	t.Helper()
	var count int
	require.NoError(t, db.QueryRowContext(ctx, query).Scan(&count))
	return count
}

func runConcurrentStatements(t *testing.T, ctx context.Context, db *sql.DB, statements []string) {
	t.Helper()
	connections := make([]*sql.Conn, len(statements))
	for i := range connections {
		conn, err := db.Conn(ctx)
		require.NoError(t, err)
		connections[i] = conn
		defer conn.Close()
	}

	start := make(chan struct{})
	errs := make(chan error, len(statements))
	var wg sync.WaitGroup
	wg.Add(len(statements))
	for i, statement := range statements {
		go func(conn *sql.Conn, sqlText string) {
			defer wg.Done()
			<-start
			_, err := conn.ExecContext(ctx, sqlText)
			errs <- err
		}(connections[i], statement)
	}
	close(start)
	wg.Wait()
	close(errs)
	for err := range errs {
		require.NoError(t, err)
	}
}

func countIssue26095Tables(t *testing.T, ctx context.Context, db *sql.DB, dbName string) int {
	t.Helper()
	var count int
	err := db.QueryRowContext(ctx,
		"select count(*) from information_schema.tables where table_schema = ? and table_name like 'b%'",
		dbName,
	).Scan(&count)
	require.NoError(t, err)
	return count
}

func countIssue26095Databases(t *testing.T, ctx context.Context, db *sql.DB, names ...string) int {
	t.Helper()
	var count int
	err := db.QueryRowContext(ctx, fmt.Sprintf(
		"select count(*) from information_schema.schemata where schema_name in ('%s')",
		strings.Join(names, "','"),
	)).Scan(&count)
	require.NoError(t, err)
	return count
}
