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
	require.NoError(t, embed.RunBaseClusterTests(func(c embed.Cluster) {
		ctx, cancel := context.WithTimeout(context.Background(), 3*time.Minute)
		defer cancel()

		cn, err := c.GetCNService(0)
		require.NoError(t, err)
		port := cn.GetServiceConfig().CN.Frontend.Port
		db, err := sql.Open("mysql", fmt.Sprintf("dump:111@tcp(127.0.0.1:%d)/", port))
		require.NoError(t, err)
		defer db.Close()
		db.SetMaxOpenConns(8)

		base := testutils.GetDatabaseName(t)
		for round := 0; round < 3; round++ {
			t.Run(fmt.Sprintf("plain_drop_table_round_%d", round), func(t *testing.T) {
				dbName := fmt.Sprintf("%s_plain_%d", base, round)
				defer execSQLMaybe(t, ctx, db, fmt.Sprintf("drop database if exists `%s`", dbName))
				createSiblingBranches(t, ctx, db, dbName)

				statements := make([]string, 4)
				for i := range statements {
					statements[i] = fmt.Sprintf("drop table `%s`.`b%d`", dbName, i)
				}
				runConcurrentStatements(t, ctx, db, statements)
				require.Equal(t, 0, countIssue26095Tables(t, ctx, db, dbName))
			})

			t.Run(fmt.Sprintf("branch_delete_table_round_%d", round), func(t *testing.T) {
				dbName := fmt.Sprintf("%s_branch_%d", base, round)
				defer execSQLMaybe(t, ctx, db, fmt.Sprintf("drop database if exists `%s`", dbName))
				createSiblingBranches(t, ctx, db, dbName)

				statements := make([]string, 4)
				for i := range statements {
					statements[i] = fmt.Sprintf("data branch delete table `%s`.`b%d`", dbName, i)
				}
				runConcurrentStatements(t, ctx, db, statements)
				require.Equal(t, 0, countIssue26095Tables(t, ctx, db, dbName))
			})

			t.Run(fmt.Sprintf("branch_delete_database_round_%d", round), func(t *testing.T) {
				source := fmt.Sprintf("%s_source_%d", base, round)
				left := fmt.Sprintf("%s_left_%d", base, round)
				right := fmt.Sprintf("%s_right_%d", base, round)
				for _, name := range []string{left, right, source} {
					defer execSQLMaybe(t, ctx, db, fmt.Sprintf("drop database if exists `%s`", name))
				}
				execSQLRequire(t, ctx, db, fmt.Sprintf("create database `%s`", source))
				execSQLRequire(t, ctx, db, fmt.Sprintf("create table `%s`.`t1` (id int primary key)", source))
				execSQLRequire(t, ctx, db, fmt.Sprintf("create table `%s`.`t2` (id bigint primary key)", source))
				execSQLRequire(t, ctx, db, fmt.Sprintf("data branch create database `%s` from `%s`", left, source))
				execSQLRequire(t, ctx, db, fmt.Sprintf("data branch create database `%s` from `%s`", right, source))

				runConcurrentStatements(t, ctx, db, []string{
					fmt.Sprintf("data branch delete database `%s`", left),
					fmt.Sprintf("data branch delete database `%s`", right),
				})
				require.Equal(t, 0, countIssue26095Databases(t, ctx, db, left, right))
			})
		}
	}))
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
