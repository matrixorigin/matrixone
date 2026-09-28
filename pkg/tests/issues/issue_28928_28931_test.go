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
	"github.com/matrixorigin/matrixone/pkg/tests/testutils"
	"github.com/matrixorigin/matrixone/pkg/util/fault"
	"github.com/stretchr/testify/require"
)

const (
	issue289CreateIndexPlanCompiled = "create-index-plan-compiled"
)

func TestIssue28928ConcurrentCreateIndexNameIsUnique(t *testing.T) {
	withIssue289Faults(t)
	runAuthenticatedClusterTest(t, func(cluster embed.Cluster) {
		ctx, cancel := context.WithTimeout(context.Background(), 180*time.Second)
		defer cancel()

		db0, db1 := issue289OpenCNs(t, cluster)
		defer db0.Close()
		defer db1.Close()

		database := testutils.GetDatabaseName(t)
		execSQLMaybe(t, ctx, db0, "drop database if exists `"+database+"`")
		defer execSQLMaybe(t, context.Background(), db0, "drop database if exists `"+database+"`")
		execSQLRequire(t, ctx, db0, "create database `"+database+"`")

		for _, testCase := range []struct {
			name       string
			table      string
			secondPart string
		}{
			{name: "identical definitions", table: "same_def", secondPart: "v"},
			{name: "different definitions", table: "different_def", secondPart: "w"},
		} {
			t.Run(testCase.name, func(t *testing.T) {
				table := fmt.Sprintf("`%s`.`%s`", database, testCase.table)
				execSQLRequire(t, ctx, db0, "create table "+table+" (id int primary key, v int, w int)")
				execSQLRequire(t, ctx, db0, "insert into "+table+" values (1, 10, 100), (2, 20, 200)")
				issue289WaitForTable(t, ctx, db1, table, 2)

				waitForPlans, release := issue289InstallPlanBarrier(t, ctx, issue289CreateIndexPlanCompiled, 2)
				conn0, err := db0.Conn(ctx)
				require.NoError(t, err)
				defer conn0.Close()
				conn1, err := db1.Conn(ctx)
				require.NoError(t, err)
				defer conn1.Close()

				type ddlResult struct {
					db  *sql.DB
					err error
				}
				results := make(chan ddlResult, 2)
				go func() {
					_, err := conn0.ExecContext(ctx, "create index ix_race on "+table+" (v)")
					results <- ddlResult{db: db0, err: err}
				}()
				go func() {
					_, err := conn1.ExecContext(ctx, "create index ix_race on "+table+" ("+testCase.secondPart+")")
					results <- ddlResult{db: db1, err: err}
				}()
				waitForPlans()
				release()

				var succeeded int
				var rejected error
				var observer *sql.DB
				for range 2 {
					result := <-results
					if result.err == nil {
						succeeded++
						observer = result.db
					} else {
						rejected = result.err
					}
				}
				require.Equal(t, 1, succeeded)
				issue289RequireMySQLError(t, rejected, 1061)
				require.NotNil(t, observer)

				var indexColumns []string
				require.Eventually(t, func() bool {
					indexColumns, err = issue289ShowIndexColumns(ctx, observer, table, "ix_race")
					return err == nil && len(indexColumns) == 1
				}, 10*time.Second, 20*time.Millisecond,
					"SHOW INDEX must expose one logical definition")
				require.True(t, indexColumns[0] == "v" || indexColumns[0] == testCase.secondPart,
					"unexpected winning index column %q", indexColumns[0])

				var rows int
				require.NoError(t, observer.QueryRowContext(ctx,
					"select count(*) from "+table+" force index(ix_race) where id = 1").Scan(&rows))
				require.Equal(t, 1, rows)

				execSQLRequire(t, ctx, observer, "drop index ix_race on "+table)
				err = observer.QueryRowContext(ctx,
					"select count(*) from "+table+" force index(ix_race) where id = 1").Scan(&rows)
				issue289RequireMySQLError(t, err, 1176)
				execSQLRequire(t, ctx, observer, "create index ix_race on "+table+" (v)")
				execSQLRequire(t, ctx, observer, "drop index ix_race on "+table)
			})
		}
	})
}

func TestIssue28931StaleUpdatePlanCannotBypassNewIndex(t *testing.T) {
	withIssue289Faults(t)
	runAuthenticatedClusterTest(t, func(cluster embed.Cluster) {
		ctx, cancel := context.WithTimeout(context.Background(), 180*time.Second)
		defer cancel()

		writerDB, ddlDB := issue289OpenCNs(t, cluster)
		defer writerDB.Close()
		defer ddlDB.Close()

		database := testutils.GetDatabaseName(t)
		execSQLMaybe(t, ctx, writerDB, "drop database if exists `"+database+"`")
		defer execSQLMaybe(t, context.Background(), writerDB, "drop database if exists `"+database+"`")
		execSQLRequire(t, ctx, writerDB, "create database `"+database+"`")

		t.Run("regular index includes the update", func(t *testing.T) {
			table := "`" + database + "`.`regular_update`"
			execSQLRequire(t, ctx, writerDB, "create table "+table+" (id int primary key, payload varchar(40) not null)")
			execSQLRequire(t, ctx, writerDB, "insert into "+table+" values (1, 'before'), (2, 'control')")
			issue289WaitForTable(t, ctx, ddlDB, table, 2)

			waitForPlan, release := issue289InstallPlanBarrier(t, ctx, issue289CreateIndexPlanCompiled, 1)
			ddlDone := make(chan error, 1)
			go func() {
				_, err := ddlDB.ExecContext(ctx, "create index ix_payload on "+table+" (payload)")
				ddlDone <- err
			}()
			waitForPlan()
			execSQLRequire(t, ctx, writerDB, "update "+table+" set payload = 'changed' where id = 1")
			release()
			require.NoError(t, <-ddlDone)

			var scannedCount, scannedSum, indexedCount, indexedSum int
			require.NoError(t, writerDB.QueryRowContext(ctx,
				"select count(*), coalesce(sum(id), 0) from "+table+" ignore index(ix_payload) where payload = 'changed'").
				Scan(&scannedCount, &scannedSum))
			require.NoError(t, writerDB.QueryRowContext(ctx,
				"select count(*), coalesce(sum(id), 0) from "+table+" force index(ix_payload) where payload = 'changed'").
				Scan(&indexedCount, &indexedSum))
			require.Equal(t, 1, scannedCount)
			require.Equal(t, 1, scannedSum)
			require.Equal(t, scannedCount, indexedCount)
			require.Equal(t, scannedSum, indexedSum)
		})

		t.Run("unique index rejects the stale conflicting update", func(t *testing.T) {
			table := "`" + database + "`.`unique_update`"
			execSQLRequire(t, ctx, writerDB, "create table "+table+" (id int primary key, a int not null)")
			execSQLRequire(t, ctx, writerDB, "insert into "+table+" values (1, 1), (2, 2)")
			issue289WaitForTable(t, ctx, ddlDB, table, 2)

			waitForPlan, release := issue289InstallPlanBarrier(t, ctx, issue289CreateIndexPlanCompiled, 1)
			ddlDone := make(chan error, 1)
			go func() {
				_, err := ddlDB.ExecContext(ctx, "create unique index uk_a on "+table+" (a)")
				ddlDone <- err
			}()
			waitForPlan()
			execSQLRequire(t, ctx, writerDB, "update "+table+" set a = 1 where id = 2")
			release()
			issue289RequireMySQLError(t, <-ddlDone, 1062)

			var rows, distinctValues int
			require.NoError(t, writerDB.QueryRowContext(ctx,
				"select count(*), count(distinct a) from "+table).Scan(&rows, &distinctValues))
			require.Equal(t, 2, rows)
			require.Equal(t, 1, distinctValues)

			var published int
			require.NoError(t, writerDB.QueryRowContext(ctx,
				"select count(*) from information_schema.statistics "+
					"where table_schema = ? and table_name = 'unique_update' and index_name = 'uk_a'",
				database).Scan(&published))
			require.Zero(t, published)
		})
	})
}

func withIssue289Faults(t *testing.T) {
	t.Helper()
	enabledHere := fault.Enable()
	if enabledHere {
		t.Cleanup(func() { fault.Disable() })
	}
}

func issue289OpenCNs(t *testing.T, cluster embed.Cluster) (*sql.DB, *sql.DB) {
	t.Helper()
	cn0, err := cluster.GetCNService(0)
	require.NoError(t, err)
	cn1, err := cluster.GetCNService(1)
	require.NoError(t, err)
	db0, err := sql.Open("mysql", issue27487DSN(cn0.GetServiceConfig().CN.Frontend.Port))
	require.NoError(t, err)
	db1, err := sql.Open("mysql", issue27487DSN(cn1.GetServiceConfig().CN.Frontend.Port))
	require.NoError(t, err)
	db0.SetMaxOpenConns(4)
	db1.SetMaxOpenConns(4)
	return db0, db1
}

func issue289InstallPlanBarrier(
	t *testing.T,
	ctx context.Context,
	barrier string,
	waiters int64,
) (wait func(), release func()) {
	t.Helper()
	probe := barrier + "-issue289-waiters"
	require.NoError(t, fault.AddFaultPoint(ctx, barrier, ":::", "wait", 0, "", false))
	require.NoError(t, fault.AddFaultPoint(ctx, probe, ":::", "getwaiters", 0, barrier, false))
	released := false
	release = func() {
		if released {
			return
		}
		released = true
		_, _ = fault.RemoveFaultPoint(context.Background(), barrier)
		_, _ = fault.RemoveFaultPoint(context.Background(), probe)
	}
	t.Cleanup(release)
	wait = func() {
		require.Eventually(t, func() bool {
			count, _, ok := fault.TriggerFault(probe)
			return ok && count == waiters
		}, 30*time.Second, 10*time.Millisecond, "%s did not reach %d waiters", barrier, waiters)
	}
	return wait, release
}

func issue289WaitForTable(t *testing.T, ctx context.Context, db *sql.DB, table string, rows int) {
	t.Helper()
	require.Eventually(t, func() bool {
		var observed int
		err := db.QueryRowContext(ctx, "select count(*) from "+table).Scan(&observed)
		return err == nil && observed == rows
	}, 30*time.Second, 10*time.Millisecond, "%s did not become visible on the second CN", table)
}

func issue289RequireMySQLError(t *testing.T, err error, code uint16) {
	t.Helper()
	require.Error(t, err)
	var mysqlErr *mysql.MySQLError
	require.ErrorAs(t, err, &mysqlErr)
	require.Equal(t, code, mysqlErr.Number)
}

func issue289ShowIndexColumns(
	ctx context.Context,
	db *sql.DB,
	table string,
	indexName string,
) ([]string, error) {
	rows, err := db.QueryContext(ctx, "show index from "+table)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	columnNames, err := rows.Columns()
	if err != nil {
		return nil, err
	}
	keyName, columnName := -1, -1
	for i, name := range columnNames {
		switch strings.ToLower(name) {
		case "key_name":
			keyName = i
		case "column_name":
			columnName = i
		}
	}
	if keyName < 0 || columnName < 0 {
		return nil, fmt.Errorf("SHOW INDEX omitted key or column name")
	}

	var result []string
	for rows.Next() {
		values := make([]sql.RawBytes, len(columnNames))
		dest := make([]any, len(values))
		for i := range values {
			dest[i] = &values[i]
		}
		if err = rows.Scan(dest...); err != nil {
			return nil, err
		}
		if string(values[keyName]) == indexName {
			result = append(result, string(values[columnName]))
		}
	}
	return result, rows.Err()
}
