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
	"bytes"
	"context"
	"database/sql"
	"fmt"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/go-sql-driver/mysql"
	"github.com/matrixorigin/matrixone/pkg/catalog"
	moruntime "github.com/matrixorigin/matrixone/pkg/common/runtime"
	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/embed"
	"github.com/matrixorigin/matrixone/pkg/lockservice"
	pblock "github.com/matrixorigin/matrixone/pkg/pb/lock"
	"github.com/matrixorigin/matrixone/pkg/pb/metadata"
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
				var id0, id1 uint32
				require.NoError(t, conn0.QueryRowContext(ctx, "select connection_id()").Scan(&id0))
				require.NoError(t, conn1.QueryRowContext(ctx, "select connection_id()").Scan(&id1))

				type ddlResult struct {
					db  *sql.DB
					err error
				}
				results := make(chan ddlResult, 2)
				ddlCtx, cancelDDL := context.WithCancel(context.Background())
				remaining := 2
				defer func() {
					defer cancelDDL()
					if remaining > 0 {
						cleanup, stop := context.WithTimeout(context.Background(), 10*time.Second)
						defer stop()
						_, _ = db0.ExecContext(cleanup, fmt.Sprintf("kill query %d", id0))
						_, _ = db1.ExecContext(cleanup, fmt.Sprintf("kill query %d", id1))
						release()
						for remaining > 0 {
							select {
							case <-results:
								remaining--
							case <-cleanup.Done():
								t.Error("concurrent CREATE INDEX did not exit during cleanup")
								return
							}
						}
					}
					release()
				}()
				go func() {
					_, err := conn0.ExecContext(ddlCtx, "create index ix_race on "+table+" (v)")
					results <- ddlResult{db: db0, err: err}
				}()
				go func() {
					_, err := conn1.ExecContext(ddlCtx, "create index ix_race on "+table+" ("+testCase.secondPart+")")
					results <- ddlResult{db: db1, err: err}
				}()
				waitForPlans()
				release()

				var succeeded int
				var rejected error
				var observer *sql.DB
				for range 2 {
					var result ddlResult
					select {
					case result = <-results:
					case <-ctx.Done():
						t.Fatal(ctx.Err())
					}
					remaining--
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

			waitForPlan, finishDDL := issue289StartPlanBlockedDDL(t, ctx, ddlDB, "create index ix_payload on "+table+" (payload)")
			waitForPlan()
			execSQLRequire(t, ctx, writerDB, "update "+table+" set payload = 'changed' where id = 1")
			require.NoError(t, finishDDL())

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
			syncAuthenticatedClusterCommit(t, ctx, cluster)
			issue289AssertPhysicalIndex(t, ctx, writerDB, database, "regular_update", "ix_payload", "payload", false,
				[]issue289IndexRow{{1, "changed"}, {2, "control"}})
		})

		t.Run("unique index rejects the stale conflicting update", func(t *testing.T) {
			table := "`" + database + "`.`unique_update`"
			execSQLRequire(t, ctx, writerDB, "create table "+table+" (id int primary key, a int not null)")
			execSQLRequire(t, ctx, writerDB, "insert into "+table+" values (1, 1), (2, 2)")
			issue289WaitForTable(t, ctx, ddlDB, table, 2)

			waitForPlan, finishDDL := issue289StartPlanBlockedDDL(t, ctx, ddlDB, "create unique index uk_a on "+table+" (a)")
			waitForPlan()
			execSQLRequire(t, ctx, writerDB, "update "+table+" set a = 1 where id = 2")
			issue289RequireMySQLError(t, finishDDL(), 1062)

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

		// A different handoff: the writer has compiled without the index, but
		// reaches its metadata lock only after CREATE INDEX has committed.
		for _, tc := range []struct {
			name, table, createIndex, update string
			unique, rollback                 bool
		}{
			{"regular index", "late_regular", "create index ix_a on %s (a)", "update %s set a = 3 where id = 1", false, false},
			{"unique conflict", "late_unique", "create unique index ix_a on %s (a)", "update %s set a = 1 where id = 2", true, false},
			{"rollback", "late_rollback", "create index ix_a on %s (a)", "update %s set a = 3 where id = 1", false, true},
		} {
			t.Run("writer admitted after "+tc.name, func(t *testing.T) {
				table := fmt.Sprintf("`%s`.`%s`", database, tc.table)
				execSQLRequire(t, ctx, writerDB, "create table "+table+" (id int primary key, a int not null)")
				execSQLRequire(t, ctx, writerDB, "insert into "+table+" values (1, 1), (2, 2)")
				issue289WaitForTable(t, ctx, ddlDB, table, 2)

				writer, err := writerDB.Conn(ctx)
				require.NoError(t, err)
				defer writer.Close()
				if tc.rollback {
					_, err = writer.ExecContext(ctx, "begin")
					require.NoError(t, err)
					defer func() {
						cleanupCtx, stop := context.WithTimeout(context.Background(), 10*time.Second)
						defer stop()
						_, _ = writer.ExecContext(cleanupCtx, "rollback")
					}()
				}
				writerCN, err := cluster.GetCNService(0)
				require.NoError(t, err)
				err = issue289RunStaleWriter(t, ctx, cluster, writerCN, writer, writerDB, ddlDB,
					fmt.Sprintf(tc.update, table), fmt.Sprintf(tc.createIndex, table))
				if tc.unique {
					issue289RequireMySQLError(t, err, 1062)
				} else {
					require.NoError(t, err)
				}
				if tc.rollback {
					_, err = writer.ExecContext(ctx, "rollback")
					require.NoError(t, err)
				}
				syncAuthenticatedClusterCommit(t, ctx, cluster)
				expected := []issue289IndexRow{{1, "1"}, {2, "2"}}
				if !tc.unique && !tc.rollback {
					expected[0].value = "3"
				}
				issue289AssertPhysicalIndex(t, ctx, ddlDB, database, tc.table, "ix_a", "a", tc.unique, expected)
				if tc.unique {
					execSQLRequire(t, ctx, writerDB, "update "+table+" set a = 3 where id = 2")
					syncAuthenticatedClusterCommit(t, ctx, cluster)
					expected[1].value = "3"
					issue289AssertPhysicalIndex(t, ctx, ddlDB, database, tc.table, "ix_a", "a", true, expected)
				}
			})
		}

		t.Run("cancel index creation waiting for a writer", func(t *testing.T) {
			const tableName = "cancel_build"
			table := fmt.Sprintf("`%s`.`%s`", database, tableName)
			execSQLRequire(t, ctx, writerDB, "create table "+table+" (id int primary key, a int not null)")
			execSQLRequire(t, ctx, writerDB, "insert into "+table+" values (1, 1)")
			issue289WaitForTable(t, ctx, ddlDB, table, 1)
			writer, err := writerDB.Conn(ctx)
			require.NoError(t, err)
			defer writer.Close()
			require.NoError(t, execIssue29715(ctx, writer, "begin"))
			defer func() {
				cleanup, stop := context.WithTimeout(context.Background(), 10*time.Second)
				defer stop()
				_, _ = writer.ExecContext(cleanup, "rollback")
			}()
			require.NoError(t, execIssue29715(ctx, writer, "update "+table+" set a = 2 where id = 1"))

			var services []lockservice.LockService
			cluster.ForeachServices(func(s embed.ServiceOperator) bool {
				if s.ServiceType() == metadata.ServiceType_CN {
					services = append(services, lockservice.GetLockServiceByServiceID(s.ServiceID()))
				}
				return true
			})
			var tableID uint64
			require.NoError(t, writerDB.QueryRowContext(ctx,
				"select rel_id from mo_catalog.mo_tables where reldatabase=? and relname=?",
				strings.ToLower(database), tableName).Scan(&tableID))
			txnID := issue29715WriterTxn(services, tableID)
			require.NotEmpty(t, txnID)
			packer := types.NewPacker()
			defer packer.Close()
			packer.EncodeUint32(0)
			packer.EncodeStringType([]byte(strings.ToLower(database)))
			packer.EncodeStringType([]byte(tableName))
			serial := bytes.Clone(packer.GetBuf())
			packer.Reset()
			packer.EncodeStringType(serial)
			key := bytes.Clone(packer.GetBuf())
			require.True(t, issue29715MetadataState(services, txnID, key, false, pblock.LockMode_Shared))

			ddl, err := ddlDB.Conn(ctx)
			require.NoError(t, err)
			defer ddl.Close()
			var ddlConnectionID uint32
			require.NoError(t, ddl.QueryRowContext(ctx, "select connection_id()").Scan(&ddlConnectionID))
			ddlCtx, cancelDDL := context.WithCancel(context.Background())
			done := make(chan error, 1)
			joined := false
			defer func() {
				if !joined {
					cleanup, stop := context.WithTimeout(context.Background(), 10*time.Second)
					defer stop()
					_, _ = ddlDB.ExecContext(cleanup, fmt.Sprintf("kill query %d", ddlConnectionID))
					select {
					case <-done:
					case <-cleanup.Done():
						t.Error("canceled CREATE INDEX did not exit")
					}
				}
				cancelDDL()
			}()
			go func() { _, err := ddl.ExecContext(ddlCtx, "create index ix_a on "+table+" (a)"); done <- err }()
			require.Eventually(t, func() bool {
				return issue29715MetadataState(services, txnID, key, true, pblock.LockMode_Shared)
			}, 20*time.Second, 10*time.Millisecond, "CREATE INDEX must wait for the writer's metadata lock")
			execSQLRequire(t, ctx, ddlDB, fmt.Sprintf("kill query %d", ddlConnectionID))
			select {
			case err := <-done:
				joined = true
				require.Error(t, err)
			case <-ctx.Done():
				t.Fatal(ctx.Err())
			}
			require.NoError(t, execIssue29715(ctx, writer, "rollback"))
			var published int
			require.NoError(t, writerDB.QueryRowContext(ctx,
				"select count(*) from mo_catalog.mo_indexes where table_id=? and name='ix_a'", tableID).Scan(&published))
			require.Zero(t, published)
			execSQLRequire(t, ctx, writerDB, "create index ix_a on "+table+" (a)")
			execSQLRequire(t, ctx, writerDB, "drop index ix_a on "+table)
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
	db0 := openIssue29715DB(t, cn0.GetServiceConfig().CN.Frontend.Port)
	db1 := openIssue29715DB(t, cn1.GetServiceConfig().CN.Frontend.Port)
	db0.SetMaxOpenConns(4)
	db1.SetMaxOpenConns(4)
	return db0, db1
}

// Pause the actual writer's first metadata admission after it has compiled its
// UPDATE, then publish the index. The result hook only observes real locks.
func issue289RunStaleWriter(t *testing.T, ctx context.Context, cluster embed.Cluster,
	writerCN embed.ServiceOperator, writer *sql.Conn, writerDB, ddlDB *sql.DB, update, createIndex string) error {
	t.Helper()
	sid := writerCN.ServiceID()
	sessions := issue29715SessionManager(t, writerCN)
	var connectionID uint32
	require.NoError(t, writer.QueryRowContext(ctx, "select connection_id()").Scan(&connectionID))

	paused, release := make(chan struct{}), make(chan struct{})
	var first atomic.Bool
	var releaseOnce sync.Once
	var owner atomic.Value
	var admissions atomic.Int32
	workCtx, cancel := context.WithCancel(context.Background())
	testCtx := moruntime.MustGetTestingContext(sid)
	testCtx.SetBeforeLockFunc(func(txnID []byte, tableID uint64) {
		if tableID == catalog.MO_TABLES_ID &&
			issue29715TxnBelongsToConnection(sessions.GetAllSessions(), sid, connectionID, txnID) &&
			first.CompareAndSwap(false, true) {
			owner.Store(bytes.Clone(txnID))
			close(paused)
			select {
			case <-release:
			case <-workCtx.Done():
			}
		}
	})
	testCtx.SetAdjustLockResultFunc(func(txnID []byte, tableID uint64, result *pblock.Result) {
		id, ok := owner.Load().([]byte)
		if ok && bytes.Equal(id, txnID) && tableID == catalog.MO_TABLES_ID && !result.Timestamp.IsEmpty() {
			admissions.Add(1)
		}
	})
	done := make(chan error, 1)
	joined := false
	defer func() {
		if !joined {
			cleanup, stop := context.WithTimeout(context.Background(), 10*time.Second)
			defer stop()
			_, _ = writerDB.ExecContext(cleanup, fmt.Sprintf("kill query %d", connectionID))
			releaseOnce.Do(func() { close(release) })
			select {
			case <-done:
			case <-cleanup.Done():
				t.Error("writer did not exit during cleanup")
			}
		}
		cancel()
		releaseOnce.Do(func() { close(release) })
		testCtx.SetBeforeLockFunc(nil)
		testCtx.SetAdjustLockResultFunc(nil)
	}()
	go func() {
		_, err := writer.ExecContext(workCtx, update)
		done <- err
	}()
	select {
	case <-paused:
	case err := <-done:
		joined = true
		t.Fatalf("writer did not reach metadata admission: %v", err)
	case <-ctx.Done():
		t.Fatal(ctx.Err())
	}
	execSQLRequire(t, ctx, ddlDB, createIndex)
	syncAuthenticatedClusterCommit(t, ctx, cluster)
	releaseOnce.Do(func() { close(release) })
	var err error
	select {
	case err = <-done:
		joined = true
	case <-ctx.Done():
		t.Fatal(ctx.Err())
	}
	require.GreaterOrEqual(t, admissions.Load(), int32(2),
		"stale writer must refresh metadata and be admitted again")
	return err
}

// A blocked DDL must be released and joined even if its caller fails before
// reaching the normal handoff. KILL QUERY cancels the server-side statement.
func issue289StartPlanBlockedDDL(t *testing.T, ctx context.Context, db *sql.DB, statement string) (func(), func() error) {
	t.Helper()
	wait, release := issue289InstallPlanBarrier(t, ctx, issue289CreateIndexPlanCompiled, 1)
	conn, err := db.Conn(ctx)
	require.NoError(t, err)
	t.Cleanup(func() { _ = conn.Close() })
	var id uint32
	require.NoError(t, conn.QueryRowContext(ctx, "select connection_id()").Scan(&id))
	ddlCtx, cancel := context.WithCancel(context.Background())
	done := make(chan error, 1)
	joined := false
	t.Cleanup(func() {
		if !joined {
			cleanup, stop := context.WithTimeout(context.Background(), 10*time.Second)
			defer stop()
			_, _ = db.ExecContext(cleanup, fmt.Sprintf("kill query %d", id))
			release()
			select {
			case <-done:
			case <-cleanup.Done():
				t.Error("CREATE INDEX did not exit during cleanup")
			}
		}
		release()
		cancel()
	})
	go func() { _, err := conn.ExecContext(ddlCtx, statement); done <- err }()
	return wait, func() error {
		release()
		select {
		case err := <-done:
			joined = true
			return err
		case <-ctx.Done():
			return ctx.Err()
		}
	}
}

type issue289IndexRow struct {
	id    int64
	value string
}

func issue289AssertPhysicalIndex(t *testing.T, ctx context.Context, db *sql.DB,
	database, table, index, column string, unique bool, expected []issue289IndexRow) {
	t.Helper()
	quote := func(s string) string { return "`" + strings.ReplaceAll(s, "`", "``") + "`" }
	qualified := quote(database) + "." + quote(table)
	var hidden string
	require.NoError(t, db.QueryRowContext(ctx,
		"select index_table_name from mo_catalog.mo_indexes where table_id=("+
			"select rel_id from mo_catalog.mo_tables where reldatabase=? and relname=?) and name=?",
		strings.ToLower(database), table, index).Scan(&hidden))
	read := func(query string) []issue289IndexRow {
		rows, err := db.QueryContext(ctx, query)
		require.NoError(t, err)
		defer rows.Close()
		var result []issue289IndexRow
		for rows.Next() {
			var row issue289IndexRow
			require.NoError(t, rows.Scan(&row.id, &row.value))
			result = append(result, row)
		}
		require.NoError(t, rows.Err())
		return result
	}
	require.Equal(t, expected, read("select id,"+quote(column)+" from "+qualified+" order by id"))
	baseKey, indexKey := "hex(serial_full("+quote(column)+",id))", "hex(__mo_index_idx_col)"
	if unique {
		baseKey, indexKey = quote(column), "__mo_index_idx_col"
	}
	base := read("select id," + baseKey + " from " + qualified + " order by id")
	actual := read("select __mo_index_pri_col," + indexKey + " from " + quote(database) + "." + quote(hidden) + " order by __mo_index_pri_col")
	require.Equal(t, base, actual, "physical index must have exactly the current base-table keys")
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
