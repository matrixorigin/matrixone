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
	"testing"
	"time"

	_ "github.com/go-sql-driver/mysql"
	"github.com/matrixorigin/matrixone/pkg/catalog"
	"github.com/matrixorigin/matrixone/pkg/embed"
	"github.com/matrixorigin/matrixone/pkg/lockservice"
	pblock "github.com/matrixorigin/matrixone/pkg/pb/lock"
	"github.com/matrixorigin/matrixone/pkg/pb/metadata"
	"github.com/matrixorigin/matrixone/pkg/tests/testutils"
	"github.com/stretchr/testify/require"
)

type issue27487IndexCase struct {
	name           string
	table          string
	createTableSQL string
	seedSQL        string
	heldInsertSQL  string
	createIndexSQL string
	prepareDDL     func(context.Context, *sql.Conn) error
	verify         func(*testing.T, context.Context, *sql.Conn)
}

func TestIssue27487ConcurrentInsertIsIncludedInNewIndex(t *testing.T) {
	runAuthenticatedClusterTest(t, func(c embed.Cluster) {
		ctx, cancel := context.WithTimeout(context.Background(), 240*time.Second)
		defer cancel()

		writerCN, err := c.GetCNService(0)
		require.NoError(t, err)
		ddlCN, err := c.GetCNService(1)
		require.NoError(t, err)
		writerPort := writerCN.GetServiceConfig().CN.Frontend.Port
		ddlPort := ddlCN.GetServiceConfig().CN.Frontend.Port
		require.NotEqual(t, writerPort, ddlPort, "writer and DDL must use different CN frontends")

		writerDB, err := sql.Open("mysql", issue27487DSN(writerPort))
		require.NoError(t, err)
		defer writerDB.Close()
		writerDB.SetMaxOpenConns(3)
		ddlDB, err := sql.Open("mysql", issue27487DSN(ddlPort))
		require.NoError(t, err)
		defer ddlDB.Close()
		ddlDB.SetMaxOpenConns(2)

		const database = "issue_27487_concurrent_index"
		execSQLMaybe(t, ctx, writerDB, "drop database if exists `"+database+"`")
		defer func() {
			cleanupCtx, cleanupCancel := context.WithTimeout(context.Background(), 30*time.Second)
			defer cleanupCancel()
			execSQLMaybe(t, cleanupCtx, writerDB, "drop database if exists `"+database+"`")
		}()
		execSQLRequire(t, ctx, writerDB, "create database `"+database+"`")

		lockServices := issue27487LockServices(c)
		require.NotEmpty(t, lockServices)
		cases := []issue27487IndexCase{
			{
				name:           "regular secondary index",
				table:          "regular_docs",
				createTableSQL: "create table `" + database + "`.`regular_docs` (id bigint primary key, k int)",
				seedSQL:        "insert into `" + database + "`.`regular_docs` values (1, 1)",
				heldInsertSQL:  "insert into `" + database + "`.`regular_docs` values (2, 27487)",
				createIndexSQL: "create index idx_k on `" + database + "`.`regular_docs` (`k`)",
				verify: func(t *testing.T, ctx context.Context, conn *sql.Conn) {
					t.Helper()
					const indexedSQL = "select count(*) from `" + database + "`.`regular_docs` " +
						"force index(idx_k) where k = 27487"
					plan, err := testutils.QueryText(ctx, conn, "explain "+indexedSQL)
					require.NoError(t, err)
					require.Contains(t, strings.ToLower(plan), "index table scan")
					require.Contains(t, strings.ToLower(plan), "idx_k")

					var indexedRows, scannedRows int
					require.NoError(t, conn.QueryRowContext(ctx, indexedSQL).Scan(&indexedRows))
					require.NoError(t, conn.QueryRowContext(ctx,
						"select count(*) from `"+database+"`.`regular_docs` "+
							"ignore index(idx_k) where k = 27487").Scan(&scannedRows))
					require.Equal(t, 1, scannedRows)
					require.Equal(t, scannedRows, indexedRows)
				},
			},
			{
				name:           "fulltext index",
				table:          "fulltext_docs",
				createTableSQL: "create table `" + database + "`.`fulltext_docs` (id bigint primary key, body text)",
				seedSQL:        "insert into `" + database + "`.`fulltext_docs` values (1, 'seedtoken')",
				heldInsertSQL:  "insert into `" + database + "`.`fulltext_docs` values (2, 'heldtoken')",
				createIndexSQL: "create fulltext index ft_body on `" + database + "`.`fulltext_docs` (`body`)",
				prepareDDL: func(ctx context.Context, conn *sql.Conn) error {
					return execIssue27487(ctx, conn, "set experimental_fulltext_index = 1")
				},
				verify: func(t *testing.T, ctx context.Context, conn *sql.Conn) {
					t.Helper()
					var indexedRows, scannedRows int
					require.NoError(t, conn.QueryRowContext(ctx,
						"select count(*) from `"+database+"`.`fulltext_docs` "+
							"where match(body) against('heldtoken')").Scan(&indexedRows))
					require.NoError(t, conn.QueryRowContext(ctx,
						"select count(*) from `"+database+"`.`fulltext_docs` "+
							"where body like '%heldtoken%'").Scan(&scannedRows))
					require.Equal(t, 1, scannedRows)
					require.Equal(t, scannedRows, indexedRows)
				},
			},
		}

		for _, testCase := range cases {
			t.Run(testCase.name, func(t *testing.T) {
				runIssue27487IndexCase(t, ctx, writerDB, ddlDB, database, lockServices, testCase)
			})
		}
	})
}

func runIssue27487IndexCase(
	t *testing.T,
	ctx context.Context,
	writerDB *sql.DB,
	ddlDB *sql.DB,
	database string,
	lockServices []lockservice.LockService,
	testCase issue27487IndexCase,
) {
	t.Helper()
	execSQLRequire(t, ctx, writerDB, testCase.createTableSQL)
	execSQLRequire(t, ctx, writerDB, testCase.seedSQL)

	var tableID uint64
	require.NoError(t, writerDB.QueryRowContext(ctx,
		"select rel_id from mo_catalog.mo_tables where reldatabase = ? and relname = ?",
		database, testCase.table).Scan(&tableID))

	writer, err := writerDB.Conn(ctx)
	require.NoError(t, err)
	defer writer.Close()
	ddlConn, err := ddlDB.Conn(ctx)
	require.NoError(t, err)
	defer ddlConn.Close()
	if testCase.prepareDDL != nil {
		require.NoError(t, testCase.prepareDDL(ctx, ddlConn))
	}

	// The table was created on another CN. Establish catalog and seed-row
	// visibility before starting the lock-order phase, so a stale CN cannot turn
	// the intended metadata wait into an unrelated no-such-table failure.
	require.Eventually(t, func() bool {
		var seedRows int
		err := ddlConn.QueryRowContext(ctx,
			"select count(*) from `"+database+"`.`"+testCase.table+"`").Scan(&seedRows)
		return err == nil && seedRows == 1
	}, 30*time.Second, 10*time.Millisecond, "DDL CN did not observe the seeded table")

	writerOpen := true
	require.NoError(t, execIssue27487(ctx, writer, "begin"))
	defer func() {
		if !writerOpen {
			return
		}
		cleanupCtx, cleanupCancel := context.WithTimeout(context.Background(), 10*time.Second)
		defer cleanupCancel()
		_, _ = writer.ExecContext(cleanupCtx, "rollback")
	}()
	require.NoError(t, execIssue27487(ctx, writer, testCase.heldInsertSQL))

	var writerTxnID []byte
	require.Eventually(t, func() bool {
		writerTxnID = findIssue27487WriterTxn(lockServices, tableID)
		return len(writerTxnID) > 0
	}, 30*time.Second, 10*time.Millisecond, "INSERT did not hold the base-table row lock")

	var metadataKeys [][]byte
	require.Eventually(t, func() bool {
		metadataKeys = findIssue27487MetadataLock(lockServices, writerTxnID)
		return len(metadataKeys) > 0
	}, 30*time.Second, 10*time.Millisecond, "INSERT did not hold its target-table metadata lock")

	ddlCtx, cancelDDL := context.WithCancel(ctx)
	defer cancelDDL()
	ddlDone := make(chan error, 1)
	ddlFinished := false
	defer func() {
		if ddlFinished {
			return
		}
		cancelDDL()
		cleanupCtx, cleanupCancel := context.WithTimeout(context.Background(), 10*time.Second)
		defer cleanupCancel()
		if writerOpen {
			_, _ = writer.ExecContext(cleanupCtx, "rollback")
			writerOpen = false
		}
		select {
		case <-ddlDone:
		case <-cleanupCtx.Done():
		}
	}()
	go func() {
		ddlDone <- execIssue27487(ddlCtx, ddlConn, testCase.createIndexSQL)
	}()

	require.Eventually(t, func() bool {
		return hasIssue27487Waiter(lockServices, metadataKeys)
	}, 30*time.Second, 10*time.Millisecond, "CREATE INDEX did not wait for the INSERT metadata lock")

	select {
	case ddlErr := <-ddlDone:
		ddlFinished = true
		require.Failf(t, "DDL returned before INSERT committed", "error: %v", ddlErr)
	default:
	}

	require.NoError(t, execIssue27487(ctx, writer, "commit"))
	writerOpen = false
	select {
	case ddlErr := <-ddlDone:
		ddlFinished = true
		require.NoError(t, ddlErr)
	case <-time.After(30 * time.Second):
		t.Fatal("CREATE INDEX did not return after INSERT committed")
	}

	testCase.verify(t, ctx, ddlConn)
}

func issue27487DSN(port int64) string {
	return fmt.Sprintf("dump:111@tcp(127.0.0.1:%d)/", port)
}

func issue27487LockServices(c embed.Cluster) []lockservice.LockService {
	var services []lockservice.LockService
	c.ForeachServices(func(service embed.ServiceOperator) bool {
		if service.ServiceType() == metadata.ServiceType_CN {
			services = append(services, lockservice.GetLockServiceByServiceID(service.ServiceID()))
		}
		return true
	})
	return services
}

func findIssue27487WriterTxn(services []lockservice.LockService, tableID uint64) []byte {
	txnIDs := make(map[string][]byte)
	for _, service := range services {
		service.IterLocks(func(lockedTableID uint64, _ [][]byte, lock lockservice.Lock) bool {
			if lockedTableID != tableID || lock.GetLockMode() != pblock.LockMode_Exclusive {
				return true
			}
			lock.IterHolders(func(holder pblock.WaitTxn) bool {
				txnIDs[string(holder.TxnID)] = bytes.Clone(holder.TxnID)
				return true
			})
			return true
		})
	}
	if len(txnIDs) != 1 {
		return nil
	}
	for _, txnID := range txnIDs {
		return txnID
	}
	return nil
}

func findIssue27487MetadataLock(services []lockservice.LockService, writerTxnID []byte) [][]byte {
	var found [][]byte
	multiple := false
	for _, service := range services {
		service.IterLocks(func(tableID uint64, keys [][]byte, lock lockservice.Lock) bool {
			if tableID != catalog.MO_TABLES_ID || lock.GetLockMode() != pblock.LockMode_Shared || len(keys) == 0 {
				return true
			}
			heldByWriter := false
			lock.IterHolders(func(holder pblock.WaitTxn) bool {
				if bytes.Equal(holder.TxnID, writerTxnID) {
					heldByWriter = true
					return false
				}
				return true
			})
			if !heldByWriter {
				return true
			}
			if found == nil {
				found = cloneIssue27487Keys(keys)
			} else if !equalIssue27487Keys(found, keys) {
				multiple = true
			}
			return true
		})
	}
	if multiple {
		return nil
	}
	return found
}

func hasIssue27487Waiter(services []lockservice.LockService, metadataKeys [][]byte) bool {
	found := false
	for _, service := range services {
		service.IterLocks(func(tableID uint64, keys [][]byte, lock lockservice.Lock) bool {
			if tableID != catalog.MO_TABLES_ID || !equalIssue27487Keys(metadataKeys, keys) {
				return true
			}
			lock.IterWaiters(func(_ pblock.WaitTxn) bool {
				found = true
				return false
			})
			return !found
		})
		if found {
			return true
		}
	}
	return false
}

func cloneIssue27487Keys(keys [][]byte) [][]byte {
	cloned := make([][]byte, len(keys))
	for i := range keys {
		cloned[i] = bytes.Clone(keys[i])
	}
	return cloned
}

func equalIssue27487Keys(left, right [][]byte) bool {
	if len(left) != len(right) {
		return false
	}
	for i := range left {
		if !bytes.Equal(left[i], right[i]) {
			return false
		}
	}
	return true
}

func execIssue27487(ctx context.Context, conn *sql.Conn, statement string) error {
	_, err := conn.ExecContext(ctx, statement)
	return err
}
