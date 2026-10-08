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
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"fmt"
	"github.com/matrixorigin/matrixone/pkg/catalog"
	moruntime "github.com/matrixorigin/matrixone/pkg/common/runtime"
	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/embed"
	"github.com/matrixorigin/matrixone/pkg/lockservice"
	pblock "github.com/matrixorigin/matrixone/pkg/pb/lock"
	"github.com/matrixorigin/matrixone/pkg/pb/metadata"
	"github.com/stretchr/testify/require"
)

func TestIssue29715ODKUConcurrentDropIndex(t *testing.T) {
	runAuthenticatedClusterTest(t, func(c embed.Cluster) {
		writerCN, err := c.GetCNService(0)
		require.NoError(t, err)
		ddlCN, err := c.GetCNService(1)
		require.NoError(t, err)
		writerDB := openIssue29715DB(t, writerCN.GetServiceConfig().CN.Frontend.Port)
		defer writerDB.Close()
		const database = "issue_29715"
		ctx, cancel := context.WithTimeout(t.Context(), 180*time.Second)
		defer cancel()
		execSQLRequire(t, ctx, writerDB, "drop database if exists "+database)
		execSQLRequire(t, ctx, writerDB, "create database "+database)
		defer func() {
			cleanupCtx, stop := context.WithTimeout(context.Background(), 10*time.Second)
			defer stop()
			execSQLMaybe(t, cleanupCtx, writerDB, "drop database if exists "+database)
		}()
		// Match the original workload with a selected DDL database; a separate
		// control exercises a qualified ALTER without a session database.
		ddlDB, err := sql.Open("mysql", fmt.Sprintf("dump:111@tcp(127.0.0.1:%d)/%s", ddlCN.GetServiceConfig().CN.Frontend.Port, database))
		require.NoError(t, err)
		defer ddlDB.Close()

		var services []lockservice.LockService
		c.ForeachServices(func(s embed.ServiceOperator) bool {
			if s.ServiceType() == metadata.ServiceType_CN {
				services = append(services, lockservice.GetLockServiceByServiceID(s.ServiceID()))
			}
			return true
		})
		for _, terminal := range []string{"commit", "rollback", "late admission", "DDL admitted first"} {
			t.Run(terminal, func(t *testing.T) {
				execSQLRequire(t, ctx, writerDB, "create table "+database+".t (id int primary key, u int, v int, unique uk(u), key idx_v(v))")
				defer execSQLMaybe(t, ctx, writerDB, "drop table if exists "+database+".t")
				execSQLRequire(t, ctx, writerDB, "insert into "+database+".t values (1,1,0)")
				require.Eventually(t, func() bool {
					var n int
					return ddlDB.QueryRowContext(ctx, "select count(*) from "+database+".t").Scan(&n) == nil && n == 1
				}, 20*time.Second, 10*time.Millisecond, "DDL CN must see the table before scheduling the race")
				writer, err := writerDB.Conn(ctx)
				require.NoError(t, err)
				defer writer.Close()
				const odku = "insert into issue_29715.t values (1,1,1) on duplicate key update v=v+1"
				const drop = "alter table issue_29715.t drop index uk"
				if terminal == "late admission" {
					testIssue29715LateAdmission(t, ctx, writer, ddlDB, writerCN.ServiceID(), odku, drop)
				} else if terminal == "DDL admitted first" {
					var tableID uint64
					var hidden string
					require.NoError(t, writer.QueryRowContext(ctx,
						"select table_id,index_table_name from mo_catalog.mo_indexes where table_id=(select rel_id from mo_catalog.mo_tables where reldatabase=? and relname='t') and name='uk'", database).Scan(&tableID, &hidden))
					testIssue29715DDLFirst(t, ctx, writer, ddlDB, writerCN.ServiceID(), ddlCN.ServiceID(), services, tableID, hidden, odku, drop)
				} else {
					var tableID uint64
					require.NoError(t, writerDB.QueryRowContext(ctx,
						"select rel_id from mo_catalog.mo_tables where reldatabase=? and relname='t'", database).Scan(&tableID))
					require.NoError(t, execIssue29715(ctx, writer, "begin"))
					defer func() {
						cleanupCtx, stop := context.WithTimeout(context.Background(), 10*time.Second)
						defer stop()
						_, _ = writer.ExecContext(cleanupCtx, "rollback")
					}()
					require.NoError(t, execIssue29715(ctx, writer, odku))
					txnID := issue29715WriterTxn(services, tableID)
					require.NotEmpty(t, txnID, "ODKU must hold its base row lock")
					packer := types.NewPacker()
					defer packer.Close()
					packer.EncodeUint32(0)
					packer.EncodeStringType([]byte(database))
					packer.EncodeStringType([]byte("t"))
					serial := bytes.Clone(packer.GetBuf())
					packer.Reset()
					packer.EncodeStringType(serial)
					key := bytes.Clone(packer.GetBuf())
					require.True(t, issue29715MetadataState(services, txnID, key, false))
					ddlCtx, stopDDL := context.WithCancel(ctx)
					defer stopDDL()
					done := make(chan error, 1)
					go func() { _, err := ddlDB.ExecContext(ddlCtx, drop); done <- err }()
					joined := false
					defer func() {
						if !joined {
							stopDDL()
							cleanupCtx, stop := context.WithTimeout(context.Background(), 10*time.Second)
							defer stop()
							_, _ = writer.ExecContext(cleanupCtx, "rollback")
							select {
							case <-done:
							case <-cleanupCtx.Done():
								t.Error("DROP did not exit during cleanup")
							}
						}
					}()
					require.Eventually(t, func() bool { return issue29715MetadataState(services, txnID, key, true) },
						20*time.Second, 10*time.Millisecond, "DROP must wait for ODKU's base metadata lock")
					require.NoError(t, execIssue29715(ctx, writer, terminal))
					select {
					case err := <-done:
						joined = true
						require.NoError(t, err)
					case <-ctx.Done():
						t.Fatal(ctx.Err())
					}
				}
				syncAuthenticatedClusterCommit(t, ctx, c)
				want := 1
				if terminal == "rollback" {
					want = 0
				}
				var count, sum int
				require.NoError(t, writer.QueryRowContext(ctx, "select count(*),sum(v) from issue_29715.t").Scan(&count, &sum))
				require.Equal(t, 1, count)
				require.Equal(t, want, sum, "ODKU must apply exactly once, or not at all after rollback")
				var indexes int
				require.NoError(t, writer.QueryRowContext(ctx,
					"select count(*) from mo_catalog.mo_indexes where table_id=(select rel_id from mo_catalog.mo_tables where reldatabase=? and relname='t') and name='uk'", database).Scan(&indexes))
				require.Zero(t, indexes)
				// With uk removed, another primary key may reuse u. The public
				// result checks that the recompiled writer did not retain that index.
				require.NoError(t, execIssue29715(ctx, writer, "insert into issue_29715.t values (2,1,0) on duplicate key update v=v+1"))
				require.NoError(t, writer.QueryRowContext(ctx, "select count(*),sum(v) from issue_29715.t").Scan(&count, &sum))
				require.Equal(t, 2, count)
				require.Equal(t, want, sum)
				// Dropping uk must preserve a neighboring regular index and its
				// maintenance across the recompiled ODKU and subsequent insert.
				require.NoError(t, writer.QueryRowContext(ctx, "select count(*) from issue_29715.t force index(idx_v) where v=0").Scan(&count))
				if want == 0 {
					require.Equal(t, 2, count)
				} else {
					require.Equal(t, 1, count)
				}
			})
		}
	})
}

// ODKU holds separate base and unique-index metadata keys. Observe the base
// key explicitly instead of the older single-metadata-key fixture assumption.
func issue29715MetadataState(services []lockservice.LockService, txnID, key []byte, waiter bool) bool {
	found := false
	for _, service := range services {
		service.IterLocks(func(tableID uint64, keys [][]byte, lock lockservice.Lock) bool {
			if tableID == catalog.MO_TABLES_ID && lock.GetLockMode() == pblock.LockMode_Shared && len(keys) == 1 && bytes.Equal(keys[0], key) {
				if waiter {
					lock.IterWaiters(func(_ pblock.WaitTxn) bool { found = true; return false })
				} else {
					lock.IterHolders(func(holder pblock.WaitTxn) bool { found = found || bytes.Equal(holder.TxnID, txnID); return !found })
				}
			}
			return !found
		})
	}
	return found
}

func testIssue29715LateAdmission(t *testing.T, ctx context.Context, writer *sql.Conn, ddlDB *sql.DB, sid, odku, drop string) {
	t.Helper()
	paused, release := make(chan struct{}), make(chan struct{})
	var first atomic.Bool
	var releaseOnce sync.Once
	var admissions atomic.Int32
	var owner atomic.Value
	tc := moruntime.MustGetTestingContext(sid)
	tc.SetBeforeLockFunc(func(txnID []byte, tableID uint64) {
		if tableID == catalog.MO_TABLES_ID && first.CompareAndSwap(false, true) {
			owner.Store(bytes.Clone(txnID))
			close(paused)
			select {
			case <-release:
			case <-ctx.Done():
			}
		}
	})
	// GetBeforeLockFunc requires the result hook. Observe real committed DDL
	// evidence without changing the result or injecting a retry.
	tc.SetAdjustLockResultFunc(func(txnID []byte, tableID uint64, result *pblock.Result) {
		id, ok := owner.Load().([]byte)
		if ok && bytes.Equal(id, txnID) && tableID == catalog.MO_TABLES_ID && !result.Timestamp.IsEmpty() {
			admissions.Add(1)
		}
	})
	defer tc.SetBeforeLockFunc(nil)
	defer tc.SetAdjustLockResultFunc(nil)
	workCtx, stop := context.WithCancel(ctx)
	defer stop()
	done := make(chan error, 1)
	go func() { done <- execIssue29715(workCtx, writer, odku) }()
	joined := false
	defer func() {
		stop()
		releaseOnce.Do(func() { close(release) })
		if !joined {
			select {
			case <-done:
			case <-time.After(10 * time.Second):
				t.Error("ODKU did not exit during cleanup")
			}
		}
	}()
	select {
	case <-paused:
	case err := <-done:
		joined = true
		t.Fatalf("ODKU did not reach metadata admission: %v", err)
	case <-ctx.Done():
		t.Fatal(ctx.Err())
	}
	execSQLRequire(t, ctx, ddlDB, drop)
	releaseOnce.Do(func() { close(release) })
	select {
	case err := <-done:
		joined = true
		require.NoError(t, err, "stale compiled ODKU must rebuild rather than expose the hidden table")
	case <-ctx.Done():
		t.Fatal(ctx.Err())
	}
	require.GreaterOrEqual(t, admissions.Load(), int32(2),
		"stale metadata admission must refresh and rebuild before the second admission")
}

func openIssue29715DB(t *testing.T, port int64) *sql.DB {
	t.Helper()
	db, err := sql.Open("mysql", fmt.Sprintf("dump:111@tcp(127.0.0.1:%d)/", port))
	require.NoError(t, err)
	return db
}
func execIssue29715(ctx context.Context, conn *sql.Conn, query string) error {
	_, err := conn.ExecContext(ctx, query)
	return err
}
func issue29715WriterTxn(services []lockservice.LockService, tableID uint64) []byte {
	var found []byte
	for _, service := range services {
		service.IterLocks(func(id uint64, _ [][]byte, l lockservice.Lock) bool {
			if id == tableID && l.GetLockMode() == pblock.LockMode_Exclusive {
				l.IterHolders(func(holder pblock.WaitTxn) bool { found = bytes.Clone(holder.TxnID); return false })
			}
			return len(found) == 0
		})
		if len(found) > 0 {
			break
		}
	}
	return found
}

// Pause ALTER after its parent catalog admission, then let ODKU acquire its
// hidden shared metadata key and wait for that parent. Nested child DROP TABLE
// would form a cycle; parent-owned deletion must finish and let ODKU rebuild.
func testIssue29715DDLFirst(t *testing.T, ctx context.Context, writer *sql.Conn, ddlDB *sql.DB,
	writerSID, ddlSID string, services []lockservice.LockService, tableID uint64, hidden, odku, drop string) {
	t.Helper()
	workCtx, stop := context.WithCancel(ctx)
	defer stop()
	paused, release := make(chan struct{}), make(chan struct{})
	var first atomic.Bool
	var once sync.Once
	var owner atomic.Value
	ddlTC := moruntime.MustGetTestingContext(ddlSID)
	ddlTC.SetBeforeLockFunc(func(_ []byte, id uint64) {
		if id == tableID && first.CompareAndSwap(false, true) {
			close(paused)
			select {
			case <-release:
			case <-workCtx.Done():
			}
		}
	})
	ddlTC.SetAdjustLockResultFunc(func([]byte, uint64, *pblock.Result) {})
	defer ddlTC.SetBeforeLockFunc(nil)
	defer ddlTC.SetAdjustLockResultFunc(nil)
	writerTC := moruntime.MustGetTestingContext(writerSID)
	writerTC.SetBeforeLockFunc(func(txn []byte, id uint64) {
		if id == catalog.MO_TABLES_ID {
			owner.Store(bytes.Clone(txn))
		}
	})
	writerTC.SetAdjustLockResultFunc(func([]byte, uint64, *pblock.Result) {})
	defer writerTC.SetBeforeLockFunc(nil)
	defer writerTC.SetAdjustLockResultFunc(nil)
	ddlDone := make(chan error, 1)
	writerDone := make(chan error, 1)
	ddlJoined, writerJoined, writerStarted := false, false, false
	defer func() {
		stop()
		once.Do(func() { close(release) })
		drain := func(done <-chan error) {
			select {
			case <-done:
			case <-time.After(10 * time.Second):
				t.Error("worker did not exit during cleanup")
			}
		}
		if !ddlJoined {
			drain(ddlDone)
		}
		if writerStarted && !writerJoined {
			drain(writerDone)
		}
	}()
	go func() { _, err := ddlDB.ExecContext(workCtx, drop); ddlDone <- err }()
	select {
	case <-paused:
	case err := <-ddlDone:
		ddlJoined = true
		t.Fatalf("DDL did not pause after parent admission: %v", err)
	case <-workCtx.Done():
		t.Fatal(workCtx.Err())
	}
	packer := types.NewPacker()
	defer packer.Close()
	packer.EncodeUint32(0)
	packer.EncodeStringType([]byte("issue_29715"))
	packer.EncodeStringType([]byte(hidden))
	serial := bytes.Clone(packer.GetBuf())
	packer.Reset()
	packer.EncodeStringType(serial)
	key := bytes.Clone(packer.GetBuf())
	writerStarted = true
	go func() { writerDone <- execIssue29715(workCtx, writer, odku) }()
	require.Eventually(t, func() bool {
		txn, ok := owner.Load().([]byte)
		return ok && issue29715MetadataState(services, txn, key, false)
	},
		20*time.Second, 10*time.Millisecond, "ODKU must hold the hidden metadata key while waiting for ALTER's parent")
	once.Do(func() { close(release) })
	select {
	case err := <-ddlDone:
		ddlJoined = true
		require.NoError(t, err)
	case <-workCtx.Done():
		t.Fatal(workCtx.Err())
	}
	select {
	case err := <-writerDone:
		writerJoined = true
		require.NoError(t, err, "metadata order must not force a public deadlock or hidden-table error")
	case <-workCtx.Done():
		t.Fatal(workCtx.Err())
	}
}

func TestIssue29715QualifiedDropIndexWithoutSessionDatabase(t *testing.T) {
	runAuthenticatedClusterTest(t, func(c embed.Cluster) {
		ctx, stop := context.WithTimeout(t.Context(), 45*time.Second)
		defer stop()
		cn, err := c.GetCNService(0)
		require.NoError(t, err)
		db := openIssue29715DB(t, cn.GetServiceConfig().CN.Frontend.Port)
		defer db.Close()
		const database = "issue29715_qualified"
		execSQLRequire(t, ctx, db, "drop database if exists "+database)
		execSQLRequire(t, ctx, db, "create database "+database)
		defer func() {
			cleanup, cancel := context.WithTimeout(context.Background(), 10*time.Second)
			defer cancel()
			execSQLMaybe(t, cleanup, db, "drop database if exists "+database)
		}()
		execSQLRequire(t, ctx, db, "create table "+database+".t (id int primary key,u int,unique uk(u))")
		execSQLRequire(t, ctx, db, "insert into "+database+".t values(1,1)")
		execSQLRequire(t, ctx, db, "alter table "+database+".t drop index uk")
		execSQLRequire(t, ctx, db, "insert into "+database+".t values(2,1)")
		var count int
		require.NoError(t, db.QueryRowContext(ctx, "select count(*) from "+database+".t").Scan(&count))
		require.Equal(t, 2, count)
	})
}
