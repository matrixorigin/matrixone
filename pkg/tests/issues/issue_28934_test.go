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
	"testing"
	"time"

	_ "github.com/go-sql-driver/mysql"
	"github.com/matrixorigin/matrixone/pkg/catalog"
	"github.com/matrixorigin/matrixone/pkg/cnservice"
	"github.com/matrixorigin/matrixone/pkg/defines"
	"github.com/matrixorigin/matrixone/pkg/embed"
	"github.com/matrixorigin/matrixone/pkg/lockservice"
	pblock "github.com/matrixorigin/matrixone/pkg/pb/lock"
	"github.com/matrixorigin/matrixone/pkg/pb/metadata"
	"github.com/matrixorigin/matrixone/pkg/tests/testutils"
	"github.com/matrixorigin/matrixone/pkg/util/executor"
	"github.com/stretchr/testify/require"
)

const issue28934AddForeignKeySQL = "alter table child add constraint fk_parent " +
	"foreign key (parent_id) references parent(id)"

type issue28934Fixture struct {
	database     string
	writerDB     *sql.DB
	ddlDB        *sql.DB
	metadataCN   embed.ServiceOperator
	lockServices []lockservice.LockService
	parentID     uint64
	childID      uint64
}

type issue28934MetadataState struct {
	childID        uint64
	childFKs       int
	parentChildren []uint64
}

type issue28934Waiter struct {
	txnID []byte
}

func TestIssue28934AddForeignKeyLockOrdering(t *testing.T) {
	embed.RunBaseClusterTests(t, func(c embed.Cluster) {
		ctx, cancel := context.WithTimeout(context.Background(), 4*time.Minute)
		defer cancel()

		cn0, err := c.GetCNService(0)
		require.NoError(t, err)
		cn1, err := c.GetCNService(1)
		require.NoError(t, err)
		writerDB := issue28934OpenDB(t, cn0)
		ddlDB := issue28934OpenDB(t, cn1)
		lockServices := issue28934LockServices(c)
		require.NotEmpty(t, lockServices)

		newFixture := func(t *testing.T) issue28934Fixture {
			t.Helper()
			database := strings.ToLower(testutils.GetDatabaseName(t))
			execSQLRequire(t, ctx, writerDB, "create database `"+database+"`")
			t.Cleanup(func() {
				cleanupCtx, cleanupCancel := context.WithTimeout(context.Background(), 15*time.Second)
				defer cleanupCancel()
				_, _ = writerDB.ExecContext(cleanupCtx, "drop database if exists `"+database+"`")
			})
			execSQLRequire(t, ctx, writerDB,
				"create table `"+database+"`.`parent` (id int primary key)")
			execSQLRequire(t, ctx, writerDB,
				"create table `"+database+"`.`child` (id int primary key, parent_id int)")
			execSQLRequire(t, ctx, writerDB,
				"insert into `"+database+"`.`parent` values (1)")
			execSQLRequire(t, ctx, writerDB,
				"insert into `"+database+"`.`child` values (1, 1)")

			// The DDL runs on the other CN. Make catalog and seed-row visibility an
			// explicit setup condition before installing any lock-order barrier.
			require.Eventually(t, func() bool {
				var parentRows, childRows int
				if err := ddlDB.QueryRowContext(ctx,
					"select count(*) from `"+database+"`.`parent`").Scan(&parentRows); err != nil {
					return false
				}
				if err := ddlDB.QueryRowContext(ctx,
					"select count(*) from `"+database+"`.`child`").Scan(&childRows); err != nil {
					return false
				}
				return parentRows == 1 && childRows == 1
			}, 30*time.Second, 10*time.Millisecond, "DDL CN did not observe the seed rows")

			fixture := issue28934Fixture{
				database:     database,
				writerDB:     writerDB,
				ddlDB:        ddlDB,
				metadataCN:   cn0,
				lockServices: lockServices,
			}
			require.NoError(t, writerDB.QueryRowContext(ctx,
				"select rel_id from mo_catalog.mo_tables where reldatabase = ? and relname = 'parent'",
				database).Scan(&fixture.parentID))
			require.NoError(t, writerDB.QueryRowContext(ctx,
				"select rel_id from mo_catalog.mo_tables where reldatabase = ? and relname = 'child'",
				database).Scan(&fixture.childID))
			return fixture
		}

		t.Run("child writer commits before alter validation", func(t *testing.T) {
			fixture := newFixture(t)
			issue28934RunWriterFirst(t, ctx, fixture,
				"update child set parent_id = 2 where id = 1", fixture.childID)

			verifier := issue28934Conn(t, ctx, fixture.writerDB, fixture.database)
			var parentID int
			require.NoError(t, verifier.QueryRowContext(ctx,
				"select parent_id from child where id = 1").Scan(&parentID))
			require.Equal(t, 2, parentID, "the committed orphan must be the state rejected by ALTER")
		})

		t.Run("alter lock precedes orphan writer", func(t *testing.T) {
			fixture := newFixture(t)
			ddlConn := issue28934Conn(t, ctx, fixture.ddlDB, fixture.database)
			writerConn := issue28934Conn(t, ctx, fixture.writerDB, fixture.database)

			require.NoError(t, issue28934Exec(ctx, ddlConn, "begin"))
			ddlOpen := true
			defer func() {
				if ddlOpen {
					issue28934Rollback(ddlConn)
				}
			}()
			require.NoError(t, issue28934Exec(ctx, ddlConn, issue28934AddForeignKeySQL))

			ddlTxnID := issue28934RequireExclusiveHolder(t, fixture.lockServices, fixture.childID)
			waiterQueued, restoreHook := issue28934InstallWaiterBarrier(ddlTxnID, fixture.childID)
			defer restoreHook()

			writerCtx, cancelWriter := context.WithCancel(ctx)
			writerDone := make(chan error, 1)
			writerFinished := false
			go func() {
				writerDone <- issue28934Exec(writerCtx, writerConn,
					"update child set parent_id = 2 where id = 1")
			}()
			defer func() {
				if ddlOpen {
					issue28934Rollback(ddlConn)
					ddlOpen = false
				}
				cancelWriter()
				if !writerFinished {
					issue28934Drain(t, writerDone)
				}
			}()

			issue28934AwaitWaiter(t, waiterQueued,
				"orphan writer did not enter the ALTER lock wait queue")
			select {
			case err := <-writerDone:
				writerFinished = true
				require.Failf(t, "writer returned before ALTER committed", "error: %v", err)
			default:
			}

			require.NoError(t, issue28934Exec(ctx, ddlConn, "commit"))
			ddlOpen = false
			writerErr := issue28934AwaitStatement(t, writerDone,
				"orphan writer did not return after ALTER committed")
			writerFinished = true
			issue28934RequireForeignKeyError(t, writerErr)

			issue28934AssertMetadata(t, ctx, fixture, true)
			var parentID int
			require.NoError(t, writerConn.QueryRowContext(ctx,
				"select parent_id from child where id = 1").Scan(&parentID))
			require.Equal(t, 1, parentID, "rejected orphan update must not change the child")
			_, err := writerConn.ExecContext(ctx, "delete from parent where id = 1")
			issue28934RequireForeignKeyError(t, err)
		})

		t.Run("parent delete commits before alter validation", func(t *testing.T) {
			fixture := newFixture(t)
			issue28934RunWriterFirst(t, ctx, fixture,
				"delete from parent where id = 1", fixture.parentID)

			verifier := issue28934Conn(t, ctx, fixture.writerDB, fixture.database)
			var rows int
			require.NoError(t, verifier.QueryRowContext(ctx,
				"select count(*) from parent").Scan(&rows))
			require.Zero(t, rows, "the committed parent delete must be the state rejected by ALTER")
		})

		t.Run("lock timeout rolls back and retry succeeds", func(t *testing.T) {
			fixture := newFixture(t)
			writerConn := issue28934Conn(t, ctx, fixture.writerDB, fixture.database)
			ddlConn := issue28934Conn(t, ctx, fixture.ddlDB, fixture.database)
			require.NoError(t, issue28934Exec(ctx, ddlConn, "set session lock_wait_timeout = 1"))

			require.NoError(t, issue28934Exec(ctx, writerConn, "begin"))
			writerOpen := true
			defer func() {
				if writerOpen {
					issue28934Rollback(writerConn)
				}
			}()
			require.NoError(t, issue28934Exec(ctx, writerConn,
				"update child set parent_id = 2 where id = 1"))
			writerTxnID := issue28934RequireExclusiveHolder(t, fixture.lockServices, fixture.childID)
			waiterQueued, restoreHook := issue28934InstallWaiterBarrier(writerTxnID, fixture.childID)
			defer restoreHook()

			alterCtx, cancelAlter := context.WithCancel(ctx)
			alterDone := make(chan error, 1)
			alterFinished := false
			go func() {
				alterDone <- issue28934Exec(alterCtx, ddlConn, issue28934AddForeignKeySQL)
			}()
			defer func() {
				cancelAlter()
				if writerOpen {
					issue28934Rollback(writerConn)
					writerOpen = false
				}
				if !alterFinished {
					issue28934Drain(t, alterDone)
				}
			}()

			waiter := issue28934AwaitWaiter(t, waiterQueued,
				"ADD FOREIGN KEY did not enter the writer's lock wait queue")
			alterErr := issue28934AwaitStatement(t, alterDone,
				"ADD FOREIGN KEY did not return after its server-side lock timeout")
			alterFinished = true
			require.ErrorContains(t, alterErr, "Lock wait timeout exceeded")
			require.Eventually(t, func() bool {
				return !issue28934TxnInLockService(fixture.lockServices, waiter.txnID)
			}, 15*time.Second, 10*time.Millisecond,
				"timed-out ALTER transaction retained a lock or waiter")
			issue28934AssertMetadata(t, ctx, fixture, false)

			require.NoError(t, issue28934Exec(ctx, writerConn, "rollback"))
			writerOpen = false
			// Reuse the timed-out connection. Success proves both its transaction
			// state and the lock-service waiter were fully cleaned up.
			require.NoError(t, issue28934Exec(ctx, ddlConn, issue28934AddForeignKeySQL))
			issue28934AssertMetadata(t, ctx, fixture, true)
			_, err := writerConn.ExecContext(ctx,
				"update child set parent_id = 2 where id = 1")
			issue28934RequireForeignKeyError(t, err)
		})
	})
}

func issue28934RunWriterFirst(
	t *testing.T,
	ctx context.Context,
	fixture issue28934Fixture,
	writerSQL string,
	targetTableID uint64,
) {
	t.Helper()
	writerConn := issue28934Conn(t, ctx, fixture.writerDB, fixture.database)
	ddlConn := issue28934Conn(t, ctx, fixture.ddlDB, fixture.database)
	require.NoError(t, issue28934Exec(ctx, writerConn, "begin"))
	writerOpen := true
	defer func() {
		if writerOpen {
			issue28934Rollback(writerConn)
		}
	}()
	require.NoError(t, issue28934Exec(ctx, writerConn, writerSQL))
	writerTxnID := issue28934RequireExclusiveHolder(t, fixture.lockServices, targetTableID)
	waiterQueued, restoreHook := issue28934InstallWaiterBarrier(writerTxnID, targetTableID)
	defer restoreHook()

	alterCtx, cancelAlter := context.WithCancel(ctx)
	alterDone := make(chan error, 1)
	alterFinished := false
	go func() {
		alterDone <- issue28934Exec(alterCtx, ddlConn, issue28934AddForeignKeySQL)
	}()
	defer func() {
		cancelAlter()
		if writerOpen {
			issue28934Rollback(writerConn)
			writerOpen = false
		}
		if !alterFinished {
			issue28934Drain(t, alterDone)
		}
	}()

	issue28934AwaitWaiter(t, waiterQueued,
		"ADD FOREIGN KEY did not enter the writer's lock wait queue")
	select {
	case err := <-alterDone:
		alterFinished = true
		require.Failf(t, "ALTER returned before writer committed", "error: %v", err)
	default:
	}

	require.NoError(t, issue28934Exec(ctx, writerConn, "commit"))
	writerOpen = false
	alterErr := issue28934AwaitStatement(t, alterDone,
		"ADD FOREIGN KEY did not return after writer committed")
	alterFinished = true
	issue28934RequireForeignKeyError(t, alterErr)
	issue28934AssertMetadata(t, ctx, fixture, false)
}

func issue28934OpenDB(t *testing.T, cn embed.ServiceOperator) *sql.DB {
	t.Helper()
	port := cn.GetServiceConfig().CN.Frontend.Port
	db, err := sql.Open("mysql", fmt.Sprintf("dump:111@tcp(127.0.0.1:%d)/", port))
	require.NoError(t, err)
	db.SetMaxOpenConns(8)
	t.Cleanup(func() { require.NoError(t, db.Close()) })
	return db
}

func issue28934Conn(t *testing.T, ctx context.Context, db *sql.DB, database string) *sql.Conn {
	t.Helper()
	conn, err := db.Conn(ctx)
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, conn.Close()) })
	require.NoError(t, issue28934Exec(ctx, conn, "use `"+database+"`"))
	return conn
}

func issue28934Exec(ctx context.Context, conn *sql.Conn, statement string) error {
	_, err := conn.ExecContext(ctx, statement)
	return err
}

func issue28934Rollback(conn *sql.Conn) {
	cleanupCtx, cleanupCancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cleanupCancel()
	_, _ = conn.ExecContext(cleanupCtx, "rollback")
}

func issue28934Drain(t *testing.T, done <-chan error) {
	t.Helper()
	select {
	case <-done:
	case <-time.After(10 * time.Second):
		t.Error("concurrent SQL did not stop during cleanup")
	}
}

func issue28934AwaitStatement(t *testing.T, done <-chan error, message string) error {
	t.Helper()
	select {
	case err := <-done:
		return err
	case <-time.After(30 * time.Second):
		t.Fatal(message)
		return nil
	}
}

func issue28934InstallWaiterBarrier(
	holderTxnID []byte,
	targetTableID uint64,
) (<-chan issue28934Waiter, func()) {
	waiterQueued := make(chan issue28934Waiter, 1)
	var once sync.Once
	restore := lockservice.SetWaiterEnqueuedHookForTest(func(
		tableID uint64, waiterTxnID []byte, holderTxnIDs [][]byte,
	) {
		if tableID != catalog.MO_TABLES_ID && tableID != targetTableID {
			return
		}
		for _, candidate := range holderTxnIDs {
			if bytes.Equal(candidate, holderTxnID) {
				once.Do(func() {
					waiterQueued <- issue28934Waiter{
						txnID: bytes.Clone(waiterTxnID),
					}
				})
				return
			}
		}
	})
	return waiterQueued, restore
}

func issue28934AwaitWaiter(
	t *testing.T,
	waiterQueued <-chan issue28934Waiter,
	message string,
) issue28934Waiter {
	t.Helper()
	select {
	case waiter := <-waiterQueued:
		require.NotEmpty(t, waiter.txnID)
		return waiter
	case <-time.After(30 * time.Second):
		t.Fatal(message)
		return issue28934Waiter{}
	}
}

func issue28934LockServices(c embed.Cluster) []lockservice.LockService {
	var services []lockservice.LockService
	c.ForeachServices(func(service embed.ServiceOperator) bool {
		if service.ServiceType() == metadata.ServiceType_CN {
			services = append(services, lockservice.GetLockServiceByServiceID(service.ServiceID()))
		}
		return true
	})
	return services
}

func issue28934RequireExclusiveHolder(
	t *testing.T,
	services []lockservice.LockService,
	tableID uint64,
) []byte {
	t.Helper()
	var txnID []byte
	require.Eventually(t, func() bool {
		txnID = issue28934ExclusiveHolder(services, tableID)
		return len(txnID) > 0
	}, 15*time.Second, 10*time.Millisecond, "transaction did not retain the target-table lock")
	return txnID
}

func issue28934ExclusiveHolder(services []lockservice.LockService, tableID uint64) []byte {
	holders := make(map[string][]byte)
	for _, service := range services {
		service.IterLocks(func(lockedTableID uint64, _ [][]byte, lock lockservice.Lock) bool {
			if lockedTableID != tableID || lock.GetLockMode() != pblock.LockMode_Exclusive {
				return true
			}
			lock.IterHolders(func(holder pblock.WaitTxn) bool {
				holders[string(holder.TxnID)] = bytes.Clone(holder.TxnID)
				return true
			})
			return true
		})
	}
	if len(holders) != 1 {
		return nil
	}
	for _, txnID := range holders {
		return txnID
	}
	return nil
}

func issue28934TxnInLockService(services []lockservice.LockService, txnID []byte) bool {
	found := false
	for _, service := range services {
		service.IterLocks(func(_ uint64, _ [][]byte, lock lockservice.Lock) bool {
			lock.IterHolders(func(holder pblock.WaitTxn) bool {
				if bytes.Equal(holder.TxnID, txnID) {
					found = true
					return false
				}
				return true
			})
			if found {
				return false
			}
			lock.IterWaiters(func(waiter pblock.WaitTxn) bool {
				if bytes.Equal(waiter.TxnID, txnID) {
					found = true
					return false
				}
				return true
			})
			return !found
		})
		if found {
			return true
		}
	}
	return false
}

func issue28934ReadMetadata(
	ctx context.Context,
	cn embed.ServiceOperator,
	database string,
) (issue28934MetadataState, error) {
	var state issue28934MetadataState
	service, ok := cn.RawService().(cnservice.Service)
	if !ok {
		return state, fmt.Errorf("unexpected CN service type %T", cn.RawService())
	}
	lookupCtx := defines.AttachAccountId(ctx, 0)
	err := service.GetSQLExecutor().ExecTxn(lookupCtx, func(txn executor.TxnExecutor) error {
		db, err := service.GetEngine().Database(lookupCtx, database, txn.Txn())
		if err != nil {
			return err
		}
		child, err := db.Relation(lookupCtx, "child", nil)
		if err != nil {
			return err
		}
		parent, err := db.Relation(lookupCtx, "parent", nil)
		if err != nil {
			return err
		}
		childDef := child.GetTableDef(lookupCtx)
		parentDef := parent.GetTableDef(lookupCtx)
		if childDef == nil || parentDef == nil {
			return fmt.Errorf("missing parent or child table definition")
		}
		state.childID = child.GetTableID(lookupCtx)
		state.childFKs = len(childDef.Fkeys)
		state.parentChildren = append([]uint64(nil), parentDef.RefChildTbls...)
		return nil
	}, executor.Options{}.WithAccountID(0))
	return state, err
}

func issue28934AssertMetadata(
	t *testing.T,
	ctx context.Context,
	fixture issue28934Fixture,
	wantForeignKey bool,
) {
	t.Helper()
	readCatalog := func(db *sql.DB) (int, error) {
		var count int
		err := db.QueryRowContext(ctx,
			"select count(*) from mo_catalog.mo_foreign_keys "+
				"where db_name = ? and table_name = 'child' and constraint_name = 'fk_parent'",
			fixture.database).Scan(&count)
		return count, err
	}
	matches := func() bool {
		writerCount, err := readCatalog(fixture.writerDB)
		if err != nil {
			return false
		}
		ddlCount, err := readCatalog(fixture.ddlDB)
		if err != nil {
			return false
		}
		state, err := issue28934ReadMetadata(ctx, fixture.metadataCN, fixture.database)
		if err != nil || state.childID != fixture.childID {
			return false
		}
		if wantForeignKey {
			return writerCount == 1 && ddlCount == 1 && state.childFKs == 1 &&
				len(state.parentChildren) == 1 && state.parentChildren[0] == fixture.childID
		}
		return writerCount == 0 && ddlCount == 0 && state.childFKs == 0 &&
			len(state.parentChildren) == 0
	}
	if wantForeignKey {
		require.Eventually(t, matches, 30*time.Second, 10*time.Millisecond,
			"committed FK metadata did not become consistent across CNs")
	} else {
		require.True(t, matches(), "failed ALTER left FK catalog or parent RefChildTbls metadata")
	}
}

func issue28934RequireForeignKeyError(t *testing.T, err error) {
	t.Helper()
	require.Error(t, err)
	require.Contains(t, strings.ToLower(err.Error()), "foreign key constraint fails")
}
