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
	"testing"
	"time"

	_ "github.com/go-sql-driver/mysql"
	"github.com/matrixorigin/matrixone/pkg/embed"
	"github.com/matrixorigin/matrixone/pkg/lockservice"
	lockpb "github.com/matrixorigin/matrixone/pkg/pb/lock"
	"github.com/matrixorigin/matrixone/pkg/pb/metadata"
	"github.com/stretchr/testify/require"
)

func TestIssue26404MixedDataBranchLifecycleDoesNotDeadlock(t *testing.T) {
	runAuthenticatedClusterTest(t, func(c embed.Cluster) {
		ctx, cancel := context.WithTimeout(context.Background(), 3*time.Minute)
		defer cancel()

		cn0, err := c.GetCNService(0)
		require.NoError(t, err)
		cn1, err := c.GetCNService(1)
		require.NoError(t, err)
		openDB := func(port int64) *sql.DB {
			db, openErr := sql.Open("mysql", fmt.Sprintf("dump:111@tcp(127.0.0.1:%d)/", port))
			require.NoError(t, openErr)
			db.SetMaxOpenConns(1)
			t.Cleanup(func() { require.NoError(t, db.Close()) })
			return db
		}
		pickDB := openDB(cn0.GetServiceConfig().CN.Frontend.Port)
		holderDB := openDB(cn1.GetServiceConfig().CN.Frontend.Port)
		dropDB := openDB(cn1.GetServiceConfig().CN.Frontend.Port)

		t.Run("database clone", func(t *testing.T) {
			runIssue26404CloneLifecycleOrderScenario(t, ctx, c, pickDB, holderDB)
		})
		t.Run("diff", func(t *testing.T) {
			runIssue26404DiffLifecycleOrderScenario(t, ctx, c, pickDB, holderDB)
		})
		for _, operation := range []string{"pick", "merge"} {
			t.Run(operation, func(t *testing.T) {
				runIssue26404LifecycleOrderScenario(
					t, ctx, c, pickDB, holderDB, dropDB, operation,
				)
			})
		}
	})
}

func runIssue26404CloneLifecycleOrderScenario(
	t *testing.T,
	ctx context.Context,
	c embed.Cluster,
	cloneDB *sql.DB,
	holderDB *sql.DB,
) {
	const (
		sourceDB = "issue_26404_clone_source"
		targetDB = "issue_26404_clone_target"
		otherDB  = "issue_26404_clone_unrelated"
	)
	cleanup := func(cleanupCtx context.Context) {
		execSQLMaybe(t, cleanupCtx, cloneDB, "drop database if exists `"+targetDB+"`")
		execSQLMaybe(t, cleanupCtx, cloneDB, "drop database if exists `"+otherDB+"`")
		execSQLMaybe(t, cleanupCtx, cloneDB, "drop database if exists `"+sourceDB+"`")
	}
	cleanup(ctx)
	defer func() {
		cleanupCtx, cleanupCancel := context.WithTimeout(context.Background(), 30*time.Second)
		defer cleanupCancel()
		cleanup(cleanupCtx)
	}()

	execSQLRequire(t, ctx, cloneDB, "create database `"+sourceDB+"`")
	execSQLRequire(t, ctx, cloneDB, "create database `"+otherDB+"`")
	execSQLRequire(t, ctx, cloneDB,
		"create table `"+sourceDB+"`.`docs` (id int primary key, body text)")
	execSQLRequire(t, ctx, cloneDB,
		"insert into `"+sourceDB+"`.`docs` values (1, 'matrixone branch regression')")
	execSQLRequire(t, ctx, cloneDB,
		"create fulltext index `ft_body` on `"+sourceDB+"`.`docs` (`body`)")
	execSQLRequire(t, ctx, cloneDB,
		"create view `"+sourceDB+"`.`docs_view` as select id, body from `"+sourceDB+"`.`docs`")

	holder, featureRegistryID := lockIssue26404LineageGate(t, ctx, cloneDB, holderDB)

	operationCtx, cancelOperation := context.WithCancel(ctx)
	clone := startIssue26404Operation(operationCtx, func(operationCtx context.Context) error {
		_, operationErr := cloneDB.ExecContext(operationCtx,
			"create database `"+targetDB+"` clone `"+sourceDB+"`")
		return operationErr
	})
	defer func() {
		cancelOperation()
		joinCtx, joinCancel := context.WithTimeout(context.Background(), 30*time.Second)
		defer joinCancel()
		if joinErr := clone.join(joinCtx); joinErr != nil {
			t.Errorf("database clone did not stop: %v", joinErr)
		}
	}()
	require.Eventually(t, func() bool {
		return issue26404WaiterCount(c, featureRegistryID) > 0
	}, 30*time.Second, 10*time.Millisecond,
		"database clone did not wait at the lineage gate")
	require.False(t, clone.completed(), "database clone returned before the lineage gate was released")

	// This DROP takes the same global lineage -> catalog path as the mixed
	// workload. It must finish while the clone is still waiting at lineage;
	// a clone that reached catalog first closes the original two-transaction cycle.
	_, err := holder.ExecContext(ctx, "drop database `"+otherDB+"`")
	require.NoError(t, err)
	require.NoError(t, holder.Commit())
	finishCtx, finishCancel := context.WithTimeout(ctx, 30*time.Second)
	defer finishCancel()
	require.NoError(t, clone.wait(finishCtx))

	var clonedRows int
	require.NoError(t, cloneDB.QueryRowContext(ctx,
		"select count(*) from `"+targetDB+"`.`docs_view`").Scan(&clonedRows))
	require.Equal(t, 1, clonedRows)
	var databaseCount int
	require.NoError(t, cloneDB.QueryRowContext(ctx,
		"select count(*) from mo_catalog.mo_database where account_id=0 and datname=?", otherDB,
	).Scan(&databaseCount))
	require.Zero(t, databaseCount)
}

func runIssue26404DiffLifecycleOrderScenario(
	t *testing.T,
	ctx context.Context,
	c embed.Cluster,
	diffDB *sql.DB,
	holderDB *sql.DB,
) {
	const (
		branchDB = "issue_26404_diff_branch"
		otherDB  = "issue_26404_diff_unrelated"
	)
	cleanup := func(cleanupCtx context.Context) {
		execSQLMaybe(t, cleanupCtx, diffDB, "drop database if exists `"+otherDB+"`")
		execSQLMaybe(t, cleanupCtx, diffDB, "drop database if exists `"+branchDB+"`")
	}
	cleanup(ctx)
	defer func() {
		cleanupCtx, cleanupCancel := context.WithTimeout(context.Background(), 30*time.Second)
		defer cleanupCancel()
		cleanup(cleanupCtx)
	}()

	execSQLRequire(t, ctx, diffDB, "create database `"+branchDB+"`")
	execSQLRequire(t, ctx, diffDB, "create database `"+otherDB+"`")
	execSQLRequire(t, ctx, diffDB,
		"create table `"+branchDB+"`.`base` (id int primary key, value int)")
	execSQLRequire(t, ctx, diffDB,
		"insert into `"+branchDB+"`.`base` values (1, 10), (2, 20)")
	execSQLRequire(t, ctx, diffDB,
		"data branch create table `"+branchDB+"`.`child` from `"+branchDB+"`.`base`")
	execSQLRequire(t, ctx, diffDB,
		"update `"+branchDB+"`.`child` set value = 200 where id = 2")

	holder, featureRegistryID := lockIssue26404LineageGate(t, ctx, diffDB, holderDB)

	operationCtx, cancelOperation := context.WithCancel(ctx)
	var diffCount int
	diff := startIssue26404Operation(operationCtx, func(operationCtx context.Context) error {
		return diffDB.QueryRowContext(operationCtx,
			"data branch diff `"+branchDB+"`.`child` against `"+branchDB+"`.`base` output count",
		).Scan(&diffCount)
	})
	defer func() {
		cancelOperation()
		joinCtx, joinCancel := context.WithTimeout(context.Background(), 30*time.Second)
		defer joinCancel()
		if joinErr := diff.join(joinCtx); joinErr != nil {
			t.Errorf("Data Branch DIFF did not stop: %v", joinErr)
		}
	}()
	require.Eventually(t, func() bool {
		return issue26404WaiterCount(c, featureRegistryID) > 0
	}, 30*time.Second, 10*time.Millisecond,
		"Data Branch DIFF did not wait at the lineage gate")
	require.False(t, diff.completed(), "Data Branch DIFF returned before the lineage gate was released")

	_, err := holder.ExecContext(ctx, "drop database `"+otherDB+"`")
	require.NoError(t, err)
	require.NoError(t, holder.Commit())
	finishCtx, finishCancel := context.WithTimeout(ctx, 30*time.Second)
	defer finishCancel()
	require.NoError(t, diff.wait(finishCtx))
	require.Equal(t, 1, diffCount)

	var databaseCount int
	require.NoError(t, diffDB.QueryRowContext(ctx,
		"select count(*) from mo_catalog.mo_database where account_id=0 and datname=?", otherDB,
	).Scan(&databaseCount))
	require.Zero(t, databaseCount)
}

func lockIssue26404LineageGate(
	t *testing.T,
	ctx context.Context,
	lookupDB *sql.DB,
	holderDB *sql.DB,
) (*sql.Tx, uint64) {
	var featureRegistryID uint64
	require.NoError(t, lookupDB.QueryRowContext(ctx,
		"select rel_id from mo_catalog.mo_tables where account_id=0 and reldatabase='mo_catalog' and relname='mo_feature_registry'",
	).Scan(&featureRegistryID))

	holder, err := holderDB.BeginTx(ctx, nil)
	require.NoError(t, err)
	t.Cleanup(func() { _ = holder.Rollback() })
	_, err = holder.ExecContext(ctx,
		"select feature_code from mo_catalog.mo_feature_registry "+
			"where feature_code='SNAPSHOT' for update")
	require.NoError(t, err)
	return holder, featureRegistryID
}

func runIssue26404LifecycleOrderScenario(
	t *testing.T,
	ctx context.Context,
	c embed.Cluster,
	mutationDB *sql.DB,
	holderDB *sql.DB,
	dropDB *sql.DB,
	operation string,
) {
	branchDB := "issue_26404_" + operation + "_branch"
	otherDB := "issue_26404_" + operation + "_unrelated"
	cleanup := func(cleanupCtx context.Context) {
		execSQLMaybe(t, cleanupCtx, mutationDB, "drop database if exists `"+otherDB+"`")
		execSQLMaybe(t, cleanupCtx, mutationDB, "drop database if exists `"+branchDB+"`")
	}
	cleanup(ctx)
	defer func() {
		cleanupCtx, cleanupCancel := context.WithTimeout(context.Background(), 30*time.Second)
		defer cleanupCancel()
		cleanup(cleanupCtx)
	}()

	execSQLRequire(t, ctx, mutationDB, "create database `"+branchDB+"`")
	execSQLRequire(t, ctx, mutationDB, "create database `"+otherDB+"`")
	execSQLRequire(t, ctx, mutationDB,
		"create table `"+branchDB+"`.`base` (id int primary key, value int)")
	execSQLRequire(t, ctx, mutationDB,
		"insert into `"+branchDB+"`.`base` values (1, 10), (2, 20)")
	execSQLRequire(t, ctx, mutationDB,
		"data branch create table `"+branchDB+"`.`dst` from `"+branchDB+"`.`base`")
	execSQLRequire(t, ctx, mutationDB,
		"data branch create table `"+branchDB+"`.`src` from `"+branchDB+"`.`base`")
	execSQLRequire(t, ctx, mutationDB,
		"update `"+branchDB+"`.`src` set value = 200 where id = 2")

	var destinationTableID, featureRegistryID uint64
	require.NoError(t, mutationDB.QueryRowContext(ctx,
		"select rel_id from mo_catalog.mo_tables where account_id=0 and reldatabase=? and relname='dst'",
		branchDB).Scan(&destinationTableID))
	require.NoError(t, mutationDB.QueryRowContext(ctx,
		"select rel_id from mo_catalog.mo_tables where account_id=0 and reldatabase='mo_catalog' and relname='mo_feature_registry'",
	).Scan(&featureRegistryID))

	holder, err := holderDB.BeginTx(ctx, nil)
	require.NoError(t, err)
	holderOpen := true
	defer func() {
		if holderOpen {
			_ = holder.Rollback()
		}
	}()
	_, err = holder.ExecContext(ctx,
		"update `"+branchDB+"`.`dst` set value = 100 where id = 2")
	require.NoError(t, err)

	operationCtx, cancelOperations := context.WithCancel(ctx)
	operations := make([]*issue26404Operation, 0, 2)
	defer func() {
		cancelOperations()
		joinCtx, joinCancel := context.WithTimeout(context.Background(), 30*time.Second)
		defer joinCancel()
		for _, concurrentOperation := range operations {
			if joinErr := concurrentOperation.join(joinCtx); joinErr != nil {
				t.Errorf("concurrent catalog operation did not stop: %v", joinErr)
			}
		}
	}()

	mutationSQL := "data branch merge `" + branchDB + "`.`src` into `" + branchDB + "`.`dst` when conflict accept"
	if operation == "pick" {
		mutationSQL = "data branch pick `" + branchDB + "`.`src` into `" + branchDB + "`.`dst` keys(2) when conflict accept"
	}
	mutation := startIssue26404Operation(operationCtx, func(operationCtx context.Context) error {
		_, operationErr := mutationDB.ExecContext(operationCtx, mutationSQL)
		return operationErr
	})
	operations = append(operations, mutation)
	require.Eventually(t, func() bool {
		return issue26404WaiterCount(c, destinationTableID) > 0
	}, 30*time.Second, 10*time.Millisecond,
		"Data Branch %s did not wait for the destination row holder", operation)
	require.False(t, mutation.completed(),
		"Data Branch %s returned before the row holder committed", operation)

	drop := startIssue26404Operation(operationCtx, func(operationCtx context.Context) error {
		_, operationErr := dropDB.ExecContext(operationCtx, "drop database `"+otherDB+"`")
		return operationErr
	})
	operations = append(operations, drop)
	require.Eventually(t, func() bool {
		return issue26404WaiterCount(c, featureRegistryID) > 0
	}, 30*time.Second, 10*time.Millisecond,
		"unrelated DROP DATABASE did not wait at the lineage gate before taking the view-metadata gate")
	require.False(t, drop.completed(),
		"DROP DATABASE returned before the %s gate was released", operation)

	require.NoError(t, holder.Commit())
	holderOpen = false
	finishCtx, finishCancel := context.WithTimeout(ctx, 30*time.Second)
	defer finishCancel()
	require.NoError(t, mutation.wait(finishCtx))
	require.NoError(t, drop.wait(finishCtx))

	var value int
	require.NoError(t, mutationDB.QueryRowContext(ctx,
		"select value from `"+branchDB+"`.`dst` where id = 2").Scan(&value))
	require.Equal(t, 200, value)
	var databaseCount int
	require.NoError(t, mutationDB.QueryRowContext(ctx,
		"select count(*) from mo_catalog.mo_database where account_id=0 and datname=?", otherDB,
	).Scan(&databaseCount))
	require.Zero(t, databaseCount)
}

type issue26404Operation struct {
	done chan struct{}
	err  error
}

func startIssue26404Operation(ctx context.Context, fn func(context.Context) error) *issue26404Operation {
	operation := &issue26404Operation{done: make(chan struct{})}
	go func() {
		defer close(operation.done)
		operation.err = fn(ctx)
	}()
	return operation
}

func (operation *issue26404Operation) completed() bool {
	select {
	case <-operation.done:
		return true
	default:
		return false
	}
}

func (operation *issue26404Operation) wait(ctx context.Context) error {
	select {
	case <-operation.done:
		return operation.err
	case <-ctx.Done():
		return ctx.Err()
	}
}

func (operation *issue26404Operation) join(ctx context.Context) error {
	select {
	case <-operation.done:
		return nil
	case <-ctx.Done():
		return ctx.Err()
	}
}

func issue26404WaiterCount(c embed.Cluster, tableIDs ...uint64) int {
	waiters := 0
	tableSet := make(map[uint64]struct{}, len(tableIDs))
	for _, tableID := range tableIDs {
		tableSet[tableID] = struct{}{}
	}
	c.ForeachServices(func(svc embed.ServiceOperator) bool {
		if svc.ServiceType() != metadata.ServiceType_CN {
			return true
		}
		lockService := lockservice.GetLockServiceByServiceID(svc.ServiceID())
		lockService.IterLocks(func(tableID uint64, _ [][]byte, lock lockservice.Lock) bool {
			if _, ok := tableSet[tableID]; !ok {
				return true
			}
			lock.IterWaiters(func(_ lockpb.WaitTxn) bool {
				waiters++
				return true
			})
			return true
		})
		return true
	})
	return waiters
}
