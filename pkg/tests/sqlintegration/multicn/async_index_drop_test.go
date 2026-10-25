// Copyright 2026 Matrix Origin
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
// http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package multicn

import (
	"context"
	"database/sql"
	"fmt"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	moruntime "github.com/matrixorigin/matrixone/pkg/common/runtime"
	"github.com/matrixorigin/matrixone/pkg/embed"
	"github.com/matrixorigin/matrixone/pkg/txn/client"
	"github.com/matrixorigin/matrixone/pkg/util/executor"
	"github.com/stretchr/testify/require"
)

type observedInternalExecutor struct {
	executor.SQLExecutor
	after func(context.Context, string, executor.Options, error)
}

func (e *observedInternalExecutor) Exec(ctx context.Context, statement string, opts executor.Options) (executor.Result, error) {
	result, err := e.SQLExecutor.Exec(ctx, statement, opts)
	e.after(ctx, statement, opts, err)
	return result, err
}

// Hold the real async clone initializer after REINDEX but before its commit.
// Release it only when the competing account-drop transaction rolls back, so
// the test exercises admission contention without fabricating a SQL error.
func TestDropAccountDuringAsyncIndexInit(t *testing.T) {
	cluster, err := embed.StartTestCluster(embed.WithCNCount(2))
	if cluster != nil {
		t.Cleanup(func() { require.NoError(t, cluster.Close()) })
	}
	require.NoError(t, err)
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Minute)
	defer cancel()
	open := func(cn int, user string) *sql.DB {
		t.Helper()
		svc, err := cluster.GetCNService(cn)
		require.NoError(t, err)
		db, err := sql.Open("mysql", fmt.Sprintf("%s:111@tcp(127.0.0.1:%d)/", user, svc.GetServiceConfig().CN.Frontend.Port))
		require.NoError(t, err)
		t.Cleanup(func() { require.NoError(t, db.Close()) })
		return db
	}
	admins := []*sql.DB{open(0, "dump"), open(1, "dump")}
	_, err = admins[0].ExecContext(ctx, "create account async_drop admin_name='admin' identified by '111'")
	require.NoError(t, err)
	var accountID uint32
	require.NoError(t, admins[0].QueryRowContext(ctx, "select account_id from mo_catalog.mo_account where account_name='async_drop'").Scan(&accountID))
	tenant := open(0, "async_drop#admin")
	conn, err := tenant.Conn(ctx)
	require.NoError(t, err)
	defer conn.Close()
	for _, statement := range []string{
		"set experimental_hnsw_index=1", "create database init_source", "use init_source",
		"create table vectors(id bigint primary key, v vecf64(2))",
		"insert into vectors values(1,'[1,0]')",
		"create index hx using hnsw on vectors(v) op_type 'vector_l2_ops' m=8 ef_construction=64 ef_search=64",
	} {
		_, err = conn.ExecContext(ctx, statement)
		require.NoError(t, err, statement)
	}
	blocked := make(chan int, 1)
	release := make(chan struct{})
	var releaseOnce sync.Once
	unblock := func() { releaseOnce.Do(func() { close(release) }) }
	// Registered after cluster cleanup: a failed assertion must not strand init.
	t.Cleanup(unblock)
	var dropping atomic.Bool
	var initSeen atomic.Bool
	var observed sync.Map
	var rollbacks atomic.Int32
	for i := 0; i < 2; i++ {
		svc, err := cluster.GetCNService(i)
		require.NoError(t, err)
		rt := moruntime.ServiceRuntime(svc.ServiceID())
		original, ok := rt.GetGlobalVariables(moruntime.InternalSQLExecutor)
		require.True(t, ok)
		t.Cleanup(func() { rt.SetGlobalVariables(moruntime.InternalSQLExecutor, original) })
		rt.SetGlobalVariables(moruntime.InternalSQLExecutor, &observedInternalExecutor{
			SQLExecutor: original.(executor.SQLExecutor),
			after: func(callCtx context.Context, statement string, opts executor.Options, execErr error) {
				lower := strings.ToLower(statement)
				if execErr == nil && strings.Contains(lower, "init_clone") && strings.Contains(lower, "alter reindex") && initSeen.CompareAndSwap(false, true) {
					t.Logf("held async init txn=%x CN=%d SQL=%s", opts.Txn().Txn().ID, i, statement)
					blocked <- i
					select {
					case <-release:
					case <-callCtx.Done():
					}
				}
				if dropping.Load() && opts.Txn() != nil && strings.Contains(lower, "select 1 from mo_catalog.mo_branch_metadata") {
					id := string(opts.Txn().Txn().ID)
					if _, loaded := observed.LoadOrStore(id, true); !loaded {
						opts.Txn().AppendEventCallback(client.RollbackEvent, client.NewTxnEventCallback(
							func(_ context.Context, _ client.TxnOperator, event client.TxnEvent, _ any) error {
								if event.CostEvent {
									t.Logf("DROP rollback txn=%x err=%v", event.Txn.ID, event.Err)
									rollbacks.Add(1)
									unblock()
								}
								return nil
							}))
					}
				}
			},
		})
	}
	_, err = conn.ExecContext(ctx, "create database init_clone clone init_source")
	require.NoError(t, err)
	var initializerCN int
	select {
	case initializerCN = <-blocked:
	case <-ctx.Done():
		t.Fatal("async index initializer was not observed", ctx.Err())
	}
	dropping.Store(true)
	_, err = admins[1-initializerCN].ExecContext(ctx, "drop account async_drop")
	require.NoError(t, err)
	require.Positive(t, rollbacks.Load(), "a real admission conflict must precede success")
	var accounts int
	require.NoError(t, admins[0].QueryRowContext(ctx, "select count(*) from mo_catalog.mo_account where account_name='async_drop'").Scan(&accounts))
	require.Zero(t, accounts)
	for _, statement := range []string{
		fmt.Sprintf("select count(*) from mo_catalog.mo_tables where account_id=%d", accountID),
		fmt.Sprintf("select count(*) from mo_catalog.mo_iscp_log where account_id=%d and drop_at is null", accountID),
	} {
		require.NoError(t, admins[0].QueryRowContext(ctx, statement).Scan(&accounts))
		require.Zero(t, accounts, statement)
	}
}
