// Copyright 2026 Matrix Origin
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package cnservice

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	_ "github.com/go-sql-driver/mysql"
	"github.com/golang/mock/gomock"
	"github.com/matrixorigin/matrixone/pkg/bootstrap"
	"github.com/matrixorigin/matrixone/pkg/bootstrap/versions"
	"github.com/matrixorigin/matrixone/pkg/common/mpool"
	moruntime "github.com/matrixorigin/matrixone/pkg/common/runtime"
	mo_config "github.com/matrixorigin/matrixone/pkg/config"
	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/frontend"
	mock_frontend "github.com/matrixorigin/matrixone/pkg/frontend/test"
	"github.com/matrixorigin/matrixone/pkg/queryservice"
	"github.com/matrixorigin/matrixone/pkg/txn/clock"
	"github.com/matrixorigin/matrixone/pkg/util/executor"
	"github.com/stretchr/testify/require"
)

type tenantUpgradeRequestKey struct{}

// Keep cluster preparation incomplete so cancellation occurs in bootstrap's
// real wait loop with a transaction active. Session/CN wrappers are not mocked.
type cancellationUpgradeExecutor struct {
	executor.SQLExecutor
	txn          *mock_frontend.MockTxnOperator
	entered      chan context.Context
	abort        chan struct{}
	current      atomic.Bool
	active       atomic.Bool
	transactions atomic.Int32
	result       func(string) executor.Result
}

func (e *cancellationUpgradeExecutor) ExecTxn(
	ctx context.Context, fn func(executor.TxnExecutor) error, _ executor.Options,
) error {
	e.transactions.Add(1)
	e.active.Store(true)
	defer e.active.Store(false)
	txn := executor.NewMemTxnExecutor(func(sql string) (executor.Result, error) {
		switch sql {
		case "select create_version from mo_account where account_id = 11":
			version := "4.0.7"
			if e.current.Load() {
				version = "4.0.9"
			}
			return e.result(version), nil
		case "select version, version_offset, state from mo_version order by create_at desc limit 1":
			return e.result("cluster"), nil
		default:
			if !strings.Contains(sql, "from mo_upgrade") {
				return executor.Result{}, fmt.Errorf("unexpected SQL: %s", sql)
			}
			select {
			case e.entered <- ctx:
			default:
			}
			select {
			case <-e.abort:
				return executor.Result{}, context.Canceled
			default:
				return e.result("route"), nil
			}
		}
	}, e.txn)
	if err := fn(txn); err != nil {
		return errors.Join(err, e.txn.Rollback(context.WithoutCancel(ctx)))
	}
	return e.txn.Commit(ctx)
}

func TestSessionTenantUpgradeCancellationReleasesCNConsumer(t *testing.T) {
	sid := "session-tenant-upgrade-cancellation"
	moruntime.RunTest(sid, func(moruntime.Runtime) {
		ctx, cancel := context.WithDeadline(
			context.WithValue(t.Context(), tenantUpgradeRequestKey{}, "login-request"),
			time.Now().Add(20*time.Second))
		defer cancel()
		txn := mock_frontend.NewMockTxnOperator(gomock.NewController(t))
		txn.EXPECT().Rollback(gomock.Any()).Return(nil).Times(1)
		txn.EXPECT().Commit(gomock.Any()).Return(nil).Times(1)
		exec := &cancellationUpgradeExecutor{
			txn: txn, entered: make(chan context.Context, 1), abort: make(chan struct{}),
			result: func(kind string) executor.Result {
				switch kind {
				case "cluster":
					return tenantUpgradeSQLResult(t, "4.0.9", uint32(1), int32(versions.StateCreated))
				case "route":
					return tenantUpgradeSQLResult(t, uint64(100), "4.0.7", "4.0.9", "4.0.9", uint32(1),
						int32(versions.StateCreated), int32(0), int32(versions.No), int32(versions.Yes), int32(1), int32(0))
				default:
					return tenantUpgradeSQLResult(t, kind)
				}
			},
		}
		b := bootstrap.NewService(sid, nil, clock.NewHLCClock(func() int64 { return 0 }, 0), nil, exec)
		defer b.Close()
		cfg := new(Config)
		cfg.UUID = sid
		s := &service{cfg: cfg, bootstrapService: b, sessionMgr: queryservice.NewSessionManager()}
		ses := newTenantUpgradeTestSession(t, s)
		done := make(chan error, 1)
		exited := make(chan struct{})
		go func() {
			defer close(exited)
			done <- ses.MaybeUpgradeTenant(ctx, "4.0.7", 11)
		}()
		// Abort and join the worker even if the regression fails on the old wrapper.
		defer func() {
			close(exec.abort)
			select {
			case <-exited:
			case <-time.After(10 * time.Second):
				t.Error("compensation worker did not finish during cleanup")
			}
		}()
		var child context.Context
		select {
		case child = <-exec.entered:
		case <-time.After(10 * time.Second):
			t.Fatal("compensation did not reach the cluster preparation wait")
		}
		require.True(t, exec.active.Load())
		cancel()
		select {
		case err := <-done:
			require.ErrorIs(t, err, context.Canceled)
		case <-time.After(10 * time.Second):
			t.Fatal("caller cancellation did not cross the Session/CN boundary")
		}
		require.Equal(t, "login-request", child.Value(tenantUpgradeRequestKey{}))
		parentDeadline, _ := ctx.Deadline()
		childDeadline, _ := child.Deadline()
		require.Equal(t, parentDeadline, childDeadline)
		require.False(t, exec.active.Load(), "the SQL transaction must be rolled back before return")

		// A subsequent successful check must execute SQL: cancellation must not
		// have populated bootstrap's checked-tenant cache. Only that check is cached.
		exec.current.Store(true)
		require.NoError(t, ses.MaybeUpgradeTenant(t.Context(), "4.0.7", 11))
		require.Equal(t, int32(2), exec.transactions.Load())
		require.NoError(t, ses.MaybeUpgradeTenant(t.Context(), "4.0.7", 11))
		require.Equal(t, int32(2), exec.transactions.Load())
		closed := make(chan error, 1)
		go func() { closed <- s.closeBootstrapService() }()
		select {
		case err := <-closed:
			require.NoError(t, err)
		case <-time.After(10 * time.Second):
			t.Fatal("cancelled login retained bootstrapMu and blocked shutdown")
		}
	})
}

func tenantUpgradeSQLResult(t *testing.T, values ...any) executor.Result {
	t.Helper()
	columnTypes := make([]types.Type, len(values))
	for i, value := range values {
		switch value.(type) {
		case string:
			columnTypes[i] = types.T_varchar.ToType()
		case uint64:
			columnTypes[i] = types.T_uint64.ToType()
		case uint32:
			columnTypes[i] = types.T_uint32.ToType()
		case int32:
			columnTypes[i] = types.T_int32.ToType()
		}
	}
	mp := mpool.MustNewZero()
	t.Cleanup(func() { mpool.DeleteMPool(mp) })
	res := executor.NewMemResult(columnTypes, mp)
	res.NewBatchWithRowCount(1)
	for i, value := range values {
		var err error
		switch value := value.(type) {
		case string:
			err = executor.AppendStringRows(res, i, []string{value})
		case uint64:
			err = executor.AppendFixedRows(res, i, []uint64{value})
		case uint32:
			err = executor.AppendFixedRows(res, i, []uint32{value})
		case int32:
			err = executor.AppendFixedRows(res, i, []int32{value})
		}
		require.NoError(t, err)
	}
	return res.GetResult()
}

func newTenantUpgradeTestSession(t *testing.T, s *service) *frontend.Session {
	t.Helper()
	// A short Unix socket path avoids OS path limits and ephemeral-port races.
	dir, err := os.MkdirTemp("/tmp", "cn-upgrade-")
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, os.Remove(dir)) })
	pu := mo_config.NewParameterUnit(&mo_config.FrontendParameters{}, nil, nil, nil)
	pu.SV.SetDefaultValues()
	pu.SV.SkipCheckUser = true // only session construction; compensation uses real wrappers
	pu.SV.KillRountinesInterval = 0
	pu.SV.UnixSocketAddress = filepath.Join(dir, "mysql.sock")
	mo := frontend.NewMOServer(t.Context(), "127.0.0.1:0", pu, nil, s)
	t.Cleanup(func() { require.NoError(t, mo.Stop()) })
	require.NoError(t, mo.Start())
	db, err := sql.Open("mysql", fmt.Sprintf("root:@unix(%s)/", pu.SV.UnixSocketAddress))
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, db.Close()) })
	ctx, cancel := context.WithTimeout(t.Context(), 10*time.Second)
	defer cancel()
	conn, err := db.Conn(ctx) // authenticate without issuing a SQL query
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, conn.Close()) })
	var ses *frontend.Session
	require.Eventually(t, func() bool {
		all := s.sessionMgr.GetAllSessions()
		if len(all) != 1 {
			return false
		}
		ses = all[0].(*frontend.Session)
		return true
	}, 10*time.Second, 10*time.Millisecond)
	return ses
}
