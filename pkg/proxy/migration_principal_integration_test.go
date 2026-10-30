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

package proxy_test

import (
	"context"
	"database/sql"
	"fmt"
	"testing"
	"time"

	_ "github.com/go-sql-driver/mysql"
	"github.com/matrixorigin/matrixone/pkg/common/runtime"
	"github.com/matrixorigin/matrixone/pkg/embed"
	"github.com/matrixorigin/matrixone/pkg/proxy"
	"github.com/stretchr/testify/require"
)

// One existing two-CN fixture proves the wire/handshake/tunnel boundary that
// component mocks cannot: equal credentials are not equal catalog identities.
func TestRenameUserProxyMigrationPrincipal(t *testing.T) {
	defer func() { require.NoError(t, embed.CloseBaseClusterTests()) }()
	embed.RunBaseClusterTests(t, func(cluster embed.Cluster) {
		ctx, cancel := context.WithTimeout(t.Context(), 180*time.Second)
		defer cancel()
		cn, err := cluster.GetCNService(0)
		require.NoError(t, err)
		cfg := cn.GetServiceConfig()
		var cleanup []func()
		defer func() {
			for i := len(cleanup) - 1; i >= 0; i-- {
				cleanup[i]()
			}
		}()
		connect := func(user, address string) *sql.Conn {
			t.Helper()
			db, err := sql.Open("mysql", fmt.Sprintf("%s@tcp(%s)/", user, address))
			require.NoError(t, err)
			cleanup = append(cleanup, func() { _ = db.Close() })
			conn, err := db.Conn(ctx)
			require.NoError(t, err)
			cleanup = append(cleanup, func() { _ = conn.Close() })
			return conn
		}
		admin := connect("dump:111", fmt.Sprintf("127.0.0.1:%d", cfg.CN.Frontend.Port))
		exec := func(q string) { t.Helper(); _, err := admin.ExecContext(ctx, q); require.NoError(t, err, q) }
		exec("create database migration_identity")
		defer func() {
			_, _ = admin.ExecContext(context.Background(), "drop user if exists migration_a,migration_b,migration_moved")
			_, _ = admin.ExecContext(context.Background(), "drop role if exists migration_ra,migration_rb")
			_, _ = admin.ExecContext(context.Background(), "drop database if exists migration_identity")
		}()
		exec("create table migration_identity.a(v int)")
		exec("create table migration_identity.b(v int)")
		exec("insert into migration_identity.a values(8)")
		exec("insert into migration_identity.b values(9)")
		exec("create role migration_ra,migration_rb")
		exec("grant connect on account * to migration_ra,migration_rb")
		exec("grant select on table migration_identity.a to migration_ra")
		exec("grant select on table migration_identity.b to migration_rb")
		exec("create user migration_a identified by 'shared-password',migration_b identified by 'shared-password'")
		exec("grant migration_ra to migration_a")
		exec("grant migration_rb to migration_b")
		exec("set default role migration_ra to migration_a")
		exec("set default role migration_rb to migration_b")
		var aID, bID uint32
		require.NoError(t, admin.QueryRowContext(ctx, "select user_id from mo_catalog.mo_user where user_name='migration_a'").Scan(&aID))
		require.NoError(t, admin.QueryRowContext(ctx, "select user_id from mo_catalog.mo_user where user_name='migration_b'").Scan(&bID))
		require.NotEqual(t, aID, bID)
		pc := proxy.Config{UUID: "migration-principal-proxy", ListenAddress: "127.0.0.1:0", RebalanceDisabled: true}
		pc.HAKeeper.ClientConfig = cfg.HAKeeperClient
		runtime.SetupServiceBasedRuntime(pc.UUID, runtime.DefaultRuntime())
		server, err := proxy.NewServer(ctx, pc, proxy.WithRuntime(runtime.ServiceRuntime(pc.UUID)))
		require.NoError(t, err)
		defer func() { require.NoError(t, server.Close()) }()
		require.NoError(t, server.Start())
		old := connect("migration_a:shared-password", proxy.MigrationTestAddress(server))
		var connID, userID uint32
		var value int
		checkA := func() {
			t.Helper()
			require.NoError(t, old.QueryRowContext(ctx, "select v from migration_identity.a").Scan(&value))
			require.Equal(t, 8, value)
			require.NoError(t, old.QueryRowContext(ctx, "select current_user_id()").Scan(&userID))
			require.Equal(t, aID, userID)
			require.Error(t, old.QueryRowContext(ctx, "select v from migration_identity.b").Scan(&value))
		}
		checkA()
		require.NoError(t, old.QueryRowContext(ctx, "select connection_id()").Scan(&connID))
		canceled, cancelTransfer := context.WithCancel(ctx)
		cancelTransfer()
		before, after, err := proxy.MigrationTestTransfer(canceled, server, connID)
		require.Error(t, err)
		require.Equal(t, before, after)
		checkA()
		before, after, err = proxy.MigrationTestTransfer(ctx, server, connID)
		require.NoError(t, err)
		require.NotEqual(t, before, after)
		checkA()
		exec("rename user migration_a to migration_moved,migration_b to migration_a")
		for i := 0; i < 2; i++ {
			before, after, err = proxy.MigrationTestTransfer(ctx, server, connID)
			require.Error(t, err)
			require.Equal(t, before, after)
			checkA()
		}
		fresh := connect("migration_a:shared-password", proxy.MigrationTestAddress(server))
		require.NoError(t, fresh.QueryRowContext(ctx, "select v from migration_identity.b").Scan(&value))
		require.Equal(t, 9, value)
		require.NoError(t, fresh.QueryRowContext(ctx, "select current_user_id()").Scan(&userID))
		require.Equal(t, bID, userID)
		require.Error(t, fresh.QueryRowContext(ctx, "select v from migration_identity.a").Scan(&value))
		var freshID uint32
		require.NoError(t, fresh.QueryRowContext(ctx, "select connection_id()").Scan(&freshID))
		checkB := func() {
			t.Helper()
			require.NoError(t, fresh.QueryRowContext(ctx, "select v from migration_identity.b").Scan(&value))
			require.Equal(t, 9, value)
			require.NoError(t, fresh.QueryRowContext(ctx, "select current_user_id()").Scan(&userID))
			require.Equal(t, bID, userID)
			require.Error(t, fresh.QueryRowContext(ctx, "select v from migration_identity.a").Scan(&value))
		}
		// These controls fail at authentication, not at the same-password ID guard.
		exec("rename user migration_a to migration_b")
		before, after, err = proxy.MigrationTestTransfer(ctx, server, freshID)
		require.Error(t, err)
		require.Equal(t, before, after)
		checkB()
		exec("create user migration_a identified by 'different-password'")
		before, after, err = proxy.MigrationTestTransfer(ctx, server, freshID)
		require.Error(t, err)
		require.Equal(t, before, after)
		checkB()

	})
}
