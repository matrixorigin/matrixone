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

package isolated

import (
	"context"
	"database/sql"
	"fmt"
	"sync"
	"testing"
	"time"

	"github.com/matrixorigin/matrixone/pkg/cnservice"
	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/embed"
	"github.com/matrixorigin/matrixone/pkg/pb/metadata"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/disttae"
	"github.com/stretchr/testify/require"
)

func TestFullAndLazyCatalogSubscribers(t *testing.T) {
	mode := "lazy"
	cluster, err := embed.StartTestCluster(embed.WithCNCount(1), embed.WithPreStart(func(op embed.ServiceOperator) {
		if op.ServiceType() == metadata.ServiceType_CN {
			op.Adjust(func(c *embed.ServiceConfig) {
				c.CN.Frontend.SkipCheckUser = false
				c.CN.Engine.CatalogSubscriptionMode = mode
			})
		}
	}))
	if cluster != nil {
		t.Cleanup(func() { require.NoError(t, cluster.Close()) })
	}
	require.NoError(t, err)
	ctx, cancel := context.WithTimeout(t.Context(), 2*time.Minute)
	defer cancel()
	open := func(user string, op embed.ServiceOperator) *sql.DB {
		t.Helper()
		db, err := sql.Open("mysql", fmt.Sprintf("%s:111@tcp(127.0.0.1:%d)/", user, op.GetServiceConfig().CN.Frontend.Port))
		require.NoError(t, err)
		t.Cleanup(func() { require.NoError(t, db.Close()) })
		return db
	}
	lazy, err := cluster.GetCNService(0)
	require.NoError(t, err)
	sys := open("dump", lazy)
	require.NoError(t, waitSystemBootstrap(ctx, sys))
	execSQLRequire(t, ctx, sys, "create account mixed_a admin_name 'root' identified by '111'")
	a := open("mixed_a#root#accountadmin", lazy)
	execSQLRequire(t, ctx, a, "create database mixed")
	execSQLRequire(t, ctx, a, "create table mixed.before_start(id int)")
	var owner uint32
	var dbID uint64
	require.NoError(t, a.QueryRowContext(ctx, "select account_id,dat_id from mo_catalog.mo_database where datname='mixed'").Scan(&owner, &dbID))
	mode = "full"
	require.NoError(t, cluster.StartNewCNService(1))
	full, err := cluster.GetCNService(1)
	require.NoError(t, err)
	reader := open("dump", full)
	eng := full.RawService().(cnservice.Service).GetEngine().(*disttae.Engine)
	check := func(name string, want int) {
		t.Helper()
		var count int
		require.NoError(t, reader.QueryRowContext(ctx, "select count(*) from mo_catalog.mo_tables where reldatabase='mixed' and relname=?", name).Scan(&count))
		require.Equal(t, want, count)
		names, _ := eng.GetLatestCatalogCache().Tables(owner, dbID, types.MaxTs().ToTimestamp())
		if want != 0 {
			require.Contains(t, names, name)
		} else {
			require.NotContains(t, names, name)
		}
	}
	check("before_start", 1)
	execSQLRequire(t, ctx, a, "create table mixed.after_start(id int)")
	check("after_start", 1)
	execSQLRequire(t, ctx, a, "drop table mixed.before_start")
	check("before_start", 0)
	execSQLRequire(t, ctx, reader, "create account mixed_b admin_name 'root' identified by '111'")
	var id uint32
	require.NoError(t, reader.QueryRowContext(ctx, "select account_id from mo_catalog.mo_account where account_name='mixed_b'").Scan(&id))
	cold := lazy.RawService().(cnservice.Service).GetEngine().(*disttae.Engine).PushClient()
	require.False(t, cold.CanServeAccount(id, types.MaxTs().ToTimestamp()))
	cn := full.RawService().(cnservice.Service)
	push := cn.GetEngine().(*disttae.Engine).PushClient()
	reconnected := make(chan struct{}, 1)
	push.SetReconnectHandler(func() {
		select {
		case reconnected <- struct{}{}:
		default:
		}
	})
	defer push.SetReconnectHandler(nil)
	require.NoError(t, push.Disconnect())
	select {
	case <-reconnected:
	case <-ctx.Done():
		t.Fatal(ctx.Err())
	}
	require.NoError(t, checkLogtailResumed(ctx, cn))
	check("after_start", 1)
	check("before_start", 0)
	require.False(t, cold.CanServeAccount(id, types.MaxTs().ToTimestamp()))
}

func TestCatalogReplayMultiCN(t *testing.T) {
	cluster, err := embed.StartTestCluster(embed.WithCNCount(2), embed.WithPreStart(func(op embed.ServiceOperator) {
		if op.ServiceType() == metadata.ServiceType_CN {
			op.Adjust(func(c *embed.ServiceConfig) { c.CN.Frontend.SkipCheckUser = false })
		}
	}))
	if cluster != nil {
		t.Cleanup(func() { require.NoError(t, cluster.Close()) })
	}
	require.NoError(t, err)
	ctx, cancel := context.WithTimeout(t.Context(), 3*time.Minute)
	defer cancel()
	a, err := cluster.GetCNService(0)
	require.NoError(t, err)
	b, err := cluster.GetCNService(1)
	require.NoError(t, err)
	t.Logf("DDL CN=%s, reconnect CN=%s", a.ServiceID(), b.ServiceID())
	open := func(user string, op embed.ServiceOperator) *sql.DB {
		db, err := sql.Open("mysql", fmt.Sprintf("%s:111@tcp(127.0.0.1:%d)/", user, op.GetServiceConfig().CN.Frontend.Port))
		require.NoError(t, err)
		t.Cleanup(func() { require.NoError(t, db.Close()) })
		return db
	}
	writer, reader := open("dump", a), open("dump", b)
	require.NoError(t, waitSystemBootstrap(ctx, writer))
	require.NoError(t, waitSystemBootstrap(ctx, reader))
	for _, stmt := range []string{
		"create account replay_tenant admin_name 'root' identified by '111'",
		"create database replay_multi",
		"create table replay_multi.events (id int primary key, payload int)",
		"insert into replay_multi.events values (1,10)",
		"create snapshot replay_multi_snap for table replay_multi events",
		"create table replay_multi.replaced (old_value int)",
		"create table replay_multi.to_rename (v int)",
		"insert into replay_multi.to_rename values (42)",
	} {
		execSQLRequire(t, ctx, writer, stmt)
	}
	tenant := open("replay_tenant#root#accountadmin", b)
	for _, stmt := range []string{
		"create database replay_multi",
		"create table replay_multi.events (id int primary key, tenant_value int)",
		"insert into replay_multi.events values (1,99)",
	} {
		execSQLRequire(t, ctx, tenant, stmt)
	}
	var value int
	require.NoError(t, reader.QueryRowContext(ctx, "select payload from replay_multi.events where id=1").Scan(&value))
	require.Equal(t, 10, value)
	session, err := reader.Conn(ctx)
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, session.Close()) })
	_, err = session.ExecContext(ctx, "set session transaction_isolation='REPEATABLE-READ'")
	require.NoError(t, err)
	tx, err := session.BeginTx(ctx, nil)
	require.NoError(t, err)
	t.Cleanup(func() { _ = tx.Rollback() })
	require.NoError(t, tx.QueryRowContext(ctx, "select payload from replay_multi.events where id=1").Scan(&value))
	require.Equal(t, 10, value)
	execSQLRequire(t, ctx, writer, "insert into replay_multi.events(id,payload) values (2,20)")
	var beforeDDL int
	require.NoError(t, tx.QueryRowContext(ctx, "select count(*) from replay_multi.events").Scan(&beforeDDL))
	require.Equal(t, 1, beforeDDL)
	for _, stmt := range []string{
		"alter table replay_multi.events add column extra int default 7",
		"alter table replay_multi.to_rename rename to renamed",
		"drop table replay_multi.replaced",
		"create table replay_multi.replaced (new_value int default 9)",
		"insert into replay_multi.replaced values (default)",
	} {
		execSQLRequire(t, ctx, writer, stmt)
	}
	var beforeReconnect int
	require.NoError(t, tx.QueryRowContext(ctx, "select count(*) from replay_multi.events").Scan(&beforeReconnect))
	require.Equal(t, 1, beforeReconnect)
	cn := b.RawService().(cnservice.Service)
	push := cn.GetEngine().(*disttae.Engine).PushClient()
	entered, resume := make(chan struct{}), make(chan struct{})
	var signal, release sync.Once
	unblock := func() { release.Do(func() { close(resume) }) }
	t.Cleanup(unblock)
	push.SetReconnectHandler(func() {
		signal.Do(func() { close(entered) })
		select {
		case <-resume:
		case <-ctx.Done():
		}
	})
	t.Cleanup(func() { push.SetReconnectHandler(nil) })
	require.NoError(t, push.Disconnect())
	select {
	case <-entered:
	case <-ctx.Done():
		t.Fatal(ctx.Err())
	}
	require.False(t, push.IsSubscriberReady())
	unblock()
	require.NoError(t, checkLogtailResumed(ctx, cn))
	require.NoError(t, tx.QueryRowContext(ctx, "select payload from replay_multi.events where id=1").Scan(&value))
	require.Equal(t, 10, value)
	var oldCount int
	require.NoError(t, tx.QueryRowContext(ctx, "select count(*) from replay_multi.events").Scan(&oldCount))
	require.Equal(t, 1, oldCount)
	require.NoError(t, tx.Commit())
	require.NoError(t, reader.QueryRowContext(ctx, "select v from replay_multi.renamed").Scan(&value))
	require.Equal(t, 42, value)
	var extra int
	require.NoError(t, reader.QueryRowContext(ctx, "select payload,extra from replay_multi.events where id=2").Scan(&value, &extra))
	require.Equal(t, 20, value)
	require.Equal(t, 7, extra)
	require.NoError(t, reader.QueryRowContext(ctx, "select payload from replay_multi.events {snapshot = 'replay_multi_snap'} where id=1").Scan(&value))
	require.Equal(t, 10, value)
	var count int
	require.NoError(t, reader.QueryRowContext(ctx, "select count(*) from replay_multi.events {snapshot = 'replay_multi_snap'}").Scan(&count))
	require.Equal(t, 1, count)
	require.NoError(t, reader.QueryRowContext(ctx, "select new_value from replay_multi.replaced").Scan(&value))
	require.Equal(t, 9, value)
	require.NoError(t, tenant.QueryRowContext(ctx, "select tenant_value from replay_multi.events where id=1").Scan(&value))
	require.Equal(t, 99, value)
	execSQLRequire(t, ctx, writer, "drop snapshot replay_multi_snap")
	execSQLRequire(t, ctx, tenant, "drop database replay_multi")
	execSQLRequire(t, ctx, writer, "drop database replay_multi")
	execSQLRequire(t, ctx, writer, "drop account replay_tenant")
}
