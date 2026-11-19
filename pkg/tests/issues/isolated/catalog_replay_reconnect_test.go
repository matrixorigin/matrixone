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

package isolated

import (
	"context"
	"database/sql"
	"fmt"
	"sync"
	"testing"
	"time"

	"github.com/go-sql-driver/mysql"
	"github.com/matrixorigin/matrixone/pkg/cnservice"
	"github.com/matrixorigin/matrixone/pkg/embed"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/disttae"
	"github.com/stretchr/testify/require"
)

// A real logtail disconnect rebuilds the catalog before SQL becomes ready.
func TestCatalogReplayReconnect(t *testing.T) {
	started := time.Now()
	cluster, err := embed.StartTestCluster(embed.WithCNCount(1))
	if cluster != nil {
		t.Cleanup(func() { require.NoError(t, cluster.Close()) })
	}
	require.NoError(t, err)
	t.Logf("cluster startup: %s", time.Since(started))
	ctx, cancel := context.WithTimeout(t.Context(), 2*time.Minute)
	defer cancel()
	op, err := cluster.GetCNService(0)
	require.NoError(t, err)
	cn := op.RawService().(cnservice.Service)
	push := cn.GetEngine().(*disttae.Engine).PushClient()
	db, err := sql.Open("mysql", fmt.Sprintf("dump:111@tcp(127.0.0.1:%d)/", op.GetServiceConfig().CN.Frontend.Port))
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, db.Close()) })
	require.NoError(t, waitSystemBootstrap(ctx, db))
	for _, stmt := range []string{
		"create database replay_reconnect",
		"create table replay_reconnect.parent (id int primary key)",
		"insert into replay_reconnect.parent values (1)",
		"create table replay_reconnect.events (id int primary key, parent_id int, payload varchar(25) default 'default-value', u int, unique key uk_u(u), key idx_parent(parent_id), constraint fk_parent foreign key (parent_id) references replay_reconnect.parent(id))",
		"insert into replay_reconnect.events (id,parent_id,u) values (1,1,101)",
		"alter table replay_reconnect.events add column extra int default 7",
		"create table replay_reconnect.replaced (old_value int)",
	} {
		execSQLRequire(t, ctx, db, stmt)
	}
	var oldID, newID uint64
	const idQuery = "select rel_id from mo_catalog.mo_tables where reldatabase='replay_reconnect' and relname='replaced'"
	require.NoError(t, db.QueryRowContext(ctx, idQuery).Scan(&oldID))
	execSQLRequire(t, ctx, db, "drop table replay_reconnect.replaced")
	execSQLRequire(t, ctx, db, "create table replay_reconnect.replaced (new_value int default 9)")
	require.NoError(t, db.QueryRowContext(ctx, idQuery).Scan(&newID))
	require.NotEqual(t, oldID, newID)
	execSQLRequire(t, ctx, db, "insert into replay_reconnect.replaced values (default)")
	var schemaBefore, schemaAfter, name string
	require.NoError(t, db.QueryRowContext(ctx, "show create table replay_reconnect.events").Scan(&name, &schemaBefore))
	reconnecting := make(chan struct{})
	resume := make(chan struct{})
	var signal, release sync.Once
	unblock := func() { release.Do(func() { close(resume) }) }
	t.Cleanup(unblock)
	push.SetReconnectHandler(func() {
		signal.Do(func() { close(reconnecting) })
		select {
		case <-resume:
		case <-ctx.Done():
		}
	})
	t.Cleanup(func() { push.SetReconnectHandler(nil) })
	require.NoError(t, push.Disconnect())
	select {
	case <-reconnecting:
	case <-ctx.Done():
		t.Fatal(ctx.Err())
	}
	require.False(t, push.IsSubscriberReady())
	unblock()
	require.NoError(t, checkLogtailResumed(ctx, cn))
	require.NoError(t, db.QueryRowContext(ctx, "show create table replay_reconnect.events").Scan(&name, &schemaAfter))
	require.Equal(t, schemaBefore, schemaAfter)
	var payload string
	var extra, value int
	require.NoError(t, db.QueryRowContext(ctx, "select payload,extra from replay_reconnect.events where id=1").Scan(&payload, &extra))
	require.Equal(t, "default-value", payload)
	require.Equal(t, 7, extra)
	require.NoError(t, db.QueryRowContext(ctx, "select new_value from replay_reconnect.replaced").Scan(&value))
	require.Equal(t, 9, value)
	var replayedID uint64
	require.NoError(t, db.QueryRowContext(ctx, idQuery).Scan(&replayedID))
	require.Equal(t, newID, replayedID)
	execSQLRequire(t, ctx, db, "insert into replay_reconnect.events(id,parent_id,u) values(2,1,102)")
	require.NoError(t, db.QueryRowContext(ctx, "select payload,extra from replay_reconnect.events where id=2").Scan(&payload, &extra))
	require.Equal(t, "default-value", payload)
	require.Equal(t, 7, extra)
	var count int
	require.NoError(t, db.QueryRowContext(ctx, "select count(*) from replay_reconnect.events force index(idx_parent) where parent_id=1").Scan(&count))
	require.Equal(t, 2, count)
	_, err = db.ExecContext(ctx, "insert into replay_reconnect.events(id,parent_id,u) values(3,999,103)")
	var mysqlErr *mysql.MySQLError
	require.ErrorAs(t, err, &mysqlErr)
	require.EqualValues(t, 1452, mysqlErr.Number, "the replayed foreign key must reject a missing parent")
	_, err = db.ExecContext(ctx, "insert into replay_reconnect.events(id,parent_id,u) values(3,1,101)")
	mysqlErr = nil
	require.ErrorAs(t, err, &mysqlErr)
	require.EqualValues(t, 1062, mysqlErr.Number, "the replayed unique key must reject duplicates")
	require.NoError(t, db.QueryRowContext(ctx, "select count(*) from replay_reconnect.events").Scan(&count))
	require.Equal(t, 2, count)
	execSQLRequire(t, ctx, db, "drop database replay_reconnect")
}
