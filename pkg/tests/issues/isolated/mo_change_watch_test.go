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
	"path/filepath"
	"testing"
	"time"

	"github.com/go-sql-driver/mysql"
	"github.com/google/uuid"
	"github.com/matrixorigin/matrixone/pkg/common/moerr"
	moruntime "github.com/matrixorigin/matrixone/pkg/common/runtime"
	"github.com/matrixorigin/matrixone/pkg/embed"
	"github.com/matrixorigin/matrixone/pkg/pb/metadata"
	"github.com/matrixorigin/matrixone/pkg/proxy"
	"github.com/stretchr/testify/require"
	"go.uber.org/zap"
)

func TestChangeWatchProxy(t *testing.T) {
	ctx, cancel := context.WithTimeout(t.Context(), 2*time.Minute)
	defer cancel()
	cluster, err := embed.StartTestCluster(embed.WithCNCount(1), embed.WithTestAdmissionContext(ctx), embed.WithPreStart(func(service embed.ServiceOperator) {
		if service.ServiceType() == metadata.ServiceType_CN {
			service.Adjust(func(config *embed.ServiceConfig) {
				config.CN.Frontend.ProxyEnabled = true
			})
		}
	}))
	if cluster != nil {
		t.Cleanup(func() { require.NoError(t, cluster.Close()) })
	}
	require.NoError(t, err)
	cn, err := cluster.GetCNService(0)
	require.NoError(t, err)
	cnConfig := cn.GetServiceConfig()

	socket := filepath.Join(t.TempDir(), "proxy.sock")
	proxyConfig := proxy.Config{UUID: uuid.NewString(), ListenAddress: "unix://" + socket, RebalanceDisabled: true}
	proxyConfig.HAKeeper.ClientConfig = cnConfig.HAKeeperClient
	rt := moruntime.NewRuntime(metadata.ServiceType_PROXY, proxyConfig.UUID, zap.NewNop())
	moruntime.SetupServiceBasedRuntime(proxyConfig.UUID, rt)
	t.Cleanup(func() { moruntime.CompareAndDeleteServiceBasedRuntime(proxyConfig.UUID, rt) })
	proxyServer, err := proxy.NewServer(ctx, proxyConfig, proxy.WithRuntime(rt))
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, proxyServer.Close()) })
	require.NoError(t, proxyServer.Start())

	open := func(network, address, user string) *sql.DB {
		t.Helper()
		c := mysql.NewConfig()
		c.Net, c.Addr, c.User, c.Passwd = network, address, user, "111"
		db, err := sql.Open("mysql", c.FormatDSN())
		require.NoError(t, err)
		t.Cleanup(func() { require.NoError(t, db.Close()) })
		require.NoError(t, db.PingContext(ctx))
		return db
	}
	exec := func(db *sql.DB, query string, args ...any) {
		t.Helper()
		_, err := db.ExecContext(ctx, query, args...)
		require.NoError(t, err, "%s", query)
	}

	sys := open("tcp", fmt.Sprintf("127.0.0.1:%d", cnConfig.CN.Frontend.Port), "dump")
	exec(sys, "create account watch_source admin_name 'admin' identified by '111'")
	exec(sys, "create account watch_observer admin_name 'admin' identified by '111'")
	source := open("unix", socket, "watch_source#admin#accountadmin")
	observer := open("unix", socket, "watch_observer#admin#accountadmin")
	public := open("unix", socket, "watch_observer#admin#public")

	exec(source, "create database watched_db")
	exec(source, "create table watched_db.t(id int primary key)")
	exec(sys, "create database mo_change_watch")
	exec(sys, "create table mo_change_watch.state(singleton tinyint unsigned primary key, recovery_epoch uuid not null, enabled bool not null)")
	exec(sys, `create table mo_change_watch.watch(observer_account_id int unsigned not null, source_account_id int unsigned not null,
		database_name varchar(256) not null, table_name varchar(256) not null, grant_id uuid not null,
		primary key(observer_account_id, source_account_id, database_name, table_name))`)
	exec(sys, `insert into mo_change_watch.watch
		select o.account_id, s.account_id, 'watched_db', 't', uuid()
		from mo_catalog.mo_account o cross join mo_catalog.mo_account s
		where o.account_name='watch_observer' and s.account_name='watch_source'`)
	exec(sys, "insert into mo_change_watch.state values(1,uuid(),true)")

	for _, tc := range []struct {
		db  *sql.DB
		sql string
	}{
		{public, "select count(*) from mo_changed_tables(NULL)"},
		{observer, "select count(*) from mo_changed_tables(NULL,'watch_observer')"},
	} {
		var count uint64
		var serverErr *mysql.MySQLError
		err := tc.db.QueryRowContext(ctx, tc.sql).Scan(&count)
		require.ErrorAs(t, err, &serverErr, "%s", tc.sql)
		require.Equal(t, moerr.ErrInvalidInput, serverErr.Number, "%s", tc.sql)
	}
	var tableID uint64
	require.NoError(t, source.QueryRowContext(ctx, "select rel_id from mo_catalog.mo_tables where reldatabase='watched_db' and relname='t'").Scan(&tableID))

	firstMarker, _ := readProjectedWatch(t, ctx, observer, nil, "BASELINE", tableID)
	exec(source, "insert into watched_db.t values(1)")
	_, to := readProjectedWatch(t, ctx, observer, firstMarker, "CHANGED", tableID)

	tx, err := source.BeginTx(ctx, &sql.TxOptions{ReadOnly: true})
	require.NoError(t, err)
	defer tx.Rollback()
	var verified bool
	require.NoError(t, tx.QueryRowContext(ctx, "select mo_assert_snapshot_after(?)", to).Scan(&verified))
	require.True(t, verified)
	var id int
	require.NoError(t, tx.QueryRowContext(ctx, "select id from watched_db.t").Scan(&id))
	require.Equal(t, 1, id)
	require.NoError(t, tx.Commit())
}

func readProjectedWatch(t *testing.T, ctx context.Context, db *sql.DB, marker any, wantKind string, tableID uint64) (string, string) {
	t.Helper()
	// A SQL projection exercises the operator's physical 14-column layout
	// even though the client asks for only eight columns.
	rows, err := db.QueryContext(ctx, `select kind, table_name, table_id, marker, to_ts, source_count, baseline_count, changed_count
		from mo_changed_tables(?)`, marker)
	require.NoError(t, err)
	defer rows.Close()

	type row struct {
		kind                       string
		tableName, marker, to      sql.NullString
		tableID                    sql.Null[uint64]
		sources, baseline, changed sql.Null[uint64]
	}
	scan := func() row {
		t.Helper()
		require.True(t, rows.Next())
		var r row
		require.NoError(t, rows.Scan(&r.kind, &r.tableName, &r.tableID, &r.marker, &r.to, &r.sources, &r.baseline, &r.changed))
		return r
	}
	payload := scan()
	require.Equal(t, wantKind, payload.kind)
	require.Equal(t, sql.NullString{String: "t", Valid: true}, payload.tableName)
	require.Equal(t, sql.Null[uint64]{V: tableID, Valid: true}, payload.tableID)
	require.False(t, payload.marker.Valid)
	require.False(t, payload.to.Valid)

	end := scan()
	require.Equal(t, "END", end.kind)
	require.False(t, end.tableName.Valid)
	require.False(t, end.tableID.Valid)
	require.True(t, end.marker.Valid)
	require.NotEmpty(t, end.marker.String)
	require.True(t, end.to.Valid)
	require.NotEmpty(t, end.to.String)
	require.Equal(t, sql.Null[uint64]{V: 1, Valid: true}, end.sources)
	if wantKind == "BASELINE" {
		require.Equal(t, sql.Null[uint64]{V: 1, Valid: true}, end.baseline)
		require.Equal(t, sql.Null[uint64]{V: 0, Valid: true}, end.changed)
	} else {
		require.Equal(t, sql.Null[uint64]{V: 0, Valid: true}, end.baseline)
		require.Equal(t, sql.Null[uint64]{V: 1, Valid: true}, end.changed)
	}
	require.False(t, rows.Next())
	require.NoError(t, rows.Err())
	return end.marker.String, end.to.String
}
