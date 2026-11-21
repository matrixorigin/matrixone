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
	"github.com/matrixorigin/matrixone/pkg/cnservice"
	"github.com/matrixorigin/matrixone/pkg/common/moerr"
	moruntime "github.com/matrixorigin/matrixone/pkg/common/runtime"
	"github.com/matrixorigin/matrixone/pkg/defines"
	"github.com/matrixorigin/matrixone/pkg/embed"
	"github.com/matrixorigin/matrixone/pkg/pb/metadata"
	"github.com/matrixorigin/matrixone/pkg/proxy"
	"github.com/matrixorigin/matrixone/pkg/util/executor"
	v2 "github.com/matrixorigin/matrixone/pkg/util/metric/v2"
	"github.com/prometheus/client_golang/prometheus"
	dto "github.com/prometheus/client_model/go"
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

	firstMarker, _ := readProjectedWatch(t, ctx, observer, nil, "BASELINE", "t", tableID)
	exec(source, "insert into watched_db.t values(1)")
	_, to := readProjectedWatch(t, ctx, observer, firstMarker, "CHANGED", "t", tableID)

	t.Run("lower CN-local protocol version", func(t *testing.T) {
		cnRuntime := moruntime.ServiceRuntime(cn.ServiceID())
		require.NotNil(t, cnRuntime)
		version, ok := cnRuntime.GetGlobalVariables(moruntime.MOProtocolVersion)
		require.True(t, ok)
		cnRuntime.SetGlobalVariables(moruntime.MOProtocolVersion, defines.MORPCVersion38)
		defer cnRuntime.SetGlobalVariables(moruntime.MOProtocolVersion, version)

		readProjectedWatch(t, ctx, observer, firstMarker, "CHANGED", "t", tableID)
	})

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

	exec(source, "create table watched_db.empty_input(id int primary key)")
	requireStandaloneSnapshotGuard(t, ctx, source)

	exec(source, "create table watched_db.partitioned_parent(id int primary key) partition by hash(id) partitions 2")
	exec(sys, `insert into mo_change_watch.watch
		select observer_account_id, source_account_id, database_name, 'partitioned_parent', uuid()
		from mo_change_watch.watch where database_name='watched_db' and table_name='t'`)
	beforeFailure := watchCatalogPhaseCounts(t)
	for _, input := range []any{nil, firstMarker} {
		serverErr := requireChangeWatchWireError(t, ctx, observer, input, moerr.ErrNotSupported)
		require.Contains(t, serverErr.Message, `granted "watched_db"."partitioned_parent"`)
	}
	afterFailure := watchCatalogPhaseCounts(t)
	require.Equal(t, [2]uint64{2, 0}, [2]uint64{afterFailure[0] - beforeFailure[0], afterFailure[1] - beforeFailure[1]})
	exec(sys, "delete from mo_change_watch.watch where database_name='watched_db' and table_name='partitioned_parent'")
	readProjectedWatch(t, ctx, observer, firstMarker, "CHANGED", "t", tableID)

	var oldEpoch, newEpoch string
	const epochSQL = "select cast(recovery_epoch as char) from mo_change_watch.state where singleton=1"
	require.NoError(t, sys.QueryRowContext(ctx, epochSQL).Scan(&oldEpoch))
	exec(sys, "update mo_change_watch.state set recovery_epoch=uuid() where singleton=1")
	require.NoError(t, sys.QueryRowContext(ctx, epochSQL).Scan(&newEpoch))
	require.NotEqual(t, oldEpoch, newEpoch)

	beforeEpoch := watchCatalogPhaseCounts(t)
	serverErr := requireChangeWatchWireError(t, ctx, observer, firstMarker, moerr.ErrChangeWatchHistoryUnavailable)
	require.Equal(t, "HY000", string(serverErr.SQLState[:]))
	require.Contains(t, serverErr.Message, "Restart with a NULL marker")
	serverErr = requireChangeWatchWireError(t, ctx, source, firstMarker, moerr.ErrInvalidInput)
	require.Contains(t, serverErr.Message, "different observer account")
	afterEpochErrors := watchCatalogPhaseCounts(t)
	require.Equal(t, [2]uint64{2, 0}, [2]uint64{afterEpochErrors[0] - beforeEpoch[0], afterEpochErrors[1] - beforeEpoch[1]})

	replacement, _ := readProjectedWatch(t, ctx, observer, nil, "BASELINE", "t", tableID)
	require.NotEqual(t, firstMarker, replacement)
	afterBaseline := watchCatalogPhaseCounts(t)
	require.Equal(t, [2]uint64{1, 1}, [2]uint64{afterBaseline[0] - afterEpochErrors[0], afterBaseline[1] - afterEpochErrors[1]})
	t.Logf("MySQL epoch rotation 22602/HY000, foreign marker rejected, both produced zero rows/END; catalog/recheck-catalog counts partition failures %v->%v, epoch errors %v->%v, NULL baseline %v->%v",
		beforeFailure, afterFailure, beforeEpoch, afterEpochErrors, afterEpochErrors, afterBaseline)

	t.Run("historical partition names and identities", func(t *testing.T) {
		const (
			ordinary = "%!%ordinary%!%events"
			legacy   = "%!%old_leaf%!%partitioned_parent"
		)
		exec := func(db *sql.DB, query string, args ...any) {
			t.Helper()
			_, err := db.ExecContext(ctx, query, args...)
			require.NoError(t, err)
		}
		var ids [2]uint64
		for i, name := range []string{ordinary, legacy} {
			exec(source, "create table watched_db.`"+name+"`(id int primary key)")
			require.NoError(t, source.QueryRowContext(ctx, "select rel_id from mo_catalog.mo_tables where reldatabase='watched_db' and relname=?", name).Scan(&ids[i]))
			exec(sys, `insert into mo_change_watch.watch
				select observer_account_id, source_account_id, database_name, ?, uuid()
				from mo_change_watch.watch where database_name='watched_db' and table_name='t'`, name)
		}
		var sourceID uint32
		require.NoError(t, sys.QueryRowContext(ctx, "select account_id from mo_catalog.mo_account where account_name='watch_source'").Scan(&sourceID))
		var parentID uint64
		require.NoError(t, source.QueryRowContext(ctx, "select rel_id from mo_catalog.mo_tables where reldatabase='watched_db' and relname='partitioned_parent'").Scan(&parentID))
		// Emulate a catalog written before durable leaf flags were stored,
		// without rewriting the physical relation via an older server.
		engine := cn.RawService().(cnservice.Service).GetSQLExecutor()
		result, err := engine.Exec(ctx, fmt.Sprintf(`insert into mo_catalog.mo_partition_tables values
			(%d, '%s', %d, 'old_leaf', 3, '', '')`, ids[1], legacy, parentID), executor.Options{}.WithAccountID(sourceID))
		require.NoError(t, err)
		result.Close()

		rows, err := observer.QueryContext(ctx, "select kind, table_name, table_id, marker from mo_changed_tables(NULL)")
		require.NoError(t, err)
		defer rows.Close()
		baselines := make(map[string]sql.Null[uint64])
		var marker string
		endCount := 0
		for rows.Next() {
			var kind string
			var name, next sql.NullString
			var id sql.Null[uint64]
			require.NoError(t, rows.Scan(&kind, &name, &id, &next))
			if kind == "END" {
				endCount++
				require.True(t, next.Valid)
				marker = next.String
			} else {
				require.Zero(t, endCount)
				require.Equal(t, "BASELINE", kind)
				require.True(t, name.Valid)
				require.NotContains(t, baselines, name.String)
				baselines[name.String] = id
			}
		}
		require.NoError(t, rows.Err())
		require.Equal(t, 1, endCount)
		require.Equal(t, map[string]sql.Null[uint64]{
			"t":      {V: tableID, Valid: true},
			ordinary: {V: ids[0], Valid: true},
			legacy:   {},
		}, baselines)
		exec(source, "insert into watched_db.`"+ordinary+"` values(1)")
		exec(source, "insert into watched_db.`"+legacy+"` values(1)")
		readProjectedWatch(t, ctx, observer, marker, "CHANGED", ordinary, ids[0])
		t.Logf("ordinary partition-like table %d remains visible; legacy catalog member %d has NULL baseline ID and is excluded from incremental output", ids[0], ids[1])
	})
}

func requireChangeWatchWireError(t *testing.T, ctx context.Context, db *sql.DB, marker any, code uint16) *mysql.MySQLError {
	t.Helper()
	rows, err := db.QueryContext(ctx, "select * from mo_changed_tables(?)", marker)
	if rows != nil {
		defer rows.Close()
		require.False(t, rows.Next(), "a failed watch must publish no table rows or END")
		err = rows.Err()
	}
	var serverErr *mysql.MySQLError
	require.ErrorAs(t, err, &serverErr)
	require.Equal(t, code, serverErr.Number)
	return serverErr
}

func watchCatalogPhaseCounts(t *testing.T) [2]uint64 {
	t.Helper()
	var counts [2]uint64
	for i, histogram := range []prometheus.Observer{
		v2.ChangeWatchCatalogDurationHistogram, v2.ChangeWatchRecheckCatalogDurationHistogram,
	} {
		var metric dto.Metric
		require.NoError(t, histogram.(prometheus.Metric).Write(&metric))
		counts[i] = metric.GetHistogram().GetSampleCount()
	}
	return counts
}

func requireStandaloneSnapshotGuard(t *testing.T, ctx context.Context, source *sql.DB) {
	t.Helper()
	conn, err := source.Conn(ctx)
	require.NoError(t, err)
	defer conn.Close()
	_, err = conn.ExecContext(ctx, "set session transaction isolation level repeatable read")
	require.NoError(t, err)
	tx, err := conn.BeginTx(ctx, &sql.TxOptions{ReadOnly: true})
	require.NoError(t, err)
	defer tx.Rollback()

	var snapshot string
	require.NoError(t, tx.QueryRowContext(ctx, "select watermark from change_watermark() w").Scan(&snapshot))
	var id int
	err = tx.QueryRowContext(ctx, "select id from watched_db.empty_input where mo_assert_snapshot_after(?)", snapshot).Scan(&id)
	require.ErrorIs(t, err, sql.ErrNoRows)
	var verified bool
	err = tx.QueryRowContext(ctx, "select mo_assert_snapshot_after(?)", snapshot).Scan(&verified)
	var serverErr *mysql.MySQLError
	require.ErrorAs(t, err, &serverErr)
	require.Equal(t, uint16(22601), serverErr.Number)
	require.Equal(t, "40001", string(serverErr.SQLState[:]))
	var after string
	require.NoError(t, tx.QueryRowContext(ctx, "select watermark from change_watermark() w").Scan(&after))
	require.Equal(t, snapshot, after)
}

func readProjectedWatch(t *testing.T, ctx context.Context, db *sql.DB, marker any, wantKind, tableName string, tableID uint64) (string, string) {
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
	require.Equal(t, sql.NullString{String: tableName, Valid: true}, payload.tableName)
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
