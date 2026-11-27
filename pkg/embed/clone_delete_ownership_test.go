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

package embed

import (
	"context"
	"database/sql"
	"fmt"
	"testing"
	"time"

	_ "github.com/go-sql-driver/mysql"
	"github.com/matrixorigin/matrixone/pkg/cnservice"
	"github.com/matrixorigin/matrixone/pkg/pb/metadata"
	"github.com/stretchr/testify/require"
)

func TestCloneRewriteKeepsPersistedDeletesAcrossCNs(t *testing.T) {
	dataDir := t.TempDir()
	cluster, err := StartTestCluster(WithDataPath(dataDir), WithCNCount(2),
		WithPreStart(func(svc ServiceOperator) {
			if svc.ServiceType() == metadata.ServiceType_CN {
				svc.Adjust(func(cfg *ServiceConfig) {
					cfg.CN.SQLAddress = fmt.Sprintf("127.0.0.1:%d", cfg.CN.Frontend.Port)
					// An existing path disables the optional Unix listener.
					// This regression connects directly to each CN over TCP.
					cfg.CN.Frontend.UnixSocketAddress = dataDir
				})
			}
		}))
	if cluster != nil {
		t.Cleanup(func() { require.NoError(t, cluster.Close()) })
	}
	require.NoError(t, err)

	ctx, cancel := context.WithTimeout(t.Context(), 90*time.Second)
	defer cancel()

	var services [2]ServiceOperator
	var conns [2]*sql.Conn
	for i := range services {
		cn, err := cluster.GetCNService(i)
		require.NoError(t, err)
		services[i] = cn
		db, err := sql.Open("mysql", fmt.Sprintf("dump:111@tcp(127.0.0.1:%d)/", cn.GetServiceConfig().CN.Frontend.Port))
		require.NoError(t, err)
		defer db.Close()
		conns[i], err = db.Conn(ctx)
		require.NoError(t, err)
		defer conns[i].Close()
	}
	require.NotEqual(t, services[0].ServiceID(), services[1].ServiceID())
	require.NotEqual(t, services[0].GetServiceConfig().CN.Frontend.Port, services[1].GetServiceConfig().CN.Frontend.Port)
	writer := conns[0]
	exec := func(statement string) {
		t.Helper()
		_, err := writer.ExecContext(ctx, statement)
		require.NoError(t, err, statement)
	}
	checkRows := func(t *testing.T, conn *sql.Conn, table string, want [][2]int) {
		t.Helper()
		query := "select id, a from " + table + " order by id"
		rows, err := conn.QueryContext(ctx, query)
		require.NoError(t, err, query)
		defer rows.Close()
		var got [][2]int
		for rows.Next() {
			var row [2]int
			require.NoError(t, rows.Scan(&row[0], &row[1]), query)
			got = append(got, row)
		}
		require.NoError(t, rows.Err(), query)
		require.Equal(t, want, got, query)
	}

	const dbName = "clone_persisted_delete_cross_cn"
	const source = dbName + ".src"
	const clone = dbName + ".copy_t"
	exec("create database " + dbName)

	exec("set session transaction_isolation='READ-COMMITTED'")
	exec("create table " + source + " (id int primary key, a int)")
	exec("insert into " + source + " values (1,11), (2,22), (3,33), (4,44)")
	flush := "select mo_ctl('dn','flush','" + source + "')"
	exec(flush)
	exec("delete from " + source + " where id=2")
	exec(flush)

	wantSource := [][2]int{{1, 11}, {3, 33}, {4, 44}}
	wantClone := [][2]int{{1, 11}, {4, 44}}
	checkRows(t, writer, source, wantSource)

	writerClient := services[0].RawService().(cnservice.Service).GetTxnClient()
	before := writerClient.GetLatestCommitTS()
	exec("begin")
	exec("create table " + clone + " clone " + source)
	checkRows(t, writer, clone, wantSource)
	exec("delete from " + clone + " where id=3")
	checkRows(t, writer, source, wantSource)
	checkRows(t, writer, clone, wantClone)
	exec("commit")

	// Fence the SQL reads on each CN's applied logtail before opening
	// new snapshots. A reader may still be behind when COMMIT returns.
	committed := writerClient.GetLatestCommitTS()
	require.True(t, before.Less(committed), "clone commit must advance the writer's commit timestamp")
	for i, conn := range conns {
		t.Run(fmt.Sprintf("CN%d", i), func(t *testing.T) {
			svc := services[i].RawService().(cnservice.Service)
			applied, err := svc.GetTxnClient().WaitLogTailAppliedAt(ctx, committed)
			require.NoError(t, err)
			require.True(t, committed.Less(applied), "read snapshot must include the clone commit")
			checkRows(t, conn, source, wantSource)
			checkRows(t, conn, clone, wantClone)
		})
	}
}
