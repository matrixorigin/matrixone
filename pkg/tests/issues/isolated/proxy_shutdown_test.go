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

package isolated

import (
	"context"
	"database/sql"
	"fmt"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/go-sql-driver/mysql"
	"github.com/google/uuid"
	moruntime "github.com/matrixorigin/matrixone/pkg/common/runtime"
	"github.com/matrixorigin/matrixone/pkg/embed"
	"github.com/matrixorigin/matrixone/pkg/pb/metadata"
	"github.com/matrixorigin/matrixone/pkg/proxy"
	"github.com/stretchr/testify/require"
	"go.uber.org/zap"
)

// Exercise the real client, Proxy and CN while a statement is running.
func TestProxyShutdownCancelsInFlightSQL(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Minute)
	t.Cleanup(cancel)
	cluster, err := embed.StartTestCluster(embed.WithCNCount(1), embed.WithTestAdmissionContext(ctx), embed.WithPreStart(func(service embed.ServiceOperator) {
		if service.ServiceType() == metadata.ServiceType_CN {
			service.Adjust(func(config *embed.ServiceConfig) { config.CN.Frontend.ProxyEnabled = true })
		}
	}))
	if cluster != nil {
		t.Cleanup(func() { require.NoError(t, cluster.Close()) })
	}
	require.NoError(t, err)
	cn, err := cluster.GetCNService(0)
	require.NoError(t, err)
	cnConfig := cn.GetServiceConfig()
	socketDir, err := os.MkdirTemp("/tmp", "mo-proxy-stop-")
	require.NoError(t, err)
	t.Cleanup(func() { _ = os.RemoveAll(socketDir) })
	socket := filepath.Join(socketDir, "proxy.sock")
	cfg := proxy.Config{UUID: uuid.NewString(), ListenAddress: "unix://" + socket, RebalanceDisabled: true}
	cfg.HAKeeper.ClientConfig = cnConfig.HAKeeperClient
	rt := moruntime.NewRuntime(metadata.ServiceType_PROXY, cfg.UUID, zap.NewNop())
	moruntime.SetupServiceBasedRuntime(cfg.UUID, rt)
	t.Cleanup(func() { moruntime.CompareAndDeleteServiceBasedRuntime(cfg.UUID, rt) })
	server, err := proxy.NewServer(ctx, cfg, proxy.WithRuntime(rt))
	require.NoError(t, err)
	t.Cleanup(func() { _ = server.Close() })
	require.NoError(t, server.Start())

	open := func(network, address string) *sql.DB {
		t.Helper()
		cfg := mysql.NewConfig()
		cfg.Net, cfg.Addr, cfg.User, cfg.Passwd = network, address, "dump", "111"
		db, openErr := sql.Open("mysql", cfg.FormatDSN())
		require.NoError(t, openErr)
		t.Cleanup(func() { _ = db.Close() }) // Proxy shutdown deliberately closes the client socket.
		require.NoError(t, db.PingContext(ctx))
		return db
	}
	observer := open("tcp", fmt.Sprintf("127.0.0.1:%d", cnConfig.CN.Frontend.Port))
	clientDB := open("unix", socket)
	conn, err := clientDB.Conn(ctx)
	require.NoError(t, err)
	defer conn.Close()
	queryCtx, cancelQuery := context.WithCancel(ctx)
	defer cancelQuery()
	var id uint32
	require.NoError(t, conn.QueryRowContext(ctx, "select connection_id()").Scan(&id))
	queryDone := make(chan error, 1)
	go func() {
		_, queryErr := conn.ExecContext(queryCtx, "select sleep(60)")
		queryDone <- queryErr
	}()
	var active int
	var lookupErr error
	require.Eventually(t, func() bool {
		lookupErr = observer.QueryRowContext(ctx,
			"select count(*) from information_schema.processlist where conn_id = ? and info = 'select sleep(60)'", id).Scan(&active)
		return lookupErr == nil && active == 1
	}, 15*time.Second, 50*time.Millisecond, "query never became active")

	require.NoError(t, server.Close())
	select {
	case queryErr := <-queryDone:
		require.Error(t, queryErr, "the in-flight client query must not report success")
	case <-time.After(10 * time.Second):
		t.Fatal("client query did not stop after Proxy shutdown")
	}
	require.Eventually(t, func() bool {
		lookupErr = observer.QueryRowContext(ctx,
			"select count(*) from information_schema.processlist where conn_id = ?", id).Scan(&active)
		return lookupErr == nil && active == 0
	}, 10*time.Second, 50*time.Millisecond, "backend session was not removed")
}
