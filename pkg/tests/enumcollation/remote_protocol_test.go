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

package enumcollation

import (
	"context"
	"database/sql"
	"fmt"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	_ "github.com/go-sql-driver/mysql"
	"github.com/matrixorigin/matrixone/pkg/clusterservice"
	"github.com/matrixorigin/matrixone/pkg/cnservice"
	moruntime "github.com/matrixorigin/matrixone/pkg/common/runtime"
	"github.com/matrixorigin/matrixone/pkg/defines"
	"github.com/matrixorigin/matrixone/pkg/embed"
	"github.com/matrixorigin/matrixone/pkg/logutil"
	"github.com/matrixorigin/matrixone/pkg/pb/metadata"
	"github.com/matrixorigin/matrixone/pkg/sql/plan"
	"github.com/matrixorigin/matrixone/pkg/util/fault"
	"github.com/stretchr/testify/require"
	"go.uber.org/zap/buffer"
	"go.uber.org/zap/zapcore"
)

// Exercise a typed ENUM scan across two actual CNs, then lower the receiver's
// protocol floor and require either safe local execution or an explicit error,
// never delivery of the affected plan to an older worker.
func TestRemoteEnumCollationProtocol(t *testing.T) {
	cluster, err := embed.StartTestCluster(embed.WithCNCount(2))
	if cluster != nil {
		t.Cleanup(func() { require.NoError(t, cluster.Close()) })
	}
	require.NoError(t, err)
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Minute)
	defer cancel()
	coordinator, err := cluster.GetCNService(0)
	require.NoError(t, err)
	peer, err := cluster.GetCNService(1)
	require.NoError(t, err)
	inventory := clusterservice.GetMOCluster(coordinator.ServiceID())
	refresher, ok := inventory.(clusterservice.AuthoritativeRefresher)
	require.True(t, ok)
	var peerAddress string
	require.Eventually(t, func() bool {
		if refresher.Refresh(ctx) != nil {
			return false
		}
		count := 0
		inventory.GetCNService(clusterservice.NewSelector(), func(metadata.CNService) bool { count++; return true })
		nodes, err := coordinator.RawService().(cnservice.Service).GetEngine().Nodes(false, "sys", "root", nil)
		for _, node := range nodes {
			if node.Id == peer.ServiceID() {
				peerAddress = node.Addr
			}
		}
		return err == nil && count == 2 && peerAddress != ""
	}, 30*time.Second, 100*time.Millisecond)

	db, err := sql.Open("mysql", fmt.Sprintf("dump:111@tcp(127.0.0.1:%d)/", coordinator.GetServiceConfig().CN.Frontend.Port))
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, db.Close()) })
	conn, err := db.Conn(ctx)
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, conn.Close()) })
	for _, statement := range []string{
		"create database enum_remote_32814",
		"create table enum_remote_32814.t(id int primary key,e enum('a','A') collate utf8mb4_bin)",
		"insert into enum_remote_32814.t values(1,1),(2,2)",
		"select mo_ctl('dn','flush','enum_remote_32814.t')",
		"insert into enum_remote_32814.t values(3,1),(4,2)",
		"select mo_ctl('dn','flush','enum_remote_32814.t')",
	} {
		_, err := conn.ExecContext(ctx, statement)
		require.NoError(t, err, statement)
	}
	oldForce := plan.GetForceScanOnMultiCN()
	plan.SetForceScanOnMultiCN(true)
	t.Cleanup(func() { plan.SetForceScanOnMultiCN(oldForce) })
	const query = "select id,e+0 from enum_remote_32814.t where id>=0 order by id"
	rows, err := conn.QueryContext(ctx, "explain phyplan analyze "+query)
	require.NoError(t, err)
	defer rows.Close()
	var physical strings.Builder
	for rows.Next() {
		var line string
		require.NoError(t, rows.Scan(&line))
		physical.WriteString(line + "\n")
	}
	require.NoError(t, rows.Err())
	require.NoError(t, rows.Close())
	require.Contains(t, physical.String(), "addr:"+peerAddress, "the remote CN must be part of the physical plan")

	var remoteReceipts atomic.Int64
	previousReport := logutil.GetReportZapFunc()
	logutil.SetLogReporter(&logutil.TraceReporter{ReportZap: func(enc zapcore.Encoder, entry zapcore.Entry, fields []zapcore.Field) (*buffer.Buffer, error) {
		if strings.HasPrefix(entry.Message, "inject_send_pipeline: client2server") {
			remoteReceipts.Add(1)
		}
		return previousReport(enc, entry, fields)
	}})
	t.Cleanup(func() { logutil.SetLogReporter(&logutil.TraceReporter{ReportZap: previousReport}) })
	if fault.Enable() {
		t.Cleanup(func() { fault.Disable() })
	}
	require.NoError(t, fault.AddFaultPoint(ctx, "inject_send_pipeline", ":::", "return", 0, "enum_remote_32814.t where id", false))
	t.Cleanup(func() { _, _ = fault.RemoveFaultPoint(context.Background(), "inject_send_pipeline") })
	read := func() ([][2]int64, error) {
		rows, err := conn.QueryContext(ctx, query)
		if err != nil {
			return nil, err
		}
		defer rows.Close()
		var got [][2]int64
		for rows.Next() {
			var row [2]int64
			if err := rows.Scan(&row[0], &row[1]); err != nil {
				return nil, err
			}
			got = append(got, row)
		}
		return got, rows.Err()
	}
	got, err := read()
	require.NoError(t, err)
	require.Equal(t, [][2]int64{{1, 1}, {2, 2}, {3, 1}, {4, 2}}, got)
	require.Positive(t, remoteReceipts.Load(), "the ENUM query must actually send a remote pipeline")

	peerRuntime := moruntime.ServiceRuntime(peer.ServiceID())
	oldVersion, hadVersion := peerRuntime.GetGlobalVariables(moruntime.MOProtocolVersion)
	restore := func() {
		if hadVersion {
			peerRuntime.SetGlobalVariables(moruntime.MOProtocolVersion, oldVersion)
		} else {
			peerRuntime.SetGlobalVariables(moruntime.MOProtocolVersion, defines.MORPCLatestVersion)
		}
	}
	t.Cleanup(restore)
	peerRuntime.SetGlobalVariables(moruntime.MOProtocolVersion, defines.MORPCLatestVersion-1)
	beforeOldRead := remoteReceipts.Load()
	got, err = read()
	if err != nil {
		require.ErrorContains(t, err, "protocol version")
	} else {
		require.Equal(t, [][2]int64{{1, 1}, {2, 2}, {3, 1}, {4, 2}}, got)
	}
	require.Equal(t, beforeOldRead, remoteReceipts.Load(), "an old worker must not receive the ENUM pipeline")
	restore()
	got, err = read()
	require.NoError(t, err)
	require.Equal(t, [][2]int64{{1, 1}, {2, 2}, {3, 1}, {4, 2}}, got)
	require.Greater(t, remoteReceipts.Load(), beforeOldRead, "the supported worker should be used again")
}
