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

package issues

import (
	"context"
	"database/sql"
	"fmt"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/matrixorigin/matrixone/pkg/clusterservice"
	"github.com/matrixorigin/matrixone/pkg/cnservice"
	"github.com/matrixorigin/matrixone/pkg/embed"
	"github.com/matrixorigin/matrixone/pkg/logutil"
	plan2 "github.com/matrixorigin/matrixone/pkg/sql/plan"
	"github.com/matrixorigin/matrixone/pkg/util/fault"
	"github.com/stretchr/testify/require"
	"go.uber.org/zap/buffer"
	"go.uber.org/zap/zapcore"
)

func TestMixedSnapshotBinaryPrepare(t *testing.T) {
	runAuthenticatedClusterTest(t, func(cluster embed.Cluster) {
		ctx, cancel := context.WithTimeout(t.Context(), 3*time.Minute)
		defer cancel()
		cn, err := cluster.GetCNService(0)
		require.NoError(t, err)
		db, err := sql.Open("mysql", issue27487DSN(cn.GetServiceConfig().CN.Frontend.Port))
		require.NoError(t, err)
		defer db.Close()
		conn, err := db.Conn(ctx)
		require.NoError(t, err)
		defer conn.Close()
		const database = "mixed_snapshot_protocol"
		const s1, s2 = "mixed_protocol_s1", "mixed_protocol_s2"
		defer func() {
			cleanup, stop := context.WithTimeout(context.Background(), 30*time.Second)
			defer stop()
			for _, query := range []string{
				"drop snapshot if exists " + s2,
				"drop snapshot if exists " + s1,
				"drop database if exists " + database,
			} {
				_, err := conn.ExecContext(cleanup, query)
				require.NoError(t, err, query)
			}
		}()
		for _, query := range []string{
			"create database " + database,
			"use " + database,
			"create table t (x int)",
			"create table m (s varchar(8))",
			"insert into t values (1)",
			"insert into m values ('old')",
			// 两个独立落盘对象提供确定性的远端扫描输入，无需扩充数据量。
			"create table r (id int primary key, x int)",
			"insert into r values (1,1)",
			"select mo_ctl('dn','flush','" + database + ".r')",
			"insert into r values (2,1)",
			"select mo_ctl('dn','flush','" + database + ".r')",
			"create snapshot " + s1 + " for account sys",
			"update t set x=2",
			"alter table m modify s varchar(13)",
			"update r set x=2",
			"select mo_ctl('dn','flush','" + database + ".r')",
			"create snapshot " + s2 + " for account sys",
			"update t set x=3",
		} {
			_, err = conn.ExecContext(ctx, query)
			require.NoError(t, err, query)
		}
		for _, tc := range []struct {
			name, query string
			want        [2]int
		}{
			{"forward", "select a.x,b.x from t{snapshot='" + s1 + "'} a cross join t{snapshot='" + s2 + "'} b", [2]int{1, 2}},
			{"reverse", "select a.x,b.x from t{snapshot='" + s2 + "'} a cross join t{snapshot='" + s1 + "'} b", [2]int{2, 1}},
			{"metadata", "select a.character_maximum_length,b.character_maximum_length from information_schema.columns{snapshot='" + s1 + "'} a cross join information_schema.columns{snapshot='" + s2 + "'} b where a.table_schema='" + database + "' and a.table_name='m' and a.column_name='s' and b.table_schema='" + database + "' and b.table_name='m' and b.column_name='s'", [2]int{8, 13}},
		} {
			t.Run(tc.name, func(t *testing.T) {
				var actual [2]int
				require.NoError(t, conn.QueryRowContext(ctx, tc.query).Scan(&actual[0], &actual[1]))
				require.Equal(t, tc.want, actual, "COM_QUERY")
				// 显式 PrepareContext 使用真正的 COM_STMT_PREPARE，而非客户端插值。
				stmt, err := conn.PrepareContext(ctx, tc.query)
				require.NoError(t, err)
				defer stmt.Close()
				for execution := 0; execution < 2; execution++ {
					require.NoError(t, stmt.QueryRowContext(ctx).Scan(&actual[0], &actual[1]))
					require.Equal(t, tc.want, actual, "COM_STMT_EXECUTE execution=%d", execution)
					var current int
					require.NoError(t, conn.QueryRowContext(ctx, "select x from t").Scan(&current))
					require.Equal(t, 3, current)
				}
			})
		}
		t.Run("remote-pipeline", func(t *testing.T) {
			peer, err := cluster.GetCNService(1)
			require.NoError(t, err)
			coordinator := cn.RawService().(cnservice.Service)
			peerID := peer.RawService().(cnservice.Service).ID()
			// SQL ready 不代表另一 CN 已进入协调者的 query worker 集合。
			// 先等待可观察的拓扑前提，不能用重跑查询掩盖本地执行。
			require.Eventually(t, func() bool {
				clusterservice.GetMOCluster(coordinator.ID()).ForceRefresh(true)
				nodes, err := coordinator.GetEngine().Nodes(false, "sys", "dump", nil)
				if err != nil || len(nodes) != 2 {
					return false
				}
				for _, node := range nodes {
					if node.Id == peerID {
						return true
					}
				}
				return false
			}, 15*time.Second, 100*time.Millisecond, "两个 CN 必须先进入协调者的可用 query worker 集合")
			oldForce := plan2.GetForceScanOnMultiCN()
			plan2.SetForceScanOnMultiCN(true)
			defer plan2.SetForceScanOnMultiCN(oldForce)
			// 使用已有诊断点：该消息在远端接收 pipeline 后记录，避免仅凭
			// EXPLAIN 中的协调者发送 scope 就声称实际执行过远端扫描。
			var remoteReceipts atomic.Int64
			previousReport := logutil.GetReportZapFunc()
			logutil.SetLogReporter(&logutil.TraceReporter{ReportZap: func(enc zapcore.Encoder, entry zapcore.Entry, fields []zapcore.Field) (*buffer.Buffer, error) {
				if strings.HasPrefix(entry.Message, "inject_send_pipeline: client2server") {
					remoteReceipts.Add(1)
				}
				return previousReport(enc, entry, fields)
			}})
			defer logutil.SetLogReporter(&logutil.TraceReporter{ReportZap: previousReport})
			if fault.Enable() {
				defer fault.Disable()
			}
			require.NoError(t, fault.AddFaultPoint(ctx, "inject_send_pipeline", ":::", "return", 0, "select a.id,a.x,b.x from r", false))
			defer func() {
				_, err := fault.RemoveFaultPoint(ctx, "inject_send_pipeline")
				require.NoError(t, err)
			}()
			for _, snapshots := range [][2]string{{s1, s2}, {s2, s1}} {
				func() {
					query := fmt.Sprintf("select a.id,a.x,b.x from r{snapshot='%s'} a join r{snapshot='%s'} b on a.id=b.id order by a.id", snapshots[0], snapshots[1])
					before := remoteReceipts.Load()
					rows, err := conn.QueryContext(ctx, query)
					require.NoError(t, err)
					defer rows.Close()
					var got [][3]int
					for rows.Next() {
						var row [3]int
						require.NoError(t, rows.Scan(&row[0], &row[1], &row[2]))
						got = append(got, row)
					}
					require.NoError(t, rows.Err())
					want := [][3]int{{1, 1, 2}, {2, 1, 2}}
					if snapshots[0] == s2 {
						want = [][3]int{{1, 2, 1}, {2, 2, 1}}
					}
					require.Equal(t, want, got)
					require.Greater(t, remoteReceipts.Load(), before, "实际查询必须到达远端 pipeline server")
				}()
			}
		})
		// 从另一个 CN 的前端重开相同历史关系，不能继承协调者的单槽快照。
		t.Run("peer-coordinator", func(t *testing.T) {
			peer, err := cluster.GetCNService(1)
			require.NoError(t, err)
			peerDB, err := sql.Open("mysql", issue27487DSN(peer.GetServiceConfig().CN.Frontend.Port))
			require.NoError(t, err)
			defer peerDB.Close()
			query := fmt.Sprintf("select a.x,b.x from %s.t{snapshot='%s'} a cross join %s.t{snapshot='%s'} b", database, s1, database, s2)
			var actual [2]int
			require.NoError(t, peerDB.QueryRowContext(ctx, query).Scan(&actual[0], &actual[1]))
			require.Equal(t, [2]int{1, 2}, actual)
		})
	})
}
