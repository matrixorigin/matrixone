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

package issues

import (
	"context"
	"database/sql"
	_ "embed"
	"encoding/json"
	"fmt"
	"strings"
	"testing"
	"time"

	"github.com/matrixorigin/matrixone/pkg/clusterservice"
	"github.com/matrixorigin/matrixone/pkg/embed"
	"github.com/matrixorigin/matrixone/pkg/pb/metadata"
	"github.com/matrixorigin/matrixone/pkg/tests/testutils"
	metricv2 "github.com/matrixorigin/matrixone/pkg/util/metric/v2"
	"github.com/prometheus/client_golang/prometheus"
	dto "github.com/prometheus/client_model/go"
	"github.com/stretchr/testify/require"
)

//go:embed testdata/issue28313.sql
var issue28313SQL string

// An empty recursive build can stop a preregistered remote dispatch before
// its notify arrives. Pipeline cleanup cancels the local context even on
// success; that cancellation must not become a query error on the peer.
func TestIssue28313EmptyRemoteDispatch(t *testing.T) {
	embed.RunBaseClusterTests(t, func(cluster embed.Cluster) {
		ctx, cancel := context.WithTimeout(context.Background(), 3*time.Minute)
		defer cancel()
		cn, err := cluster.GetCNService(0)
		require.NoError(t, err)
		for i := 0; i < 2; i++ {
			svc, err := cluster.GetCNService(i)
			require.NoError(t, err)
			inventory := clusterservice.GetMOCluster(svc.ServiceID())
			require.Eventually(t, func() bool {
				inventory.ForceRefresh(true)
				n := 0
				inventory.GetCNService(clusterservice.NewSelector(), func(s metadata.CNService) bool { n++; return true })
				return n >= 2
			}, 20*time.Second, 200*time.Millisecond)
		}
		db, err := sql.Open("mysql", fmt.Sprintf("dump:111@tcp(127.0.0.1:%d)/", cn.GetServiceConfig().CN.Frontend.Port))
		require.NoError(t, err)
		defer db.Close()
		conn, err := db.Conn(ctx)
		require.NoError(t, err)
		defer conn.Close()
		exec := func(s string) { t.Helper(); _, err := conn.ExecContext(ctx, s); require.NoError(t, err, s) }
		name := strings.ToLower(testutils.GetDatabaseName(t))
		exec("set role moadmin")
		exec("create database `" + name + "`")
		defer func() {
			cleanupCtx, cleanupCancel := context.WithTimeout(context.Background(), 30*time.Second)
			defer cleanupCancel()
			_, err := conn.ExecContext(cleanupCtx, "drop database `"+name+"`")
			require.NoError(t, err)
		}()
		exec("use `" + name + "`")
		exec("set @@max_dop=16")
		exec("set session optimizer_hints=''")
		defer resetOptimizerHintsOnCN(t, cn.GetServiceConfig().CN.Frontend.Port)
		exec("create table dwd_bw_1cpmb_bkgd4b76(cpmb_kgd4b76 varchar(64), parenth1 varchar(64))")
		exec("create table dwd_s4_bkpf(belnr varchar(64),bukrs varchar(64),gjahr varchar(64),bldat varchar(64))")
		exec("create table dwd_s4_bseg(belnr varchar(64),bukrs varchar(64),gjahr varchar(64),buzei varchar(64),hkont varchar(64),dmbtr decimal(20,2),zuonr varchar(64))")
		exec("create table payment_details(belnr varchar(64),bukrs varchar(64),gjahr varchar(64),payment_type varchar(64))")
		exec("insert into dwd_bw_1cpmb_bkgd4b76 values ('200000',''),('200001','200000'),('200002','200001'),('200003','200002')")
		// More than two storage blocks distributes actual scan work across CNs.
		exec("insert into dwd_s4_bkpf select cast(result as varchar), '1000','2024','2024-01-01' from generate_series(1,20000) g")
		exec("insert into dwd_s4_bseg select cast(result as varchar), '1000','2024','1','200003',1,'x' from generate_series(1,20000) g")
		exec("insert into payment_details select cast(result as varchar), '1000','2024','cash' from generate_series(1,20000) g")
		tables := []string{"dwd_s4_bkpf", "dwd_s4_bseg", "payment_details"}
		for _, table := range tables {
			exec("select mo_ctl('dn','flush','" + name + "." + table + "')")
		}
		query := strings.NewReplacer("dwd_dcp.", "`"+name+"`.", "jst_receipts_tables.", "`"+name+"`.").Replace(issue28313SQL)
		// Warm the cache before patching statistics, then retain default join order.
		_ = queryJoinSpillText(t, ctx, conn, "explain "+query)
		for _, table := range tables {
			ndv := map[string]int{}
			for _, col := range []string{"belnr", "bukrs", "gjahr", "bldat", "buzei", "hkont", "dmbtr", "zuonr", "payment_type"} {
				ndv[col] = 1
				if col == "belnr" {
					ndv[col] = 100_000_000
				}
			}
			stats, err := json.Marshal(map[string]any{"table_cnt": 100_000_000, "block_number": 2048, "accurate_object_number": 128, "approx_object_number": 128, "ndv_map": ndv})
			require.NoError(t, err)
			var patched float64
			require.NoError(t, conn.QueryRowContext(ctx, fmt.Sprintf("select table_cnt from table_stats('%s.%s', 'patch', '%s') g", name, table, stats)).Scan(&patched))
			require.Equal(t, float64(100_000_000), patched)
		}
		plan := queryJoinSpillText(t, ctx, conn, "explain "+query)
		require.Contains(t, plan, "shuffle:")
		remoteAttaches := func() uint64 {
			metric := new(dto.Metric)
			require.NoError(t, metricv2.PipelineRemoteReceiverWaitReadyHistogram.(prometheus.Metric).Write(metric))
			return metric.GetHistogram().GetSampleCount()
		}
		before := remoteAttaches()
		for _, want := range []int{500, 0, 500} {
			switch want {
			case 0:
				exec("delete from dwd_bw_1cpmb_bkgd4b76 where cpmb_kgd4b76='200000'")
			case 500:
				// Restore the root after the empty-result phase to also check reuse.
				exec("insert into dwd_bw_1cpmb_bkgd4b76 select '200000','' where not exists (select 1 from dwd_bw_1cpmb_bkgd4b76 where cpmb_kgd4b76='200000')")
			}
			for iteration := 0; iteration < 3; iteration++ {
				count := func() int {
					rows, err := conn.QueryContext(ctx, query)
					require.NoError(t, err, "expected %d rows, iteration %d", want, iteration)
					defer func() { require.NoError(t, rows.Close()) }()
					count := 0
					for rows.Next() {
						count++
					}
					require.NoError(t, rows.Err())
					return count
				}()
				require.Equal(t, want, count)
				if want == 500 {
					require.Greater(t, remoteAttaches(), before, "query must exercise remote receiver attachment")
					before = remoteAttaches()
				}
			}
		}
	})
}
