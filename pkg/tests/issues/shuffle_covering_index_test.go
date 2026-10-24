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

package issues

import (
	"context"
	"database/sql"
	"fmt"
	"regexp"
	"strings"
	"testing"
	"time"

	"github.com/matrixorigin/matrixone/pkg/clusterservice"
	"github.com/matrixorigin/matrixone/pkg/embed"
	"github.com/matrixorigin/matrixone/pkg/pb/metadata"
	"github.com/matrixorigin/matrixone/pkg/sql/plan"
	"github.com/matrixorigin/matrixone/pkg/tests/testutils"
	"github.com/matrixorigin/matrixone/pkg/util/executor"
	"github.com/stretchr/testify/require"
)

func TestCoveringIndexShuffleSQL(t *testing.T) {
	embed.RunBaseClusterTests(t, func(c embed.Cluster) {
		ctx, cancel := context.WithTimeout(context.Background(), 180*time.Second)
		defer cancel()
		cn, err := c.GetCNService(0)
		require.NoError(t, err)
		_, err = c.GetCNService(2)
		require.NoError(t, err)
		membership := clusterservice.GetMOCluster(cn.ServiceID())
		tick := time.NewTicker(100 * time.Millisecond)
		defer tick.Stop()
		for {
			membership.ForceRefresh(true)
			working := 0
			membership.GetCNService(clusterservice.NewSelector(), func(metadata.CNService) bool { working++; return true })
			if working == 3 {
				break
			}
			select {
			case <-ctx.Done():
				t.Fatalf("three working CNs not visible: count=%d err=%v", working, ctx.Err())
			case <-tick.C:
			}
		}

		db, err := sql.Open("mysql", fmt.Sprintf("dump:111@tcp(127.0.0.1:%d)/", cn.GetServiceConfig().CN.Frontend.Port))
		require.NoError(t, err)
		defer db.Close()
		conn, err := db.Conn(ctx)
		require.NoError(t, err)
		defer conn.Close()
		name := strings.ToLower(testutils.GetDatabaseName(t))
		mustExec(t, ctx, conn, "create database "+name)
		defer func() {
			cleanup, stop := context.WithTimeout(context.Background(), 10*time.Second)
			defer stop()
			_, err := db.ExecContext(cleanup, "drop database if exists "+name)
			require.NoError(t, err)
		}()
		mustExec(t, ctx, conn, "use "+name)
		// 固定阈值要求足够的 NDV；该规模只用于证明真实规划器路径。
		// 多 CN 使用已有开关，不为跨过 512-block 阈值额外造数。
		for _, query := range []string{
			"create table vl(id bigint primary key, racct varchar(16), hid varchar(255), key idx_racct_hid(racct,hid))",
			"create table vm(hid varchar(255) primary key)",
			"insert into vl select result, if(result%10=0,'other','6608010000'), concat(lpad(cast(result as char),8,'0'),'-voucher') from generate_series(1,300000) g",
			"insert into vl values (300001,'6608010000','00000001-voucher'),(300002,'6608010000','00000001-voucher'),(300003,'6608010000',null),(300004,'6608010000',null),(300005,'6608010000','99999999-unmatched')",
			"insert into vm select concat(lpad(cast(result as char),8,'0'),'-voucher') from generate_series(1,149000) g",
		} {
			mustExec(t, ctx, conn, query)
		}
		// 基础集群跳过用户认证；管理员维护动作通过已有内部 executor 执行。
		for _, table := range []string{"vl", "vm"} {
			res, err := testutils.GetSQLExecutor(cn).Exec(ctx,
				fmt.Sprintf("select mo_ctl('dn','flush','%s.%s')", name, table),
				executor.Options{}.WithWaitCommittedLogApplied())
			require.NoError(t, err)
			res.Close()
		}
		readPlan := func(t *testing.T, query string) string {
			t.Helper()
			rows, err := conn.QueryContext(ctx, query)
			require.NoError(t, err)
			defer rows.Close()
			var text strings.Builder
			for rows.Next() {
				var line string
				require.NoError(t, rows.Scan(&line))
				text.WriteString(line)
				text.WriteByte('\n')
			}
			require.NoError(t, rows.Err())
			return text.String()
		}
		previous := plan.GetForceScanOnMultiCN()
		defer plan.SetForceScanOnMultiCN(previous)
		for _, topology := range []struct {
			name  string
			multi bool
		}{{"single_cn", false}, {"multi_cn", true}} {
			t.Run(topology.name, func(t *testing.T) {
				plan.SetForceScanOnMultiCN(topology.multi)
				for _, tc := range []struct {
					name  string
					query string
					count int64
				}{
					{"distinct", "select count(1) from (select distinct hid from vl where racct='6608010000') q", 270002},
					{"group", "select count(1) from (select hid from vl where racct='6608010000' group by hid) q", 270002},
					{"join", "select count(1) from vl join vm on vl.hid=vm.hid where vl.racct='6608010000'", 134102},
					{"group_join", "select count(1) from vm join (select hid from vl where racct='6608010000' group by hid) g on vm.hid=g.hid", 134100},
				} {
					t.Run(tc.name, func(t *testing.T) {
						logical := readPlan(t, "explain verbose "+tc.query)
						require.Contains(t, logical, "Index Table Scan")
						require.Contains(t, logical, "shuffle: range(serial_extract")
						var count int64
						require.NoError(t, conn.QueryRowContext(ctx, tc.query).Scan(&count))
						require.Equal(t, tc.count, count)
						if tc.name == "group" || tc.name == "join" {
							physical := readPlan(t, "explain phyplan analyze "+tc.query)
							t.Log(physical)
							addresses := make(map[string]struct{})
							for _, match := range regexp.MustCompile(`Magic: Remote, addr:([^,]+),`).FindAllStringSubmatch(physical, -1) {
								addresses[match[1]] = struct{}{}
							}
							if topology.multi {
								require.GreaterOrEqual(t, len(addresses), 2, "must execute on multiple CNs: %s", physical)
							} else {
								require.LessOrEqual(t, len(addresses), 1, "must stay on one CN: %s", physical)
							}
						}
					})
				}
			})
		}
		var alive int
		require.NoError(t, conn.QueryRowContext(ctx, "select 1").Scan(&alive))
		require.Equal(t, 1, alive)
	})
}
