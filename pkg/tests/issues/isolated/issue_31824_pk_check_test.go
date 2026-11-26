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
	"testing"
	"time"

	_ "github.com/go-sql-driver/mysql"
	"github.com/matrixorigin/matrixone/pkg/config"
	"github.com/matrixorigin/matrixone/pkg/embed"
	"github.com/matrixorigin/matrixone/pkg/pb/metadata"
	"github.com/matrixorigin/matrixone/pkg/pb/txn"
	"github.com/stretchr/testify/require"
)

func TestIssue31824RepeatedIgnorePrimaryKeyCheck(t *testing.T) {
	previous := config.CNPrimaryCheck.Load()
	t.Cleanup(func() { config.CNPrimaryCheck.Store(previous) })
	for _, check := range []bool{true, false} {
		t.Run(fmt.Sprintf("CN_primary_key_check_%t", check), func(t *testing.T) {
			cluster, err := embed.StartTestCluster(embed.WithCNCount(1), embed.WithPreStart(func(s embed.ServiceOperator) {
				if s.ServiceType() == metadata.ServiceType_CN {
					s.Adjust(func(cfg *embed.ServiceConfig) {
						cfg.CN.Txn.Mode = txn.TxnMode_Optimistic.String()
						cfg.CN.Txn.Isolation = txn.TxnIsolation_SI.String()
						cfg.CN.PrimaryKeyCheck = check
						cfg.CN.Frontend.SkipCheckUser = false
					})
				}
			}))
			if cluster != nil {
				t.Cleanup(func() { require.NoError(t, cluster.Close()) })
			}
			require.NoError(t, err)
			cn, err := cluster.GetCNService(0)
			require.NoError(t, err)
			cfg := cn.GetServiceConfig().CN
			require.Equal(t, txn.TxnMode_Optimistic, txn.GetTxnMode(cfg.Txn.Mode))
			require.Equal(t, txn.TxnIsolation_SI, txn.GetTxnIsolation(cfg.Txn.Isolation))
			require.Equal(t, check, cfg.PrimaryKeyCheck)
			require.Equal(t, check, config.CNPrimaryCheck.Load(), "planner setting must follow the CN actually running the SQL")
			t.Logf("CN startup: txn=%s/%s primary-key-check=%t planner-check=%t", cfg.Txn.Mode, cfg.Txn.Isolation, cfg.PrimaryKeyCheck, config.CNPrimaryCheck.Load())

			ctx, cancel := context.WithTimeout(t.Context(), time.Minute)
			defer cancel()
			db, err := sql.Open("mysql", fmt.Sprintf("dump:111@tcp(127.0.0.1:%d)/", cfg.Frontend.Port))
			require.NoError(t, err)
			defer db.Close()
			conn, err := db.Conn(ctx)
			require.NoError(t, err)
			defer conn.Close()
			for _, query := range []string{"CREATE DATABASE repeated_pk_lookup", "USE repeated_pk_lookup",
				"CREATE TABLE t(id INT PRIMARY KEY, v INT)", "INSERT INTO t VALUES (1,10),(2,20)"} {
				_, err := conn.ExecContext(ctx, query)
				require.NoError(t, err, query)
			}
			rows := func() [][2]int {
				rs, err := conn.QueryContext(ctx, "SELECT id,v FROM t ORDER BY id")
				require.NoError(t, err)
				defer rs.Close()
				var result [][2]int
				for rs.Next() {
					var row [2]int
					require.NoError(t, rs.Scan(&row[0], &row[1]))
					result = append(result, row)
				}
				require.NoError(t, rs.Err())
				return result
			}
			require.Equal(t, [][2]int{{1, 10}, {2, 20}}, rows())
			for _, scenario := range []struct {
				key, value, count int
				want              [][2]int
			}{
				{2, 30, 1, [][2]int{{1, 30}, {2, 20}}},
				{3, 40, 2, [][2]int{{2, 20}, {3, 40}}},
			} {
				query := fmt.Sprintf("UPDATE IGNORE t x JOIN t y ON x.id=1 AND y.id=1 SET x.id=%d, y.v=%d", scenario.key, scenario.value)
				res, err := conn.ExecContext(ctx, query)
				if err != nil {
					t.Logf("After SQL error: rows=%v", rows())
				}
				require.NoError(t, err, query)
				actual, err := res.RowsAffected()
				require.NoError(t, err)
				require.EqualValues(t, scenario.count, actual)
				require.Equal(t, scenario.want, rows())
				t.Logf("proposal key=%d, affected=%d, rows=%v", scenario.key, actual, rows())
			}
		})
	}
}
