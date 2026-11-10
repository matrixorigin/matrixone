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
	"testing"
	"time"

	"github.com/matrixorigin/matrixone/pkg/embed"
	"github.com/matrixorigin/matrixone/pkg/tests/testutils"
	"github.com/stretchr/testify/require"
)

// Persisted inputs and analytical cost statistics select the real multi-CN
// shuffle topology. Neither a one-CN hint nor a local mock can exercise remote
// dispatch registration and drain ordering at this boundary.
func TestMultiCNShuffleJoinDrainsPersistedInputs(t *testing.T) {
	embed.RunBaseClusterTests(t, func(cluster embed.Cluster) {
		ctx, cancel := context.WithTimeout(t.Context(), 3*time.Minute)
		defer cancel()
		const database = "shuffle_multicn_drain"
		var connections []*sql.Conn
		for i := 0; i < 2; i++ {
			cn, err := cluster.GetCNService(i)
			require.NoError(t, err)
			db, err := sql.Open("mysql", fmt.Sprintf("dump:111@tcp(127.0.0.1:%d)/",
				cn.GetServiceConfig().CN.Frontend.Port))
			require.NoError(t, err)
			defer db.Close()
			conn, err := db.Conn(ctx)
			require.NoError(t, err)
			defer conn.Close()
			connections = append(connections, conn)
		}
		setup := connections[0]
		execJoinSpillSQL(t, ctx, setup, "create database "+database)
		defer func() {
			cleanupCtx, stop := context.WithTimeout(context.Background(), 30*time.Second)
			defer stop()
			_, _ = setup.ExecContext(cleanupCtx, "drop database "+database)
		}()
		execJoinSpillSQL(t, ctx, setup, "use "+database)
		for _, input := range []struct {
			table string
			rows  int
		}{{"probe", 100000}, {"build", 80000}} {
			execJoinSpillSQL(t, ctx, setup, "create table "+input.table+
				"(k int not null, payload int not null) cluster by k")
			execJoinSpillSQL(t, ctx, setup, fmt.Sprintf(
				"insert into %s select result*50+result%%50, result from generate_series(%d) g",
				input.table, input.rows))
			execJoinSpillSQL(t, ctx, setup, "select mo_ctl('dn','flush','"+database+"."+input.table+"')")
		}
		for coordinator, conn := range connections {
			execJoinSpillSQL(t, ctx, conn, "use "+database)
			patchJoinSpillStats(t, ctx, conn, database, "probe", 5000000)
			patchJoinSpillStats(t, ctx, conn, database, "build", 4000000)
			const query = "select count(*) from probe p join build b on p.k=b.k"
			plan, err := testutils.QueryTextResult(ctx, conn, "explain "+query)
			require.NoError(t, err)
			require.Contains(t, plan.ColumnName, "AP QUERY PLAN ON MULTICN", plan)
			require.Contains(t, plan.Text, "shuffle: range", plan)
			for _, spill := range []int{0, 1000} {
				t.Logf("coordinator=%d spill=%d", coordinator, spill)
				execJoinSpillSQL(t, ctx, conn, fmt.Sprintf("set session join_spill_mem=%d", spill))
				queryCtx, stop := context.WithTimeout(ctx, 30*time.Second)
				var count int64
				err := conn.QueryRowContext(queryCtx, query).Scan(&count)
				stop()
				require.NoError(t, err)
				require.Equal(t, int64(80000), count)
			}
		}
	})
}
