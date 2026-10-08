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
	"strings"
	"testing"
	"time"

	"github.com/matrixorigin/matrixone/pkg/cnservice"
	"github.com/matrixorigin/matrixone/pkg/embed"
	"github.com/matrixorigin/matrixone/pkg/tests/testutils"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestTableStatisticsOwnerPolicySQL(t *testing.T) {
	embed.RunBaseClusterTests(t, func(c embed.Cluster) {
		ctx, cancel := context.WithTimeout(t.Context(), time.Minute)
		defer cancel()
		conns := make([]*sql.Conn, 2)
		services := make([]cnservice.Service, 2)
		controls := make([]interface{ HandleMoTableStatsCtl(string) string }, 2)
		for i := range conns {
			cn, err := c.GetCNService(i)
			require.NoError(t, err)
			services[i] = cn.RawService().(cnservice.Service)
			controls[i] = services[i].GetEngine().(interface{ HandleMoTableStatsCtl(string) string })
			var move, old, force bool
			_, err = fmt.Sscanf(controls[i].HandleMoTableStatsCtl("echo_current_setting:true"), "move_on(%t), use_old_impl(%t), force_update(%t)", &move, &old, &force)
			require.NoError(t, err)
			defer func(i int, move, old, force bool) {
				controls[i].HandleMoTableStatsCtl(fmt.Sprintf("move_on:%t", move))
				controls[i].HandleMoTableStatsCtl(fmt.Sprintf("use_old_impl:%t", old))
				controls[i].HandleMoTableStatsCtl(fmt.Sprintf("force_update:%t", force))
			}(i, move, old, force)
			db, err := sql.Open("mysql", fmt.Sprintf("dump:111@tcp(127.0.0.1:%d)/", cn.GetServiceConfig().CN.Frontend.Port))
			require.NoError(t, err)
			defer db.Close()
			conns[i], err = db.Conn(ctx)
			require.NoError(t, err)
			defer conns[i].Close()
			_, err = conns[i].ExecContext(ctx, "set mo_table_stats.use_old_impl='no'")
			require.NoError(t, err)
			controls[i].HandleMoTableStatsCtl("force_update:false")
		}
		dbName := strings.ToLower(testutils.GetDatabaseName(t))
		_, err := conns[0].ExecContext(ctx, "create database `"+dbName+"`")
		require.NoError(t, err)
		defer func() {
			cleanup, stop := context.WithTimeout(context.Background(), 10*time.Second)
			defer stop()
			_, err := conns[0].ExecContext(cleanup, "drop database `"+dbName+"`")
			assert.NoError(t, err)
		}()
		_, err = conns[0].ExecContext(ctx, "create table `"+dbName+"`.t (id int primary key)")
		require.NoError(t, err)
		_, err = conns[0].ExecContext(ctx, "insert into `"+dbName+"`.t values (1),(2),(3)")
		require.NoError(t, err)
		frontier := services[0].GetTxnClient().GetLatestCommitTS()
		_, err = services[1].GetTxnClient().WaitLogTailAppliedAt(ctx, frontier)
		require.NoError(t, err)
		query := fmt.Sprintf("select mo_table_rows('%s','t'), mo_table_size('%s','t')", dbName, dbName)
		for _, oldOwner := range []int{0, 1} {
			controls[oldOwner].HandleMoTableStatsCtl("use_old_impl:true")
			controls[1-oldOwner].HandleMoTableStatsCtl("use_old_impl:false")
			var rows, size int64
			require.NoError(t, conns[oldOwner].QueryRowContext(ctx, query).Scan(&rows, &size))
			require.Equal(t, int64(3), rows)
			require.Positive(t, size)
			var otherRows, otherSize int64
			require.NoError(t, conns[1-oldOwner].QueryRowContext(ctx, query).Scan(&otherRows, &otherSize))
			require.GreaterOrEqual(t, otherRows, int64(0))
			require.GreaterOrEqual(t, otherSize, int64(0))
			require.NoError(t, conns[oldOwner].QueryRowContext(ctx, query).Scan(&rows, &size))
			require.Equal(t, int64(3), rows, "querying the new-mode CN must not change the old-mode CN policy")
		}
	})
}
