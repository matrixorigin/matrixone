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

package multicn

import (
	"context"
	"database/sql"
	"fmt"
	"strings"
	"testing"
	"time"

	_ "github.com/go-sql-driver/mysql"
	"github.com/matrixorigin/matrixone/pkg/clusterservice"
	"github.com/matrixorigin/matrixone/pkg/embed"
	"github.com/matrixorigin/matrixone/pkg/pb/metadata"
	"github.com/matrixorigin/matrixone/pkg/sql/plan"
	"github.com/stretchr/testify/require"
)

// The window-dependent path prevents JSON_VALUE from running at the scan.
// Draining the coordinator forces JSON documents across the real CN transport
// before the coordinator evaluates the window and both JSON_VALUE overloads.
func TestJSONValueReceivedDocuments(t *testing.T) {
	cluster, err := embed.StartTestCluster(embed.WithCNCount(2))
	if cluster != nil {
		t.Cleanup(func() { require.NoError(t, cluster.Close()) })
	}
	require.NoError(t, err)
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Minute)
	defer cancel()
	cn, err := cluster.GetCNService(0)
	require.NoError(t, err)
	inventory := clusterservice.GetMOCluster(cn.ServiceID())
	refresher, ok := inventory.(clusterservice.AuthoritativeRefresher)
	require.True(t, ok)
	updater, ok := inventory.(clusterservice.CNWorkStateUpdaterWithContext)
	require.True(t, ok)
	var peer string
	waitWorkers := func(ctx context.Context, want int) {
		t.Helper()
		require.Eventually(t, func() bool {
			if refresher.Refresh(ctx) != nil {
				return false
			}
			count := 0
			inventory.GetCNService(clusterservice.NewSelector(), func(service metadata.CNService) bool {
				count++
				if service.ServiceID != cn.ServiceID() {
					peer = service.PipelineServiceAddress
				}
				return true
			})
			return count == want
		}, 30*time.Second, 100*time.Millisecond)
	}
	waitWorkers(ctx, 2)
	oldForce := plan.GetForceScanOnMultiCN()
	plan.SetForceScanOnMultiCN(true)
	t.Cleanup(func() { plan.SetForceScanOnMultiCN(oldForce) })
	db, err := sql.Open("mysql", fmt.Sprintf("dump:111@tcp(127.0.0.1:%d)/", cn.GetServiceConfig().CN.Frontend.Port))
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, db.Close()) })
	conn, err := db.Conn(ctx)
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, conn.Close()) })
	const schema = "json_value_received"
	t.Cleanup(func() {
		cleanupCtx, cleanupCancel := context.WithTimeout(context.Background(), 30*time.Second)
		defer cleanupCancel()
		require.NoError(t, updater.DebugUpdateCNWorkStateWithContext(cleanupCtx, cn.ServiceID(), int(metadata.WorkState_Working)))
		waitWorkers(cleanupCtx, 2)
		_, err := conn.ExecContext(cleanupCtx, "drop database if exists "+schema)
		require.NoError(t, err)
	})
	exec := func(statement string) {
		t.Helper()
		_, err := conn.ExecContext(ctx, statement)
		require.NoError(t, err)
	}
	for _, statement := range []string{
		"create database " + schema, "use " + schema,
		"create table docs(id bigint primary key, doc json)",
		`insert into docs values (1,'{"keep":11,"other":12}'),(2,'{"keep":21,"other":22}'),(3,'{"keep":31,"other":32}'),(4,'{"keep":41,"other":42}')`,
		"select mo_ctl('dn','flush','" + schema + ".docs')",
	} {
		exec(statement)
	}
	require.NoError(t, updater.DebugUpdateCNWorkStateWithContext(ctx, cn.ServiceID(), int(metadata.WorkState_Draining)))
	waitWorkers(ctx, 1)
	const query = `select id,json_value(doc,if(rn%2=0,'$.keep','$.other')),json_value(doc,if(rn%2=0,'$.keep','$.other') returning signed) from (select id,doc,row_number() over(order by id) rn from docs) d where id>=? order by id`
	prepared, err := conn.PrepareContext(ctx, query)
	require.NoError(t, err)
	defer prepared.Close()
	check := func(min int64, expected []int64) {
		t.Helper()
		rows, err := prepared.QueryContext(ctx, min)
		require.NoError(t, err)
		defer rows.Close()
		var actual []int64
		for rows.Next() {
			var id, number int64
			var text string
			require.NoError(t, rows.Scan(&id, &text, &number))
			require.Equal(t, fmt.Sprint(number), text)
			actual = append(actual, number)
		}
		require.NoError(t, rows.Err())
		require.Equal(t, expected, actual)
	}
	check(1, []int64{12, 21, 32, 41})
	check(3, []int64{32, 41})
	exec(`update docs set doc='{"keep":211,"other":212}' where id=2`)
	check(1, []int64{12, 211, 32, 41})
	rows, err := conn.QueryContext(ctx, "explain phyplan analyze "+strings.Replace(query, "?", "1", 1))
	require.NoError(t, err)
	defer rows.Close()
	var physical strings.Builder
	for rows.Next() {
		var line string
		require.NoError(t, rows.Scan(&line))
		physical.WriteString(line + "\n")
	}
	require.NoError(t, rows.Err())
	require.NotEmpty(t, peer)
	require.Contains(t, physical.String(), peer, "JSON scan must run on the other CN")
	require.Contains(t, strings.ToLower(physical.String()), "window")
}
