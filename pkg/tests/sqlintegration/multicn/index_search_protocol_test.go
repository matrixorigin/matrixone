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
	moruntime "github.com/matrixorigin/matrixone/pkg/common/runtime"
	"github.com/matrixorigin/matrixone/pkg/defines"
	"github.com/matrixorigin/matrixone/pkg/embed"
	"github.com/matrixorigin/matrixone/pkg/pb/metadata"
	"github.com/matrixorigin/matrixone/pkg/sql/colexec/vectorscan"
	"github.com/matrixorigin/matrixone/pkg/sql/plan"
	"github.com/stretchr/testify/require"
)

// An index search dispatched to a CN that reports a protocol version below the
// index search scan's fails the query; with that CN at the current version the
// same query returns the exact rows.
func TestIndexSearchScanRefusesOlderCN(t *testing.T) {
	cluster, err := embed.StartTestCluster(embed.WithCNCount(2))
	if cluster != nil {
		t.Cleanup(func() { require.NoError(t, cluster.Close()) })
	}
	require.NoError(t, err)
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Minute)
	defer cancel()

	coordinator, err := cluster.GetCNService(0)
	require.NoError(t, err)
	older, err := cluster.GetCNService(1)
	require.NoError(t, err)
	inventory := clusterservice.GetMOCluster(coordinator.ServiceID())
	refresher, ok := inventory.(clusterservice.AuthoritativeRefresher)
	require.True(t, ok)
	require.Eventually(t, func() bool {
		if refresher.Refresh(ctx) != nil {
			return false
		}
		count := 0
		inventory.GetCNService(clusterservice.NewSelector(), func(metadata.CNService) bool {
			count++
			return true
		})
		return count == 2
	}, 30*time.Second, 100*time.Millisecond)

	oldForce := plan.GetForceScanOnMultiCN()
	t.Cleanup(func() { plan.SetForceScanOnMultiCN(oldForce) })
	olderRuntime := moruntime.ServiceRuntime(older.ServiceID())
	oldVersion, hadVersion := olderRuntime.GetGlobalVariables(moruntime.MOProtocolVersion)
	restore := func() {
		if hadVersion {
			olderRuntime.SetGlobalVariables(moruntime.MOProtocolVersion, oldVersion)
		} else {
			olderRuntime.SetGlobalVariables(moruntime.MOProtocolVersion, defines.MORPCLatestVersion)
		}
	}
	t.Cleanup(restore)

	db, err := sql.Open("mysql", fmt.Sprintf("dump:111@tcp(127.0.0.1:%d)/", coordinator.GetServiceConfig().CN.Frontend.Port))
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, db.Close()) })
	conn, err := db.Conn(ctx)
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, conn.Close()) })
	exec := func(statement string) {
		t.Helper()
		_, err := conn.ExecContext(ctx, statement)
		require.NoError(t, err, statement)
	}
	query := func(statement string) ([]string, error) {
		rows, err := conn.QueryContext(ctx, statement)
		if err != nil {
			return nil, err
		}
		defer rows.Close()
		var out []string
		for rows.Next() {
			var id string
			if err := rows.Scan(&id); err != nil {
				return nil, err
			}
			out = append(out, id)
		}
		return out, rows.Err()
	}

	const schema = "index_search_protocol"
	for _, statement := range []string{
		"create database " + schema, "use " + schema,
		"set experimental_ivf_index = 1",
		"create table t(id bigint primary key, tag int, v vecf32(3))",
		"insert into t select result, result % 7, concat('[', result, ',', result + 1, ',', result + 2, ']') from generate_series(1, 2000) g",
		"create index vi using ivfflat on t(v) lists = 4 op_type 'vector_l2_ops'",
	} {
		exec(statement)
	}
	_, err = conn.ExecContext(ctx, "select mo_ctl('dn', 'flush', '"+schema+".t')")
	require.NoError(t, err)

	const search = "select id from t order by l2_distance(v, '[0,0,0]') limit 3"
	plan.SetForceScanOnMultiCN(true)

	// With every CN current, the search is dispatched to both CNs.
	physicalPlan := func() string {
		rows, err := conn.QueryContext(ctx, "explain phyplan analyze "+search)
		require.NoError(t, err)
		defer rows.Close()
		var plans strings.Builder
		for rows.Next() {
			var line string
			require.NoError(t, rows.Scan(&line))
			plans.WriteString(line + "\n")
		}
		require.NoError(t, rows.Err())
		return plans.String()
	}
	plans := physicalPlan()
	require.GreaterOrEqual(t, strings.Count(plans, "DataSource: "+schema+".t[pkid score]"), 2,
		"the index search must run on both CNs:\n%s", plans)

	olderRuntime.SetGlobalVariables(moruntime.MOProtocolVersion, vectorscan.IndexSearchScanProtocolVersion-1)
	_, err = query(search)
	require.Error(t, err, "an index search dispatched to an older CN must fail")
	require.ErrorContains(t, err, fmt.Sprintf("index search scan requires MORPC protocol version %d on every CN", vectorscan.IndexSearchScanProtocolVersion))

	restore()
	got, err := query(search)
	require.NoError(t, err)
	require.Equal(t, []string{"1", "2", "3"}, got)
}
