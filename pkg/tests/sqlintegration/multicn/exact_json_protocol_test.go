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

	"github.com/matrixorigin/matrixone/pkg/clusterservice"
	moruntime "github.com/matrixorigin/matrixone/pkg/common/runtime"
	"github.com/matrixorigin/matrixone/pkg/defines"
	"github.com/matrixorigin/matrixone/pkg/embed"
	"github.com/matrixorigin/matrixone/pkg/pb/metadata"
	"github.com/matrixorigin/matrixone/pkg/sql/plan"
	"github.com/stretchr/testify/require"
)

// Exercise real SQL, placement and capability RPCs. Both services run the new
// binary; only the worker's advertised protocol changes. The unchanged worker-
// wire tests also run against the pre-feature source to prove executor differences.
func TestExactJSONProtocolSQLFallbackAndUpgrade(t *testing.T) {
	cluster, err := embed.StartTestCluster(embed.WithCNCount(2))
	if cluster != nil {
		t.Cleanup(func() { require.NoError(t, cluster.Close()) })
	}
	require.NoError(t, err)
	ctx, cancel := context.WithTimeout(t.Context(), 3*time.Minute)
	defer cancel()
	coordinator, err := cluster.GetCNService(0)
	require.NoError(t, err)
	worker, err := cluster.GetCNService(1)
	require.NoError(t, err)
	inventory := clusterservice.GetMOCluster(coordinator.ServiceID())
	refresher, ok := inventory.(clusterservice.AuthoritativeRefresher)
	require.True(t, ok)
	require.Eventually(t, func() bool {
		if refresher.Refresh(ctx) != nil {
			return false
		}
		count := 0
		inventory.GetCNService(clusterservice.NewSelector(), func(metadata.CNService) bool { count++; return true })
		return count == 2
	}, 30*time.Second, 100*time.Millisecond)
	var workerAddress string
	inventory.GetCNService(clusterservice.NewServiceIDSelector(worker.ServiceID()), func(cn metadata.CNService) bool {
		workerAddress = cn.PipelineServiceAddress
		return false
	})
	require.NotEmpty(t, workerAddress)

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
	query := func(statement string) []string {
		t.Helper()
		rows, err := conn.QueryContext(ctx, statement)
		require.NoError(t, err, statement)
		defer rows.Close()
		var result []string
		for rows.Next() {
			var value string
			require.NoError(t, rows.Scan(&value))
			result = append(result, value)
		}
		require.NoError(t, rows.Err())
		return result
	}
	const schema = "exact_json_protocol"
	exec("create database " + schema)
	exec("use " + schema)
	exec("create table t(id bigint primary key, s varchar(200), j json)")
	exec(`insert into t values
		(1,'{"x":"v","cost":9007199254740993.001}',json_parse_exact('{"cost":9007199254740993.001}')),
		(2,'{"x":"v","cost":0.125}',json_parse_exact('{"cost":0.125}')),
		(3,'{"x":"other"}',json_parse_exact('{"cost":null}')),
		(4,NULL,NULL)`)
	exec("select mo_ctl('dn','flush','" + schema + ".t')")
	oldForce := plan.GetForceScanOnMultiCN()
	t.Cleanup(func() { plan.SetForceScanOnMultiCN(oldForce) })
	plan.SetForceScanOnMultiCN(true)
	rt := moruntime.ServiceRuntime(worker.ServiceID())
	oldVersion, hadVersion := rt.GetGlobalVariables(moruntime.MOProtocolVersion)
	t.Cleanup(func() {
		if hadVersion {
			rt.SetGlobalVariables(moruntime.MOProtocolVersion, oldVersion)
		} else {
			rt.SetGlobalVariables(moruntime.MOProtocolVersion, defines.MORPCLatestVersion)
		}
	})
	const parserQuery = `select cast(id as varchar) from t where json_extract_string(json_parse_exact(s),'$.x')='v' order by id`
	const castQuery = `select cast(cast(json_extract(j,'$.cost') as decimal(38,3)) as varchar) from t where id <= 2 order by id`
	for _, version := range []int64{defines.MORPCVersion118, defines.MORPCVersion119} {
		rt.SetGlobalVariables(moruntime.MOProtocolVersion, version)
		require.Equal(t, []string{"1", "2"}, query(parserQuery))
		require.Equal(t, []string{"9007199254740993.001", "0.125"}, query(castQuery))
		require.Equal(t, []string{"4"}, query("select cast(count(*) as varchar) from t"))
		for _, statement := range []string{parserQuery, castQuery} {
			physical := strings.Join(query("explain phyplan analyze "+statement), "\n")
			require.Contains(t, physical, "DataSource: "+schema+".t[")
			if version == defines.MORPCVersion118 {
				require.NotContains(t, physical, "addr:"+workerAddress,
					"unsupported worker must be absent from execution:\n%s", physical)
			} else {
				require.Contains(t, physical, "addr:"+workerAddress,
					"upgraded worker must execute the distributed control:\n%s", physical)
			}
		}
	}
}
