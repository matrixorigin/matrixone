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

package embed

import (
	"context"
	"database/sql"
	"fmt"
	"testing"
	"time"

	"github.com/matrixorigin/matrixone/pkg/clusterservice"
	"github.com/matrixorigin/matrixone/pkg/logservice"
	"github.com/matrixorigin/matrixone/pkg/pb/metadata"
	"github.com/stretchr/testify/require"
)

// A long refresh interval isolates startup from the periodic inventory task.
// Once admission is enabled, a newly added CN must refresh its own admitted
// generation before SQL readiness, without SHOW BACKEND SERVERS or query retry.
func TestStartupQueryReadiness(t *testing.T) {
	c, err := StartTestCluster(WithCNCount(2), WithPreStart(func(op ServiceOperator) {
		if op.ServiceType() == metadata.ServiceType_CN {
			op.Adjust(func(cfg *ServiceConfig) {
				cfg.CN.Cluster.RefreshInterval.Duration = time.Hour
				cfg.CN.AutomaticUpgrade = true
			})
		}
	}))
	if c != nil {
		t.Cleanup(func() { require.NoError(t, c.Close()) })
	}
	require.NoError(t, err)
	ctx, cancel := context.WithTimeout(t.Context(), time.Minute)
	defer cancel()

	initial, err := c.GetCNService(0)
	require.NoError(t, err)
	client, err := logservice.NewCNHAKeeperClient(ctx, initial.GetServiceConfig().CN.UUID, initial.GetServiceConfig().HAKeeperClient)
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, client.Close()) })
	require.Eventually(t, func() bool {
		d, e := client.GetClusterDetails(ctx)
		return e == nil && d.ViewMetadataAdmission != nil && d.ViewMetadataAdmission.Enabled
	}, 30*time.Second, 100*time.Millisecond)
	require.NoError(t, c.StartNewCNService(1))
	cn, err := c.GetCNService(2)
	require.NoError(t, err)
	cfg := cn.GetServiceConfig().CN
	cluster, err := clusterservice.GetMOClusterWithContext(ctx, cfg.UUID)
	require.NoError(t, err)
	require.True(t, cluster.(clusterservice.ViewMetadataAdmissionReader).GetViewMetadataAdmission().Enabled)
	var admitted []string
	require.NoError(t, clusterservice.GetCNServiceWithoutWorkingStateWithContext(ctx, cluster, clusterservice.NewSelector(), func(cn metadata.CNService) bool {
		admitted = append(admitted, cn.ServiceID)
		return true
	}))
	db, err := sql.Open("mysql", fmt.Sprintf("dump:111@tcp(127.0.0.1:%d)/?interpolateParams=false", cfg.Frontend.Port))
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, db.Close()) })
	conn, err := db.Conn(ctx)
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, conn.Close()) })
	var one int
	require.NoError(t, conn.QueryRowContext(ctx, "select 1").Scan(&one))
	require.Equal(t, 1, one)
	for _, stmt := range []string{"create database startup_query_readiness", "use startup_query_readiness", "create table t(id int primary key, v vecf32(3))", "insert into t values (1,'[1,1,1]'),(2,'[2,2,2]'),(3,'[9,9,9]'),(4,'[10,10,10]')", "create index idx using ivfflat on t(v) lists=1 op_type 'vector_l2_ops'"} {
		_, err = conn.ExecContext(ctx, stmt)
		require.NoError(t, err, stmt)
	}
	rows, err := conn.QueryContext(ctx, "select id from t where l2_distance(v,'[1,1,1]') < 5 order by l2_distance(v,'[1,1,1]') limit 2")
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, rows.Close()) })
	var ids []int
	for rows.Next() {
		var id int
		require.NoError(t, rows.Scan(&id))
		ids = append(ids, id)
	}
	require.NoError(t, rows.Err())
	require.NoError(t, rows.Close())
	require.Equal(t, []int{1, 2}, ids)
	require.Contains(t, admitted, cfg.UUID)
}
