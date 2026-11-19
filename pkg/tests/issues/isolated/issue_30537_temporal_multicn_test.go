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

package isolated

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
	"github.com/matrixorigin/matrixone/pkg/tests/testutils"
	"github.com/stretchr/testify/require"
)

func TestIssue30537TemporalWarningsPeerCN(t *testing.T) {
	cluster, err := embed.StartTestCluster(embed.WithCNCount(2))
	if cluster != nil {
		t.Cleanup(func() { require.NoError(t, cluster.Close()) })
	}
	require.NoError(t, err)

	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Minute)
	defer cancel()
	coordinator, err := cluster.GetCNService(0)
	require.NoError(t, err)
	inventory := clusterservice.GetMOCluster(coordinator.ServiceID())
	refresher, ok := inventory.(clusterservice.AuthoritativeRefresher)
	require.True(t, ok)
	updater, ok := inventory.(clusterservice.CNWorkStateUpdaterWithContext)
	require.True(t, ok)

	var peerAddr string
	var peerServiceID string
	var coordinatorAddr string
	waitWorkers := func(ctx context.Context, want int) {
		t.Helper()
		require.Eventually(t, func() bool {
			if refresher.Refresh(ctx) != nil {
				return false
			}
			count := 0
			inventory.GetCNService(clusterservice.NewSelector(), func(service metadata.CNService) bool {
				count++
				if service.ServiceID == coordinator.ServiceID() {
					coordinatorAddr = service.PipelineServiceAddress
				} else {
					peerAddr = service.PipelineServiceAddress
					peerServiceID = service.ServiceID
				}
				return true
			})
			return count == want && (want != 1 || peerAddr != "")
		}, 30*time.Second, 100*time.Millisecond)
	}
	waitWorkers(ctx, 2)
	require.NotEmpty(t, peerAddr)

	db, err := sql.Open("mysql", fmt.Sprintf("dump:111@tcp(127.0.0.1:%d)/?interpolateParams=false", coordinator.GetServiceConfig().CN.Frontend.Port))
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, db.Close()) })
	db.SetMaxOpenConns(1)
	require.NoError(t, waitSystemBootstrap(ctx, db))
	conn, err := db.Conn(ctx)
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, conn.Close()) })
	const schema = "issue30537_peer_warning"
	t.Cleanup(func() {
		cleanupCtx, cleanupCancel := context.WithTimeout(context.Background(), 30*time.Second)
		defer cleanupCancel()
		require.NoError(t, updater.DebugUpdateCNWorkStateWithContext(cleanupCtx, coordinator.ServiceID(), int(metadata.WorkState_Working)))
		waitWorkers(cleanupCtx, 2)
		_, err := conn.ExecContext(cleanupCtx, "drop database if exists "+schema)
		require.NoError(t, err)
	})
	exec := func(statement string) {
		t.Helper()
		_, err := conn.ExecContext(ctx, statement)
		require.NoError(t, err, statement)
	}
	for _, statement := range []string{
		"drop database if exists " + schema,
		"create database " + schema,
		"use " + schema,
		"create table t(x varchar(20))",
		"insert into t values('bad'),('bad')",
		"select mo_ctl('dn','flush','" + schema + ".t')",
	} {
		exec(statement)
	}

	oldForce := plan.GetForceScanOnMultiCN()
	plan.SetForceScanOnMultiCN(true)
	t.Cleanup(func() { plan.SetForceScanOnMultiCN(oldForce) })
	require.NoError(t, updater.DebugUpdateCNWorkStateWithContext(ctx, coordinator.ServiceID(), int(metadata.WorkState_Draining)))
	waitWorkers(ctx, 1)
	require.NotEqual(t, coordinator.ServiceID(), peerServiceID)
	require.NotEqual(t, coordinatorAddr, peerAddr)
	t.Logf("forced remote placement: coordinator_cn=%s addr=%s peer_cn=%s addr=%s", coordinator.ServiceID(), coordinatorAddr, peerServiceID, peerAddr)

	query := "select cast(x as time) from t"
	physical, err := testutils.QueryTextResult(ctx, conn, "explain phyplan analyze "+query)
	require.NoError(t, err)
	t.Logf("physical plan: %s", physical.Text)
	inPeer := false
	peerProjection := false
	for _, line := range strings.Split(physical.Text, "\n") {
		trimmed := strings.TrimSpace(line)
		if strings.HasPrefix(trimmed, "Scope ") {
			inPeer = strings.Contains(trimmed, "Magic: Remote, addr:"+peerAddr+",")
		}
		if strings.HasPrefix(trimmed, "PreScopes:") || trimmed == "}" {
			inPeer = false
		}
		if inPeer && strings.Contains(line, "projection") {
			peerProjection = true
		}
	}
	require.True(t, peerProjection, "projection must be placed on peer: %s", physical.Text)
	require.Contains(t, physical.Text, "InRows:2 OutRows:2", "coordinator merge must consume the peer rows")
	for _, mode := range []string{"", "STRICT_TRANS_TABLES"} {
		exec("set sql_mode='" + mode + "'")
		for _, tc := range []struct {
			query          string
			rows, warnings int
		}{
			{query, 2, 2},
			{"select x from t where cast(x as time) is null", 2, 2},
			{"select x from t where cast('abc' as time) is null", 2, 1},
		} {
			func() {
				rows, err := conn.QueryContext(ctx, tc.query)
				require.NoError(t, err)
				defer func() { require.NoError(t, rows.Close()) }()
				count := 0
				for rows.Next() {
					var value any
					require.NoError(t, rows.Scan(&value))
					if tc.query == query {
						require.Nil(t, value)
					} else {
						require.Equal(t, "bad", string(value.([]byte)))
					}
					count++
				}
				require.NoError(t, rows.Err())
				var warnings int
				require.NoError(t, conn.QueryRowContext(ctx, "show count(*) warnings").Scan(&warnings))
				require.Equal(t, tc.rows, count, tc.query)
				require.Equal(t, tc.warnings, warnings, tc.query)
				warningRows, err := conn.QueryContext(ctx, "show warnings")
				require.NoError(t, err)
				defer func() { require.NoError(t, warningRows.Close()) }()
				seen := 0
				for warningRows.Next() {
					var level, message string
					var code int
					require.NoError(t, warningRows.Scan(&level, &code, &message))
					input := "bad"
					if strings.Contains(tc.query, "'abc'") {
						input = "abc"
					}
					require.Equal(t, "Warning", level)
					require.Equal(t, 1292, code)
					require.Equal(t, "Truncated incorrect time value: '"+input+"'", message)
					seen++
				}
				require.NoError(t, warningRows.Err())
				require.Equal(t, tc.warnings, seen)
			}()
		}
	}
}
