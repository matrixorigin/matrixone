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
	"regexp"
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

func TestIssue30708UnionNullTopExecutesOnPeerCN(t *testing.T) {
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
	const schema = "issue30708_peer_top"
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
		"create table t(x int)",
		"insert into t values(1),(2)",
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

	assertPeerRemoteScan := func(statement, topRows string) {
		t.Helper()
		physical, err := testutils.QueryTextResult(ctx, conn, "explain phyplan analyze "+statement)
		require.NoError(t, err, statement)
		t.Logf("query-owned physical profile for %q:\n%s", statement, physical.Text)
		require.Contains(t, physical.Text, "top", "the analyzed plan must execute a Top")
		remotePeerScan := false
		localTableSource := false
		inPeerScope := false
		for _, line := range strings.Split(physical.Text, "\n") {
			trimmed := strings.TrimSpace(line)
			if strings.HasPrefix(trimmed, "Scope ") {
				inPeerScope = strings.HasPrefix(trimmed, "Scope ") && strings.Contains(trimmed, "Magic: Remote, addr:"+peerAddr+",")
			}
			if strings.HasPrefix(trimmed, "PreScopes:") || trimmed == "}" {
				inPeerScope = false
			}
			if inPeerScope && strings.Contains(line, "tablescan") {
				remotePeerScan = true
			}
			if strings.Contains(trimmed, "DataSource: "+schema+".t[x]") && !inPeerScope {
				localTableSource = true
			}
		}
		require.True(t, remotePeerScan, "the peer CN must execute a non-empty remote TableScan; peer=%s\n%s", peerAddr, physical.Text)
		require.False(t, localTableSource, "the coordinator plan must not contain a local table-scan fallback\n%s", physical.Text)
		if topRows != "" {
			require.Regexp(t, regexp.QuoteMeta("top")+` CallNum:[^\n]*`+regexp.QuoteMeta(topRows), physical.Text,
				"the same query-owned runtime profile must show the Top consuming and producing rows")
		}
	}

	reported := "select NULL as x union all select x from t where x=2 order by x desc limit 2"
	assertPeerRemoteScan(reported, "InRows:2 OutRows:2")
	rows, err := conn.QueryContext(ctx, reported)
	require.NoError(t, err)
	defer rows.Close()
	var got []sql.NullInt64
	for rows.Next() {
		var value sql.NullInt64
		require.NoError(t, rows.Scan(&value))
		got = append(got, value)
	}
	require.NoError(t, rows.Err())
	require.NoError(t, rows.Close())
	require.Equal(t, 2, len(got))
	require.Equal(t, sql.NullInt64{Int64: 2, Valid: true}, got[0])
	require.Equal(t, sql.NullInt64{Valid: false}, got[1])

	control := "select x from t order by x desc limit 1"
	assertPeerRemoteScan(control, "")
	var winner int
	require.NoError(t, conn.QueryRowContext(ctx, control).Scan(&winner))
	require.Equal(t, 2, winner)
}
