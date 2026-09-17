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

package dml

import (
	"context"
	"strings"
	"testing"
	"time"

	"github.com/matrixorigin/matrixone/pkg/clusterservice"
	moruntime "github.com/matrixorigin/matrixone/pkg/common/runtime"
	"github.com/matrixorigin/matrixone/pkg/defines"
	"github.com/matrixorigin/matrixone/pkg/embed"
	"github.com/matrixorigin/matrixone/pkg/sql/plan"
	"github.com/matrixorigin/matrixone/pkg/tests/testutils"
	"github.com/stretchr/testify/require"
)

// TestJSONMinMaxMixedVersionRemoteTopology executes the protocol counterexample
// through a real two-CN SQL topology. The coordinator runs the candidate
// code, while the peer is first made legacy-compatible (v80) and then capable
// (candidate v85).
// Draining the coordinator makes the peer the actual remote destination rather
// than merely inspecting a mocked pipeline or version response.
func TestJSONMinMaxMixedVersionRemoteTopology(t *testing.T) {
	var fixtureInvalidationErr error
	defer func() {
		if fixtureInvalidationErr == nil {
			return
		}
		if err := embed.CloseBaseClusterTests(); err != nil {
			t.Errorf("failed to discard shared two-CN fixture after an unverified work-state transition (%v): %v", fixtureInvalidationErr, err)
			return
		}
		t.Logf("discarded shared two-CN fixture after an unverified work-state transition: %v", fixtureInvalidationErr)
	}()

	embed.RunBaseClusterTests(t, func(cluster embed.Cluster) {
		ctx, cancel := context.WithTimeout(context.Background(), 2*time.Minute)
		defer cancel()
		cn, err := cluster.GetCNService(0)
		require.NoError(t, err)
		peer, err := cluster.GetCNService(1)
		require.NoError(t, err)

		clusterInventory := clusterservice.GetMOCluster(cn.ServiceID())
		inventory, ok := clusterInventory.(cnWorkStateInventory)
		require.True(t, ok, "CN inventory must support caller-bounded work-state updates")
		refresher, ok := clusterInventory.(clusterservice.AuthoritativeRefresher)
		require.True(t, ok, "CN inventory must support authoritative refresh")
		readinessCtx, cancelReadiness := context.WithTimeout(ctx, 30*time.Second)
		readiness, readinessErr := waitForCNReadiness(
			readinessCtx, cnWorkStatePollInterval, inventory, refresher, cn.ServiceID(), peer.ServiceID())
		cancelReadiness()
		require.NoError(t, readinessErr,
			"last refresh error=%v, admission-ready CNs=%v, normally discoverable CNs=%v",
			readiness.lastRefreshErr, readiness.admissionReady, readiness.normallyDiscoverable)
		peerAddr := readiness.peerAddr

		db := openRetestSQLDB(t, cluster)
		defer db.Close()
		name := strings.ToLower(testutils.GetDatabaseName(t))
		defer func() {
			if fixtureInvalidationErr == nil {
				cleanupTestDatabases(t, db, name)
			}
		}()
		execSQLDB(t, ctx, db, "create database "+name)
		execSQLDB(t, ctx, db, "use "+name)
		execSQLDB(t, ctx, db, "create table src(id int, j json)")
		execSQLDB(t, ctx, db, "insert into src values (1, '1'), (2, '256')")
		execSQLDB(t, ctx, db, "select mo_ctl('dn','flush','"+name+".src')")

		oldForce := plan.GetForceScanOnMultiCN()
		plan.SetForceScanOnMultiCN(true)
		defer plan.SetForceScanOnMultiCN(oldForce)
		peerRuntime := moruntime.ServiceRuntime(peer.ServiceID())
		oldVersion, hadVersion := peerRuntime.GetGlobalVariables(moruntime.MOProtocolVersion)
		require.True(t, hadVersion, "peer CN must expose its protocol version")
		defer peerRuntime.SetGlobalVariables(moruntime.MOProtocolVersion, oldVersion)

		const query = "select min(j), max(j) from src"
		stateErr := withCNDraining(ctx, inventory, refresher, cn.ServiceID(),
			[]string{cn.ServiceID(), peer.ServiceID()},
			func(err error) { fixtureInvalidationErr = err },
			func() {
				// v80 is the current-main worker before this change. The
				// coordinator must reject remote JSON partials and fall back to
				// one-CN execution, while retaining the correct SQL result.
				peerRuntime.SetGlobalVariables(moruntime.MOProtocolVersion, defines.MORPCVersion80)
				physical, err := testutils.QueryTextResult(ctx, db, "explain phyplan analyze "+query)
				require.NoError(t, err)
				require.NotContains(t, physical.Text, peerAddr,
					"a v80 worker must not receive a JSON MIN/MAX partial")
				var minValue, maxValue string
				require.NoError(t, db.QueryRowContext(ctx, query).Scan(&minValue, &maxValue))
				require.Equal(t, "1", minValue)
				require.Equal(t, "256", maxValue)

				// Candidate v85 is the capable remote worker. The same production SQL
				// query must place the JSON aggregate on the peer and preserve
				// the typed ordering counterexample 1 < 256.
				peerRuntime.SetGlobalVariables(moruntime.MOProtocolVersion, defines.MORPCVersion85)
				physical, err = testutils.QueryTextResult(ctx, db, "explain phyplan analyze "+query)
				require.NoError(t, err)
				require.Contains(t, physical.Text, peerAddr,
					"a capable worker must execute the remote JSON MIN/MAX partial")
				require.NoError(t, db.QueryRowContext(ctx, query).Scan(&minValue, &maxValue))
				require.Equal(t, "1", minValue)
				require.Equal(t, "256", maxValue)
			},
		)
		require.NoError(t, stateErr)
	})
}
