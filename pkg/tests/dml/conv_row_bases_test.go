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

// Reuse the shared two-CN fixture; two persisted rows force the real remote
// consumer without introducing a large workload or an additional cluster.
func TestConvRowBasesRemoteFallback(t *testing.T) {
	var fixtureInvalidationErr error
	// Run holds the fixture mutex; discard only after all callback defers run.
	defer func() {
		if fixtureInvalidationErr != nil {
			t.Errorf("discarding shared two-CN fixture after an unverified work-state transition: %v", fixtureInvalidationErr)
			if err := embed.CloseBaseClusterTests(); err != nil {
				t.Errorf("failed to discard shared two-CN fixture: %v", err)
			}
		}
	}()
	embed.RunBaseClusterTests(t, func(c embed.Cluster) {
		ctx, cancel := context.WithTimeout(context.Background(), 2*time.Minute)
		defer cancel()
		cn, err := c.GetCNService(0)
		require.NoError(t, err)
		peer, err := c.GetCNService(1)
		require.NoError(t, err)
		clusterInventory := clusterservice.GetMOCluster(cn.ServiceID())
		inventory, ok := clusterInventory.(cnWorkStateInventory)
		require.True(t, ok, "CN inventory must support caller-bounded work-state updates")
		refresher, ok := clusterInventory.(clusterservice.AuthoritativeRefresher)
		require.True(t, ok, "CN inventory must support authoritative refresh")
		readinessCtx, cancelReadiness := context.WithTimeout(ctx, 30*time.Second)
		defer cancelReadiness()
		readiness, readinessErr := waitForCNReadiness(
			readinessCtx, cnWorkStatePollInterval, inventory, refresher, cn.ServiceID(), peer.ServiceID())
		cancelReadiness()
		require.NoError(t, readinessErr,
			"last refresh error=%v, admission-ready CNs=%v, normally discoverable CNs=%v",
			readiness.lastRefreshErr, readiness.admissionReady, readiness.normallyDiscoverable)
		peerAddr := readiness.peerAddr
		db := openRetestSQLDB(t, c)
		defer db.Close()
		name := strings.ToLower(testutils.GetDatabaseName(t))
		defer func() {
			if fixtureInvalidationErr == nil {
				cleanupTestDatabases(t, db, name)
			}
		}()
		execSQLDB(t, ctx, db, "create database "+name)
		execSQLDB(t, ctx, db, "use "+name)
		execSQLDB(t, ctx, db, "create table src(id int,n varchar(20),f bigint)")
		execSQLDB(t, ctx, db, "insert into src values(1,'ff',16),(2,'1010',2)")
		execSQLDB(t, ctx, db, "select mo_ctl('dn','flush','"+name+".src')")
		runQueries := func() {
			oldForce := plan.GetForceScanOnMultiCN()
			plan.SetForceScanOnMultiCN(true)
			defer plan.SetForceScanOnMultiCN(oldForce)
			const query = "select group_concat(conv(n,f,10) order by id) from src"
			physical, err := testutils.QueryTextResult(ctx, db, "explain phyplan analyze "+query)
			require.NoError(t, err)
			require.Contains(t, physical.Text, peerAddr)
			var value string
			require.NoError(t, db.QueryRowContext(ctx, query).Scan(&value))
			require.Equal(t, "255,10", value)
			rt := moruntime.ServiceRuntime(peer.ServiceID())
			oldVersion, _ := rt.GetGlobalVariables(moruntime.MOProtocolVersion)
			rt.SetGlobalVariables(moruntime.MOProtocolVersion, defines.MORPCVersion69)
			defer rt.SetGlobalVariables(moruntime.MOProtocolVersion, oldVersion)
			physical, err = testutils.QueryTextResult(ctx, db, "explain phyplan analyze "+query)
			require.NoError(t, err)
			require.NotContains(t, physical.Text, peerAddr)
			require.NoError(t, db.QueryRowContext(ctx, query).Scan(&value))
			require.Equal(t, "255,10", value)
			stmt, err := db.PrepareContext(ctx, "select conv(?,?,?)")
			require.NoError(t, err)
			defer stmt.Close()
			for _, tc := range []struct {
				n     string
				f, to int64
				want  string
			}{{"ff", 16, 10, "255"}, {"1010", 2, 16, "A"}, {"-10", 10, -16, "-A"}} {
				require.NoError(t, stmt.QueryRowContext(ctx, tc.n, tc.f, tc.to).Scan(&value))
				require.Equal(t, tc.want, value)
			}
		}
		stateErr := withCNDraining(ctx, inventory, refresher, cn.ServiceID(),
			[]string{cn.ServiceID(), peer.ServiceID()},
			func(err error) { fixtureInvalidationErr = err }, runQueries)
		require.NoError(t, stateErr)
	})
}
