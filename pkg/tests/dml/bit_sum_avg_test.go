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
	"github.com/matrixorigin/matrixone/pkg/embed"
	"github.com/matrixorigin/matrixone/pkg/sql/plan"
	"github.com/matrixorigin/matrixone/pkg/tests/testutils"
	"github.com/stretchr/testify/require"
)

// Reuse the shared two-CN fixture; two persisted rows force the real remote
// consumer without introducing a large workload or an additional cluster.
func TestBitSumAvgExactRemote(t *testing.T) {
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
		execSQLDB(t, ctx, db, "create table src(id int,b bit(64))")
		execSQLDB(t, ctx, db, "insert into src values(1,9223372036854775808),(2,18446744073709551615)")
		execSQLDB(t, ctx, db, "select mo_ctl('dn','flush','"+name+".src')")
		oldForce := plan.GetForceScanOnMultiCN()
		plan.SetForceScanOnMultiCN(true)
		defer plan.SetForceScanOnMultiCN(oldForce)
		stateErr := withCNDraining(ctx, inventory, refresher, cn.ServiceID(),
			[]string{cn.ServiceID(), peer.ServiceID()},
			func(err error) {
				if fixtureInvalidationErr == nil {
					fixtureInvalidationErr = err
				}
			},
			func() {
				const query = "select sum(b),avg(b) from src"
				physical, err := testutils.QueryTextResult(ctx, db, "explain phyplan analyze "+query)
				require.NoError(t, err)
				require.Contains(t, physical.Text, peerAddr)
				var sum, avg string
				require.NoError(t, db.QueryRowContext(ctx, query).Scan(&sum, &avg))
				require.Equal(t, "27670116110564327423", sum)
				require.Equal(t, "13835058055282163711.5000", avg)
				stmt, err := db.PrepareContext(ctx, "select sum(b),avg(b) from src where id>=?")
				require.NoError(t, err)
				defer stmt.Close()
				require.NoError(t, stmt.QueryRowContext(ctx, 1).Scan(&sum, &avg))
				require.Equal(t, "27670116110564327423", sum)
				require.Equal(t, "13835058055282163711.5000", avg)
			},
		)
		require.NoError(t, stateErr)
	})
}
