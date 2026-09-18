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
	"database/sql"
	"fmt"
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

// Two persisted rows and the shared two-CN fixture are enough to put final
// GROUP_CONCAT rendering on a remote owner. No large/skewed data is required.
func TestGroupConcatNamedTimeZoneRemoteOwner(t *testing.T) {
	var fixtureInvalidationErr error
	// Run holds the fixture mutex. Discard only after its callback unwinds,
	// including when an assertion calls Goexit.
	defer func() {
		if fixtureInvalidationErr != nil {
			t.Errorf("discarding shared two-CN fixture after an unverified work-state transition: %v", fixtureInvalidationErr)
			if err := embed.CloseBaseClusterTests(); err != nil {
				t.Errorf("failed to discard shared two-CN fixture: %v", err)
			}
		}
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
		defer cancelReadiness()
		readiness, readinessErr := waitForCNReadiness(
			readinessCtx, cnWorkStatePollInterval, inventory, refresher, cn.ServiceID(), peer.ServiceID())
		cancelReadiness()
		require.NoError(t, readinessErr,
			"last refresh error=%v, admission-ready CNs=%v, normally discoverable CNs=%v",
			readiness.lastRefreshErr, readiness.admissionReady, readiness.normallyDiscoverable)
		peerAddr := readiness.peerAddr
		db, err := sql.Open("mysql", fmt.Sprintf("dump:111@tcp(127.0.0.1:%d)/", cn.GetServiceConfig().CN.Frontend.Port))
		require.NoError(t, err)
		defer db.Close()
		db.SetMaxOpenConns(1)
		name := strings.ToLower(testutils.GetDatabaseName(t))
		defer func() {
			if fixtureInvalidationErr == nil {
				cleanupTestDatabases(t, db, name)
			}
		}()
		execSQLDB(t, ctx, db, "create database "+name)
		execSQLDB(t, ctx, db, "use "+name)
		execSQLDB(t, ctx, db, "set time_zone='+00:00'")
		execSQLDB(t, ctx, db, "create table seasonal (id int, ts timestamp)")
		execSQLDB(t, ctx, db, "insert into seasonal values (1,'2024-01-01 00:00:00'),(2,'2024-07-01 00:00:00')")
		execSQLDB(t, ctx, db, "select mo_ctl('dn','flush','"+name+".seasonal')")
		runQueries := func() {
			oldForce := plan.GetForceScanOnMultiCN()
			plan.SetForceScanOnMultiCN(true)
			defer plan.SetForceScanOnMultiCN(oldForce)
			const query = "select id,group_concat(ts) from seasonal group by id order by id"
			require.NotEmpty(t, peerAddr)
			for _, test := range []struct {
				zone string
				want []string
			}{
				{"Asia/Shanghai", []string{"2024-01-01 08:00:00", "2024-07-01 08:00:00"}},
				{"America/New_York", []string{"2023-12-31 19:00:00", "2024-06-30 20:00:00"}}} {
				func() {
					execSQLDB(t, ctx, db, "set time_zone='"+test.zone+"'")
					physical, err := testutils.QueryTextResult(ctx, db, "explain phyplan analyze "+query)
					require.NoError(t, err)
					require.Contains(t, physical.Text, peerAddr)
					require.NotContains(t, strings.ToLower(physical.Text), "merge group", "the remote worker must finalize the aggregate")
					rows, err := db.QueryContext(ctx, query)
					require.NoError(t, err)
					defer rows.Close()
					var got []string
					for rows.Next() {
						var id int
						var value string
						require.NoError(t, rows.Scan(&id, &value))
						got = append(got, value)
					}
					require.NoError(t, rows.Err())
					require.NoError(t, rows.Close())
					require.Equal(t, test.want, got)
				}()
			}
			peerRuntime := moruntime.ServiceRuntime(peer.ServiceID())
			oldVersion, _ := peerRuntime.GetGlobalVariables(moruntime.MOProtocolVersion)
			peerRuntime.SetGlobalVariables(moruntime.MOProtocolVersion, defines.MORPCVersion66)
			defer peerRuntime.SetGlobalVariables(moruntime.MOProtocolVersion, oldVersion)
			physical, err := testutils.QueryTextResult(ctx, db, "explain phyplan analyze "+query)
			require.NoError(t, err)
			require.NotContains(t, physical.Text, peerAddr, "old workers must not own named-zone rendering")
			var value string
			require.NoError(t, db.QueryRowContext(ctx, "select group_concat(ts) from seasonal where id=2").Scan(&value))
			require.Equal(t, "2024-06-30 20:00:00", value)
		}
		stateErr := withCNDraining(ctx, inventory, refresher, cn.ServiceID(),
			[]string{cn.ServiceID(), peer.ServiceID()},
			func(err error) { fixtureInvalidationErr = err }, runQueries)
		require.NoError(t, stateErr)
	})
}
