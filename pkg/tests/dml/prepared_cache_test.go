// Copyright 2026 Matrix Origin
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
// http://www.apache.org/licenses/LICENSE-2.0
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
	"github.com/matrixorigin/matrixone/pkg/embed"
	"github.com/matrixorigin/matrixone/pkg/sql/plan"
	"github.com/matrixorigin/matrixone/pkg/sql/schedule"
	"github.com/matrixorigin/matrixone/pkg/tests/testutils"
	"github.com/stretchr/testify/require"
)

func TestPreparedAPRuntimeCacheWorkspaceTransition(t *testing.T) {
	var fixtureInvalidationErr error
	embed.RunBaseClusterTests(t, func(c embed.Cluster) {
		ctx, cancel := context.WithTimeout(context.Background(), 2*time.Minute)
		defer cancel()
		cn, err := c.GetCNService(0)
		require.NoError(t, err)
		peer, err := c.GetCNService(1)
		require.NoError(t, err)
		cluster := clusterservice.GetMOCluster(cn.ServiceID())
		inventory, ok := cluster.(cnWorkStateInventory)
		require.True(t, ok, "CN inventory must support caller-bounded work-state updates")
		refresher, ok := cluster.(clusterservice.AuthoritativeRefresher)
		require.True(t, ok)
		readinessCtx, cancelReadiness := context.WithTimeout(ctx, 30*time.Second)
		readiness, readinessErr := waitForCNReadiness(
			readinessCtx, cnWorkStatePollInterval, inventory, refresher, cn.ServiceID(), peer.ServiceID())
		cancelReadiness()
		require.NoError(t, readinessErr,
			"last refresh error=%v, admission-ready CNs=%v, normally discoverable CNs=%v",
			readiness.lastRefreshErr, readiness.admissionReady, readiness.normallyDiscoverable)

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
		execSQLDB(t, ctx, db, "create database `"+name+"`")
		execSQLDB(t, ctx, db, "use `"+name+"`")
		execSQLDB(t, ctx, db, "create table src (id bigint primary key)")
		execSQLDB(t, ctx, db, "insert into src values (1),(2)")

		for _, remoteOnly := range []bool{false, true} {
			t.Run(fmt.Sprintf("remote-only=%t", remoteOnly), func(t *testing.T) {
				runQueries := func() {
					tx, err := db.BeginTx(ctx, nil)
					require.NoError(t, err)
					defer tx.Rollback()
					// The existing test hook selects AP with two committed rows.
					// Never force fixture DDL, transaction writes, or cleanup to AP.
					plan.SetForceScanOnMultiCN(true)
					stmt, err := tx.PrepareContext(ctx, "select sum(id + abs(?)) from src") //nolint:sqlclosecheck // stmt closed via outer defer; not tracked across t.Run subtest closures
					plan.SetForceScanOnMultiCN(false)
					require.NoError(t, err)
					defer stmt.Close()
					query := func(value int64) (int64, error) {
						plan.SetForceScanOnMultiCN(true)
						defer plan.SetForceScanOnMultiCN(false)
						var sum int64
						err := stmt.QueryRowContext(ctx, value).Scan(&sum)
						return sum, err
					}
					sum, err := query(-1)
					require.NoError(t, err)
					require.Equal(t, int64(5), sum)
					_, err = tx.ExecContext(ctx, "insert into src values (3)")
					require.NoError(t, err)
					// The unchanged binary parameter category hits the runtime cache.
					// A writable ingress must participate, or scheduling must fail.
					sum, err = query(-2)
					if remoteOnly {
						require.ErrorContains(t, err, schedule.ReasonCurrentCNDraining)
					} else {
						require.NoError(t, err)
						require.Equal(t, int64(12), sum)
					}
				}

				if !remoteOnly {
					runQueries()
					return
				}
				stateErr := withCNDraining(
					ctx,
					inventory,
					refresher,
					cn.ServiceID(),
					[]string{cn.ServiceID(), peer.ServiceID()},
					func(err error) {
						if fixtureInvalidationErr == nil {
							fixtureInvalidationErr = err
						}
					},
					runQueries,
				)
				require.NoError(t, stateErr)
			})
			if fixtureInvalidationErr != nil {
				return
			}
		}
		var sum int64
		require.NoError(t, db.QueryRowContext(ctx, "select sum(id) from src").Scan(&sum))
		require.Equal(t, int64(3), sum, "the uncommitted write must be rolled back")

		t.Run("ordered pagination across CNs", func(t *testing.T) {
			execSQLDB(t, ctx, db, "insert into src values (3),(4),(5)")
			execSQLDB(t, ctx, db, "select mo_ctl('dn','flush','"+name+".src')")
			previous := plan.GetForceScanOnMultiCN()
			plan.SetForceScanOnMultiCN(true)
			defer plan.SetForceScanOnMultiCN(previous)

			stmt, err := db.PrepareContext(ctx, "select id from src order by id limit ? offset ?")
			require.NoError(t, err)
			defer stmt.Close()
			for _, tc := range []struct {
				limit, offset int
				want          []int64
			}{
				{limit: 2, offset: 0, want: []int64{1, 2}},
				{limit: 2, offset: 1, want: []int64{2, 3}},
				{limit: 1, offset: 3, want: []int64{4}},
				{limit: 2, offset: 0, want: []int64{1, 2}},
			} {
				func() {
					rows, err := stmt.QueryContext(ctx, tc.limit, tc.offset)
					require.NoError(t, err)
					defer rows.Close()
					var got []int64
					for rows.Next() {
						var id int64
						require.NoError(t, rows.Scan(&id))
						got = append(got, id)
					}
					require.NoError(t, rows.Err())
					require.Equal(t, tc.want, got)
				}()
			}
			rows, err := db.QueryContext(ctx, "explain phyplan analyze select id from src order by id limit ? offset ?", 2, 1)
			require.NoError(t, err)
			defer rows.Close()
			var physical string
			for rows.Next() {
				var line string
				require.NoError(t, rows.Scan(&line))
				physical += line + "\n"
			}
			require.NoError(t, rows.Err())
			require.Contains(t, physical, "Magic: Remote")
			require.Contains(t, physical, readiness.peerAddr, "the second CN must participate")
			require.Contains(t, strings.ToLower(physical), "top", "prepared pagination must use bounded Top")

			t.Run("both CNs contribute", func(t *testing.T) {
				// A single insert and flush produce one persisted object. Creating
				// a separate table keeps the setup independent of earlier writes.
				plan.SetForceScanOnMultiCN(false)
				execSQLDB(t, ctx, db, "create table src_split (id bigint primary key)")
				execSQLDB(t, ctx, db, "insert into src_split values (1),(2),(3),(4),(5)")
				execSQLDB(t, ctx, db, "select mo_ctl('dn','flush','"+name+".src_split')")
				plan.SetForceScanOnMultiCN(true)
				peerCluster := clusterservice.GetMOCluster(peer.ServiceID())
				peerInventory, ok := peerCluster.(cnWorkStateInventory)
				require.True(t, ok)
				peerRefresher, ok := peerCluster.(clusterservice.AuthoritativeRefresher)
				require.True(t, ok)
				peerReadyCtx, cancelPeerReady := context.WithTimeout(ctx, 30*time.Second)
				peerReadiness, err := waitForCNReadiness(peerReadyCtx, cnWorkStatePollInterval,
					peerInventory, peerRefresher, peer.ServiceID(), cn.ServiceID())
				cancelPeerReady()
				require.NoError(t, err, "peer readiness: %+v", peerReadiness)
				peerDB, err := sql.Open("mysql", fmt.Sprintf("dump:111@tcp(127.0.0.1:%d)/", peer.GetServiceConfig().CN.Frontend.Port))
				require.NoError(t, err)
				defer peerDB.Close()
				peerDB.SetMaxOpenConns(1)
				execSQLDB(t, ctx, peerDB, "use `"+name+"`")

				type queryer interface {
					QueryContext(context.Context, string, ...any) (*sql.Rows, error)
				}
				explain := func(q queryer) string {
					t.Helper()
					rows, err := q.QueryContext(ctx, "explain phyplan analyze select id from src_split order by id desc limit ? offset ?", 2, 1)
					require.NoError(t, err)
					defer rows.Close()
					var result strings.Builder
					for rows.Next() {
						var line string
						require.NoError(t, rows.Scan(&line))
						result.WriteString(line)
						result.WriteByte('\n')
					}
					require.NoError(t, rows.Err())
					return result.String()
				}
				hasInputRows := func(physical, op, count string) bool {
					for _, line := range strings.Split(physical, "\n") {
						if strings.Contains(line, "└── "+op+" CallNum:") && strings.Contains(line, "InRows:"+count+" ") {
							return true
						}
					}
					return false
				}

				// The persisted object belongs to one CN. Select the other as the
				// coordinator, then add a transaction-local row there. The global
				// Top must consume candidates from both sides to return [5, 4].
				var coordinator *sql.DB
				var probePlans []string
				for _, candidate := range []*sql.DB{db, peerDB} {
					plan := explain(candidate)
					probePlans = append(probePlans, plan)
					if hasInputRows(plan, "top", "0") {
						coordinator = candidate
						break
					}
				}
				require.NotNil(t, coordinator, "the persisted rows must be remote from one coordinator: %v", probePlans)
				tx, err := coordinator.BeginTx(ctx, nil)
				require.NoError(t, err)
				defer tx.Rollback()
				_, err = tx.ExecContext(ctx, "insert into src_split values (100)")
				require.NoError(t, err)
				stmt, err := tx.PrepareContext(ctx, "select id from src_split order by id desc limit ? offset ?")
				require.NoError(t, err)
				defer stmt.Close()
				for _, tc := range []struct {
					limit, offset int
					want          []int64
				}{
					{limit: 2, offset: 1, want: []int64{5, 4}},
					{limit: 1, offset: 0, want: []int64{100}},
					{limit: 2, offset: 3, want: []int64{3, 2}},
					{limit: 2, offset: 1, want: []int64{5, 4}},
				} {
					func() {
						rows, err := stmt.QueryContext(ctx, tc.limit, tc.offset)
						require.NoError(t, err)
						defer rows.Close()
						var got []int64
						for rows.Next() {
							var id int64
							require.NoError(t, rows.Scan(&id))
							got = append(got, id)
						}
						require.NoError(t, rows.Err())
						require.Equal(t, tc.want, got)
					}()
				}
				physical := explain(tx)
				require.True(t, hasInputRows(physical, "top", "1"), "the coordinator must read its transaction-local row: %s", physical)
				require.True(t, hasInputRows(physical, "merge top", "4"), "the merge must receive rows from both CNs: %s", physical)
				require.NoError(t, stmt.Close())
				require.NoError(t, tx.Rollback())
			})
		})
	})
}
