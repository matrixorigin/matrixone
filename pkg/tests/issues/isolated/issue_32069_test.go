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

package isolated

import (
	"context"
	"database/sql"
	"fmt"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	_ "github.com/go-sql-driver/mysql"
	"github.com/matrixorigin/matrixone/pkg/clusterservice"
	"github.com/matrixorigin/matrixone/pkg/cnservice/cnclient"
	"github.com/matrixorigin/matrixone/pkg/common/moerr"
	"github.com/matrixorigin/matrixone/pkg/common/morpc"
	moruntime "github.com/matrixorigin/matrixone/pkg/common/runtime"
	"github.com/matrixorigin/matrixone/pkg/embed"
	"github.com/matrixorigin/matrixone/pkg/pb/metadata"
	"github.com/matrixorigin/matrixone/pkg/sql/plan"
	"github.com/matrixorigin/matrixone/pkg/tests/testutils"
	"github.com/stretchr/testify/require"
)

type issue32069FailPeerClient struct {
	cnclient.PipelineClient
	peer  string
	calls atomic.Int32
}

func (c *issue32069FailPeerClient) NewStream(ctx context.Context, backend string) (morpc.Stream, error) {
	if backend == c.peer {
		c.calls.Add(1)
		return nil, moerr.NewInternalErrorNoCtx("issue32069 injected remote stream failure")
	}
	return c.PipelineClient.NewStream(ctx, backend)
}

// The rows exceed the automatic parallel admission threshold and provide
// enough scan workers for eight owners when CPUs allow. Each assertion checks
// repeated keys, including NULL, as a bag rather than merely its size.
func TestIssue32069SetMembershipOwners(t *testing.T) {
	cluster, err := embed.StartTestCluster(embed.WithCNCount(1))
	if cluster != nil {
		t.Cleanup(func() { require.NoError(t, cluster.Close()) })
	}
	require.NoError(t, err)
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Minute)
	defer cancel()
	cn, err := cluster.GetCNService(0)
	require.NoError(t, err)
	db, err := sql.Open("mysql", fmt.Sprintf("dump:111@tcp(127.0.0.1:%d)/", cn.GetServiceConfig().CN.Frontend.Port))
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, db.Close()) })
	db.SetMaxOpenConns(1)
	exec := func(q string) {
		t.Helper()
		_, err := db.ExecContext(ctx, q)
		require.NoError(t, err, q)
	}
	exec("create database issue32069_owners")
	exec("use issue32069_owners")
	exec("set max_dop=16")
	exec("create table l(k bigint)")
	exec("create table r(k bigint)")
	exec("insert into l select case when result % 4 = 0 then null else result % 4 end from generate_series(0,1048575) g")
	exec("insert into r select case when result % 4 = 0 then null else result % 4 end from generate_series(0,524287) g")
	exec("create table ltext(k bigint, v varchar(8))")
	exec("create table rtext(k bigint, v char(8))")
	exec("insert into ltext select k, 'a ' from l")
	exec("insert into rtext select k, 'a' from r")
	for _, table := range []string{"l", "r", "ltext", "rtext"} {
		exec("select mo_ctl('dn','flush','issue32069_owners." + table + "')")
	}
	exec("analyze table l, r, ltext, rtext")
	oldForce := plan.GetForceScanOnMultiCN()
	t.Cleanup(func() { plan.SetForceScanOnMultiCN(oldForce) })
	for _, cnCount := range []int{1, 2} {
		t.Run(fmt.Sprintf("cn=%d", cnCount), func(t *testing.T) {
			var peerAddr string
			if cnCount == 2 {
				require.NoError(t, cluster.StartNewCNService(1))
				inventory := clusterservice.GetMOCluster(cn.ServiceID())
				refresher, ok := inventory.(clusterservice.AuthoritativeRefresher)
				require.True(t, ok)
				require.Eventually(t, func() bool {
					if refresher.Refresh(ctx) != nil {
						return false
					}
					count := 0
					inventory.GetCNService(clusterservice.NewSelector(), func(service metadata.CNService) bool {
						count++
						if service.ServiceID != cn.ServiceID() {
							peerAddr = service.PipelineServiceAddress
						}
						return true
					})
					return count == 2
				}, 30*time.Second, 100*time.Millisecond)
			}
			plan.SetForceScanOnMultiCN(cnCount == 2)
			for _, tc := range []struct {
				op   string
				want int64
			}{
				{"intersect all", 131072},
				{"except all", 131072},
				{"intersect", 1},
				{"except", 0},
			} {
				t.Run(tc.op, func(t *testing.T) {
					set := "select k from l " + tc.op + " select k from r"
					physical, err := testutils.QueryTextResult(ctx, db, "explain phyplan analyze "+set)
					require.NoError(t, err)
					operatorName := strings.Replace(tc.op, "except", "minus", 1)
					owners := strings.Count(strings.ToLower(physical.Text), operatorName)
					require.Positive(t, owners, physical.Text)
					t.Logf("membership owners: %d", owners)
					if cnCount == 2 {
						require.NotEmpty(t, peerAddr)
						require.Contains(t, physical.Text, "addr:"+peerAddr, "a remote scan must participate")
					}
					query := "select k,count(*) from (" + set + ") s group by k"
					groupedPlan, err := testutils.QueryTextResult(ctx, db, "explain phyplan analyze "+query)
					require.NoError(t, err)
					groupedOwners := strings.Count(strings.ToLower(groupedPlan.Text), operatorName)
					require.Positive(t, groupedOwners, groupedPlan.Text)
					t.Run("parallel plan witness", func(t *testing.T) {
						if cnCount == 1 {
							if groupedOwners < 8 {
								t.Skipf("eight owners were not admitted (got %d)", groupedOwners)
							}
							require.Equal(t, 8, groupedOwners, groupedPlan.Text)
							return
						}
						if groupedOwners < 2 {
							t.Skipf("parallel owners were not admitted (got %d)", groupedOwners)
						}
						require.Contains(t, groupedPlan.Text, "addr:"+peerAddr, "a remote scan must participate")
					})
					rows, err := db.QueryContext(ctx, query)
					require.NoError(t, err)
					defer rows.Close()
					got := make(map[sql.NullInt64]int64)
					for rows.Next() {
						var key sql.NullInt64
						var count int64
						require.NoError(t, rows.Scan(&key, &count))
						got[key] = count
					}
					require.NoError(t, rows.Err())
					want := make(map[sql.NullInt64]int64)
					if tc.want != 0 {
						want[sql.NullInt64{}] = tc.want
						for i := int64(1); i < 4; i++ {
							want[sql.NullInt64{Int64: i, Valid: true}] = tc.want
						}
					}
					require.Equal(t, want, got)
				})
			}
			// The integer subset routes equal tuples together while the existing
			// full key applies PAD SPACE; the surviving left bytes stay unchanged.
			for _, op := range []string{"intersect all", "except all"} {
				set := "select k,v from ltext " + op + " select k,v from rtext"
				physical, err := testutils.QueryTextResult(ctx, db, "explain phyplan analyze "+set)
				require.NoError(t, err)
				require.Positive(t, strings.Count(strings.ToLower(physical.Text), strings.Replace(op, "except", "minus", 1)), physical.Text)
				query := "select hex(v),count(*) from (" + set + ") s group by hex(v)"
				groupedPlan, err := testutils.QueryTextResult(ctx, db, "explain phyplan analyze "+query)
				require.NoError(t, err)
				require.Positive(t, strings.Count(strings.ToLower(groupedPlan.Text), strings.Replace(op, "except", "minus", 1)), groupedPlan.Text)
				var hex string
				var rows int64
				require.NoError(t, db.QueryRowContext(ctx, query).Scan(&hex, &rows))
				require.Equal(t, "6120", hex)
				require.Equal(t, int64(524288), rows)
			}
			var count int
			require.NoError(t, db.QueryRowContext(ctx, "select count(*) from (select k from l intersect all select k from r limit 1) s").Scan(&count))
			require.Equal(t, 1, count)
			require.NoError(t, db.QueryRowContext(ctx, "select count(*) from (select k from r except all select k from l) s").Scan(&count))
			require.Zero(t, count)
			if cnCount == 2 {
				// Exercise the existing remote transport failure boundary on this
				// private cluster, then restore the original client for reuse.
				serviceRuntime := moruntime.ServiceRuntime(cn.ServiceID())
				originalClient := cnclient.GetPipelineClient(cn.ServiceID())
				require.NotNil(t, originalClient)
				failingClient := &issue32069FailPeerClient{PipelineClient: originalClient, peer: peerAddr}
				serviceRuntime.SetGlobalVariables(moruntime.PipelineClient, failingClient)
				defer serviceRuntime.SetGlobalVariables(moruntime.PipelineClient, originalClient)
				failCtx, failCancel := context.WithTimeout(ctx, 10*time.Second)
				err := db.QueryRowContext(failCtx, "select count(*) from (select k from l except all select k from r) s").Scan(&count)
				failCancel()
				require.ErrorContains(t, err, "issue32069 injected remote stream failure")
				require.Positive(t, failingClient.calls.Load())
				serviceRuntime.SetGlobalVariables(moruntime.PipelineClient, originalClient)
				require.NoError(t, db.QueryRowContext(ctx, "select count(*) from (select k from r except all select k from l) s").Scan(&count))
				require.Zero(t, count)

				queryCtx, stop := context.WithCancel(ctx)
				defer stop()
				rows, err := db.QueryContext(queryCtx, "select k from l except all select k from r")
				require.NoError(t, err)
				defer rows.Close()
				require.True(t, rows.Next(), "the remote build and local result path must start")
				stop()
				for rows.Next() {
				}
				require.ErrorIs(t, rows.Err(), context.Canceled)
				if closeErr := rows.Close(); closeErr != nil {
					require.ErrorIs(t, closeErr, context.Canceled)
				}
				// A stopped receiver must not strand the remote or local producers.
				// The MySQL driver discards the cancelled connection, so restore
				// the session state on its replacement before executing again.
				exec("use issue32069_owners")
				exec("set max_dop=16")
				require.NoError(t, db.QueryRowContext(ctx, "select count(*) from (select k from r except all select k from l) s").Scan(&count))
				require.Zero(t, count)
			}
		})
	}
}

func TestIssue32069LowBudgetFallback(t *testing.T) {
	cluster, err := embed.StartTestCluster(embed.WithCNCount(1), embed.WithPreStart(func(op embed.ServiceOperator) {
		if op.ServiceType() == metadata.ServiceType_CN {
			op.Adjust(func(cfg *embed.ServiceConfig) { cfg.CN.Frontend.ProcessLimitationSize = 16 << 20 })
		}
	}))
	if cluster != nil {
		t.Cleanup(func() { require.NoError(t, cluster.Close()) })
	}
	require.NoError(t, err)
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Minute)
	defer cancel()
	cn, err := cluster.GetCNService(0)
	require.NoError(t, err)
	db, err := sql.Open("mysql", fmt.Sprintf("dump:111@tcp(127.0.0.1:%d)/", cn.GetServiceConfig().CN.Frontend.Port))
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, db.Close()) })
	db.SetMaxOpenConns(1)
	for _, q := range []string{
		"create database issue32069_budget",
		"use issue32069_budget",
		"set max_dop=4",
		"create table l(k bigint)",
		"create table r(k bigint)",
		"insert into l select result % 4 from generate_series(0,262143) g",
		"insert into r select result % 4 from generate_series(0,131071) g",
		"select mo_ctl('dn','flush','issue32069_budget.l')",
		"select mo_ctl('dn','flush','issue32069_budget.r')",
		"analyze table l, r",
	} {
		_, err := db.ExecContext(ctx, q)
		require.NoError(t, err, q)
	}
	const set = "select k from l except all select k from r"
	physical, err := testutils.QueryTextResult(ctx, db, "explain phyplan analyze "+set)
	require.NoError(t, err)
	require.Equal(t, 1, strings.Count(strings.ToLower(physical.Text), "minus all"), physical.Text)
	var count int
	require.NoError(t, db.QueryRowContext(ctx, "select count(*) from ("+set+") s").Scan(&count))
	require.Equal(t, 131072, count)
}
