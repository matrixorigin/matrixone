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

package issues

import (
	"context"
	"database/sql"
	"encoding/json"
	"fmt"
	"os"
	"sort"
	"testing"
	"time"

	"github.com/matrixorigin/matrixone/pkg/embed"
	"github.com/matrixorigin/matrixone/pkg/pb/metadata"
	metricv2 "github.com/matrixorigin/matrixone/pkg/util/metric/v2"
	"github.com/prometheus/client_golang/prometheus"
	dto "github.com/prometheus/client_model/go"
	"github.com/stretchr/testify/require"
)

// Run with -run '^$' -bench '^BenchmarkRoleRuleFreshnessTCP$' -benchtime=1x.
// Each cell uses a separate authenticated TCP session and retained binary plan:
// connection/PREPARE/first execution, one 5s warmup, then three 5s measurements.
// MO_ROLE_FRESHNESS_BENCH_CONFIG selects a cell's configuration so frozen base
// and candidate binaries can be alternated without competing live clusters.
func BenchmarkRoleRuleFreshnessTCP(b *testing.B) {
	for _, mode := range []string{"default", "preserve", "limited"} {
		if selected := os.Getenv("MO_ROLE_FRESHNESS_BENCH_CONFIG"); selected != "" && selected != mode {
			continue
		}
		b.Run(mode, func(b *testing.B) {
			require.Equal(b, 1, b.N, "use -benchtime=1x; the benchmark owns its measurement rounds")
			b.StopTimer()
			c, err := embed.NewCluster(embed.WithTesting(), embed.WithCNCount(1), embed.WithPreStart(func(op embed.ServiceOperator) {
				if op.ServiceType() == metadata.ServiceType_CN {
					op.Adjust(func(cfg *embed.ServiceConfig) {
						cfg.CN.Frontend.SkipCheckUser = false
						cfg.CN.Txn.EnableSacrificingFreshness = 1
						if mode == "preserve" {
							cfg.CN.Txn.EnableSacrificingFreshness = -1
						}
						if mode == "limited" {
							cfg.CN.Txn.Limit = 100
						}
					})
				}
			}))
			if c != nil {
				b.Cleanup(func() { require.NoError(b, c.Close()) })
			}
			require.NoError(b, err)
			require.NoError(b, c.Start())
			cn, err := c.GetCNService(0)
			require.NoError(b, err)
			cfg := cn.GetServiceConfig().CN
			b.Logf("configuration: sacrificing-freshness=%d txn-limit=%d", cfg.Txn.EnableSacrificingFreshness, cfg.Txn.Limit)
			ctx, cancel := context.WithTimeout(b.Context(), 10*time.Minute)
			defer cancel()
			open := func(user string) (*sql.DB, *sql.Conn) {
				db, err := sql.Open("mysql", fmt.Sprintf("%s:111@tcp(127.0.0.1:%d)/", user, cfg.Frontend.Port))
				require.NoError(b, err)
				b.Cleanup(func() { require.NoError(b, db.Close()) })
				conn, err := db.Conn(ctx)
				require.NoError(b, err)
				b.Cleanup(func() { require.NoError(b, conn.Close()) })
				return db, conn
			}
			_, admin := open("dump")
			exec := func(conn *sql.Conn, text string) {
				_, err := conn.ExecContext(ctx, text)
				require.NoError(b, err)
			}
			exec(admin, "create account role_perf admin_name 'admin' identified by '111'")
			defer func() {
				cleanup, stop := context.WithTimeout(context.Background(), 30*time.Second)
				defer stop()
				_, err := admin.ExecContext(cleanup, "drop account role_perf")
				require.NoError(b, err)
			}()
			_, owner := open("role_perf#admin#accountadmin")
			for _, text := range []string{
				"create database app", "create table app.t(id int primary key, tenant int)",
				"insert into app.t values (1,1),(2,2)", "create role reader",
				"create user learner identified by '111' default role reader", "grant reader to learner",
				"grant connect on account * to reader", "grant select on table app.t to reader",
				"alter role reader add rule 'select id, tenant from app.t where tenant=1' on table app.t",
			} {
				exec(owner, text)
			}
			for _, protocol := range []string{"text", "binary"} {
				func() {
					started := time.Now()
					_, conn := open("role_perf#learner#reader")
					connect := time.Since(started)
					const query = "select id from app.t order by id"
					read := func() (*sql.Rows, error) { return conn.QueryContext(ctx, query) }
					var prepare time.Duration
					if protocol == "binary" {
						started = time.Now()
						stmt, err := conn.PrepareContext(ctx, query)
						require.NoError(b, err)
						defer stmt.Close()
						prepare = time.Since(started)
						read = func() (*sql.Rows, error) { return stmt.QueryContext(ctx) }
					}
					request := func() {
						rows, err := read()
						require.NoError(b, err)
						defer rows.Close()
						require.True(b, rows.Next())
						var id int
						require.NoError(b, rows.Scan(&id))
						require.Equal(b, 1, id)
						require.False(b, rows.Next(), "the mandatory rule must exclude tenant 2")
						require.NoError(b, rows.Err())
					}
					started = time.Now()
					request()
					emitRoleFreshnessSample(b, map[string]any{"mode": mode, "protocol": protocol, "phase": "first-execution", "connect_ns": connect.Nanoseconds(), "prepare_ns": prepare.Nanoseconds(), "latency_ns": time.Since(started).Nanoseconds()})
					for round := 0; round <= 3; round++ {
						before := roleFreshnessSnapshotObservations(b)
						started = time.Now()
						var samples []int64
						for time.Since(started) < 5*time.Second {
							start := time.Now()
							request()
							samples = append(samples, time.Since(start).Nanoseconds())
						}
						elapsed := time.Since(started)
						observations := roleFreshnessSnapshotObservations(b) - before
						sort.Slice(samples, func(i, j int) bool { return samples[i] < samples[j] })
						phase := "measured"
						if round == 0 {
							phase = "warmup"
						}
						emitRoleFreshnessSample(b, map[string]any{"mode": mode, "protocol": protocol, "phase": phase, "round": round, "requests": len(samples), "errors": 0, "elapsed_ns": elapsed.Nanoseconds(), "requests_per_second": float64(len(samples)) / elapsed.Seconds(), "p50_ns": samples[len(samples)/2], "p95_ns": samples[(len(samples)-1)*95/100], "snapshot_observations": observations, "snapshot_observations_per_request": float64(observations) / float64(len(samples))})
					}
				}()
			}
		})
	}
}

func emitRoleFreshnessSample(b *testing.B, sample map[string]any) {
	b.Helper()
	encoded, err := json.Marshal(sample)
	require.NoError(b, err)
	b.Logf("ROLE_FRESHNESS %s", encoded)
}

// This existing histogram observes all snapshot determinations, including real
// transaction starts and background work. It is a process-wide observation,
// not an isolated role-proof counter. Deterministic frontend tests separately
// assert the exact per-request role-proof count and invalidation boundaries.
func roleFreshnessSnapshotObservations(b *testing.B) uint64 {
	b.Helper()
	var sample dto.Metric
	require.NoError(b, metricv2.TxnDetermineSnapshotDurationHistogram.(prometheus.Metric).Write(&sample))
	return sample.GetHistogram().GetSampleCount()
}
