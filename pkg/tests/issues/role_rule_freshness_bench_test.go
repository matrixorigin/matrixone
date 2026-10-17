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

	"github.com/matrixorigin/matrixone/pkg/cnservice"
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
			ctx, cancel := context.WithTimeout(b.Context(), 15*time.Minute)
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
			} {
				exec(owner, text)
			}
			counter, ok := cn.RawService().(cnservice.Service).GetTxnClient().(interface{ SnapshotAdmissionCounters() (uint64, uint64) })
			require.True(b, ok, "build both matched benchmark trees with the documented count-only instrumentation")
			for _, rules := range []bool{false, true} {
				if rules {
					for _, role := range []string{"reader", "accountadmin"} {
						exec(owner, "alter role "+role+" add rule 'select id, tenant from app.t where tenant=1' on table app.t")
					}
				}
				for _, administrator := range []bool{false, true} {
					protocols := []string{"text", "binary"}
					if mode == "default" {
						protocols = append(protocols, "text-delimiter", "text-literal")
					}
					for _, protocol := range protocols {
						if selected := os.Getenv("MO_ROLE_FRESHNESS_BENCH_CELL"); selected != "" && selected != fmt.Sprintf("%t/%t/%s", rules, administrator, protocol) {
							continue
						}
						func() {
							query := "select id from app.t order by id"
							if protocol == "text-delimiter" {
								query += ";"
							} else if protocol == "text-literal" {
								query = "select id from app.t where ';' = ';' order by id"
							}
							sampleBase := func(phase string) map[string]any {
								return map[string]any{"mode": mode, "protocol": protocol, "sql": query, "rules": rules, "admin": administrator, "phase": phase}
							}
							beforeConnect, _ := counter.SnapshotAdmissionCounters()
							started := time.Now()
							user := "role_perf#learner#reader"
							if administrator {
								user = "role_perf#admin#accountadmin"
							}
							_, conn := open(user)
							connect := time.Since(started)
							afterConnect, _ := counter.SnapshotAdmissionCounters()
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
							afterPrepare, _ := counter.SnapshotAdmissionCounters()
							request := func() {
								rows, err := read()
								require.NoError(b, err)
								defer rows.Close()
								var ids []int
								for rows.Next() {
									var id int
									require.NoError(b, rows.Scan(&id))
									ids = append(ids, id)
								}
								require.NoError(b, rows.Err())
								expected := []int{1, 2}
								if rules {
									expected = []int{1}
								}
								require.Equal(b, expected, ids)
							}
							started = time.Now()
							request()
							firstLatency := time.Since(started)
							afterFirst, _ := counter.SnapshotAdmissionCounters()
							first := sampleBase("first-execution")
							first["connect_ns"], first["prepare_ns"], first["latency_ns"] = connect.Nanoseconds(), prepare.Nanoseconds(), firstLatency.Nanoseconds()
							first["connect_admissions"], first["prepare_admissions"], first["first_admissions"] = afterConnect-beforeConnect, afterPrepare-afterConnect, afterFirst-afterPrepare
							emitRoleFreshnessSample(b, first)
							for round := 0; round <= 3; round++ {
								before := roleFreshnessSnapshotObservations(b)
								admittedBefore, successfulBefore := counter.SnapshotAdmissionCounters()
								started = time.Now()
								var samples []int64
								for time.Since(started) < 5*time.Second {
									start := time.Now()
									request()
									samples = append(samples, time.Since(start).Nanoseconds())
								}
								elapsed := time.Since(started)
								admittedAfter, successfulAfter := counter.SnapshotAdmissionCounters()
								observations := roleFreshnessSnapshotObservations(b) - before
								sort.Slice(samples, func(i, j int) bool { return samples[i] < samples[j] })
								phase := "measured"
								if round == 0 {
									phase = "warmup"
								}
								sample := sampleBase(phase)
								sample["round"], sample["requests"], sample["errors"] = round, len(samples), 0
								sample["elapsed_ns"], sample["requests_per_second"] = elapsed.Nanoseconds(), float64(len(samples))/elapsed.Seconds()
								sample["p50_ns"], sample["p95_ns"] = samples[len(samples)/2], samples[(len(samples)-1)*95/100]
								sample["snapshot_observations"], sample["snapshot_observations_per_request"] = observations, float64(observations)/float64(len(samples))
								sample["admission_attempts"], sample["admission_successes"] = admittedAfter-admittedBefore, successfulAfter-successfulBefore
								sample["admissions_per_request"] = float64(admittedAfter-admittedBefore) / float64(len(samples))
								emitRoleFreshnessSample(b, sample)
							}
						}()
					}
				}
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
