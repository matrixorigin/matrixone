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
	"sort"
	"strings"
	"testing"
	"time"

	_ "github.com/go-sql-driver/mysql"
	"github.com/matrixorigin/matrixone/pkg/embed"
)

// Copy this exact file to both the clean base and the fix. Run through the
// public MySQL frontend; no internal SQL executor or production testing hook.
//
// Each iteration starts with TRUNCATE plus the same seed, outside Exec timing.
// Only the autocommit ExecContext is timed. Setup, reset, result validation,
// SQL construction, connection acquisition and teardown are excluded. The
// TRUNCATE means this measures a warm engine with a freshly reset relation;
// it does not claim to measure a reusable prepared-plan-cache workload.
//
// Hot-key actions have the SAME v=1 and payload in every input row. Their
// result is independent of input arrival order and is correct on the base.
// They do not use the base's broken empty-table repeated-UNIQUE insertion.
// ROW_COUNT is intentionally not an oracle: legacy multi-action affected-row
// counting is outside this correction and differs from physical row counts.
//
// Suggested controlled sampling:
//
//	-test.run=^$ -test.bench=^BenchmarkOrderedODKU -test.benchtime=7x -test.count=7
//
// Three warm-up Execs per invocation are excluded from the samples. Each result
// also logs the individual timed Exec durations and reports median/p90. Use
// separate base/fix processes, alternate their execution order, and keep the
// environment/configuration and benchmark file identical.
// Use fixed iteration counts for comparisons. The framework timer includes
// warmups, resets and verification only to calibrate duration-based runs. Avoid
// toggling it per Exec: reading memory statistics pauses the embedded server.
// Report Exec samples explicitly; process-wide allocations are not query costs.
func BenchmarkOrderedODKU(b *testing.B) {
	embed.RunBaseClusterTests(b, func(cluster embed.Cluster) {
		cn, err := cluster.GetCNService(0)
		if err != nil {
			b.Fatal(err)
		}
		db, err := sql.Open("mysql", fmt.Sprintf("dump:111@tcp(127.0.0.1:%d)/", cn.GetServiceConfig().CN.Frontend.Port))
		if err != nil {
			b.Fatal(err)
		}
		defer db.Close()
		db.SetMaxOpenConns(1)
		db.SetMaxIdleConns(1)
		conn, err := db.Conn(b.Context())
		if err != nil {
			b.Fatal(err)
		}
		defer conn.Close()

		// This process owns this unique database. No other test's data or cache
		// is removed, and no table grows with b.N.
		database := fmt.Sprintf("ordered_odku_bench_%d", time.Now().UnixNano())
		orderedODKUBenchExec(b, b.Context(), conn, "create database `"+database+"`")
		defer func() {
			// Cleanup owns an independent bounded budget after a failed/cancelled
			// benchmark; it does not extend the measured operation's context.
			ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
			defer cancel()
			if _, err := conn.ExecContext(ctx, "drop database if exists `"+database+"`"); err != nil {
				b.Errorf("benchmark database cleanup: %v", err)
			}
		}()
		orderedODKUBenchExec(b, b.Context(), conn, "use `"+database+"`")

		cases := []orderedODKUBenchmarkCase{
			{name: "single_insert", actions: 1, outputRows: 1},
			{name: "single_conflict", actions: 1, seedRows: 1, outputRows: 1},
			{name: "single_noop", actions: 1, seedRows: 1, outputRows: 1, noop: true},
			{name: "bulk_insert_8192", actions: 8192, outputRows: 8192},
			{name: "bulk_existing_8192", actions: 8192, seedRows: 8192, outputRows: 8192},
			{name: "bulk_hot_8192_100keys", actions: 8192, seedRows: 100, outputRows: 100, hotKeys: 100},
			{name: "bulk_noop_8192", actions: 8192, seedRows: 8192, outputRows: 8192, noop: true},
			{name: "varlen512_insert_8192", actions: 8192, outputRows: 8192, payloadBytes: 512},
			{name: "varlen512_existing_8192", actions: 8192, seedRows: 8192, outputRows: 8192, payloadBytes: 512},
			{name: "varlen512_hot_8192_100keys", actions: 8192, seedRows: 100, outputRows: 100, hotKeys: 100, payloadBytes: 512},
		}
		for index, tc := range cases {
			b.Run(tc.name, func(b *testing.B) {
				b.StopTimer()
				ctx, cancel := context.WithTimeout(b.Context(), 10*time.Minute)
				defer cancel()
				table := fmt.Sprintf("case_%02d", index)
				orderedODKUBenchExec(b, ctx, conn, "create table `"+table+"` ("+
					"id bigint primary key,u1 bigint not null,u2 bigint not null,"+
					"v bigint not null,payload varchar(4096) not null,"+
					"unique key uk_u1(u1),unique key uk_u2(u2))")
				defer func() {
					cleanupCtx, cleanupCancel := context.WithTimeout(context.Background(), 30*time.Second)
					defer cleanupCancel()
					if _, err := conn.ExecContext(cleanupCtx, "drop table if exists `"+table+"`"); err != nil {
						b.Errorf("benchmark table cleanup: %v", err)
					}
				}()
				payload := "value"
				seedPayload := "seed0"
				if tc.payloadBytes != 0 {
					payload = strings.Repeat("x", tc.payloadBytes)
					seedPayload = strings.Repeat("s", tc.payloadBytes)
				}
				seedValue := 0
				if tc.noop {
					seedValue = 1
					seedPayload = payload
				}
				// All literals below are fixed fixture constants, never external SQL.
				seedSQL := fmt.Sprintf("insert into `%s` select result,result*10,result*100,%d,'%s' from generate_series(1,%d) g", table, seedValue, seedPayload, tc.seedRows)
				key := "result"
				if tc.hotKeys != 0 {
					key = fmt.Sprintf("(1+((result-1)%%%d))", tc.hotKeys)
				}
				statement := fmt.Sprintf("insert into `%s` select 1000000+result,(%s)*10,(%s)*100,1,'%s' from generate_series(1,%d) g order by result on duplicate key update v=values(v),payload=values(payload)", table, key, key, payload, tc.actions)
				if tc.actions == 1 {
					statement = fmt.Sprintf("insert into `%s` values (1000001,10,100,1,'%s') on duplicate key update v=values(v),payload=values(payload)", table, payload)
				}
				reset := func() {
					orderedODKUBenchExec(b, ctx, conn, "truncate table `"+table+"`")
					if tc.seedRows != 0 {
						orderedODKUBenchExec(b, ctx, conn, seedSQL)
					}
				}
				verify := func() {
					var count, distinctIDs, minID, maxID, minV, maxV int64
					var minPayload, maxPayload string
					query := "select count(*),count(distinct id),min(id),max(id),min(v),max(v),min(payload),max(payload) from `" + table + "`"
					if err := conn.QueryRowContext(ctx, query).Scan(&count, &distinctIDs, &minID, &maxID, &minV, &maxV, &minPayload, &maxPayload); err != nil {
						b.Fatal(err)
					}
					offset := int64(0)
					if tc.seedRows == 0 {
						offset = 1000000
					}
					if count != int64(tc.outputRows) || distinctIDs != count || minID != offset+1 || maxID != offset+int64(tc.outputRows) || minV != 1 || maxV != 1 || minPayload != payload || maxPayload != payload {
						b.Fatalf("invalid benchmark result: rows=%d distinct_ids=%d id_range=[%d,%d] value_range=[%d,%d] payload_lengths=[%d,%d]; expected rows=%d id_range=[%d,%d] and constant v=1/payload", count, distinctIDs, minID, maxID, minV, maxV, len(minPayload), len(maxPayload), tc.outputRows, offset+1, offset+int64(tc.outputRows))
					}
				}

				elapsed := make([]time.Duration, 0, b.N)
				b.ResetTimer()
				b.StartTimer()
				defer b.StopTimer()
				for range 3 {
					reset()
					orderedODKUBenchExec(b, ctx, conn, statement)
					verify()
				}
				for range b.N {
					reset()
					start := time.Now()
					_, err := conn.ExecContext(ctx, statement)
					duration := time.Since(start)
					if err != nil {
						b.Fatalf("timed ODKU Exec: %v", err)
					}
					elapsed = append(elapsed, duration)
					verify()
				}
				b.StopTimer()
				// Report individual samples before sorting: -test.v retains their
				// distribution rather than only the benchmark framework's mean.
				rawNS := make([]int64, len(elapsed))
				var totalNS int64
				for i, duration := range elapsed {
					rawNS[i] = duration.Nanoseconds()
					totalNS += rawNS[i]
				}
				b.ReportMetric(float64(totalNS)/float64(b.N), "ns/op")
				b.Logf("timed_exec_ns=%v actions_per_exec=%d final_rows=%d payload_bytes=%d warmups=3", rawNS, tc.actions, tc.outputRows, len(payload))
				sort.Slice(elapsed, func(i, j int) bool { return elapsed[i] < elapsed[j] })
				median := elapsed[len(elapsed)/2]
				if len(elapsed)%2 == 0 {
					median = elapsed[len(elapsed)/2-1] + (median-elapsed[len(elapsed)/2-1])/2
				}
				p90 := elapsed[(9*len(elapsed)+9)/10-1]
				b.ReportMetric(float64(median.Nanoseconds()), "median-ns/exec")
				b.ReportMetric(float64(p90.Nanoseconds()), "p90-ns/exec")
				b.ReportMetric(float64(tc.actions)/median.Seconds(), "actions/s")
			})
		}
	})
}

type orderedODKUBenchmarkCase struct {
	name         string
	actions      int
	seedRows     int
	outputRows   int
	hotKeys      int
	payloadBytes int
	noop         bool
}

func orderedODKUBenchExec(b *testing.B, ctx context.Context, conn *sql.Conn, statement string) {
	b.Helper()
	if _, err := conn.ExecContext(ctx, statement); err != nil {
		b.Fatalf("untimed fixture Exec: %v", err)
	}
}
