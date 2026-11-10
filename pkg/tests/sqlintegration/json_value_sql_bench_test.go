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

package sqlintegration

import (
	"context"
	"fmt"
	"strings"
	"testing"
	"time"

	"github.com/matrixorigin/matrixone/pkg/cnservice"
	"github.com/matrixorigin/matrixone/pkg/embed"
	"github.com/matrixorigin/matrixone/pkg/logutil"
	"github.com/matrixorigin/matrixone/pkg/util/executor"
	"github.com/stretchr/testify/require"
)

// This measures repeated real SQL execution, including planning, input
// delivery, per-execution publication/copies, query and result release.
// Ingestion and correctness probes are outside timing; probes can warm cache
// admission. It is a resident SQL control, not cold first-admission acceptance.
// Both arms use the same source, topology and ERROR logging. Per-query INFO
// writes otherwise dominate this small workload and obscure attribution.
func BenchmarkJSONValueSQLInputDelivery(b *testing.B) {
	originalLog := logutil.GetDefaultConfig()
	quietLog := originalLog
	quietLog.Level = "error"
	quietLog.DisableStore = true
	logutil.SetupMOLogger(&quietLog)
	b.Cleanup(func() { logutil.SetupMOLogger(&originalLog) })
	cluster, err := embed.StartTestCluster(embed.WithCNCount(1), embed.WithPreStart(func(service embed.ServiceOperator) {
		service.Adjust(func(config *embed.ServiceConfig) {
			config.Log.Level = "error"
			config.Log.DisableStore = true
		})
	}))
	require.NoError(b, err)
	b.Cleanup(func() { require.NoError(b, cluster.Close()) })
	cn, err := cluster.GetCNService(0)
	require.NoError(b, err)
	sql := cn.RawService().(cnservice.Service).GetSQLExecutor()
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Minute)
	defer cancel()
	opts := executor.Options{}.WithAccountID(0).WithWaitCommittedLogApplied()
	exec := func(b *testing.B, statement string, options executor.Options) {
		b.Helper()
		result, err := sql.Exec(ctx, statement, options)
		if err == nil {
			result.Close()
		}
		require.NoError(b, err)
	}
	exec(b, "create database json_value_delivery_bench", opts)
	opts = opts.WithDatabase("json_value_delivery_bench")
	for _, elements := range []int{16, 4096} {
		b.Run(fmt.Sprintf("integer/elements=%d", elements), func(b *testing.B) {
			exec(b, "create table docs (id int primary key, doc json)", opts)
			b.Cleanup(func() { exec(b, "drop table docs", opts) })
			document := `{"keep":1,"p1":1,"p2":1,"p3":1,"p4":1,"p5":1,"p6":1,"p7":1,"large":[` + strings.Repeat("1,", elements-1) + "1]}"
			values := make([]string, 64)
			for row := range values {
				values[row] = fmt.Sprintf("(%d,'%s')", row, document)
			}
			exec(b, "insert into docs values "+strings.Join(values, ","), opts)
			for _, paths := range []int{1, 2, 8} {
				b.Run(fmt.Sprintf("paths=%d", paths), func(b *testing.B) {
					expressions := make([]string, paths)
					for i := range expressions {
						key := "keep"
						if i > 0 {
							key = fmt.Sprintf("p%d", i)
						}
						expressions[i] = fmt.Sprintf("json_value(doc, '$.%s')", key)
					}
					statement := "select " + strings.Join(expressions, ",") + " from docs"
					check := func(result executor.Result) {
						rows := 0
						for _, bat := range result.Batches {
							rows += bat.RowCount()
							require.Len(b, bat.Vecs, paths)
							for _, vec := range bat.Vecs {
								for row := 0; row < bat.RowCount(); row++ {
									require.Equal(b, "1", string(vec.GetBytesAt(row)))
								}
							}
						}
						require.Equal(b, 64, rows)
					}
					result, err := sql.Exec(ctx, statement, opts)
					require.NoError(b, err)
					check(result)
					result.Close()
					b.ReportAllocs()
					b.ResetTimer()
					for range b.N {
						result, err = sql.Exec(ctx, statement, opts)
						if err != nil {
							b.Fatal(err)
						}
						result.Close()
					}
					b.StopTimer()
					result, err = sql.Exec(ctx, statement, opts)
					require.NoError(b, err)
					check(result)
					result.Close()
					b.ReportMetric(64, "rows/SQL")
					b.ReportMetric(float64(len(document)), "textbytes/document")
				})
			}
		})
	}
}
