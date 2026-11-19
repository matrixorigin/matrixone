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
	"github.com/matrixorigin/matrixone/pkg/common/mpool"
	"github.com/matrixorigin/matrixone/pkg/defines"
	"github.com/matrixorigin/matrixone/pkg/embed"
	"github.com/matrixorigin/matrixone/pkg/fileservice"
	"github.com/matrixorigin/matrixone/pkg/logutil"
	"github.com/matrixorigin/matrixone/pkg/util/executor"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/disttae"
	"github.com/stretchr/testify/require"
)

// Every timed execution starts after synchronous cache eviction. There is no
// pre-query probe: initial column loading, admission, capability construction,
// copies, planning, execution and result release stay inside timing. Source
// ingestion/flush and cache eviction are explicit environmental preparation.
// Correctness checks inspect only the already returned result outside timing.
func BenchmarkJSONValueColdSQL(b *testing.B) {
	originalLog := logutil.GetDefaultConfig()
	quiet := originalLog
	quiet.Level = "error"
	quiet.DisableStore = true
	logutil.SetupMOLogger(&quiet)
	b.Cleanup(func() { logutil.SetupMOLogger(&originalLog) })
	cluster, err := embed.StartTestCluster(embed.WithCNCount(1), embed.WithPreStart(func(service embed.ServiceOperator) {
		service.Adjust(func(config *embed.ServiceConfig) { config.Log.Level = "error"; config.Log.DisableStore = true })
	}))
	require.NoError(b, err)
	b.Cleanup(func() { require.NoError(b, cluster.Close()) })
	cn, err := cluster.GetCNService(0)
	require.NoError(b, err)
	service := cn.RawService().(cnservice.Service)
	sql := service.GetSQLExecutor()
	cache, err := fileservice.Get[interface {
		FlushCache(context.Context)
		SetAsyncUpdate(bool)
	}](service.GetEngine().(*disttae.Engine).FS(), defines.SharedFileServiceName)
	require.NoError(b, err)
	cache.SetAsyncUpdate(false)
	ctx, cancel := context.WithTimeout(context.Background(), 15*time.Minute)
	defer cancel()
	opts := executor.Options{}.WithAccountID(0).WithWaitCommittedLogApplied()
	var queryPool *mpool.MPool
	exec := func(b *testing.B, statement string, options executor.Options) {
		b.Helper()
		result, err := sql.Exec(ctx, statement, options)
		if err == nil {
			queryPool = result.Mp
			result.Close()
		}
		require.NoError(b, err)
	}
	exec(b, "create database json_value_cold_bench", opts)
	opts = opts.WithDatabase("json_value_cold_bench")
	for _, content := range []string{"integer", "string", "mixed", "wide", "deep"} {
		for _, elements := range []int{16, 4096} {
			b.Run(fmt.Sprintf("%s/elements=%d", content, elements), func(b *testing.B) {
				exec(b, "create table docs (id int primary key, doc json)", opts)
				b.Cleanup(func() { exec(b, "drop table docs", opts) })
				document, prefix, expected := jsonColdSQLDocument(content, elements)
				values := make([]string, 64)
				for row := range values {
					values[row] = fmt.Sprintf("(%d,'%s')", row, document)
				}
				exec(b, "insert into docs values "+strings.Join(values, ","), opts)
				exec(b, "select mo_ctl('dn','flush','json_value_cold_bench.docs')", opts)
				for _, paths := range []int{1, 2, 8} {
					b.Run(fmt.Sprintf("paths=%d", paths), func(b *testing.B) {
						expressions := make([]string, paths)
						for i := range expressions {
							key := "keep"
							if i > 0 {
								key = fmt.Sprintf("p%d", i)
							}
							expressions[i] = fmt.Sprintf("json_value(doc, '%s.%s')", prefix, key)
						}
						statement := "select " + strings.Join(expressions, ",") + " from docs"
						b.ReportAllocs()
						b.ResetTimer()
						var poolPeak uint64
						measure := func() {
							b.StopTimer()
							cache.FlushCache(ctx)
							require.NotNil(b, queryPool)
							epoch := queryPool.StartResourcePeakEpoch()
							require.NotNil(b, epoch)
							defer func() {
								b.StopTimer()
								peak, valid := queryPool.EndResourcePeakEpoch(epoch)
								require.True(b, valid)
								if peak > poolPeak {
									poolPeak = peak
								}
							}()
							b.StartTimer()
							result, err := sql.Exec(ctx, statement, opts)
							b.StopTimer()
							require.NoError(b, err)
							defer result.Close()
							rows := 0
							for _, bat := range result.Batches {
								rows += bat.RowCount()
								require.Len(b, bat.Vecs, paths)
								for _, vec := range bat.Vecs {
									for row := 0; row < bat.RowCount(); row++ {
										require.Equal(b, expected, string(vec.GetBytesAt(row)))
									}
								}
							}
							require.Equal(b, 64, rows)
							require.Same(b, queryPool, result.Mp, "peak observation must cover the execution pool")
							b.StartTimer()
							// Successful result release remains timed; on any failed check the
							// deferred release still runs before the epoch is closed.
						}
						for range b.N {
							measure()
						}
						b.StopTimer()
						b.ReportMetric(64, "rows/SQL")
						b.ReportMetric(float64(len(document)), "textbytes/document")
						// Allocator-domain peak spans execution through result release. It
						// excludes cache/Go-heap domains and does not claim process RSS.
						b.ReportMetric(float64(poolPeak), "query_pool_epoch_peak_B")
					})
				}
			})
		}
	}
}

func jsonColdSQLDocument(content string, count int) (string, string, string) {
	value, expected := `"value"`, "value"
	if content == "integer" {
		value, expected = "1", "1"
	}
	capacity := 9
	if content == "wide" {
		capacity = count + 8
	}
	fields := make([]string, 0, capacity)
	fields = append(fields, `"keep":`+value)
	for i := 1; i < 8; i++ {
		fields = append(fields, fmt.Sprintf(`"p%d":%s`, i, value))
	}
	if content == "wide" {
		for i := 0; i < count; i++ {
			fields = append(fields, fmt.Sprintf(`"unrelated%04d":1`, i))
		}
	} else {
		items := make([]string, count)
		mixed := []string{"1", `"s"`, "null", "true", `{"v":2}`}
		for i := range items {
			items[i] = "1"
			if content == "string" {
				items[i] = `"string payload"`
			}
			if content == "mixed" {
				items[i] = mixed[i%len(mixed)]
			}
		}
		fields = append(fields, `"large":[`+strings.Join(items, ",")+`]`)
	}
	text, prefix := "{"+strings.Join(fields, ",")+"}", "$"
	if content == "deep" {
		for range 32 {
			text = `{"level":` + text + `}`
			prefix += ".level"
		}
	}
	return text, prefix, expected
}
