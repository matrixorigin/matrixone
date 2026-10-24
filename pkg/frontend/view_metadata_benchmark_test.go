//go:build integration && viewmetadatabench

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

package frontend_test

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"runtime"
	"slices"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/matrixorigin/matrixone/pkg/catalog"
	"github.com/matrixorigin/matrixone/pkg/cnservice"
	"github.com/matrixorigin/matrixone/pkg/container/vector"
	"github.com/matrixorigin/matrixone/pkg/defines"
	"github.com/matrixorigin/matrixone/pkg/embed"
	"github.com/matrixorigin/matrixone/pkg/frontend"
	"github.com/matrixorigin/matrixone/pkg/sql/plan"
	"github.com/matrixorigin/matrixone/pkg/txn/client"
	"github.com/matrixorigin/matrixone/pkg/util/executor"
	"github.com/matrixorigin/matrixone/pkg/vm/process"
	"github.com/stretchr/testify/require"
)

type viewMetadataBenchShape struct {
	name                   string
	roots, depth, columns  int
	unrelated              int
	tablesOnly, fullSchema bool
}

type viewMetadataBenchMode struct {
	name            string
	legacy, memoOff bool
}

var viewMetadataBenchModes = []viewMetadataBenchMode{
	{"current_compat", true, false}, // 当前 head 的兼容路径，不冒称历史 B0。
	{"cache_off", false, true},
	{"statement_memo", false, false},
}

type viewMetadataBenchSample struct {
	elapsed time.Duration
	stats   plan.ViewSchemaBenchmarkStats
	peak    uint64 // 父执行代被计费的峰值，不是完整 binder heap/RSS。
	err     error
}

func viewMetadataBenchContext() context.Context {
	return defines.AttachAccount(context.Background(), catalog.System_Account, catalog.System_User, catalog.System_Role)
}

func viewMetadataBenchExec(ctx context.Context, svc cnservice.Service, database, sql string) error {
	ctx, cancel := context.WithTimeout(ctx, 2*time.Minute)
	defer cancel()
	result, err := svc.GetSQLExecutor().Exec(ctx, sql, executor.Options{}.WithDatabase(database))
	if err != nil {
		return err
	}
	result.Close()
	return nil
}

// 大 fixture 只由显式 benchmark 选择。普通合同测试使用两根/两列。
func setupViewMetadataBench(t testing.TB, svc cnservice.Service, shape viewMetadataBenchShape) (string, []string) {
	t.Helper()
	const database = "view_metadata_benchmark"
	ctx := viewMetadataBenchContext()
	require.NoError(t, viewMetadataBenchExec(ctx, svc, "", "create database "+database))
	var createdViews, createdTables []string
	t.Cleanup(func() {
		// DROP DATABASE 一次删除上万对象会把全部 catalog 删除留在同一事务，
		// 不是读基准要测的负载。按逆依赖顺序逐对象提交，保持清理事务有界；
		// MO 不支持一次 DROP 多个 View。不增加读 deadline，也不在失败时重试。
		for _, objects := range []struct {
			kind  string
			names []string
		}{{"view", createdViews}, {"table", createdTables}} {
			for i := len(objects.names) - 1; i >= 0; i-- {
				require.NoError(t, viewMetadataBenchExec(ctx, svc, database,
					"drop "+objects.kind+" "+objects.names[i]))
			}
		}
		require.NoError(t, viewMetadataBenchExec(ctx, svc, "", "drop database "+database))
		checkCtx, cancel := context.WithTimeout(ctx, 2*time.Minute)
		defer cancel()
		result, err := svc.GetSQLExecutor().Exec(checkCtx,
			"select cast(count(*) as char) from mo_catalog.mo_database where account_id=0 and datname='"+database+"'", executor.Options{})
		require.NoError(t, err)
		defer result.Close()
		rows := 0
		result.ReadRows(func(n int, columns []*vector.Vector) bool {
			rows += n
			require.Len(t, columns, 1)
			require.Equal(t, "0", columns[0].GetStringAt(0), "fixture teardown must remove its database")
			return true
		})
		require.Equal(t, 1, rows)
	})
	columns := make([]string, shape.columns)
	definitions := make([]string, shape.columns)
	for i := range columns {
		columns[i] = fmt.Sprintf("c%04d", i)
		definitions[i] = columns[i] + " int"
	}
	var names []string
	if shape.tablesOnly {
		for i := 0; i < shape.roots; i++ {
			name := fmt.Sprintf("t%05d", i)
			require.NoError(t, viewMetadataBenchExec(ctx, svc, database, "create table "+name+" ("+strings.Join(definitions, ",")+")"))
			createdTables = append(createdTables, name)
			names = append(names, name)
		}
		return database, names
	}
	// 宽列由一个物理列重复直接投影生成，避免把隐藏物理列数量混入 4096 输出边界。
	baseDefinitions := definitions
	projection := strings.Join(columns, ",")
	if shape.columns > 8 {
		baseDefinitions = []string{"c0000 int"}
		wide := make([]string, shape.columns)
		for i := range wide {
			wide[i] = "c0000 as " + columns[i]
		}
		projection = strings.Join(wide, ",")
	}
	require.NoError(t, viewMetadataBenchExec(ctx, svc, database, "create table source ("+strings.Join(baseDefinitions, ",")+")"))
	createdTables = append(createdTables, "source")
	source := "source"
	if shape.fullSchema {
		names = append(names, source)
	}
	for layer := 1; layer < shape.depth; layer++ {
		name := fmt.Sprintf("middle_%02d", layer)
		require.NoError(t, viewMetadataBenchExec(ctx, svc, database, "create view "+name+" as select "+projection+" from "+source))
		createdViews = append(createdViews, name)
		source = name
		if shape.fullSchema {
			names = append(names, name)
		}
	}
	for i := 0; i < shape.roots; i++ {
		name := fmt.Sprintf("v%05d", i)
		require.NoError(t, viewMetadataBenchExec(ctx, svc, database, "create view "+name+" as select "+projection+" from "+source))
		createdViews = append(createdViews, name)
		names = append(names, name)
	}
	for i := 0; i < shape.unrelated; i++ {
		name := fmt.Sprintf("unrelated_%05d", i)
		require.NoError(t, viewMetadataBenchExec(ctx, svc, database, "create view "+name+" as select "+projection+" from source"))
		createdViews = append(createdViews, name)
	}
	slices.Sort(names)
	return database, names
}

// result 的发送者只在 Exec 返回后关闭结果通道；消费者即使校验失败也取消并
// 排空所有已发布 batch，再 join。不会把 Exec 的一次 error send 阻塞在无人读的通道。
func streamViewMetadataBench(ctx context.Context, svc cnservice.Service, sql string, options executor.Options, consume func(int, []*vector.Vector) error) error {
	ctx, cancel := context.WithCancel(ctx)
	defer cancel()
	results := make(chan executor.Result, 1)
	execErrors := make(chan error, 1)
	done := make(chan error, 1)
	go func() {
		result, err := svc.GetSQLExecutor().Exec(ctx, sql, options.WithStreaming(results, execErrors))
		result.Close()
		close(results)
		done <- err
	}()
	var readErr error
	for result := range results {
		func() {
			defer result.Close()
			if readErr != nil {
				return
			}
			result.ReadRows(func(n int, columns []*vector.Vector) bool {
				readErr = consume(n, columns)
				if readErr != nil {
					cancel()
				}
				return readErr == nil
			})
		}()
	}
	return errors.Join(readErr, <-done)
}

func readViewMetadataBench(ctx context.Context, cn embed.ServiceOperator, svc cnservice.Service,
	database string, names []string, columns int, mode viewMetadataBenchMode, collect, show, fullSchema, widened bool,
) (sample viewMetadataBenchSample) {
	start := time.Now()
	defer func() { sample.elapsed = time.Since(start) }()
	ctx, cancel := context.WithTimeout(ctx, time.Minute)
	defer cancel()
	sample.err = withNativeViewSchemaStatement(ctx, cn, svc, database, nil,
		func(compiler *frontend.TxnCompilerContext, op client.TxnOperator, proc *process.Process) error {
			before := op.GetWorkspace().WriteOffset()
			execCtx := ctx
			var request *plan.ViewMetadataRequest
			if !mode.legacy {
				request = compiler.NewViewMetadataRequest(ctx, frontend.NativeViewSchemaRoleAuthorizerForTest(compiler))
				defer request.Close()
				if err := request.ConfigureBenchmark(mode.memoOff, collect); err != nil {
					return err
				}
				execCtx = request.Context()
			}
			rowIndex := 0
			check := func(table, column, typ string) error {
				if rowIndex >= len(names)*columns {
					return fmt.Errorf("unexpected metadata row %d: %s.%s", rowIndex, table, column)
				}
				wantTable := names[rowIndex/columns]
				wantColumn := fmt.Sprintf("c%04d", rowIndex%columns)
				wantType := "int"
				if widened && rowIndex%columns == 0 {
					wantType = "bigint"
				}
				if table != wantTable || column != wantColumn || strings.ToLower(typ) != wantType {
					return fmt.Errorf("row %d: got %s.%s %s, want %s.%s %s", rowIndex, table, column, typ, wantTable, wantColumn, wantType)
				}
				rowIndex++
				return nil
			}
			if show {
				rows, err := frontend.ReadNativeViewMetadataSQLForTest(execCtx, compiler, "show columns from "+names[0])
				if err != nil {
					return err
				}
				for _, row := range rows {
					if len(row) < 2 {
						return fmt.Errorf("incomplete SHOW result: %v", row)
					}
					if err := check(names[0], row[0], row[1]); err != nil {
						return err
					}
				}
			} else {
				query := "select table_name, column_name, lower(column_type) from information_schema.columns where table_schema='" + database + "'"
				if !fullSchema {
					query += " and table_name in ('" + strings.Join(names, "','") + "')"
				}
				query += " order by table_name, ordinal_position"
				// SQLExecutor 本来每次解析/优化；ForceRebuildPlan 会额外构造第二份计划，
				// 不能把这种测试强制开销算作正常消费者成本。
				options := executor.Options{}.WithDatabase(database).WithTxn(op).WithKeepTxnAlive().WithDisableIncrStatement()
				if err := streamViewMetadataBench(execCtx, svc, query, options, func(n int, vectors []*vector.Vector) error {
					if len(vectors) != 3 {
						return fmt.Errorf("metadata projection width %d, want 3", len(vectors))
					}
					for row := 0; row < n; row++ {
						for _, v := range vectors {
							if v.IsNull(uint64(row)) {
								return fmt.Errorf("unexpected NULL in metadata identity/type")
							}
						}
						if err := check(vectors[0].GetStringAt(row), vectors[1].GetStringAt(row), vectors[2].GetStringAt(row)); err != nil {
							return err
						}
					}
					return nil
				}); err != nil {
					return err
				}
			}
			if rowIndex != len(names)*columns {
				return fmt.Errorf("metadata rows %d, want %d", rowIndex, len(names)*columns)
			}
			if request != nil {
				var err error
				sample.stats, err = request.BenchmarkStats()
				if err != nil {
					return err
				}
				request.Close()
				wantRoots := 0
				for _, name := range names {
					if strings.HasPrefix(name, "v") || strings.HasPrefix(name, "middle_") {
						wantRoots++
					}
				}
				if sample.stats.Roots != wantRoots {
					return fmt.Errorf("opt-in roots %d, want %d (excluded views must not bind)", sample.stats.Roots, wantRoots)
				}
			}
			if before != op.GetWorkspace().WriteOffset() {
				return fmt.Errorf("metadata read changed catalog workspace")
			}
			generation, err := proc.GetExecutionResourceBudget()
			if err != nil {
				return err
			}
			sample.peak = generation.Peak()
			if generation.Used() != 0 {
				return fmt.Errorf("request retained %d charged bytes", generation.Used())
			}
			return nil
		})
	return sample
}

func reportViewMetadataBench(b *testing.B, samples []viewMetadataBenchSample) {
	b.Helper()
	durations := make([]time.Duration, len(samples))
	var binds, lookups, hits, peak uint64
	for i, sample := range samples {
		require.NoError(b, sample.err, "sample %d", i)
		durations[i] = sample.elapsed
		binds += sample.stats.ViewBinds
		lookups += sample.stats.RelationLookups
		hits += sample.stats.MemoHits
		peak = max(peak, sample.peak)
	}
	if path := os.Getenv("MO_VIEW_BENCH_OUTPUT"); path != "" {
		// 单条记录最多 100000 个 duration，且只在计时外写文件。
		file, err := os.OpenFile(path, os.O_CREATE|os.O_WRONLY|os.O_APPEND, 0600)
		require.NoError(b, err)
		err = json.NewEncoder(file).Encode(struct {
			Benchmark, Go, OS, Arch                                string
			GOMAXPROCS                                             int
			DurationsNS                                            []time.Duration
			ViewBinds, RelationLookups, MemoHits, ChargedPeakBytes uint64
		}{b.Name(), runtime.Version(), runtime.GOOS, runtime.GOARCH, runtime.GOMAXPROCS(0), durations, binds, lookups, hits, peak})
		closeErr := file.Close()
		require.NoError(b, errors.Join(err, closeErr))
	}
	slices.Sort(durations)
	for _, percentile := range []int{50, 95, 99} {
		// p99 不从不足 100 个样本推断；所有样本总是保留最大值。
		if percentile == 99 && len(samples) < 100 {
			continue
		}
		b.ReportMetric(float64(durations[(len(durations)*percentile+99)/100-1].Nanoseconds()), fmt.Sprintf("p%d-ns/op", percentile))
	}
	b.ReportMetric(float64(durations[len(durations)-1].Nanoseconds()), "max-ns/op")
	b.ReportMetric(float64(binds)/float64(len(samples)), "view-binds/op")
	b.ReportMetric(float64(lookups)/float64(len(samples)), "relation-resolves/op")
	b.ReportMetric(float64(hits)/float64(len(samples)), "memo-hits/op")
	b.ReportMetric(float64(peak), "charged-peak-B")
	b.ReportMetric(float64(len(samples))/b.Elapsed().Seconds(), "requests/s")
}

func BenchmarkNativeViewMetadata(b *testing.B) {
	b.StopTimer()
	collect := os.Getenv("MO_VIEW_BENCH_COUNTS") != "0"
	runNativeViewSchemaCluster(b, func(cn embed.ServiceOperator, svc cnservice.Service) {
		for _, shape := range []viewMetadataBenchShape{
			{"single", 1, 1, 8, 0, false, false},
			{"table_single", 1, 1, 8, 0, true, false},
			{"selective10000", 1, 1, 8, 10000, false, false},
			{"tables1000", 1000, 1, 8, 0, true, true},
			{"chain32", 1, 32, 8, 0, false, false},
			{"shared128_chain16", 128, 17, 8, 0, false, false},
			{"shallow1000", 1000, 1, 8, 0, false, true},
			{"shallow10000", 10000, 1, 8, 0, false, true},
			{"wide4096", 1, 1, 4096, 0, false, false},
		} {
			b.Run(shape.name, func(b *testing.B) {
				database, names := setupViewMetadataBench(b, svc, shape)
				for _, mode := range viewMetadataBenchModes {
					b.Run(mode.name, func(b *testing.B) {
						for _, concurrency := range []int{1, 8} {
							b.Run(fmt.Sprintf("concurrency=%d/counts=%t", concurrency, collect), func(b *testing.B) {
								if b.N > 100000 {
									b.Fatal("bounded latency recorder supports at most 100000 samples; use -benchtime=Nx")
								}
								ctx, cancel := context.WithCancelCause(viewMetadataBenchContext())
								defer cancel(nil)
								warm := readViewMetadataBench(ctx, cn, svc, database, names, shape.columns, mode, collect, false, shape.fullSchema, false)
								require.NoError(b, warm.err)
								samples := make([]viewMetadataBenchSample, b.N)
								stopProfile := startViewMetadataBenchProfile(b)
								defer stopProfile()
								var next atomic.Int64
								var workers sync.WaitGroup
								start := make(chan struct{})
								for range concurrency {
									workers.Add(1)
									go func() {
										defer workers.Done()
										<-start
										for {
											i := int(next.Add(1) - 1)
											if i >= len(samples) {
												return
											}
											samples[i] = readViewMetadataBench(ctx, cn, svc, database, names, shape.columns, mode, collect, false, shape.fullSchema, false)
											if samples[i].err != nil {
												cancel(samples[i].err)
											}
										}
									}()
								}
								b.ReportAllocs()
								b.ResetTimer()
								close(start)
								workers.Wait()
								b.StopTimer()
								stopProfile()
								// 保留导致停止的主因，不能让较早下标的取消错误掩盖它。
								require.NoError(b, context.Cause(ctx))
								reportViewMetadataBench(b, samples)
							})
						}
					})
				}
			})
			if b.Failed() {
				return // 失败 fixture 不得污染下一个 workload。
			}
		}
	})
}

func TestNativeViewMetadataBenchmarkModes(t *testing.T) {
	runNativeViewSchemaCluster(t, func(cn embed.ServiceOperator, svc cnservice.Service) {
		shape := viewMetadataBenchShape{name: "contract", roots: 2, depth: 2, columns: 2, unrelated: 1}
		database, names := setupViewMetadataBench(t, svc, shape)
		ctx := viewMetadataBenchContext()
		for _, mode := range viewMetadataBenchModes {
			for _, collect := range []bool{false, true} {
				for _, show := range []bool{false, true} {
					t.Run(fmt.Sprintf("%s/counts=%t/show=%t", mode.name, collect, show), func(t *testing.T) {
						selected := names
						if show {
							selected = names[:1]
						}
						sample := readViewMetadataBench(ctx, cn, svc, database, selected, 2, mode, collect, show, false, false)
						require.NoError(t, sample.err)
						if !mode.legacy && collect {
							require.Positive(t, sample.stats.ViewBinds)
							if !mode.memoOff && !show {
								require.Positive(t, sample.stats.MemoHits)
							}
						}
					})
				}
			}
		}
		t.Run("ordinary table", func(t *testing.T) {
			for _, mode := range viewMetadataBenchModes {
				sample := readViewMetadataBench(ctx, cn, svc, database, []string{"source"}, 2, mode, true, false, false, false)
				require.NoError(t, sample.err, mode.name)
				if !mode.legacy {
					require.Zero(t, sample.stats.Roots)
					require.Zero(t, sample.stats.ViewBinds)
					require.Zero(t, sample.stats.MemoHits)
				}
			}
		})
		t.Run("planning consumers", func(t *testing.T) {
			for _, query := range []string{
				"select * from v00000 limit 0",
				"create table plan_only_target as select * from v00000",
				"prepare p from 'select * from v00000'",
			} {
				for _, mode := range viewMetadataBenchModes {
					sample := planViewMetadataBench(ctx, cn, svc, database, query, 2, mode, true)
					require.NoError(t, sample.err, "%s: %s", mode.name, query)
					if !mode.legacy {
						require.Positive(t, sample.stats.ViewBinds)
						require.Zero(t, sample.stats.MemoHits, "S3 不能消费描述 memo 的合成扫描")
					}
				}
			}
		})
		t.Run("independent concurrent statements", func(t *testing.T) {
			start := make(chan struct{})
			done := make(chan viewMetadataBenchSample, 2)
			for range 2 {
				go func() {
					<-start
					done <- readViewMetadataBench(ctx, cn, svc, database, names, 2, viewMetadataBenchModes[2], true, false, false, false)
				}()
			}
			close(start)
			first, second := <-done, <-done
			require.NoError(t, first.err)
			require.NoError(t, second.err)
			require.Equal(t, first.stats, second.stats, "独立语句不能共享 memo/计数")
		})
		t.Run("cancelled statement", func(t *testing.T) {
			cancelled, cancel := context.WithCancel(ctx)
			cancel()
			sample := readViewMetadataBench(cancelled, cn, svc, database, names, 2, viewMetadataBenchModes[2], true, false, false, false)
			require.ErrorIs(t, sample.err, context.Canceled)
		})
		require.NoError(t, viewMetadataBenchExec(ctx, svc, database, "alter table source modify column c0000 bigint"))
		for _, mode := range viewMetadataBenchModes {
			sample := readViewMetadataBench(ctx, cn, svc, database, names, 2, mode, true, false, false, true)
			require.NoError(t, sample.err, mode.name)
		}
	})
}
