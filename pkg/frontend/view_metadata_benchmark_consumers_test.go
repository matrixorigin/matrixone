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
	"fmt"
	"os"
	"slices"
	"testing"
	"time"

	"github.com/matrixorigin/matrixone/pkg/cnservice"
	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/embed"
	"github.com/matrixorigin/matrixone/pkg/frontend"
	"github.com/matrixorigin/matrixone/pkg/sql/parsers/dialect/mysql"
	"github.com/matrixorigin/matrixone/pkg/sql/plan"
	"github.com/matrixorigin/matrixone/pkg/txn/client"
	"github.com/matrixorigin/matrixone/pkg/vm/process"
	"github.com/stretchr/testify/require"
)

func BenchmarkNativeViewMetadataShow(b *testing.B) {
	b.StopTimer()
	runNativeViewSchemaCluster(b, func(cn embed.ServiceOperator, svc cnservice.Service) {
		database, names := setupViewMetadataBench(b, svc, viewMetadataBenchShape{name: "single", roots: 1, depth: 1, columns: 8})
		for _, mode := range viewMetadataBenchModes {
			b.Run(mode.name, func(b *testing.B) {
				benchmarkViewMetadataSerial(b, func() viewMetadataBenchSample {
					return readViewMetadataBench(viewMetadataBenchContext(), cn, svc, database, names, 8, mode, os.Getenv("MO_VIEW_BENCH_COUNTS") != "0", true, false, false)
				})
			})
		}
	})
}

// 此项测当前仍保留兼容维护的 DDL 与其后首读，不声称 S8 的零 refresh-only
// 成本已经成立。每次 DDL 改变类型，首读必须验证新类型，不能只计时不验结果。
func BenchmarkNativeViewMetadataDDLFirstRead(b *testing.B) {
	b.StopTimer()
	runNativeViewSchemaCluster(b, func(cn embed.ServiceOperator, svc cnservice.Service) {
		for _, fanout := range []int{0, 1000, 10000} {
			b.Run(fmt.Sprintf("fanout=%d", fanout), func(b *testing.B) {
				database, names := setupViewMetadataBench(b, svc, viewMetadataBenchShape{name: "ddl", roots: fanout, depth: 1, columns: 8})
				selected := []string{"source"}
				if len(names) > 0 {
					selected = names[:1]
				}
				for _, mode := range viewMetadataBenchModes {
					b.Run(mode.name, func(b *testing.B) {
						if b.N > 100000 {
							b.Fatal("use -benchtime=Nx with N <= 100000")
						}
						ctx := viewMetadataBenchContext()
						require.NoError(b, viewMetadataBenchExec(ctx, svc, database, "alter table source modify column c0000 int"))
						samples := make([]viewMetadataBenchSample, b.N)
						ddl := make([]time.Duration, b.N)
						stopProfile := startViewMetadataBenchProfile(b)
						defer stopProfile()
						b.ReportAllocs()
						b.ResetTimer()
						for i := range samples {
							widened := i%2 == 0
							typ := "int"
							if widened {
								typ = "bigint"
							}
							start := time.Now()
							err := viewMetadataBenchExec(ctx, svc, database, "alter table source modify column c0000 "+typ)
							ddl[i] = time.Since(start)
							require.NoError(b, err)
							samples[i] = readViewMetadataBench(ctx, cn, svc, database, selected, 8, mode, os.Getenv("MO_VIEW_BENCH_COUNTS") != "0", false, false, widened)
							require.NoError(b, samples[i].err)
						}
						b.StopTimer()
						stopProfile()
						reportViewMetadataBench(b, samples)
						slices.Sort(ddl)
						b.ReportMetric(float64(ddl[(len(ddl)*95+99)/100-1]), "ddl-p95-ns/op")
						b.ReportMetric(float64(ddl[len(ddl)-1]), "ddl-max-ns/op")
					})
				}
			})
		}
	})
}

func benchmarkViewMetadataSerial(b *testing.B, run func() viewMetadataBenchSample) {
	b.Helper()
	if b.N > 100000 {
		b.Fatal("use -benchtime=Nx with N <= 100000")
	}
	require.NoError(b, run().err) // 显式 warm-up，不计为 cold 样本。
	samples := make([]viewMetadataBenchSample, b.N)
	stopProfile := startViewMetadataBenchProfile(b)
	defer stopProfile()
	b.ReportAllocs()
	b.ResetTimer()
	for i := range samples {
		samples[i] = run()
		require.NoError(b, samples[i].err)
	}
	b.StopTimer()
	stopProfile()
	reportViewMetadataBench(b, samples)
}

// 只测 S3 的真实规划消费者，不执行 CTAS、不把描述 memo 当成执行子树。
func planViewMetadataBench(ctx context.Context, cn embed.ServiceOperator, svc cnservice.Service,
	database, sql string, columns int, mode viewMetadataBenchMode, collect bool,
) (sample viewMetadataBenchSample) {
	start := time.Now()
	defer func() { sample.elapsed = time.Since(start) }()
	ctx, cancel := context.WithTimeout(ctx, time.Minute)
	defer cancel()
	sample.err = withNativeViewSchemaStatement(ctx, cn, svc, database, nil,
		func(compiler *frontend.TxnCompilerContext, op client.TxnOperator, proc *process.Process) error {
			before := op.GetWorkspace().WriteOffset()
			statements, err := mysql.Parse(ctx, sql, 1)
			if err != nil {
				return err
			}
			defer func() {
				for _, statement := range statements {
					statement.Free()
				}
			}()
			if len(statements) != 1 {
				return fmt.Errorf("benchmark requires one statement")
			}
			var p *plan.Plan
			if mode.legacy {
				p, err = plan.BuildPlan(compiler, statements[0], false)
			} else {
				request := compiler.NewViewSchemaRequest(ctx, frontend.NativeViewSchemaRoleAuthorizerForTest(compiler))
				defer request.Close()
				if err = request.ConfigureBenchmark(mode.memoOff, collect); err != nil {
					return err
				}
				p, err = request.BuildPlan(statements[0], false)
				if err != nil {
					return err
				}
				sample.stats, err = request.BenchmarkStats()
				request.Close()
			}
			if err != nil {
				return err
			}
			if prepared := p.GetDcl().GetPrepare(); prepared != nil {
				p = prepared.Plan
			}
			defs := plan.GetResultColumnsFromPlan(p)
			if target := p.GetDdl().GetCreateTable(); target != nil {
				defs = nil
				for _, column := range target.TableDef.Cols {
					if !column.Hidden {
						defs = append(defs, column)
					}
				}
			}
			if len(defs) != columns {
				return fmt.Errorf("planned columns %d, want %d", len(defs), columns)
			}
			for i, column := range defs {
				if column.Name != fmt.Sprintf("c%04d", i) || column.Typ.Id != int32(types.T_int32) {
					return fmt.Errorf("unexpected planned column %s: type %v", column.Name, column.Typ)
				}
			}
			if op.GetWorkspace().WriteOffset() != before {
				return fmt.Errorf("planning changed catalog workspace")
			}
			generation, err := proc.GetExecutionResourceBudget()
			if err != nil {
				return err
			}
			sample.peak = generation.Peak()
			if generation.Used() != 0 {
				return fmt.Errorf("planning retained %d charged bytes", generation.Used())
			}
			return nil
		})
	return sample
}

func BenchmarkNativeViewPlanning(b *testing.B) {
	b.StopTimer()
	runNativeViewSchemaCluster(b, func(cn embed.ServiceOperator, svc cnservice.Service) {
		for _, depth := range []int{1, 32} {
			b.Run(fmt.Sprintf("depth=%d", depth), func(b *testing.B) {
				database, _ := setupViewMetadataBench(b, svc, viewMetadataBenchShape{name: "planning", roots: 1, depth: depth, columns: 8})
				for _, consumer := range []struct{ name, sql string }{
					{"select", "select * from v00000 limit 0"},
					{"ctas", "create table plan_only_target as select * from v00000"},
					{"prepare", "prepare p from 'select * from v00000'"},
				} {
					b.Run(consumer.name, func(b *testing.B) {
						for _, mode := range viewMetadataBenchModes {
							b.Run(mode.name, func(b *testing.B) {
								benchmarkViewMetadataSerial(b, func() viewMetadataBenchSample {
									return planViewMetadataBench(viewMetadataBenchContext(), cn, svc, database, consumer.sql, 8, mode, os.Getenv("MO_VIEW_BENCH_COUNTS") != "0")
								})
							})
						}
					})
				}
			})
		}
	})
}
