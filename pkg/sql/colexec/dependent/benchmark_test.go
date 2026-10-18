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

package dependent_test

import (
	"context"
	"fmt"
	"testing"

	"github.com/matrixorigin/matrixone/pkg/common/mpool"
	"github.com/matrixorigin/matrixone/pkg/container/batch"
	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/container/vector"
	"github.com/matrixorigin/matrixone/pkg/pb/plan"
	"github.com/matrixorigin/matrixone/pkg/sql/colexec/dependent"
	"github.com/matrixorigin/matrixone/pkg/sql/parsers/dialect/mysql"
	plan2 "github.com/matrixorigin/matrixone/pkg/sql/plan"
	"github.com/matrixorigin/matrixone/pkg/testutil"
	"github.com/matrixorigin/matrixone/pkg/vm/process"
	"github.com/stretchr/testify/require"
)

// Separate E, I and lexical depth while checking the independent row oracle,
// reader count, one-time compilation and zero live resources after each run.
func BenchmarkDependentExecutionScaling(b *testing.B) {
	for _, tc := range []struct{ outer, inner, depth int }{{1, 8, 2}, {16, 8, 2}, {16, 64, 2}, {16, 64, 8}} {
		b.Run(fmt.Sprintf("E%d_I%d_D%d", tc.outer, tc.inner, tc.depth), func(b *testing.B) {
			proc := testutil.NewProcessWithOwnedMPool(b, "", mpool.MustNewZero())
			defer proc.Free()
			inner := "SELECT x.N_NATIONKEY+n.N_NATIONKEY FROM NATION x WHERE x.N_NATIONKEY=n.N_NATIONKEY"
			for range tc.depth - 1 {
				inner = "SELECT (" + inner + ")"
			}
			sql := "SELECT n.N_NATIONKEY,(" + inner + ") FROM NATION n"
			stmts, err := mysql.Parse(proc.Ctx, sql, 1)
			require.NoError(b, err)
			defer stmts[0].Free()
			optimizer := plan2.NewMockOptimizer(true, proc)
			p, err := plan2.BuildPlan(optimizer.CurrentContext(), stmts[0], false)
			require.NoError(b, err)
			require.NotNil(b, p.GetQuery().Correlation)
			budget, err := proc.GetExecutionResourceBudget()
			require.NoError(b, err)
			registry, err := mpool.NewAllocationAccountRegistry(1, 8192)
			require.NoError(b, err)
			account, err := registry.OpenWithController(1<<28, budget)
			require.NoError(b, err)
			defer func() {
				account.Seal()
				_, err := registry.Finalize(account)
				require.NoError(b, err)
			}()
			allocation, err := vector.NewAllocationAccountSelection(account, mpool.AllocationOwnerDependent, 1, 2, 3, 4)
			require.NoError(b, err)
			compiled, tableOpened, sourceOpened, sourceClosed, scanned := 0, 0, 0, 0, 0
			template, err := dependent.Compile(proc.Ctx, p.GetQuery(), func(node *plan.Node) (dependent.SourceFactory, error) {
				compiled++
				isTable := node.NodeType == plan.Node_TABLE_SCAN
				isOuter := node.NodeId == 0
				return func(child *process.Process, a *vector.AllocationAccountSelection, _ *mpool.AllocationAccount) (dependent.Source, error) {
					sourceOpened++
					rows := 1
					cols := 0
					if isTable {
						tableOpened++
						rows = tc.inner
						cols = len(node.TableDef.Cols)
						if isOuter {
							rows = tc.outer
						}
						scanned += rows
					}
					bat := batch.NewWithSize(cols)
					for i := range cols {
						var err error
						bat.Vecs[i], err = vector.NewOffHeapVecWithTypeAndAllocation(types.T_int32.ToType(), a)
						if err != nil {
							bat.Clean(child.Mp())
							return nil, err
						}
						for row := range rows {
							value := int32(row + 1)
							if isOuter {
								value = int32(row%tc.inner + 1)
							}
							if err = vector.AppendFixed(bat.Vecs[i], value, false, child.Mp()); err != nil {
								bat.Clean(child.Mp())
								return nil, err
							}
						}
					}
					bat.SetRowCount(rows)
					return &testSource{batch: bat, close: func() { sourceClosed++; bat.Clean(child.Mp()) }}, nil
				}, nil
			})
			require.NoError(b, err)
			compileCount := compiled
			b.ReportAllocs()
			b.ResetTimer()
			run := func() {
				before := tableOpened
				execution, err := template.NewExecution(proc, allocation, account)
				if err != nil {
					b.Fatal(err)
				}
				defer execution.Close()
				count := 0
				for {
					out, err := execution.Next(context.Background())
					if err != nil {
						b.Fatal(err)
					}
					if out == nil {
						break
					}
					for row := range out.RowCount() {
						key := int32(count%tc.inner + 1)
						if vector.GetFixedAtNoTypeCheck[int32](out.Vecs[0], row) != key ||
							vector.GetFixedAtNoTypeCheck[int32](out.Vecs[1], row) != 2*key {
							b.Fatalf("incorrect row %d (expected %d, %d)", count, key, 2*key)
						}
						count++
					}
				}
				if err := execution.Close(); err != nil {
					b.Fatal(err)
				}
				if count != tc.outer || tableOpened-before != tc.outer+1 || sourceOpened != sourceClosed || compiled != compileCount {
					b.Fatalf("invalid work: rows=%d table opens=%d sources=%d/%d compiled=%d", count, tableOpened-before, sourceOpened, sourceClosed, compiled)
				}
				if budget.Used() != 0 || account.Snapshot().Used != 0 {
					b.Fatal("execution retained resources")
				}
			}
			for range b.N {
				run()
			}
			b.StopTimer()
			b.ReportMetric(float64(scanned)/float64(b.N), "input_rows/op")
			b.ReportMetric(float64(sourceOpened)/float64(b.N), "source_opens/op")
			b.ReportMetric(float64(budget.Peak()), "peak_budget_bytes")
			b.ReportMetric(float64(compileCount), "compiled_sources")
		})
	}
}
