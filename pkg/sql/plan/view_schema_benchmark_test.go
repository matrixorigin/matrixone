//go:build viewmetadatabench

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

package plan

import (
	"context"
	"fmt"
	"testing"

	"github.com/matrixorigin/matrixone/pkg/common/moerr"
	"github.com/stretchr/testify/require"
)

func TestViewSchemaBenchmarkInstrumentation(t *testing.T) {
	f := newViewSchemaTestFixture(t)
	f.addView(t, "leaf", "select n_nationkey as k from nation")
	f.addView(t, "shared", "select k from leaf")
	f.addView(t, "root_a", "select k from shared")
	f.addView(t, "root_b", "select k from shared")
	type value struct {
		columns []*ColDef
		deps    []ViewDependency
		origins []ViewSchemaColumnProvenance
	}
	var oracle []value
	for _, disabled := range []bool{true, false} {
		for _, collect := range []bool{false, true} {
			t.Run(fmt.Sprintf("memo_off=%t/collect=%t", disabled, collect), func(t *testing.T) {
				r := NewViewSchemaRequest(t.Context(), f.provider)
				defer r.Close()
				require.NoError(t, r.ConfigureBenchmark(disabled, collect))
				var values []value
				for _, name := range []string{"root_a", "root_b"} {
					func() {
						result, err := r.Describe("tpch", name, nil)
						require.NoError(t, err)
						defer result.Release()
						columns, err := result.Columns()
						require.NoError(t, err)
						require.Len(t, columns, 1)
						require.Equal(t, "k", columns[0].Name)
						deps, err := result.Dependencies()
						require.NoError(t, err)
						require.Len(t, deps, 4)
						origins, err := result.Provenance()
						require.NoError(t, err)
						values = append(values, value{columns, deps, origins})
					}()
				}
				if oracle == nil {
					oracle = values
				} else {
					require.Equal(t, oracle, values)
				}
				require.Error(t, r.ConfigureBenchmark(!disabled, !collect), "不能在工作中切换基准模式")
				r.Close()
				stats, err := r.BenchmarkStats()
				require.NoError(t, err)
				require.Equal(t, 2, stats.Roots)
				require.Equal(t, 6, stats.LogicalWork, "memo 不得减免结构性工作预算")
				require.EqualValues(t, 2, stats.CompletedRootBinds)
				if disabled {
					require.Zero(t, stats.MemoHits)
				} else {
					require.EqualValues(t, 1, stats.MemoHits)
				}
				if collect {
					wantBinds := uint64(4)
					if disabled {
						wantBinds = 6
					}
					require.Equal(t, wantBinds, stats.ViewBinds)
					require.Greater(t, stats.RelationLookups, stats.CompletedRootBinds)
				} else {
					require.Zero(t, stats.ViewBinds)
					require.Zero(t, stats.RelationLookups)
					require.Zero(t, stats.DatabaseLookups)
				}
				require.Zero(t, f.generation.Used())
				require.ErrorIs(t, r.ConfigureBenchmark(false, true), ErrViewSchemaClosed)
			})
		}
	}
}

func TestViewSchemaBenchmarkActiveBinder(t *testing.T) {
	f := newViewSchemaTestFixture(t)
	f.addView(t, "v", "select n_nationkey from nation")
	r := NewViewSchemaRequest(t.Context(), f.provider)
	defer r.Close()
	require.NoError(t, r.ConfigureBenchmark(false, true))
	entered := make(chan struct{})
	f.compiler.resolve = func(ctx context.Context, _, _ string, _ *Snapshot) error {
		close(entered)
		<-ctx.Done()
		return ctx.Err()
	}
	done := make(chan error, 1)
	go func() {
		result, err := r.Describe("tpch", "v", nil)
		result.Release()
		done <- err
	}()
	viewSchemaTestWait(t, entered)
	require.ErrorIs(t, r.ConfigureBenchmark(true, false), ErrViewSchemaBusy)
	_, err := r.BenchmarkStats()
	require.ErrorIs(t, err, ErrViewSchemaBusy)
	r.Close()
	require.ErrorIs(t, <-done, ErrViewSchemaClosed)
	stats, err := r.BenchmarkStats()
	require.NoError(t, err)
	require.EqualValues(t, 1, stats.RelationLookups)
	require.Zero(t, stats.ViewBinds)
	require.Zero(t, f.generation.Used())
}

func TestViewSchemaBenchmarkPreservesErrors(t *testing.T) {
	f := newViewSchemaTestFixture(t)
	f.addView(t, "broken", "select missing from nation")
	for _, disabled := range []bool{false, true} {
		for _, collect := range []bool{false, true} {
			r := NewViewSchemaRequest(t.Context(), f.provider)
			func() {
				defer r.Close()
				require.NoError(t, r.ConfigureBenchmark(disabled, collect))
				result, err := r.Describe("tpch", "broken", nil)
				require.Nil(t, result)
				require.True(t, moerr.IsMoErrCode(err, moerr.ErrBadFieldError), "错误分类必须与权威 binder 相同: %v", err)
				require.Empty(t, r.memo)
				require.Empty(t, r.nestedMemo)
				r.cancel(context.Canceled)
				result, err = r.Describe("tpch", "broken", nil)
				require.Nil(t, result)
				require.ErrorIs(t, err, context.Canceled)
			}()
			require.Zero(t, f.generation.Used())
		}
	}
}

// 每个 op 都创建/关闭一个 request；repeat 只表示同一语句多次消费，
// 不是跨语句缓存。此微基准使用内存目录，不替代 native 消费者预算。
func BenchmarkViewSchemaStatements(b *testing.B) {
	for _, shape := range []struct {
		name                 string
		roots, depth, repeat int
	}{
		{"single", 1, 1, 1},
		{"same_statement_repeat", 1, 1, 8},
		{"chain32", 1, 32, 1},
		{"shared128_chain16", 128, 17, 1},
		{"shallow1000", 1000, 1, 1},
		{"shallow10000", 10000, 1, 1},
	} {
		b.Run(shape.name, func(b *testing.B) {
			f := newViewSchemaTestFixture(b)
			source, projection := "nation", "n_nationkey as k"
			for layer := 1; layer < shape.depth; layer++ {
				name := fmt.Sprintf("chain_%d", layer)
				f.addView(b, name, "select "+projection+" from "+source)
				source, projection = name, "k"
			}
			roots := make([]string, shape.roots)
			for i := range roots {
				roots[i] = fmt.Sprintf("root_%d", i)
				f.addView(b, roots[i], "select "+projection+" from "+source)
			}
			for _, disabled := range []bool{true, false} {
				b.Run(fmt.Sprintf("memo_off=%t", disabled), func(b *testing.B) {
					var total ViewSchemaBenchmarkStats
					b.ReportAllocs()
					b.ResetTimer()
					for range b.N {
						func() {
							r := NewViewSchemaRequest(b.Context(), f.provider)
							defer r.Close()
							require.NoError(b, r.ConfigureBenchmark(disabled, true))
							for range shape.repeat {
								for _, name := range roots {
									func() {
										result, err := r.Describe("tpch", name, nil)
										require.NoError(b, err)
										defer result.Release()
										columns, err := result.Columns()
										require.NoError(b, err)
										require.Len(b, columns, 1)
										require.Equal(b, "k", columns[0].Name)
									}()
								}
							}
							stats, err := r.BenchmarkStats()
							require.NoError(b, err)
							total.ViewBinds += stats.ViewBinds
							total.RelationLookups += stats.RelationLookups
							total.MemoHits += stats.MemoHits
						}()
					}
					b.StopTimer()
					require.Zero(b, f.generation.Used())
					b.ReportMetric(float64(total.ViewBinds)/float64(b.N), "view-binds/op")
					b.ReportMetric(float64(total.RelationLookups)/float64(b.N), "relation-resolves/op")
					b.ReportMetric(float64(total.MemoHits)/float64(b.N), "memo-hits/op")
					b.ReportMetric(float64(shape.roots), "selected-roots/op")
				})
			}
		})
	}
}
