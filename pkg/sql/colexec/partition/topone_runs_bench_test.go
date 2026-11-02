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

package partition

import (
	"strconv"
	"strings"
	"testing"

	"github.com/matrixorigin/matrixone/pkg/common/mpool"
	"github.com/matrixorigin/matrixone/pkg/container/batch"
	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/container/vector"
	"github.com/matrixorigin/matrixone/pkg/sql/colexec"
	"github.com/matrixorigin/matrixone/pkg/testutil"
	"github.com/matrixorigin/matrixone/pkg/vm"
	"github.com/stretchr/testify/require"
)

// Exercise the same operator/input on both revisions. GroupedIncreasing is the
// replacement-heavy case; the other layouts expose neutral/negative cases and
// K2 checks that the general heap path remains unchanged. These microbenchmarks
// supplement (rather than substitute for) the complete Q4 native-client run.
func BenchmarkPartitionTopOneRuns(b *testing.B) {
	const rowCount = 65536
	for _, tc := range []struct {
		name   string
		layout string
		limit  uint64
	}{
		{"GroupedIncreasingK1", "increasing", 1},
		{"GroupedRandomK1", "random", 1},
		{"InterleavedK1", "interleaved", 1},
		{"EveryRowNewGroupK1", "unique", 1},
		{"GroupedIncreasingK2", "increasing", 2},
	} {
		b.Run(tc.name, func(b *testing.B) {
			proc := testutil.NewProcessWithOwnedMPool(b, "", mpool.MustNewZero())
			b.Cleanup(func() { require.Zero(b, proc.Mp().CurrNB()) })
			b.Cleanup(proc.Free)
			rows := make([]topOneRunRow, rowCount)
			groupCount := rowCount / 1024
			if tc.layout == "unique" {
				groupCount = rowCount
			}
			for i := range rows {
				group, score := i/1024, i%1024
				switch tc.layout {
				case "random":
					score = (i*7919 + 17) % 1024
				case "interleaved":
					group = i % groupCount
				case "unique":
					group = i
				}
				rows[i] = topOneRunRow{group: int32(group), score: int64(score), id: int64(i)}
			}
			payload := []byte(strings.Repeat("payload", 12))
			b.ReportAllocs()
			b.ResetTimer()
			for iteration := 0; iteration < b.N; iteration++ {
				b.StopTimer()
				child := colexec.NewMockOperator()
				func() {
					defer func() {
						child.Free(proc, false, nil)
						require.Zero(b, proc.Mp().CurrNB())
					}()
					arg := topOneRunArgument(true, true)
					defer arg.Free(proc, false, nil)
					arg.Limit = newTopNArgument(tc.limit).Limit
					arg.PreReduce = true // emit packed groups, keeping client overhead small
					arg.AppendChild(child)
					// Batches exceed UnitLimit and do not divide the 1024-row
					// group runs, so both chunk and input boundaries are exercised.
					for start := 0; start < rowCount; start += 8191 {
						bat := topOneRunBatch(b, proc, rows[start:min(start+8191, rowCount)])
						child.WithBatchs([]*batch.Batch{bat})
						vec := vector.NewVec(types.T_varchar.ToType())
						bat.Vecs = append(bat.Vecs, vec)
						for row := 0; row < bat.RowCount(); row++ {
							require.NoError(b, vector.AppendBytes(vec, payload, false, proc.Mp()))
						}
					}
					require.NoError(b, arg.Prepare(proc))
					b.StartTimer()
					outputRows := 0
					for {
						result, err := arg.Call(proc)
						require.NoError(b, err)
						if result.Batch == nil || result.Status == vm.ExecStop {
							break
						}
						outputRows += result.Batch.RowCount()
					}
					b.StopTimer()
					wantRows := groupCount * int(tc.limit)
					require.Equal(b, wantRows, outputRows)
					b.ReportMetric(float64(outputRows), "output_rows")
				}()
			}
		})
	}
}

// A retained cheap winner must prevent comparisons between expensive losers.
// Time the full operator; setup and cleanup use the same fixture as the numeric controls.
func BenchmarkPartitionTopOneLongPrefix(b *testing.B) {
	proc := testutil.NewProcessWithOwnedMPool(b, "", mpool.MustNewZero())
	b.Cleanup(func() { require.Zero(b, proc.Mp().CurrNB()) })
	b.Cleanup(proc.Free)
	keys := make([]string, 16384)
	keys[0] = "a"
	prefix := strings.Repeat("z", 4096)
	for row := 1; row < len(keys); row++ {
		keys[row] = prefix + strconv.Itoa(row)
	}
	b.ReportAllocs()
	b.ResetTimer()
	for iteration := 0; iteration < b.N; iteration++ {
		b.StopTimer()
		func() {
			child := colexec.NewMockOperator()
			defer func() { child.Free(proc, false, nil); require.Zero(b, proc.Mp().CurrNB()) }()
			arg := newTopNArgument(1)
			defer arg.Free(proc, false, nil)
			arg.OrderBySpecs = arg.OrderBySpecs[:2]
			arg.OrderBySpecs[1].Expr = topNCol(1, types.T_varchar)
			arg.OrderBySpecs[1].Flag = 0
			arg.PreReduce = true
			arg.AppendChild(child)
			child.WithBatchs([]*batch.Batch{topOneStringRunBatch(b, proc, keys, 0)})
			require.NoError(b, arg.Prepare(proc))
			b.StartTimer()
			result, err := arg.Call(proc)
			require.NoError(b, err)
			require.NotNil(b, result.Batch)
			require.Equal(b, 1, result.Batch.RowCount())
			require.Equal(b, int64(0), vector.GetFixedAtWithTypeCheck[int64](result.Batch.Vecs[2], 0))
			result, err = arg.Call(proc)
			require.NoError(b, err)
			require.True(b, result.Batch == nil || result.Status == vm.ExecStop)
			b.StopTimer()
		}()
	}
}
