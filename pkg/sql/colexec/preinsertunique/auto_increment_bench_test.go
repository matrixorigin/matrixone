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

package preinsertunique

import (
	"fmt"
	"testing"

	"github.com/matrixorigin/matrixone/pkg/common/mpool"
	"github.com/matrixorigin/matrixone/pkg/container/batch"
	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/container/vector"
	"github.com/matrixorigin/matrixone/pkg/testutil"
	"github.com/matrixorigin/matrixone/pkg/vm"
	"github.com/stretchr/testify/require"
)

// Each operation is one complete statement, including prepare and cleanup.
// Input construction is excluded; both GC allocations and admitted pool memory
// are reported so moving storage between heaps cannot look like free memory.
func BenchmarkInsertIgnoreAutoIncrementArbiter(b *testing.B) {
	const rows = 1024
	for _, workload := range []string{"all_accepted", "null_unique", "duplicate_dense", "explicit_mix"} {
		for _, parts := range []int{1, 32} {
			b.Run(fmt.Sprintf("%s/batches_%d", workload, parts), func(b *testing.B) {
				mp := mpool.MustNewZero()
				proc := testutil.NewProcessWithMPool(b, "", mp)
				input := batch.NewWithSize(5)
				for i := range input.Vecs {
					typ := types.T_bool.ToType()
					if i < 2 {
						typ = types.T_int32.ToType()
					}
					input.Vecs[i] = vector.NewVec(typ)
				}
				for i := 0; i < rows; i++ {
					require.NoError(b, vector.AppendFixed(input.Vecs[0], int32(i+1), false, mp))
					require.NoError(b, vector.AppendFixed(input.Vecs[1], int32(i+1), workload == "null_unique", mp))
					require.NoError(b, vector.AppendFixed(input.Vecs[2], false, false, mp))
					require.NoError(b, vector.AppendFixed(input.Vecs[3], false, false, mp))
					require.NoError(b, vector.AppendFixed(input.Vecs[4], workload != "explicit_mix" || i%4 != 0, false, mp))
				}
				input.SetRowCount(rows)
				defer input.Clean(mp)
				expectedRows := rows
				if workload == "duplicate_dense" {
					expectedRows /= 16
				}
				baseline := mp.CurrNB()
				b.ReportAllocs()
				b.SetBytes(int64(parts * rows * 8))
				b.ResetTimer()
				for n := 0; n < b.N; n++ {
					proc.SetStatementLastInsertID(0)
					arg := newInsertIgnoreAutoIncrementArgument()
					if err := arg.Prepare(proc); err != nil {
						b.Fatal(err)
					}
					for part := 0; part < parts; part++ {
						ids := vector.MustFixedColNoTypeCheck[int32](input.Vecs[0])
						keys := vector.MustFixedColNoTypeCheck[int32](input.Vecs[1])
						for i := range ids {
							ids[i] = int32(part*rows + i + 1)
							keys[i] = ids[i]
							if workload == "duplicate_dense" {
								keys[i] = (ids[i]-1)/16 + 1
							}
						}
						result, err := arg.callInsertIgnoreMultiDedup(proc, vm.CallResult{Batch: input})
						if err != nil {
							b.Fatal(err)
						}
						if result.Batch.RowCount() != expectedRows {
							b.Fatalf("accepted %d rows, want %d", result.Batch.RowCount(), expectedRows)
						}
					}
					arg.Free(proc, false, nil)
				}
				b.StopTimer()
				summary, _ := mp.ResourceSnapshot()
				b.ReportMetric(float64(summary.PeakLiveBytes)-float64(baseline), "pool-peak-B/op")
				b.ReportMetric(float64(parts*rows), "input-rows/op")
				b.ReportMetric(float64(parts*expectedRows), "accepted-rows/op")
				input.Clean(mp)
				require.Zero(b, mp.CurrNB())
			})
		}
	}
}
