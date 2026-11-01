// Copyright 2026 Matrix Origin
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at http://www.apache.org/licenses/LICENSE-2.0
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package window

import (
	"fmt"
	"testing"

	"github.com/matrixorigin/matrixone/pkg/common/mpool"
	"github.com/matrixorigin/matrixone/pkg/container/batch"
	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/container/vector"
	"github.com/matrixorigin/matrixone/pkg/pb/plan"
	"github.com/matrixorigin/matrixone/pkg/sql/colexec"
	"github.com/matrixorigin/matrixone/pkg/sql/colexec/aggexec"
	"github.com/matrixorigin/matrixone/pkg/testutil"
	"github.com/matrixorigin/matrixone/pkg/vm"
	"github.com/stretchr/testify/require"
)

// Immutable source setup is excluded. Each iteration includes Prepare, every
// Call through EOF, row-count/last-ordinal checks, Reset and Free. In particular,
// a streaming implementation must consume both child batches, not just measure
// its first output. Ordinary source vectors can live outside mpool accounting,
// so their backing is reported separately. Account peak measures only Window's
// owned admission; fixed/non-NULL backing peak also includes the source bytes.
func BenchmarkRowNumberFullDrain(b *testing.B) {
	for _, shape := range []struct{ rows, cols int }{{8, 64}, {128, 8}, {8193, 8}} {
		b.Run(fmt.Sprintf("rows%d_cols%d", shape.rows, shape.cols), func(b *testing.B) {
			mp := mpool.MustNewZero()
			proc := testutil.NewProcessWithOwnedMPool(b, "", mp)
			defer func() { proc.Free(); require.Zero(b, mp.CurrNB()) }()
			inputs := make([]*batch.Batch, 2)
			for part := range inputs {
				input := batch.NewWithSize(shape.cols)
				inputs[part] = input
				defer input.Clean(mp)
				n := shape.rows / 2
				if part == 1 {
					n = shape.rows - n
				}
				for col := range shape.cols {
					input.Vecs[col] = vector.NewVec(types.T_int64.ToType())
					for row := range n {
						require.NoError(b, vector.AppendFixed(input.Vecs[col], int64(row+col), false, mp))
					}
				}
				input.SetRowCount(n)
			}
			var sourceBacking uint64
			for _, input := range inputs {
				for _, v := range input.Vecs {
					sourceBacking += uint64(v.Allocated())
				}
			}
			spec := makeWindowSpec()
			spec.GetW().Name = "row_number"
			spec.GetW().WindowFunc = newFunExpr("row_number")
			ag := aggexec.MakeAggFunctionExpression(aggexec.WinIdOfRowNumber, false, nil, nil)
			registry, err := mpool.NewAllocationAccountRegistry(1, 512)
			require.NoError(b, err)
			account, err := registry.Open(64 << 20)
			require.NoError(b, err)
			var peakPool, peakInput uint64
			b.ReportAllocs()
			b.ResetTimer()
			for iteration := 0; iteration < b.N; iteration++ {
				child := colexec.NewMockOperator().WithBatchs(inputs)
				arg := &Window{WinSpecList: []*plan.Expr{spec}, Aggs: []aggexec.AggFuncExecExpression{ag}}
				arg.AppendChild(child)
				require.NoError(b, arg.SetAllocationAccount(account))
				require.NoError(b, arg.Prepare(proc))
				token := mp.StartResourcePeakEpoch()
				require.NotNil(b, token)
				rowsSeen := 0
				last := uint64(0)
				for {
					result, err := vm.Exec(arg, proc)
					require.NoError(b, err)
					if result.Status == vm.ExecYield {
						continue
					}
					if result.Batch == nil {
						require.Equal(b, vm.ExecStop, result.Status)
						break
					}
					rowsSeen += result.Batch.RowCount()
					last = vector.GetFixedAtNoTypeCheck[uint64](result.Batch.Vecs[shape.cols], result.Batch.RowCount()-1)
					if arg.ctr.bat != nil {
						var backing uint64
						for _, v := range arg.ctr.bat.Vecs {
							backing += uint64(v.Allocated())
						}
						peakInput = max(peakInput, backing)
					}
				}
				require.Equal(b, shape.rows, rowsSeen)
				require.Equal(b, uint64(shape.rows), last)
				arg.Reset(proc, false, nil)
				arg.Free(proc, false, nil)
				require.NoError(b, arg.ClearAllocationAccount(account))
				require.Zero(b, account.Snapshot().Used)
				measured, ok := mp.EndResourcePeakEpoch(token)
				require.True(b, ok)
				peakPool = max(peakPool, measured)
			}
			b.StopTimer()
			b.ReportMetric(float64(account.Snapshot().Peak), "peak-window-account-B")
			b.ReportMetric(float64(sourceBacking), "source-vector-B")
			b.ReportMetric(float64(sourceBacking+account.Snapshot().Peak), "peak-vector-backing-B")
			b.ReportMetric(float64(peakInput), "owned-input-B")
			b.ReportMetric(float64(peakPool), "peak-mpool-B")
			terminal, _, err := registry.CompleteTerminal(account)
			require.NoError(b, err)
			require.Zero(b, terminal.LiveAllocations)
		})
	}
}
