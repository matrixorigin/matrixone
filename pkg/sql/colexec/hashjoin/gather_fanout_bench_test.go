// Copyright 2021 Matrix Origin
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

package hashjoin

import (
	"fmt"
	"strings"
	"testing"

	"github.com/matrixorigin/matrixone/pkg/container/batch"
	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/container/vector"
	"github.com/matrixorigin/matrixone/pkg/pb/plan"
	"github.com/matrixorigin/matrixone/pkg/sql/colexec"
	"github.com/matrixorigin/matrixone/pkg/vm"
	"github.com/stretchr/testify/require"
)

// BenchmarkHashJoinDuplicateFanout probes keys with 128 build duplicates each,
// spread over four build batches, and projects three build payload columns:
// the many-source build gather of a duplicate-key inner join.
func BenchmarkHashJoinDuplicateFanout(b *testing.B) {
	const (
		buildBatches = 4
		buildRows    = 8192
		keys         = 256
		probeRows    = 64
	)
	for _, tc := range []struct {
		name string
		typ  types.Type
		val  func(row int) any
	}{
		{"int64", types.T_int64.ToType(), func(row int) any { return int64(row) }},
		{"decimal128", types.T_decimal128.ToType(), func(row int) any { return types.Decimal128{B0_63: uint64(row)} }},
		{"varchar-inline", types.T_varchar.ToType(), func(row int) any { return fmt.Sprintf("v%07d", row) }},
		{"varchar-long", types.T_varchar.ToType(), func(row int) any { return fmt.Sprintf("%s-%07d", strings.Repeat("p", 40), row) }},
	} {
		b.Run(tc.name, func(b *testing.B) {
			key := types.T_int64.ToType()
			tc2 := newTestCase(b, []types.Type{key, key}, nil, [][]*plan.Expr{{newExpr(0, key)}, {newExpr(0, key)}})
			tc2.arg.JoinType, tc2.arg.NonEqCond = plan.Node_INNER, nil
			tc2.arg.LeftTypes = []types.Type{key, key}
			tc2.arg.RightTypes = []types.Type{key, tc.typ, tc.typ, tc.typ}
			tc2.arg.ResultCols = []colexec.ResultPos{{Rel: 0, Pos: 1}, {Rel: 1, Pos: 1}, {Rel: 1, Pos: 2}, {Rel: 1, Pos: 3}}
			mp := tc2.proc.Mp()
			builds := make([]*batch.Batch, 0, buildBatches)
			for bi := range buildBatches {
				bat := batch.NewWithSize(4)
				bat.Vecs[0] = vector.NewVec(key, mp)
				for col := 1; col < 4; col++ {
					bat.Vecs[col] = vector.NewVec(tc.typ, mp)
				}
				for r := range buildRows {
					row := bi*buildRows + r
					require.NoError(b, vector.AppendFixed(bat.Vecs[0], int64(row%keys), false, mp))
					for col := 1; col < 4; col++ {
						switch v := tc.val(row + col).(type) {
						case string:
							require.NoError(b, vector.AppendBytes(bat.Vecs[col], []byte(v), false, mp))
						case int64:
							require.NoError(b, vector.AppendFixed(bat.Vecs[col], v, false, mp))
						case types.Decimal128:
							require.NoError(b, vector.AppendFixed(bat.Vecs[col], v, false, mp))
						}
					}
				}
				bat.SetRowCount(buildRows)
				builds = append(builds, bat)
			}
			probe := batch.NewWithSize(2)
			probe.Vecs[0], probe.Vecs[1] = vector.NewVec(key, mp), vector.NewVec(key, mp)
			for row := range probeRows {
				require.NoError(b, vector.AppendFixed(probe.Vecs[0], int64(row*3%keys), false, mp))
				require.NoError(b, vector.AppendFixed(probe.Vecs[1], int64(row), false, mp))
			}
			probe.SetRowCount(probeRows)
			b.Cleanup(func() {
				tc2.arg.Reset(tc2.proc, false, nil)
				tc2.barg.Reset(tc2.proc, false, nil)
				tc2.arg.Free(tc2.proc, false, nil)
				freeTestChildren(tc2.arg)
				tc2.barg.Free(tc2.proc, false, nil)
				freeTestChildren(tc2.barg)
				probe.Clean(mp)
				for _, bat := range builds {
					bat.Clean(mp)
				}
				tc2.proc.GetMessageBoard().Reset()
				tc2.cancel()
				tc2.proc.Free()
			})
			op := colexec.NewMockOperator().WithBatchs(builds)
			freeTestChildren(tc2.barg)
			tc2.barg.Children = nil
			tc2.barg.AppendChild(op)
			resetChildrenWithBatch(tc2.arg, probe)
			require.NoError(b, tc2.barg.Prepare(tc2.proc))
			require.NoError(b, tc2.arg.Prepare(tc2.proc))
			_, err := vm.Exec(tc2.barg, tc2.proc)
			require.NoError(b, err)
			require.NoError(b, tc2.arg.build(tc2.arg.OpAnalyzer, tc2.proc))
			require.Greater(b, len(tc2.arg.ctr.rightBats), 1)
			run := func() int {
				require.NoError(b, tc2.arg.resetResultBat(tc2.proc))
				ctr := &tc2.arg.ctr
				ctr.leftBat = probe
				ctr.lastIdx = 0
				ctr.vsIdx = 0
				ctr.sels = nil
				ctr.probeState = psNextBatch
				result := vm.NewCallResult()
				if err := ctr.probe(tc2.arg, tc2.proc, &result); err != nil {
					b.Fatal(err)
				}
				return ctr.resBat.RowCount()
			}
			rows := run()
			require.Greater(b, rows, 0)
			b.ReportAllocs()
			b.ResetTimer()
			for range b.N {
				run()
			}
			b.ReportMetric(float64(rows), "rows/op")
		})
	}
}
