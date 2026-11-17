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

package hashjoin

import (
	"fmt"
	"testing"

	"github.com/matrixorigin/matrixone/pkg/container/batch"
	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/container/vector"
	"github.com/matrixorigin/matrixone/pkg/pb/plan"
	"github.com/matrixorigin/matrixone/pkg/sql/colexec"
	"github.com/matrixorigin/matrixone/pkg/vm"
	"github.com/stretchr/testify/require"
)

func BenchmarkHashJoinSmallDuplicatePayload(b *testing.B) {
	for _, duplicates := range []int{2, 4, 7} {
		b.Run(fmt.Sprintf("matches=%d", duplicates), func(b *testing.B) {
			typ := types.T_int64.ToType()
			tc := newTestCase(b, []types.Type{typ, typ}, nil, [][]*plan.Expr{{newExpr(0, typ)}, {newExpr(0, typ)}})
			tc.arg.JoinType, tc.arg.NonEqCond = plan.Node_INNER, nil
			tc.arg.LeftTypes = []types.Type{typ, typ}
			tc.arg.RightTypes = make([]types.Type, 8)
			for col := range tc.arg.RightTypes {
				tc.arg.RightTypes[col] = typ
			}
			tc.arg.ResultCols = []colexec.ResultPos{{Rel: 0, Pos: 1}}
			for col := 1; col < 8; col++ {
				tc.arg.ResultCols = append(tc.arg.ResultCols, colexec.ResultPos{Rel: 1, Pos: int32(col)})
			}
			probe, build := batch.NewWithSize(2), batch.NewWithSize(8)
			b.Cleanup(func() {
				tc.arg.Reset(tc.proc, false, nil)
				tc.barg.Reset(tc.proc, false, nil)
				tc.arg.Free(tc.proc, false, nil)
				tc.barg.Free(tc.proc, false, nil)
				probe.Clean(tc.proc.Mp())
				build.Clean(tc.proc.Mp())
				tc.proc.GetMessageBoard().Reset()
				tc.cancel()
				tc.proc.Free()
				require.Zero(b, tc.proc.Mp().CurrNB())
			})
			for col := range probe.Vecs {
				probe.Vecs[col] = vector.NewVec(typ)
			}
			for col := range build.Vecs {
				build.Vecs[col] = vector.NewVec(typ)
			}
			const groups = 512
			for row := range groups * duplicates {
				for col, v := range build.Vecs {
					value := int64(row + col)
					if col == 0 {
						value = int64(row / duplicates)
					}
					require.NoError(b, vector.AppendFixed(v, value, false, tc.proc.Mp()))
				}
			}
			build.SetRowCount(groups * duplicates)
			for row := range 1024 {
				require.NoError(b, vector.AppendFixed(probe.Vecs[0], int64(row%groups), false, tc.proc.Mp()))
				require.NoError(b, vector.AppendFixed(probe.Vecs[1], int64(row), false, tc.proc.Mp()))
			}
			probe.SetRowCount(1024)
			resetHashBuildChildrenWithBatch(tc.barg, build)
			resetChildrenWithBatch(tc.arg, probe)
			require.NoError(b, tc.barg.Prepare(tc.proc))
			require.NoError(b, tc.arg.Prepare(tc.proc))
			_, err := vm.Exec(tc.barg, tc.proc)
			require.NoError(b, err)
			require.NoError(b, tc.arg.build(tc.arg.OpAnalyzer, tc.proc))
			require.False(b, tc.arg.ctr.probeHashOnPK)
			run := func() {
				require.NoError(b, tc.arg.resetResultBat())
				ctr := &tc.arg.ctr
				ctr.leftBat = probe
				ctr.lastIdx = 0
				ctr.vsIdx = 0
				ctr.sels = nil
				ctr.probeState = psNextBatch
				result := vm.NewCallResult()
				if err := ctr.probe(tc.arg, tc.proc, &result); err != nil {
					b.Fatal(err)
				}
			}
			run()
			require.Equal(b, 1024*duplicates, tc.arg.ctr.resBat.RowCount())
			b.ReportAllocs()
			b.ResetTimer()
			for range b.N {
				run()
			}
		})
	}
}
