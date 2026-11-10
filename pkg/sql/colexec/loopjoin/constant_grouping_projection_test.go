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

package loopjoin

import (
	"testing"

	"github.com/matrixorigin/matrixone/pkg/common/mpool"
	"github.com/matrixorigin/matrixone/pkg/container/batch"
	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/container/vector"
	"github.com/matrixorigin/matrixone/pkg/pb/plan"
	"github.com/matrixorigin/matrixone/pkg/sql/colexec"
	"github.com/matrixorigin/matrixone/pkg/testutil"
	"github.com/matrixorigin/matrixone/pkg/vm"
	"github.com/matrixorigin/matrixone/pkg/vm/message"
	"github.com/stretchr/testify/require"
)

func TestLoopJoinConstantGroupingProjection(t *testing.T) {
	for _, method := range []string{"scalar-control", "range", "operator-resume"} {
		t.Run(method, func(t *testing.T) {
			proc := testutil.NewProcessWithOwnedMPool(t, "", mpool.MustNewZero())
			typ := types.T_varchar.ToType()
			makeSource := func() *batch.Batch {
				bat := batch.NewWithSize(1)
				var err error
				bat.Vecs[0], err = vector.NewConstBytes(typ, []byte("constant payload"), 5, proc.Mp())
				require.NoError(t, err)
				bat.Vecs[0].GetGrouping().Add(0, 2)
				bat.Vecs[0].SetPrepareParamKind(vector.PrepareParamInteger)
				bat.Vecs[0].SetIsBinaryString(true)
				require.NoError(t, bat.Vecs[0].SetStringSource(types.StringSourceLiteral))
				bat.SetRowCount(5)
				return bat
			}
			left, right := makeSource(), makeSource()
			join := &LoopJoin{
				ResultCols: []colexec.ResultPos{{Rel: 0, Pos: 0}, {Rel: 1, Pos: 0}},
				LeftTypes:  []types.Type{typ}, RightTypes: []types.Type{typ},
			}
			installLoopJoinTestAllocation(t, join)
			defer func() {
				join.Free(proc, false, nil)
				left.Clean(proc.Mp())
				right.Clean(proc.Mp())
				proc.Free()
				require.Zero(t, proc.Mp().CurrNB())
			}()
			if method == "operator-resume" {
				join.JoinType = plan.Node_INNER
				join.ctr.resultBatchByteLimit = 144
				join.ctr.state = Probe
				join.ctr.mp = message.NewJoinMap(message.GroupSels{}, nil, nil, nil, []*batch.Batch{right}, proc.Mp())
				join.ctr.mp.IncRef(1)
				join.AppendChild(colexec.NewMockOperator().WithBatchs([]*batch.Batch{left}))
				require.NoError(t, join.Prepare(proc))
				seen, batches := 0, 0
				for {
					result, err := vm.Exec(join, proc)
					require.NoError(t, err)
					if result.Batch != nil {
						batches++
						for row := range result.Batch.RowCount() {
							for side, v := range result.Batch.Vecs {
								sourceRow := seen / 5
								if side == 1 {
									sourceRow = seen % 5
								}
								require.Equal(t, sourceRow == 0 || sourceRow == 2, v.GetGrouping().Contains(uint64(row)), "side %d output row %d", side, seen)
								require.False(t, v.IsNull(uint64(row)))
								require.Equal(t, "constant payload", v.GetStringAt(row))
								require.Equal(t, types.StringSourceLiteral, v.GetStringSourceAt(row))
								require.Equal(t, vector.PrepareParamInteger, v.GetPrepareParamKindAt(row))
								require.True(t, v.GetIsBinaryStringAt(row))
							}
							seen++
						}
					}
					if result.Status == vm.ExecStop {
						break
					}
				}
				require.Equal(t, 25, seen)
				require.Greater(t, batches, 1)
				return
			}
			require.NoError(t, join.resetResultBat())
			if method == "range" {
				require.NoError(t, join.ctr.appendJoinedRange(join, proc, left, 2, right, 1, 4))
			} else {
				for row := 1; row < 5; row++ {
					require.NoError(t, join.ctr.appendJoinedRow(join, proc, left, 2, right, row))
				}
			}
			for side, v := range join.ctr.resBat.Vecs {
				for row := range 4 {
					require.Equal(t, side == 0 || row == 1, v.GetGrouping().Contains(uint64(row)), "side %d row %d", side, row)
					require.False(t, v.IsNull(uint64(row)))
					require.Equal(t, "constant payload", v.GetStringAt(row))
					require.Equal(t, types.StringSourceLiteral, v.GetStringSourceAt(row))
					require.Equal(t, vector.PrepareParamInteger, v.GetPrepareParamKindAt(row))
					require.True(t, v.GetIsBinaryStringAt(row))
				}
			}
		})
	}
}
