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

func TestLoopJoinConstantProjection(t *testing.T) {
	for _, method := range []string{"scalar-control", "range", "operator-resume", "scalar-build-resume"} {
		t.Run(method, func(t *testing.T) {
			proc := testutil.NewProcessWithOwnedMPool(t, "", mpool.MustNewZero())
			typ := types.T_varchar.ToType()
			makeSource := func() *batch.Batch {
				bat := batch.NewWithSize(1)
				var err error
				bat.Vecs[0], err = vector.NewConstBytes(typ, []byte("constant payload"), 5, proc.Mp())
				require.NoError(t, err)
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
				freeTestChildren(join)
				left.Clean(proc.Mp())
				right.Clean(proc.Mp())
				proc.Free()
				require.Zero(t, proc.Mp().CurrNB())
			}()
			if method == "operator-resume" || method == "scalar-build-resume" {
				buildRows := 5
				if method == "scalar-build-resume" {
					buildRows = 1
					right.SetRowCount(1)
					right.Vecs[0].SetLength(1)
				}
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
							for _, v := range result.Batch.Vecs {
								require.False(t, v.IsNull(uint64(row)))
								require.Equal(t, "constant payload", v.GetStringAt(row))
							}
							seen++
						}
					}
					if result.Status == vm.ExecStop {
						break
					}
				}
				require.Equal(t, 5*buildRows, seen)
				require.Greater(t, batches, 1)
				require.Nil(t, join.ctr.probeCopy)
				require.Nil(t, join.ctr.buildCopy)
				require.Nil(t, join.ctr.buildCopyBatch)
				return
			}
			require.NoError(t, join.resetResultBat(proc))
			if method == "range" {
				join.ctr.setProbeBatch(left)
				require.NoError(t, join.ctr.appendJoinedRange(join, proc, left, 2, right, 1, 4))
			} else {
				for row := 1; row < 5; row++ {
					require.NoError(t, join.ctr.appendJoinedRow(join, proc, left, 2, right, row))
				}
			}
			for _, v := range join.ctr.resBat.Vecs {
				for row := range 4 {
					require.False(t, v.IsNull(uint64(row)))
					require.Equal(t, "constant payload", v.GetStringAt(row))
				}
			}
		})
	}
}

func TestLoopJoinCopyBindingNewGeneration(t *testing.T) {
	proc := testutil.NewProcessWithOwnedMPool(t, "", mpool.MustNewZero())
	t.Cleanup(func() { proc.Free(); require.Zero(t, proc.Mp().CurrNB()) })
	join := &LoopJoin{
		ResultCols: []colexec.ResultPos{{Rel: 0, Pos: 0}, {Rel: 1, Pos: 0}},
		LeftTypes:  []types.Type{types.T_int32.ToType()}, RightTypes: []types.Type{types.T_int32.ToType()},
	}
	account := installLoopJoinTestAllocation(t, join)
	t.Cleanup(func() { join.Free(proc, false, nil); require.Zero(t, account.Snapshot().Used) })
	flat := makeInt32LoopJoinBatch(proc.Mp(), []int32{11, 22, 33})
	t.Cleanup(func() { flat.Clean(proc.Mp()) })
	other := makeInt32LoopJoinBatch(proc.Mp(), []int32{33, 11, 22})
	t.Cleanup(func() { other.Clean(proc.Mp()) })
	// A reused batch header may present a different vector on the next
	// input. Immutable build batches may also change during a probe.
	input := batch.NewWithSize(1)
	input.SetRowCount(3)
	selectedBuild := other
	for _, source := range []*vector.Vector{flat.Vecs[0], other.Vecs[0], flat.Vecs[0]} {
		input.Vecs[0] = source
		join.ctr.setProbeBatch(input)
		for _, build := range []*batch.Batch{flat, selectedBuild, flat} {
			require.NoError(t, join.resetResultBat(proc))
			require.NoError(t, join.ctr.appendJoinedRange(join, proc, input, 0, build, 0, 3))
			for row := range 3 {
				require.Equal(t, vector.GetFixedAtNoTypeCheck[int32](source, 0), vector.GetFixedAtNoTypeCheck[int32](join.ctr.resBat.Vecs[0], row))
				require.Equal(t, vector.GetFixedAtNoTypeCheck[int32](build.Vecs[0], row), vector.GetFixedAtNoTypeCheck[int32](join.ctr.resBat.Vecs[1], row))
			}
		}
	}
	join.Reset(proc, false, nil)
	require.Nil(t, join.ctr.probeCopy)
	require.Nil(t, join.ctr.buildCopy)
	require.Nil(t, join.ctr.buildCopyBatch)
	require.Zero(t, account.Snapshot().Used)
	join.ctr.setProbeBatch(input)
	join.Free(proc, false, nil)
	freeTestChildren(join)
	require.Nil(t, join.ctr.probeCopy)
	require.Nil(t, join.ctr.buildCopyBatch)
}
