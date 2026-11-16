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
	"context"
	"testing"

	"github.com/matrixorigin/matrixone/pkg/common/mpool"
	"github.com/matrixorigin/matrixone/pkg/container/vector"
	"github.com/matrixorigin/matrixone/pkg/pb/plan"
	"github.com/matrixorigin/matrixone/pkg/sql/colexec"
	"github.com/matrixorigin/matrixone/pkg/vm"
	"github.com/stretchr/testify/require"
)

func TestLoopJoinScalarBuild(t *testing.T) {
	for _, joinType := range []plan.Node_JoinType{plan.Node_INNER, plan.Node_LEFT} {
		t.Run(joinType.String(), func(t *testing.T) {
			join, proc, account := newLoopJoinCancellationFixture(t, joinType, [][]int32{{7}})
			join.NonEqCond = nil
			require.NoError(t, join.Prepare(proc))
			build := join.ctr.mp.GetBatches()[0]
			build.Vecs[0].GetNulls().Add(0)
			values := make([]int32, colexec.DefaultBatchSize+1)
			for row := range values {
				values[row] = int32(row)
			}
			input := makeInt32LoopJoinBatch(proc.Mp(), values)
			t.Cleanup(func() { input.Clean(proc.Mp()) })
			join.ctr.setProbeBatch(input)
			for _, start := range []int{0, colexec.DefaultBatchSize} {
				require.NoError(t, join.resetResultBat())
				result := vm.NewCallResult()
				require.NoError(t, join.ctr.probe(join, proc, &result))
				count := min(colexec.DefaultBatchSize, len(values)-start)
				require.Equal(t, count, result.Batch.RowCount())
				require.Equal(t, values[start:start+count], vector.MustFixedColNoTypeCheck[int32](result.Batch.Vecs[0]))
				for row := range count {
					require.True(t, result.Batch.Vecs[1].IsNull(uint64(row)))
				}
				require.Nil(t, join.ctr.joinBat, "unconditional scalar build needs no condition headers")
			}
			require.Nil(t, join.ctr.inBat)
			require.Nil(t, join.ctr.probeCopy)
			join.Reset(proc, false, nil)
			require.Zero(t, account.Snapshot().Used)
		})
	}
}

func TestLoopJoinScalarBuildDoesNotPublishOnFailure(t *testing.T) {
	for _, cancel := range []bool{false, true} {
		name := "capacity"
		if cancel {
			name = "cancel"
		}
		t.Run(name, func(t *testing.T) {
			join, proc, _ := newLoopJoinCancellationFixture(t, plan.Node_LEFT, [][]int32{{7}})
			join.NonEqCond = nil
			require.NoError(t, join.Prepare(proc))
			input := makeInt32LoopJoinBatch(proc.Mp(), []int32{1, 2})
			t.Cleanup(func() { input.Clean(proc.Mp()) })
			join.ctr.setProbeBatch(input)
			require.NoError(t, join.resetResultBat())
			if !cancel {
				registry, err := mpool.NewAllocationAccountRegistry(1, 16)
				require.NoError(t, err)
				account, err := registry.Open(1)
				require.NoError(t, err)
				selection, err := vector.NewAllocationAccountSelection(account,
					mpool.AllocationOwnerHashBuild, loopJoinAllocationSiteResultData,
					loopJoinAllocationSiteResultArea, loopJoinAllocationSiteResultNulls,
					loopJoinAllocationSiteResultGrouping)
				require.NoError(t, err)
				// Refuse the second column after the first column has copied.
				require.NoError(t, join.ctr.resBat.Vecs[1].SetAllocationAccount(selection))
				t.Cleanup(func() { join.Reset(proc, true, nil); require.Zero(t, account.Snapshot().Used) })
			} else {
				ctx, stop := context.WithCancel(proc.Ctx)
				t.Cleanup(stop)
				proc.Ctx = ctx
				stop()
			}
			result := vm.NewCallResult()
			err := join.ctr.probe(join, proc, &result)
			if cancel {
				require.ErrorIs(t, err, context.Canceled)
			} else {
				require.ErrorIs(t, err, mpool.ErrAllocationAccountCapacity)
			}
			require.Nil(t, result.Batch)
			require.Zero(t, join.ctr.probeIdx)
			require.Zero(t, join.ctr.resBat.RowCount())
			require.Nil(t, join.ctr.probeCopy)
		})
	}
}
