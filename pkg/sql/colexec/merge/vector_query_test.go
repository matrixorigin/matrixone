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

package merge_test

import (
	"testing"

	"github.com/matrixorigin/matrixone/pkg/container/batch"
	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/container/vector"
	"github.com/matrixorigin/matrixone/pkg/sql/colexec/merge"
	"github.com/matrixorigin/matrixone/pkg/sql/internal/materialized"
	"github.com/matrixorigin/matrixone/pkg/testutil"
	"github.com/matrixorigin/matrixone/pkg/vm"
	"github.com/matrixorigin/matrixone/pkg/vm/process"
	"github.com/stretchr/testify/require"
)

func TestScalarReceiverActivation(t *testing.T) {
	proc := testutil.NewProcess(t)
	proc.Reg.MergeReceivers = []*process.WaitRegister{process.NewPipelineEdge(2, 1), process.NewPipelineEdge(2, 1)}
	op := merge.NewArgument().WithPartial(0, 0)
	defer op.Release()
	require.ErrorContains(t, op.ActivateReceiverRange(proc, 0, 1), "still active")
	require.NoError(t, op.Prepare(proc))
	for _, r := range [][2]int32{{-1, 1}, {0, 0}, {1, 3}} {
		require.Error(t, op.ActivateReceiverRange(proc, r[0], r[1]))
	}
	require.NoError(t, op.ActivateReceiverRange(proc, 0, 1))
	require.ErrorContains(t, op.ActivateReceiverRange(proc, 1, 2), "still active")
	proc.Reg.MergeReceivers[0].Ch2 <- process.NewEndSignal()
	result, err := op.Call(proc)
	require.NoError(t, err)
	require.Equal(t, vm.ExecStop, result.Status)
	require.NoError(t, op.ActivateReceiverRange(proc, 1, 2))
	proc.Reg.MergeReceivers[1].Ch2 <- process.NewEndSignal()
	_, err = op.Call(proc)
	require.NoError(t, err)
	op.Reset(proc, false, nil)
	op.Partial = false
	require.Error(t, op.ActivateReceiverRange(proc, 0, 1))
}

func TestScalarMaterializedBatchOwnership(t *testing.T) {
	for _, twoBatches := range []bool{false, true} {
		t.Run(map[bool]string{false: "one_batch", true: "proof_violation"}[twoBatches], func(t *testing.T) {
			proc := testutil.NewProcess(t)
			input := batch.NewWithSize(1)
			input.Vecs[0] = vector.NewVec(types.T_int64.ToType())
			require.NoError(t, vector.AppendFixed(input.Vecs[0], int64(42), false, proc.Mp()))
			input.SetRowCount(1)
			before := proc.Mp().CurrNB()
			source := materialized.NewSource(1)
			require.NoError(t, source.Begin(proc.Mp()))
			require.NoError(t, source.Append(input))
			if twoBatches {
				require.NoError(t, source.Append(input))
			}
			source.Finish(nil)
			op := merge.NewArgument().WithSinkScan(true)
			defer op.Release()
			op.MaterializedSource = source
			op.MaterializedSingleBatch = true
			require.NoError(t, op.Prepare(proc))
			first, err := op.Call(proc)
			require.NoError(t, err)
			require.Equal(t, int64(42), vector.GetFixedAtNoTypeCheck[int64](first.Batch.Vecs[0], 0))
			_, err = op.Call(proc)
			if twoBatches {
				require.ErrorContains(t, err, "one batch")
			} else {
				require.NoError(t, err)
			}
			// EOF 前后均允许同 pipeline 的 TVF 借用首批，但不能要求 TVF 负责释放。
			require.Equal(t, int64(42), vector.GetFixedAtNoTypeCheck[int64](first.Batch.Vecs[0], 0))
			op.Reset(proc, false, nil)
			op.Free(proc, false, nil)
			source.Close()
			require.Equal(t, before, proc.Mp().CurrNB())
			input.Clean(proc.Mp())
			require.Zero(t, proc.Mp().CurrNB())
		})
	}
}
