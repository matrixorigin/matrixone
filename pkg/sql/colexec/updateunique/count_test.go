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

package updateunique

import (
	"testing"

	"github.com/matrixorigin/matrixone/pkg/common/moerr"
	"github.com/matrixorigin/matrixone/pkg/container/batch"
	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/container/vector"
	"github.com/matrixorigin/matrixone/pkg/sql/colexec"
	"github.com/matrixorigin/matrixone/pkg/testutil"
	"github.com/stretchr/testify/require"
)

func TestCountPublishesOnceOnlyAfterSuccess(t *testing.T) {
	proc := testutil.NewProcess(t)
	input := batch.NewWithSize(1)
	input.Vecs[0] = vector.NewVec(types.T_uint64.ToType())
	for _, value := range []uint64{1, 2} {
		require.NoError(t, vector.AppendFixed(input.Vecs[0], value, false, proc.Mp()))
	}
	input.SetRowCount(2)
	child := &lateFailure{MockOperator: colexec.NewMockOperator().WithBatchs([]*batch.Batch{input}), err: moerr.NewTxnNeedRetry(proc.Ctx)}
	defer child.Free(proc, false, nil)
	var totals []uint64
	op := &Count{Column: 0, Publish: func(value uint64) { totals = append(totals, value) }}
	op.AppendChild(child)
	require.NoError(t, op.Prepare(proc))
	result, err := op.Call(proc)
	require.NoError(t, err)
	require.Same(t, input, result.Batch)
	require.Empty(t, totals)
	result, err = op.Call(proc)
	require.Nil(t, result.Batch)
	require.Same(t, child.err, err)
	require.Empty(t, totals, "a late error must not publish a partial count")

	op.Reset(proc, true, err)
	child.Reset(proc, true, err)
	input.Clean(proc.Mp())
	child.ResetBatchs()
	next := batch.NewWithSize(1)
	next.Vecs[0] = vector.NewVec(types.T_uint64.ToType())
	require.NoError(t, vector.AppendFixed(next.Vecs[0], uint64(3), false, proc.Mp()))
	next.SetRowCount(1)
	child.WithBatchs([]*batch.Batch{next})
	child.err = nil
	require.NoError(t, op.Prepare(proc))
	_, err = op.Call(proc)
	require.NoError(t, err)
	_, err = op.Call(proc)
	require.NoError(t, err)
	_, err = op.Call(proc)
	require.NoError(t, err)
	require.Equal(t, []uint64{3}, totals)
}
