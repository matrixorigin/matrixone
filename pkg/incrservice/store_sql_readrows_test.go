// Copyright 2024 Matrix Origin
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

package incrservice

import (
	"testing"

	"github.com/matrixorigin/matrixone/pkg/common/mpool"
	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/util/executor"
	"github.com/stretchr/testify/require"
)

func TestReadSingleOffsetStepReturnsSingleRow(t *testing.T) {
	result, mp := newOffsetStepResult(t, [][]uint64{{10}}, [][]uint64{{2}})
	defer func() {
		result.Close()
		require.Equal(t, int64(0), mp.CurrNB())
		mpool.DeleteMPool(mp)
	}()

	current, step, rows := readSingleOffsetStep(result)
	require.Equal(t, uint64(10), current)
	require.Equal(t, uint64(2), step)
	require.Equal(t, 1, rows)
}

func TestReadSingleOffsetStepCountsRowsAcrossBatches(t *testing.T) {
	result, mp := newOffsetStepResult(t, [][]uint64{{10}, {20}}, [][]uint64{{2}, {4}})
	defer func() {
		result.Close()
		require.Equal(t, int64(0), mp.CurrNB())
		mpool.DeleteMPool(mp)
	}()

	current, step, rows := readSingleOffsetStep(result)
	require.Equal(t, uint64(10), current)
	require.Equal(t, uint64(2), step)
	require.Equal(t, 2, rows)
}

func newOffsetStepResult(t *testing.T, offsetBatches, stepBatches [][]uint64) (executor.Result, *mpool.MPool) {
	t.Helper()
	require.Len(t, offsetBatches, len(stepBatches))

	mp := mpool.MustNewZero()
	memRes := executor.NewMemResult([]types.Type{types.T_uint64.ToType(), types.T_uint64.ToType()}, mp)
	for i := range offsetBatches {
		require.Len(t, offsetBatches[i], len(stepBatches[i]))
		memRes.NewBatchWithRowCount(len(offsetBatches[i]))
		require.NoError(t, executor.AppendFixedRows(memRes, 0, offsetBatches[i]))
		require.NoError(t, executor.AppendFixedRows(memRes, 1, stepBatches[i]))
	}
	return memRes.GetResult(), mp
}
