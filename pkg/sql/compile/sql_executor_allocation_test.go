// Copyright 2026 Matrix Origin
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

package compile

import (
	"context"
	"testing"

	"github.com/matrixorigin/matrixone/pkg/common/mpool"
	"github.com/matrixorigin/matrixone/pkg/container/batch"
	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/container/vector"
	"github.com/matrixorigin/matrixone/pkg/util/executor"
	"github.com/stretchr/testify/require"
)

func TestInternalExecutorResultEndsStatementAllocationOwnership(t *testing.T) {
	for _, streaming := range []bool{false, true} {
		t.Run(map[bool]string{false: "retained", true: "streaming"}[streaming], func(t *testing.T) {
			mp := mpool.MustNewZero()
			registry, err := mpool.NewAllocationAccountRegistry(1, 8)
			require.NoError(t, err)
			account, err := registry.Open(1 << 20)
			require.NoError(t, err)
			selection, err := vector.NewAllocationAccountSelection(
				account,
				1,
				1,
				2,
				3,
				4,
			)
			require.NoError(t, err)

			source := batch.NewWithSchema(
				true,
				[]string{"id", "value"},
				[]types.Type{types.T_int64.ToType(), types.T_varchar.ToType()},
			)
			require.NoError(t, source.SetAllocationAccount(selection))
			require.NoError(t, vector.AppendFixed(source.Vecs[0], int64(42), false, mp))
			require.NoError(t, vector.AppendBytes(source.Vecs[1], []byte("result"), false, mp))
			source.SetRowCount(1)

			cloned, err := cloneInternalExecutorResultBatch(source, mp, streaming)
			require.NoError(t, err)
			require.Nil(t, cloned.AllocationAccountSelection())
			for _, vec := range cloned.Vecs {
				require.Nil(t, vec.AllocationAccountSelection())
			}

			var result executor.Result
			if streaming {
				results := make(chan executor.Result, 1)
				published := executor.NewResult(mp)
				published.Batches = []*batch.Batch{cloned}
				results <- published
				result = <-results
			} else {
				result = executor.NewResult(mp)
				result.Batches = []*batch.Batch{cloned}
			}

			source.Clean(mp)
			snapshot := account.Seal()
			require.Zero(t, snapshot.Used)
			require.Zero(t, registry.LiveAllocationMetadata())
			_, err = registry.Finalize(account)
			require.NoError(t, err)

			require.Equal(
				t,
				int64(42),
				vector.GetFixedAtNoTypeCheck[int64](result.Batches[0].Vecs[0], 0),
			)
			require.Equal(t, []byte("result"), result.Batches[0].Vecs[1].GetBytesAt(0))
			result.Close()
			require.Zero(t, mp.CurrNB())
		})
	}
}

func TestInternalExecutorStreamCancellationCleansUnpublishedResult(t *testing.T) {
	mp := mpool.MustNewZero()
	registry, err := mpool.NewAllocationAccountRegistry(1, 8)
	require.NoError(t, err)
	account, err := registry.Open(1 << 20)
	require.NoError(t, err)
	selection, err := vector.NewAllocationAccountSelection(account, 1, 1, 2, 3, 4)
	require.NoError(t, err)

	source := batch.NewWithSchema(
		true,
		[]string{"value"},
		[]types.Type{types.T_int64.ToType()},
	)
	require.NoError(t, source.SetAllocationAccount(selection))
	require.NoError(t, vector.AppendFixed(source.Vecs[0], int64(42), false, mp))
	source.SetRowCount(1)

	cloned, err := cloneInternalExecutorResultBatch(source, mp, true)
	require.NoError(t, err)
	result := executor.NewResult(mp)
	result.Batches = []*batch.Batch{cloned}
	results := make(chan executor.Result, 1)
	results <- executor.NewResult(mp)
	procCtx, cancel := context.WithCancel(context.Background())
	cancel()

	err = publishInternalExecutorStreamResult(
		procCtx,
		context.Background(),
		results,
		result,
	)
	require.Error(t, err)
	require.Len(t, results, 1)

	source.Clean(mp)
	snapshot := account.Seal()
	require.Zero(t, snapshot.Used)
	require.Zero(t, registry.LiveAllocationMetadata())
	_, err = registry.Finalize(account)
	require.NoError(t, err)
	require.Zero(t, mp.CurrNB())
}
