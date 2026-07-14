// Copyright 2025 Matrix Origin
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

package function

import (
	"testing"

	"github.com/matrixorigin/matrixone/pkg/common/mpool"
	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/container/vector"
	"github.com/matrixorigin/matrixone/pkg/util/executor"
	"github.com/stretchr/testify/require"
)

func TestReadSingleResultRowAcceptsSingleRow(t *testing.T) {
	result, mp := newFeatureSingleRowResult(t, [][]string{{"feature-a"}})
	defer func() {
		result.Close()
		require.Equal(t, int64(0), mp.CurrNB())
		mpool.DeleteMPool(mp)
	}()

	var got string
	found, err := readSingleResultRow(result, func(cols []*vector.Vector) {
		got = string(cols[0].GetBytesAt(0))
	})
	require.NoError(t, err)
	require.True(t, found)
	require.Equal(t, "feature-a", got)
}

func TestReadSingleResultRowCountsRowsAcrossBatches(t *testing.T) {
	result, mp := newFeatureSingleRowResult(t, [][]string{{"feature-a"}, {"feature-b"}})
	defer func() {
		result.Close()
		require.Equal(t, int64(0), mp.CurrNB())
		mpool.DeleteMPool(mp)
	}()

	found, err := readSingleResultRow(result, func(cols []*vector.Vector) {})
	require.Error(t, err)
	require.True(t, found)
	require.Contains(t, err.Error(), "unexpected rows count: 2")
}

func newFeatureSingleRowResult(t *testing.T, batches [][]string) (executor.Result, *mpool.MPool) {
	t.Helper()

	mp := mpool.MustNewZero()
	memRes := executor.NewMemResult([]types.Type{types.T_varchar.ToType()}, mp)
	for _, batch := range batches {
		memRes.NewBatchWithRowCount(len(batch))
		require.NoError(t, executor.AppendStringRows(memRes, 0, batch))
	}
	return memRes.GetResult(), mp
}
