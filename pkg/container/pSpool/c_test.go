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

package pSpool

import (
	"context"
	"github.com/matrixorigin/matrixone/pkg/container/batch"
	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/container/vector"
	"github.com/matrixorigin/matrixone/pkg/testutil"
	"github.com/stretchr/testify/require"
	"testing"
	"time"
)

// TestCacheBatchBehavior tests the behavior of the cachedBatch.
//
// 1. GetCopiedBatch: generate a copied batch, or block until there is enough message space.
// 2. CacheBatch: put the byte slices of batch's vectors into the cache.
// 3. Free: if the cachedBatch is not in use, free the cached byte slices.
func TestCacheBatchBehavior_Get_Cache_Free(t *testing.T) {
	proc := testutil.NewProcess()
	cache := initCachedBatch(proc.Mp(), 1)

	originBatch := batch.NewWithSize(1)
	originBatch.Vecs[0] = testutil.NewInt64Vector(1, types.T_int64.ToType(), proc.Mp(), false, []int64{1})
	originBatch.SetRowCount(1)

	// 1. get first batch.
	b1, done, err := cache.GetCopiedBatch(proc.Ctx, originBatch)
	require.NoError(t, err)
	require.False(t, done)
	{
		require.NotNil(t, b1)
		require.NotEqual(t, originBatch, b1)
		require.Equal(t, 1, b1.RowCount())
		require.Equal(t, 1, len(b1.Vecs))
		vs := vector.MustFixedCol[int64](b1.Vecs[0])
		require.Equal(t, 1, len(vs))
		require.Equal(t, int64(1), vs[0])
	}

	// 2. block until the first batch is consumed or context is done.
	deadlineCtx, cancel := context.WithTimeout(context.TODO(), 100*time.Millisecond)
	b2, done, err := cache.GetCopiedBatch(deadlineCtx, originBatch)
	require.NoError(t, err)
	require.True(t, done)
	require.Nil(t, b2)
	cancel()

	// 3. cache the first batch.
	cache.CacheBatch(b1)
	require.True(t, len(cache.bytesCache) > 0)

	// 4. get the second batch.
	b3, done, err := cache.GetCopiedBatch(proc.Ctx, originBatch)
	require.NoError(t, err)
	require.False(t, done)
	{
		require.NotNil(t, b3)
		require.NotEqual(t, originBatch, b3)
		require.Equal(t, 1, b3.RowCount())
		require.Equal(t, 1, len(b3.Vecs))
		vs := vector.MustFixedCol[int64](b3.Vecs[0])
		require.Equal(t, 1, len(vs))
	}
	require.True(t, len(cache.bytesCache) == 0)

	// 5. cache the second batch.
	cache.CacheBatch(b3)

	// 6. do free.
	originBatch.Clean(proc.Mp())
	cache.Free()
	require.Equal(t, int64(0), proc.Mp().CurrNB())
}
