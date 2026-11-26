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

package pSpool

import (
	"bytes"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/matrixorigin/matrixone/pkg/common/mpool"
	"github.com/matrixorigin/matrixone/pkg/container/batch"
	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/container/vector"
)

func TestCachedBatchStableShapeRetention(t *testing.T) {
	const rows = 64
	mp := mpool.MustNewZeroNoFixed()
	t.Cleanup(func() {
		defer mpool.DeleteMPool(mp)
		require.Zero(t, mp.CurrNB())
	})
	source := newWideSpoolSource(t, mp, rows)
	sourceBytes := mp.CurrNB()
	cache := newBatchCache(mp, 1)
	t.Cleanup(cache.free)

	retained := make([]int64, 0, 6)
	for iteration := range 6 {
		copied, _, aliased, err := cache.copyBatch(source)
		require.NoError(t, err)
		func() {
			defer cache.put(copied, aliased)
			require.Equal(t, rows, copied.RowCount())
			require.Equal(t, vector.MustFixedColNoTypeCheck[int64](source.Vecs[0]),
				vector.MustFixedColNoTypeCheck[int64](copied.Vecs[0]))
			for column := 1; column < len(source.Vecs); column++ {
				for row := range rows {
					require.Equal(t, source.Vecs[column].GetBytesAt(row), copied.Vecs[column].GetBytesAt(row))
				}
			}
		}()
		retained = append(retained, mp.CurrNB()-sourceBytes)
		t.Logf("iteration=%d retained=%d", iteration+1, retained[iteration])
	}
	for _, size := range retained[1:] {
		require.Equal(t, retained[0], size, "identical batch shapes must reuse their warmed backing capacity")
	}
}

func TestCopyBatchMarkerBatchesAreNotCopied(t *testing.T) {
	mp := mpool.MustNewZeroNoFixed()
	t.Cleanup(func() { mpool.DeleteMPool(mp) })
	for _, src := range []*batch.Batch{nil, batch.EmptyBatch, batch.CteEndBatch} {
		dst, owned, _, err := newBatchCache(mp, 1).copyBatch(src)
		require.NoError(t, err)
		require.False(t, owned)
		require.Same(t, src, dst)
	}
	require.Zero(t, mp.CurrNB())
}

// copyBatch must produce an independent, mp-owned batch that preserves every
// column shape: nil, flat fixed, flat varlen with nulls, const, and a vector
// shared by several columns.
func TestCopyBatchCopiesMixedShapes(t *testing.T) {
	const rows = 3
	sourceMP := mpool.MustNewZeroNoFixed()
	t.Cleanup(func() { defer mpool.DeleteMPool(sourceMP); require.Zero(t, sourceMP.CurrNB()) })

	source := batch.NewWithSize(5)
	fixed := vector.NewVec(types.T_int64.ToType(), sourceMP)
	require.NoError(t, vector.AppendFixedList(fixed, []int64{1, 2, 3}, nil, sourceMP))
	fixed.SetSorted(true)
	varlen := vector.NewVec(types.T_text.ToType(), sourceMP)
	long := bytes.Repeat([]byte{'x'}, 64)
	require.NoError(t, vector.AppendBytes(varlen, long, false, sourceMP))
	require.NoError(t, vector.AppendBytes(varlen, nil, true, sourceMP))
	require.NoError(t, vector.AppendBytes(varlen, []byte("short"), false, sourceMP))
	constant, err := vector.NewConstBytes(types.T_text.ToType(), []byte("constant"), rows, sourceMP)
	require.NoError(t, err)
	source.Vecs[0], source.Vecs[2], source.Vecs[3], source.Vecs[4] = fixed, varlen, constant, varlen
	source.SetRowCount(rows)
	source.Recursive = 1
	source.ShuffleIDX = 7
	t.Cleanup(func() { source.Clean(sourceMP) })

	mp := mpool.MustNewZeroNoFixed()
	t.Cleanup(func() { defer mpool.DeleteMPool(mp); require.Zero(t, mp.CurrNB()) })
	cache := newBatchCache(mp, 1)
	t.Cleanup(cache.free)
	copied, owned, aliased, err := cache.copyBatch(source)
	require.NoError(t, err)
	require.True(t, owned)
	require.True(t, aliased)

	require.Equal(t, rows, copied.RowCount())
	require.Equal(t, source.Recursive, copied.Recursive)
	require.Equal(t, source.ShuffleIDX, copied.ShuffleIDX)
	require.Nil(t, copied.Vecs[1])
	require.Same(t, copied.Vecs[2], copied.Vecs[4], "a shared source vector is copied once")
	for i, vec := range copied.Vecs {
		if vec == nil {
			continue
		}
		require.NotSame(t, source.Vecs[i], vec)
		require.Same(t, mp, vec.MPool())
	}

	require.Equal(t, []int64{1, 2, 3}, vector.MustFixedColNoTypeCheck[int64](copied.Vecs[0]))
	require.True(t, copied.Vecs[0].GetSorted())
	require.Equal(t, long, copied.Vecs[2].GetBytesAt(0))
	require.True(t, copied.Vecs[2].IsNull(1))
	require.Equal(t, []byte("short"), copied.Vecs[2].GetBytesAt(2))
	require.True(t, copied.Vecs[3].IsConst())
	require.Equal(t, rows, copied.Vecs[3].Length())
	require.Equal(t, []byte("constant"), copied.Vecs[3].GetBytesAt(0))

	// The copy is independent of its source.
	source.Clean(sourceMP)
	require.Equal(t, long, copied.Vecs[2].GetBytesAt(0))
	cache.put(copied, aliased)
}

// A failing copy returns what it already copied to the cache and leaves the
// source intact.
func TestCopyBatchFailureReleasesPartialCopy(t *testing.T) {
	sourceMP := mpool.MustNewZeroNoFixed()
	t.Cleanup(func() { defer mpool.DeleteMPool(sourceMP); require.Zero(t, sourceMP.CurrNB()) })
	source := batch.NewWithSize(2)
	for i, size := range []int{64, 2 << 20} {
		source.Vecs[i] = vector.NewVec(types.T_text.ToType(), sourceMP)
		require.NoError(t, vector.AppendBytes(source.Vecs[i], bytes.Repeat([]byte{byte(i + 1)}, size), false, sourceMP))
	}
	source.SetRowCount(1)
	t.Cleanup(func() { source.Clean(sourceMP) })

	mp, err := mpool.NewMPool(t.Name(), 1<<20, mpool.NoFixed)
	require.NoError(t, err)
	t.Cleanup(func() { mpool.DeleteMPool(mp) })
	cache := newBatchCache(mp, 1)
	_, _, _, err = cache.copyBatch(source)
	require.Error(t, err)
	require.Empty(t, cache.bats, "a failed copy frees its batch")
	require.Zero(t, mp.CurrNB())
	cache.free()
	require.Equal(t, bytes.Repeat([]byte{1}, 64), source.Vecs[0].GetBytesAt(0))
}

func TestCopyBatchCopiesAllNullAny(t *testing.T) {
	mp := mpool.MustNewZeroNoFixed()
	t.Cleanup(func() {
		defer mpool.DeleteMPool(mp)
		require.Zero(t, mp.CurrNB())
	})
	source := batch.NewWithSize(1)
	t.Cleanup(func() { source.Clean(mp) })
	source.Vecs[0] = vector.NewVec(types.T_any.ToType(), mp)
	for range 3 {
		require.NoError(t, vector.AppendNull(source.Vecs[0], mp))
	}
	source.SetRowCount(3)
	cache := newBatchCache(mp, 1)
	t.Cleanup(cache.free)
	for range 2 {
		copied, owned, aliased, err := cache.copyBatch(source)
		require.NoError(t, err)
		require.True(t, owned)
		require.Equal(t, 3, copied.RowCount())
		require.Equal(t, types.T_any.ToType(), *copied.Vecs[0].GetType())
		require.True(t, copied.Vecs[0].AllNull())
		cache.put(copied, aliased)
	}
}

func newWideSpoolSource(tb testing.TB, mp *mpool.MPool, rows int) *batch.Batch {
	tb.Helper()
	source := batch.NewWithSize(3)
	tb.Cleanup(func() { source.Clean(mp) })
	source.Vecs[0] = vector.NewVec(types.T_int64.ToType(), mp)
	require.NoError(tb, vector.AppendFixedList(source.Vecs[0], make([]int64, rows), nil, mp))
	for i, size := range []int{3072, 16384} {
		typ := types.T_text.ToType()
		if i == 0 {
			typ = types.T_array_float32.ToType()
			typ.Width = 768
		}
		vec := vector.NewVec(typ, mp)
		source.Vecs[i+1] = vec
		require.NoError(tb, vec.PreExtendWithArea(rows, rows*size, mp))
		payload := bytes.Repeat([]byte{byte(i + 1)}, size)
		for range rows {
			require.NoError(tb, vector.AppendBytes(vec, payload, false, mp))
		}
	}
	source.SetRowCount(rows)
	return source
}
