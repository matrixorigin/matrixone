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

package search

import (
	"context"
	"math"
	"testing"

	"github.com/matrixorigin/matrixone/pkg/common/mpool"
	"github.com/matrixorigin/matrixone/pkg/container/batch"
	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/container/vector"
	"github.com/matrixorigin/matrixone/pkg/testutil"
	"github.com/matrixorigin/matrixone/pkg/vectorindex"
	"github.com/stretchr/testify/require"
)

func TestReadScorePages(t *testing.T) {
	mp := mpool.MustNewZero()
	proc := testutil.NewProcessWithOwnedMPool(t, "", mp)
	pks, scores := make([]int64, 8193), make([]float32, 8193)
	for i := range pks {
		pks[i], scores[i] = int64(i), float32(i)+0.5
	}
	u := &reader{proc: proc, inited: true, started: true, pkVecIdx: 0, scoreVecIdx: 1,
		out: &vectorindex.SearchOutput{Keys: int64ColumnBuffer(pks...), Dists: scores}}
	t.Cleanup(func() { require.NoError(t, u.Close()) })
	out := pkScoreBatch()
	t.Cleanup(func() { out.Clean(mp) })
	for _, page := range []struct{ start, n int }{{0, 8192}, {8192, 1}} {
		end, err := u.Read(context.Background(), nil, nil, mp, out)
		require.NoError(t, err)
		require.False(t, end)
		require.Equal(t, page.n, out.RowCount())
		for i := 0; i < page.n; i++ {
			require.Equal(t, int64(page.start+i), vector.GetFixedAtWithTypeCheck[int64](out.Vecs[0], i))
			require.Equal(t, math.Float32bits(scores[page.start+i]), math.Float32bits(vector.GetFixedAtWithTypeCheck[float32](out.Vecs[1], i)))
		}
	}
	end, err := u.Read(context.Background(), nil, nil, mp, out)
	require.NoError(t, err)
	require.True(t, end)
	require.Zero(t, out.RowCount())
}

func TestAppendOutputRangeScoreOwnership(t *testing.T) {
	mp := mpool.MustNewZero()
	t.Cleanup(func() { mpool.DeleteMPool(mp) })
	// Both the >=16 bulk-copy path and a one-row nonzero-offset tail preserve
	// payload bits; these transport values need not pass the SQL MATCH > 0 filter.
	bits := []uint32{0, 0x80000000, 1, 0x7f7fffff, 0x7f800000, 0xff800000, 0x7fc12345, 0xffc12345}
	for _, page := range []struct{ start, n int }{{0, 16}, {16, 1}} {
		func() {
			scores := make([]float32, 17)
			for i := range scores {
				scores[i] = math.Float32frombits(bits[i%len(bits)])
			}
			out := batch.NewWithSize(1)
			out.Vecs[0] = vector.NewVec(types.T_float32.ToType())
			defer out.Clean(mp)
			u := &reader{pkVecIdx: -1, scoreVecIdx: 0}
			require.NoError(t, u.appendOutputRange(&vectorindex.SearchOutput{Dists: scores}, page.start, page.n, mp, out))
			for i := range scores {
				scores[i] = 42 // source storage can be immediately overwritten
			}
			for i := 0; i < page.n; i++ {
				require.False(t, out.Vecs[0].IsNull(uint64(i)))
				require.Equal(t, bits[(page.start+i)%len(bits)], math.Float32bits(vector.GetFixedAtWithTypeCheck[float32](out.Vecs[0], i)))
			}
		}()
	}
	// Empty/negative score ranges never slice source data or validate the mpool.
	out := batch.NewWithSize(1)
	out.Vecs[0] = vector.NewVec(types.T_float32.ToType())
	defer out.Clean(mp)
	u := &reader{pkVecIdx: -1, scoreVecIdx: 0}
	for _, n := range []int{0, -1} {
		require.NoError(t, u.appendOutputRange(nil, 100, n, nil, out))
		require.Zero(t, out.Vecs[0].Length())
	}
	// A pruned score column cannot read Dists or dereference an output score vector.
	u.scoreVecIdx = -1
	require.NoError(t, u.appendOutputRange(nil, 0, 16, nil, nil))
}

func TestReadScoreAppendFailure(t *testing.T) {
	// One MiB is the allocator's minimum configurable pool cap. Fill it so only
	// the off-heap score allocation fails; the ordinary PK vector appends first.
	mp, err := mpool.NewMPool("fulltext-score-failure", 1<<20, mpool.NoFixed)
	require.NoError(t, err)
	t.Cleanup(func() { mpool.DeleteMPool(mp) })
	out := pkScoreBatch()
	out.Vecs[1] = vector.NewOffHeapVecWithType(types.T_float32.ToType())
	var ballast []byte
	t.Cleanup(func() {
		out.Clean(mp)
		if ballast != nil {
			mp.Free(ballast)
		}
		require.Zero(t, mp.CurrNB())
		require.Zero(t, mp.OnHeapCurrNB())
	})
	ballast, err = mp.Alloc(int(mp.Cap()), true)
	require.NoError(t, err)
	u := streamingReader(testutil.NewProc(t))
	t.Cleanup(func() { require.NoError(t, u.Close()) })
	pks := make([]int64, 16)
	for i := range pks {
		pks[i] = int64(i)
	}
	u.streamCh <- &vectorindex.SearchOutput{Keys: int64ColumnBuffer(pks...), Dists: make([]float32, 16)}
	close(u.streamCh)
	u.errCh <- nil
	end, err := u.Read(context.Background(), nil, nil, mp, out)
	require.Error(t, err)
	require.False(t, end)
	require.Zero(t, out.RowCount(), "append error must not publish a batch")
	require.Equal(t, 16, out.Vecs[0].Length(), "failure occurs after PK copy")
	require.Zero(t, out.Vecs[1].Length())
	// next's unchanged defer returns the source Keys even on this error. Never
	// inspect a buffer after it has been returned to its pool.
}
