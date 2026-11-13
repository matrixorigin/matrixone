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

package fulltext2

import (
	"bytes"
	"context"
	"sync"
	"testing"

	"github.com/matrixorigin/matrixone/pkg/common/mpool"
	"github.com/matrixorigin/matrixone/pkg/container/batch"
	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/container/vector"
	"github.com/matrixorigin/matrixone/pkg/util/executor"
	veccache "github.com/matrixorigin/matrixone/pkg/vectorindex/cache"
	"github.com/matrixorigin/matrixone/pkg/vectorindex/sqlexec"
	"github.com/stretchr/testify/require"
)

func TestMoIndexStatsFulltext2LiveDocumentFrequencies(t *testing.T) {
	for _, loaded := range []bool{false, true} {
		name := "build"
		if loaded {
			name = "loaded"
		}
		t.Run(name, func(t *testing.T) {
			base, err := BuildSegmentFromTokenized("base", int32(types.T_int64), []TokenizedDoc{
				{Pk: int64(1), Terms: []string{"apple", "apple", "banana"}},
				{Pk: int64(2), Terms: []string{"obsolete"}},
				{Pk: int64(3), Terms: []string{"deleted"}},
			})
			require.NoError(t, err)
			tail, err := BuildSegmentFromTokenized("tail", int32(types.T_int64), []TokenizedDoc{
				{Pk: int64(2), Terms: []string{"apple", "pear"}},
				{Pk: int64(4), Terms: []string{"banana"}},
			})
			require.NoError(t, err)
			segments := []*Segment{base, tail}
			if loaded {
				for i, seg := range segments {
					data, err := seg.Serialize()
					require.NoError(t, err)
					segments[i], err = Deserialize(seg.Id, bytes.NewReader(data))
					require.NoError(t, err)
				}
			}
			segments[1].Recency = 1
			idx := NewIndex(segments, map[any]int64{normalizeKey(int64(3)): 1})
			defer idx.Free()
			stats, err := idx.Stats(t.Context())
			require.NoError(t, err)
			require.Equal(t, int64(3), stats.Documents)
			require.Equal(t, int64(3), stats.Terms)
			require.Equal(t, int64(1), stats.MinDF)
			require.Equal(t, int64(2), stats.MaxDF)
			require.InDelta(t, 5.0/3, stats.MeanDF, 1e-12)
			// The scan must not change query liveness or mutate the generation.
			require.Equal(t, []any{int64(2)}, iquery(t, idx, "pear"))
			require.Empty(t, iquery(t, idx, "obsolete"))
			require.Empty(t, iquery(t, idx, "deleted"))
		})
	}
}

func TestMoIndexStatsFulltext2EmptyAndCancellation(t *testing.T) {
	idx := NewIndex(nil, nil)
	stats, err := idx.Stats(t.Context())
	require.NoError(t, err)
	require.Zero(t, stats.Documents)
	require.Zero(t, stats.Terms)
	ctx, cancel := context.WithCancel(t.Context())
	cancel()
	_, err = idx.Stats(ctx)
	require.ErrorIs(t, err, context.Canceled)
}

func TestMoIndexStatsFulltext2DFUsesHeader(t *testing.T) {
	seg := loadedHeaderOnlySegment(t, 7)
	idx := &Index{segments: []*Segment{seg}, liveOrd: [][]bool{nil}, globalN: 7}
	stats, err := idx.Stats(t.Context())
	require.NoError(t, err)
	require.Equal(t, int64(1), stats.Terms)
	require.Equal(t, int64(7), stats.MinDF)
	require.Equal(t, float64(7), stats.MeanDF)
}

func TestMoIndexStatsFulltext2ReusesMatchingWarmGeneration(t *testing.T) {
	oldSQL, oldCache := runSql, veccache.Cache
	veccache.Cache = veccache.NewVectorIndexCache()
	t.Cleanup(func() { veccache.Cache.Destroy(); veccache.Cache = oldCache; runSql = oldSQL })
	reader := loadedSearch(t)
	reader.genValid = true
	reader.loadedTs = 11
	reader.loadedTail = 22
	entry := &veccache.VectorIndexSearch{Algo: reader}
	entry.Cond = sync.NewCond(entry.Mutex.RLocker())
	entry.Status.Store(1)
	veccache.Cache.IndexMap.Store("stats_warm", entry)
	n := 0
	mp := mpool.MustNewZero()
	runSql = func(_ *sqlexec.SqlProcess, _ string) (executor.Result, error) {
		n++
		v := int64(11)
		if n%2 == 0 {
			v = 22
		}
		bat := batch.NewWithSize(1)
		bat.Vecs[0] = vector.NewVec(types.T_int64.ToType())
		require.NoError(t, vector.AppendFixed(bat.Vecs[0], v, false, mp))
		bat.SetRowCount(1)
		return executor.Result{Mp: mp, Batches: []*batch.Batch{bat}}, nil
	}
	sqlproc := sqlexec.NewSqlProcessWithContext(sqlexec.NewSqlContext(t.Context(), "", nil, 0, nil))
	got, err := ReadStats(t.Context(), sqlproc, TableConfig{IndexTable: "stats_warm"})
	require.NoError(t, err)
	want, err := reader.Stats(t.Context())
	require.NoError(t, err)
	require.Equal(t, want, got)
	require.Equal(t, 2, n)
	reader.loadedTail = 99
	_, err = ReadStats(t.Context(), sqlproc, TableConfig{IndexTable: "stats_warm"})
	require.ErrorContains(t, err, "read snapshot is unavailable", "a different generation must never serve the statement")
}
