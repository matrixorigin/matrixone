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
	"strings"
	"sync"
	"testing"

	"github.com/golang/mock/gomock"
	"github.com/matrixorigin/matrixone/pkg/common/mpool"
	"github.com/matrixorigin/matrixone/pkg/container/batch"
	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/container/vector"
	mock_frontend "github.com/matrixorigin/matrixone/pkg/frontend/test"
	"github.com/matrixorigin/matrixone/pkg/pb/timestamp"
	"github.com/matrixorigin/matrixone/pkg/pb/txn"
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
	baseTS := int64(11)
	n := 0
	mp := mpool.MustNewZero()
	runSql = func(_ *sqlexec.SqlProcess, query string) (executor.Result, error) {
		n++
		v := baseTS
		if strings.Contains(query, "chunk_id") {
			v = 22
		}
		bat := batch.NewWithSize(1)
		bat.Vecs[0] = vector.NewVec(types.T_int64.ToType(), mp)
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
	reader.loadedTs, baseTS = 0, 0
	_, err = ReadStats(t.Context(), sqlproc, TableConfig{IndexTable: "stats_warm"})
	require.ErrorContains(t, err, "read snapshot is unavailable", "base-less generations must bypass the bare key even when their tail positions match")
	reader.loadedTs, baseTS = 11, 11
	reader.loadedTail = 99
	_, err = ReadStats(t.Context(), sqlproc, TableConfig{IndexTable: "stats_warm"})
	require.ErrorContains(t, err, "read snapshot is unavailable", "a different generation must never serve the statement")
	// The cache contains only the generation loaded by a previous diagnostic.
	veccache.Cache.IndexMap.Delete("stats_warm")
	reader.loadedTail = 22
	key := veccache.GenerationKey("stats_warm", 11, 22)
	veccache.Cache.IndexMap.Store(key, entry)
	for _, physical := range []int64{100, 200} {
		op := mock_frontend.NewMockTxnOperator(gomock.NewController(t))
		op.EXPECT().Txn().Return(txn.TxnMeta{SnapshotTS: timestamp.Timestamp{PhysicalTime: physical}}).AnyTimes()
		sqlproc.SqlCtx.TxnOperator = op
		before := n
		got, err = ReadStats(t.Context(), sqlproc, TableConfig{IndexTable: "stats_warm"})
		require.NoError(t, err)
		require.Equal(t, want, got)
		require.Equal(t, before+2, n, "a matching immutable generation only needs the two fingerprint reads")
		require.Equal(t, []string{key}, veccache.Cache.Keys())
	}

}

func TestMoIndexStatsFulltext2GenerationCaptureRetry(t *testing.T) {
	previous := veccache.Cache
	veccache.Cache = veccache.NewVectorIndexCache()
	t.Cleanup(func() { veccache.Cache.Destroy(); veccache.Cache = previous })
	sp, mp := mockSqlProc(t)
	op := mock_frontend.NewMockTxnOperator(gomock.NewController(t))
	op.EXPECT().Txn().Return(txn.TxnMeta{SnapshotTS: timestamp.Timestamp{PhysicalTime: 100}}).AnyTimes()
	sp.Proc.Base.TxnOperator = op
	reads := 0
	swapRunSql(t, func(_ *sqlexec.SqlProcess, sql string) (executor.Result, error) {
		switch {
		case strings.Contains(sql, "MAX(timestamp"):
			reads++
			if reads != 2 {
				return executor.Result{}, context.DeadlineExceeded
			}
			return executor.Result{Mp: mp, Batches: []*batch.Batch{docsAndBytesBatch(mp, 11, 0)}}, nil
		case strings.Contains(sql, "MAX(chunk_id"), strings.Contains(sql, "SUM("), strings.Contains(sql, "COUNT(*)"):
			return executor.Result{Mp: mp, Batches: []*batch.Batch{docsAndBytesBatch(mp, 0, 0)}}, nil
		default:
			return executor.Result{Mp: mp}, nil
		}
	})
	_, err := ReadStats(t.Context(), sp, testStorageCfg())
	require.ErrorIs(t, err, context.DeadlineExceeded)
	require.Empty(t, veccache.Cache.Keys())
	_, err = ReadStats(t.Context(), sp, testStorageCfg())
	require.NoError(t, err)
	require.Equal(t, 2, reads, "each attempt captures the generation once, before cache publication")
}
