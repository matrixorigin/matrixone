// Copyright 2022 Matrix Origin
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//	http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package hnsw

import (
	"context"
	"fmt"
	"os"
	"strings"
	"sync"
	"testing"
	"time"

	fallocate "github.com/detailyang/go-fallocate"
	"github.com/matrixorigin/matrixone/pkg/common/moerr"
	"github.com/matrixorigin/matrixone/pkg/common/mpool"
	"github.com/matrixorigin/matrixone/pkg/container/batch"
	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/container/vector"
	"github.com/matrixorigin/matrixone/pkg/testutil"
	"github.com/matrixorigin/matrixone/pkg/util/executor"
	"github.com/matrixorigin/matrixone/pkg/vectorindex"
	"github.com/matrixorigin/matrixone/pkg/vectorindex/cache"
	"github.com/matrixorigin/matrixone/pkg/vectorindex/sqlexec"
	"github.com/matrixorigin/matrixone/pkg/vm/process"
	"github.com/stretchr/testify/require"
	usearch "github.com/unum-cloud/usearch/golang"
)

// give metadata [index_id, checksum, timestamp]
func mock_runSql(sqlproc *sqlexec.SqlProcess, sql string) (executor.Result, error) {
	proc := sqlproc.Proc
	return executor.Result{Mp: proc.Mp(), Batches: []*batch.Batch{makeMetaBatch(proc)}}, nil
}

// give blob
func mock_runSql_streaming(
	ctx context.Context,
	sqlproc *sqlexec.SqlProcess,
	sql string,
	ch chan executor.Result,
	err_chan chan error,
) (executor.Result, error) {

	proc := sqlproc.Proc
	res := executor.Result{Mp: proc.Mp(), Batches: []*batch.Batch{makeIndexBatch(proc)}}
	ch <- res
	return executor.Result{}, nil
}

// give metadata [index_id, checksum, timestamp]
func mock_runSql_2files(sqlproc *sqlexec.SqlProcess, sql string) (executor.Result, error) {
	proc := sqlproc.Proc
	return executor.Result{Mp: proc.Mp(), Batches: []*batch.Batch{makeMetaBatch2Files(proc)}}, nil
}

// give blob
func mock_runSql_streaming_2files(
	ctx context.Context,
	sqlproc *sqlexec.SqlProcess,
	sql string,
	ch chan executor.Result,
	err_chan chan error,
) (executor.Result, error) {

	fmt.Printf("SQL %s\n", sql)
	idx := 0
	if strings.Contains(sql, "abc-1") {
		idx = 1
	}

	proc := sqlproc.Proc
	res := executor.Result{Mp: proc.Mp(), Batches: []*batch.Batch{makeIndexBatch2Files(proc, idx)}}
	ch <- res
	return executor.Result{}, nil
}

func TestHnswSearchFloat32(t *testing.T) {
	m := mpool.MustNewZero()
	proc := testutil.NewProcessWithMPool(t, "", m)
	sqlproc := sqlexec.NewSqlProcess(proc)

	idxcfg := vectorindex.IndexConfig{Type: "hnsw", Usearch: usearch.DefaultConfig(3)}
	idxcfg.Usearch.Metric = usearch.L2sq
	tblcfg := vectorindex.IndexTableConfig{DbName: "db", SrcTable: "src", MetadataTable: "__secondary_meta", IndexTable: "__secondary_index"}

	s := NewHnswSearch[float32](idxcfg, tblcfg)
	// mock Search call by providing a minimal environment where Search might return nil or some values
	// Since s.Indexes is empty, Search will return nil, nil, nil or error.

	rt := vectorindex.RuntimeConfig{Limit: 4}
	query := []float32{1, 2, 3}

	// 1. Test with nil results (no indexes loaded)
	outKeys := make([]int64, 4)
	outDists := make([]float32, 4)
	err := s.SearchFloat32(sqlproc, query, rt, outKeys, outDists)
	require.NoError(t, err)

	// 2. Mock some indexes to test copying logic
	idx, err := usearch.NewIndex(idxcfg.Usearch)
	require.NoError(t, err)
	defer idx.Destroy()

	err = idx.Reserve(1)
	require.NoError(t, err)
	err = idx.Add(0, []float32{1, 2, 3})
	require.NoError(t, err)

	s.Indexes = []*HnswModel[float32]{
		{
			Id:    "abc-0",
			Index: idx,
		},
	}

	keysAny, dists64, err := s.Search(sqlproc, query, rt)
	require.NoError(t, err)
	expectedKeys := keysAny.([]int64)

	err = s.SearchFloat32(sqlproc, query, rt, outKeys, outDists)
	require.NoError(t, err)

	require.Equal(t, expectedKeys, outKeys[:len(expectedKeys)])
	for i := range dists64 {
		require.InDelta(t, dists64[i], float64(outDists[i]), 1e-5)
	}
}

func TestHnswSearchFloat32_BadQueryType(t *testing.T) {
	m := mpool.MustNewZero()
	proc := testutil.NewProcessWithMPool(t, "", m)
	sqlproc := sqlexec.NewSqlProcess(proc)

	idxcfg := vectorindex.IndexConfig{Type: "hnsw", Usearch: usearch.DefaultConfig(3)}
	idxcfg.Usearch.Metric = usearch.L2sq
	tblcfg := vectorindex.IndexTableConfig{}

	s := NewHnswSearch[float32](idxcfg, tblcfg)
	rt := vectorindex.RuntimeConfig{Limit: 1}

	// pass non-[]float32 query — Search returns error, SearchFloat32 propagates it
	err := s.SearchFloat32(sqlproc, "wrong", rt, nil, nil)
	require.Error(t, err)
}

func TestBoundedHnswSearchLimits(t *testing.T) {
	// requested >= every file: each file returns its full cardinality, result = total.
	perIndex, resultLimit := boundedHnswSearchLimits([]uint{3, 7}, ^uint(0))
	require.Equal(t, []uint{3, 7}, perIndex)
	require.Equal(t, uint(10), resultLimit)

	// requested caps each per-file limit and the result; the heap bound is this resultLimit
	// (5), NOT the sum of per-file limits (8) — that was the shard_count*LIMIT bug (#25637).
	perIndex, resultLimit = boundedHnswSearchLimits([]uint{3, 7}, 5)
	require.Equal(t, []uint{3, 5}, perIndex)
	require.Equal(t, uint(5), resultLimit)

	// total overflows to saturate; resultLimit is capped by requested.
	_, resultLimit = boundedHnswSearchLimits([]uint{^uint(0), ^uint(0)}, ^uint(0))
	require.Equal(t, ^uint(0), resultLimit)
}

func TestHnswSearchUnlockSynchronizesWithWaitPredicate(t *testing.T) {
	s := NewHnswSearch[float32](vectorindex.IndexConfig{}, vectorindex.IndexTableConfig{ThreadsSearch: 1})
	s.Concurrency.Store(1)
	s.Cond.L.Lock()
	started := make(chan struct{})
	done := make(chan struct{})
	go func() {
		close(started)
		s.unlock()
		close(done)
	}()
	<-started

	select {
	case <-done:
		t.Fatal("unlock changed the predicate without acquiring the condition lock")
	case <-time.After(20 * time.Millisecond):
	}
	s.Cond.L.Unlock()
	<-done
	require.Zero(t, s.Concurrency.Load())
}

func TestHnsw(t *testing.T) {
	m := mpool.MustNewZero()
	proc := testutil.NewProcessWithMPool(t, "", m)
	sqlproc := sqlexec.NewSqlProcess(proc)

	oldRunSQL := runSql
	oldRunSQLStreaming := runSql_streaming
	oldTTL := cache.VectorIndexCacheTTL
	oldCache := cache.Cache

	// Stub the SQL functions and isolate the process-global cache state.
	runSql = mock_runSql
	runSql_streaming = mock_runSql_streaming
	cacheTTL := 2 * time.Second
	nthread := 64
	iterations := 20000
	if testing.Short() {
		// PR CI needs the concurrent load/search/expiry transitions, not the
		// production-scale stress count.
		cacheTTL = 100 * time.Millisecond
		nthread = 16
		iterations = 1000
	}
	cache.VectorIndexCacheTTL = cacheTTL
	testCache := cache.NewVectorIndexCache()
	cache.Cache = testCache
	t.Cleanup(func() {
		testCache.Destroy()
		cache.Cache = oldCache
		cache.VectorIndexCacheTTL = oldTTL
		runSql = oldRunSQL
		runSql_streaming = oldRunSQLStreaming
	})

	idxcfg := vectorindex.IndexConfig{Type: "hnsw", Usearch: usearch.DefaultConfig(3)}
	idxcfg.Usearch.Metric = usearch.L2sq
	tblcfg := vectorindex.IndexTableConfig{DbName: "db", SrcTable: "src", MetadataTable: "__secondary_meta", IndexTable: "__secondary_index"}
	fp32a := []float32{0, 1, 2}

	var wg sync.WaitGroup

	for i := 0; i < nthread; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for j := 0; j < iterations; j++ {
				cache.Cache.Once()

				algo := NewHnswSearch[float32](idxcfg, tblcfg)
				anykeys, distances, err := cache.Cache.Search(sqlproc, tblcfg.IndexTable, algo, fp32a, vectorindex.RuntimeConfig{Limit: 4})
				require.Nil(t, err)
				keys, ok := anykeys.([]int64)
				require.True(t, ok)

				require.Equal(t, len(keys), 4)
				require.Equal(t, keys[0], int64(0))
				require.Equal(t, distances[0], float64(0))
				//os.Stderr.WriteString(fmt.Sprintf("keys %v distance %v\n", keys, distances))
			}
		}()
	}

	wg.Wait()

	require.Eventually(t, func() bool {
		empty := true
		testCache.IndexMap.Range(func(_, _ any) bool {
			empty = false
			return false
		})
		return empty
	}, 3*cacheTTL, 10*time.Millisecond, "cache entry must expire after searches stop")
}

func makeMetaBatch(proc *process.Process) *batch.Batch {
	indexfile := "resources/hnsw0.bin"

	bat := batch.NewWithSize(4)
	bat.Vecs[0] = vector.NewVec(types.New(types.T_varchar, 128, 0))   // index_id
	bat.Vecs[1] = vector.NewVec(types.New(types.T_varchar, 65536, 0)) // checksum
	bat.Vecs[2] = vector.NewVec(types.New(types.T_int64, 8, 0))       // timestamp
	bat.Vecs[3] = vector.NewVec(types.New(types.T_int64, 8, 0))       // timestamp

	vector.AppendBytes(bat.Vecs[0], []byte("abc-0"), false, proc.Mp())
	chksum, err := vectorindex.CheckSum(indexfile)
	if err != nil {
		panic("file checksum error")
	}

	finfo, err := os.Stat(indexfile)
	if err != nil {
		panic("file not found")
	}

	vector.AppendBytes(bat.Vecs[1], []byte(chksum), false, proc.Mp())
	vector.AppendFixed[int64](bat.Vecs[2], int64(0), false, proc.Mp())
	vector.AppendFixed[int64](bat.Vecs[3], finfo.Size(), false, proc.Mp())

	bat.SetRowCount(1)
	return bat
}

func makeIndexBatch(proc *process.Process) *batch.Batch {
	bat := batch.NewWithSize(2)
	bat.Vecs[0] = vector.NewVec(types.New(types.T_int64, 8, 0))    // chunk_id
	bat.Vecs[1] = vector.NewVec(types.New(types.T_blob, 65536, 0)) // data

	dat, err := os.ReadFile("resources/hnsw0.bin")
	if err != nil {
		panic("read file error")
	}
	vector.AppendFixed[int64](bat.Vecs[0], int64(0), false, proc.Mp())
	vector.AppendBytes(bat.Vecs[1], dat, false, proc.Mp())
	bat.SetRowCount(1)
	return bat
}

func TestFallocate(t *testing.T) {

	f, err := os.Create("apple")
	require.Nil(t, err)
	fallocate.Fallocate(f, 0, 10000)
	f.Close()
}

func makeMetaBatch2Files(proc *process.Process) *batch.Batch {
	indexfiles := []string{"resources/hnsw0.bin", "resources/hnsw1.bin"}
	ids := []string{"abc-0", "abc-1"}

	bat := batch.NewWithSize(4)
	bat.Vecs[0] = vector.NewVec(types.New(types.T_varchar, 128, 0))   // index_id
	bat.Vecs[1] = vector.NewVec(types.New(types.T_varchar, 65536, 0)) // checksum
	bat.Vecs[2] = vector.NewVec(types.New(types.T_int64, 8, 0))       // timestamp
	bat.Vecs[3] = vector.NewVec(types.New(types.T_int64, 8, 0))       // timestamp

	for i, indexfile := range indexfiles {

		vector.AppendBytes(bat.Vecs[0], []byte(ids[i]), false, proc.Mp())
		chksum, err := vectorindex.CheckSum(indexfile)
		if err != nil {
			panic("file checksum error")
		}

		finfo, err := os.Stat(indexfile)
		if err != nil {
			panic("file not found")
		}

		vector.AppendBytes(bat.Vecs[1], []byte(chksum), false, proc.Mp())
		vector.AppendFixed[int64](bat.Vecs[2], int64(0), false, proc.Mp())
		vector.AppendFixed[int64](bat.Vecs[3], finfo.Size(), false, proc.Mp())

	}

	bat.SetRowCount(len(indexfiles))
	return bat
}

func makeIndexBatch2Files(proc *process.Process, id int) *batch.Batch {
	indexfiles := []string{"resources/hnsw0.bin", "resources/hnsw1.bin"}
	indexfile := indexfiles[id]

	bat := batch.NewWithSize(2)
	bat.Vecs[0] = vector.NewVec(types.New(types.T_int64, 8, 0))    // chunk_id
	bat.Vecs[1] = vector.NewVec(types.New(types.T_blob, 65536, 0)) // data

	dat, err := os.ReadFile(indexfile)
	if err != nil {
		panic("read file error")
	}
	vector.AppendFixed[int64](bat.Vecs[0], int64(0), false, proc.Mp())
	vector.AppendBytes(bat.Vecs[1], dat, false, proc.Mp())
	bat.SetRowCount(1)
	return bat
}

// TestHnswSearchIsStaleUncheckableEvicts: the index model loaded fine but generation capture
// failed (genValid=false — a transient error on the tiny generation SELECT after the model itself
// loaded). IsStale must report stale, NOT a no-op: an uncheckable entry whose TTL keeps sliding
// on every search would otherwise serve pre-CDC/rebuild data forever. Reporting stale forces a
// bounded evict+reload that retries capture and self-heals once it succeeds.
func TestHnswSearchIsStaleUncheckableEvicts(t *testing.T) {
	// genValid=false: model present, generation never captured.
	s := &HnswSearch[float32]{genValid: false, cnUUID: "some-cn"}
	stale, err := s.IsStale()
	require.NoError(t, err)
	require.True(t, stale, "an entry that can't self-check freshness must be evicted, not pinned")

	// no service to re-query with (cnUUID empty) is equally uncheckable → stale.
	s2 := &HnswSearch[float32]{genValid: true, cnUUID: ""}
	stale, err = s2.IsStale()
	require.NoError(t, err)
	require.True(t, stale)
}

// TestHnswSyncNextTimestampMonotonic covers the enforced-monotonic generation: even when the
// existing max timestamp EXCEEDS the writer's wall-clock (cross-CN clock skew, or a local clock
// stepping backward), nextTimestamp still allocates strictly above it — so MAX(metadata.timestamp)
// always advances on a CDC save and HnswSearch.IsStale cannot miss the update.
func TestHnswSyncNextTimestampMonotonic(t *testing.T) {
	// existing max is 1h in the FUTURE relative to wall-clock.
	future := time.Now().UnixMicro() + int64(time.Hour/time.Microsecond)
	s := &HnswSync[float32]{indexes: []*HnswModel[float32]{{Timestamp: future - 1}, {Timestamp: future}}}
	require.Equal(t, future+1, s.nextTimestamp(), "must advance past the existing max, not use wall-clock")

	// no existing rows → falls back to wall-clock now (bounded).
	empty := &HnswSync[float32]{}
	ts := empty.nextTimestamp()
	require.Positive(t, ts)
	require.LessOrEqual(t, ts, time.Now().UnixMicro()+1)
}

// TestHnswSearchIsStaleQueryError covers the IsStale query path: with a captured generation but
// an unresolvable CN service, queryHnswGeneration errors, and IsStale treats a query error as
// stale (so a dropped/rebuilt index's dead cache entry is reclaimed) while surfacing the error.
func TestHnswSearchIsStaleQueryError(t *testing.T) {
	s := &HnswSearch[float32]{
		genValid: true,
		cnUUID:   "no-such-cn-uuid",
		Tblcfg:   vectorindex.IndexTableConfig{DbName: "db", MetadataTable: "m", IndexTable: "s"},
	}
	stale, err := s.IsStale()
	require.Error(t, err)
	require.True(t, stale)
}

// TestHnswGenerationSqls pins the two-part generation: MAX(timestamp) AND COUNT(*) over the
// metadata table. COUNT(*) is the deletion-sensitive half — without it, emptying a model (which
// deletes its metadata row with no compensating insert) would not move the generation.
func TestHnswGenerationSqls(t *testing.T) {
	tsSQL, countSQL := hnswGenerationSqls(vectorindex.IndexTableConfig{DbName: "db", MetadataTable: "__meta"})
	require.Contains(t, tsSQL, "MAX(timestamp)")
	require.Contains(t, tsSQL, "`db`.`__meta`")
	require.Contains(t, countSQL, "COUNT(*)")
	require.Contains(t, countSQL, "`db`.`__meta`")
}

func genInt64Result(t *testing.T, mp *mpool.MPool, v int64) executor.Result {
	bat := batch.NewWithSize(1)
	bat.Vecs[0] = vector.NewVec(types.T_int64.ToType())
	require.NoError(t, vector.AppendFixed[int64](bat.Vecs[0], v, false, mp))
	bat.SetRowCount(1)
	return executor.Result{Mp: mp, Batches: []*batch.Batch{bat}}
}

// TestHnswSearchIsStaleEmptyModelDropsCount is the regression for the empty-model generation gap:
// CDC empties the lower-timestamp model in a two-model index, so its metadata row is DELETED with
// no insert. MAX(timestamp) stays pinned at the surviving model's 200 — unchanged from load — so
// a timestamp-only generation would report fresh and the remote cache would keep serving the
// deleted vectors forever. COUNT(*) drops 2→1, so the (ts, count) generation still changes and
// IsStale correctly reports stale.
func TestHnswSearchIsStaleEmptyModelDropsCount(t *testing.T) {
	mp := mpool.MustNewZero()
	old := runSqlAutoCommit
	defer func() { runSqlAutoCommit = old }()

	// current on-disk generation: MAX(timestamp)=200 (unchanged), COUNT(*)=1 (emptied model's row gone).
	runSqlAutoCommit = func(_ context.Context, _ string, _ uint32, _, sql string) (executor.Result, error) {
		if strings.Contains(sql, "COUNT(*)") {
			return genInt64Result(t, mp, 1), nil
		}
		return genInt64Result(t, mp, 200), nil
	}
	cfg := vectorindex.IndexTableConfig{DbName: "db", MetadataTable: "m", IndexTable: "s"}

	// loaded at (ts=200, count=2): two models, the lower-ts one about to be emptied.
	s := &HnswSearch[float32]{genValid: true, cnUUID: "cn", Tblcfg: cfg, loadedTs: 200, loadedCount: 2}
	stale, err := s.IsStale()
	require.NoError(t, err)
	require.True(t, stale, "emptied model drops COUNT(*) even though MAX(timestamp) is unchanged")

	// control: loaded at the current generation (ts=200, count=1) → not stale.
	s2 := &HnswSearch[float32]{genValid: true, cnUUID: "cn", Tblcfg: cfg, loadedTs: 200, loadedCount: 1}
	stale, err = s2.IsStale()
	require.NoError(t, err)
	require.False(t, stale)
}

// TestLoadHnswGenerationHappy stubs the live-txn reader to cover the two-read (timestamp, count)
// load path plus the second-read error branch.
func TestLoadHnswGenerationHappy(t *testing.T) {
	mp := mpool.MustNewZero()
	cfg := vectorindex.IndexTableConfig{DbName: "db", MetadataTable: "m", IndexTable: "s"}
	old := runSql
	defer func() { runSql = old }()

	runSql = func(_ *sqlexec.SqlProcess, sql string) (executor.Result, error) {
		if strings.Contains(sql, "COUNT(*)") {
			return genInt64Result(t, mp, 3), nil // model count
		}
		return genInt64Result(t, mp, 150), nil // MAX(timestamp)
	}
	ts, count, err := loadHnswGeneration(nil, cfg)
	require.NoError(t, err)
	require.Equal(t, int64(150), ts)
	require.Equal(t, int64(3), count)

	// count read errors → propagated.
	runSql = func(_ *sqlexec.SqlProcess, sql string) (executor.Result, error) {
		if strings.Contains(sql, "COUNT(*)") {
			return executor.Result{}, moerr.NewInternalErrorNoCtx("count read failed")
		}
		return genInt64Result(t, mp, 150), nil
	}
	_, _, err = loadHnswGeneration(nil, cfg)
	require.Error(t, err)
}
