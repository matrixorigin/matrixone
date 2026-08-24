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
	"context"
	"math"
	"strings"
	"testing"

	"github.com/matrixorigin/matrixone/pkg/common/moerr"
	"github.com/matrixorigin/matrixone/pkg/common/mpool"
	"github.com/matrixorigin/matrixone/pkg/container/batch"
	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/container/vector"
	"github.com/matrixorigin/matrixone/pkg/testutil"
	"github.com/matrixorigin/matrixone/pkg/util/executor"
	"github.com/matrixorigin/matrixone/pkg/vectorindex"
	"github.com/matrixorigin/matrixone/pkg/vectorindex/sqlexec"
	"github.com/stretchr/testify/require"
)

func newSearchProc(t *testing.T) *sqlexec.SqlProcess {
	return sqlexec.NewSqlProcess(testutil.NewProc(t))
}

// loadedSearch builds a Fulltext2Search whose Index is assembled in memory (via the
// serialize→deserialize loadedSeg path), bypassing Load/DB.
func loadedSearch(t *testing.T) *Fulltext2Search {
	bb := NewBuilder("base", int32(types.T_int64))
	feed(t, bb, int64(0), "quick", "brown", "fox")
	feed(t, bb, int64(1), "quick", "brown", "dog")
	feed(t, bb, int64(2), "lazy", "fox", "sleeps")
	seg := loadedSeg(t, bb)
	s := NewFulltext2Search(TableConfig{IndexTable: "__store", Parser: ParserDefault})
	s.idx = NewIndex([]*Segment{seg}, nil)
	s.loaded = true
	return s
}

func TestFulltext2SearchNewAndUnloaded(t *testing.T) {
	proc := newSearchProc(t)
	s := NewFulltext2Search(TableConfig{IndexTable: "__store"})
	require.Equal(t, "__store", s.cfg.IndexTable)
	require.False(t, s.loaded)

	// Search before Load → "not loaded".
	_, _, err := s.Search(proc, Fulltext2Query{Pattern: []byte("fox")}, vectorindex.RuntimeConfig{})
	require.ErrorContains(t, err, "not loaded")

	// SearchFloat32 is unsupported.
	require.ErrorContains(t, s.SearchFloat32(proc, nil, vectorindex.RuntimeConfig{}, nil, nil), "not supported")
}

func TestFulltext2SearchLoadEmitsTrace(t *testing.T) {
	sp := &sqlexec.SqlProcess{SqlCtx: sqlexec.NewSqlContext(context.Background(), "cn-1", nil, 7, nil)}
	mp := mpool.MustNewZero()
	cfg := testStorageCfg()
	var events []LoadEvent
	restore := setLoadObserver(func(event LoadEvent) { events = append(events, event) })
	defer restore()
	invalidateLoadGeneration(cfg, LoadMissCDCFlush)
	swapRunSql(t, func(_ *sqlexec.SqlProcess, sql string) (executor.Result, error) {
		switch {
		case strings.Contains(sql, "CAST(COALESCE"):
			return executor.Result{Mp: mp, Batches: []*batch.Batch{int64Batch(mp, 0)}}, nil
		case strings.Contains(sql, "MAX(timestamp)"):
			return executor.Result{Mp: mp, Batches: []*batch.Batch{int64Batch(mp, 11)}}, nil
		case strings.Contains(sql, "MAX(chunk_id)"):
			return executor.Result{Mp: mp, Batches: []*batch.Batch{int64Batch(mp, 22)}}, nil
		case strings.Contains(sql, "LENGTH("):
			return executor.Result{Mp: mp, Batches: []*batch.Batch{int64Batch(mp, 0)}}, nil
		default:
			return executor.Result{Mp: mp}, nil
		}
	})

	s := NewFulltext2Search(cfg)
	require.NoError(t, s.Load(sp))
	s.SetLoadWaiters(3)
	s.FinishLoadObservation()
	require.Len(t, events, 1)
	require.Equal(t, LoadMissCDCFlush, events[0].MissReason)
	require.Equal(t, int64(11), events[0].BaseGeneration)
	require.Equal(t, int64(22), events[0].TailGeneration)
	require.Equal(t, int64(3), events[0].SingleflightWaiters)
	require.True(t, events[0].LoadSuccess)
	reason, at := peekLoadReason(loadReasonKey(cfg.DbName, cfg.IndexTable))
	require.Empty(t, reason)
	require.True(t, at.IsZero())
	s.Destroy()
}

func TestFulltext2SearchLoadClassifiesQueryInterruptionAsCancel(t *testing.T) {
	sp := &sqlexec.SqlProcess{SqlCtx: sqlexec.NewSqlContext(context.Background(), "cn-1", nil, 7, nil)}
	var event LoadEvent
	restore := setLoadObserver(func(got LoadEvent) { event = got })
	defer restore()
	swapRunSql(t, func(_ *sqlexec.SqlProcess, _ string) (executor.Result, error) {
		return executor.Result{}, moerr.NewQueryInterrupted(context.Background())
	})

	s := NewFulltext2Search(testStorageCfg())
	require.Error(t, s.Load(sp))
	s.FinishLoadObservation()
	require.True(t, event.LoadCancel)
	require.False(t, event.LoadError)
	require.False(t, event.LoadSuccess)
}

func TestFulltext2SearchLoadRetainsInvalidationReasonAfterFailure(t *testing.T) {
	sp := &sqlexec.SqlProcess{SqlCtx: sqlexec.NewSqlContext(context.Background(), "cn-1", nil, 7, nil)}
	cfg := testStorageCfg()
	var events []LoadEvent
	restore := setLoadObserver(func(got LoadEvent) { events = append(events, got) })
	defer restore()

	pendingLoadReasons.Lock()
	previous := pendingLoadReasons.m
	pendingLoadReasons.m = make(map[string]pendingLoadReason)
	pendingLoadReasons.Unlock()
	t.Cleanup(func() {
		pendingLoadReasons.Lock()
		pendingLoadReasons.m = previous
		pendingLoadReasons.Unlock()
	})

	swapRunSql(t, func(_ *sqlexec.SqlProcess, _ string) (executor.Result, error) {
		return executor.Result{}, moerr.NewQueryInterrupted(context.Background())
	})
	invalidateLoadGeneration(cfg, LoadMissCDCFlush)

	s := NewFulltext2Search(cfg)
	require.Error(t, s.Load(sp))
	s.FinishLoadObservation()
	require.Error(t, s.Load(sp))
	s.FinishLoadObservation()

	require.Len(t, events, 2)
	require.Equal(t, LoadMissCDCFlush, events[0].MissReason)
	require.Equal(t, LoadMissCDCFlush, events[1].MissReason)
	require.True(t, events[0].LoadCancel)
	require.True(t, events[1].LoadCancel)
}

// TestStaleGenSqls pins the cache-freshness generation queries: MAX(timestamp) over the
// metadata table (REBUILD/MERGE signal) and MAX(chunk_id) over the tag=1 CdcTail (CDC-append
// signal), scoped to (CdcTailId, tag=1) so a base sub-index cannot mask a fresh append.
func TestStaleGenSqls(t *testing.T) {
	cfg := TableConfig{DbName: "db", IndexTable: "__store", MetadataTable: "__meta"}
	tsSQL, tailSQL := StaleGenSqls(cfg)
	require.Contains(t, tsSQL, "MAX(timestamp)")
	require.Contains(t, tsSQL, "`db`.`__meta`")
	require.Contains(t, tailSQL, "MAX(chunk_id)")
	require.Contains(t, tailSQL, "`db`.`__store`")
	require.Contains(t, tailSQL, "tag = 1")             // tag=Tag_CdcEvents
	require.Contains(t, tailSQL, vectorindex.CdcTailId) // scoped to the single CDC tail
}

// TestIsStaleUncheckableEvicts: the index loaded fine (loadedSearch assembles segments in memory)
// but no generation was captured (genValid=false — the load-time capture failed). IsStale must
// report stale, NOT a no-op: an uncheckable entry whose TTL keeps sliding on every search would
// otherwise serve pre-CDC/rebuild data forever. Reporting stale forces a bounded evict+reload
// that retries capture and self-heals once it succeeds.
func TestIsStaleUncheckableEvicts(t *testing.T) {
	s := loadedSearch(t)
	require.False(t, s.genValid) // loaded, but generation never captured
	stale, err := s.IsStale()
	require.NoError(t, err)
	require.True(t, stale, "an entry that can't self-check freshness must be evicted, not pinned")
}

func TestFulltext2SearchEmptyIndex(t *testing.T) {
	proc := newSearchProc(t)
	s := NewFulltext2Search(TableConfig{IndexTable: "__store", Parser: ParserDefault})
	s.idx = NewIndex(nil, nil) // loaded but doc-less
	s.loaded = true

	keys, dists, err := s.Search(proc, Fulltext2Query{Pattern: []byte("fox")}, vectorindex.RuntimeConfig{})
	require.NoError(t, err)
	require.Empty(t, keys)
	require.Empty(t, dists)
}

func TestFulltext2SearchInvalidPayload(t *testing.T) {
	proc := newSearchProc(t)
	s := loadedSearch(t)
	defer s.Destroy()

	_, _, err := s.Search(proc, "not a query", vectorindex.RuntimeConfig{})
	require.ErrorContains(t, err, "invalid query payload")
}

func TestFulltext2SearchTopK(t *testing.T) {
	proc := newSearchProc(t)
	s := loadedSearch(t)
	defer s.Destroy()

	// single-term NL query with a pushed LIMIT.
	keys, dists, err := s.Search(proc, Fulltext2Query{Pattern: []byte("fox"), Algo: BM25},
		vectorindex.RuntimeConfig{Limit: 10})
	require.NoError(t, err)
	ks, ok := keys.([]any)
	require.True(t, ok)
	require.Len(t, dists, len(ks))
	require.NotEmpty(t, ks) // "fox" hits docs 0 and 2

	// k <= 0 (no pushed LIMIT) falls back to NumDocs.
	keys, _, err = s.Search(proc, Fulltext2Query{Pattern: []byte("fox"), Algo: BM25},
		vectorindex.RuntimeConfig{Limit: 0})
	require.NoError(t, err)
	require.NotEmpty(t, keys.([]any))

	// an absurd LIMIT past MaxInt32 is clamped, not wrapped negative.
	keys, _, err = s.Search(proc, Fulltext2Query{Pattern: []byte("fox"), Algo: BM25},
		vectorindex.RuntimeConfig{Limit: uint(math.MaxInt32) + 100})
	require.NoError(t, err)
	require.NotEmpty(t, keys.([]any))

	// bag-of-words (IN BM25 MODE) path.
	keys, _, err = s.Search(proc, Fulltext2Query{Pattern: []byte("quick fox"), BagOfWords: true, Algo: BM25},
		vectorindex.RuntimeConfig{Limit: 10})
	require.NoError(t, err)
	require.NotEmpty(t, keys.([]any))
}

func TestFulltext2SearchStreamingEmit(t *testing.T) {
	proc := newSearchProc(t)
	s := loadedSearch(t)
	defer s.Destroy()

	// Emit set + no pushed LIMIT → streaming: results handed off via Emit, empty return.
	for _, bagOfWords := range []bool{false, true} {
		emitted := 0
		emit := func(o *vectorindex.SearchOutput) error {
			emitted += o.Keys.N
			PutColumnBuffer(o.Keys) // recycle like the real consumer
			return nil
		}
		keys, dists, err := s.Search(proc,
			Fulltext2Query{Pattern: []byte("fox"), BagOfWords: bagOfWords, Algo: BM25},
			vectorindex.RuntimeConfig{Emit: emit})
		require.NoError(t, err)
		require.Empty(t, keys)
		require.Empty(t, dists)
		require.Positive(t, emitted, "bagOfWords=%v should emit docs", bagOfWords)
	}
}

func TestFulltext2SearchDestroy(t *testing.T) {
	s := loadedSearch(t)

	// The cached config is immutable for the entry's lifetime (no UpdateConfig hook —
	// a config change evicts the entry), so Search is pure-read; here we just pin that
	// the constructed cfg is what Load queries with and that Destroy tears down cleanly.
	require.Equal(t, ParserDefault, s.cfg.Parser)

	// Destroy frees and clears the loaded index.
	s.Destroy()
	require.Nil(t, s.idx)
	require.False(t, s.loaded)
}

// TestLoadGenerationHappy stubs the package runSql to cover the fulltext2 generation reader
// (StaleGenSqls + resultScalarInt64 + the two-read LoadGeneration body).
func TestLoadGenerationHappy(t *testing.T) {
	mp := mpool.MustNewZero()
	old := runSql
	defer func() { runSql = old }()
	n := 0
	runSql = func(_ *sqlexec.SqlProcess, _ string) (executor.Result, error) {
		n++
		bat := batch.NewWithSize(1)
		bat.Vecs[0] = vector.NewVec(types.T_int64.ToType())
		v := int64(11)
		if n == 2 {
			v = 22
		}
		require.NoError(t, vector.AppendFixed[int64](bat.Vecs[0], v, false, mp))
		bat.SetRowCount(1)
		return executor.Result{Mp: mp, Batches: []*batch.Batch{bat}}, nil
	}
	ts, tail, err := LoadGeneration(nil, TableConfig{DbName: "db", MetadataTable: "m", IndexTable: "s"})
	require.NoError(t, err)
	require.Equal(t, int64(11), ts)   // MAX(timestamp)
	require.Equal(t, int64(22), tail) // MAX(chunk_id) tag=1
}

// TestLoadGenerationRecover: if the generation read panics (e.g. a background housekeeping call
// hits a torn-down executor), LoadGeneration must recover it into an error — never let it crash
// the caller. The caller then leaves genValid=false, and IsStale evicts to retry (see
// TestIsStaleUncheckableEvicts).
func TestLoadGenerationRecover(t *testing.T) {
	old := runSql
	defer func() { runSql = old }()
	runSql = func(_ *sqlexec.SqlProcess, _ string) (executor.Result, error) {
		panic("simulated executor teardown")
	}
	_, _, err := LoadGeneration(nil, TableConfig{DbName: "db", MetadataTable: "m", IndexTable: "s"})
	require.Error(t, err)
	require.Contains(t, err.Error(), "LoadGeneration recovered")
}

// TestFulltext2IsStaleQueryError: with a captured generation but an unresolvable CN service, the
// background QueryGeneration read errors (its recover turns the ServiceRuntime panic into an
// error), and IsStale treats a query error as stale so a dropped/rebuilt index's dead cache entry
// is reclaimed — while surfacing the error for logging.
func TestFulltext2IsStaleQueryError(t *testing.T) {
	s := NewFulltext2Search(TableConfig{DbName: "db", MetadataTable: "m", IndexTable: "s"})
	s.genValid = true
	s.cnUUID = "no-such-cn-uuid"
	stale, err := s.IsStale()
	require.Error(t, err)
	require.True(t, stale)
}

// TestFulltext2SearchInto pins the box-free LIMIT path: SearchInto fills the caller-owned
// SearchOutput (pk column, float32 scores, one nullable ColumnBuffer per FULL INCLUDE column)
// — box-free and reused across calls. incIdx has 5 docs all matching "x", includes
// [status varchar, prio int64] with a NULL status (pk4).
func TestFulltext2SearchInto(t *testing.T) {
	proc := newSearchProc(t)
	idx := incIdx(t)
	s := &Fulltext2Search{idx: idx, loaded: true, cfg: TableConfig{IndexTable: "__store", Parser: ParserDefault}}
	mp := mpool.MustNewZero()

	out := &vectorindex.SearchOutput{}
	rt := vectorindex.RuntimeConfig{Limit: 10, RequestedIncludeColumns: []string{"status", "prio"}}
	require.NoError(t, s.SearchInto(proc, Fulltext2Query{Pattern: []byte("x"), Algo: BM25}, rt, out))

	require.Equal(t, 5, out.Keys.N) // all 5 docs contain "x"
	require.Len(t, out.Dists, 5)
	require.Len(t, out.Include, 2) // status, prio (FULL include order)

	// Decode the box-free buffers into vectors and zip by row (Keys[i] <-> Include[*][i]) into
	// a pk -> (status, prio) map (result order is score-desc; equal scores are unspecified).
	keyVec := vector.NewVec(types.T_int64.ToType())
	require.NoError(t, vectorindex.AppendColumnBuffer(out.Keys, keyVec, mp))
	statusVec := vector.NewVec(types.T_varchar.ToType())
	require.NoError(t, vectorindex.AppendColumnBuffer(out.Include[0], statusVec, mp))
	prioVec := vector.NewVec(types.T_int64.ToType())
	require.NoError(t, vectorindex.AppendColumnBuffer(out.Include[1], prioVec, mp))

	pks := vector.MustFixedColWithTypeCheck[int64](keyVec)
	prios := vector.MustFixedColWithTypeCheck[int64](prioVec)
	require.Len(t, pks, 5)
	type sv struct {
		status any
		prio   int64
	}
	got := map[int64]sv{}
	for i := range pks {
		var st any
		if !statusVec.IsNull(uint64(i)) {
			st = statusVec.GetStringAt(i)
		}
		got[pks[i]] = sv{st, prios[i]}
	}
	require.Equal(t, sv{"active", int64(10)}, got[1])
	require.Equal(t, sv{"inactive", int64(20)}, got[2])
	require.Equal(t, sv{"active", int64(30)}, got[3])
	require.Equal(t, sv{nil, int64(40)}, got[4]) // NULL status preserved
	require.Equal(t, sv{"archived", int64(5)}, got[5])

	// Reuse: a second SearchInto Resets out and refills (no stale rows accumulated).
	require.NoError(t, s.SearchInto(proc, Fulltext2Query{Pattern: []byte("x"), Algo: BM25}, rt, out))
	require.Equal(t, 5, out.Keys.N)
	require.Len(t, out.Dists, 5)

	// No requested INCLUDE columns → out.Include emptied.
	require.NoError(t, s.SearchInto(proc, Fulltext2Query{Pattern: []byte("x"), Algo: BM25},
		vectorindex.RuntimeConfig{Limit: 10}, out))
	require.Equal(t, 5, out.Keys.N)
	require.Empty(t, out.Include)

	// nil out → error, not a nil-deref.
	require.Error(t, s.SearchInto(proc, Fulltext2Query{Pattern: []byte("x")}, rt, nil))
}

// TestFulltext2SearchIntoNotLoaded / empty: the two prepare() early-outs on the SearchInto path.
func TestFulltext2SearchIntoNotLoadedAndEmpty(t *testing.T) {
	proc := newSearchProc(t)
	out := &vectorindex.SearchOutput{}

	// not loaded → error.
	s := NewFulltext2Search(TableConfig{IndexTable: "__store"})
	require.ErrorContains(t, s.SearchInto(proc, Fulltext2Query{Pattern: []byte("x")}, vectorindex.RuntimeConfig{Limit: 10}, out), "not loaded")

	// loaded but empty index → no rows, no error, out emptied.
	s2 := &Fulltext2Search{idx: NewIndex(nil, nil), loaded: true, cfg: TableConfig{IndexTable: "__store"}}
	require.NoError(t, s2.SearchInto(proc, Fulltext2Query{Pattern: []byte("x")}, vectorindex.RuntimeConfig{Limit: 10}, out))
}
