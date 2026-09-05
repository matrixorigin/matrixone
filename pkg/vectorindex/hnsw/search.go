// Copyright 2022 Matrix Origin
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

package hnsw

import (
	"context"
	"fmt"
	"math"
	"sync"
	"sync/atomic"
	"time"

	"github.com/matrixorigin/matrixone/pkg/common/concurrent"
	"github.com/matrixorigin/matrixone/pkg/common/moerr"
	"github.com/matrixorigin/matrixone/pkg/common/sqlquote"
	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/container/vector"
	"github.com/matrixorigin/matrixone/pkg/logutil"
	"github.com/matrixorigin/matrixone/pkg/vectorindex"
	"github.com/matrixorigin/matrixone/pkg/vectorindex/metric"
	"github.com/matrixorigin/matrixone/pkg/vectorindex/sqlexec"
)

var runSql = sqlexec.RunSql
var runSql_streaming = sqlexec.RunStreamingSql

// This is the HNSW search implementation that implement VectorIndexSearchIf interface
type HnswSearch[T types.RealNumbers] struct {
	Idxcfg        vectorindex.IndexConfig
	Tblcfg        vectorindex.IndexTableConfig
	Indexes       []*HnswModel[T]
	Concurrency   atomic.Int64
	Mutex         sync.Mutex
	Cond          *sync.Cond
	ThreadsSearch int64

	// Generation captured at Load for the cross-CN cache freshness check (IsStale): a CONTENT
	// fingerprint over the per-model file checksums (MD5s) plus the model-row count — see
	// hnswGenerationSql. loadedFp moves on ANY content change (CDC append/delete rewrites a model
	// file, REBUILD/MERGE, or a model emptied) and is clock-independent, so it survives an
	// intermediate empty state that a timestamp-based generation could not. cnUUID/accountID re-query
	// in the background. genValid=false (capture failed) ⇒ IsStale reports stale so the entry is
	// evicted and reloaded.
	cnUUID      string
	accountID   uint32
	loadedFp    uint64
	loadedCount int64
	genValid    bool
}

func NewHnswSearch[T types.RealNumbers](idxcfg vectorindex.IndexConfig, tblcfg vectorindex.IndexTableConfig) *HnswSearch[T] {
	nthread := vectorindex.GetConcurrency(tblcfg.ThreadsSearch)
	s := &HnswSearch[T]{Idxcfg: idxcfg, Tblcfg: tblcfg, ThreadsSearch: nthread}
	s.Cond = sync.NewCond(&s.Mutex)
	return s
}

// acquire lock from a usearch threads
func (s *HnswSearch[T]) lock() {
	// check max threads
	s.Cond.L.Lock()
	defer s.Cond.L.Unlock()
	for s.Concurrency.Load() >= s.ThreadsSearch {
		s.Cond.Wait()
	}
	s.Concurrency.Add(1)
}

// release a lock from a usearch threads
func (s *HnswSearch[T]) unlock() {
	s.Cond.L.Lock()
	defer s.Cond.L.Unlock()
	s.Concurrency.Add(-1)
	s.Cond.Signal()
}

// Search the hnsw index (implement VectorIndexSearch.Search)
func (s *HnswSearch[T]) Search(sqlproc *sqlexec.SqlProcess, anyquery any, rt vectorindex.RuntimeConfig) (keys any, distances []float64, err error) {

	query, ok := anyquery.([]T)
	if !ok {
		return nil, nil, moerr.NewInternalErrorNoCtx("query is not []float32")
	}

	limit := rt.Limit

	if len(s.Indexes) == 0 {
		return []int64{}, []float64{}, nil
	}
	if limit == 0 {
		return []int64{}, []float64{}, nil
	}

	s.lock()
	defer s.unlock()

	indexCounts := make([]uint, len(s.Indexes))
	for i, idx := range s.Indexes {
		if idx.Index == nil {
			return nil, nil, moerr.NewInternalErrorNoCtx("usearch index is nil")
		}
		// Use the index's authoritative cardinality. The atomic Len is updated
		// before some add/remove operations complete, so using it to cap K can
		// transiently undercount after a failed remove and lose valid results.
		indexCounts[i], err = idx.Index.Len()
		if err != nil {
			return nil, nil, err
		}
	}
	indexLimits, limit := boundedHnswSearchLimits(indexCounts, limit)
	if limit == 0 {
		return []int64{}, []float64{}, nil
	}

	// search. The merge heap retains only the global best `limit` candidates, so peak
	// memory is O(limit) rather than O(shard_count * limit) (#25637). Clamp for the int
	// conversion; a KNN LIMIT beyond MaxInt is absurd and would exhaust memory regardless.
	hlimit := limit
	if hlimit > uint(math.MaxInt) {
		hlimit = uint(math.MaxInt)
	}
	heap := vectorindex.NewSearchResultSafeHeap(int(hlimit))

	nthread := int(vectorindex.GetConcurrency(0))
	if nthread > len(s.Indexes) {
		nthread = len(s.Indexes)
	}

	exec := concurrent.NewThreadPoolExecutor(nthread)
	err = exec.Execute(sqlproc.GetContext(),
		len(s.Indexes),
		func(ctx context.Context, thread_id int, start, end int) (err2 error) {
			subindex := s.Indexes[start:end]
			for j := range subindex {
				if ctx.Err() != nil {
					return ctx.Err()
				}

				keys, distances, err2 := subindex[j].Search(query, indexLimits[start+j])
				if err2 != nil {
					return err2
				}

				for k := range keys {
					heap.Push(&vectorindex.SearchResult{Id: int64(keys[k]), Distance: float64(distances[k])})
				}
			}
			return
		})
	if err != nil {
		return nil, nil, err
	}

	// The heap is already bounded to `limit`, so it holds at most `limit` (the global best)
	// results. It is a max-heap: Pop returns the worst (largest distance) first, so fill the
	// buffers back-to-front to produce ascending (nearest-first) order.
	n := heap.Len()
	reskeys := make([]int64, n)
	resdistances := make([]float64, n)
	for i := n - 1; i >= 0; i-- {
		srif := heap.Pop()
		sr, ok := srif.(*vectorindex.SearchResult)
		if !ok {
			return nil, nil, moerr.NewInternalError(sqlproc.GetContext(), "heap return key is not int64")
		}
		reskeys[i] = sr.Id
		sr.Distance = metric.DistanceTransformHnsw(sr.Distance, metric.DistFuncNameToMetricType[rt.OrigFuncName], s.Idxcfg.Usearch.Metric)
		resdistances[i] = sr.Distance
	}

	return reskeys, resdistances, nil
}

// boundedHnswSearchLimits prevents a very large LIMIT from being multiplied by
// the number of index files. Each file can return at most its own cardinality,
// and the final result cannot contain more rows than all files combined. The returned
// limit is also the bound on the merge heap (it retains only the global best `limit`).
func boundedHnswSearchLimits(indexCounts []uint, requested uint) ([]uint, uint) {
	perIndex := make([]uint, len(indexCounts))
	var total uint
	for i, count := range indexCounts {
		perIndex[i] = min(requested, count)
		if total > ^uint(0)-count {
			total = ^uint(0)
		} else {
			total += count
		}
	}
	return perIndex, min(requested, total)
}

func (s *HnswSearch[T]) Contains(key int64) (bool, error) {
	if len(s.Indexes) == 0 {
		return false, nil
	}
	s.lock()
	defer s.unlock()

	for _, idx := range s.Indexes {
		found, err := idx.Index.Contains(uint64(key))
		if err != nil {
			return false, err
		}
		if found {
			return true, nil
		}
	}
	return false, nil
}

// Destroy HnswSearch (implement VectorIndexSearch.Destroy)
func (s *HnswSearch[T]) Destroy() {
	// Through the model's own Destroy, not idx.Index.Destroy(): after Preload a model carries
	// metadata with a nil Index (and possibly a fetched Path), and a load abandoned between
	// Preload and Load reaches here. HnswModel.Destroy nil-checks the handle and also releases
	// the on-disk file and buffer, matching what LoadIndex's error path already does.
	for _, idx := range s.Indexes {
		idx.Destroy()
	}
	s.Indexes = nil
}

// load metadata from database
func LoadMetadata[T types.RealNumbers](sqlproc *sqlexec.SqlProcess, dbname string, metatbl string) ([]*HnswModel[T], error) {

	sql := fmt.Sprintf("SELECT * FROM %s ORDER BY timestamp ASC", sqlquote.QualifiedIdent(dbname, metatbl))
	res, err := runSql(sqlproc, sql)
	if err != nil {
		return nil, err
	}
	defer res.Close()

	total := 0
	for _, bat := range res.Batches {
		total += bat.RowCount()
	}

	indexes := make([]*HnswModel[T], 0, total)
	for _, bat := range res.Batches {
		idVec := bat.Vecs[0]
		chksumVec := bat.Vecs[1]
		tsVec := bat.Vecs[2]
		fsVec := bat.Vecs[3]
		for i := 0; i < bat.RowCount(); i++ {
			id := idVec.GetStringAt(i)
			chksum := chksumVec.GetStringAt(i)
			ts := vector.GetFixedAtWithTypeCheck[int64](tsVec, i)
			fs := vector.GetFixedAtWithTypeCheck[int64](fsVec, i)

			idx := &HnswModel[T]{Id: id, Checksum: chksum, Timestamp: ts, FileSize: fs}
			// nrow and build_ts were appended after the original four columns, and the
			// metadata table is created per index at CREATE INDEX -- REINDEX rewrites its rows,
			// not the table -- so an index created before they existed still has four. Read
			// them only when the batch actually carries them; absent means unknown.
			if len(bat.Vecs) > 4 {
				idx.Nrow = vector.GetFixedAtWithTypeCheck[int64](bat.Vecs[4], i)
			}
			if len(bat.Vecs) > 5 {
				idx.BuildTS = vector.GetFixedAtWithTypeCheck[int64](bat.Vecs[5], i)
			}
			indexes = append(indexes, idx)
		}
	}

	var rows, newest int64
	for _, idx := range indexes {
		rows += idx.Nrow
		if idx.BuildTS > newest {
			newest = idx.BuildTS
		}
	}
	logMetadataProvenance(metatbl, len(indexes), rows, newest)

	return indexes, nil
}

// load index from database
func (s *HnswSearch[T]) LoadIndex(sqlproc *sqlexec.SqlProcess, indexes []*HnswModel[T]) ([]*HnswModel[T], error) {
	var err error

	for _, idx := range indexes {
		err = idx.LoadIndexFromBuffer(sqlproc, s.Idxcfg, s.Tblcfg, s.ThreadsSearch, true)
		if err != nil {
			break
		}
	}

	if err != nil {
		for _, idx := range indexes {
			idx.Destroy()
		}
		return nil, err
	}

	return indexes, nil
}

// Preload reads the metadata rows, which carry each model's FileSize -- the cost the following
// Load will claim in host memory. The models are parked on s.Indexes unloaded, so GetIndexSize
// answers before a single model file is read.
func (s *HnswSearch[T]) Preload(sqlproc *sqlexec.SqlProcess) error {
	indexes, err := LoadMetadata[T](sqlproc, s.Tblcfg.DbName, s.Tblcfg.MetadataTable)
	if err != nil {
		return err
	}
	s.Indexes = indexes
	return nil
}

// load index from database (implement VectorIndexSearch.LoadFromDatabase)
func (s *HnswSearch[T]) Load(sqlproc *sqlexec.SqlProcess) error {
	// Metadata was read by Preload; a caller that skipped it still gets a correct load.
	indexes := s.Indexes
	if indexes == nil {
		var err error
		if indexes, err = LoadMetadata[T](sqlproc, s.Tblcfg.DbName, s.Tblcfg.MetadataTable); err != nil {
			return err
		}
	}

	if len(indexes) > 0 {
		// load index model
		var err error
		indexes, err = s.LoadIndex(sqlproc, indexes)
		if err != nil {
			s.Indexes = nil
			return err
		}
	}

	s.Indexes = indexes

	// Capture the generation + durable handles for IsStale. Same txn as the load, so the captured
	// generation matches the loaded snapshot. If capture fails here, genValid stays false and
	// IsStale reports the entry as uncheckable-hence-stale (evict + reload to retry capture)
	// rather than pinning it forever — see IsStale.
	s.cnUUID = sqlproc.GetService()
	if acc, e := sqlproc.GetAccountID(); e == nil {
		if fp, count, e2 := loadHnswGeneration(sqlproc, s.Tblcfg); e2 == nil {
			s.accountID, s.loadedFp, s.loadedCount, s.genValid = acc, fp, count, true
		}
	}
	return nil
}

// hnswViewedBytesPerRow is the HOST cost of one row in a VIEWED (mmap'd) usearch index: the
// per-node bookkeeping usearch keeps outside the mapping (limits_.members * sizeof(node_t)).
//
// Measured against usearch's own memory_usage(), which is what GetIndexSize charges once the
// index is loaded. It is exactly linear in the row count and completely independent of
// dimension -- at 20k rows, dim 32 and dim 512 report the identical 161,536 bytes while the
// model file grows 8x:
//
//	n=5000  dim=32   file=1.4MB   viewed=41536
//	n=5000  dim=512  file=11.0MB  viewed=41536
//	n=20000 dim=32   file=5.5MB   viewed=161536
//	n=20000 dim=512  file=43.9MB  viewed=161536
//	n=50000 dim=128  file=33.0MB  viewed=401536
//
// The deltas are 120000/15000 and 240000/30000 -- 8.000 bytes per row, over a fixed ~1536-byte
// per-thread-context term that is not worth modelling.
const hnswViewedBytesPerRow = 8

// logMetadataProvenance reports what the metadata rows say a loaded index is: how many source
// rows its generations cover, and the newest data version they were built from. build_ts is 0
// for a generation written before the column existed, and for a fulltext2 MERGE, whose content
// has no single source version -- both print as "unknown" rather than as an epoch timestamp.
//
// This is the read side of the provenance columns: without it nrow/build_ts are written and
// never surfaced, and an operator asking "how far behind is this resident index?" has to query
// the hidden metadata table by hand.
func logMetadataProvenance(metatbl string, count int, rows, buildTS int64) {
	if buildTS <= 0 {
		logutil.Infof("%s: loaded %d generation(s), rows=%d, build_ts=unknown", metatbl, count, rows)
		return
	}
	logutil.Infof("%s: loaded %d generation(s), rows=%d, build_ts=%d", metatbl, count, rows, buildTS)
}

// GetIndexSize charges usearch's native bookkeeping PLUS the mapped model file.
//
// The mmap is reclaimable page cache rather than allocation, so usearch's own figure omits it:
// memory_usage() skips the node and vector bytes whenever viewed_file_ is set (index.hpp:
// `if (!viewed_file_)`), reporting ~1.2% of the file. But it is not free, and it is not only
// page cache. LoadIndexFromBuffer unlinks the spill file once View() has mapped it, so the
// inode's blocks are held on the LOCAL fileservice volume until Destroy -- and since residency
// is bounded by BYTES alone, the omission is what bounds admission. Charging only the
// bookkeeping lets one client hold N whole models for N distinct named-snapshot timestamps
// while the governor sees a hundredth of them, filling the CN's scratch mount under a cap that
// looks satisfied.
//
// So the file is charged too. It over-states heap, which is the honest trade: the number the
// governor acts on is "bytes this entry keeps the CN from reusing", and for a viewed hnsw model
// that is dominated by the mapping, not by the allocator.
//
// Existing four-column metadata cannot estimate this before Load, so the charge lands after
// materialization. No catalog migration is introduced just to estimate a cache entry.
func (s *HnswSearch[T]) GetIndexSize() (hostBytes, deviceBytes int64) {
	for _, idx := range s.Indexes {
		if idx == nil {
			continue
		}
		// nrow*8 + FileSize: the allocation, plus the mapping that dominates it.
		//
		// Measured on a viewed index (100k rows, dim 128, 63 MB file): usearch MemoryUsage
		// reports 0.76 MB, while the process's VmRSS grows 67.25 MB -- 107% of the file. The
		// mapping is nominally reclaimable page cache, but reclaiming it makes the next search
		// fault the graph back off disk, so it is resident for as long as the entry is useful.
		// Charging the allocation alone under-states the entry by ~88x. Page tables are a
		// third, far smaller cost (VmPTE grew 0.14 MB, 0.22% of the file, per ADDRESS SPACE not
		// per thread), which FileSize covers by a wide margin.
		//
		// The same formula before and after load. Both terms come from the metadata row, so the
		// pre-load reservation and the post-load charge are the SAME number -- the property the
		// admission decision needs, since a reservation that under-states the charge lets a load
		// blow a budget the pass just declared satisfied. usearch's MemoryUsage() is
		// deliberately not consulted: at 100k and 200k rows it agrees with nrow*8 to within
		// 0.4%, so asking it buys nothing and would make the two paths disagree.
		//
		// A generation written before the nrow column existed contributes 0 for the allocation
		// term; its mapping is still charged.
		hostBytes += idx.Nrow*hnswViewedBytesPerRow + max(idx.FileSize, 0)
	}
	return hostBytes, 0
}

// IsStale reports whether the loaded model has fallen behind persisted data.
// Runs on the housekeeping goroutine via a background auto-commit txn (cnUUID/accountID captured
// at load). A query error ⇒ (true, err): the index was likely dropped/rebuilt, so reclaim the
// dead entry. No captured generation (capture failed at load, or no service to re-query) ⇒
// (true, nil): the entry cannot self-check freshness, so evict it to force a reload that retries
// capture. Returning (false, nil) here would pin a hot entry — whose TTL keeps sliding on every
// search — in cache indefinitely, serving pre-CDC/rebuild data forever; the bounded reload (one
// per freshness sweep) self-heals the moment capture succeeds.
func (s *HnswSearch[T]) IsStale() (bool, error) {
	if !s.genValid || s.cnUUID == "" {
		return true, nil
	}
	ctx, cancel := context.WithTimeout(context.Background(), time.Minute)
	defer cancel()
	fp, count, err := queryHnswGeneration(ctx, s.cnUUID, s.accountID, s.Tblcfg)
	if err != nil {
		return true, err
	}
	return fp != s.loadedFp || count != s.loadedCount, nil
}

// check config and update some parameters such as ef_search
func (s *HnswSearch[T]) SearchFloat32(proc *sqlexec.SqlProcess, query any, rt vectorindex.RuntimeConfig, outKeys []int64, outDists []float32) error {
	keys, dists, err := s.Search(proc, query, rt)
	if err != nil {
		return err
	}
	if keys == nil {
		return nil
	}
	switch ks := keys.(type) {
	case []int64:
		copy(outKeys, ks)
	case []any:
		for i, k := range ks {
			outKeys[i] = k.(int64)
		}
	default:
		return moerr.NewInternalErrorNoCtx("HnswSearch: unknown keys type")
	}
	for i, d := range dists {
		outDists[i] = float32(d)
	}
	return nil
}

// SearchInto is not yet implemented for this algo (box-free LIMIT path); it will migrate
// from the []any Search per the SearchOutput plan. Mirrors fulltext2's SearchFloat32 stub.
func (s *HnswSearch[T]) SearchInto(_ *sqlexec.SqlProcess, _ any, _ vectorindex.RuntimeConfig, _ *vectorindex.SearchOutput) error {
	return moerr.NewInternalErrorNoCtx("SearchInto not supported")
}
