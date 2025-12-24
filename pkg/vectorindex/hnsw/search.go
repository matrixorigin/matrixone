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
	"sync"
	"sync/atomic"

	"github.com/matrixorigin/matrixone/pkg/common/concurrent"
	"github.com/matrixorigin/matrixone/pkg/common/moerr"
	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/container/vector"
	"github.com/matrixorigin/matrixone/pkg/vectorindex"
	"github.com/matrixorigin/matrixone/pkg/vectorindex/cache"
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

	s.lock()
	defer s.unlock()

	// search
	size := len(s.Indexes) * int(limit)
	heap := vectorindex.NewSearchResultSafeHeap(size)

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

				keys, distances, err2 := subindex[j].Search(query, limit)
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

	reskeys := make([]int64, 0, limit)
	resdistances := make([]float64, 0, limit)

	n := heap.Len()
	for i := 0; i < int(limit) && i < n; i++ {
		srif := heap.Pop()
		sr, ok := srif.(*vectorindex.SearchResult)
		if !ok {
			return nil, nil, moerr.NewInternalError(sqlproc.GetContext(), "heap return key is not int64")
		}
		reskeys = append(reskeys, sr.Id)
		sr.Distance = metric.DistanceTransformHnsw(sr.Distance, metric.DistFuncNameToMetricType[rt.OrigFuncName], s.Idxcfg.Usearch.Metric)
		resdistances = append(resdistances, sr.Distance)
	}

	return reskeys, resdistances, nil
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
	// destroy index
	for _, idx := range s.Indexes {
		idx.Index.Destroy()
	}
	s.Indexes = nil
}

// load metadata from database
func LoadMetadata[T types.RealNumbers](sqlproc *sqlexec.SqlProcess, dbname string, metatbl string) ([]*HnswModel[T], error) {

	sql := fmt.Sprintf("SELECT * FROM `%s`.`%s` ORDER BY timestamp ASC", dbname, metatbl)
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
			indexes = append(indexes, idx)
		}
	}

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

// load index from database (implement VectorIndexSearch.LoadFromDatabase)
func (s *HnswSearch[T]) Load(sqlproc *sqlexec.SqlProcess) error {
	// load metadata
	indexes, err := LoadMetadata[T](sqlproc, s.Tblcfg.DbName, s.Tblcfg.MetadataTable)
	if err != nil {
		return err
	}

	if len(indexes) > 0 {
		// load index model
		indexes, err = s.LoadIndex(sqlproc, indexes)
		if err != nil {
			return err
		}
	}

	s.Indexes = indexes

	return nil
}

// check config and update some parameters such as ef_search
func (s *HnswSearch[T]) UpdateConfig(newalgo cache.VectorIndexSearchIf) error {
	return nil
}
