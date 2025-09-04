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
	"errors"
	"fmt"
	"io"
	"os"
	"sync"
	"sync/atomic"

	"github.com/detailyang/go-fallocate"
	"github.com/matrixorigin/matrixone/pkg/common/moerr"
	"github.com/matrixorigin/matrixone/pkg/container/vector"
	"github.com/matrixorigin/matrixone/pkg/util/executor"
	"github.com/matrixorigin/matrixone/pkg/vectorindex"
	"github.com/matrixorigin/matrixone/pkg/vectorindex/cache"
	"github.com/matrixorigin/matrixone/pkg/vectorindex/sqlexec"
	"github.com/matrixorigin/matrixone/pkg/vm/process"

	usearch "github.com/unum-cloud/usearch/golang"
)

var runSql = sqlexec.RunSql
var runSql_streaming = sqlexec.RunStreamingSql

// Hnsw search index struct to hold the usearch index
type HnswSearchIndex struct {
	Id        string
	Path      string
	Index     *usearch.Index
	Timestamp int64
	Checksum  string
	Filesize  int64
}

// This is the HNSW search implementation that implement VectorIndexSearchIf interface
type HnswSearch struct {
	Idxcfg        vectorindex.IndexConfig
	Tblcfg        vectorindex.IndexTableConfig
	Indexes       []*HnswSearchIndex
	Concurrency   atomic.Int64
	Mutex         sync.Mutex
	Cond          *sync.Cond
	ThreadsSearch int64
}

// load chunk from database
func (idx *HnswSearchIndex) loadChunk(
	ctx context.Context,
	proc *process.Process,
	stream_chan chan executor.Result,
	error_chan chan error,
	fp *os.File,
) (stream_closed bool, err error) {
	var res executor.Result
	var ok bool

	select {
	case res, ok = <-stream_chan:
		if !ok {
			return true, nil
		}
	case err = <-error_chan:
		return false, err
	case <-proc.Ctx.Done():
		return false, moerr.NewInternalError(proc.Ctx, "context cancelled")
	case <-ctx.Done():
		return false, moerr.NewInternalErrorf(ctx, "context cancelled: %v", ctx.Err())
	}

	bat := res.Batches[0]
	defer res.Close()

	chunkIds := vector.MustFixedColNoTypeCheck[int64](bat.Vecs[0])
	for i, chunkId := range chunkIds {
		data := bat.Vecs[1].GetRawBytesAt(i)
		offset := chunkId * vectorindex.MaxChunkSize

		if _, err = fp.Seek(offset, io.SeekStart); err != nil {
			return false, err
		}

		if _, err = fp.Write(data); err != nil {
			return false, err
		}
	}
	return false, nil
}

// load index from database
// TODO: loading file is tricky.
// 1. we need to know the size of the file.
// 2. Write Zero to file to have a pre-allocated size
// 3. SELECT chunk_id, data from index_table WHERE index_id = id.  Result will be out of order
// 4. according to the chunk_id, seek to the offset and write the chunk
// 5. check the checksum to verify the correctness of the file
func (idx *HnswSearchIndex) LoadIndex(
	proc *process.Process,
	idxcfg vectorindex.IndexConfig,
	tblcfg vectorindex.IndexTableConfig,
	nthread int64,
) (err error) {

	var (
		fp          *os.File
		stream_chan = make(chan executor.Result, 2)
		error_chan  = make(chan error)
	)

	// create tempfile for writing
	if fp, err = os.CreateTemp("", "hnswindx"); err != nil {
		return
	}
	fname := fp.Name()
	defer func() {
		if fp != nil {
			fp.Close()
			fp = nil
		}
		os.Remove(fname)
	}()

	if err = fallocate.Fallocate(fp, 0, idx.Filesize); err != nil {
		return
	}

	// run streaming sql
	sql := fmt.Sprintf(
		"SELECT chunk_id, data from `%s`.`%s` WHERE index_id = '%s'",
		tblcfg.DbName, tblcfg.IndexTable, idx.Id,
	)

	ctx, cancel := context.WithCancelCause(proc.GetTopContext())
	defer cancel(nil)

	go func() {
		if _, err2 := runSql_streaming(
			ctx, proc, sql, stream_chan, error_chan,
		); err2 != nil {
			error_chan <- err2
			return
		}
	}()

	// incremental load from database
	sql_closed := false
	for !sql_closed {
		if sql_closed, err = idx.loadChunk(
			ctx, proc, stream_chan, error_chan, fp,
		); err != nil {
			// notify the producer to stop the sql streaming
			cancel(err)
			break
		}
	}

	// wait for the sql streaming to be closed. make sure all the remaining
	// results in stream_chan are closed.
	if !sql_closed {
		for res := range stream_chan {
			res.Close()
		}
	}

	if err == nil {
		// fetch potential remaining errors from error_chan
		select {
		case err = <-error_chan:
		default:
		}
	}

	if err != nil {
		return
	}

	fp.Close()
	fp = nil

	// check checksum
	var chksum string
	if chksum, err = vectorindex.CheckSum(fname); err != nil {
		return
	}
	if chksum != idx.Checksum {
		return moerr.NewInternalError(ctx, "Checksum mismatch with the index file")
	}

	var usearchidx *usearch.Index
	if usearchidx, err = usearch.NewIndex(idxcfg.Usearch); err != nil {
		return
	}

	if err = usearchidx.ChangeThreadsSearch(uint(nthread)); err != nil {
		return
	}

	if err = usearchidx.Load(fname); err != nil {
		return
	}

	idx.Index = usearchidx

	return
}

// Call usearch.Search
func (idx *HnswSearchIndex) Search(query []float32, limit uint) (keys []usearch.Key, distances []float32, err error) {
	return idx.Index.Search(query, limit)
}

func NewHnswSearch(idxcfg vectorindex.IndexConfig, tblcfg vectorindex.IndexTableConfig) *HnswSearch {
	nthread := vectorindex.GetConcurrency(tblcfg.ThreadsSearch)
	s := &HnswSearch{Idxcfg: idxcfg, Tblcfg: tblcfg, ThreadsSearch: nthread}
	s.Cond = sync.NewCond(&s.Mutex)
	return s
}

// acquire lock from a usearch threads
func (s *HnswSearch) lock() {
	// check max threads
	s.Cond.L.Lock()
	defer s.Cond.L.Unlock()
	for s.Concurrency.Load() >= s.ThreadsSearch {
		s.Cond.Wait()
	}
	s.Concurrency.Add(1)
}

// release a lock from a usearch threads
func (s *HnswSearch) unlock() {
	s.Concurrency.Add(-1)
	s.Cond.Signal()
}

// Search the hnsw index (implement VectorIndexSearch.Search)
func (s *HnswSearch) Search(
	proc *process.Process, anyquery any, rt vectorindex.RuntimeConfig,
) (keys any, distances []float64, err error) {

	query, ok := anyquery.([]float32)
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
	var wg sync.WaitGroup

	var errs error

	nthread := int(vectorindex.GetConcurrency(0))
	if nthread > len(s.Indexes) {
		nthread = len(s.Indexes)
	}

	for i := 0; i < nthread; i++ {
		wg.Add(1)
		go func(tid int) {
			defer wg.Done()
			for j, idx := range s.Indexes {
				if j%nthread == tid {
					keys, distances, err := idx.Search(query, limit)
					if err != nil {
						errs = errors.Join(errs, err)
						return
					}

					for k := range keys {
						heap.Push(&vectorindex.SearchResult{Id: int64(keys[k]), Distance: float64(distances[k])})
					}
				}
			}
		}(i)
	}

	wg.Wait()

	if errs != nil {
		return nil, nil, errs
	}

	reskeys := make([]int64, 0, limit)
	resdistances := make([]float64, 0, limit)

	n := heap.Len()
	for i := 0; i < int(limit) && i < n; i++ {
		srif := heap.Pop()
		sr, ok := srif.(*vectorindex.SearchResult)
		if !ok {
			return nil, nil, moerr.NewInternalError(proc.Ctx, "heap return key is not int64")
		}
		reskeys = append(reskeys, sr.Id)
		resdistances = append(resdistances, sr.Distance)
	}

	return reskeys, resdistances, nil
}

func (s *HnswSearch) Contains(key int64) (bool, error) {
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
func (s *HnswSearch) Destroy() {
	// destroy index
	for _, idx := range s.Indexes {
		idx.Index.Destroy()
	}
	s.Indexes = nil
}

// load metadata from database
func (s *HnswSearch) LoadMetadata(proc *process.Process) ([]*HnswSearchIndex, error) {

	sql := fmt.Sprintf("SELECT * FROM `%s`.`%s`", s.Tblcfg.DbName, s.Tblcfg.MetadataTable)
	res, err := runSql(proc, sql)
	if err != nil {
		return nil, err
	}
	defer res.Close()

	total := 0
	for _, bat := range res.Batches {
		total += bat.RowCount()
	}

	indexes := make([]*HnswSearchIndex, 0, total)
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

			idx := &HnswSearchIndex{Id: id, Checksum: chksum, Timestamp: ts, Filesize: fs}
			indexes = append(indexes, idx)
		}
	}

	return indexes, nil
}

// load index from database
func (s *HnswSearch) LoadIndex(
	proc *process.Process, indexes []*HnswSearchIndex,
) ([]*HnswSearchIndex, error) {
	for _, idx := range indexes {
		if err := idx.LoadIndex(
			proc, s.Idxcfg, s.Tblcfg, s.ThreadsSearch,
		); err != nil {
			return nil, err
		}
	}
	return indexes, nil
}

// load index from database (implement VectorIndexSearch.LoadFromDatabase)
func (s *HnswSearch) Load(proc *process.Process) error {
	// load metadata
	indexes, err := s.LoadMetadata(proc)
	if err != nil {
		return err
	}

	if len(indexes) > 0 {
		// load index model
		if indexes, err = s.LoadIndex(proc, indexes); err != nil {
			return err
		}
	}

	s.Indexes = indexes

	return nil
}

// check config and update some parameters such as ef_search
func (s *HnswSearch) UpdateConfig(newalgo cache.VectorIndexSearchIf) error {

	return nil
}
