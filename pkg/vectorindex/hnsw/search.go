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
	"errors"
	"fmt"
	"io"
	"os"
	"runtime"
	"sync"
	"sync/atomic"

	"github.com/detailyang/go-fallocate"
	"github.com/matrixorigin/matrixone/pkg/common/moerr"
	moruntime "github.com/matrixorigin/matrixone/pkg/common/runtime"
	"github.com/matrixorigin/matrixone/pkg/container/vector"
	"github.com/matrixorigin/matrixone/pkg/defines"
	"github.com/matrixorigin/matrixone/pkg/util/executor"
	"github.com/matrixorigin/matrixone/pkg/vectorindex"
	"github.com/matrixorigin/matrixone/pkg/vectorindex/cache"
	"github.com/matrixorigin/matrixone/pkg/vm/process"

	usearch "github.com/unum-cloud/usearch/golang"
)

var MaxUSearchThreads = int32(runtime.NumCPU())

// Hnsw search index struct to hold the usearch index
type HnswSearchIndex struct {
	Id        int64
	Path      string
	Index     *usearch.Index
	Timestamp int64
	Checksum  string
	Filesize  int64
}

// This is the HNSW search implementation that implement VectorIndexSearchIf interface
type HnswSearch struct {
	Idxcfg      vectorindex.IndexConfig
	Tblcfg      vectorindex.IndexTableConfig
	Indexes     []*HnswSearchIndex
	Concurrency atomic.Int32
	Mutex       sync.Mutex
	Cond        *sync.Cond
}

// load chunk from database
func (idx *HnswSearchIndex) loadChunk(proc *process.Process, stream_chan chan executor.Result, error_chan chan error, fp *os.File) (stream_closed bool, err error) {
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
	}

	bat := res.Batches[0]
	defer res.Close()

	for i := 0; i < bat.RowCount(); i++ {
		chunk_id := vector.GetFixedAtWithTypeCheck[int64](bat.Vecs[0], i)
		data := bat.Vecs[1].GetRawBytesAt(i)

		offset := chunk_id * vectorindex.MaxChunkSize
		_, err = fp.Seek(offset, io.SeekStart)
		if err != nil {
			return false, err
		}

		_, err = fp.Write(data)
		if err != nil {
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
func (idx *HnswSearchIndex) LoadIndex(proc *process.Process, idxcfg vectorindex.IndexConfig, tblcfg vectorindex.IndexTableConfig) error {

	stream_chan := make(chan executor.Result, 2)
	error_chan := make(chan error)

	// create tempfile for writing
	fp, err := os.CreateTemp("", "hnswindx")
	if err != nil {
		return err
	}
	defer os.Remove(fp.Name())

	err = fallocate.Fallocate(fp, 0, idx.Filesize)
	if err != nil {
		return err
	}

	// run streaming sql
	sql := fmt.Sprintf("SELECT chunk_id, data from `%s`.`%s` WHERE index_id = %d", tblcfg.DbName, tblcfg.IndexTable, idx.Id)
	go func() {
		_, err := runSql_streaming(proc, sql, stream_chan, error_chan)
		if err != nil {
			error_chan <- err
			return
		}
	}()

	// incremental load from database
	sql_closed := false
	for !sql_closed {
		sql_closed, err = idx.loadChunk(proc, stream_chan, error_chan, fp)
		if err != nil {
			return err
		}
	}

	// load index to memory
	fp.Close()

	// check checksum
	chksum, err := vectorindex.CheckSum(fp.Name())
	if err != nil {
		return err
	}
	if chksum != idx.Checksum {
		return moerr.NewInternalError(proc.Ctx, "Checksum mismatch with the index file")
	}

	usearchidx, err := usearch.NewIndex(idxcfg.Usearch)
	if err != nil {
		return err
	}

	err = usearchidx.ChangeThreadsSearch(uint(MaxUSearchThreads))
	if err != nil {
		return err
	}

	err = usearchidx.Load(fp.Name())
	if err != nil {
		return err
	}

	idx.Index = usearchidx

	return nil
}

// Call usearch.Search
func (idx *HnswSearchIndex) Search(query []float32, limit uint) (keys []usearch.Key, distances []float32, err error) {
	return idx.Index.Search(query, limit)
}

func NewHnswSearch(idxcfg vectorindex.IndexConfig, tblcfg vectorindex.IndexTableConfig) *HnswSearch {
	s := &HnswSearch{Idxcfg: idxcfg, Tblcfg: tblcfg}
	s.Cond = sync.NewCond(&s.Mutex)
	return s
}

// acquire lock from a usearch threads
func (s *HnswSearch) lock() {
	// check max threads
	s.Cond.L.Lock()
	defer s.Cond.L.Unlock()
	for s.Concurrency.Load() >= MaxUSearchThreads {
		s.Cond.Wait()
	}
	s.Concurrency.Add(1)
}

// release a lock from a usearch threads
func (s *HnswSearch) unlock() {
	s.Concurrency.Add(int32(-1))
	s.Cond.Signal()
}

// Search the hnsw index (implement VectorIndexSearch.Search)
func (s *HnswSearch) Search(query []float32, limit uint) (keys []int64, distances []float32, err error) {
	if len(s.Indexes) == 0 {
		return []int64{}, []float32{}, nil
	}

	s.lock()
	defer s.unlock()

	// search
	size := len(s.Indexes) * int(limit)
	heap := vectorindex.NewSearchResultSafeHeap(size)
	var wg sync.WaitGroup

	var errs error

	nthread := runtime.NumCPU()
	if nthread > len(s.Indexes) {
		nthread = len(s.Indexes)
	}

	for i := 0; i < nthread; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for j, idx := range s.Indexes {
				if j%nthread == i {
					keys, distances, err := idx.Search(query, limit)
					if err != nil {
						errs = errors.Join(errs, err)
						return
					}

					for k := range keys {
						heap.Push(int64(keys[k]), distances[k])
					}
				}
			}
		}()
	}

	wg.Wait()

	if errs != nil {
		return nil, nil, errs
	}

	reskeys := make([]int64, 0, limit)
	resdistances := make([]float32, 0, limit)

	n := heap.Len()
	for i := 0; i < int(limit) && i < n; i++ {
		key, distance := heap.Pop()
		reskeys = append(reskeys, key)
		resdistances = append(resdistances, distance)
		//os.Stderr.WriteString(fmt.Sprintf("i=%d Key %d distance = %f\n", i, key, distance))
	}

	return reskeys, resdistances, nil
}

// Destroy HnswSearch (implement VectorIndexSearch.Destroy)
func (s *HnswSearch) Destroy() {
	// destroy index
	for _, idx := range s.Indexes {
		idx.Index.Destroy()
	}
	s.Indexes = nil
	os.Stderr.WriteString("hnsw search destroy\n")
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
			id := vector.GetFixedAtWithTypeCheck[int64](idVec, i)
			chksum := chksumVec.GetStringAt(i)
			ts := vector.GetFixedAtWithTypeCheck[int64](tsVec, i)
			fs := vector.GetFixedAtWithTypeCheck[int64](fsVec, i)

			idx := &HnswSearchIndex{Id: id, Checksum: chksum, Timestamp: ts, Filesize: fs}
			indexes = append(indexes, idx)
			os.Stderr.WriteString(fmt.Sprintf("Meta %d %d %d %s\n", id, ts, fs, chksum))
		}
	}

	return indexes, nil
}

// load index from database
func (s *HnswSearch) LoadIndex(proc *process.Process, indexes []*HnswSearchIndex) ([]*HnswSearchIndex, error) {

	for _, idx := range indexes {
		err := idx.LoadIndex(proc, s.Idxcfg, s.Tblcfg)
		if err != nil {
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
		indexes, err = s.LoadIndex(proc, indexes)
		if err != nil {
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

var runSql = runSql_fn

// run SQL in batch mode. Result batches will stored in memory and return once all result batches received.
func runSql_fn(proc *process.Process, sql string) (executor.Result, error) {
	v, ok := moruntime.ServiceRuntime(proc.GetService()).GetGlobalVariables(moruntime.InternalSQLExecutor)
	if !ok {
		panic("missing lock service")
	}

	//-------------------------------------------------------
	topContext := proc.GetTopContext()
	accountId, err := defines.GetAccountId(proc.Ctx)
	if err != nil {
		return executor.Result{}, err
	}
	//-------------------------------------------------------

	exec := v.(executor.SQLExecutor)
	opts := executor.Options{}.
		// All runSql and runSqlWithResult is a part of input sql, can not incr statement.
		// All these sub-sql's need to be rolled back and retried en masse when they conflict in pessimistic mode
		WithDisableIncrStatement().
		WithTxn(proc.GetTxnOperator()).
		WithDatabase(proc.GetSessionInfo().Database).
		WithTimeZone(proc.GetSessionInfo().TimeZone).
		WithAccountID(accountId)
	return exec.Exec(topContext, sql, opts)
}

var runSql_streaming = runSql_streaming_fn

// run SQL in WithStreaming() and pass the channel to SQL executor
func runSql_streaming_fn(proc *process.Process, sql string, stream_chan chan executor.Result, error_chan chan error) (executor.Result, error) {
	v, ok := moruntime.ServiceRuntime(proc.GetService()).GetGlobalVariables(moruntime.InternalSQLExecutor)
	if !ok {
		panic("missing lock service")
	}

	//-------------------------------------------------------
	topContext := proc.GetTopContext()
	accountId, err := defines.GetAccountId(proc.Ctx)
	if err != nil {
		return executor.Result{}, err
	}
	//-------------------------------------------------------

	exec := v.(executor.SQLExecutor)
	opts := executor.Options{}.
		// All runSql and runSqlWithResult is a part of input sql, can not incr statement.
		// All these sub-sql's need to be rolled back and retried en masse when they conflict in pessimistic mode
		WithDisableIncrStatement().
		WithTxn(proc.GetTxnOperator()).
		WithDatabase(proc.GetSessionInfo().Database).
		WithTimeZone(proc.GetSessionInfo().TimeZone).
		WithAccountID(accountId).
		WithStreaming(stream_chan, error_chan)
	return exec.Exec(topContext, sql, opts)
}
