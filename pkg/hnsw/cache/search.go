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

package cache

import (
	"errors"
	"fmt"
	"os"
	"sync"
	"time"

	"github.com/matrixorigin/matrixone/pkg/common/moerr"
	moruntime "github.com/matrixorigin/matrixone/pkg/common/runtime"
	"github.com/matrixorigin/matrixone/pkg/container/vector"
	"github.com/matrixorigin/matrixone/pkg/hnsw"
	"github.com/matrixorigin/matrixone/pkg/util/executor"
	"github.com/matrixorigin/matrixone/pkg/vm/process"
	usearch "github.com/unum-cloud/usearch/golang"
)

type HnswSearchIndex struct {
	Id        int64
	Path      string
	Index     *usearch.Index
	Timestamp int64
	Checksum  string
}

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
		data := bat.Vecs[0].GetRawBytesAt(i)
		_, err = fp.Write(data)
		if err != nil {
			return false, err
		}
	}
	return false, nil
}

func (idx *HnswSearchIndex) LoadIndex(proc *process.Process, idxcfg hnsw.IndexConfig, tblcfg hnsw.IndexTableConfig) error {

	stream_chan := make(chan executor.Result, 2)
	error_chan := make(chan error)

	sql := fmt.Sprintf("SELECT data from `%s`.`%s` WHERE index_id = %d ORDER BY chunk_id ASC", tblcfg.DbName, tblcfg.IndexTable, idx.Id)

	go func() {
		_, err := runSql_streaming(proc, sql, stream_chan, error_chan)
		if err != nil {
			error_chan <- err
			return
		}
	}()

	// create tempfile for writing
	fp, err := os.CreateTemp("", "hnswindx")
	if err != nil {
		return err
	}
	defer os.Remove(fp.Name())

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
	chksum, err := hnsw.CheckSum(fp.Name())
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

	err = usearchidx.Load(fp.Name())
	if err != nil {
		return err
	}

	idx.Index = usearchidx

	return nil
}

func (idx *HnswSearchIndex) Search(query []float32, limit uint) (keys []usearch.Key, distances []float32, err error) {
	return idx.Index.Search(query, limit)
}

type HnswSearch struct {
	VectorIndexSearch
	Indexes []*HnswSearchIndex
}

func (h *HnswSearch) Search(query []float32, limit uint) (keys []int64, distances []float32, err error) {
	h.Mutex.RLock()
	defer h.Mutex.RUnlock()
	if h.Indexes == nil {
		return nil, nil, moerr.NewInternalErrorNoCtx("HNSW cannot find index from database.")
	}

	ts := time.Now().Add(VectorIndexCacheTTL).UnixMicro()
	h.ExpireAt.Store(ts)

	// search
	size := len(h.Indexes) * int(limit)
	heap := hnsw.NewSearchResultSafeHeap(size)
	var wg sync.WaitGroup

	var errs error

	for _, idx := range h.Indexes {
		wg.Add(1)

		go func() {
			defer wg.Done()
			keys, distances, err := idx.Search(query, limit)
			if err != nil {
				errs = errors.Join(errs, err)
				return
			}

			for i := range keys {
				heap.Push(int64(keys[i]), distances[i])
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
		os.Stderr.WriteString(fmt.Sprintf("i=%d Key %d distance = %f\n", i, key, distance))
	}

	return reskeys, resdistances, nil
}

func (h *HnswSearch) Destroy() {
	h.Mutex.Lock()
	defer h.Mutex.Unlock()
	// destroy index
	for _, idx := range h.Indexes {
		idx.Index.Destroy()
	}
	h.Indexes = nil
	os.Stderr.WriteString("hnsw search destroy\n")
}

func (h *HnswSearch) Expired() bool {
	h.Mutex.RLock()
	defer h.Mutex.RUnlock()

	ts := h.ExpireAt.Load()
	now := time.Now().UnixMicro()
	return (ts > 0 && ts < now)
}

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
		for i := 0; i < bat.RowCount(); i++ {
			id := vector.GetFixedAtWithTypeCheck[int64](idVec, i)
			chksum := chksumVec.GetStringAt(i)
			ts := vector.GetFixedAtWithTypeCheck[int64](tsVec, i)

			idx := &HnswSearchIndex{Id: id, Checksum: chksum, Timestamp: ts}
			indexes = append(indexes, idx)
			os.Stderr.WriteString(fmt.Sprintf("Meta %d %d %s\n", id, ts, chksum))
		}
	}

	return indexes, nil
}

func (s *HnswSearch) LoadIndex(proc *process.Process, indexes []*HnswSearchIndex) ([]*HnswSearchIndex, error) {

	for _, idx := range indexes {
		err := idx.LoadIndex(proc, s.Idxcfg, s.Tblcfg)
		if err != nil {
			return nil, err
		}
	}
	return indexes, nil
}

func (s *HnswSearch) LoadFromDatabase(proc *process.Process) error {
	s.Mutex.Lock()
	defer s.Mutex.Unlock()

	// load metadata
	indexes, err := s.LoadMetadata(proc)
	if err != nil {
		return err
	}

	// load index model
	indexes, err = s.LoadIndex(proc, indexes)
	if err != nil {
		return err
	}

	s.Indexes = indexes

	now := time.Now()
	s.LastUpdate.Store(now.UnixMicro())
	ts := now.Add(time.Duration(VectorIndexCacheTTL)).UnixMicro()
	s.ExpireAt.Store(ts)

	return nil
}

var runSql = runSql_fn

// run SQL in batch mode. Result batches will stored in memory and return once all result batches received.
func runSql_fn(proc *process.Process, sql string) (executor.Result, error) {
	v, ok := moruntime.ServiceRuntime(proc.GetService()).GetGlobalVariables(moruntime.InternalSQLExecutor)
	if !ok {
		panic("missing lock service")
	}
	exec := v.(executor.SQLExecutor)
	opts := executor.Options{}.
		// All runSql and runSqlWithResult is a part of input sql, can not incr statement.
		// All these sub-sql's need to be rolled back and retried en masse when they conflict in pessimistic mode
		WithDisableIncrStatement().
		WithTxn(proc.GetTxnOperator()).
		WithDatabase(proc.GetSessionInfo().Database).
		WithTimeZone(proc.GetSessionInfo().TimeZone).
		WithAccountID(proc.GetSessionInfo().AccountId)
	return exec.Exec(proc.GetTopContext(), sql, opts)
}

var runSql_streaming = runSql_streaming_fn

// run SQL in WithStreaming() and pass the channel to SQL executor
func runSql_streaming_fn(proc *process.Process, sql string, stream_chan chan executor.Result, error_chan chan error) (executor.Result, error) {
	v, ok := moruntime.ServiceRuntime(proc.GetService()).GetGlobalVariables(moruntime.InternalSQLExecutor)
	if !ok {
		panic("missing lock service")
	}
	exec := v.(executor.SQLExecutor)
	opts := executor.Options{}.
		// All runSql and runSqlWithResult is a part of input sql, can not incr statement.
		// All these sub-sql's need to be rolled back and retried en masse when they conflict in pessimistic mode
		WithDisableIncrStatement().
		WithTxn(proc.GetTxnOperator()).
		WithDatabase(proc.GetSessionInfo().Database).
		WithTimeZone(proc.GetSessionInfo().TimeZone).
		WithAccountID(proc.GetSessionInfo().AccountId).
		WithStreaming(stream_chan, error_chan)
	return exec.Exec(proc.GetTopContext(), sql, opts)
}
