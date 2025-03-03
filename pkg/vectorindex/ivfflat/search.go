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

package ivfflat

import (
	"fmt"
	"os"
	"sync"
	"sync/atomic"

	"github.com/matrixorigin/matrixone/pkg/catalog"
	"github.com/matrixorigin/matrixone/pkg/common/moerr"
	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/container/vector"
	"github.com/matrixorigin/matrixone/pkg/util/executor"
	"github.com/matrixorigin/matrixone/pkg/vectorindex"
	"github.com/matrixorigin/matrixone/pkg/vectorindex/cache"
	"github.com/matrixorigin/matrixone/pkg/vectorindex/sqlexec"
	"github.com/matrixorigin/matrixone/pkg/vm/process"
)

var runSql = sqlexec.RunSql
var runSql_streaming = sqlexec.RunStreamingSql

type Centroid[T types.RealNumbers] struct {
	Id  int64
	Vec []T
}

// Ivf search index struct to hold the usearch index
type IvfflatSearchIndex[T types.RealNumbers] struct {
	Version   int64
	Centroids []Centroid[T]
}

// This is the Ivf search implementation that implement VectorIndexSearchIf interface
type IvfflatSearch[T types.RealNumbers] struct {
	Idxcfg        vectorindex.IndexConfig
	Tblcfg        vectorindex.IndexTableConfig
	Index         *IvfflatSearchIndex[T]
	Concurrency   atomic.Int64
	Mutex         sync.Mutex
	Cond          *sync.Cond
	ThreadsSearch int64
}

/*
// load chunk from database
func (idx *IvfflatSearchIndex) loadChunk(proc *process.Process, stream_chan chan executor.Result, error_chan chan error, fp *os.File) (stream_closed bool, err error) {
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
func (idx *IvfflatSearchIndex) LoadIndex(proc *process.Process, idxcfg vectorindex.IndexConfig, tblcfg vectorindex.IndexTableConfig, nthread int64) error {

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
	sql := fmt.Sprintf("SELECT chunk_id, data from `%s`.`%s` WHERE index_id = '%s'", tblcfg.DbName, tblcfg.IndexTable, idx.Id)
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

	err = usearchidx.ChangeThreadsSearch(uint(nthread))
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
*/

func (idx *IvfflatSearchIndex[T]) LoadIndex(proc *process.Process, idxcfg vectorindex.IndexConfig, tblcfg vectorindex.IndexTableConfig, nthread int64) error {

	idx.Version = idxcfg.Ivfflat.Version
	sql := fmt.Sprintf("SELECT `%s`, `%s` FROM `%s`.`%s` WHERE `%s` = %d",
		catalog.SystemSI_IVFFLAT_TblCol_Centroids_id,
		catalog.SystemSI_IVFFLAT_TblCol_Centroids_centroid,
		tblcfg.DbName, tblcfg.IndexTable,
		catalog.SystemSI_IVFFLAT_TblCol_Centroids_version,
		idxcfg.Ivfflat.Version)
	res, err := runSql(proc, sql)
	if err != nil {
		return err
	}
	defer res.Close()

	if len(res.Batches) == 0 {
		return nil
	}

	idx.Centroids = make([]Centroid[T], 0, idxcfg.Ivfflat.Lists)
	for _, bat := range res.Batches {
		idVec := bat.Vecs[0]
		faVec := bat.Vecs[1]
		for r := range bat.RowCount() {
			id := vector.GetFixedAtWithTypeCheck[int64](idVec, r)
			if faVec.IsNull(uint64(r)) {
				continue
			}
			vec := types.BytesToArray[T](faVec.GetBytesAt(r))
			copyvec := append(make([]T, 0, len(vec)), vec...)
			idx.Centroids = append(idx.Centroids, Centroid[T]{Id: id, Vec: copyvec})
		}
	}

	return nil
}

func (idx *IvfflatSearchIndex[T]) findCentroids(query []T, rt vectorindex.RuntimeConfig, nthread int64) []int64 {
	return []int64{0, 1, 2}
}

// Call usearch.Search
func (idx *IvfflatSearchIndex[T]) Search(proc *process.Process, idxcfg vectorindex.IndexConfig, tblcfg vectorindex.IndexTableConfig, query []T, rt vectorindex.RuntimeConfig, nthread int64) (keys any, distances []float64, err error) {

	stream_chan := make(chan executor.Result, nthread)
	error_chan := make(chan error, nthread)

	var instr string
	centroids_ids := idx.findCentroids(query, rt, nthread)
	for i, c := range centroids_ids {
		if i > 0 {
			instr += ","
		}
		instr += string(c)
	}

	sql := fmt.Sprintf("SELECT `%s`, `%s` FROM `%s`.`%s` WHERE `%s` = %d AND `%s` IN (%s)",
		catalog.SystemSI_IVFFLAT_TblCol_Entries_pk,
		catalog.SystemSI_IVFFLAT_TblCol_Entries_entry,
		tblcfg.DbName, tblcfg.EntriesTable,
		catalog.SystemSI_IVFFLAT_TblCol_Entries_version,
		idx.Version,
		catalog.SystemSI_IVFFLAT_TblCol_Entries_id,
		instr,
	)

	os.Stderr.WriteString(sql)
	go func() {
		_, err := runSql_streaming(proc, sql, stream_chan, error_chan)
		if err != nil {
			error_chan <- err
			return
		}
	}()

	return nil, nil, nil
}

func (idx *IvfflatSearchIndex[T]) Destroy() {
}

func NewIvfflatSearch[T types.RealNumbers](idxcfg vectorindex.IndexConfig, tblcfg vectorindex.IndexTableConfig) *IvfflatSearch[T] {
	nthread := vectorindex.GetConcurrency(tblcfg.ThreadsSearch)
	s := &IvfflatSearch[T]{Idxcfg: idxcfg, Tblcfg: tblcfg, ThreadsSearch: nthread}
	s.Cond = sync.NewCond(&s.Mutex)
	return s
}

// acquire lock from a usearch threads
func (s *IvfflatSearch[T]) lock() {
	// check max threads
	s.Cond.L.Lock()
	defer s.Cond.L.Unlock()
	for s.Concurrency.Load() >= s.ThreadsSearch {
		s.Cond.Wait()
	}
	s.Concurrency.Add(1)
}

// release a lock from a usearch threads
func (s *IvfflatSearch[T]) unlock() {
	s.Concurrency.Add(-1)
	s.Cond.Signal()
}

// Search the hnsw index (implement VectorIndexSearch.Search)
func (s *IvfflatSearch[T]) Search(proc *process.Process, anyquery any, rt vectorindex.RuntimeConfig) (keys any, distances []float64, err error) {
	query, ok := anyquery.([]T)
	if !ok {
		return nil, nil, moerr.NewInternalErrorNoCtx("IvfSearch: query not match with index type")
	}

	return s.Index.Search(proc, s.Idxcfg, s.Tblcfg, query, rt, s.ThreadsSearch)
}

func (s *IvfflatSearch[T]) Contains(key int64) (bool, error) {
	return true, nil
}

// Destroy IvfflatSearch (implement VectorIndexSearch.Destroy)
func (s *IvfflatSearch[T]) Destroy() {
	// destroy index
	if s.Index != nil {
		s.Index.Destroy()
	}
	s.Index = nil
}

// load index from database (implement VectorIndexSearch.LoadFromDatabase)
func (s *IvfflatSearch[T]) Load(proc *process.Process) error {

	idx := &IvfflatSearchIndex[T]{}
	// load index model
	err := idx.LoadIndex(proc, s.Idxcfg, s.Tblcfg, s.ThreadsSearch)
	if err != nil {
		return err
	}
	s.Index = idx
	return nil
}

// check config and update some parameters such as ef_search
func (s *IvfflatSearch[T]) UpdateConfig(newalgo cache.VectorIndexSearchIf) error {

	return nil
}
