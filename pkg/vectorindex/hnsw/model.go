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
	"io"
	"math"
	"os"
	"strings"
	"sync/atomic"

	"github.com/detailyang/go-fallocate"
	"github.com/matrixorigin/matrixone/pkg/catalog"
	"github.com/matrixorigin/matrixone/pkg/common/moerr"
	"github.com/matrixorigin/matrixone/pkg/common/util"
	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/container/vector"
	"github.com/matrixorigin/matrixone/pkg/logutil"
	"github.com/matrixorigin/matrixone/pkg/util/executor"
	"github.com/matrixorigin/matrixone/pkg/vectorindex"
	"github.com/matrixorigin/matrixone/pkg/vm/process"
	usearch "github.com/unum-cloud/usearch/golang"
)

// HnswModel struct - This structure shares with Search, Build and Sync
type HnswModel[T types.RealNumbers] struct {
	Id       string
	Index    *usearch.Index
	Path     string
	FileSize int64

	// info required for build
	MaxCapacity uint

	// from metadata.  info required for search
	Timestamp int64
	Checksum  string

	// for cdc update
	Dirty atomic.Bool
	View  bool
	Len   atomic.Int64
}

// New HnswModel struct
func NewHnswModelForBuild[T types.RealNumbers](id string, cfg vectorindex.IndexConfig, nthread int, max_capacity uint) (*HnswModel[T], error) {
	var err error
	idx := &HnswModel[T]{}

	idx.Id = id

	idx.Index, err = usearch.NewIndex(cfg.Usearch)
	if err != nil {
		return nil, err
	}

	idx.MaxCapacity = max_capacity

	err = idx.Index.Reserve(idx.MaxCapacity)
	if err != nil {
		return nil, err
	}

	err = idx.Index.ChangeThreadsAdd(uint(nthread))
	if err != nil {
		return nil, err
	}

	err = idx.Index.ChangeThreadsSearch(uint(nthread))
	if err != nil {
		return nil, err
	}
	return idx, nil
}

// Destroy the struct
func (idx *HnswModel[T]) Destroy() error {
	if idx.Index != nil {
		err := idx.Index.Destroy()
		if err != nil {
			return err
		}
		idx.Index = nil
	}

	if len(idx.Path) > 0 {
		// remove the file
		if _, err := os.Stat(idx.Path); err == nil || os.IsExist(err) {
			err := os.Remove(idx.Path)
			if err != nil {
				return err
			}
		}
	}
	return nil
}

// Save the index to file
func (idx *HnswModel[T]) SaveToFile() error {

	if idx.Index == nil {
		// index is nil. ignore
		return nil
	}

	if !idx.Dirty.Load() {
		// nothing change. ignore
		return nil
	}

	// delete old file
	oldpath := idx.Path
	if len(oldpath) > 0 {
		// remove the file
		if _, err := os.Stat(oldpath); err == nil || os.IsExist(err) {
			err := os.Remove(oldpath)
			if err != nil {
				return err
			}
		}
	}
	idx.Path = ""

	empty, err := idx.Empty()
	if err != nil {
		return err
	}
	if empty {
		// index empty, no file need to save
		err = idx.Index.Destroy()
		if err != nil {
			return err
		}
		idx.Index = nil
		return nil
	}

	// save to file
	f, err := os.CreateTemp("", "hnsw")
	if err != nil {
		return err
	}

	err = idx.Index.Save(f.Name())
	if err != nil {
		os.Remove(f.Name())
		return err
	}

	// get new checksum
	chksum, err := vectorindex.CheckSum(f.Name())
	if err != nil {
		return err
	}
	idx.Checksum = chksum

	// free memory
	err = idx.Index.Destroy()
	if err != nil {
		return err
	}
	idx.Index = nil
	idx.Path = f.Name()

	return nil
}

// Generate the SQL to update the secondary index tables.
// 1. store the index file into the index table
func (idx *HnswModel[T]) ToSql(cfg vectorindex.IndexTableConfig) ([]string, error) {

	err := idx.SaveToFile()
	if err != nil {
		return nil, err
	}

	if len(idx.Path) == 0 {
		// file path is empty string. No file is written
		return []string{}, nil
	}

	fi, err := os.Stat(idx.Path)
	if err != nil {
		return nil, err
	}

	filesz := fi.Size()
	offset := int64(0)
	chunksz := int64(0)
	chunkid := int64(0)

	idx.FileSize = filesz

	if idx.FileSize == 0 {
		return []string{}, nil
	}

	logutil.Infof("HnswModel.ToSql idx %s, len = %d\n", idx.Id, idx.Len.Load())

	sqls := make([]string, 0, 5)

	sql := fmt.Sprintf("INSERT INTO `%s`.`%s` VALUES ", cfg.DbName, cfg.IndexTable)
	values := make([]string, 0, int64(math.Ceil(float64(filesz)/float64(vectorindex.MaxChunkSize))))
	n := 0
	for offset = 0; offset < filesz; {
		if offset+vectorindex.MaxChunkSize < filesz {
			chunksz = vectorindex.MaxChunkSize

		} else {
			chunksz = filesz - offset
		}

		url := fmt.Sprintf("file://%s?offset=%d&size=%d", idx.Path, offset, chunksz)
		tuple := fmt.Sprintf("('%s', %d, load_file(cast('%s' as datalink)), 0)", idx.Id, chunkid, url)
		values = append(values, tuple)

		// offset and chunksz
		offset += chunksz
		chunkid++

		n++
		if n == 2000 {
			newsql := sql + strings.Join(values, ", ")
			sqls = append(sqls, newsql)
			values = values[:0]
			n = 0
		}
	}

	if len(values) > 0 {
		newsql := sql + strings.Join(values, ", ")
		sqls = append(sqls, newsql)
	}

	//sql += strings.Join(values, ", ")
	//return []string{sql}, nil
	return sqls, nil
}

func (idx *HnswModel[T]) ToDeleteSql(cfg vectorindex.IndexTableConfig) ([]string, error) {
	sqls := make([]string, 0, 2)

	sql := fmt.Sprintf("DELETE FROM `%s`.`%s` WHERE %s = '%s'", cfg.DbName, cfg.IndexTable, catalog.Hnsw_TblCol_Storage_Index_Id, idx.Id)
	sqls = append(sqls, sql)
	sql = fmt.Sprintf("DELETE FROM `%s`.`%s` WHERE %s = '%s'", cfg.DbName, cfg.MetadataTable, catalog.Hnsw_TblCol_Metadata_Index_Id, idx.Id)
	sqls = append(sqls, sql)

	return sqls, nil
}

// is the index empty
func (idx *HnswModel[T]) Empty() (bool, error) {
	if idx.Index == nil {
		return false, moerr.NewInternalErrorNoCtx("usearch index is nil")
	}

	sz, err := idx.Index.Len()
	if err != nil {
		return false, err
	}
	return (sz == 0), nil
}

// check the index is full, i.e. 10K vectors
func (idx *HnswModel[T]) Full() (bool, error) {
	if idx.Index == nil {
		return false, moerr.NewInternalErrorNoCtx("usearch index is nil")
	}
	sz, err := idx.Index.Len()
	if err != nil {
		return false, err
	}
	return (sz == idx.MaxCapacity), nil
}

// add vector to the index
func (idx *HnswModel[T]) Add(key int64, vec []T) error {
	if idx.Index == nil {
		return moerr.NewInternalErrorNoCtx("usearch index is nil")
	}
	idx.Dirty.Store(true)
	idx.Len.Add(1)

	if vec == nil {
		return moerr.NewInternalErrorNoCtx("usearch query is nil")
	}

	dim, err := idx.Index.Dimensions()
	if err != nil {
		return err
	}

	if uint(len(vec)) != dim {
		return moerr.NewInternalErrorNoCtx("usearch dimension not match")
	}

	return idx.Index.AddUnsafe(uint64(key), util.UnsafePointer(&vec[0]))
}

// add vector without increment the counter.  concurrency add will increment the counter before Add
func (idx *HnswModel[T]) AddWithoutIncr(key int64, vec []T) error {
	if idx.Index == nil {
		return moerr.NewInternalErrorNoCtx("usearch index is nil")
	}
	idx.Dirty.Store(true)
	//idx.Len.Add(1)

	if vec == nil {
		return moerr.NewInternalErrorNoCtx("usearch query is nil")
	}

	dim, err := idx.Index.Dimensions()
	if err != nil {
		return err
	}

	if uint(len(vec)) != dim {
		return moerr.NewInternalErrorNoCtx("usearch dimension not match")
	}

	return idx.Index.AddUnsafe(uint64(key), util.UnsafePointer(&vec[0]))
}

// remove key
func (idx *HnswModel[T]) Remove(key int64) error {
	if idx.Index == nil {
		return moerr.NewInternalErrorNoCtx("usearch index is nil")
	}
	idx.Dirty.Store(true)
	idx.Len.Add(-1)
	return idx.Index.Remove(uint64(key))
}

// contains key
func (idx *HnswModel[T]) Contains(key int64) (found bool, err error) {
	if idx.Index == nil {
		return false, moerr.NewInternalErrorNoCtx("usearch index is nil")
	}
	return idx.Index.Contains(uint64(key))
}

// load chunk from database
func (idx *HnswModel[T]) loadChunk(ctx context.Context,
	proc *process.Process,
	stream_chan chan executor.Result,
	error_chan chan error,
	fp *os.File) (stream_closed bool, err error) {
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
func (idx *HnswModel[T]) LoadIndex(
	proc *process.Process,
	idxcfg vectorindex.IndexConfig,
	tblcfg vectorindex.IndexTableConfig,
	nthread int64,
	view bool) (err error) {

	var (
		fp          *os.File
		stream_chan = make(chan executor.Result, 2)
		error_chan  = make(chan error)
		fname       string
	)

	if idx.Index != nil {
		// index already loaded. ignore
		return nil
	}

	if len(idx.Path) == 0 {
		// create tempfile for writing
		fp, err = os.CreateTemp("", "hnsw")
		if err != nil {
			return err
		}

		fname = fp.Name()

		// load index to memory
		defer func() {
			if fp != nil {
				fp.Close()
				fp = nil
			}

			if view {
				// if view == true, remove the file.  right now view equals to read-only model when search.
				// since model loads into memory anyway, we can safely remove the file after load.
				// NOTE: when choose to load with usearch.View() mmap(), we cannot remove this file.
				// for update, we need this file for Load() and unload().
				if len(fname) > 0 {
					os.Remove(fname)
				}
			}
		}()

		err = fallocate.Fallocate(fp, 0, idx.FileSize)
		if err != nil {
			return err
		}

		// run streaming sql
		sql := fmt.Sprintf("SELECT chunk_id, data from `%s`.`%s` WHERE index_id = '%s'", tblcfg.DbName, tblcfg.IndexTable, idx.Id)

		ctx, cancel := context.WithCancelCause(proc.GetTopContext())
		defer cancel(nil)

		go func() {
			_, err2 := runSql_streaming(ctx, proc, sql, stream_chan, error_chan)
			if err2 != nil {
				error_chan <- err2
				return
			}
		}()

		// incremental load from database
		sql_closed := false
		for !sql_closed {
			sql_closed, err = idx.loadChunk(ctx, proc, stream_chan, error_chan, fp)
			if err != nil {
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

		idx.Path = fp.Name()
		fp.Close()
		fp = nil
	}

	// check checksum
	chksum, err := vectorindex.CheckSum(idx.Path)
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

	err = usearchidx.ChangeThreadsAdd(uint(nthread))
	if err != nil {
		return err
	}

	if view {
		err = usearchidx.Load(idx.Path)
		idx.View = true
	} else {
		err = usearchidx.Load(idx.Path)
		usearchidx.Reserve(uint(tblcfg.IndexCapacity))
	}
	if err != nil {
		return err
	}

	// always get the number of item and capacity when model loaded.
	idx.Index = usearchidx
	idxLen, err := idx.Index.Len()
	if err != nil {
		return err
	}
	idx.Len.Store(int64(idxLen))

	logutil.Debugf("HnswModel.LoadIndex idx %s, len = %d\n", idx.Id, idxLen)

	idx.MaxCapacity, err = idx.Index.Capacity()
	if err != nil {
		return err
	}

	if !view {
		// sometimes Reserve() will give bigger capacity than requested
		if idx.MaxCapacity > uint(tblcfg.IndexCapacity) {
			idx.MaxCapacity = uint(tblcfg.IndexCapacity)
		}
	}

	return nil
}

// unload
func (idx *HnswModel[T]) Unload() error {
	if idx.Index == nil {
		return moerr.NewInternalErrorNoCtx("usearch index is nil")
	}

	idxLen, err := idx.Index.Len()
	if err != nil {
		return err
	}
	logutil.Debugf("HnswModel.Unload idx %s, len = %d\n", idx.Id, idxLen)

	// SaveToFile will check Dirty bit. If dirty is true, save to file before unload
	err = idx.SaveToFile()
	if err != nil {
		return err
	}

	// SaveToFile will release the usearch index when dirty is true so always check nil index
	if idx.Index != nil {
		err := idx.Index.Destroy()
		if err != nil {
			return err
		}
		// reset variable
		idx.Index = nil
	}
	return nil
}

// Call usearch.Search
func (idx *HnswModel[T]) Search(query []T, limit uint) (keys []usearch.Key, distances []float32, err error) {
	if idx.Index == nil {
		return nil, nil, moerr.NewInternalErrorNoCtx("usearch index is nil")
	}

	if query == nil {
		return nil, nil, moerr.NewInternalErrorNoCtx("usearch query is nil")
	}

	dim, err := idx.Index.Dimensions()
	if err != nil {
		return nil, nil, err
	}

	if uint(len(query)) != dim {
		return nil, nil, moerr.NewInternalErrorNoCtx("usearch dimension not match")
	}

	return idx.Index.SearchUnsafe(util.UnsafePointer(&query[0]), limit)
}
