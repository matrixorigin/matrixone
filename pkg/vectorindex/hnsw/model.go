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
	"math"
	"os"
	"runtime"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/detailyang/go-fallocate"
	"github.com/matrixorigin/matrixone/pkg/catalog"
	"github.com/matrixorigin/matrixone/pkg/common/moerr"
	"github.com/matrixorigin/matrixone/pkg/common/sqlquote"
	"github.com/matrixorigin/matrixone/pkg/common/util"
	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/container/vector"
	"github.com/matrixorigin/matrixone/pkg/logutil"
	"github.com/matrixorigin/matrixone/pkg/util/executor"
	"github.com/matrixorigin/matrixone/pkg/vectorindex"
	vimemory "github.com/matrixorigin/matrixone/pkg/vectorindex/memory"
	"github.com/matrixorigin/matrixone/pkg/vectorindex/sqlexec"
	usearch "github.com/unum-cloud/usearch/golang"
)

// HnswModel struct - This structure shares with Search, Build and Sync
type HnswModel[T types.RealNumbers] struct {
	Id       string
	Index    *usearch.Index
	Path     string
	FileSize int64

	// TmpDir is where this model's local file is created: the LOCAL fileservice scratch
	// volume, resolved once by whoever built the model (see hnswSpillDir). Empty means
	// $TMPDIR, which os.CreateTemp reads from "" -- the behaviour for callers with no
	// fileservice to reach, such as unit tests. Mirrors CagraModel/IvfpqModel.TmpDir.
	TmpDir string

	// info required for build
	MaxCapacity uint
	NThread     uint

	// inflight counts adds that have been ASSIGNED to this index (a slot reserved
	// under HnswBuild.mutex) but not yet completed. A concurrent capacity rollover
	// must wait for this to drain before SaveToFile() saves+destroys the index, so an
	// in-flight worker never adds to a destroyed usearch index or persists a partial
	// one. Build-only; unused for Search/Sync.
	inflight sync.WaitGroup

	// from metadata.  info required for search
	Timestamp int64
	Checksum  string

	// Nrow is the source rows this generation indexes and BuildTS is the transaction
	// SnapshotTS its content was built from. Both 0 when the metadata row predates the
	// columns -- read as unknown, never as "empty" or "built at the epoch".
	Nrow    int64
	BuildTS int64

	// for cdc update
	Dirty atomic.Bool
	View  bool
	Len   atomic.Int64

	// view memory byffer for search
	buffer []byte
}

// New HnswModel struct
func NewHnswModelForBuild[T types.RealNumbers](id string, cfg vectorindex.IndexConfig, nthread int, max_capacity uint, tmpdir string) (*HnswModel[T], error) {
	var err error
	idx := &HnswModel[T]{TmpDir: tmpdir}

	idx.Id = id
	idx.NThread = uint(nthread)
	idx.MaxCapacity = max_capacity

	err = idx.initIndex(cfg)
	if err != nil {
		return nil, err
	}

	return idx, nil
}

func (idx *HnswModel[T]) initIndex(cfg vectorindex.IndexConfig) (err error) {
	idx.Index, err = usearch.NewIndex(cfg.Usearch)
	if err != nil {
		return err
	}

	defer func() {
		if err != nil {
			if idx.Index != nil {
				idx.Index.Destroy()
				idx.Index = nil
			}
		}
	}()

	err = idx.Index.Reserve(idx.MaxCapacity)
	if err != nil {
		return err
	}

	err = idx.Index.ChangeThreadsAdd(idx.NThread)
	if err != nil {
		return err
	}

	err = idx.Index.ChangeThreadsSearch(idx.NThread)
	if err != nil {
		return err
	}

	return nil
}

// Destroy the struct
func (idx *HnswModel[T]) Destroy() error {
	// Release the index handle, the on-disk file and the buffer independently: they do
	// not depend on each other, so returning early on the first error used to leak the
	// remaining two for the lifetime of the process. Collect the outcomes instead.
	var errs error
	if idx.Index != nil {
		if err := idx.Index.Destroy(); err != nil {
			errs = errors.Join(errs, err)
		} else {
			idx.Index = nil
		}
	}

	if len(idx.Path) > 0 {
		// remove the file
		if _, err := os.Stat(idx.Path); err == nil || os.IsExist(err) {
			if err := os.Remove(idx.Path); err != nil {
				errs = errors.Join(errs, err)
			}
		}
		idx.Path = ""
	}

	if idx.buffer != nil {
		idx.buffer = nil
	}

	return errs
}

// Save the index to file
// saveToFileCheckSum computes the on-disk checksum in SaveToFile. Indirected as a var so tests
// can stub it to exercise the failure/cleanup path (#25630).
var saveToFileCheckSum = vectorindex.CheckSum

func (idx *HnswModel[T]) SaveToFile() error {

	if idx.Index == nil {
		// index is nil. ignore
		return nil
	}

	if idx.buffer != nil {
		// model is in memory buffer. ignore
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

	// Capture the vector count while the index is still alive: SaveToFile destroys the handle
	// below, and ToInsertSql needs the count for the metadata row.
	if n, lerr := idx.Index.Len(); lerr == nil {
		idx.Len.Store(int64(n))
	}

	empty, err := idx.Empty()
	if err != nil {
		return err
	}
	if empty {
		// index empty, no file need to save
		logutil.Infof("HnswModel.SaveToFile: empty index idx=%s, destroy only", idx.Id)
		err = idx.Index.Destroy()
		if err != nil {
			return err
		}
		idx.Index = nil
		return nil
	}

	// save to file, on the LOCAL fileservice volume when the builder resolved one
	f, err := os.CreateTemp(idx.TmpDir, "hnsw")
	if err != nil {
		return err
	}
	// os.CreateTemp opens the file; we only need its (now reserved) name — usearch's
	// Index.Save and CheckSum reopen the path themselves. Close our handle immediately,
	// otherwise every save leaks a file descriptor (#25630). The deferred cleanup removes the
	// temp file on any failure below (idx.Path is set to fpath only on success), so a partial
	// file is never orphaned on disk.
	fpath := f.Name()
	_ = f.Close()
	// destroyed indicates whether GPU/native memory has been freed; used by the
	// deferred cleanup to distinguish "save/checksum failed, tar is bogus, drop it"
	// from "save+checksum succeeded but Destroy failed, tar is VALID, keep it".
	destroyed := false
	defer func() {
		if idx.Path != fpath && !destroyed {
			os.Remove(fpath)
		}
	}()

	logutil.Infof("HnswModel.SaveToFile: idx=%s calling Save -> %s", idx.Id, fpath)
	t0 := time.Now()
	if err = idx.Index.Save(fpath); err != nil {
		logutil.Errorf("HnswModel.SaveToFile: Save FAILED idx=%s after %v: %v", idx.Id, time.Since(t0), err)
		return err
	}
	saveDur := time.Since(t0)
	fi, _ := os.Stat(fpath)
	savedBytes := int64(0)
	if fi != nil {
		savedBytes = fi.Size()
	}
	logutil.Infof("HnswModel.SaveToFile: Save done idx=%s in %v (%d bytes)", idx.Id, saveDur, savedBytes)

	// get new checksum
	chksum, err := saveToFileCheckSum(fpath)
	if err != nil {
		logutil.Errorf("HnswModel.SaveToFile: CheckSum FAILED idx=%s: %v", idx.Id, err)
		return err
	}
	idx.Checksum = chksum

	// Record the successfully-saved artifact BEFORE attempting Destroy. A Destroy
	// failure does not invalidate the on-disk file, and letting the deferred cleanup
	// remove it here would lose committed data.
	idx.Path = fpath

	// free memory
	if err = idx.Index.Destroy(); err != nil {
		logutil.Errorf("HnswModel.SaveToFile: Destroy FAILED idx=%s (file RETAINED at %s): %v", idx.Id, fpath, err)
		return err
	}
	destroyed = true
	idx.Index = nil

	// Do NOT set filesize here. filesize == 0 means file didn't save to database yet
	/*
		fi, err := os.Stat(idx.Path)
		if err != nil {
			return err
		}
		idx.FileSize := fi.Size()
	*/

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

	sql := fmt.Sprintf("INSERT INTO %s VALUES ", sqlquote.QualifiedIdent(cfg.DbName, cfg.IndexTable))
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

	sql := fmt.Sprintf("DELETE FROM %s WHERE %s = %s", sqlquote.QualifiedIdent(cfg.DbName, cfg.IndexTable), catalog.Hnsw_TblCol_Storage_Index_Id, sqlquote.String(idx.Id))
	sqls = append(sqls, sql)
	sql = fmt.Sprintf("DELETE FROM %s WHERE %s = %s", sqlquote.QualifiedIdent(cfg.DbName, cfg.MetadataTable), catalog.Hnsw_TblCol_Metadata_Index_Id, sqlquote.String(idx.Id))
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
	if idx.View {
		return moerr.NewInternalErrorNoCtx("usearch Add not support when readonly view = true")
	}

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
		return moerr.NewInternalErrorNoCtx(fmt.Sprintf("usearch dimension not match (expected %d but got %d)", dim, len(vec)))
	}

	defer runtime.KeepAlive(vec)
	return idx.Index.AddUnsafe(uint64(key), util.UnsafePointer(&vec[0]))
}

// add vector without increment the counter.  concurrency add will increment the counter before Add
func (idx *HnswModel[T]) AddWithoutIncr(key int64, vec []T) error {
	if idx.View {
		return moerr.NewInternalErrorNoCtx("usearch Add not support when readonly view = true")
	}

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

	defer runtime.KeepAlive(vec)
	return idx.Index.AddUnsafe(uint64(key), util.UnsafePointer(&vec[0]))
}

// remove key
func (idx *HnswModel[T]) Remove(key int64) error {
	if idx.View {
		return moerr.NewInternalErrorNoCtx("usearch Remove not support when readonly view = true")
	}

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

// hnswSpillDir returns where a model's local file belongs: the LOCAL fileservice volume, not
// $TMPDIR. A model is multi-GB and the load path MMAPS it for the entry's whole cache lifetime,
// so on a host where $TMPDIR is a tmpfs the "off-heap" index is really sitting in RAM -- the
// memory the index cache governor budgets. The LOCAL volume is also the one provisioned for
// exactly this, while /tmp is frequently small or slow.
//
// Mirrors what ivfpq/cagra FetchArtifact already does. HostSpillDir returns "" when there is no
// LOCAL fileservice, and os.CreateTemp reads "" as $TMPDIR, so unit tests and one-shot tools keep
// today's behaviour with no branch at the call sites.
// spillDir is this model's scratch directory: whatever the builder already resolved, else
// resolved now from the request. Mirrors ivfpq/cagra FetchArtifact's TmpDir-then-HostSpillDir.
func (idx *HnswModel[T]) spillDir(sqlproc *sqlexec.SqlProcess) string {
	if idx.TmpDir != "" {
		return idx.TmpDir
	}
	return hnswSpillDir(sqlproc)
}

func hnswSpillDir(sqlproc *sqlexec.SqlProcess) string {
	if sqlproc == nil {
		return ""
	}
	if sqlproc.Proc == nil {
		// A background / ISCP job runs on a SqlContext with no process.Process, so there is
		// no FileService to reach from here. Those callers resolve the directory themselves
		// and hand it to NewHnswSync -- see pkg/iscp, which does the same for fulltext2.
		return ""
	}
	return vimemory.HostSpillDir(sqlproc.GetTopContext(), sqlproc.Proc.Base.FileService, sqlproc.GetService())
}

func (idx *HnswModel[T]) LoadIndexFromBuffer(
	sqlproc *sqlexec.SqlProcess,
	idxcfg vectorindex.IndexConfig,
	tblcfg vectorindex.IndexTableConfig,
	nthread int64,
	view bool) (err error) {

	var (
		fp          *os.File
		stream_chan = make(chan executor.Result, 2)
		error_chan  = make(chan error, 2)
		wg          sync.WaitGroup
	)

	if idx.Index != nil {
		// index already loaded. ignore
		return nil
	}

	if !view {
		return moerr.NewInternalError(sqlproc.GetContext(), "LoadIndexFromBuffer only enable when view = true")
	}
	idx.View = true

	// ownsTempFile records that THIS call created the spill file, so only a file we
	// created is unlinked after the mapping is established. A caller-supplied Path is
	// left alone.
	ownsTempFile := false
	if len(idx.Path) == 0 {
		// Stream index chunks from DB into a temp file, then let usearch
		// mmap it via View(). This keeps the index data entirely off the
		// Go heap, eliminating GC pressure for multi-GB indexes.

		ownsTempFile = true
		fp, err = os.CreateTemp(idx.spillDir(sqlproc), "hnsw")
		if err != nil {
			return err
		}
		// Assign Path immediately so the deferred cleanup can always
		// find and remove the temp file, even if errors occur before
		// the streaming phase completes.
		idx.Path = fp.Name()
		defer func() {
			if fp != nil {
				fp.Close()
			}
			if err != nil {
				// clean up on error
				idx.Destroy()
			}
		}()

		err = fallocate.Fallocate(fp, 0, idx.FileSize)
		if err != nil {
			return err
		}

		// run streaming sql
		sql := fmt.Sprintf("SELECT chunk_id, data from %s WHERE index_id = %s", sqlquote.QualifiedIdent(tblcfg.DbName, tblcfg.IndexTable), sqlquote.String(idx.Id))

		ctx, cancel := context.WithCancelCause(sqlproc.GetTopContext())
		defer cancel(nil)

		wg.Add(1)
		go func() {
			defer func() {
				close(stream_chan)
				wg.Done()
			}()
			_, err2 := runSql_streaming(ctx, sqlproc, sql, stream_chan, error_chan)
			if err2 != nil {
				error_chan <- err2
				return
			}
		}()

		// incremental load from database
		sql_closed := false
		for !sql_closed {
			sql_closed, err = idx.loadChunk(ctx, sqlproc, stream_chan, error_chan, fp)
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

		wg.Wait()

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
	}

	// verify checksum from file
	chksum, err := vectorindex.CheckSum(idx.Path)
	if err != nil {
		return err
	}
	if chksum != idx.Checksum {
		return moerr.NewInternalError(sqlproc.GetContext(), "Checksum mismatch with index file")
	}

	usearchidx, err := usearch.NewIndex(idxcfg.Usearch)
	if err != nil {
		return err
	}
	defer func() {
		if err != nil {
			usearchidx.Destroy()
			if idx.Index == usearchidx {
				idx.Index = nil
			}
		}
	}()

	err = usearchidx.ChangeThreadsSearch(uint(nthread))
	if err != nil {
		return err
	}

	err = usearchidx.ChangeThreadsAdd(uint(nthread))
	if err != nil {
		return err
	}

	// View() mmaps the file — data stays off Go heap, OS can page out
	// under memory pressure.
	err = usearchidx.View(idx.Path)
	if err != nil {
		return err
	}

	// Unlink the spill file now that it is mapped. unlink() drops the directory entry,
	// not the inode: usearch holds the mapping (mmap MAP_SHARED, PROT_READ) and its own
	// descriptor open until Destroy(), so reads keep faulting in from the still-live
	// inode and the blocks are released only when that mapping goes away -- including on
	// a crash, via process teardown. Without this a killed CN leaves a full-size model
	// behind in the LOCAL fileservice volume with nothing to ever collect it.
	//
	// Only a file this call created is unlinked; Path is cleared so Destroy skips the
	// remove and a later reload recreates its own temp file.
	if ownsTempFile {
		if rerr := os.Remove(idx.Path); rerr != nil && !os.IsNotExist(rerr) {
			logutil.Warnf("HnswModel.LoadIndexFromBuffer: unlink spill file %s: %v", idx.Path, rerr)
		} else {
			idx.Path = ""
		}
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

	return nil
}

// load chunk from database
func (idx *HnswModel[T]) loadChunk(ctx context.Context,
	sqlproc *sqlexec.SqlProcess,
	stream_chan chan executor.Result,
	error_chan chan error,
	fp *os.File) (stream_closed bool, err error) {
	var res executor.Result
	var ok bool

	procCtx := sqlproc.GetContext()

	select {
	case res, ok = <-stream_chan:
		if !ok {
			return true, nil
		}
	case err = <-error_chan:
		return false, err
	case <-procCtx.Done():
		return false, moerr.NewInternalError(procCtx, "context cancelled")
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
	sqlproc *sqlexec.SqlProcess,
	idxcfg vectorindex.IndexConfig,
	tblcfg vectorindex.IndexTableConfig,
	nthread int64,
	view bool) (err error) {

	var (
		fp          *os.File
		stream_chan = make(chan executor.Result, 2)
		error_chan  = make(chan error, 2)
		wg          sync.WaitGroup
	)

	if idx.Index != nil {
		// index already loaded. ignore
		return nil
	}

	if idx.FileSize == 0 && len(idx.Path) == 0 {
		// indx is newly created and not save to file yet so simply create a usearch index here
		return idx.initIndex(idxcfg)
	}

	if len(idx.Checksum) == 0 {
		// Checksum is empty.  We shouldn't get the file from database
		return moerr.NewInternalErrorNoCtx("checksum is empty.  Cannot read index file from database")
	}

	if len(idx.Path) == 0 {

		// create tempfile for writing
		fp, err = os.CreateTemp(idx.spillDir(sqlproc), "hnsw")
		if err != nil {
			return err
		}

		// Assign Path immediately so cleanup can find the file on error.
		idx.Path = fp.Name()

		defer func() {
			if fp != nil {
				fp.Close()
				fp = nil
			}

			if err != nil || view {
				// On error: always remove the temp file.
				// On success with view: Load() copies data to C heap,
				// file is no longer needed.
				if len(idx.Path) > 0 {
					os.Remove(idx.Path)
					idx.Path = ""
				}
			}
		}()

		err = fallocate.Fallocate(fp, 0, idx.FileSize)
		if err != nil {
			return err
		}

		// run streaming sql
		sql := fmt.Sprintf("SELECT chunk_id, data from %s WHERE index_id = %s", sqlquote.QualifiedIdent(tblcfg.DbName, tblcfg.IndexTable), sqlquote.String(idx.Id))

		ctx, cancel := context.WithCancelCause(sqlproc.GetTopContext())
		defer cancel(nil)

		wg.Add(1)
		go func() {

			defer func() {
				close(stream_chan)
				wg.Done()
			}()
			_, err2 := runSql_streaming(ctx, sqlproc, sql, stream_chan, error_chan)
			if err2 != nil {
				error_chan <- err2
				return
			}
		}()

		// incremental load from database
		sql_closed := false
		for !sql_closed {
			sql_closed, err = idx.loadChunk(ctx, sqlproc, stream_chan, error_chan, fp)
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

		wg.Wait()

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

	}

	// check checksum
	chksum, err := vectorindex.CheckSum(idx.Path)
	if err != nil {
		return err
	}
	if chksum != idx.Checksum {
		return moerr.NewInternalError(sqlproc.GetContext(), "Checksum mismatch with the index file")
	}

	usearchidx, err := usearch.NewIndex(idxcfg.Usearch)
	if err != nil {
		return err
	}
	defer func() {
		if err != nil {
			usearchidx.Destroy()
			if idx.Index == usearchidx {
				idx.Index = nil
			}
		}
	}()

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
		if err != nil {
			return err
		}
		idx.View = true
	} else {
		err = usearchidx.Load(idx.Path)
		if err != nil {
			return err
		}
		err = usearchidx.Reserve(uint(idxcfg.IndexCapacity))
		if err != nil {
			return err
		}
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
		if idx.MaxCapacity > uint(idxcfg.IndexCapacity) {
			idx.MaxCapacity = uint(idxcfg.IndexCapacity)
		}
	}

	return nil
}

// unload
func (idx *HnswModel[T]) Unload() error {
	if idx.View {
		return moerr.NewInternalErrorNoCtx("Unload not support when View = true")
	}

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

	defer runtime.KeepAlive(query)
	return idx.Index.SearchUnsafe(util.UnsafePointer(&query[0]), limit)
}
