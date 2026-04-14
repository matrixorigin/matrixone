//go:build gpu

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

package cagra

import (
	"context"
	"fmt"
	"io"
	"math"
	"os"
	"strings"
	"sync"

	"github.com/detailyang/go-fallocate"
	"github.com/matrixorigin/matrixone/pkg/catalog"
	"github.com/matrixorigin/matrixone/pkg/common/moerr"
	"github.com/matrixorigin/matrixone/pkg/container/vector"
	"github.com/matrixorigin/matrixone/pkg/cuvs"
	"github.com/matrixorigin/matrixone/pkg/logutil"
	"github.com/matrixorigin/matrixone/pkg/util/executor"
	"github.com/matrixorigin/matrixone/pkg/vectorindex"
	"github.com/matrixorigin/matrixone/pkg/vectorindex/metric"
	"github.com/matrixorigin/matrixone/pkg/vectorindex/sqlexec"
)

var runSql = sqlexec.RunSql
var runSql_streaming = sqlexec.RunStreamingSql

// CagraModel wraps a GpuCagra index and handles load/save to the secondary index tables.
// The serialized form is a tar file produced by cuvs.Pack / cuvs.Unpack.
// T must satisfy cuvs.VectorType (float32 | Float16 | int8 | uint8).
type CagraModel[T cuvs.VectorType] struct {
	Id          string
	Index       *cuvs.GpuCagra[T]
	Path        string // local tar file path; empty when index is in GPU memory only
	FileSize    int64
	MaxCapacity uint64

	// build/load configuration
	Idxcfg  vectorindex.IndexConfig
	NThread uint32
	Devices []int

	// from DB metadata
	Timestamp int64
	Checksum  string

	// CDC / sync tracking
	Dirty bool
	View  bool
	Len   int64
}

// NewCagraModelForBuild creates a CagraModel ready for bulk-build.
// Call InitEmpty once the total vector count is known, then AddChunk, then Build.
func NewCagraModelForBuild[T cuvs.VectorType](id string, cfg vectorindex.IndexConfig, nthread uint32, devices []int) (*CagraModel[T], error) {
	return &CagraModel[T]{
		Id:      id,
		Idxcfg:  cfg,
		NThread: nthread,
		Devices: devices,
	}, nil
}

// cagraConfig returns the cuvs types derived from idx.Idxcfg.
func (idx *CagraModel[T]) cagraConfig() (cuvsMetric cuvs.DistanceType, bp cuvs.CagraBuildParams, mode cuvs.DistributionMode, err error) {
	cfg := idx.Idxcfg.CuvsCagra
	var ok bool
	cuvsMetric, ok = metric.MetricTypeToCuvsMetric[metric.MetricType(cfg.Metric)]
	if !ok {
		err = moerr.NewInternalErrorNoCtx("CagraModel: unsupported metric type")
		return
	}
	bp = cuvs.DefaultCagraBuildParams()
	if cfg.IntermediateGraphDegree > 0 {
		bp.IntermediateGraphDegree = cfg.IntermediateGraphDegree
	}
	if cfg.GraphDegree > 0 {
		bp.GraphDegree = cfg.GraphDegree
	}
	mode = cuvs.DistributionMode(cfg.DistributionMode)
	return
}

// InitEmpty allocates the GPU buffer for totalCount vectors.
// Must be called after NewCagraModelForBuild and before any AddChunk call.
func (idx *CagraModel[T]) InitEmpty(totalCount uint64) error {
	if idx.Index != nil {
		return moerr.NewInternalErrorNoCtx("CagraModel: index already initialized")
	}
	cuvsMetric, bp, mode, err := idx.cagraConfig()
	if err != nil {
		return err
	}
	gi, err := cuvs.NewGpuCagraEmpty[T](
		totalCount,
		uint32(idx.Idxcfg.CuvsCagra.Dimensions),
		cuvsMetric,
		bp,
		idx.Devices,
		idx.NThread,
		mode,
	)
	if err != nil {
		return err
	}
	if err = gi.Start(); err != nil {
		gi.Destroy()
		return err
	}
	idx.Index = gi
	idx.MaxCapacity = totalCount
	return nil
}

// AddChunk appends a chunk of typed vectors to the pre-allocated GPU buffer.
func (idx *CagraModel[T]) AddChunk(chunk []T, chunkCount uint64, ids []uint32) error {
	if idx.Index == nil {
		return moerr.NewInternalErrorNoCtx("CagraModel: index not initialized; call InitEmpty first")
	}
	if err := idx.Index.AddChunk(chunk, chunkCount, ids); err != nil {
		return err
	}
	idx.Len += int64(chunkCount)
	return nil
}

// AddChunkFloat appends a chunk of float32 vectors, quantizing on the fly when T is a 1-byte type.
func (idx *CagraModel[T]) AddChunkFloat(chunk []float32, chunkCount uint64, ids []uint32) error {
	if idx.Index == nil {
		return moerr.NewInternalErrorNoCtx("CagraModel: index not initialized; call InitEmpty first")
	}
	if err := idx.Index.AddChunkFloat(chunk, chunkCount, ids); err != nil {
		return err
	}
	idx.Len += int64(chunkCount)
	return nil
}

// Build constructs the CAGRA graph from the loaded vectors and starts the worker pool.
func (idx *CagraModel[T]) Build() error {
	if idx.Index == nil {
		return moerr.NewInternalErrorNoCtx("CagraModel: index not initialized")
	}
	if err := idx.Index.Build(); err != nil {
		return err
	}
	idx.Dirty = true
	return nil
}

// Destroy frees GPU memory and removes the local tar file if present.
func (idx *CagraModel[T]) Destroy() error {
	if idx.Index != nil {
		if err := idx.Index.Destroy(); err != nil {
			return err
		}
		idx.Index = nil
	}
	if len(idx.Path) > 0 {
		if _, err := os.Stat(idx.Path); err == nil || os.IsExist(err) {
			os.Remove(idx.Path)
		}
		idx.Path = ""
	}
	return nil
}

// saveToFile serializes the CAGRA index to a local tar file and updates idx.Path / idx.Checksum.
// If the index is clean (not dirty) or nil, it is a no-op.
// On success the GPU memory is freed and idx.Index is set to nil.
func (idx *CagraModel[T]) saveToFile() error {
	if idx.Index == nil {
		return nil
	}
	if !idx.Dirty {
		return nil
	}

	// Remove stale file if any.
	if len(idx.Path) > 0 {
		if _, statErr := os.Stat(idx.Path); statErr == nil || os.IsExist(statErr) {
			os.Remove(idx.Path)
		}
		idx.Path = ""
	}

	if idx.Len == 0 {
		// Empty index — just release GPU memory, nothing to persist.
		if err := idx.Index.Destroy(); err != nil {
			return err
		}
		idx.Index = nil
		return nil
	}

	tarFile, err := os.CreateTemp("", "cagra")
	if err != nil {
		return err
	}
	tarPath := tarFile.Name()
	tarFile.Close()

	if err = idx.Index.Pack(tarPath); err != nil {
		os.Remove(tarPath)
		return err
	}

	chksum, err := vectorindex.CheckSum(tarPath)
	if err != nil {
		os.Remove(tarPath)
		return err
	}
	idx.Checksum = chksum

	// Free GPU memory — the index is now persisted on disk.
	if err = idx.Index.Destroy(); err != nil {
		os.Remove(tarPath)
		return err
	}
	idx.Index = nil
	idx.Path = tarPath
	return nil
}

// ToSql generates INSERT SQL statements to store the model in the secondary index storage table.
// Mirrors HnswModel.ToSql — callers are responsible for generating the metadata INSERT.
func (idx *CagraModel[T]) ToSql(cfg vectorindex.IndexTableConfig) ([]string, error) {
	if err := idx.saveToFile(); err != nil {
		return nil, err
	}
	if len(idx.Path) == 0 {
		return []string{}, nil
	}

	fi, err := os.Stat(idx.Path)
	if err != nil {
		return nil, err
	}
	filesz := fi.Size()
	idx.FileSize = filesz

	if filesz == 0 {
		return []string{}, nil
	}

	logutil.Infof("CagraModel.ToSql idx %s, len = %d\n", idx.Id, idx.Len)

	sqls := make([]string, 0, 5)
	sqlPrefix := fmt.Sprintf("INSERT INTO `%s`.`%s` VALUES ", cfg.DbName, cfg.IndexTable)
	values := make([]string, 0, int64(math.Ceil(float64(filesz)/float64(vectorindex.MaxChunkSize))))
	n := 0
	chunkid := int64(0)
	for offset := int64(0); offset < filesz; {
		chunksz := int64(vectorindex.MaxChunkSize)
		if offset+chunksz > filesz {
			chunksz = filesz - offset
		}
		url := fmt.Sprintf("file://%s?offset=%d&size=%d", idx.Path, offset, chunksz)
		tuple := fmt.Sprintf("('%s', %d, load_file(cast('%s' as datalink)), 0)", idx.Id, chunkid, url)
		values = append(values, tuple)
		offset += chunksz
		chunkid++
		n++
		if n == 2000 {
			sqls = append(sqls, sqlPrefix+strings.Join(values, ", "))
			values = values[:0]
			n = 0
		}
	}
	if len(values) > 0 {
		sqls = append(sqls, sqlPrefix+strings.Join(values, ", "))
	}
	return sqls, nil
}

// ToDeleteSql generates DELETE SQL for both the storage and metadata tables.
func (idx *CagraModel[T]) ToDeleteSql(cfg vectorindex.IndexTableConfig) ([]string, error) {
	sqls := make([]string, 0, 2)
	sqls = append(sqls, fmt.Sprintf("DELETE FROM `%s`.`%s` WHERE %s = '%s'",
		cfg.DbName, cfg.IndexTable, catalog.Cagra_TblCol_Storage_Index_Id, idx.Id))
	sqls = append(sqls, fmt.Sprintf("DELETE FROM `%s`.`%s` WHERE %s = '%s'",
		cfg.DbName, cfg.MetadataTable, catalog.Cagra_TblCol_Metadata_Index_Id, idx.Id))
	return sqls, nil
}

// Empty returns true when no vectors have been added.
func (idx *CagraModel[T]) Empty() bool {
	return idx.Len == 0
}

// Full returns true when the index has reached its maximum capacity.
func (idx *CagraModel[T]) Full() bool {
	return idx.MaxCapacity > 0 && uint64(idx.Len) >= idx.MaxCapacity
}

// Search performs a KNN search and returns external PKs (uint32 cast to int64) with distances.
func (idx *CagraModel[T]) Search(query []T, limit uint32) (keys []int64, distances []float32, err error) {
	if idx.Index == nil {
		return nil, nil, moerr.NewInternalErrorNoCtx("CagraModel: index not loaded")
	}
	if len(query) == 0 {
		return nil, nil, moerr.NewInternalErrorNoCtx("CagraModel: query is nil")
	}
	sp := cuvs.DefaultCagraSearchParams()
	res, err := idx.Index.Search(query, 1, uint32(idx.Idxcfg.CuvsCagra.Dimensions), limit, sp)
	if err != nil {
		return nil, nil, err
	}
	keys = make([]int64, len(res.Neighbors))
	for i, n := range res.Neighbors {
		keys[i] = int64(n)
	}
	return keys, res.Distances, nil
}

// loadChunk reads one streaming result batch and writes each chunk at the correct file offset.
func (idx *CagraModel[T]) loadChunk(ctx context.Context,
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
		if _, err = fp.Seek(offset, io.SeekStart); err != nil {
			return false, err
		}
		if _, err = fp.Write(data); err != nil {
			return false, err
		}
	}
	return false, nil
}

// LoadIndex downloads the tar from the database, unpacks it, and loads the CAGRA index into GPU memory.
// Mirrors HnswModel.LoadIndex.
// idx.Devices must be set before calling LoadIndex.
func (idx *CagraModel[T]) LoadIndex(
	sqlproc *sqlexec.SqlProcess,
	idxcfg vectorindex.IndexConfig,
	tblcfg vectorindex.IndexTableConfig,
	nthread int64,
	view bool) (err error) {

	var (
		fp         *os.File
		streamChan = make(chan executor.Result, 2)
		errorChan  = make(chan error, 2)
		fname      string
		wg         sync.WaitGroup
	)

	if idx.Index != nil {
		return nil
	}

	if idx.FileSize == 0 && len(idx.Path) == 0 {
		return moerr.NewInternalErrorNoCtx("CagraModel: index not built; call InitEmpty/AddChunk/Build first")
	}

	if len(idx.Checksum) == 0 {
		return moerr.NewInternalErrorNoCtx("CagraModel: checksum is empty; cannot load from database")
	}

	if len(idx.Path) == 0 {
		// Download the tar file from the database via streaming SQL.
		fp, err = os.CreateTemp("", "cagra")
		if err != nil {
			return err
		}
		fname = fp.Name()

		defer func() {
			if fp != nil {
				fp.Close()
				fp = nil
			}
			if view {
				if len(fname) > 0 {
					os.Remove(fname)
				}
			}
		}()

		if err = fallocate.Fallocate(fp, 0, idx.FileSize); err != nil {
			return err
		}

		sql := fmt.Sprintf("SELECT chunk_id, data FROM `%s`.`%s` WHERE index_id = '%s'",
			tblcfg.DbName, tblcfg.IndexTable, idx.Id)

		ctx, cancel := context.WithCancelCause(sqlproc.GetTopContext())
		defer cancel(nil)

		wg.Add(1)
		go func() {
			defer func() {
				close(streamChan)
				wg.Done()
			}()
			_, err2 := runSql_streaming(ctx, sqlproc, sql, streamChan, errorChan)
			if err2 != nil {
				errorChan <- err2
			}
		}()

		sql_closed := false
		for !sql_closed {
			sql_closed, err = idx.loadChunk(ctx, sqlproc, streamChan, errorChan, fp)
			if err != nil {
				cancel(err)
				break
			}
		}

		// Drain the channel so the producer goroutine can finish.
		if !sql_closed {
			for res := range streamChan {
				res.Close()
			}
		}
		wg.Wait()

		if err == nil {
			select {
			case err = <-errorChan:
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

	// Verify checksum.
	chksum, err := vectorindex.CheckSum(idx.Path)
	if err != nil {
		return err
	}
	if chksum != idx.Checksum {
		return moerr.NewInternalError(sqlproc.GetContext(), "CagraModel: checksum mismatch")
	}

	// Reconstruct the GpuCagra instance from configuration.
	idx.Idxcfg = idxcfg
	idx.NThread = uint32(nthread)

	cuvsMetric, bp, mode, err := idx.cagraConfig()
	if err != nil {
		return err
	}

	gi, err := cuvs.NewGpuCagraEmpty[T](
		0,
		uint32(idxcfg.CuvsCagra.Dimensions),
		cuvsMetric,
		bp,
		idx.Devices,
		uint32(nthread),
		mode,
	)
	if err != nil {
		return err
	}

	if err = gi.Start(); err != nil {
		gi.Destroy()
		return err
	}

	if err = gi.Unpack(idx.Path); err != nil {
		gi.Destroy()
		return err
	}

	gi.SetUseBatching(true)

	idx.Index = gi
	idx.View = view
	idx.Len = int64(gi.Len())
	idx.MaxCapacity = uint64(gi.Cap())

	logutil.Debugf("CagraModel.LoadIndex idx %s, len = %d\n", idx.Id, idx.Len)

	if view {
		// Remove the local tar; the index is fully in GPU memory.
		if len(idx.Path) > 0 {
			os.Remove(idx.Path)
		}
		idx.Path = ""
	}

	return nil
}

// Unload persists dirty state to a local tar file and frees GPU memory.
func (idx *CagraModel[T]) Unload() error {
	if idx.Index == nil {
		return nil
	}
	logutil.Debugf("CagraModel.Unload idx %s, len = %d\n", idx.Id, idx.Len)

	if err := idx.saveToFile(); err != nil {
		return err
	}
	// saveToFile frees GPU memory when dirty; always ensure cleanup.
	if idx.Index != nil {
		if err := idx.Index.Destroy(); err != nil {
			return err
		}
		idx.Index = nil
	}
	return nil
}

// LoadMetadata loads CagraModel descriptors from the metadata table.
// Each returned model has Id, Checksum, Timestamp, and FileSize set; Index is nil.
func LoadMetadata[T cuvs.VectorType](sqlproc *sqlexec.SqlProcess, dbname string, metatbl string) ([]*CagraModel[T], error) {
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

	indexes := make([]*CagraModel[T], 0, total)
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
			idx := &CagraModel[T]{Id: id, Checksum: chksum, Timestamp: ts, FileSize: fs}
			indexes = append(indexes, idx)
		}
	}
	return indexes, nil
}
