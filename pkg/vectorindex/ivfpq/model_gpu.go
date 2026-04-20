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

package ivfpq

import (
	"context"
	"fmt"
	"io"
	"math"
	"os"
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

// IvfpqModel wraps a GpuIvfPq index and handles load/save to secondary index tables.
type IvfpqModel[T cuvs.VectorType] struct {
	Id          string
	Index       *cuvs.GpuIvfPq[T]
	Path        string
	FileSize    int64
	MaxCapacity uint64

	Idxcfg  vectorindex.IndexConfig
	NThread uint32
	Devices []int

	Timestamp int64
	Checksum  string

	Dirty bool
	View  bool
	Len   int64
}

func NewIvfpqModelForBuild[T cuvs.VectorType](id string, cfg vectorindex.IndexConfig, nthread uint32, devices []int) (*IvfpqModel[T], error) {
	return &IvfpqModel[T]{
		Id:      id,
		Idxcfg:  cfg,
		NThread: nthread,
		Devices: devices,
	}, nil
}

func (idx *IvfpqModel[T]) ivfpqConfig() (cuvsMetric cuvs.DistanceType, bp cuvs.IvfPqBuildParams, mode cuvs.DistributionMode, err error) {
	cfg := idx.Idxcfg.CuvsIvfpq
	var ok bool
	cuvsMetric, ok = metric.MetricTypeToCuvsMetric[metric.MetricType(cfg.Metric)]
	if !ok {
		err = moerr.NewInternalErrorNoCtx("IvfpqModel: unsupported metric type")
		return
	}
	bp = cuvs.DefaultIvfPqBuildParams()
	if cfg.Lists > 0 {
		bp.NLists = uint32(cfg.Lists)
	}
	if cfg.M > 0 {
		bp.M = uint32(cfg.M)
	}
	if cfg.BitsPerCode > 0 {
		bp.BitsPerCode = uint32(cfg.BitsPerCode)
	}
	if cfg.KmeansTrainsetFraction > 0 {
		bp.KmeansTrainsetFraction = cfg.KmeansTrainsetFraction
	}
	mode = cuvs.DistributionMode(cfg.DistributionMode)
	return
}

// InitEmpty allocates the GPU buffer for totalCount vectors.
func (idx *IvfpqModel[T]) InitEmpty(totalCount uint64) error {
	if idx.Index != nil {
		return moerr.NewInternalErrorNoCtx("IvfpqModel: index already initialized")
	}
	cuvsMetric, bp, mode, err := idx.ivfpqConfig()
	if err != nil {
		return err
	}
	gi, err := cuvs.NewGpuIvfPqEmpty[T](
		totalCount,
		uint32(idx.Idxcfg.CuvsIvfpq.Dimensions),
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

func (idx *IvfpqModel[T]) AddChunkFloat(chunk []float32, chunkCount uint64, ids []int64) error {
	if idx.Index == nil {
		return moerr.NewInternalErrorNoCtx("IvfpqModel: index not initialized; call InitEmpty first")
	}
	if err := idx.Index.AddChunkFloat(chunk, chunkCount, ids); err != nil {
		return err
	}
	idx.Len += int64(chunkCount)
	return nil
}

func (idx *IvfpqModel[T]) Build() error {
	if idx.Index == nil {
		return moerr.NewInternalErrorNoCtx("IvfpqModel: index not initialized")
	}
	if err := idx.Index.Build(); err != nil {
		return err
	}
	idx.Dirty = true
	return nil
}

func (idx *IvfpqModel[T]) Destroy() error {
	if idx.Index != nil {
		if err := idx.Index.Destroy(); err != nil {
			return err
		}
		idx.Index = nil
	}
	if len(idx.Path) > 0 {
		os.Remove(idx.Path)
		idx.Path = ""
	}
	return nil
}

func (idx *IvfpqModel[T]) saveToFile() error {
	if idx.Index == nil {
		return nil
	}
	if !idx.Dirty {
		return nil
	}

	if len(idx.Path) > 0 {
		if _, statErr := os.Stat(idx.Path); statErr == nil || os.IsExist(statErr) {
			os.Remove(idx.Path)
		}
		idx.Path = ""
	}

	if idx.Len == 0 {
		if err := idx.Index.Destroy(); err != nil {
			return err
		}
		idx.Index = nil
		return nil
	}

	tarFile, err := os.CreateTemp("", "ivfpq")
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

	if err = idx.Index.Destroy(); err != nil {
		os.Remove(tarPath)
		return err
	}
	idx.Index = nil
	idx.Path = tarPath
	return nil
}

func (idx *IvfpqModel[T]) ToSql(cfg vectorindex.IndexTableConfig) ([]string, error) {
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

	logutil.Infof("IvfpqModel.ToSql idx %s, len = %d\n", idx.Id, idx.Len)

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
			sqls = append(sqls, sqlPrefix+joinStrings(values, ", "))
			values = values[:0]
			n = 0
		}
	}
	if len(values) > 0 {
		sqls = append(sqls, sqlPrefix+joinStrings(values, ", "))
	}
	return sqls, nil
}

func joinStrings(ss []string, sep string) string {
	if len(ss) == 0 {
		return ""
	}
	result := ss[0]
	for _, s := range ss[1:] {
		result += sep + s
	}
	return result
}

func (idx *IvfpqModel[T]) Empty() bool {
	return idx.Len == 0
}

func (idx *IvfpqModel[T]) Full() bool {
	return idx.MaxCapacity > 0 && uint64(idx.Len) >= idx.MaxCapacity
}

// SearchF32 performs a KNN search using a float32 query vector.
func (idx *IvfpqModel[T]) SearchF32(query []float32, limit uint32, nprobes uint32) (keys []int64, distances []float32, err error) {
	if idx.Index == nil {
		return nil, nil, moerr.NewInternalErrorNoCtx("IvfpqModel: index not loaded")
	}
	if len(query) == 0 {
		return nil, nil, moerr.NewInternalErrorNoCtx("IvfpqModel: query is nil")
	}
	sp := cuvs.IvfPqSearchParams{NProbes: nprobes}
	if sp.NProbes == 0 {
		sp = cuvs.DefaultIvfPqSearchParams()
	}
	res, err := idx.Index.SearchFloat(query, 1, uint32(idx.Idxcfg.CuvsIvfpq.Dimensions), limit, sp)
	if err != nil {
		return nil, nil, err
	}
	return res.Neighbors, res.Distances, nil
}

func (idx *IvfpqModel[T]) Search(query []T, limit uint32, nprobes uint32) (keys []int64, distances []float32, err error) {
	if idx.Index == nil {
		return nil, nil, moerr.NewInternalErrorNoCtx("IvfpqModel: index not loaded")
	}
	if len(query) == 0 {
		return nil, nil, moerr.NewInternalErrorNoCtx("IvfpqModel: query is nil")
	}
	sp := cuvs.IvfPqSearchParams{NProbes: nprobes}
	if sp.NProbes == 0 {
		sp = cuvs.DefaultIvfPqSearchParams()
	}
	res, err := idx.Index.Search(query, 1, uint32(idx.Idxcfg.CuvsIvfpq.Dimensions), limit, sp)
	if err != nil {
		return nil, nil, err
	}
	return res.Neighbors, res.Distances, nil
}

func (idx *IvfpqModel[T]) loadChunk(ctx context.Context,
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

func (idx *IvfpqModel[T]) LoadIndex(
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
		return moerr.NewInternalErrorNoCtx("IvfpqModel: index not built; call InitEmpty/AddChunk/Build first")
	}

	if len(idx.Checksum) == 0 {
		return moerr.NewInternalErrorNoCtx("IvfpqModel: checksum is empty; cannot load from database")
	}

	if len(idx.Path) == 0 {
		fp, err = os.CreateTemp("", "ivfpq")
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

	chksum, err := vectorindex.CheckSum(idx.Path)
	if err != nil {
		return err
	}
	if chksum != idx.Checksum {
		return moerr.NewInternalError(sqlproc.GetContext(), "IvfpqModel: checksum mismatch")
	}

	idx.Idxcfg = idxcfg
	idx.NThread = uint32(nthread)

	cuvsMetric, bp, mode, err := idx.ivfpqConfig()
	if err != nil {
		return err
	}

	gi, err := cuvs.NewGpuIvfPqEmpty[T](
		uint64(tblcfg.IndexCapacity),
		uint32(idxcfg.CuvsIvfpq.Dimensions),
		cuvsMetric,
		bp,
		idx.Devices,
		uint32(nthread),
		mode,
	)
	if err != nil {
		return err
	}

	gi.SetBatchWindow(tblcfg.BatchWindow)

	if err = gi.Start(); err != nil {
		gi.Destroy()
		return err
	}

	if err = gi.Unpack(idx.Path); err != nil {
		gi.Destroy()
		return err
	}

	idx.Index = gi
	idx.View = view
	idx.Len = int64(gi.Len())
	idx.MaxCapacity = uint64(gi.Cap())

	logutil.Debugf("IvfpqModel.LoadIndex idx %s, len = %d\n", idx.Id, idx.Len)

	if view {
		if len(idx.Path) > 0 {
			os.Remove(idx.Path)
		}
		idx.Path = ""
	}

	return nil
}

func (idx *IvfpqModel[T]) Unload() error {
	if idx.Index == nil {
		return nil
	}
	logutil.Debugf("IvfpqModel.Unload idx %s, len = %d\n", idx.Id, idx.Len)

	if err := idx.saveToFile(); err != nil {
		return err
	}
	if idx.Index != nil {
		if err := idx.Index.Destroy(); err != nil {
			return err
		}
		idx.Index = nil
	}
	return nil
}

// LoadMetadata loads IvfpqModel descriptors from the metadata table.
func LoadMetadata[T cuvs.VectorType](sqlproc *sqlexec.SqlProcess, dbname string, metatbl string) ([]*IvfpqModel[T], error) {
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

	indexes := make([]*IvfpqModel[T], 0, total)
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
			idx := &IvfpqModel[T]{Id: id, Checksum: chksum, Timestamp: ts, FileSize: fs}
			indexes = append(indexes, idx)
		}
	}
	return indexes, nil
}

// ToDeleteSql generates DELETE SQL for storage and metadata tables.
func (idx *IvfpqModel[T]) ToDeleteSql(cfg vectorindex.IndexTableConfig) ([]string, error) {
	sqls := make([]string, 0, 2)
	sqls = append(sqls, fmt.Sprintf("DELETE FROM `%s`.`%s` WHERE %s = '%s'",
		cfg.DbName, cfg.IndexTable, catalog.Ivfpq_TblCol_Storage_Index_Id, idx.Id))
	sqls = append(sqls, fmt.Sprintf("DELETE FROM `%s`.`%s` WHERE %s = '%s'",
		cfg.DbName, cfg.MetadataTable, catalog.Ivfpq_TblCol_Metadata_Index_Id, idx.Id))
	return sqls, nil
}
