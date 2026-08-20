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
	"errors"
	"fmt"
	"io"
	"math"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"time"

	"github.com/detailyang/go-fallocate"
	"github.com/matrixorigin/matrixone/pkg/catalog"
	"github.com/matrixorigin/matrixone/pkg/common/moerr"
	"github.com/matrixorigin/matrixone/pkg/common/sqlquote"
	"github.com/matrixorigin/matrixone/pkg/common/util"
	"github.com/matrixorigin/matrixone/pkg/container/vector"
	"github.com/matrixorigin/matrixone/pkg/cuvs"
	"github.com/matrixorigin/matrixone/pkg/logutil"
	"github.com/matrixorigin/matrixone/pkg/util/executor"
	"github.com/matrixorigin/matrixone/pkg/vectorindex"
	cuvscdc "github.com/matrixorigin/matrixone/pkg/vectorindex/cuvs"
	vimemory "github.com/matrixorigin/matrixone/pkg/vectorindex/memory"
	"github.com/matrixorigin/matrixone/pkg/vectorindex/metric"
	"github.com/matrixorigin/matrixone/pkg/vectorindex/sqlexec"
)

var runSql = sqlexec.RunSql
var runSql_streaming = sqlexec.RunStreamingSql

// CagraModel wraps a GpuCagra index and handles load/save to the secondary index tables.
// The serialized form is a tar file produced by cuvs.Pack / cuvs.Unpack.
// T must satisfy cuvs.VectorType (float32 | Float16 | int8 | uint8).
type CagraModel[B, Q cuvs.VectorType] struct {
	Id     string
	Index  *cuvs.GpuCagra[B, Q]
	Path   string
	TmpDir string
	// TmpDir scopes this model's packed tar to its builder's private directory so
	// the builder can reclaim every tar with one RemoveAll. Empty means $TMPDIR,
	// which keeps any non-builder caller behaving exactly as before. // local tar file path; empty when index is in GPU memory only
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

	// CDC delete state — pkids that the unified-log replay marked for deletion
	// (DELETE record with no later INSERT). Replayed through Index.DeleteIds
	// after Unpack to apply to the in-memory cuvs deleted_bitset_.
	DeletedPkids []int64

	// CDC insert overflow — pkids that the replay left in the brute-force
	// overflow (INSERT record with no later DELETE). Brute-force searched at
	// query time and merged with main-index results. F32 regardless of T
	// (quantizer params live in the model tar, not available at CDC write
	// time).
	OverflowPkids []int64
	OverflowVecs  []B // len = len(OverflowPkids) * dim (native base type B)

	// INCLUDE column data carried alongside each overflow row. Layout
	// matches the EncodeEventRecord INSERT-record include section:
	// row-major in column-meta order, then ceil(ncols/8) trailing bytes per
	// row for the null mask. Empty when the index has no INCLUDE columns.
	OverflowIncludeBytes []byte
	IncludeBytesPerRow   int

	// OverflowColMetaJSON carries the persisted INCLUDE-column layout
	// recovered from the CdcOpHeader record in tag=1 chunk_id=0 when
	// this synthetic CDC-tail model is the only one in s.Indexes
	// (small-data-only index, no tag=0 sub-index was ever built).
	// buildOverflow consults this when GetFilterColMetaJSON() can't be
	// asked of a main-index. Empty for the normal "tag=1 alongside
	// tag=0" case.
	OverflowColMetaJSON string
}

// NewCagraModelForBuild creates a CagraModel ready for bulk-build.
// Call InitEmpty once the total vector count is known, then AddChunk, then Build.
func NewCagraModelForBuild[B, Q cuvs.VectorType](id string, cfg vectorindex.IndexConfig, nthread uint32, devices []int) (*CagraModel[B, Q], error) {
	return &CagraModel[B, Q]{
		Id:      id,
		Idxcfg:  cfg,
		NThread: nthread,
		Devices: devices,
	}, nil
}

// cagraConfig returns the cuvs types derived from idx.Idxcfg.
func (idx *CagraModel[B, Q]) cagraConfig() (cuvsMetric cuvs.DistanceType, bp cuvs.CagraBuildParams, mode cuvs.DistributionMode, err error) {
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
	if cfg.QuantizerTrainLimit > 0 {
		bp.QuantizerTrainLimit = cfg.QuantizerTrainLimit
	}
	mode = cuvs.DistributionMode(cfg.DistributionMode)
	return
}

// InitEmpty allocates the GPU buffer for totalCount vectors.
// Must be called after NewCagraModelForBuild and before any AddChunk call.
func (idx *CagraModel[B, Q]) InitEmpty(totalCount uint64) error {
	if idx.Index != nil {
		return moerr.NewInternalErrorNoCtx("CagraModel: index already initialized")
	}
	cuvsMetric, bp, mode, err := idx.cagraConfig()
	if err != nil {
		return err
	}
	gi, err := cuvs.NewGpuCagraEmpty[B, Q](
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
func (idx *CagraModel[B, Q]) AddChunk(chunk []Q, chunkCount uint64, ids []int64) error {
	if idx.Index == nil {
		return moerr.NewInternalErrorNoCtx("CagraModel: index not initialized; call InitEmpty first")
	}
	/*
		if len(ids) > 0 {
			logutil.Infof("[DEBUG] CagraModel.AddChunk: chunkCount=%d, first_id=%d, last_id=%d", chunkCount, ids[0], ids[len(ids)-1])
		}
	*/
	if err := idx.Index.AddChunk(chunk, chunkCount, ids); err != nil {
		return err
	}
	idx.Len += int64(chunkCount)
	return nil
}

// AddChunkQuantize appends a chunk of base-typed (B) vectors, quantizing
// natively to the 1-byte storage type Q (int8/uint8). Used for a vecf16 base
// with QUANTIZATION=int8/uint8 — no f32 detour.
func (idx *CagraModel[B, Q]) AddChunkQuantize(chunk []B, chunkCount uint64, ids []int64) error {
	if idx.Index == nil {
		return moerr.NewInternalErrorNoCtx("CagraModel: index not initialized; call InitEmpty first")
	}
	if err := idx.Index.AddChunkQuantize(chunk, chunkCount, ids); err != nil {
		return err
	}
	idx.Len += int64(chunkCount)
	return nil
}

// Build constructs the CAGRA graph from the loaded vectors and starts the worker pool.
func (idx *CagraModel[B, Q]) Build() error {
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
func (idx *CagraModel[B, Q]) Destroy() error {
	// Release the GPU handle and the packed tar independently: the file does not
	// depend on the handle, so returning early on a Destroy() error used to leak it
	// for the lifetime of the process. Collect both outcomes instead.
	var errs error
	if idx.Index != nil {
		if err := idx.Index.Destroy(); err != nil {
			errs = errors.Join(errs, err)
		} else {
			idx.Index = nil
		}
	}
	if len(idx.Path) > 0 {
		if _, err := os.Stat(idx.Path); err == nil || os.IsExist(err) {
			os.Remove(idx.Path)
		}
		idx.Path = ""
	}
	return errs
}

// saveToFile serializes the CAGRA index to a local tar file and updates idx.Path / idx.Checksum.
// If the index is clean (not dirty) or nil, it is a no-op.
// On success the GPU memory is freed and idx.Index is set to nil.
func (idx *CagraModel[B, Q]) saveToFile() error {
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
		logutil.Infof("CagraModel.saveToFile: empty index idx=%s, destroy only", idx.Id)
		if err := idx.Index.Destroy(); err != nil {
			return err
		}
		idx.Index = nil
		return nil
	}

	tarFile, err := os.CreateTemp(idx.TmpDir, "cagra")
	if err != nil {
		return err
	}
	tarPath := tarFile.Name()
	tarFile.Close()

	logutil.Infof("CagraModel.saveToFile: idx=%s len=%d calling Pack -> %s", idx.Id, idx.Len, tarPath)
	t0 := time.Now()
	if err = idx.Index.Pack(tarPath, idx.TmpDir); err != nil {
		logutil.Errorf("CagraModel.saveToFile: Pack FAILED idx=%s after %v: %v", idx.Id, time.Since(t0), err)
		os.Remove(tarPath)
		return err
	}
	packDur := time.Since(t0)
	fi, _ := os.Stat(tarPath)
	packedBytes := int64(0)
	if fi != nil {
		packedBytes = fi.Size()
	}
	logutil.Infof("CagraModel.saveToFile: Pack done idx=%s in %v (%d bytes)", idx.Id, packDur, packedBytes)

	chksum, err := vectorindex.CheckSum(tarPath)
	if err != nil {
		logutil.Errorf("CagraModel.saveToFile: CheckSum FAILED idx=%s: %v", idx.Id, err)
		os.Remove(tarPath)
		return err
	}
	idx.Checksum = chksum

	// Record the successfully-packed tar BEFORE attempting Destroy: a Destroy
	// failure does not invalidate the on-disk artifact, and removing it here
	// would lose committed data.
	idx.Path = tarPath

	// Free GPU memory — the index is now persisted on disk.
	if err = idx.Index.Destroy(); err != nil {
		logutil.Errorf("CagraModel.saveToFile: Destroy FAILED idx=%s (tar RETAINED at %s): %v", idx.Id, tarPath, err)
		return err
	}
	idx.Index = nil
	logutil.Infof("CagraModel.saveToFile: DONE idx=%s path=%s", idx.Id, tarPath)
	return nil
}

// ToSql generates INSERT SQL statements to store the model in the secondary index storage table.
// Mirrors HnswModel.ToSql — callers are responsible for generating the metadata INSERT.
func (idx *CagraModel[B, Q]) ToSql(cfg vectorindex.IndexTableConfig) ([]string, error) {
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
	sqlPrefix := fmt.Sprintf("INSERT INTO %s VALUES ", sqlquote.QualifiedIdent(cfg.DbName, cfg.IndexTable))
	values := make([]string, 0, int64(math.Ceil(float64(filesz)/float64(vectorindex.MaxChunkSize))))
	n := 0
	chunkid := int64(0)
	for offset := int64(0); offset < filesz; {
		chunksz := int64(vectorindex.MaxChunkSize)
		if offset+chunksz > filesz {
			chunksz = filesz - offset
		}
		url := fmt.Sprintf("file://%s?offset=%d&size=%d", idx.Path, offset, chunksz)
		tuple := fmt.Sprintf("('%s', %d, load_file(cast('%s' as datalink)), %d)", idx.Id, chunkid, url, vectorindex.Tag_ModelChunk)
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
func (idx *CagraModel[B, Q]) ToDeleteSql(cfg vectorindex.IndexTableConfig) ([]string, error) {
	sqls := make([]string, 0, 2)
	sqls = append(sqls, fmt.Sprintf("DELETE FROM %s WHERE %s = %s",
		sqlquote.QualifiedIdent(cfg.DbName, cfg.IndexTable), catalog.Cagra_TblCol_Storage_Index_Id, sqlquote.String(idx.Id)))
	sqls = append(sqls, fmt.Sprintf("DELETE FROM %s WHERE %s = %s",
		sqlquote.QualifiedIdent(cfg.DbName, cfg.MetadataTable), catalog.Cagra_TblCol_Metadata_Index_Id, sqlquote.String(idx.Id)))
	return sqls, nil
}

// Empty returns true when no vectors have been added.
func (idx *CagraModel[B, Q]) Empty() bool {
	return idx.Len == 0
}

// Full returns true when the index has reached its maximum capacity.
func (idx *CagraModel[B, Q]) Full() bool {
	return idx.MaxCapacity > 0 && uint64(idx.Len) >= idx.MaxCapacity
}

// Search performs a KNN search and returns external PKs with distances.
func (idx *CagraModel[B, Q]) Search(query []Q, limit uint32) (keys []int64, distances []float32, err error) {
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
	return res.Neighbors, res.Distances, nil
}

// loadChunk reads one streaming result batch and writes each chunk at the correct file offset.
func (idx *CagraModel[B, Q]) loadChunk(ctx context.Context,
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
//
// Two storage tags are loaded in parallel:
//   - tag=0: model tar chunks (streaming, multi-GB)
//   - tag=1: CDC event log (small KB–MB; replayed once after Unpack to derive
//     the deleted-pkid set and the brute-force overflow)
func (idx *CagraModel[B, Q]) LoadIndex(
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

	// Fire the tag=1 event-log fetch in parallel with the model tar streaming.
	// Replay (which needs includeBytesPerRow from the loaded cuvs index) is
	// deferred until after Unpack — we only fetch the raw chunks here.
	var (
		dim         = int(idxcfg.CuvsCagra.Dimensions)
		eventChunks []cuvscdc.EventChunk
	)

	// Fetch the tag=1 CDC chunks first, SEQUENTIALLY: this and the model-tar
	// (tag=ModelChunk) streaming load below both execute SQL on sqlproc's single
	// txn operator, which is not safe to drive concurrently. eventChunks is only
	// replayed later (after Unpack), so fetching it up front changes nothing.
	eventChunks, err = idx.loadCdcEventsFromDB(sqlproc, tblcfg)
	if err != nil {
		return err
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

		sql := fmt.Sprintf("SELECT chunk_id, data FROM %s WHERE index_id = %s AND tag = %d",
			sqlquote.QualifiedIdent(tblcfg.DbName, tblcfg.IndexTable), sqlquote.String(idx.Id), vectorindex.Tag_ModelChunk)

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

	// Reconcile idx.Devices with the shard topology recorded in the tar's
	// manifest.json. On a single-GPU host the loader auto-pads so a SHARDED
	// index built under gpu_multi_simulation=N loads all N shards even when
	// the search session has the sim var unset (without this, only shard_0
	// gets loaded and every search returns 100% shard-0 results). On a
	// multi-GPU host with fewer physical GPUs than the saved shard count
	// this errors, since silently overloading some GPUs would just tank
	// throughput and hide the misconfig.
	resolved, shardCount, perr := cuvs.ResolveDevicesForTarLoad(idx.Devices, idx.Path)
	if perr != nil {
		return perr
	}
	if shardCount > 0 && len(resolved) != len(idx.Devices) {
		logutil.Infof("CagraModel.LoadIndex: adjusted idx.Devices from %v to %v to match manifest shard_count=%d",
			idx.Devices, resolved, shardCount)
		idx.Devices = resolved
	}

	// VRAM admission happens HERE rather than in a caller pre-pass, because it
	// needs the shard topology from the tar manifest and the tar only exists
	// locally after the download above. Admitting earlier meant opening idx.Path
	// while it was still "" -- LoadMetadata builds models with Id/Checksum/
	// Timestamp/FileSize but no Path -- so ResolveDevicesForTarLoad failed on
	// os.Open("") and every SHARDED cold load died before its first query.
	//
	// Per-sub-index rather than one aggregate pre-check: sub-indexes load
	// sequentially, so re-sampling free VRAM for each one already accounts for
	// the ones loaded ahead of it, and the reservation covers the window where
	// an admitted load is not resident yet. This still runs before
	// NewGpuCagraEmpty, so no device memory has been committed at this point.
	releaseVRAM, aerr := vimemory.DeviceReserveLoad(
		vimemory.DeviceLoadBytes(
			vectorindex.DistributionMode(idxcfg.CuvsCagra.DistributionMode),
			idx.Devices, shardCount, uint64(idx.FileSize)),
		func(d int) (uint64, error) {
			_, freeBytes, ferr := cuvs.RowsFittingFreeMem(d, 1)
			return freeBytes, ferr
		}, "CagraModel.LoadIndex")
	if aerr != nil {
		return aerr
	}
	// Released on every path out: once LoadIndex returns, the bytes are either
	// really resident (so the next free-VRAM sample counts them) or were freed
	// by the failure path.
	defer releaseVRAM()

	cuvsMetric, bp, mode, err := idx.cagraConfig()
	if err != nil {
		return err
	}

	gi, err := cuvs.NewGpuCagraEmpty[B, Q](
		uint64(idxcfg.IndexCapacity),
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

	// idx.Path lives in HostSpillDir; extract into the same directory so the
	// intermediate (same-size scratch as the tar) does NOT land in /tmp.
	if err = gi.Unpack(idx.Path, filepath.Dir(idx.Path), mode); err != nil {
		gi.Destroy()
		return err
	}

	gi.SetBatchWindow(tblcfg.BatchWindow)

	// The model tar carries the INCLUDE col meta; pull it and replay the
	// fetched tag=1 event log at the right INSERT-record size.
	colMetaJSON := gi.GetFilterColMetaJSON()
	includeBytesPerRow := 0
	if colMetaJSON != "" {
		ibpr, e := cuvscdc.CdcIncludeBytesPerRow(colMetaJSON)
		if e != nil {
			gi.Destroy()
			return e
		}
		includeBytesPerRow = ibpr
	}
	delPkids, ovPkids, ovVecs, ovInc, err := replayEventChunks[B](eventChunks, dim, includeBytesPerRow)
	if err != nil {
		gi.Destroy()
		return err
	}
	idx.DeletedPkids = delPkids
	idx.OverflowPkids = ovPkids
	idx.OverflowVecs = ovVecs
	idx.OverflowIncludeBytes = ovInc
	idx.IncludeBytesPerRow = includeBytesPerRow

	// Replay CDC deletes onto the freshly-loaded cuvs index. delete_id is
	// idempotent and silently no-ops on pkids the cuvs id_map doesn't know
	// (e.g. a row that was inserted post-build and now lives only in
	// OverflowPkids — that case is handled at search time).
	if err = gi.DeleteIds(idx.DeletedPkids); err != nil {
		gi.Destroy()
		return err
	}

	idx.Index = gi
	idx.View = view
	idx.Len = int64(gi.Len())
	idx.MaxCapacity = uint64(gi.Cap())

	logutil.Debugf("CagraModel.LoadIndex idx %s, len = %d, deletes = %d, overflow = %d\n",
		idx.Id, idx.Len, len(idx.DeletedPkids), len(idx.OverflowPkids))

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
func (idx *CagraModel[B, Q]) Unload() error {
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

// loadCdcEventsFromDB reads the tag=1 event-log rows for this index and
// returns one EventChunk per row. The caller (LoadIndex / search) sorts by
// chunk_id before replay since record ordering across chunks encodes the
// temporal ordering between DELETE and INSERT events for the same pkid.
func (idx *CagraModel[B, Q]) loadCdcEventsFromDB(
	sqlproc *sqlexec.SqlProcess,
	tblcfg vectorindex.IndexTableConfig,
) ([]cuvscdc.EventChunk, error) {
	sql := cuvscdc.CdcLoadEventsSql(tblcfg, idx.Id)
	res, err := runSql(sqlproc, sql)
	if err != nil {
		return nil, err
	}
	defer res.Close()

	var chunks []cuvscdc.EventChunk
	for _, bat := range res.Batches {
		idVec := bat.Vecs[0]
		dataVec := bat.Vecs[1]
		for i := 0; i < bat.RowCount(); i++ {
			raw := dataVec.GetRawBytesAt(i)
			cp := make([]byte, len(raw))
			copy(cp, raw)
			chunks = append(chunks, cuvscdc.EventChunk{
				ChunkId: vector.GetFixedAtWithTypeCheck[int64](idVec, i),
				Data:    cp,
			})
		}
	}
	return chunks, nil
}

// replayEventChunks sorts the chunks by chunk_id, replays the records, and
// flattens the (deleted, overflow) replay state into the parallel slices the
// CagraModel struct carries (pkids/vecs/include layout that buildOverflow
// expects). Pass includeBytesPerRow=0 for indexes without INCLUDE columns.
func replayEventChunks[B cuvs.VectorType](
	chunks []cuvscdc.EventChunk,
	dim int,
	includeBytesPerRow int,
) ([]int64, []int64, []B, []byte, error) {
	if len(chunks) == 0 {
		return nil, nil, nil, nil, nil
	}
	cuvscdc.SortChunks(chunks)
	// The codec stores vectors as opaque bytes; the per-row byte length is
	// dim * sizeof(B). Reinterpret each row's bytes back to the native base
	// type B for the overflow brute force — no f32 detour.
	vecBytesPerRow := dim * int(util.UnsafeSizeOf[B]())
	state, err := cuvscdc.ReplayEventLog(chunks, vecBytesPerRow, includeBytesPerRow)
	if err != nil {
		return nil, nil, nil, nil, err
	}
	deletedPkids := state.Deleted
	if len(deletedPkids) == 0 {
		deletedPkids = nil
	}
	if len(state.Overflow) == 0 {
		return deletedPkids, nil, nil, nil, nil
	}
	ovPkids := make([]int64, len(state.Overflow))
	ovVecs := make([]B, len(state.Overflow)*dim)
	ovVecBytes := util.UnsafeSliceToBytes(ovVecs)
	var ovInc []byte
	if includeBytesPerRow > 0 {
		ovInc = make([]byte, len(state.Overflow)*includeBytesPerRow)
	}
	for i, e := range state.Overflow {
		ovPkids[i] = e.Pkid
		copy(ovVecBytes[i*vecBytesPerRow:(i+1)*vecBytesPerRow], e.Vec)
		if includeBytesPerRow > 0 {
			copy(ovInc[i*includeBytesPerRow:(i+1)*includeBytesPerRow], e.Include)
		}
	}
	return deletedPkids, ovPkids, ovVecs, ovInc, nil
}

// LoadMetadata loads CagraModel descriptors from the metadata table.
// Each returned model has Id, Checksum, Timestamp, and FileSize set; Index is nil.
func LoadMetadata[B, Q cuvs.VectorType](sqlproc *sqlexec.SqlProcess, dbname string, metatbl string) ([]*CagraModel[B, Q], error) {
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

	indexes := make([]*CagraModel[B, Q], 0, total)
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
			idx := &CagraModel[B, Q]{Id: id, Checksum: chksum, Timestamp: ts, FileSize: fs}
			indexes = append(indexes, idx)
		}
	}
	return indexes, nil
}
