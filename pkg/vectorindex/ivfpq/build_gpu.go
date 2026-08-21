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
	"errors"
	"fmt"
	"os"
	"strings"

	"github.com/matrixorigin/matrixone/pkg/common/moerr"
	"github.com/matrixorigin/matrixone/pkg/common/sqlquote"
	"github.com/matrixorigin/matrixone/pkg/common/util"
	"github.com/matrixorigin/matrixone/pkg/cuvs"
	"github.com/matrixorigin/matrixone/pkg/vectorindex"
	"github.com/matrixorigin/matrixone/pkg/vectorindex/memory"
)

// IvfpqBuild manages bulk index construction across one or more IvfpqModel sub-indexes.
// When the current sub-index reaches IndexCapacity it is finalized (Build), packed to a
// temp tar and released (saveToFile), and a new sub-index is created, mirroring the
// CagraBuild pattern.
//
// Retiring a sub-index at rotation rather than at ToInsertSql is what makes IndexCapacity
// bound the build. Build() already frees the sub-index's host dataset
// (flattened_host_dataset, cleared at the end of cgo/cuvs/ivf_pq.hpp build()), but the
// cuVS index and its device-side build matrix (dataset_device_ptr_) live until destroy().
// Holding every finished sub-index to the end of the scan therefore accumulated
// capacity*dim*sizeof(Q) of DEVICE memory per sub-index, so a lower capacity bought
// nothing. HnswBuild has always done it this way (hnsw/build.go getIndexForAdd +
// SaveToFile).
//
// IvfpqBuild carries two element types: base/quantizer-source B (the decoded
// source column type — f32 or f16) and storage Q (the cuVS sub-index storage
// type). For a direct index B==Q; for a quantized index (e.g. vecf16 base ->
// int8 storage) B is the base type and Q the 1-byte storage type.
//
// IvfpqBuild is single-threaded; the ivfpq_create table function runs with IsSingle=true.
type IvfpqBuild[B, Q cuvs.VectorType] struct {
	uid     string
	idxcfg  vectorindex.IndexConfig
	tblcfg  vectorindex.IndexTableConfig
	indexes []*IvfpqModel[B, Q]
	current *IvfpqModel[B, Q]
	nthread uint32
	devices []int

	// deviceBytesPerRow is the per-row DEVICE cost supplied by the create TVF,
	// which owns the per-algo model. 0 means the caller did not supply it and
	// no claim is taken -- direct API users and tests keep today's behaviour.
	deviceBytesPerRow uint64

	// hostBytesPerRow is the per-row HOST cost of the eager capacity-sized
	// staging buffers (vector staging + INCLUDE columns + per-row ids), supplied
	// by the create TVF for the same reason as deviceBytesPerRow. 0 means no
	// claim is taken, which keeps direct API users and tests as they were.
	hostBytesPerRow uint64
	// hostClaim covers the CURRENT sub-index's host buffers. Exactly one
	// sub-index is being filled at a time, so one claim tracks the whole
	// resident host footprint of this builder.
	hostClaim *memory.HostReservation

	count int64
	idBuf [1]int64

	// (B, Q) routing tags computed once at construction. bIsHalf: the base
	// type is f16. qIsHalf: the storage type is f16 (so a half base goes
	// native rather than quantized).
	bIsHalf bool
	qIsHalf bool

	// Filter column metadata (INCLUDE columns) — see CagraBuild.filterColMetaJSON.
	filterColMetaJSON string

	// tmpDir holds this build's packed tars. Destroy removes the whole directory,
	// so a failed per-file remove cannot strand a tar, and anything orphaned by a
	// crash carries the owning pid in its name.
	tmpDir string
}

func NewIvfpqBuild[B, Q cuvs.VectorType](
	uid string,
	idxcfg vectorindex.IndexConfig,
	tblcfg vectorindex.IndexTableConfig,
	nthread uint32,
	devices []int,
	spillDir string,
) (*IvfpqBuild[B, Q], error) {
	// One private directory per build. Tars land here instead of directly in
	// $TMPDIR, so Destroy reclaims them with a single RemoveAll and a crash leaves
	// files whose name identifies the owning process.
	// spillDir is the LOCAL fileservice's scratch directory (vectorindex.LocalSpillDir):
	// the packed tars are whole sub-indexes, GB-scale on a large build, and the LOCAL
	// fileservice is the provisioned data volume rather than whatever /tmp happens to
	// be mounted on. "" means no LOCAL fileservice was attached, and os.MkdirTemp
	// already reads that as $TMPDIR -- the previous behaviour, unchanged.
	tmpDir, err := os.MkdirTemp(spillDir, fmt.Sprintf("mo-ivfpq-%d-", os.Getpid()))
	if err != nil {
		return nil, err
	}

	return &IvfpqBuild[B, Q]{
		uid:     uid,
		idxcfg:  idxcfg,
		tblcfg:  tblcfg,
		indexes: make([]*IvfpqModel[B, Q], 0, 4),
		nthread: nthread,
		devices: devices,
		bIsHalf: cuvs.GetQuantization[B]() == cuvs.F16,
		qIsHalf: cuvs.GetQuantization[Q]() == cuvs.F16,
		tmpDir:  tmpDir,
	}, nil
}

func (b *IvfpqBuild[B, Q]) createKey(n int) string {
	return fmt.Sprintf("%s:%d", b.uid, n)
}

func (b *IvfpqBuild[B, Q]) getOrCreateCurrent() (*IvfpqModel[B, Q], error) {
	capacity := b.idxcfg.IndexCapacity

	// capacity == 0 means "no rotation" (one sub-index for the whole scan). Without this
	// guard `b.count >= 0` holds on every AddRow, which would retire and pack a sub-index
	// per row. Today the create TVF always resolves a positive capacity, so the guard is
	// belt-and-braces against a second provenance for the value.
	if b.current != nil && capacity > 0 && b.count >= capacity {
		// Claim the VRAM this sub-index is about to allocate before building it.
		claim, cerr := b.reserveBuildVRAM(b.count)
		if cerr != nil {
			return nil, cerr
		}
		// Safety net for the paths the explicit release below cannot cover: a panic
		// inside Build(), or any early return a later edit inserts between here and
		// it. A leaked claim is not self-correcting -- it shrinks this device's
		// budget for the life of the process and refuses every later load.
		defer func() {
			if claim != nil {
				claim.Release()
				claim = nil
			}
		}()
		err := b.current.Build()
		// Release promptly on the normal path, so the next admission sees the memory
		// through cudaMemGetInfo rather than through the ledger. Clearing claim makes
		// the deferred release a no-op; Release is idempotent regardless.
		claim.Release()
		claim = nil
		if err != nil {
			return nil, err
		}
		full := b.current
		// Hand ownership to b.indexes BEFORE packing: if saveToFile fails, Destroy() must
		// still be able to reach this model and free its GPU handle.
		b.indexes = append(b.indexes, full)
		b.current = nil
		b.count = 0
		// Pack to a temp tar and release the GPU/host residency now. ToSql() calls
		// saveToFile again later, which is a no-op once Index == nil.
		serr := full.saveToFile()
		// saveToFile drops this sub-index's GPU/host residency, so the host claim
		// is returned here rather than at build end -- otherwise a rotating build
		// would hold every sub-index's host budget at once. On failure the build
		// aborts and Destroy releases anyway; Release is idempotent.
		b.releaseHostClaim()
		if serr != nil {
			return nil, serr
		}
	}

	if b.current == nil {
		// Claim the host memory BEFORE anything allocates it. InitEmpty's native
		// constructor resizes host_ids to capacity, and SetFilterColumns resizes
		// every INCLUDE column to capacity * elem_size, so by the time either
		// returns the memory is already spent.
		hostClaim, herr := b.reserveBuildHost(capacity)
		if herr != nil {
			return nil, herr
		}
		// Roll back on EVERY exit that does not hand the claim to b.hostClaim,
		// including a panic out of the C++ constructor.
		committed := false
		defer func() {
			if !committed {
				hostClaim.Release()
			}
		}()

		key := b.createKey(len(b.indexes))
		m, err := NewIvfpqModelForBuild[B, Q](key, b.idxcfg, b.nthread, b.devices)
		if err != nil {
			return nil, err
		}
		if err = m.InitEmpty(uint64(capacity)); err != nil {
			m.Destroy()
			return nil, err
		}
		if b.filterColMetaJSON != "" {
			if err = m.Index.SetFilterColumns(b.filterColMetaJSON, uint64(capacity)); err != nil {
				m.Destroy()
				return nil, err
			}
		}
		m.TmpDir = b.tmpDir
		b.current = m
		b.count = 0
		b.hostClaim = hostClaim
		committed = true
	}

	return b.current, nil
}

// SetFilterColumns — see cagra.CagraBuild.SetFilterColumns.
func (b *IvfpqBuild[B, Q]) SetFilterColumns(colMetaJSON string) {
	b.filterColMetaJSON = colMetaJSON
}

// AddFilterChunk — see cagra.CagraBuild.AddFilterChunk.
func (b *IvfpqBuild[B, Q]) AddFilterChunk(colIdx uint32, data []byte, nullBitmap []uint32, nrows uint64) error {
	if b.current == nil {
		return moerr.NewInternalErrorNoCtx("IvfpqBuild.AddFilterChunk: no current sub-index (call AddRow first)")
	}
	return b.current.Index.AddFilterChunk(colIdx, data, nullBitmap, nrows)
}

// AddRow buffers one source row. vecBytes is the raw little-endian base-type
// bytes of one vector (4*dim for an f32 base, 2*dim for an f16 base) — the
// non-generic ivfpqBuilder interface can't name the concrete element type B, so
// the bytes are reinterpreted here with UnsafeSliceCast (zero-copy, no per-row
// heap alloc). Routing by (B, Q):
//   - f16 base, f16 storage (direct, Q==B): native AddChunk([]Q).
//   - otherwise (f32 base, or f16 base -> int8/uint8): AddChunkQuantize([]B),
//     which converts B -> Q on device (B==Q copy, or learned/cast quantizer).
func (b *IvfpqBuild[B, Q]) AddRow(id int64, vecBytes []byte) error {
	idx, err := b.getOrCreateCurrent()
	if err != nil {
		return err
	}
	b.idBuf[0] = id

	if b.bIsHalf && b.qIsHalf {
		err = idx.AddChunk(util.UnsafeSliceCast[Q](vecBytes), 1, b.idBuf[:])
	} else {
		err = idx.AddChunkQuantize(util.UnsafeSliceCast[B](vecBytes), 1, b.idBuf[:])
	}
	if err != nil {
		return err
	}
	b.count++
	return nil
}

func (b *IvfpqBuild[B, Q]) ToInsertSql(ts int64) ([]string, error) {
	if b.current != nil && b.count > 0 {
		// Claim the VRAM this sub-index is about to allocate before building it.
		claim, cerr := b.reserveBuildVRAM(b.count)
		if cerr != nil {
			return nil, cerr
		}
		// Safety net for the paths the explicit release below cannot cover: a panic
		// inside Build(), or any early return a later edit inserts between here and
		// it. A leaked claim is not self-correcting -- it shrinks this device's
		// budget for the life of the process and refuses every later load.
		defer func() {
			if claim != nil {
				claim.Release()
				claim = nil
			}
		}()
		err := b.current.Build()
		// Release promptly on the normal path, so the next admission sees the memory
		// through cudaMemGetInfo rather than through the ledger. Clearing claim makes
		// the deferred release a no-op; Release is idempotent regardless.
		claim.Release()
		claim = nil
		if err != nil {
			return nil, err
		}
		b.indexes = append(b.indexes, b.current)
		b.current = nil
	}

	if len(b.indexes) == 0 {
		return []string{}, nil
	}

	sqls := make([]string, 0, len(b.indexes)+1)
	metas := make([]string, 0, len(b.indexes))

	for _, idx := range b.indexes {
		indexsqls, err := idx.ToSql(b.tblcfg)
		if err != nil {
			return nil, err
		}
		sqls = append(sqls, indexsqls...)
		metas = append(metas, fmt.Sprintf("('%s', '%s', %d, %d)", idx.Id, idx.Checksum, ts, idx.FileSize))
	}

	metasql := fmt.Sprintf("INSERT INTO %s VALUES %s",
		sqlquote.QualifiedIdent(b.tblcfg.DbName, b.tblcfg.MetadataTable), strings.Join(metas, ", "))
	sqls = append(sqls, metasql)
	return sqls, nil
}

func (b *IvfpqBuild[B, Q]) Destroy() error {
	var errs error
	// Return the host claim first: it must leave the ledger even if a Destroy
	// below fails, or an aborted build would strand the budget for the CN's life.
	b.releaseHostClaim()
	if b.current != nil {
		if err := b.current.Destroy(); err != nil {
			errs = errors.Join(errs, err)
		}
		b.current = nil
	}
	for _, idx := range b.indexes {
		if err := idx.Destroy(); err != nil {
			errs = errors.Join(errs, err)
		}
	}
	b.indexes = nil
	// Reclaim the whole build directory in one shot: a per-file remove that failed
	// above cannot strand a tar, and the empty directory itself goes too.
	if b.tmpDir != "" {
		if err := os.RemoveAll(b.tmpDir); err != nil {
			errs = errors.Join(errs, err)
		}
		b.tmpDir = ""
	}
	return errs
}

func (b *IvfpqBuild[B, Q]) GetIndexes() []*IvfpqModel[B, Q] {
	return b.indexes
}

// SetDeviceBytesPerRow records the per-row device cost used to claim VRAM
// around each sub-index build. See the ivfpqBuilder interface for why the
// number is passed in rather than recomputed here.
func (b *IvfpqBuild[B, Q]) SetDeviceBytesPerRow(perRow uint64) {
	b.deviceBytesPerRow = perRow
}

// SetHostBytesPerRow records the per-row host cost used to claim host memory
// around each sub-index's eager capacity-sized allocation.
func (b *IvfpqBuild[B, Q]) SetHostBytesPerRow(perRow uint64) {
	b.hostBytesPerRow = perRow
}

// reserveBuildHost claims the host memory this sub-index is about to allocate
// eagerly. HostRowsFitting only answers "does it fit" from a snapshot, which two
// concurrent CREATE INDEX statements both pass before either allocates; the
// claim is what makes that decision exclusive. It is taken BEFORE InitEmpty,
// whose native constructor performs the capacity-sized resize.
//
// Returns a no-op when the per-row cost was never supplied or the capacity is
// zero: ReserveHostMemory refuses a zero claim by design and there is nothing
// to protect.
func (b *IvfpqBuild[B, Q]) reserveBuildHost(rows int64) (*memory.HostReservation, error) {
	if b.hostBytesPerRow == 0 || rows <= 0 {
		return nil, nil
	}
	return memory.ReserveHostMemory(uint64(rows)*b.hostBytesPerRow, "ivfpq build")
}

// releaseHostClaim returns the current sub-index's host claim. Safe to call when
// no claim is held; Release itself is idempotent.
func (b *IvfpqBuild[B, Q]) releaseHostClaim() {
	if b.hostClaim != nil {
		b.hostClaim.Release()
		b.hostClaim = nil
	}
}

// reserveBuildVRAM claims what this sub-index build is about to allocate, in the
// same C++ ledger index loads claim through (cgo/cuvs/device_memory.hpp). The
// claim is taken HERE, not inside the C++ build, so it spans the whole window
// where the build has decided its size but has not allocated yet -- a claim
// taken at the allocation would leave that window exactly as exposed as before.
//
// Returns a no-op release when the per-row cost was never supplied or the
// sub-index is empty: reserve() refuses a zero claim by design, and there is
// nothing to protect.
func (b *IvfpqBuild[B, Q]) reserveBuildVRAM(rows int64) (cuvs.DeviceReservations, error) {
	if b.deviceBytesPerRow == 0 || rows <= 0 || len(b.devices) == 0 {
		return nil, nil
	}
	total := uint64(rows) * b.deviceBytesPerRow
	perDev := memory.DeviceBuildBytes(
		vectorindex.DistributionMode(b.idxcfg.CuvsIvfpq.DistributionMode), b.devices, total)
	return cuvs.ReserveBuildMemory(perDev)
}
