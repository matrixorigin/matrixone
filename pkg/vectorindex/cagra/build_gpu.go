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
	"errors"
	"fmt"
	"strings"

	"github.com/matrixorigin/matrixone/pkg/common/moerr"
	"github.com/matrixorigin/matrixone/pkg/common/sqlquote"
	"github.com/matrixorigin/matrixone/pkg/common/util"
	"github.com/matrixorigin/matrixone/pkg/cuvs"
	"github.com/matrixorigin/matrixone/pkg/vectorindex"
)

// CagraBuild manages bulk index construction across one or more CagraModel sub-indexes.
// When the current sub-index reaches IndexCapacity, it is finalized (Build called) and a
// new sub-index is created, mirroring the HnswBuild pattern.
//
// CagraBuild carries two element types: base/quantizer-source B (the decoded
// source column type — f32 or f16) and storage Q (the cuVS sub-index storage
// type). For a direct index B==Q; for a quantized index (e.g. vecf16 base ->
// int8 storage) B is the base type and Q the 1-byte storage type.
//
// CagraBuild is single-threaded; the cagra_create table function runs with IsSingle=true.
type CagraBuild[B, Q cuvs.VectorType] struct {
	uid     string
	idxcfg  vectorindex.IndexConfig
	tblcfg  vectorindex.IndexTableConfig
	indexes []*CagraModel[B, Q] // completed sub-indexes (Build already called)
	current *CagraModel[B, Q]   // sub-index currently being filled
	nthread uint32
	devices []int
	count   int64    // vectors in current sub-index
	idBuf   [1]int64 // reusable buffer for AddRow to avoid per-call heap allocation

	// (B, Q) routing tags computed once at construction. bIsHalf: the base
	// type is f16. qIsHalf: the storage type is f16 (so a half base goes
	// native rather than quantized).
	bIsHalf bool
	qIsHalf bool

	// Filter column metadata (INCLUDE columns). Stashed once via SetFilterColumns
	// and re-applied to every new sub-index allocated by getOrCreateCurrent, so
	// each sub-index carries its own filter data buffer.
	filterColMetaJSON string
}

// NewCagraBuild creates a new CagraBuild ready for AddRow calls.
func NewCagraBuild[B, Q cuvs.VectorType](
	uid string,
	idxcfg vectorindex.IndexConfig,
	tblcfg vectorindex.IndexTableConfig,
	nthread uint32,
	devices []int,
) (*CagraBuild[B, Q], error) {
	return &CagraBuild[B, Q]{
		uid:     uid,
		idxcfg:  idxcfg,
		tblcfg:  tblcfg,
		indexes: make([]*CagraModel[B, Q], 0, 4),
		nthread: nthread,
		devices: devices,
		bIsHalf: cuvs.GetQuantization[B]() == cuvs.F16,
		qIsHalf: cuvs.GetQuantization[Q]() == cuvs.F16,
	}, nil
}

func (b *CagraBuild[B, Q]) createKey(n int) string {
	return fmt.Sprintf("%s:%d", b.uid, n)
}

// getOrCreateCurrent returns the current sub-index, creating a new one if needed.
// When the current sub-index is full it is finalized (Build called) and a new one is started.
func (b *CagraBuild[B, Q]) getOrCreateCurrent() (*CagraModel[B, Q], error) {
	capacity := b.idxcfg.IndexCapacity

	if b.current != nil && b.count >= capacity {
		// Current index is full: build it and retire it.
		if err := b.current.Build(); err != nil {
			return nil, err
		}
		b.indexes = append(b.indexes, b.current)
		b.current = nil
		b.count = 0
	}

	if b.current == nil {
		key := b.createKey(len(b.indexes))
		m, err := NewCagraModelForBuild[B, Q](key, b.idxcfg, b.nthread, b.devices)
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
		b.current = m
		b.count = 0
	}

	return b.current, nil
}

// SetFilterColumns registers pre-filter (INCLUDE column) metadata. The JSON
// is re-applied to each new sub-index allocated during the build. Must be
// called before the first AddRow.
func (b *CagraBuild[B, Q]) SetFilterColumns(colMetaJSON string) {
	b.filterColMetaJSON = colMetaJSON
}

// AddFilterChunk appends nrows raw filter-column bytes to the *current*
// sub-index being filled. Call once per filter column per row batch, in the
// same cadence as AddRow (which drives sub-index rotation).
// nullBitmap is a packed []uint32 (LSB-first, bit i = 1 means row i IS NULL)
// of ceil(nrows/32) entries, or nil when the chunk has no nulls.
func (b *CagraBuild[B, Q]) AddFilterChunk(colIdx uint32, data []byte, nullBitmap []uint32, nrows uint64) error {
	if b.current == nil {
		return moerr.NewInternalErrorNoCtx("CagraBuild.AddFilterChunk: no current sub-index (call AddRow first)")
	}
	return b.current.Index.AddFilterChunk(colIdx, data, nullBitmap, nrows)
}

// AddRow buffers one source row. vecBytes is the raw little-endian base-type
// bytes of one vector (4*dim for an f32 base, 2*dim for an f16 base) — the
// non-generic cagraBuilder interface can't name the concrete element type B, so
// the bytes are reinterpreted here with UnsafeSliceCast (zero-copy, no per-row
// heap alloc). Routing by (B, Q):
//   - f16 base, f16 storage (direct, Q==B): native AddChunk([]Q).
//   - otherwise (f32 base, or f16 base -> int8/uint8): AddChunkQuantize([]B),
//     which converts B -> Q on device (B==Q copy, or learned/cast quantizer).
//
// idBuf is reused across calls to avoid a per-call heap allocation.
func (b *CagraBuild[B, Q]) AddRow(id int64, vecBytes []byte) error {
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

// ToInsertSql finalizes any in-progress sub-index, serializes all sub-indexes to the
// storage table, and returns INSERT SQL statements (storage chunks + single metadata row).
func (b *CagraBuild[B, Q]) ToInsertSql(ts int64) ([]string, error) {
	// Finalize the current sub-index if it contains vectors.
	if b.current != nil && b.count > 0 {
		if err := b.current.Build(); err != nil {
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
		// ToSql calls saveToFile which packs the index to a tar file,
		// frees GPU memory, and sets idx.Checksum / idx.FileSize.
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

// Destroy frees all GPU memory and removes any temporary files.
func (b *CagraBuild[B, Q]) Destroy() error {
	var errs error
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
	return errs
}

// GetIndexes returns the completed sub-indexes (for testing).
func (b *CagraBuild[B, Q]) GetIndexes() []*CagraModel[B, Q] {
	return b.indexes
}
