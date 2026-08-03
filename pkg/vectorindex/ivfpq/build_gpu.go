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
	"strings"

	"github.com/matrixorigin/matrixone/pkg/common/moerr"
	"github.com/matrixorigin/matrixone/pkg/common/sqlquote"
	"github.com/matrixorigin/matrixone/pkg/common/util"
	"github.com/matrixorigin/matrixone/pkg/cuvs"
	"github.com/matrixorigin/matrixone/pkg/vectorindex"
)

// IvfpqBuild manages bulk index construction across one or more IvfpqModel sub-indexes.
// When the current sub-index reaches IndexCapacity, it is finalized (Build called) and a
// new sub-index is created, mirroring the CagraBuild pattern.
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
	count   int64
	idBuf   [1]int64

	// (B, Q) routing tags computed once at construction. bIsHalf: the base
	// type is f16. qIsHalf: the storage type is f16 (so a half base goes
	// native rather than quantized).
	bIsHalf bool
	qIsHalf bool

	// Filter column metadata (INCLUDE columns) — see CagraBuild.filterColMetaJSON.
	filterColMetaJSON string
}

func NewIvfpqBuild[B, Q cuvs.VectorType](
	uid string,
	idxcfg vectorindex.IndexConfig,
	tblcfg vectorindex.IndexTableConfig,
	nthread uint32,
	devices []int,
) (*IvfpqBuild[B, Q], error) {
	return &IvfpqBuild[B, Q]{
		uid:     uid,
		idxcfg:  idxcfg,
		tblcfg:  tblcfg,
		indexes: make([]*IvfpqModel[B, Q], 0, 4),
		nthread: nthread,
		devices: devices,
		bIsHalf: cuvs.GetQuantization[B]() == cuvs.F16,
		qIsHalf: cuvs.GetQuantization[Q]() == cuvs.F16,
	}, nil
}

func (b *IvfpqBuild[B, Q]) createKey(n int) string {
	return fmt.Sprintf("%s:%d", b.uid, n)
}

func (b *IvfpqBuild[B, Q]) getOrCreateCurrent() (*IvfpqModel[B, Q], error) {
	capacity := b.idxcfg.IndexCapacity

	if b.current != nil && b.count >= capacity {
		if err := b.current.Build(); err != nil {
			return nil, err
		}
		b.indexes = append(b.indexes, b.current)
		b.current = nil
		b.count = 0
	}

	if b.current == nil {
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
		b.current = m
		b.count = 0
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

func (b *IvfpqBuild[B, Q]) GetIndexes() []*IvfpqModel[B, Q] {
	return b.indexes
}
