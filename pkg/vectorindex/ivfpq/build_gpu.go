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

	"github.com/matrixorigin/matrixone/pkg/cuvs"
	"github.com/matrixorigin/matrixone/pkg/vectorindex"
)

// IvfpqBuild manages bulk index construction across one or more IvfpqModel sub-indexes.
// When the current sub-index reaches IndexCapacity, it is finalized (Build called) and a
// new sub-index is created, mirroring the CagraBuild pattern.
//
// IvfpqBuild is single-threaded; the ivfpq_create table function runs with IsSingle=true.
type IvfpqBuild[T cuvs.VectorType] struct {
	uid     string
	idxcfg  vectorindex.IndexConfig
	tblcfg  vectorindex.IndexTableConfig
	indexes []*IvfpqModel[T]
	current *IvfpqModel[T]
	nthread uint32
	devices []int
	count   int64
	idBuf   [1]int64

	// Filter column metadata (INCLUDE columns) — see CagraBuild.filterColMetaJSON.
	filterColMetaJSON string
}

func NewIvfpqBuild[T cuvs.VectorType](
	uid string,
	idxcfg vectorindex.IndexConfig,
	tblcfg vectorindex.IndexTableConfig,
	nthread uint32,
	devices []int,
) (*IvfpqBuild[T], error) {
	return &IvfpqBuild[T]{
		uid:     uid,
		idxcfg:  idxcfg,
		tblcfg:  tblcfg,
		indexes: make([]*IvfpqModel[T], 0, 4),
		nthread: nthread,
		devices: devices,
	}, nil
}

func (b *IvfpqBuild[T]) createKey(n int) string {
	return fmt.Sprintf("%s:%d", b.uid, n)
}

func (b *IvfpqBuild[T]) getOrCreateCurrent() (*IvfpqModel[T], error) {
	capacity := b.tblcfg.IndexCapacity

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
		m, err := NewIvfpqModelForBuild[T](key, b.idxcfg, b.nthread, b.devices)
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
func (b *IvfpqBuild[T]) SetFilterColumns(colMetaJSON string) {
	b.filterColMetaJSON = colMetaJSON
}

// AddFilterChunk — see cagra.CagraBuild.AddFilterChunk.
func (b *IvfpqBuild[T]) AddFilterChunk(colIdx uint32, data []byte, nullBitmap []uint32, nrows uint64) error {
	if b.current == nil {
		return fmt.Errorf("IvfpqBuild.AddFilterChunk: no current sub-index (call AddFloat first)")
	}
	return b.current.Index.AddFilterChunk(colIdx, data, nullBitmap, nrows)
}

func (b *IvfpqBuild[T]) AddFloat(id int64, vec []float32) error {
	idx, err := b.getOrCreateCurrent()
	if err != nil {
		return err
	}
	b.idBuf[0] = id
	if err = idx.AddChunkFloat(vec, 1, b.idBuf[:]); err != nil {
		return err
	}
	b.count++
	return nil
}

func (b *IvfpqBuild[T]) ToInsertSql(ts int64) ([]string, error) {
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

	metasql := fmt.Sprintf("INSERT INTO `%s`.`%s` VALUES %s",
		b.tblcfg.DbName, b.tblcfg.MetadataTable, strings.Join(metas, ", "))
	sqls = append(sqls, metasql)
	return sqls, nil
}

func (b *IvfpqBuild[T]) Destroy() error {
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

func (b *IvfpqBuild[T]) GetIndexes() []*IvfpqModel[T] {
	return b.indexes
}
