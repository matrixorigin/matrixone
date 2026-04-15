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

	"github.com/matrixorigin/matrixone/pkg/cuvs"
	"github.com/matrixorigin/matrixone/pkg/vectorindex"
)

// CagraBuild manages bulk index construction across one or more CagraModel sub-indexes.
// When the current sub-index reaches IndexCapacity, it is finalized (Build called) and a
// new sub-index is created, mirroring the HnswBuild pattern.
//
// CagraBuild is single-threaded; the cagra_create table function runs with IsSingle=true.
type CagraBuild[T cuvs.VectorType] struct {
	uid     string
	idxcfg  vectorindex.IndexConfig
	tblcfg  vectorindex.IndexTableConfig
	indexes []*CagraModel[T] // completed sub-indexes (Build already called)
	current *CagraModel[T]   // sub-index currently being filled
	nthread uint32
	devices []int
	count   int64   // vectors in current sub-index
	idBuf   [1]int64 // reusable buffer for AddFloat to avoid per-call heap allocation
}

// NewCagraBuild creates a new CagraBuild ready for AddFloat calls.
func NewCagraBuild[T cuvs.VectorType](
	uid string,
	idxcfg vectorindex.IndexConfig,
	tblcfg vectorindex.IndexTableConfig,
	nthread uint32,
	devices []int,
) (*CagraBuild[T], error) {
	return &CagraBuild[T]{
		uid:     uid,
		idxcfg:  idxcfg,
		tblcfg:  tblcfg,
		indexes: make([]*CagraModel[T], 0, 4),
		nthread: nthread,
		devices: devices,
	}, nil
}

func (b *CagraBuild[T]) createKey(n int) string {
	return fmt.Sprintf("%s:%d", b.uid, n)
}

// getOrCreateCurrent returns the current sub-index, creating a new one if needed.
// When the current sub-index is full it is finalized (Build called) and a new one is started.
func (b *CagraBuild[T]) getOrCreateCurrent() (*CagraModel[T], error) {
	capacity := b.tblcfg.IndexCapacity

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
		m, err := NewCagraModelForBuild[T](key, b.idxcfg, b.nthread, b.devices)
		if err != nil {
			return nil, err
		}
		if err = m.InitEmpty(uint64(capacity)); err != nil {
			m.Destroy()
			return nil, err
		}
		b.current = m
		b.count = 0
	}

	return b.current, nil
}

// AddFloat appends one float32 vector with the given int64 id.
// The internal quantization (T) is handled by AddChunkFloat.
// idBuf is reused across calls to avoid a per-call heap allocation.
func (b *CagraBuild[T]) AddFloat(id int64, vec []float32) error {
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

// ToInsertSql finalizes any in-progress sub-index, serializes all sub-indexes to the
// storage table, and returns INSERT SQL statements (storage chunks + single metadata row).
func (b *CagraBuild[T]) ToInsertSql(ts int64) ([]string, error) {
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

	metasql := fmt.Sprintf("INSERT INTO `%s`.`%s` VALUES %s",
		b.tblcfg.DbName, b.tblcfg.MetadataTable, strings.Join(metas, ", "))
	sqls = append(sqls, metasql)
	return sqls, nil
}

// Destroy frees all GPU memory and removes any temporary files.
func (b *CagraBuild[T]) Destroy() error {
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
func (b *CagraBuild[T]) GetIndexes() []*CagraModel[T] {
	return b.indexes
}
