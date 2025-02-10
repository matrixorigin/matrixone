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
	"errors"
	"fmt"
	"math"
	"os"
	"strings"
	"sync/atomic"

	"github.com/matrixorigin/matrixone/pkg/vectorindex"
	usearch "github.com/unum-cloud/usearch/golang"
)

// Hnsw Build index implementation
type HnswBuildIndex struct {
	Id    int64
	Index *usearch.Index
	Path  string
	Saved bool
	Size  int64
	Count atomic.Int64
}

type HnswBuild struct {
	cfg     vectorindex.IndexConfig
	tblcfg  vectorindex.IndexTableConfig
	indexes []*HnswBuildIndex
}

// New HnswBuildIndex struct
func NewHnswBuildIndex(id int64, cfg vectorindex.IndexConfig) (*HnswBuildIndex, error) {
	var err error
	idx := &HnswBuildIndex{}

	idx.Id = id

	idx.Index, err = usearch.NewIndex(cfg.Usearch)
	if err != nil {
		return nil, err
	}

	err = idx.Index.Reserve(vectorindex.MaxIndexCapacity)
	if err != nil {
		return nil, err
	}
	return idx, nil
}

// Destroy the struct
func (idx *HnswBuildIndex) Destroy() error {
	if idx.Index != nil {
		err := idx.Index.Destroy()
		if err != nil {
			return err
		}
		idx.Index = nil
	}

	if idx.Saved && len(idx.Path) > 0 {
		// remove the file
		err := os.Remove(idx.Path)
		if err != nil {
			return err
		}
	}
	return nil
}

// Save the index to file
func (idx *HnswBuildIndex) SaveToFile() error {

	if idx.Saved {
		return nil
	}

	f, err := os.CreateTemp("", "hnsw")
	if err != nil {
		return err
	}

	err = idx.Index.Save(f.Name())
	if err != nil {
		os.Remove(f.Name())
		return err
	}

	idx.Saved = true
	idx.Path = f.Name()
	return nil
}

// Generate the SQL to update the secondary index tables.
// 1. store the index file into the index table
func (idx *HnswBuildIndex) ToSql(cfg vectorindex.IndexTableConfig) (string, error) {

	err := idx.SaveToFile()
	if err != nil {
		return "", err
	}

	fi, err := os.Stat(idx.Path)
	if err != nil {
		return "", err
	}

	filesz := fi.Size()
	offset := int64(0)
	chunksz := int64(0)
	chunkid := int64(0)

	idx.Size = filesz

	sql := fmt.Sprintf("INSERT INTO `%s`.`%s` VALUES ", cfg.DbName, cfg.IndexTable)
	values := make([]string, 0, int64(math.Ceil(float64(filesz)/float64(vectorindex.MaxChunkSize))))
	for offset = 0; offset < filesz; {
		if offset+vectorindex.MaxChunkSize < filesz {
			chunksz = vectorindex.MaxChunkSize

		} else {
			chunksz = filesz - offset
		}

		url := fmt.Sprintf("file://%s?offset=%d&size=%d", idx.Path, offset, chunksz)
		tuple := fmt.Sprintf("(%d, %d, load_file(cast('%s' as datalink)), 0)", idx.Id, chunkid, url)
		values = append(values, tuple)

		// offset and chunksz
		offset += chunksz
		chunkid++
	}

	sql += strings.Join(values, ", ")
	return sql, nil
}

// is the index empty
func (idx *HnswBuildIndex) Empty() (bool, error) {
	sz := idx.Count.Load()
	return (sz == 0), nil
}

// check the index is full, i.e. 10K vectors
func (idx *HnswBuildIndex) Full() (bool, error) {
	sz := idx.Count.Load()
	return (sz == vectorindex.MaxIndexCapacity), nil
}

// add vector to the index
func (idx *HnswBuildIndex) Add(key int64, vec []float32) error {
	idx.Count.Add(1)
	return idx.Index.Add(uint64(key), vec)
}

// create HsnwBuild struct
func NewHnswBuild(cfg vectorindex.IndexConfig, tblcfg vectorindex.IndexTableConfig) (info *HnswBuild, err error) {
	info = &HnswBuild{cfg, tblcfg, make([]*HnswBuildIndex, 0, 16)}
	return info, nil
}

// destroy
func (h *HnswBuild) Destroy() error {

	var errs error
	for _, idx := range h.indexes {
		err := idx.Destroy()
		if err != nil {
			errs = errors.Join(errs, err)
		}
	}
	h.indexes = nil
	return errs
}

// add vector to the build
// it will check the current index is full and add the vector to available index
func (h *HnswBuild) Add(key int64, vec []float32) error {
	var err error
	var idx *HnswBuildIndex
	nidx := int64(len(h.indexes))
	if nidx == 0 {
		idx, err = NewHnswBuildIndex(nidx, h.cfg)
		if err != nil {
			return err
		}
		h.indexes = append(h.indexes, idx)
	} else {
		// get last index
		idx = h.indexes[nidx-1]

		full, err := idx.Full()
		if err != nil {
			return err
		}

		if full {
			// save the current index to file
			err = idx.SaveToFile()
			if err != nil {
				return err
			}

			// create new index
			idx, err = NewHnswBuildIndex(nidx, h.cfg)
			if err != nil {
				return err
			}
			h.indexes = append(h.indexes, idx)
		}
	}

	return idx.Add(key, vec)
}

// generate SQL to update the secondary index tables
// 1. sync the metadata table
// 2. sync the index file to index table
func (h *HnswBuild) ToInsertSql(ts int64) ([]string, error) {

	if len(h.indexes) == 0 {
		return []string{}, nil
	}

	sqls := make([]string, 0, len(h.indexes)+1)
	os.Stderr.WriteString(fmt.Sprintf("SaveToDb len = %d\n", len(h.indexes)))

	metas := make([]string, 0, len(h.indexes))
	for _, idx := range h.indexes {
		sql, err := idx.ToSql(h.tblcfg)
		if err != nil {
			return nil, err
		}

		sqls = append(sqls, sql)

		os.Stderr.WriteString(fmt.Sprintf("Sql: %s\n", sql))
		chksum, err := vectorindex.CheckSum(idx.Path)
		if err != nil {
			return nil, err
		}

		finfo, err := os.Stat(idx.Path)
		if err != nil {
			return nil, err
		}
		fs := finfo.Size()

		metas = append(metas, fmt.Sprintf("(%d, '%s', %d, %d)", idx.Id, chksum, ts, fs))
	}

	metasql := fmt.Sprintf("INSERT INTO `%s`.`%s` VALUES %s", h.tblcfg.DbName, h.tblcfg.MetadataTable, strings.Join(metas, ", "))

	sqls = append(sqls, metasql)
	return sqls, nil
}
