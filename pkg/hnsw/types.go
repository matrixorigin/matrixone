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
	"crypto/md5"
	"encoding/hex"
	"errors"
	"fmt"
	"io"
	"math"
	"os"
	"strings"

	usearch "github.com/unum-cloud/usearch/golang"
)

const (
	MaxIndexCapacity = 100000
	MaxChunkSize     = 65536
)

type IndexTableConfig struct {
	DbName        string `json:"db"`
	SrcTable      string `json:"src"`
	MetadataTable string `json:"hnsw_meta"`
	IndexTable    string `json:"hnsw_index"`
}

type HnswParam struct {
	M              string `json:"m"`
	EfConstruction string `json:"ef_construction"`
	Quantization   string `json:"quantization"`
	OpType         string `json:"op_type"`
}

type IndexConfig struct {
	Type    string
	Usearch usearch.IndexConfig
}

type HnswBuildIndex struct {
	Id    int64
	Index *usearch.Index
	Path  string
	Saved bool
	size  uint64
}

func NewHnswBuildIndex(id int64, cfg IndexConfig) (*HnswBuildIndex, error) {
	var err error
	idx := &HnswBuildIndex{}

	idx.Id = id

	idx.Index, err = usearch.NewIndex(cfg.Usearch)
	if err != nil {
		return nil, err
	}

	err = idx.Index.Reserve(MaxIndexCapacity)
	if err != nil {
		return nil, err
	}
	return idx, nil
}

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

func (idx *HnswBuildIndex) ToSql(cfg IndexTableConfig) (string, error) {

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

	sql := fmt.Sprintf("INSERT INTO `%s`.`%s` VALUES ", cfg.DbName, cfg.IndexTable)
	values := make([]string, 0, int64(math.Ceil(float64(filesz)/float64(MaxChunkSize))))
	for offset = 0; offset < filesz; {
		if offset+MaxChunkSize < filesz {
			chunksz = MaxChunkSize

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

func CheckSum(path string) (string, error) {
	f, err := os.Open(path)
	if err != nil {
		return "", err
	}
	defer f.Close()

	h := md5.New()
	if _, err := io.Copy(h, f); err != nil {
		return "", err
	}
	chksum := hex.EncodeToString(h.Sum(nil))

	return chksum, nil
}

func (idx *HnswBuildIndex) Empty() (bool, error) {

	sz, err := idx.Index.Len()
	if err != nil {
		return false, err
	}

	return (sz == 0), nil
}

func (idx *HnswBuildIndex) Full() (bool, error) {

	sz, err := idx.Index.Len()
	if err != nil {
		return false, err
	}

	return (sz == MaxIndexCapacity), nil
}

func (idx *HnswBuildIndex) Add(key int64, vec []float32) error {

	return idx.Index.Add(uint64(key), vec)
}

type HnswBuild struct {
	cfg     IndexConfig
	tblcfg  IndexTableConfig
	indexes []*HnswBuildIndex
}

func NewHnswBuild(cfg IndexConfig, tblcfg IndexTableConfig) (info *HnswBuild, err error) {
	info = &HnswBuild{cfg, tblcfg, make([]*HnswBuildIndex, 0, 16)}
	return info, nil
}

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

func (h *HnswBuild) ToInsertSql(ts int64) ([]string, error) {

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
		chksum, err := CheckSum(idx.Path)
		if err != nil {
			return nil, err
		}

		metas = append(metas, fmt.Sprintf("(%d, '%s', %d)", idx.Id, chksum, ts))
	}

	metasql := fmt.Sprintf("INSERT INTO `%s`.`%s` VALUES %s", h.tblcfg.DbName, h.tblcfg.MetadataTable, strings.Join(metas, ", "))

	sqls = append(sqls, metasql)
	return sqls, nil
}
