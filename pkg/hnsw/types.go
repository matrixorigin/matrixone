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
	"os"

	usearch "github.com/unum-cloud/usearch/golang"
)

const (
	MaxIndexCapacity = 100000
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

type HnswBuildIndex struct {
	Index *usearch.Index
	Path  string
	Saved bool
	size  uint64
}

func NewHnswBuildIndex(cfg usearch.IndexConfig) (*HnswBuildIndex, error) {
	var err error
	idx := &HnswBuildIndex{}

	idx.Index, err = usearch.NewIndex(cfg)
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

	return nil
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
	cfg     usearch.IndexConfig
	tblcfg  IndexTableConfig
	indexes []*HnswBuildIndex
}

func NewHnswBuild(cfg usearch.IndexConfig, tblcfg IndexTableConfig) (info *HnswBuild, err error) {
	info = &HnswBuild{cfg, tblcfg, make([]*HnswBuildIndex, 0, 16)}
	return info, nil
}

func (h *HnswBuild) Destroy() error {

	var errs error
	for _, idx := range h.indexes {
		err := idx.Destroy()
		if err != nil {
			errs = errors.Join(err)
		}
	}
	h.indexes = nil
	return errs
}

func (h *HnswBuild) Add(key int64, vec []float32) error {
	var err error
	var idx *HnswBuildIndex
	if len(h.indexes) == 0 {
		idx, err = NewHnswBuildIndex(h.cfg)
		if err != nil {
			return err
		}
		h.indexes = append(h.indexes, idx)
	} else {
		// get last index
		idx = h.indexes[len(h.indexes)-1]

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
			idx, err = NewHnswBuildIndex(h.cfg)
			if err != nil {
				return err
			}
			h.indexes = append(h.indexes, idx)
		}
	}

	return idx.Add(key, vec)
}

func (h *HnswBuild) SaveToDB() error {

	os.Stderr.WriteString(fmt.Sprintf("SaveToDb len = %d\n", len(h.indexes)))
	for i, idx := range h.indexes {
		sz, err := idx.Index.Len()
		if err != nil {
			return err
		}
		os.Stderr.WriteString(fmt.Sprintf("%d: len %d\n", i, sz))
	}
	return nil
}
