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

package cache

import (
	"sync"

	"github.com/matrixorigin/matrixone/pkg/common/moerr"
	"github.com/matrixorigin/matrixone/pkg/hnsw"
	"github.com/matrixorigin/matrixone/pkg/vm/process"
	usearch "github.com/unum-cloud/usearch/golang"
)

var gIndexMap sync.Map

type HnswSearchIndex struct {
	Id        int64
	Path      string
	Index     *usearch.Index
	Timestamp int64
	Checksum  string
}

type HnswSearch struct {
	Mutex   sync.RWMutex
	Indexes []*HnswSearchIndex
}

func (h *HnswSearch) Search(v []float32) error {

	return nil
}

func GetIndex(proc *process.Process, key string) (*HnswSearch, error) {
	value, loaded := gIndexMap.LoadOrStore(key, &HnswSearch{})
	if !loaded {
		idx := value.(*HnswSearch)
		idx.Mutex.Lock()
		defer idx.Mutex.Unlock()

		// load model from database and if error during loading, remove the entry from gIndexMap

		return idx, nil
	}
	return value.(*HnswSearch), nil
}

func Hnsw_Search(proc *process.Process, cfg usearch.IndexConfig, tblcfg hnsw.IndexTableConfig, fp32a []float32) error {

	search, err := GetIndex(proc, tblcfg.IndexTable)
	if err != nil {
		return err
	}

	// start search
	search.Mutex.RLock()
	defer search.Mutex.RUnlock()
	if search.Indexes == nil {
		return moerr.NewInternalErrorNoCtx("HNSW cannot find index from database")
	}
	return nil
}
