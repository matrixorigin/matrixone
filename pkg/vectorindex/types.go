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

package vectorindex

import (
	"runtime"

	"github.com/bytedance/sonic"
	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/pb/plan"
	usearch "github.com/unum-cloud/usearch/golang"
)

/*
  HNSW vector index using usearch

  Multiple mini-indexes with max size 10K vectors will be build and search will run usearch.Search concurrently with all mini-indexes.
  Single model file will be splited into chunks and each chunk size is 64K maximum.
*/

const (
	MaxChunkSize = 65536
)

const (
	HNSW    = "HNSW"
	IVFFLAT = "IVFFLAT"
)

const (
	CDC_INSERT = "I"
	CDC_UPSERT = "U"
	CDC_DELETE = "D"
)

// HNSW have two secondary index tables, metadata and index storage.  For new vector index algorithm that share the same secondary tables,
// can use the same IndexTableConfig struct
type IndexTableConfig struct {
	DbName        string `json:"db"`
	SrcTable      string `json:"src"`
	MetadataTable string `json:"metadata"`
	IndexTable    string `json:"index"`
	PKey          string `json:"pkey"`
	KeyPart       string `json:"part"`
	ThreadsBuild  int64  `json:"threads_build"`
	ThreadsSearch int64  `json:"threads_search"`
	IndexCapacity int64  `json:"index_capacity"`

	// HNSW related
	UseMMap bool `json:"use_mmap"`

	// IVF related
	EntriesTable       string `json:"entries"`
	DataSize           int64  `json:"datasize"`
	Nprobe             uint   `json:"nprobe"`
	PKeyType           int32  `json:"pktype"`
	KeyPartType        int32  `json:"parttype"`
	KmeansTrainPercent int64  `json:"kmeans_train_percent"`
	KmeansMaxIteration int64  `json:"kmeans_max_iteration"`
}

// HNSW specified parameters
type HnswParam struct {
	M              string `json:"m"`
	EfConstruction string `json:"ef_construction"`
	OpType         string `json:"op_type"`
	EfSearch       string `json:"ef_search"`
	Async          string `json:"async"`
}

// IVF specified parameters
type IvfParam struct {
	Lists  string `json:"lists"`
	OpType string `json:"op_type"`
	Async  string `json:"async"`
}

type IvfflatIndexConfig struct {
	Lists      uint
	Metric     uint16
	InitType   uint16
	Dimensions uint
	Spherical  bool
	Version    int64
	VectorType int32
}

// This is generalized index config and able to share between various algorithm types.  Simply add your new configuration such as usearch.IndexConfig
type IndexConfig struct {
	Type    string
	OpType  string
	Usearch usearch.IndexConfig
	Ivfflat IvfflatIndexConfig
}

type RuntimeConfig struct {
	Limit             uint
	Probe             uint
	BackgroundQueries []*plan.Query
}

type VectorIndexCdc[T types.RealNumbers] struct {
	// Start string                   `json:"start"`
	// End   string                   `json:"end"`
	Data []VectorIndexCdcEntry[T] `json:"cdc"`
}

func NewVectorIndexCdc[T types.RealNumbers](capacity int) *VectorIndexCdc[T] {
	return &VectorIndexCdc[T]{
		Data: make([]VectorIndexCdcEntry[T], 0, capacity),
	}
}

func (h *VectorIndexCdc[T]) Reset() {
	h.Data = h.Data[:0]
}

func (h *VectorIndexCdc[T]) Empty() bool {
	return len(h.Data) == 0
}

func (h *VectorIndexCdc[T]) Full() bool {
	return len(h.Data) >= cap(h.Data)
}

func (h *VectorIndexCdc[T]) Insert(key int64, v []T) {
	e := VectorIndexCdcEntry[T]{
		Type: CDC_INSERT,
		PKey: key,
		Vec:  v,
	}

	h.Data = append(h.Data, e)
}

func (h *VectorIndexCdc[T]) Upsert(key int64, v []T) {
	e := VectorIndexCdcEntry[T]{
		Type: CDC_UPSERT,
		PKey: key,
		Vec:  v,
	}

	h.Data = append(h.Data, e)
}

func (h *VectorIndexCdc[T]) Delete(key int64) {
	e := VectorIndexCdcEntry[T]{
		Type: CDC_DELETE,
		PKey: key,
	}

	h.Data = append(h.Data, e)
}

func (h *VectorIndexCdc[T]) ToJson() (string, error) {

	b, err := sonic.Marshal(h)
	if err != nil {
		return "", err
	}
	return string(b), nil
}

type VectorIndexCdcEntry[T types.RealNumbers] struct {
	Type string `json:"t"` // I - INSERT, D - DELETE, U - UPSERT
	PKey int64  `json:"pk"`
	Vec  []T    `json:"v,omitempty"`
}

type HnswCdcParam struct {
	DbName    string    `json:"db"`
	Table     string    `json:"table"`
	MetaTbl   string    `json:"meta"`
	IndexTbl  string    `json:"index"`
	Params    HnswParam `json:"params"`
	Dimension int32     `json:"dimension"`
	VecType   int32     `json:"type"`
}

// nthread == 0, result will return NumCPU - 1
func GetConcurrency(nthread int64) int64 {
	if nthread > 0 {
		return nthread
	}
	ncpu := runtime.NumCPU()
	return int64(ncpu)
}

// nthread == 0, result will return NumCPU
func GetConcurrencyForBuild(nthread int64) int64 {
	if nthread > 0 {
		return nthread
	}
	return int64(runtime.NumCPU())
}
