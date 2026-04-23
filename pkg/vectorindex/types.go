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
	IVFPQ   = "IVFPQ"
	CAGRA   = "CAGRA"
)

const (
	CDC_INSERT = "I"
	CDC_UPSERT = "U"
	CDC_DELETE = "D"
)

type DistributionMode uint16

const (
	DistributionMode_SINGLE_GPU DistributionMode = iota
	DistributionMode_SHARDED
	DistributionMode_REPLICATED
)

const (
	DistributionMode_SINGLE_GPU_Str = "single"
	DistributionMode_SHARDED_Str    = "sharded"
	DistributionMode_REPLICATED_Str = "replicated"
)

func ValidDistributionMode(val string) bool {
	lists := []string{DistributionMode_SINGLE_GPU_Str,
		DistributionMode_SHARDED_Str,
		DistributionMode_REPLICATED_Str}

	for _, mode := range lists {
		if mode == val {
			return true
		}
	}
	return false
}

// HNSW have two secondary index tables, metadata and index storage.  For new vector index algorithm that share the same secondary tables,
// can use the same IndexTableConfig struct
type IndexTableConfig struct {
	DbName        string `json:"db"`
	SrcTable      string `json:"src"`
	MetadataTable string `json:"metadata"`
	IndexTable    string `json:"index"`
	PKey          string `json:"pkey"`
	KeyPart       string `json:"part"`
	OrigFuncName  string `json:"orig_func_name"`
	ThreadsBuild  int64  `json:"threads_build"`
	ThreadsSearch int64  `json:"threads_search"`
	IndexCapacity int64  `json:"index_capacity"`

	// IVF related
	EntriesTable       string  `json:"entries"`
	DataSize           int64   `json:"datasize"`
	Nprobe             uint    `json:"nprobe"`
	PKeyType           int32   `json:"pktype"`
	KeyPartType        int32   `json:"parttype"`
	KmeansTrainPercent float64 `json:"kmeans_train_percent"`
	KmeansMaxIteration int64   `json:"kmeans_max_iteration"`
	Limit              uint64  `json:"limit"`
	LowerBoundType     int8    `json:"lower_bound_type"`
	LowerBound         float64 `json:"lower_bound"`
	UpperBoundType     int8    `json:"upper_bound_type"`
	UpperBound         float64 `json:"upper_bound"`

	// GPU related
	BatchWindow int64 `json:"batch_window"`
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
	Lists        string `json:"lists"`
	OpType       string `json:"op_type"`
	Async        string `json:"async"`
	Quantization string `json:"quantization"`
	Distribution string `json:"distribution_mode"`
}

// IVF-PQ specified parameters
type IvfpqParam struct {
	Lists           string `json:"lists"`
	M               string `json:"m"`
	BitsPerCode     string `json:"bits_per_code"`
	OpType          string `json:"op_type"`
	Quantization    string `json:"quantization"`
	Distribution    string `json:"distribution_mode"`
	IncludedColumns string `json:"included_columns"`
}

// CAGRA specified parameters
type CagraParam struct {
	M                      string `json:"m"`
	EfConstruction         string `json:"ef_construction"`
	OpType                 string `json:"op_type"`
	EfSearch               string `json:"ef_search"`
	Async                  string `json:"async"`
	Quantization           string `json:"quantization"`
	Distribution           string `json:"distribution_mode"`
	IntermediateGraphDegee string `json:"intermediate_graph_degree"`
	GraphDegee             string `json:"graph_degree"`
	ITopkSize              string `json:"itopk_size"`
	IncludedColumns        string `json:"included_columns"`
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

type CuvsIvfIndexConfig struct {
	Lists            uint
	Metric           uint16
	InitType         uint16
	Dimensions       uint
	Spherical        bool
	Version          int64
	VectorType       int32
	Quantization     uint16
	DistributionMode uint16
}

type CuvsCagraIndexConfig struct {
	IntermediateGraphDegree uint64
	GraphDegree             uint64
	ITopkSize               uint64
	Metric                  uint16
	Dimensions              uint
	Version                 int64
	VectorType              int32
	Quantization            uint16
	DistributionMode        uint16
	IncludedColumns         []string
}

type CuvsIvfpqIndexConfig struct {
	Lists                  uint
	M                      uint
	BitsPerCode            uint
	Metric                 uint16
	Dimensions             uint
	Quantization           uint16
	DistributionMode       uint16
	Version                int64
	KmeansTrainsetFraction float64
	IncludedColumns        []string
}

// This is generalized index config and able to share between various algorithm types.  Simply add your new configuration such as usearch.IndexConfig
type IndexConfig struct {
	Type      string
	OpType    string
	Usearch   usearch.IndexConfig
	Ivfflat   IvfflatIndexConfig
	CuvsIvf   CuvsIvfIndexConfig
	CuvsCagra CuvsCagraIndexConfig
	CuvsIvfpq CuvsIvfpqIndexConfig
}

type RuntimeConfig struct {
	Limit             uint
	Probe             uint
	OrigFuncName      string
	BackgroundQueries []*plan.Query
	NThreads          uint // Brute Force Index

	// FilterJSON is a JSON predicate array forwarded verbatim to CGo
	// (gpu_<idx>_search_with_filter). Empty → unfiltered search path.
	// Go never parses this payload; it's produced by the SQL layer and
	// consumed by the C++ eval_filter_bitmap_cpu.
	FilterJSON string
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
