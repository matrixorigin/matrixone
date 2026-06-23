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

type ChunkTag int64

const (
	Tag_ModelChunk ChunkTag = 0
	// Tag_CdcEvents stores an ordered event log of CDC mutations. Each chunk
	// is a sequence of op-tagged records; replay in chunk_id order produces
	// the (deleted, overflow) state used by the load path. Replaces the
	// earlier two-stream design (separate deleted-pkid list + insert
	// overflow), which lost temporal ordering between DELETE/INSERT events
	// for the same pkid. See cuvs_cdc.md for the full record format.
	Tag_CdcEvents ChunkTag = 1
)

// CdcTailId is the literal index_id under which CDC writes tag=1 event-log
// rows in the storage table for cagra/ivfpq indexes. CDC is single-threaded
// and re-index quiesces it, so this fixed sentinel removes the per-sub-index
// "active id" coordination problem entirely — search reads it once at Load
// time alongside the real sub-index models.
const CdcTailId = "cdc_tail"

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

	// IVF related
	EntriesTable       string   `json:"entries"`
	DataSize           int64    `json:"datasize"`
	Nprobe             uint     `json:"nprobe"`
	PKeyType           int32    `json:"pktype"`
	KeyPartType        int32    `json:"parttype"`
	KmeansTrainPercent float64  `json:"kmeans_train_percent"`
	KmeansMaxIteration int64    `json:"kmeans_max_iteration"`
	Limit              uint64   `json:"limit"`
	LowerBoundType     int8     `json:"lower_bound_type"`
	LowerBound         float64  `json:"lower_bound"`
	UpperBoundType     int8     `json:"upper_bound_type"`
	UpperBound         float64  `json:"upper_bound"`
	IncludeColumns     []string `json:"include_columns,omitempty"`
	IncludeColumnTypes []int32  `json:"include_column_types,omitempty"`

	// GPU related
	BatchWindow int64 `json:"batch_window"`
	// GpuMultiSimulation is a test-only knob: when >= 2, the device list is
	// replaced with physical device 0 repeated N times so SHARDED / REPLICATED
	// distribution modes can be exercised on a single-GPU host. 0/1 = real devices.
	GpuMultiSimulation int64 `json:"gpu_multi_simulation"`
}

// HNSW specified parameters
type HnswParam struct {
	M                string `json:"m"`
	EfConstruction   string `json:"ef_construction"`
	OpType           string `json:"op_type"`
	EfSearch         string `json:"ef_search"`
	Async            string `json:"async"`
	MaxIndexCapacity string `json:"max_index_capacity"`
}

// IVF specified parameters
type IvfParam struct {
	Lists              string `json:"lists"`
	OpType             string `json:"op_type"`
	Async              string `json:"async"`
	Quantization       string `json:"quantization"`
	Distribution       string `json:"distribution_mode"`
	KmeansTrainPercent string `json:"kmeans_train_percent"`
	KmeansMaxIteration string `json:"kmeans_max_iteration"`
}

// IVF-PQ specified parameters
type IvfpqParam struct {
	Lists              string `json:"lists"`
	M                  string `json:"m"`
	BitsPerCode        string `json:"bits_per_code"`
	OpType             string `json:"op_type"`
	Quantization       string `json:"quantization"`
	Distribution       string `json:"distribution_mode"`
	IncludedColumns    string `json:"included_columns"`
	KmeansTrainPercent string `json:"kmeans_train_percent"`
	KmeansMaxIteration string `json:"kmeans_max_iteration"`
	MaxIndexCapacity   string `json:"max_index_capacity"`
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
	MaxIndexCapacity       string `json:"max_index_capacity"`
}

type IvfflatIndexConfig struct {
	Lists              uint
	Metric             uint16
	InitType           uint16
	Dimensions         uint
	Spherical          bool
	Version            int64
	VectorType         int32
	KmeansTrainPercent float64
	KmeansMaxIteration int64
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
	Type          string
	OpType        string
	IndexCapacity int64
	Usearch       usearch.IndexConfig
	Ivfflat       IvfflatIndexConfig
	CuvsIvf       CuvsIvfIndexConfig
	CuvsCagra     CuvsCagraIndexConfig
	CuvsIvfpq     CuvsIvfpqIndexConfig
}

type RuntimeConfig struct {
	Limit        uint
	Probe        uint
	OrigFuncName string
	// Optional name-based filter payload for backends that can consume planner
	// filter lowering without depending on scan binding coordinates.
	FilterPayload     string
	BackgroundQueries []*plan.Query

	// FilterJSON is a JSON predicate array forwarded verbatim to CGo
	// (gpu_<idx>_search_with_filter). Empty → unfiltered search path.
	// Go never parses this payload; it's produced by the SQL layer and
	// consumed by the C++ eval_filter_bitmap_cpu.
	FilterJSON string

	// Optional raw runtime-filter payload from the build side. IVF search turns
	// this into either an exact-pk filter or a membership filter for entries.
	BloomFilter []byte
	NThreads    uint // Brute Force Index

	// Query-scoped IVF search state. These fields must not be cached on the
	// shared index object because every query can ask for different output
	// columns, push down different predicates, and advance through different
	// search rounds.
	//
	// RuntimeConfig is passed to Search by value. Pointer fields such as
	// IncludeResult and SearchCursor can be mutated by Search and observed by
	// the caller. Value fields such as SearchRoundLimit and BucketExpandStep are
	// copied into Search and do not propagate caller-visible mutations back out.
	RequestedIncludeColumns []string
	PushdownFilterSQL       string
	IncludeResult           *IvfIncludeResult
	TargetRows              uint
	SearchRoundLimit        uint
	BucketExpandStep        uint
	SearchCursor            *IvfSearchCursor
}

type IvfIncludeResult struct {
	ColNames []string
	Data     map[string][]any
	Nulls    map[string][]bool
}

type IvfSearchCursor struct {
	RankedCentroidIDs  []int64
	NextBucketOffset   uint
	CurrentBucketCount uint
	Round              uint
	Exhausted          bool
}

type VectorIndexCdc[T types.RealNumbers] struct {
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

// Insert appends a CDC INSERT event. includeBytes carries the row's INCLUDE
// column values for cuvs CAGRA / IVF-PQ in row-major + trailing null-mask
// layout (see EncodeEventRecord in cuvs_cdc.go for the format). Pass nil for
// HNSW or for indexes without INCLUDE columns.
func (h *VectorIndexCdc[T]) Insert(key int64, v []T, includeBytes []byte) {
	e := VectorIndexCdcEntry[T]{
		Type:         CDC_INSERT,
		PKey:         key,
		Vec:          v,
		IncludeBytes: includeBytes,
	}

	h.Data = append(h.Data, e)
}

// Upsert appends a CDC UPSERT event. See Insert for includeBytes semantics.
func (h *VectorIndexCdc[T]) Upsert(key int64, v []T, includeBytes []byte) {
	e := VectorIndexCdcEntry[T]{
		Type:         CDC_UPSERT,
		PKey:         key,
		Vec:          v,
		IncludeBytes: includeBytes,
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
	// IncludeBytes carries this row's INCLUDE column values (row-major
	// in column-meta order + trailing null mask). Empty/omitted for HNSW
	// and for cuvs indexes with no INCLUDE columns.
	IncludeBytes []byte `json:"i,omitempty"`
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

// SimulateDevices is a test-only seam for exercising SHARDED / REPLICATED
// distribution modes on a single-GPU host. When n >= 2 it returns a device list
// of physical device 0 repeated n times ([0,0,...]), so the cuVS worker pool
// spins up n logical ranks (each with its own stream/handle) all on device 0.
// When n < 2 the real device list is returned unchanged.
func SimulateDevices(devices []int, n int64) []int {
	if n < 2 {
		return devices
	}
	sim := make([]int, n)
	// all zeros -> every logical rank maps to physical device 0
	return sim
}
