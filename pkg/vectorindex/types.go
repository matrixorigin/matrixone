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
	"math"
	"runtime"
	"strings"

	"github.com/bytedance/sonic"
	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/pb/plan"
	"github.com/matrixorigin/matrixone/pkg/vectorindex/metric"
	usearch "github.com/unum-cloud/usearch/golang"
)

// QuantizationToVectorType maps a CREATE INDEX QUANTIZATION='...' value to the
// vector element type the ivfflat ENTRIES are down-cast to (the base column and
// centroids are unaffected). The accepted names are the canonical
// metric.Quantization_*_Str constants (case-insensitive): float32 -> vecf32,
// float16 -> vecf16, bf16 -> vecbf16, int8 -> vecint8. float32/float16/bf16 are
// float formats (plain cast); int8 uses the trained scalar quantizer. float32 is
// accepted because it is a real down-cast for an f64 base; float64 (an up-cast)
// and uint8 / "" return ok=false (no quantization; entries keep the base type).
func QuantizationToVectorType(q string) (types.T, bool) {
	switch strings.ToLower(strings.TrimSpace(q)) {
	case metric.Quantization_F32_Str:
		return types.T_array_float32, true
	case metric.Quantization_F16_Str:
		return types.T_array_float16, true
	case metric.Quantization_BF16_Str:
		return types.T_array_bf16, true
	case metric.Quantization_INT8_Str:
		return types.T_array_int8, true
	}
	return 0, false
}

// Int8QuantizeParams returns (mul, add) for the cuVS-style asymmetric int8 scalar
// quantizer that maps [min,max] onto the full int8 range [-128,127]:
//
//	q(x) = round(x*mul + add), clamped to [-128,127]
//
// add folds the -min offset and the -128 int8 shift into one constant, so both
// the build (cast(base*mul+add as vecint8)) and search apply it with a single
// multiply-add. A degenerate range (max<=min) falls back to identity.
func Int8QuantizeParams(min, max float64) (mul, add float64) {
	rng := max - min
	// !(rng > 0) also rejects NaN (every NaN comparison is false), and the IsInf
	// guard rejects a non-finite range — either would otherwise yield NaN/Inf mul
	// that poisons the build SQL and the query transform.
	if !(rng > 0) || math.IsInf(rng, 0) {
		return 1.0, 0.0
	}
	mul = 255.0 / rng
	add = -min*mul - 128.0
	return mul, add
}

// QuantizationSQLTypeName returns the SQL type name for a vector element type,
// for use in CAST(... AS <name>(dim)).
func QuantizationSQLTypeName(t types.T) string {
	switch t {
	case types.T_array_float32:
		return "vecf32"
	case types.T_array_float64:
		return "vecf64"
	case types.T_array_bf16:
		return "vecbf16"
	case types.T_array_float16:
		return "vecf16"
	case types.T_array_int8:
		return "vecint8"
	}
	return ""
}

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
	EntriesTable   string  `json:"entries"`
	DataSize       int64   `json:"datasize"`
	Nprobe         uint    `json:"nprobe"`
	PKeyType       int32   `json:"pktype"`
	KeyPartType    int32   `json:"parttype"`
	Limit          uint64  `json:"limit"`
	LowerBoundType int8    `json:"lower_bound_type"`
	LowerBound     float64 `json:"lower_bound"`
	UpperBoundType int8    `json:"upper_bound_type"`
	UpperBound     float64 `json:"upper_bound"`

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
	Lists      uint
	Metric     uint16
	InitType   uint16
	Dimensions uint
	Spherical  bool
	Version    int64
	VectorType int32
	// CentroidType is the element type the centroid hidden table is stored in.
	// Entries always keep VectorType (the input/quantization type); centroids may
	// be f32 (decoupled — best recall, fast f32 SIMD search, negligible RAM for
	// few centroids) or follow VectorType (least RAM, narrow-native search). 0 ==
	// unset is treated as T_array_float32. (cuVS allows the same choice.)
	CentroidType       int32
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
