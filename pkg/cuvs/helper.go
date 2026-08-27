//go:build gpu

// Copyright 2021 - 2022 Matrix Origin
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

package cuvs

/*
#include "../../cgo/cuvs/helper.h"
#include <stdlib.h>
*/
import "C"
import (
	"runtime"
	"strings"
	"sync"
	"unsafe"

	"github.com/matrixorigin/matrixone/pkg/common/moerr"
)

// DistanceType maps to C.distance_type_t
type DistanceType C.distance_type_t

const (
	L2Expanded          DistanceType = C.DistanceType_L2Expanded
	L2SqrtExpanded      DistanceType = C.DistanceType_L2SqrtExpanded
	CosineExpanded      DistanceType = C.DistanceType_CosineExpanded
	L1                  DistanceType = C.DistanceType_L1
	L2Unexpanded        DistanceType = C.DistanceType_L2Unexpanded
	L2SqrtUnexpanded    DistanceType = C.DistanceType_L2SqrtUnexpanded
	InnerProduct        DistanceType = C.DistanceType_InnerProduct
	Linf                DistanceType = C.DistanceType_Linf
	Canberra            DistanceType = C.DistanceType_Canberra
	LpUnexpanded        DistanceType = C.DistanceType_LpUnexpanded
	CorrelationExpanded DistanceType = C.DistanceType_CorrelationExpanded
	JaccardExpanded     DistanceType = C.DistanceType_JaccardExpanded
	HellingerExpanded   DistanceType = C.DistanceType_HellingerExpanded
	Haversine           DistanceType = C.DistanceType_Haversine
	BrayCurtis          DistanceType = C.DistanceType_BrayCurtis
	JensenShannon       DistanceType = C.DistanceType_JensenShannon
	HammingUnexpanded   DistanceType = C.DistanceType_HammingUnexpanded
	KLDivergence        DistanceType = C.DistanceType_KLDivergence
	RusselRaoExpanded   DistanceType = C.DistanceType_RusselRaoExpanded
	DiceExpanded        DistanceType = C.DistanceType_DiceExpanded
	BitwiseHamming      DistanceType = C.DistanceType_BitwiseHamming
	Precomputed         DistanceType = C.DistanceType_Precomputed
	// Aliases
	CosineSimilarity DistanceType = C.DistanceType_CosineSimilarity
	Jaccard          DistanceType = C.DistanceType_Jaccard
	Hamming          DistanceType = C.DistanceType_Hamming
	Unknown          DistanceType = C.DistanceType_Unknown
)

// Quantization maps to C.quantization_t
type Quantization C.quantization_t

const (
	F32   Quantization = C.Quantization_F32
	F16   Quantization = C.Quantization_F16
	INT8  Quantization = C.Quantization_INT8
	UINT8 Quantization = C.Quantization_UINT8
)

// DistributionMode maps to C.distribution_mode_t
type DistributionMode C.distribution_mode_t

const (
	SingleGpu  DistributionMode = C.DistributionMode_SINGLE_GPU
	Sharded    DistributionMode = C.DistributionMode_SHARDED
	Replicated DistributionMode = C.DistributionMode_REPLICATED
)

// CagraBuildParams maps to C.cagra_build_params_t
type CagraBuildParams struct {
	IntermediateGraphDegree uint64
	GraphDegree             uint64
	AttachDatasetOnBuild    bool
	// QuantizerTrainLimit bounds the rows sampled to train the int8/uint8 scalar
	// quantizer. 0 = C++ default (100000).
	QuantizerTrainLimit uint64
}

func DefaultCagraBuildParams() CagraBuildParams {
	return CagraBuildParams{
		IntermediateGraphDegree: 128,
		GraphDegree:             64,
		AttachDatasetOnBuild:    true,
	}
}

// CagraSearchParams maps to C.cagra_search_params_t
type CagraSearchParams struct {
	ItopkSize   uint64
	SearchWidth uint64
}

func DefaultCagraSearchParams() CagraSearchParams {
	return CagraSearchParams{
		ItopkSize:   64,
		SearchWidth: 1,
	}
}

// IvfFlatBuildParams maps to C.ivf_flat_build_params_t
type IvfFlatBuildParams struct {
	NLists                 uint32
	AddDataOnBuild         bool
	KmeansTrainsetFraction float64
}

func DefaultIvfFlatBuildParams() IvfFlatBuildParams {
	return IvfFlatBuildParams{
		NLists:                 1024,
		AddDataOnBuild:         true,
		KmeansTrainsetFraction: 0.5,
	}
}

// IvfFlatSearchParams maps to C.ivf_flat_search_params_t
type IvfFlatSearchParams struct {
	NProbes uint32
}

func DefaultIvfFlatSearchParams() IvfFlatSearchParams {
	return IvfFlatSearchParams{
		NProbes: 20,
	}
}

// IvfPqBuildParams maps to C.ivf_pq_build_params_t
type IvfPqBuildParams struct {
	NLists                 uint32
	M                      uint32
	BitsPerCode            uint32
	AddDataOnBuild         bool
	KmeansTrainsetFraction float64
	// QuantizerTrainLimit bounds the rows sampled to train the int8/uint8 scalar
	// quantizer. 0 = C++ default (100000).
	QuantizerTrainLimit uint64
}

func DefaultIvfPqBuildParams() IvfPqBuildParams {
	return IvfPqBuildParams{
		NLists:                 1024,
		M:                      16,
		BitsPerCode:            8,
		AddDataOnBuild:         true,
		KmeansTrainsetFraction: 0.5,
	}
}

// IvfPqSearchParams maps to C.ivf_pq_search_params_t
type IvfPqSearchParams struct {
	NProbes uint32
}

func DefaultIvfPqSearchParams() IvfPqSearchParams {
	return IvfPqSearchParams{
		NProbes: 20,
	}
}

// Float16 is a 16-bit floating point type (IEEE 754-2008).
// Go does not have a native float16 type, so we use uint16 to represent its memory layout.
type Float16 uint16

// VectorType is a constraint for types that can be used as vector data.
type VectorType interface {
	float32 | Float16 | int8 | uint8
}

// GpuIndexBase is an interface for all GPU-accelerated indexes.
type GpuIndexBase interface {
	Start() error
	Build() error
	Destroy() error
	Info() (string, error)
	Cap() uint64
	Len() uint64
}

// GpuIndex is a generic interface for all GPU-accelerated indexes that support async search.
type GpuIndex[T VectorType] interface {
	SearchAsync(queries []T, numQueries uint64, dimension uint32, limit uint32) (uint64, error)
	SearchWait(jobID uint64, numQueries uint64, limit uint32) ([]int64, []float32, error)
	Destroy() error
	Cap() uint64
	Len() uint64
}

// GetQuantization returns the Quantization enum for a given VectorType.
func GetQuantization[T VectorType]() Quantization {
	var zero T
	switch any(zero).(type) {
	case float32:
		return F32
	case Float16:
		return F16
	case int8:
		return INT8
	case uint8:
		return UINT8
	default:
		panic("unsupported vector type")
	}
}

// GpuConvertF32ToF16 converts a float32 slice to a Float16 slice using the GPU.
func GpuConvertF32ToF16(src []float32, dst []Float16, deviceID int) error {
	if len(src) == 0 {
		return nil
	}
	if len(src) != len(dst) {
		return moerr.NewInternalErrorNoCtx("source and destination slices must have the same length")
	}

	var errmsg *C.char
	C.gpu_convert_f32_to_f16(
		(*C.float)(unsafe.Pointer(&src[0])),
		unsafe.Pointer(&dst[0]),
		C.uint64_t(len(src)),
		C.int(deviceID),
		unsafe.Pointer(&errmsg),
	)
	runtime.KeepAlive(src)
	runtime.KeepAlive(dst)

	if errmsg != nil {
		errStr := C.GoString(errmsg)
		C.free(unsafe.Pointer(errmsg))
		return moerr.NewInternalErrorNoCtx(errStr)
	}
	return nil
}

// GetNextGpuDeviceId returns the next GPU device ID in round-robin order
// across all visible CUDA devices. Thread-safe; the counter is global.
func GetNextGpuDeviceId() int {
	return int(C.gpu_get_next_device_id())
}

// GetGpuDeviceCount returns the number of available CUDA devices.
func GetGpuDeviceCount() (int, error) {
	count := int(C.gpu_get_device_count())
	if count < 0 {
		return 0, moerr.NewInternalErrorNoCtx("failed to get GPU device count")
	}
	return count, nil
}

// GetGpuDeviceList returns a slice of available CUDA device IDs.
func GetGpuDeviceList() ([]int, error) {
	count, err := GetGpuDeviceCount()
	if err != nil {
		return nil, err
	}
	if count == 0 {
		return []int{}, nil
	}

	cDevices := make([]C.int, count)
	C.gpu_get_device_list(&cDevices[0], C.int(count))

	devices := make([]int, count)
	for i := 0; i < count; i++ {
		devices[i] = int(cDevices[i])
	}
	runtime.KeepAlive(cDevices)
	return devices, nil
}

// IndexBudgetPercent reports an algorithm's VRAM budget fraction, read from its
// cost class rather than duplicated here, so a CREATE-time gate cannot admit
// against a different fraction than the build was sized and claimed with.
//
// indexType is the name IndexConfig.Type already carries (vectorindex.IVFPQ,
// vectorindex.CAGRA, ...); unknown names take the default.
func IndexBudgetPercent(indexType string) uint64 {
	cs := C.CString(indexType)
	defer C.free(unsafe.Pointer(cs))
	return uint64(C.gpu_index_budget_percent(cs))
}

// BudgetFor pairs an algorithm's two admission bounds, both derived from ONE
// IndexBudgetPercent call.
//
// This is the only supported way to obtain them. The permanent CREATE gate and the
// situational load gate ask the same question of different pools, and while each
// caller picked its own fraction they picked differently -- the load pre-flight
// stayed on the 75% default while IVF-PQ's own claim used 65%, so an index sized
// between the two passed the pre-flight and then threw on the first deserialize,
// with the whole artifact already downloaded. One lookup feeding both closures
// makes that mismatch unrepresentable.
//
// indexType is the name IndexConfig.Type already carries (vectorindex.IVFPQ, ...).
func BudgetFor(indexType string) DeviceBudget {
	return DeviceBudget{pct: IndexBudgetPercent(indexType)}
}

// DeviceBudget carries ONE budget fraction and derives both admission bounds from
// it. It satisfies memory.DeviceBudget structurally, which is why this package
// does not import that one -- an explicit dependency here would be an import cycle
// in memory's own GPU test.
//
// The fraction is unexported on purpose: the only way to obtain a DeviceBudget is
// BudgetFor, so the two bounds cannot be built from different fractions.
type DeviceBudget struct{ pct uint64 }

// MaxAdmissible: the fraction of TOTAL VRAM -- the permanent ceiling.
func (b DeviceBudget) MaxAdmissible(dev int) (uint64, error) {
	return DeviceMaxAdmissible(dev, b.pct)
}

// RowsFitting: the fraction of FREE VRAM -- the situational ceiling.
func (b DeviceBudget) RowsFitting(dev int, perRowBytes uint64) (int64, uint64, error) {
	return rowsFittingFreeMem(dev, perRowBytes, b.pct)
}

// DeviceMaxAdmissible reports the most VRAM any admission could EVER grant on a
// device: the governor's budget fraction of TOTAL memory.
//
// The load gate admits against that fraction of FREE memory, and free is at most
// total, so an index needing more than this can never be loaded however empty the
// card becomes. That makes it the exact threshold for refusing a build outright --
// tighter than total (which would let through indexes that always fail at query
// time) and not situational like a free-memory check.
//
// Derived in C++ from the same constants the admission path uses, so the two
// cannot drift.
// budgetPercent is the algorithm's own fraction (index_cost_base::budget_percent);
// 0 uses the governor default. Passing the index's own value keeps this gate on
// the same fraction the build was sized and admitted against.
func DeviceMaxAdmissible(deviceID int, budgetPercent uint64) (uint64, error) {
	var errmsg *C.char
	var maxAdm C.uint64_t
	rc := C.gpu_device_total_mem(C.int(deviceID), nil, &maxAdm, C.uint64_t(budgetPercent), unsafe.Pointer(&errmsg))
	if errmsg != nil {
		errStr := C.GoString(errmsg)
		C.free(unsafe.Pointer(errmsg))
		return 0, moerr.NewInternalErrorNoCtx(errStr)
	}
	if rc != 0 {
		return 0, moerr.NewInternalErrorNoCtxf("cuvs: cannot read admissible VRAM of device %d", deviceID)
	}
	return uint64(maxAdm), nil
}

// QuantizerStagingBytes reports the HOST bytes the int8/uint8 staging arena will
// occupy, from the same C++ expression prereserve_staging_arena() allocates.
//
// The build claim has to cover the arena, and the arena is allocated inside
// start(); asking here rather than recomputing min(limit, device cap, rows) in Go
// is what keeps the claim and the allocation from disagreeing.
//
// deviceID should be the PRIMARY gpu -- the arena is reserved under submit_main.
// maxRows caps it by the rows that exist; pass the source row count, an upper
// bound on the capacity the planner has not derived yet.
func QuantizerStagingBytes(deviceID int, dim, elemSize, trainLimit, maxRows uint64, indexType string) (uint64, error) {
	if dim == 0 || elemSize == 0 {
		return 0, nil
	}
	var errmsg *C.char
	n := C.gpu_quantizer_staging_bytes(
		C.int(deviceID), C.uint64_t(dim), C.uint64_t(elemSize), C.uint64_t(trainLimit),
		C.uint64_t(maxRows), C.uint64_t(IndexBudgetPercent(indexType)), unsafe.Pointer(&errmsg))
	if errmsg != nil {
		errStr := C.GoString(errmsg)
		C.free(unsafe.Pointer(errmsg))
		return 0, moerr.NewInternalErrorNoCtx(errStr)
	}
	return uint64(n), nil
}

// hostResidentComponents are the packed components that live in HOST memory when
// an index loads, not on the device: host_ids, the INCLUDE-column filter store,
// the scalar-quantizer min/max, the deleted bitset, and the manifest itself.
// Everything else -- index.bin, shard_N.bin -- is deserialized onto the GPU.
//
// Anything NOT listed counts as device-resident. That default is deliberate: a
// component added later and not classified will over-state device demand, which
// over-refuses, rather than under-state it and let a build through that cannot
// be loaded.
//
// The list is READ from the C++ that owns it (helper.cpp,
// kHostResidentComponents) rather than restated here, because the same list
// decides both governors -- the host claim in load_dir sums the host-resident
// files, and the device gates sum everything else by exclusion. With a copy on
// each side, a new component was charged twice or not at all depending on which
// copy someone remembered to update.
//
// Read once behind a sync.Once: the classification is asked per file per
// sub-index, which is far too hot for a cgo call each time.
var (
	hostResidentOnce sync.Once
	hostResidentSet  map[string]bool
)

func hostResidentComponents() map[string]bool {
	hostResidentOnce.Do(func() {
		// Static storage owned by the library: not ours to free.
		raw := C.GoString(C.gpu_host_resident_components())
		hostResidentSet = make(map[string]bool)
		for _, n := range strings.Split(raw, ",") {
			if n = strings.TrimSpace(n); n != "" {
				hostResidentSet[n] = true
			}
		}
	})
	return hostResidentSet
}

// IsHostResidentComponent reports whether a packed component stays in host
// memory rather than being deserialized onto the GPU.
func IsHostResidentComponent(name string) bool { return hostResidentComponents()[name] }

// MaxQuantizerTrainLimit is the hard ceiling on quantizer_train_limit, read from
// the C++ that enforces it (helper.h, kMaxQuantizerTrainLimit) rather than
// restated here.
//
// The native sample resolution clamps to this silently, which is the right
// backstop but the wrong answer for DDL: a CREATE INDEX that asks for more
// should be told, not quietly given less. The create paths reject against this.
func MaxQuantizerTrainLimit() uint64 {
	return uint64(C.gpu_max_quantizer_train_limit())
}

// DeviceTotalMem reports a device's TOTAL VRAM in bytes -- the hardware capacity,
// not what is currently free.
//
// Admission normally works off free memory, but "can this index EVER be searched
// on this GPU" is a different question with a stable answer: if its resident
// footprint exceeds the card itself, no amount of eviction helps. That makes
// total the right basis for refusing a build outright, where using free would
// refuse builds that merely collided with whatever else was resident at the time.
func DeviceTotalMem(deviceID int) (uint64, error) {
	var errmsg *C.char
	var total C.uint64_t
	rc := C.gpu_device_total_mem(C.int(deviceID), &total, nil, 0, unsafe.Pointer(&errmsg))
	if errmsg != nil {
		errStr := C.GoString(errmsg)
		C.free(unsafe.Pointer(errmsg))
		return 0, moerr.NewInternalErrorNoCtx(errStr)
	}
	if rc != 0 {
		return 0, moerr.NewInternalErrorNoCtxf("cuvs: cannot read total VRAM of device %d", deviceID)
	}
	return uint64(total), nil
}

// RowsFittingFreeMem reports how many rows of perRowBytes fit in ~60% of the free VRAM on
// deviceID, together with the free-byte reading used. It shares one implementation with the
// quantizer's staging bound (matrixone::rows_fitting_gpu_mem in cgo/cuvs/helper.h), so a
// build sized by this cannot disagree with a quantizer sized by that.
//
// It ERRORS rather than guessing when cudaMemGetInfo fails. A caller that cannot measure the
// device cannot size an upload for it, and the historical fallback -- assume the whole table
// fits -- is exactly the out-of-memory this is meant to prevent.
//
// Sample this BEFORE constructing any index: the RMM pool that worker->start() creates counts
// as used memory, so a later reading understates what is actually available for the build.
func RowsFittingFreeMem(deviceID int, perRowBytes uint64) (rows int64, freeBytes uint64, err error) {
	return rowsFittingFreeMem(deviceID, perRowBytes, 0)
}

func rowsFittingFreeMem(deviceID int, perRowBytes uint64, budgetPercent uint64) (rows int64, freeBytes uint64, err error) {
	if perRowBytes == 0 {
		return 0, 0, moerr.NewInternalErrorNoCtx("RowsFittingFreeMem: per-row size is 0")
	}
	var errmsg *C.char
	var cRows C.int64_t
	var cFree C.uint64_t
	rc := C.gpu_rows_fitting_free_mem(
		C.int(deviceID),
		C.uint64_t(perRowBytes),
		&cRows,
		&cFree,
		C.uint64_t(budgetPercent),
		unsafe.Pointer(&errmsg),
	)
	if errmsg != nil {
		errStr := C.GoString(errmsg)
		C.free(unsafe.Pointer(errmsg))
		return 0, 0, moerr.NewInternalErrorNoCtx(errStr)
	}
	if rc != 0 {
		return 0, 0, moerr.NewInternalErrorNoCtx("RowsFittingFreeMem: failed to query GPU memory")
	}
	return int64(cRows), uint64(cFree), nil
}

// GpuAllocPinned allocates pinned (non-pageable) host memory.
func GpuAllocPinned(size uint64) (unsafe.Pointer, error) {
	if size == 0 {
		return nil, nil
	}

	var errmsg *C.char
	ptr := C.gpu_alloc_pinned(C.uint64_t(size), unsafe.Pointer(&errmsg))

	if errmsg != nil {
		errStr := C.GoString(errmsg)
		C.free(unsafe.Pointer(errmsg))
		return nil, moerr.NewInternalErrorNoCtx(errStr)
	}
	if ptr == nil {
		return nil, moerr.NewInternalErrorNoCtx("gpu_alloc_pinned returned nil")
	}
	return ptr, nil
}

// GpuFreePinned frees pinned host memory.
func GpuFreePinned(ptr unsafe.Pointer) error {
	if ptr == nil {
		return nil
	}

	var errmsg *C.char
	C.gpu_free_pinned(ptr, unsafe.Pointer(&errmsg))

	if errmsg != nil {
		errStr := C.GoString(errmsg)
		C.free(unsafe.Pointer(errmsg))
		return moerr.NewInternalErrorNoCtx(errStr)
	}
	return nil
}

// PinnedPool is a pool of pinned memory allocations.
// It shares the same API as sync.Pool.
type PinnedPool struct {
	// New optionally specifies a function to generate
	// a value when Get would otherwise return nil.
	New func() unsafe.Pointer

	mu    sync.Mutex
	items []unsafe.Pointer
}

// NewPinnedPool creates a new PinnedPool.
// The caller must call Destroy to release all pinned memory when done.
func NewPinnedPool(newFunc func() unsafe.Pointer) *PinnedPool {
	return &PinnedPool{New: newFunc}
}

// Get selects an arbitrary item from the PinnedPool, removes it from the
// PinnedPool, and returns it to the caller.
// Get may return nil if the pool is empty and New is nil.
func (p *PinnedPool) Get() unsafe.Pointer {
	p.mu.Lock()
	if len(p.items) == 0 {
		p.mu.Unlock()
		if p.New != nil {
			return p.New()
		}
		return nil
	}
	item := p.items[len(p.items)-1]
	p.items = p.items[:len(p.items)-1]
	p.mu.Unlock()
	return item
}

// Put adds x to the pool.
func (p *PinnedPool) Put(x unsafe.Pointer) {
	if x == nil {
		return
	}
	p.mu.Lock()
	p.items = append(p.items, x)
	p.mu.Unlock()
}

// Destroy frees all pinned memory currently held in the pool.
// All items are freed regardless of individual errors; the last non-nil error is returned.
func (p *PinnedPool) Destroy() error {
	p.mu.Lock()
	defer p.mu.Unlock()
	var lastErr error
	for _, item := range p.items {
		if err := GpuFreePinned(item); err != nil {
			lastErr = err
		}
	}
	p.items = nil
	return lastErr
}
