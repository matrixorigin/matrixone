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

package metric

import (
	"math"
	"sync"

	"github.com/matrixorigin/matrixone/pkg/common/malloc"
	"github.com/matrixorigin/matrixone/pkg/common/moerr"
	"github.com/matrixorigin/matrixone/pkg/common/util"
	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/cuvs"
)

// GPUThresholdSync is the minimum nX*nY*dim work size required to use the GPU
// when there is no I/O to overlap with (e.g. in-memory blocks). Below this
// threshold the GPU kernel-launch overhead exceeds the compute savings.
const GPUThresholdSync = uint64(4 * 1024 * 1024)

// GPUThresholdOverlapped should be used when the GPU compute is pipelined with
// synchronous block I/O. The GPU time is hidden inside the I/O wait, so even
// small workloads benefit from offloading. Pass 0 to always use the GPU for
// any supported metric.
const GPUThresholdOverlapped = uint64(0)

// GPUThresholdSQL is the threshold for SQL scalar distance functions (e.g.
// l2_distance_sq). SQL operators are typically parallelised across ~4 threads,
// each processing a smaller partition, so a threshold of GPUThresholdSync/4
// better reflects the per-thread workload at which GPU offload pays off.
const GPUThresholdSQL = GPUThresholdSync / 4

var (
	MetricTypeToCuvsMetric = map[MetricType]cuvs.DistanceType{
		Metric_L2sqDistance:   cuvs.L2Expanded,
		Metric_L2Distance:     cuvs.L2Expanded,
		Metric_InnerProduct:   cuvs.InnerProduct,
		Metric_CosineDistance: cuvs.CosineExpanded,
		Metric_L1Distance:     cuvs.L1,
	}
)

// gpuPairwiseSupported reports whether T is a cuVS-supported pairwise element
// type (float32 or types.Float16). bf16/int8/uint8/float64 run on CPU.
func gpuPairwiseSupported[T types.ArrayElement]() bool {
	switch any(*new(T)).(type) {
	case float32, types.Float16:
		return true
	default:
		return false
	}
}

func PairWiseDistance[T types.ArrayElement](
	x [][]T,
	y [][]T,
	metric MetricType,
	gpuMode bool,
) ([]float32, error) {
	// Operator opted out of GPU dispatch — fall through to the
	// existing CPU body unconditionally.
	if !gpuMode {
		return GoPairWiseDistance(x, y, metric)
	}

	nX := len(x)
	nY := len(y)
	if nX == 0 || nY == 0 {
		return nil, nil
	}
	dim := len(x[0])

	_, ok := MetricTypeToCuvsMetric[metric]
	// Use GPU only for large enough workloads where overhead is justified
	if !ok || uint64(nX)*uint64(nY)*uint64(dim) < GPUThresholdSync {
		return GoPairWiseDistance(x, y, metric)
	}

	if gpuPairwiseSupported[T]() {
		res := make([]float32, nX*nY)
		handle, err := PairwiseDistanceLaunch(x, y, metric, res, GPUThresholdSync, gpuMode)
		if err != nil {
			return nil, err
		}
		return PairwiseDistanceWait(handle, metric)
	}

	return GoPairWiseDistance(x, y, metric)
}

type gpuJob struct {
	cuvsJobID    uint64
	deallocators []malloc.Deallocator
	dist         []float32
}

type gpuJobManager struct {
	mu   sync.Mutex
	jobs map[uint64]*gpuJob
	// Go-side Job IDs for GPU tasks to avoid collision with C++ IDs
	nextID uint64
}

var globalGpuJobManager = &gpuJobManager{
	jobs:   make(map[uint64]*gpuJob),
	nextID: 1,
}

func (m *gpuJobManager) add(dist []float32) uint64 {
	m.mu.Lock()
	defer m.mu.Unlock()
	id := m.nextID
	m.nextID++
	if m.nextID >= (1 << 63) {
		m.nextID = 1
	}
	m.jobs[id] = &gpuJob{dist: dist, deallocators: make([]malloc.Deallocator, 0, 2)}
	return id
}

func (m *gpuJobManager) update(jobID uint64, cuvsID uint64, d ...malloc.Deallocator) {
	m.mu.Lock()
	defer m.mu.Unlock()
	job := m.jobs[jobID]
	if job != nil {
		job.cuvsJobID = cuvsID
		job.deallocators = append(job.deallocators, d...)
	}
}

func (m *gpuJobManager) pop(jobID uint64) *gpuJob {
	m.mu.Lock()
	defer m.mu.Unlock()
	job := m.jobs[jobID]
	if job != nil {
		delete(m.jobs, jobID)
	}
	return job
}

// PairwiseDistanceLaunch initiates an asynchronous GPU distance calculation.
// It flattens the input vectors on the CPU and then launches a CUDA kernel.
// This allows for overlapping the CPU-bound flattening work with GPU execution
// when pipelined at the reader level.
func PairwiseDistanceLaunch[T types.ArrayElement](
	x [][]T,
	y [][]T,
	metric MetricType,
	dist []float32,
	minWorkSize uint64,
	gpuMode bool,
) (PairwiseJobHandle, error) {
	// Operator opted out of GPU dispatch — use the CPU launch path,
	// the same fallback the existing threshold/type-check failures
	// would take.
	if !gpuMode {
		return PairwiseDistanceLaunchCPU(x, y, metric, dist)
	}

	nX := len(x)
	nY := len(y)
	if nX == 0 || nY == 0 {
		return 0, nil
	}
	dim := len(x[0])

	cuvsMetric, ok := MetricTypeToCuvsMetric[metric]
	if ok && uint64(nX)*uint64(nY)*uint64(dim) >= minWorkSize {
		// cuVS pairwise supports float32 and Float16 only.
		switch xs := any(x).(type) {
		case [][]float32:
			return gpuPairwiseLaunch[float32](xs, any(y).([][]float32), dim, cuvsMetric, dist, 4)
		case [][]types.Float16:
			ys := any(y).([][]types.Float16)
			// types.Float16 and cuvs.Float16 are both IEEE binary16 uint16 — a
			// per-row reinterpret (no element copy) hands them to the cuVS kernel.
			xc := make([][]cuvs.Float16, len(xs))
			for i, v := range xs {
				xc[i] = util.UnsafeSliceCast[cuvs.Float16](v)
			}
			yc := make([][]cuvs.Float16, len(ys))
			for i, v := range ys {
				yc[i] = util.UnsafeSliceCast[cuvs.Float16](v)
			}
			return gpuPairwiseLaunch[cuvs.Float16](xc, yc, dim, cuvsMetric, dist, 2)
		}
	}

	return PairwiseDistanceLaunchCPU(x, y, metric, dist)
}

func PairwiseDistanceLaunchOneToMany[T types.RealNumbers](
	query []T,
	rowCount int,
	rowAt func(int) []T,
	metric MetricType,
	dist []float32,
	minWorkSize uint64,
	gpuMode bool,
) (PairwiseJobHandle, error) {
	if !gpuMode {
		return PairwiseDistanceLaunchOneToManyCPU(
			query,
			rowCount,
			rowAt,
			metric,
			dist,
		)
	}
	if rowCount < 0 || len(dist) < rowCount {
		return 0, moerr.NewInternalErrorNoCtx(
			"pairwise distance output is smaller than the row count",
		)
	}
	if rowCount == 0 {
		return PairwiseDistanceLaunchOneToManyCPU(
			query,
			rowCount,
			rowAt,
			metric,
			dist,
		)
	}

	dim := len(query)
	work := uint64(rowCount)
	if dim != 0 && work > ^uint64(0)/uint64(dim) {
		work = ^uint64(0)
	} else {
		work *= uint64(dim)
	}
	cuvsMetric, supportedMetric := MetricTypeToCuvsMetric[metric]
	if supportedMetric &&
		work >= minWorkSize {
		if typedQuery, ok := any(query).([]float32); ok {
			return gpuPairwiseLaunchRows(
				1,
				rowCount,
				dim,
				func(_ int) []float32 {
					return typedQuery
				},
				func(row int) []float32 {
					return any(rowAt(row)).([]float32)
				},
				cuvsMetric,
				dist[:rowCount],
				4,
			)
		}
	}
	return PairwiseDistanceLaunchOneToManyCPU(
		query,
		rowCount,
		rowAt,
		metric,
		dist,
	)
}

// gpuPairwiseLaunch flattens [][]C into a C-allocator buffer (elemSize bytes per
// element) and launches the async cuVS pairwise distance. C is float32 (4B) or
// cuvs.Float16 (2B). Mirrors the old f32-only path, generalized over the element.
func gpuPairwiseLaunch[C cuvs.VectorType](
	x, y [][]C,
	dim int,
	cuvsMetric cuvs.DistanceType,
	dist []float32,
	elemSize int,
) (PairwiseJobHandle, error) {
	return gpuPairwiseLaunchRows(
		len(x),
		len(y),
		dim,
		func(row int) []C {
			return x[row]
		},
		func(row int) []C {
			return y[row]
		},
		cuvsMetric,
		dist,
		elemSize,
	)
}

func gpuPairwiseLaunchRows[C cuvs.VectorType](
	nX, nY, dim int,
	xAt, yAt func(int) []C,
	cuvsMetric cuvs.DistanceType,
	dist []float32,
	elemSize int,
) (PairwiseJobHandle, error) {
	if nX < 0 ||
		nY < 0 ||
		dim < 0 ||
		elemSize <= 0 ||
		uint64(dim) > uint64(^uint32(0)) ||
		uint64(dim) > ^uint64(0)/uint64(elemSize) {
		return 0, moerr.NewInternalErrorNoCtx(
			"pairwise distance input is too large",
		)
	}
	rowBytes := uint64(dim) * uint64(elemSize)
	if rowBytes != 0 &&
		(uint64(nX) > ^uint64(0)/rowBytes ||
			uint64(nY) > ^uint64(0)/rowBytes) {
		return 0, moerr.NewInternalErrorNoCtx(
			"pairwise distance input is too large",
		)
	}
	allocator := malloc.NewCAllocator()

	// 1. Flatten Y
	yBuf, yDeallocator, err := allocator.Allocate(
		uint64(nY)*rowBytes,
		malloc.NoClear,
	)
	if err != nil {
		return 0, err
	}
	yf := util.UnsafeSliceCast[C](yBuf)
	for i := 0; i < nY; i++ {
		v := yAt(i)
		if len(v) != dim {
			yDeallocator.Deallocate()
			return 0, moerr.NewInternalErrorNoCtx(
				"vector dimension not matched",
			)
		}
		copy(yf[i*dim:(i+1)*dim], v)
	}

	// 2. Flatten X
	xBuf, xDeallocator, err := allocator.Allocate(
		uint64(nX)*rowBytes,
		malloc.NoClear,
	)
	if err != nil {
		yDeallocator.Deallocate()
		return 0, err
	}
	xf := util.UnsafeSliceCast[C](xBuf)
	for i := 0; i < nX; i++ {
		v := xAt(i)
		if len(v) != dim {
			xDeallocator.Deallocate()
			yDeallocator.Deallocate()
			return 0, moerr.NewInternalErrorNoCtx(
				"vector dimension not matched",
			)
		}
		copy(xf[i*dim:(i+1)*dim], v)
	}

	// Register job before launch so the slot exists if Wait is called
	// concurrently. On launch failure, pop removes it before returning.
	gpuID := globalGpuJobManager.add(dist)

	cuvsID, err := cuvs.PairwiseDistanceLaunch(
		xf,
		uint64(nX),
		yf,
		uint64(nY),
		uint32(dim),
		cuvsMetric,
		dist,
	)
	if err != nil {
		xDeallocator.Deallocate()
		yDeallocator.Deallocate()
		globalGpuJobManager.pop(gpuID)
		return 0, err
	}

	globalGpuJobManager.update(gpuID, cuvsID, xDeallocator, yDeallocator)

	return PairwiseJobHandle(gpuID), nil
}

// PairwiseDistanceWait waits for the completion of the asynchronous GPU distance
// calculation initiated by Launch.
func PairwiseDistanceWait(handle PairwiseJobHandle, metric MetricType) ([]float32, error) {
	if handle&pairwiseCPUBit != 0 {
		return PairwiseDistanceWaitCPU(handle, metric)
	}

	job := globalGpuJobManager.pop(uint64(handle))
	if job == nil {
		return nil, nil
	}

	var err error
	if job.cuvsJobID != 0 {
		err = cuvs.PairwiseDistanceWait(job.cuvsJobID)
	}

	for _, d := range job.deallocators {
		d.Deallocate()
	}

	if err != nil {
		return nil, err
	}

	dist := job.dist
	if dist != nil {
		if metric == Metric_L2Distance {
			for i := range dist {
				dist[i] = float32(math.Sqrt(float64(dist[i])))
			}
		} else if metric == Metric_InnerProduct {
			for i := range dist {
				dist[i] = -dist[i]
			}
		}
		return dist, nil
	}

	return nil, nil
}
