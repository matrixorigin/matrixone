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
	"github.com/matrixorigin/matrixone/pkg/common/util"
	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/cuvs"
)

// GPUThresholdSync is the minimum nX*nY*dim work size required to use the GPU
// when there is no I/O to overlap with (e.g. in-memory blocks). Below this
// threshold the GPU kernel-launch overhead exceeds the compute savings.
const GPUThresholdSync = uint64(200 * 1024 * 1024)

// GPUThresholdOverlapped should be used when the GPU compute is pipelined with
// synchronous block I/O. The GPU time is hidden inside the I/O wait, so even
// small workloads benefit from offloading. Pass 0 to always use the GPU for
// any supported metric.
const GPUThresholdOverlapped = uint64(0)

var (
	MetricTypeToCuvsMetric = map[MetricType]cuvs.DistanceType{
		Metric_L2sqDistance:   cuvs.L2Expanded,
		Metric_L2Distance:     cuvs.L2Expanded,
		Metric_InnerProduct:   cuvs.InnerProduct,
		Metric_CosineDistance: cuvs.CosineExpanded,
		Metric_L1Distance:     cuvs.L1,
	}
)

func PairWiseDistance[T types.RealNumbers](
	x [][]T,
	y [][]T,
	metric MetricType,
	deviceID int,
) ([]float32, error) {
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

	var zero T
	if _, isF32 := any(zero).(float32); isF32 {
		res := make([]float32, nX*nY)
		handle, err := PairwiseDistanceLaunch(x, y, metric, deviceID, res, GPUThresholdSync)
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
func PairwiseDistanceLaunch[T types.RealNumbers](
	x [][]T,
	y [][]T,
	metric MetricType,
	deviceID int,
	dist []float32,
	minWorkSize uint64,
) (PairwiseJobHandle, error) {
	nX := len(x)
	nY := len(y)
	if nX == 0 || nY == 0 {
		return 0, nil
	}
	dim := len(x[0])

	cuvsMetric, ok := MetricTypeToCuvsMetric[metric]
	var zero T
	_, isF32 := any(zero).(float32)

	if ok && isF32 && uint64(nX)*uint64(nY)*uint64(dim) >= minWorkSize {
		allocator := malloc.NewCAllocator()

		// 1. Flatten Y
		yf32Slice, yDeallocator, err := allocator.Allocate(uint64(nY*dim*4), malloc.NoClear)
		if err != nil {
			return 0, err
		}
		yf32 := util.UnsafeSliceCast[float32](yf32Slice)
		y32 := any(y).([][]float32)
		for i, v := range y32 {
			copy(yf32[i*dim:(i+1)*dim], v)
		}

		// 2. Flatten X
		xf32Slice, xDeallocator, err := allocator.Allocate(uint64(nX*dim*4), malloc.NoClear)
		if err != nil {
			yDeallocator.Deallocate()
			return 0, err
		}
		xf32 := util.UnsafeSliceCast[float32](xf32Slice)
		x32 := any(x).([][]float32)
		for i, v := range x32 {
			copy(xf32[i*dim:(i+1)*dim], v)
		}

		// Register job before launch so the slot exists if Wait is called
		// concurrently. On launch failure, pop removes it before returning;
		// no caller can see the job because cuvsJobID is only set by update()
		// below, which is never reached on this error path.
		gpuID := globalGpuJobManager.add(dist)

		cuvsID, err := cuvs.PairwiseDistanceLaunch(
			xf32,
			uint64(nX),
			yf32,
			uint64(nY),
			uint32(dim),
			cuvsMetric,
			deviceID,
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

	return PairwiseDistanceLaunchCPU(x, y, metric, deviceID, dist)
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
