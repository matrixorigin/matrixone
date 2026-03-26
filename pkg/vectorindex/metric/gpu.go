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
	//dim := len(x[0])

	_, ok := MetricTypeToCuvsMetric[metric]
	// Use GPU only for large enough workloads where overhead is justified
	//if !ok || uint64(nX)*uint64(nY)*uint64(dim) < 200*1024*1024 {
	if !ok {
		return GoPairWiseDistance(x, y, metric)
	}

	var zero T
	if any(zero).(interface{}) == any(float32(0)).(interface{}) {
		res := make([]float32, nX*nY)
		jobID, err := PairwiseDistanceLaunch(x, y, metric, deviceID, res)
		if err != nil {
			return nil, err
		}
		return PairwiseDistanceWait(jobID, metric)
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
	m.jobs[id] = &gpuJob{dist: dist}
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

func PairwiseDistanceLaunch[T types.RealNumbers](
	x [][]T,
	y [][]T,
	metric MetricType,
	deviceID int,
	dist []float32,
) (uint64, error) {
	nX := len(x)
	nY := len(y)
	if nX == 0 || nY == 0 {
		return 0, nil
	}
	dim := len(x[0])

	cuvsMetric, ok := MetricTypeToCuvsMetric[metric]
	var zero T
	isF32 := any(zero).(interface{}) == any(float32(0)).(interface{})

	// if ok && isF32 && uint64(nX)*uint64(nY)*uint64(dim) >= 200*1024*1024 {
	if ok && isF32 {
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

		jobID := globalGpuJobManager.add(dist)

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
			globalGpuJobManager.pop(jobID)
			return 0, err
		}

		globalGpuJobManager.update(jobID, cuvsID, xDeallocator, yDeallocator)

		return jobID, nil
	}

	return PairwiseDistanceLaunchCPU(x, y, metric, deviceID, dist)
}

func PairwiseDistanceWait(jobID uint64, metric MetricType) ([]float32, error) {
	if jobID >= (1 << 60) {
		return PairwiseDistanceWaitCPU(jobID, metric)
	}

	job := globalGpuJobManager.pop(jobID)
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
