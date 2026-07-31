// Copyright 2026 Matrix Origin
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

	"github.com/matrixorigin/matrixone/pkg/common/moerr"
	"github.com/matrixorigin/matrixone/pkg/container/types"
)

// PairwiseJobHandle identifies a pending pairwise-distance computation.
// It is a plain uint64 to avoid heap allocation:
//
//	bit 63 = 1  →  CPU job
//	bit 63 = 0, value ≠ 0  →  GPU job
//	value = 0  →  invalid (zero value)
//
// The map key in the CPU job store is the full handle value (CPU bit included),
// so no masking is needed on the wait side.
type PairwiseJobHandle uint64

// pairwiseCPUBit is OR'd into CPU handles to distinguish them from GPU handles.
const pairwiseCPUBit = PairwiseJobHandle(1 << 63)

// IsValid reports whether the handle refers to a real pending job.
func (h PairwiseJobHandle) IsValid() bool { return h != 0 }

type pairWiseJob struct {
	dist []float32
	err  error
}

var (
	jobMap = make(map[uint64]*pairWiseJob)
	jobMu  sync.Mutex
	nextID uint64 = 1
)

// PairwiseDistanceLaunchCPU captures parameters for a pairwise distance calculation on CPU.
// While this is currently synchronous for CPU (it performs the calculation in Launch),
// it follows the asynchronous interface to support the pipelined execution model
// used in the block reader.
func PairwiseDistanceLaunchCPU[T types.ArrayElement](
	x [][]T,
	y [][]T,
	metric MetricType,
	dist []float32,
) (PairwiseJobHandle, error) {
	// R=float32: the output is []float32, matching the prior float32(d) truncation.
	distFn, err := ResolveDistanceFn[T, float32](metric)
	if err != nil {
		return 0, err
	}

	nX := len(x)
	nY := len(y)
	if len(dist) < nX*nY {
		dist = make([]float32, nX*nY)
	}

	// One unified loop over any ArrayElement type — the resolver handles f32/f64
	// and the narrow kernels (bf16/f16/int8/uint8) uniformly.
	var jobErr error
	for r := 0; r < nX; r++ {
		xr := x[r]
		for c := 0; c < nY; c++ {
			d, err := distFn(xr, y[c])
			if err != nil {
				jobErr = err
				goto DONE
			}
			dist[r*nY+c] = d
		}
	}

	if metric == Metric_L2Distance {
		for i := range dist {
			dist[i] = float32(math.Sqrt(float64(dist[i])))
		}
	}

DONE:
	return registerPairwiseCPUJob(dist, jobErr), nil
}

// PairwiseDistanceLaunchOneToManyCPU computes the distances between one query
// and rowCount caller-owned rows. The caller supplies the output storage so the
// SQL expression path does not need a row-scaled [][]T descriptor slice or an
// additional result allocation.
func PairwiseDistanceLaunchOneToManyCPU[T types.RealNumbers](
	query []T,
	rowCount int,
	rowAt func(int) []T,
	metric MetricType,
	dist []float32,
) (PairwiseJobHandle, error) {
	if rowCount < 0 || len(dist) < rowCount {
		return 0, moerr.NewInternalErrorNoCtx(
			"pairwise distance output is smaller than the row count",
		)
	}
	dist = dist[:rowCount]
	distFn, err := ResolveDistanceFn[T, float32](metric)
	if err != nil {
		return 0, err
	}

	var jobErr error
	for row := 0; row < rowCount; row++ {
		value, err := distFn(query, rowAt(row))
		if err != nil {
			jobErr = err
			break
		}
		dist[row] = value
	}
	if jobErr == nil && metric == Metric_L2Distance {
		for idx := range dist {
			dist[idx] = float32(math.Sqrt(float64(dist[idx])))
		}
	}
	return registerPairwiseCPUJob(dist, jobErr), nil
}

func registerPairwiseCPUJob(
	dist []float32,
	err error,
) PairwiseJobHandle {
	job := &pairWiseJob{
		dist: dist,
		err:  err,
	}
	jobMu.Lock()
	id := nextID
	nextID++
	if nextID >= (1 << 63) {
		nextID = 1
	}
	handle := pairwiseCPUBit | PairwiseJobHandle(id)
	jobMap[uint64(handle)] = job
	jobMu.Unlock()

	return handle
}

// PairwiseDistanceWaitCPU returns the results of the pairwise distance calculation
// performed on the CPU.
func PairwiseDistanceWaitCPU(handle PairwiseJobHandle, metric MetricType) ([]float32, error) {
	jobMu.Lock()
	job, ok := jobMap[uint64(handle)]
	if !ok {
		jobMu.Unlock()
		return nil, moerr.NewInternalErrorNoCtx("invalid job ID")
	}
	delete(jobMap, uint64(handle))
	jobMu.Unlock()

	if job.err != nil {
		return nil, job.err
	}

	return job.dist, nil
}
