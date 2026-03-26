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

type pairWiseJob struct {
	dist []float32
	err  error
}

var (
	jobMap = make(map[uint64]*pairWiseJob)
	jobMu  sync.Mutex
	// Start with a very high ID to avoid collision with C++ job IDs (which start at 1)
	nextID uint64 = 1 << 60
)

// PairwiseDistanceLaunchCPU captures parameters for a pairwise distance calculation on CPU.
// While this is currently synchronous for CPU (it performs the calculation in Launch),
// it follows the asynchronous interface to support the pipelined execution model
// used in the block reader.
func PairwiseDistanceLaunchCPU[T types.RealNumbers](
	x [][]T,
	y [][]T,
	metric MetricType,
	_ int, // deviceID (ignored on CPU)
	dist []float32,
) (uint64, error) {
	distFn, err := ResolveDistanceFn[T](metric)
	if err != nil {
		return 0, err
	}

	nX := len(x)
	nY := len(y)
	if len(dist) < nX*nY {
		dist = make([]float32, nX*nY)
	}

	job := &pairWiseJob{
		dist: dist,
	}

	// Do the calculation in Launch
	switch xTyped := any(x).(type) {
	case [][]float32:
		yTyped := any(y).([][]float32)
		dFn := any(distFn).(DistanceFunction[float32])
		for r := 0; r < nX; r++ {
			xr := xTyped[r]
			for c := 0; c < nY; c++ {
				d, err := dFn(xr, yTyped[c])
				if err != nil {
					job.err = err
					goto DONE
				}
				dist[r*nY+c] = float32(d)
			}
		}
	case [][]float64:
		yTyped := any(y).([][]float64)
		dFn := any(distFn).(DistanceFunction[float64])
		for r := 0; r < nX; r++ {
			xr := xTyped[r]
			for c := 0; c < nY; c++ {
				d, err := dFn(xr, yTyped[c])
				if err != nil {
					job.err = err
					goto DONE
				}
				dist[r*nY+c] = float32(d)
			}
		}
	default:
		return 0, moerr.NewInternalErrorNoCtx("unsupported type in PairwiseDistanceLaunchCPU")
	}

	if metric == Metric_L2Distance {
		for i := range dist {
			dist[i] = float32(math.Sqrt(float64(dist[i])))
		}
	}

DONE:
	jobMu.Lock()
	id := nextID
	nextID++
	jobMap[id] = job
	jobMu.Unlock()

	return id, nil
}

// PairwiseDistanceWaitCPU returns the results of the pairwise distance calculation
// performed on the CPU.
func PairwiseDistanceWaitCPU(jobID uint64, metric MetricType) ([]float32, error) {
	jobMu.Lock()
	job, ok := jobMap[jobID]
	if !ok {
		jobMu.Unlock()
		return nil, moerr.NewInternalErrorNoCtx("invalid job ID")
	}
	delete(jobMap, jobID)
	jobMu.Unlock()

	if job.err != nil {
		return nil, job.err
	}

	return job.dist, nil
}
