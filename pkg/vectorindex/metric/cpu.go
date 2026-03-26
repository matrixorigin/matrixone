//go:build !gpu

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
	"github.com/matrixorigin/matrixone/pkg/container/types"
)

func PairWiseDistance[T types.RealNumbers](
	x [][]T,
	y [][]T,
	metric MetricType,
	_ int,
) ([]float32, error) {
	return GoPairWiseDistance(x, y, metric)
}

func PairwiseDistanceLaunch[T types.RealNumbers](
	x [][]T,
	y [][]T,
	metric MetricType,
	deviceID int,
	dist []float32,
) (uint64, error) {
	return PairwiseDistanceLaunchCPU(x, y, metric, deviceID, dist)
}

func PairwiseDistanceWait(jobID uint64, metric MetricType) ([]float32, error) {
	return PairwiseDistanceWaitCPU(jobID, metric)
}
