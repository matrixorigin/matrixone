// Copyright 2023 Matrix Origin
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

package elkans_kmeans

import (
	"errors"
	"github.com/matrixorigin/matrixone/pkg/vectorize/moarray"
)

type DistanceType uint16

const (
	L2 DistanceType = iota
	InnerProduct
	CosineDistance
)

// findCorrespondingDistanceFunc returns the distance function corresponding to the distance type
// Distance function should satisfy triangle inequality.
// We use
// - L2Distance distance for L2
// - AngularDistance for InnerProduct and CosineDistance
// Ref: https://github.com/pgvector/pgvector/blob/c599f92b52d20d1555654803f8f9a4955a97bc11/src/ivfkmeans.c#L155
func findCorrespondingDistanceFunc(distType DistanceType) (DistanceFunction, error) {
	var distanceFunction DistanceFunction
	switch distType {
	case L2:
		distanceFunction = L2Distance
	case InnerProduct, CosineDistance:
		distanceFunction = AngularDistance
	default:
		return nil, errors.New("invalid distance type")
	}
	return distanceFunction, nil
}

// DistanceFunction is a function that computes the distance between two vectors
// NOTE: clusterer already ensures that the all the input vectors are of the same length,
// so we don't need to check for that here again and return error if the lengths are different.
type DistanceFunction func(v1, v2 []float64) float64

func L2Distance(v1, v2 []float64) float64 {
	val, _ := moarray.L2Distance[float64](v1, v2)
	return val
}

func AngularDistance(v1, v2 []float64) float64 {
	val, _ := moarray.AngularDistance[float64](v1, v2)
	return val
}
