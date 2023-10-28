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

import "math"

type DistanceType uint16

const (
	L2 DistanceType = iota
	InnerProduct
	CosineDistance
)

func findCorrespondingDistanceFunc(distType DistanceType) DistanceFunction {
	var distanceFunction DistanceFunction
	switch distType {
	case L2:
		distanceFunction = L2Distance
	case InnerProduct, CosineDistance:
		distanceFunction = AngularDistance
	default:
		distanceFunction = L2Distance
	}
	return distanceFunction
}

// DistanceFunction is a function that computes the distance between two vectors
// NOTE: clusterer already ensures that the all the input vectors are of the same length,
// so we don't need to check for that here again.
type DistanceFunction func(v1, v2 []float64) float64

func L2Distance(v1, v2 []float64) float64 {
	distance := 0.0
	for c := range v1 {
		distance += math.Pow(v1[c]-v2[c], 2)
	}
	return math.Sqrt(distance)
}

func AngularDistance(v1, v2 []float64) float64 {

	// Dot product and magnitude calculation
	var dotProduct, magV1, magV2 float64
	for i := range v1 {
		dotProduct += v1[i] * v2[i]
		magV1 += v1[i] * v1[i]
		magV2 += v2[i] * v2[i]
	}

	// Cosine similarity calculation
	cosTheta := dotProduct / (math.Sqrt(magV1) * math.Sqrt(magV2))

	// Clamp the value between -1 and 1 to avoid NaNs due to floating point errors
	if cosTheta > 1 {
		cosTheta = 1
	} else if cosTheta < -1 {
		cosTheta = -1
	}

	// Compute the angular distance
	return math.Acos(cosTheta)
}
