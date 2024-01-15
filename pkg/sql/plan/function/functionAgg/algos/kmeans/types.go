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

package kmeans

import "gonum.org/v1/gonum/mat"

const DefaultRandSeed = 1

type Clusterer interface {
	InitCentroids() error
	Cluster() ([][]float64, error)
	SSE() float64
}

type DistanceType uint16

const (
	L2Distance DistanceType = iota
	//InnerProduct
	//CosineDistance
)

type InitType uint16

const (
	Random InitType = iota
	KmeansPlusPlus
)

// DistanceFunction is a function that computes the distance between two vectors
// NOTE: clusterer already ensures that the all the input vectors are of the same length,
// so we don't need to check for that here again and return error if the lengths are different.
type DistanceFunction func(v1, v2 *mat.VecDense) float64
