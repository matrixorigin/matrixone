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

package elkans

import (
	"github.com/matrixorigin/matrixone/pkg/sql/plan/function/functionAgg/algos/kmeans"
	"gonum.org/v1/gonum/mat"
	"math/rand"
)

type Initializer interface {
	InitCentroids(vectors []*mat.VecDense, k int) (centroids []*mat.VecDense)
}

var _ Initializer = (*Random)(nil)

// Random initializes the centroids with random centroids from the vector list.
// As mentioned in the FAISS discussion here https://github.com/facebookresearch/faiss/issues/268#issuecomment-348184505
// "We have not implemented Kmeans++ in Faiss, because with our former Yael library, which implements both k-means++
// and regular random initialization, we observed that the overhead computational cost was not
// worth the saving (negligible) in all large-scale settings we have considered."
type Random struct {
	rand rand.Rand
}

func NewRandomInitializer() Initializer {
	return &Random{
		rand: *rand.New(rand.NewSource(kmeans.DefaultRandSeed)),
	}
}

func (r *Random) InitCentroids(vectors []*mat.VecDense, k int) (centroids []*mat.VecDense) {
	centroids = make([]*mat.VecDense, k)
	for i := 0; i < k; i++ {
		randIdx := r.rand.Intn(len(vectors))
		centroids[i] = vectors[randIdx]
	}
	return centroids
}
