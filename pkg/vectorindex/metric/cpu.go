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

func NewVectorSet[T types.RealNumbers](count uint, dimension uint, elemSize uint) *VectorSet[T] {
	c := &VectorSet[T]{Count: count, Dimension: dimension, Elemsz: elemSize}
	c.Idxmap = make(map[int64]int64, count*dimension)
	c.Vector = make([]T, count*dimension)
	return c
}

func ExactSearch[T types.RealNumbers](_dataset, _queries VectorSetIf[T],
	metric MetricType,
	maxResults uint,
	ncpu uint) (keys []uint64, distances []float32, err error) {

	return UsearchExactSearch[T](_dataset, _queries, metric, maxResults, ncpu)
}
