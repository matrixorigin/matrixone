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

package brute_force

import (
	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/vectorindex/metric"
)

type BruteForceIndexIf[T types.RealNumbers] interface {
	BuildIndex(metric.VectorSetIf[T], metric.MetricType)
	SearchIndex(metric.VectorSetIf[T], uint, uint) ([]uint64, []float32, error)
	Close() error
}

type BruteForceIndex[T types.RealNumbers] struct {
	Dataset *metric.VectorSet[T]
	Queries *metric.VectorSet[T]
	Metric  metric.MetricType
}

func CreateIndex[T types.RealNumbers]() BruteForceIndexIf[T] {
	return &BruteForceIndex[T]{}
}

func (idx *BruteForceIndex[T]) BuildIndex(dataset metric.VectorSetIf[T], m metric.MetricType) {
	idx.Dataset = dataset.(*metric.VectorSet[T])
	idx.Metric = m
}

func (idx *BruteForceIndex[T]) SearchIndex(queries metric.VectorSetIf[T], maxResults uint, ncpu uint) ([]uint64, []float32, error) {
	return metric.UsearchExactSearch[T](idx.Dataset, queries, idx.Metric, maxResults, ncpu)
}

func (idx *BruteForceIndex[T]) Close() error {

	return nil
}
