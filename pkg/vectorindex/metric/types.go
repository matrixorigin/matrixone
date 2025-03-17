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
	usearch "github.com/unum-cloud/usearch/golang"
	"gonum.org/v1/gonum/mat"
)

type MetricType uint16

const (
	OpType_L2Distance     = "vector_l2_ops"
	OpType_InnerProduct   = "vector_ip_ops"
	OpType_CosineDistance = "vector_cosine_ops"
	OpType_L1Distance     = "vector_l1_ops"

	DistFn_L2Distance     = "l2_distance"
	DistFn_InnerProduct   = "inner_product"
	DistFn_CosineDistance = "cosine_distance"

	DistIntFn_L2Distance     = "l2_distance_sq"
	DistIntFn_InnerProduct   = "inner_product"
	DistIntFn_CosineDistance = "cosine_distance"
)

const (
	Metric_L2Distance MetricType = iota
	Metric_InnerProduct
	Metric_CosineDistance
	Metric_L1Distance
	Metric_TypeCount
)

var (
	DistFuncOpTypes = map[string]string{
		DistFn_L2Distance:     OpType_L2Distance,
		DistFn_InnerProduct:   OpType_InnerProduct,
		DistFn_CosineDistance: OpType_CosineDistance,
	}
	DistFuncInternalDistFunc = map[string]string{
		DistFn_L2Distance:     DistIntFn_L2Distance,
		DistFn_InnerProduct:   DistIntFn_InnerProduct,
		DistFn_CosineDistance: DistIntFn_CosineDistance,
	}

	OpTypeToIvfMetric = map[string]MetricType{
		OpType_L2Distance:     Metric_L2Distance,
		OpType_InnerProduct:   Metric_InnerProduct,
		OpType_CosineDistance: Metric_CosineDistance,
		OpType_L1Distance:     Metric_L1Distance,
	}

	OpTypeToUsearchMetric = map[string]usearch.Metric{
		OpType_L2Distance:     usearch.L2sq,
		OpType_InnerProduct:   usearch.InnerProduct,
		OpType_CosineDistance: usearch.Cosine,
		/*
			"vector_haversine_ops":  usearch.Haversine,
			"vector_divergence_ops": usearch.Divergence,
			"vector_pearson_ops":    usearch.Pearson,
			"vector_hamming_ops":    usearch.Hamming,
			"vector_tanimoto_ops":   usearch.Tanimoto,
			"vector_sorensen_ops":   usearch.Sorensen,
		*/
	}
)

// DistanceFunction is a function that computes the distance between two vectors
// NOTE: clusterer already ensures that the all the input vectors are of the same length,
// so we don't need to check for that here again and return error if the lengths are different.
type DistanceFunction func(v1, v2 *mat.VecDense) float64
