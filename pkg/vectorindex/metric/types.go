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
	"math"
	"reflect"

	"github.com/matrixorigin/matrixone/pkg/container/types"
	usearch "github.com/unum-cloud/usearch/golang"
)

type MetricType uint16

const (
	OpType_L2Distance     = "vector_l2_ops"
	OpType_L2sqDistance   = "vector_l2sq_ops"
	OpType_InnerProduct   = "vector_ip_ops"
	OpType_CosineDistance = "vector_cosine_ops"
	OpType_L1Distance     = "vector_l1_ops"

	DistFn_L2Distance     = "l2_distance"
	DistFn_L2sqDistance   = "l2_distance_sq"
	DistFn_InnerProduct   = "inner_product"
	DistFn_CosineDistance = "cosine_distance"
	DistFn_L1Distance     = "l1_distance"

	DistIntFn_L2Distance     = "l2_distance_sq"
	DistIntFn_InnerProduct   = "inner_product"
	DistIntFn_CosineDistance = "cosine_distance"
	DistIntFn_L1Distance     = "l1_distance"
)

const (
	Metric_L2Distance MetricType = iota
	Metric_L2sqDistance
	Metric_InnerProduct
	Metric_CosineDistance
	Metric_L1Distance
	Metric_TypeCount
)

var (
	DistFuncOpTypes = map[string]string{
		DistFn_L2Distance:     OpType_L2Distance,
		DistFn_L2sqDistance:   OpType_L2Distance,
		DistFn_InnerProduct:   OpType_InnerProduct,
		DistFn_CosineDistance: OpType_CosineDistance,
	}

	OpTypeToIvfMetric = map[string]MetricType{
		OpType_L2Distance:     Metric_L2sqDistance,
		OpType_L2sqDistance:   Metric_L2sqDistance,
		OpType_InnerProduct:   Metric_InnerProduct,
		OpType_CosineDistance: Metric_CosineDistance,
		OpType_L1Distance:     Metric_L1Distance,
	}

	OpTypeToUsearchMetric = map[string]usearch.Metric{
		OpType_L2Distance:     usearch.L2sq,
		OpType_L2sqDistance:   usearch.L2sq,
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

	MetricTypeToUsearchMetric = map[MetricType]usearch.Metric{
		Metric_L2Distance:     usearch.L2sq,
		Metric_L2sqDistance:   usearch.L2sq,
		Metric_InnerProduct:   usearch.InnerProduct,
		Metric_CosineDistance: usearch.Cosine,
	}

	MetricTypeToDistFuncName = map[MetricType]string{
		Metric_L2Distance:     DistFn_L2Distance,
		Metric_L2sqDistance:   DistFn_L2sqDistance,
		Metric_InnerProduct:   DistFn_InnerProduct,
		Metric_CosineDistance: DistFn_CosineDistance,
		Metric_L1Distance:     DistFn_L1Distance,
	}

	DistFuncNameToMetricType = map[string]MetricType{
		DistFn_L2Distance:     Metric_L2Distance,
		DistFn_L2sqDistance:   Metric_L2sqDistance,
		DistFn_InnerProduct:   Metric_InnerProduct,
		DistFn_CosineDistance: Metric_CosineDistance,
		DistFn_L1Distance:     Metric_L1Distance,
	}
)

// DistanceFunction is a function that computes the distance between two vectors
// NOTE: clusterer already ensures that the all the input vectors are of the same length,
// so we don't need to check for that here again and return error if the lengths are different.
type DistanceFunction[T types.RealNumbers] func(v1, v2 []T) (T, error)

func MaxFloat[T types.RealNumbers]() T {

	typ := reflect.TypeFor[T]()
	switch typ.Kind() {
	case reflect.Float32:
		return T(math.MaxFloat32)
	case reflect.Float64:
		v := math.MaxFloat64
		val := reflect.ValueOf(v).Convert(typ)
		return val.Interface().(T)
	default:
		panic("MaxFloat: type not supported")
	}
}

func DistanceTransformHnsw(dist float64, origMetricType MetricType, metricType usearch.Metric) float64 {
	if origMetricType == Metric_L2Distance && metricType == usearch.L2sq {
		// metric is l2sq but origin is l2_distance
		return math.Sqrt(dist)
	}
	return dist
}

func DistanceTransformIvfflat(dist float64, origMetricType, metricType MetricType) float64 {
	if origMetricType == Metric_L2Distance && metricType == Metric_L2sqDistance {
		// metric is l2sq but origin is l2_distance
		return math.Sqrt(dist)
	}
	return dist
}
