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
	"strings"

	"github.com/matrixorigin/matrixone/pkg/container/types"
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
	Metric_Invalid
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
)

func GetIVFMetricType(opType string) (ret MetricType, ok bool) {
	opType = strings.ToLower(opType)
	switch opType {
	case OpType_L2Distance:
		return Metric_L2Distance, true
	case OpType_InnerProduct:
		return Metric_InnerProduct, true
	case OpType_CosineDistance:
		return Metric_CosineDistance, true
	case OpType_L1Distance:
		return Metric_L1Distance, true
	default:
		return Metric_Invalid, false
	}
}

// DistanceFunction is a function that computes the distance between two vectors
// NOTE: clusterer already ensures that the all the input vectors are of the same length,
// so we don't need to check for that here again and return error if the lengths are different.
type DistanceFunction[T types.RealNumbers] func(v1, v2 []T) (T, error)

func MaxFloat[T types.RealNumbers]() T {

	typ := reflect.TypeFor[T]()
	switch typ.Kind() {
	case reflect.Float32:
		return math.MaxFloat32
	case reflect.Float64:
		v := math.MaxFloat64
		val := reflect.ValueOf(v).Convert(typ)
		return val.Interface().(T)
	default:
		panic("MaxFloat: type not supported")
	}
}
