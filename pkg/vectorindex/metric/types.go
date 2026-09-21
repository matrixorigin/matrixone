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

type QuantizationType uint16

const (
	Quantization_F32 QuantizationType = iota
	Quantization_F16
	Quantization_INT8
	Quantization_UINT8
	Quantization_F64
)

const (
	Quantization_F32_Str   = "float32"
	Quantization_F16_Str   = "float16"
	Quantization_BF16_Str  = "bf16"
	Quantization_INT8_Str  = "int8"
	Quantization_UINT8_Str = "uint8"
	Quantization_F64_Str   = "float64"
)

// UsearchQuantizationNameToType maps a SQL quantization name to its
// enum value for the usearch (HNSW) backend, which supports float32,
// float16, float64, int8, and uint8.
var UsearchQuantizationNameToType = map[string]QuantizationType{
	Quantization_F32_Str:   Quantization_F32,
	Quantization_F16_Str:   Quantization_F16,
	Quantization_F64_Str:   Quantization_F64,
	Quantization_INT8_Str:  Quantization_INT8,
	Quantization_UINT8_Str: Quantization_UINT8,
}

// CuvsQuantizationNameToType is the analogous map for the cuvs
// (CAGRA / IVF-PQ) backend. cuvs does NOT support float64, so f64
// is intentionally omitted; including it here would let CREATE INDEX
// pass the validator and then fail downstream in the GPU code path.
var CuvsQuantizationNameToType = map[string]QuantizationType{
	Quantization_F32_Str:   Quantization_F32,
	Quantization_F16_Str:   Quantization_F16,
	Quantization_INT8_Str:  Quantization_INT8,
	Quantization_UINT8_Str: Quantization_UINT8,
}

// ValidQuantization gates the QUANTIZATION='X' option in CREATE INDEX
// for CAGRA / IVF-PQ — both cuvs-backed, so the cuvs map is the
// source of truth. See pkg/catalog/secondary_index_utils.go.
func ValidQuantization(val string) bool {
	_, ok := CuvsQuantizationNameToType[val]
	return ok
}

var (
	// DistFuncOpTypes maps an indexable SQL distance function to the op_type an index
	// is given by default for it. Membership doubles as "this distance function can be
	// served by a vector index at all". It is NOT the index-matching test: an index may
	// carry a different but equivalent op_type — use OpTypeServesDistFunc for that.
	DistFuncOpTypes = map[string]string{
		DistFn_L2Distance:     OpType_L2Distance,
		DistFn_L2sqDistance:   OpType_L2Distance,
		DistFn_InnerProduct:   OpType_InnerProduct,
		DistFn_CosineDistance: OpType_CosineDistance,
		DistFn_L1Distance:     OpType_L1Distance,
	}

	// DistFuncOpTypeSet lists every index op_type that can serve a query on the given
	// distance function:
	//
	//	vector_l2_ops    -> l2_distance, l2_distance_sq
	//	vector_l2sq_ops  -> l2_distance, l2_distance_sq
	//	vector_l1_ops    -> l1_distance
	//	vector_ip_ops    -> inner_product
	//	vector_cosine_ops-> cosine_distance
	//
	// It is a SET rather than one canonical op_type only because of the L2 pair: the two
	// op_types build a byte-identical index (each maps to Metric_L2sqDistance) and whether
	// the score is sqrt-ed is decided at SEARCH time from the query's function name
	// (OrigFuncName -> DistanceTransformIvfflat / DistanceTransformHnsw), never from the
	// index. So either one answers either form with a correctly scaled score, and the
	// distinction is naming only. Matching one canonical op_type per function left every
	// vector_l2sq_ops index unusable — accepted at CREATE INDEX, never chosen (#25966).
	DistFuncOpTypeSet = map[string][]string{
		DistFn_L2Distance:     {OpType_L2Distance, OpType_L2sqDistance},
		DistFn_L2sqDistance:   {OpType_L2Distance, OpType_L2sqDistance},
		DistFn_InnerProduct:   {OpType_InnerProduct},
		DistFn_CosineDistance: {OpType_CosineDistance},
		DistFn_L1Distance:     {OpType_L1Distance},
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

// OpTypeServesDistFunc reports whether an index built with opType can answer a query
// that uses distFn — the index-selection test for every vector algorithm. False for an
// unindexable distFn and for an op_type whose metric differs from the query's.
func OpTypeServesDistFunc(opType, distFn string) bool {
	for _, ok := range DistFuncOpTypeSet[distFn] {
		if ok == opType {
			return true
		}
	}
	return false
}

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

// DistanceTransformHnsw converts a raw usearch distance to the value MO's SQL distance
// function named by the QUERY returns, so an index-served score and the scalar distance
// agree in the float32 domain both live in (see RoundDistanceToElemDomain for why that is
// float32-domain agreement, not bitwise equality). Every conversion is monotonic and the
// caller applies it after the result heap is ordered, so ranking is unaffected.
//
// usearch is the only backend needing this HERE: the Go CPU kernels already return MO's
// convention (InnerProduct returns -a·b), and cuVS output is negated inside cgo/cuvs
// (index_base.hpp search path, distance.hpp / distance_c.cpp pairwise) before it reaches
// Go. (Note: the gpu-tagged pairwise wait in gpu.go negates IP a SECOND time on top of
// the cgo flip — a separate, pre-existing GPU-only defect, not something this transform
// compensates for.) Within usearch only inner product differs — its IP metric is 1 - a·b against MO's
// -a·b, so an untranslated score is exactly 1 too high; ordering stays correct, which is
// why only a value comparison catches it. Cosine is deliberately not repaired here:
// usearch's cosine score can be wrong for zero and subnormal vectors, and this function
// has neither the vectors nor the candidate set needed to recompute a correct score.
// The HNSW planner therefore keeps cosine queries on the exact SQL path.
func DistanceTransformHnsw(dist float64, origMetricType MetricType, metricType usearch.Metric) float64 {
	if origMetricType == Metric_L2Distance && metricType == usearch.L2sq {
		// metric is l2sq but origin is l2_distance
		return RoundDistanceToElemDomain(math.Sqrt(dist))
	}
	if metricType == usearch.InnerProduct {
		return RoundDistanceToElemDomain(dist - 1)
	}
	return RoundDistanceToElemDomain(dist)
}

func DistanceTransformIvfflat(dist float64, origMetricType, metricType MetricType) float64 {
	if origMetricType == Metric_L2Distance && metricType == Metric_L2sqDistance {
		// metric is l2sq but origin is l2_distance
		return RoundDistanceToElemDomain(math.Sqrt(dist))
	}
	return RoundDistanceToElemDomain(dist)
}

// RoundDistanceToElemDomain rounds a distance into the float32 domain MO's vector distance functions
// use. usearch/cuvs return distances in float32 (usearch.h: typedef float usearch_distance_t), and
// the scalar l2_distance / l2_distance_sq / inner_product / cosine_distance likewise deliver a
// float32-precision value for every supported base type (float32 and the narrow bf16/f16/int8/uint8,
// computed via float32; cuvs has no float64 vectors at all). Standardizing every path on this one
// domain brings an index-served distance and the scalar one into the same precision, so they agree
// in the float32 domain and neither a projected value nor a pushed range predicate carries a
// float64 tail the other lacks (#29040 / #29050). It is a trivial float32 round-trip -- the
// compiler inlines it, so there is no per-row cost.
//
// This is float32-domain agreement, NOT bitwise equality. The two paths can still differ by up to
// one float32 ULP at an exact boundary: usearch accumulates in float32 SIMD whose order varies by
// CPU/kernel (see vector_index_optype_matrix's round() note), and for a float64 base usearch returns
// the squared distance already rounded to float32 -- so the index rounds the square before this sqrt
// while the scalar rounds after. Guaranteeing bitwise equality would require recomputing the served
// distance from the source vectors (ANN for candidate selection + exact scalar re-rank), which this
// does not do.
func RoundDistanceToElemDomain(dist float64) float64 {
	return float64(float32(dist))
}

// smallestNormalFloat32 is the smallest positive normal (non-subnormal) float32. Below it, usearch's
// float32 cosine norm underflows and the score leaves MO's cosine_distance contract.
const smallestNormalFloat32 = 1.1754943508222875e-38

// CosineVectorL2Norm returns the exact float64 L2 norm of a cosine vector and whether it is usable on
// the HNSW/usearch cosine index. ok is false when the squared norm underflows the float32 domain
// usearch computes cosine in -- a zero or subnormal-magnitude vector -- which usearch cannot score to
// MO's cosine_distance, and HNSW ranks candidates before any output transform. HNSW cosine assumes
// caller-normalized vectors, so the caller rejects an unusable vector fail-fast (it does NOT modify
// the vector); the returned norm is for the diagnostic message. Computed once per search on the query
// vector, not per row (#29082).
func CosineVectorL2Norm[T types.RealNumbers](v []T) (norm float64, ok bool) {
	var sumSq float64
	for _, x := range v {
		d := float64(x)
		sumSq += d * d
	}
	return math.Sqrt(sumSq), float32(sumSq) >= smallestNormalFloat32
}

// HasFloat64DistanceOverflow reports whether an index search must fail fast because a float64 base
// produced a distance the float32 domain cannot represent. Index distances are float32
// (usearch_distance_t is float32; cuvs is float32-only), so only a float64 base can hold a finite
// value whose distance overflows and saturates to +/-Inf -- serving that would silently corrupt the
// value, Top-K order, and any outer predicate (#29040 / #29050). Returns false immediately for any
// non-float64 base (the common path), so the Inf scan runs only for float64.
func HasFloat64DistanceOverflow[T types.RealNumbers](distances []float64) bool {
	if _, ok := any(*new(T)).(float64); !ok {
		return false
	}
	for _, d := range distances {
		if math.IsInf(d, 0) {
			return true
		}
	}
	return false
}
