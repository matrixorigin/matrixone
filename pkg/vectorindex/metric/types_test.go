// Copyright 2026 Matrix Origin
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
	"testing"

	"github.com/stretchr/testify/require"
	usearch "github.com/unum-cloud/usearch/golang"
)

// TestValidQuantization_DrivenByCuvsMap pins ValidQuantization to the
// cuvs map. The previous shape had a hand-maintained list inside the
// validator that drifted from the (single) NameToType map — float16
// was accepted by the validator but missing from the map; float64 was
// the reverse. Iterating the map here means any future entry added or
// removed has to update both or the test catches it.
func TestValidQuantization_DrivenByCuvsMap(t *testing.T) {
	require.NotEmpty(t, CuvsQuantizationNameToType)
	for name := range CuvsQuantizationNameToType {
		require.True(t, ValidQuantization(name), "validator rejected cuvs-mapped name %q", name)
	}
	// float64 is usearch-only; cuvs does not support it, and the
	// validator drives the cuvs CREATE INDEX path.
	require.False(t, ValidQuantization(Quantization_F64_Str))
	require.False(t, ValidQuantization("bogus"))
	require.False(t, ValidQuantization(""))
}

func TestUsearchQuantizationNameToType(t *testing.T) {
	require.Equal(t, Quantization_F32, UsearchQuantizationNameToType[Quantization_F32_Str])
	require.Equal(t, Quantization_F16, UsearchQuantizationNameToType[Quantization_F16_Str])
	require.Equal(t, Quantization_F64, UsearchQuantizationNameToType[Quantization_F64_Str])
	require.Equal(t, Quantization_INT8, UsearchQuantizationNameToType[Quantization_INT8_Str])
	require.Equal(t, Quantization_UINT8, UsearchQuantizationNameToType[Quantization_UINT8_Str])
}

func TestCuvsQuantizationNameToType(t *testing.T) {
	require.Equal(t, Quantization_F32, CuvsQuantizationNameToType[Quantization_F32_Str])
	require.Equal(t, Quantization_F16, CuvsQuantizationNameToType[Quantization_F16_Str])
	require.Equal(t, Quantization_INT8, CuvsQuantizationNameToType[Quantization_INT8_Str])
	require.Equal(t, Quantization_UINT8, CuvsQuantizationNameToType[Quantization_UINT8_Str])
	_, ok := CuvsQuantizationNameToType[Quantization_F64_Str]
	require.False(t, ok, "cuvs map must not include float64")
}

// TestOpTypeServesDistFunc pins the index-selection test to the op_type -> distance
// function mapping:
//
//	vector_l2_ops, vector_l2sq_ops -> l2_distance, l2_distance_sq;
//	vector_l1_ops -> l1_distance;  vector_ip_ops -> inner_product;
//	vector_cosine_ops -> cosine_distance
//
// The L2 pair is interchangeable because both op_types build the same index and the
// sqrt is applied from the QUERY's function name, so either serves either form with a
// correctly scaled score. Before #25966 each function matched ONE canonical op_type, so
// a vector_l2sq_ops index was accepted at CREATE INDEX and then never chosen.
func TestOpTypeServesDistFunc(t *testing.T) {
	for _, distFn := range []string{DistFn_L2Distance, DistFn_L2sqDistance} {
		require.True(t, OpTypeServesDistFunc(OpType_L2Distance, distFn), distFn)
		require.True(t, OpTypeServesDistFunc(OpType_L2sqDistance, distFn), distFn)
		// A different metric must never be substituted: the score would be wrong.
		require.False(t, OpTypeServesDistFunc(OpType_InnerProduct, distFn), distFn)
		require.False(t, OpTypeServesDistFunc(OpType_CosineDistance, distFn), distFn)
		require.False(t, OpTypeServesDistFunc(OpType_L1Distance, distFn), distFn)
	}

	require.True(t, OpTypeServesDistFunc(OpType_InnerProduct, DistFn_InnerProduct))
	require.True(t, OpTypeServesDistFunc(OpType_CosineDistance, DistFn_CosineDistance))
	require.True(t, OpTypeServesDistFunc(OpType_L1Distance, DistFn_L1Distance))
	require.False(t, OpTypeServesDistFunc(OpType_L2Distance, DistFn_L1Distance))

	// Unknown / non-indexable inputs yield no match rather than a default.
	require.False(t, OpTypeServesDistFunc(OpType_L2Distance, "cosine_similarity"))
	require.False(t, OpTypeServesDistFunc("", ""))
}

// TestDistFuncOpTypeSetMatchesDefaults keeps the two tables from drifting: every
// indexable distance function must have a serving set, and the op_type it is given by
// default must be a member of that set — otherwise CREATE INDEX would produce an index
// the planner then refuses to use, which is the bug this pair of maps encodes.
func TestDistFuncOpTypeSetMatchesDefaults(t *testing.T) {
	require.Len(t, DistFuncOpTypeSet, len(DistFuncOpTypes))
	for distFn, defaultOp := range DistFuncOpTypes {
		require.Containsf(t, DistFuncOpTypeSet, distFn, "no serving op_types for %q", distFn)
		require.Truef(t, OpTypeServesDistFunc(defaultOp, distFn),
			"default op_type %q for %q is not in its own serving set", defaultOp, distFn)
	}
	// Every op_type named in the sets must be a real IVF op_type.
	for distFn, ops := range DistFuncOpTypeSet {
		for _, op := range ops {
			require.Containsf(t, OpTypeToIvfMetric, op, "%q lists unknown op_type %q", distFn, op)
		}
	}
}

func TestMaxFloat(t *testing.T) {
	require.Equal(t, float32(math.MaxFloat32), MaxFloat[float32]())
	require.Equal(t, float64(math.MaxFloat64), MaxFloat[float64]())
}

func TestDistanceTransformHnsw(t *testing.T) {
	// L2Distance with usearch.L2sq -> sqrt
	in := 9.0
	out := DistanceTransformHnsw(in, Metric_L2Distance, usearch.L2sq)
	require.InDelta(t, 3.0, out, 1e-9)

	// non-matching combinations -> identity
	out = DistanceTransformHnsw(in, Metric_L2sqDistance, usearch.L2sq)
	require.Equal(t, in, out)

	// usearch's IP metric is 1 - a·b; MO's inner_product SQL function is -a·b, so the
	// raw distance must come back one lower. Without this an HNSW vector_ip_ops index
	// reported every score exactly 1 above the brute-force value (ordering intact, so
	// only the number was wrong). Independent of origMetricType: the query function is
	// inner_product either way.
	require.InDelta(t, 8.0, DistanceTransformHnsw(in, Metric_InnerProduct, usearch.InnerProduct), 1e-9)
	require.InDelta(t, 8.0, DistanceTransformHnsw(in, Metric_L2Distance, usearch.InnerProduct), 1e-9)
	// -a·b for a·b = 200 is -200; usearch hands us 1 - 200 = -199.
	require.InDelta(t, -200.0, DistanceTransformHnsw(-199.0, Metric_InnerProduct, usearch.InnerProduct), 1e-9)

	// Cosine already agrees with cosine_distance (both 1 - cos_sim) — no rescale.
	require.Equal(t, in, DistanceTransformHnsw(in, Metric_CosineDistance, usearch.Cosine))
}

func TestDistanceTransformIvfflat(t *testing.T) {
	in := 16.0
	out := DistanceTransformIvfflat(in, Metric_L2Distance, Metric_L2sqDistance)
	require.InDelta(t, 4.0, out, 1e-9)

	out = DistanceTransformIvfflat(in, Metric_L2sqDistance, Metric_L2sqDistance)
	require.Equal(t, in, out)
	out = DistanceTransformIvfflat(in, Metric_L2Distance, Metric_InnerProduct)
	require.Equal(t, in, out)
}

func TestRoundDistanceToElemDomain(t *testing.T) {
	// An exact float32 value round-trips unchanged.
	require.Equal(t, 3.0, RoundDistanceToElemDomain(3.0))
	require.Equal(t, 0.0, RoundDistanceToElemDomain(0.0))
	require.Equal(t, -2.5, RoundDistanceToElemDomain(-2.5))

	// A float64 with more precision than float32 can hold snaps to the nearest float32.
	in := 33.674915313720703
	require.Equal(t, float64(float32(in)), RoundDistanceToElemDomain(in))
	// The result must itself be representable in float32 (idempotent).
	require.Equal(t, RoundDistanceToElemDomain(in), RoundDistanceToElemDomain(RoundDistanceToElemDomain(in)))

	// float64(0.9999999999999998) rounds up to exactly 1 in the float32 domain — this
	// subsumes the old cosine-similarity corner snap.
	require.Equal(t, 1.0, RoundDistanceToElemDomain(0.9999999999999998))

	// A magnitude beyond the float32 range saturates to +/-Inf rather than wrapping.
	require.True(t, math.IsInf(RoundDistanceToElemDomain(1e40), 1))
	require.True(t, math.IsInf(RoundDistanceToElemDomain(-1e40), -1))
}

func TestHasFloat64DistanceOverflow(t *testing.T) {
	// A non-float64 base never triggers the check, even with an Inf present.
	require.False(t, HasFloat64DistanceOverflow[float32]([]float64{1, 2, math.Inf(1)}))

	// A float64 base with only finite distances is fine.
	require.False(t, HasFloat64DistanceOverflow[float64]([]float64{1, 2, 3}))
	require.False(t, HasFloat64DistanceOverflow[float64](nil))

	// A float64 base whose distance saturated to +/-Inf must fail fast.
	require.True(t, HasFloat64DistanceOverflow[float64]([]float64{1, math.Inf(1), 3}))
	require.True(t, HasFloat64DistanceOverflow[float64]([]float64{math.Inf(-1)}))
}

func TestCosineVectorL2Norm(t *testing.T) {
	// Zero and subnormal-magnitude vectors underflow the float32 cosine norm: ok=false, the caller
	// errors out. The norm is still returned (for the diagnostic message).
	norm, ok := CosineVectorL2Norm([]float32{0, 0, 0})
	require.False(t, ok)
	require.Equal(t, 0.0, norm)
	_, ok = CosineVectorL2Norm([]float64{0, 0, 0})
	require.False(t, ok)
	norm, ok = CosineVectorL2Norm([]float32{1e-20, 1e-20, 1e-20}) // sumSq 3e-40 underflows float32
	require.False(t, ok)
	require.InEpsilon(t, math.Sqrt(3e-40), norm, 1e-6) // norm itself is still computed
	_, ok = CosineVectorL2Norm([]float64{1e-20, 1e-20, 1e-20})
	require.False(t, ok)

	// Normal / normalized vectors are usable and the returned norm is exact.
	norm, ok = CosineVectorL2Norm([]float32{1, 0, 0})
	require.True(t, ok)
	require.InEpsilon(t, 1.0, norm, 1e-6)

	norm, ok = CosineVectorL2Norm([]float32{0.5773503, 0.5773503, 0.5773503}) // unit
	require.True(t, ok)
	require.InEpsilon(t, 1.0, norm, 1e-6)

	norm, ok = CosineVectorL2Norm([]float64{3, 4}) // norm 5
	require.True(t, ok)
	require.InEpsilon(t, 5.0, norm, 1e-9)

	// Boundary: sumSq just above the float32 smallest-normal is ok; just below underflows.
	_, ok = CosineVectorL2Norm([]float32{2e-19}) // sumSq 4e-38 > 1.18e-38 normal
	require.True(t, ok)
	_, ok = CosineVectorL2Norm([]float32{1e-19}) // sumSq 1e-38 < 1.18e-38 normal
	require.False(t, ok)
}
