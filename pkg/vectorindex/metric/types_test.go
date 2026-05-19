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
	out = DistanceTransformHnsw(in, Metric_L2Distance, usearch.InnerProduct)
	require.Equal(t, in, out)
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
