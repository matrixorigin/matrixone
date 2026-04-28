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

func TestValidQuantization(t *testing.T) {
	require.True(t, ValidQuantization(Quantization_F32_Str))
	require.True(t, ValidQuantization(Quantization_F16_Str))
	require.True(t, ValidQuantization(Quantization_INT8_Str))
	require.True(t, ValidQuantization(Quantization_UINT8_Str))
	// f64 is not in the validation list
	require.False(t, ValidQuantization(Quantization_F64_Str))
	require.False(t, ValidQuantization("bogus"))
	require.False(t, ValidQuantization(""))
}

func TestQuantizationNameToType(t *testing.T) {
	require.Equal(t, Quantization_F32, QuantizationNameToType[Quantization_F32_Str])
	require.Equal(t, Quantization_F64, QuantizationNameToType[Quantization_F64_Str])
	require.Equal(t, Quantization_INT8, QuantizationNameToType[Quantization_INT8_Str])
	require.Equal(t, Quantization_UINT8, QuantizationNameToType[Quantization_UINT8_Str])
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
