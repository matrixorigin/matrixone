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

package types

import (
	"math"
	"testing"

	"github.com/stretchr/testify/require"
)

func TestFloat32ScaleNormalizer(t *testing.T) {
	tests := []struct {
		name  string
		scale int32
		value float32
		want  float32
	}{
		{name: "unscaled", value: 1.234, want: 1.234},
		{name: "negative-scale", scale: -1, value: 1.234, want: 1.234},
		{name: "round-down", scale: 2, value: 1.234, want: 1.23},
		{name: "round-up", scale: 2, value: 1.236, want: 1.24},
		{name: "scale-three-round-down", scale: 3, value: 1.2304, want: 1.23},
		{name: "scale-three-round-up", scale: 3, value: 1.2306, want: 1.231},
		{name: "negative", scale: 2, value: -1.234, want: -1.23},
		{name: "positive-infinity", scale: 2, value: float32(math.Inf(1)), want: float32(math.Inf(1))},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			normalizer := NewFloat32ScaleNormalizer(test.scale)
			require.Equal(t, test.want, normalizer.Normalize(test.value))
		})
	}

	// FLOAT(M,D) is represented as FLOAT32 only while M < 24, so 23 is
	// the largest legal positive FLOAT32 scale. Keep its finite and subnormal
	// behavior explicit because this normalizer is also used by hash keys.
	maxScale := NewFloat32ScaleNormalizer(23)
	require.Equal(t, float32(math.MaxFloat32), maxScale.Normalize(float32(math.MaxFloat32)))
	require.Equal(t, float32(0), maxScale.Normalize(float32(math.SmallestNonzeroFloat32)))
	negativeTiny := maxScale.Normalize(-float32(math.SmallestNonzeroFloat32))
	require.True(t, math.Signbit(float64(negativeTiny)))
	require.True(t, math.IsNaN(float64(maxScale.Normalize(float32(math.NaN())))))
}
