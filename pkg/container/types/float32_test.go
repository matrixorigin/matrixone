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
		{name: "negative", scale: 2, value: -1.234, want: -1.23},
		{name: "positive-infinity", scale: 2, value: float32(math.Inf(1)), want: float32(math.Inf(1))},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			normalizer := NewFloat32ScaleNormalizer(test.scale)
			require.Equal(t, test.want, normalizer.Normalize(test.value))
		})
	}
}
