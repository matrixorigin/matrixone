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

import "math"

// Float32ScaleNormalizer applies the value normalization used by SQL
// comparisons for FLOAT32 values with a positive scale. Construct it once per
// vector or comparison operation so the scale factor is not recomputed per row.
type Float32ScaleNormalizer struct {
	factor float64
}

// NewFloat32ScaleNormalizer returns the FLOAT32 SQL comparison normalizer for
// scale. A non-positive scale preserves the stored value.
func NewFloat32ScaleNormalizer(scale int32) Float32ScaleNormalizer {
	if scale <= 0 {
		return Float32ScaleNormalizer{}
	}
	return Float32ScaleNormalizer{factor: math.Pow10(int(scale))}
}

// Normalize returns the value observed by SQL FLOAT32 comparisons.
func (n Float32ScaleNormalizer) Normalize(value float32) float32 {
	if n.factor == 0 {
		return value
	}
	return float32(math.Round(float64(value)*n.factor) / n.factor)
}
