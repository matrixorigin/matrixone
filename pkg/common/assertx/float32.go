// Copyright 2023 Matrix Origin
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

package assertx

import (
	"math"
)

const float32EqualityThreshold = 1e-5

// InEpsilonF32Slices returns true if all the elements in v1 and v2 are within epsilon of each other.
func InEpsilonF32Slices(want, got [][]float32) bool {
	if len(want) != len(got) {
		return false
	}

	for i := range want {
		if !InEpsilonF32Slice(want[i], got[i]) {
			return false
		}
	}
	return true
}

// InEpsilonF32Slice returns true if all the elements in v1 and v2 are within epsilon of each other.
// assert.InEpsilonSlice requires v1 to be non-zero.
func InEpsilonF32Slice(want, got []float32) bool {
	if len(want) != len(got) {
		return false
	}

	for i := range want {
		if !InEpsilonF32(want[i], got[i]) {
			return false
		}
	}
	return true
}

// InEpsilonF32 returns true if v1 and v2 are within epsilon of each other.
// assert.InEpsilon requires v1 to be non-zero.
func InEpsilonF32(want, got float32) bool {
	return want == got || math.Abs(float64(want)-float64(got)) < float32EqualityThreshold || (math.IsNaN(float64(want)) && math.IsNaN(float64(got)))
}
