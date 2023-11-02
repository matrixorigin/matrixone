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

package elkans

import "math"

const defaultEpsilon = 1e-5

// InEpsilonF64Slice returns true if all the elements in v1 and v2 are within epsilon of each other.
// assert.InEpsilonSlice requires v1 to be non-zero.
func InEpsilonF64Slice(v1, v2 []float64, epsilon float64) bool {
	if len(v1) != len(v2) {
		return false
	}

	for i := 0; i < len(v1); i++ {
		if !InEpsilonF64(v1[i], v2[i], epsilon) {
			return false
		}
	}
	return true
}

// InEpsilonF64 returns true if v1 and v2 are within epsilon of each other.
// assert.InEpsilon requires v1 to be non-zero.
func InEpsilonF64(v1, v2 float64, epsilon float64) bool {
	return math.Abs(v1-v2) <= epsilon
}
