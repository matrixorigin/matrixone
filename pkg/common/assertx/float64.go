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

// Float64 values have minor numerical differences between Apple M2 and Linux EC2,
// so we use an epsilon value to compare them in UT. We could not use reflect.DeepEqual because
// of these minor differences. We cannot use assert.InEpsilon because it requires the first
// argument to be non-zero.
//
// The epsilon 1e-9 epsilon is based on the following observation:
//   - `strconv.FormatFloat(float64(value), 'f', -1, 64)` we use for outputting float64 values.
//   - precision difference between Apple M2 and Linux EC2 float64 results (approx 1e-13)
//   - service_weaver: https://github.com/ServiceWeaver/weaver/blob/8e7c225542b8f8267ec0da3e01a098366b6f8daf/internal/weaver/load.go#L31
//   - google lib: https://github.com/google/differential-privacy/blob/91b4ecebf33e71b8a215b7ea8325fb5e47b12671/privacy-on-beam/pbeam/pbeamtest/pbeamtest_test.go#L1406
const float64EqualityThreshold = 1e-9

// InEpsilonF64Slices returns true if all the elements in v1 and v2 are within epsilon of each other.
func InEpsilonF64Slices(want, got [][]float64) bool {
	if len(want) != len(got) {
		return false
	}

	for i := 0; i < len(want); i++ {
		if !InEpsilonF64Slice(want[i], got[i]) {
			return false
		}
	}
	return true
}

// InEpsilonF64Slice returns true if all the elements in v1 and v2 are within epsilon of each other.
// assert.InEpsilonSlice requires v1 to be non-zero.
func InEpsilonF64Slice(want, got []float64) bool {
	if len(want) != len(got) {
		return false
	}

	for i := 0; i < len(want); i++ {
		if !InEpsilonF64(want[i], got[i]) {
			return false
		}
	}
	return true
}

// InEpsilonF64 returns true if v1 and v2 are within epsilon of each other.
// assert.InEpsilon requires v1 to be non-zero.
func InEpsilonF64(want, got float64) bool {
	return want == got || math.Abs(want-got) < float64EqualityThreshold || (math.IsNaN(want) && math.IsNaN(got))
}
