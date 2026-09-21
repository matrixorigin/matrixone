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

import "github.com/matrixorigin/matrixone/pkg/common/moerr"

// wideL2DistanceSqFloat32 computes a float64 squared L2 distance in one pass.
// Converting each operand before subtraction preserves the range of finite
// float32 inputs without the extra scale-detection pass used by the general
// stable kernel. Non-finite input, or an unexpected non-finite accumulator,
// falls back to the stable implementation so its NaN/Inf semantics remain the
// same.
func wideL2DistanceSqFloat32(p, q []float32) (float64, error) {
	if len(p) != len(q) {
		return 0, moerr.NewInternalErrorNoCtx("vector dimension not matched")
	}

	var sum0, sum1, sum2, sum3 float64
	i := 0
	for ; i <= len(p)-4; i += 4 {
		d0 := float64(p[i]) - float64(q[i])
		d1 := float64(p[i+1]) - float64(q[i+1])
		d2 := float64(p[i+2]) - float64(q[i+2])
		d3 := float64(p[i+3]) - float64(q[i+3])
		sum0 += d0 * d0
		sum1 += d1 * d1
		sum2 += d2 * d2
		sum3 += d3 * d3
	}
	for ; i < len(p); i++ {
		d := float64(p[i]) - float64(q[i])
		sum0 += d * d
	}

	sum := (sum0 + sum1) + (sum2 + sum3)
	if !isFiniteStableFloat(sum) {
		return StableL2DistanceSq(p, q)
	}
	return sum, nil
}
