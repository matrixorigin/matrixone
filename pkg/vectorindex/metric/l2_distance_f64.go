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

	"github.com/matrixorigin/matrixone/pkg/common/moerr"
	"github.com/matrixorigin/matrixone/pkg/container/types"
)

// l2DistanceF64 computes the L2 distance accumulating the squared sum in float64. Accumulating in
// float64 (not the element type) keeps the intermediate squared sum from overflowing for a float32
// base whose distance is still representable (#29083). The result is cast back to T, so the exposed
// float32 distance stays in its own domain (#29040/#29050).
func l2DistanceF64[T types.RealNumbers](v1, v2 []T) (T, error) {
	if len(v1) != len(v2) {
		return 0, moerr.NewInternalErrorNoCtx("vector dimension not matched")
	}
	var sum float64
	for i := range v1 {
		d := float64(v1[i]) - float64(v2[i])
		sum += d * d
	}
	return T(math.Sqrt(sum)), nil
}
