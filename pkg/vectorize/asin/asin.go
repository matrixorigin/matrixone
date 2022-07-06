// Copyright 2022 Matrix Origin
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

package asin

import (
	"math"

	"github.com/matrixorigin/matrixone/pkg/container/nulls"
)

type AsinResult struct {
	Result []float64
	Nsp    *nulls.Nulls
}

func Asin(inputValues []float64, rs []float64) AsinResult {
	result := AsinResult{Result: rs, Nsp: new(nulls.Nulls)}
	for i, n := range inputValues {
		if n < -1 || n > 1 {
			nulls.Add(result.Nsp, uint64(i))
		} else {
			result.Result[i] = math.Asin(n)
		}
	}
	return result
}
