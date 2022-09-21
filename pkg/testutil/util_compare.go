// Copyright 2021 Matrix Origin
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

package testutil

import (
	"reflect"

	"github.com/matrixorigin/matrixone/pkg/container/nulls"
	"github.com/matrixorigin/matrixone/pkg/container/vector"
)

func CompareVectors(expected *vector.Vector, got *vector.Vector) bool {
	if expected.IsScalar() {
		if !got.IsScalar() {
			return false
		}
		if expected.IsScalarNull() {
			return got.IsScalarNull()
		} else {
			if expected.GetType().IsVarlen() {
				v1 := vector.GetStrVectorValues(expected)
				v2 := vector.GetStrVectorValues(got)
				return reflect.DeepEqual(v1[0], v2[0])
			} else {
				return reflect.DeepEqual(expected.Col, got.Col)
			}
		}
	} else {
		if got.IsScalar() {
			return false
		}
		// expected length and got length
		expectedLength := vector.Length(expected)
		gotLength := vector.Length(got)
		if expectedLength != gotLength {
			return false
		}
		if nulls.Any(expected.Nsp) {
			k := uint64(0)
			if !nulls.Any(got.Nsp) {
				return false
			}
			for k = 0; k < uint64(expectedLength); k++ {
				c1 := nulls.Contains(expected.Nsp, k)
				c2 := nulls.Contains(got.Nsp, k)
				if c1 != c2 {
					return false
				}
			}
		} else if nulls.Any(got.Nsp) {
			return false
		}

		if expected.GetType().IsVarlen() {
			v1 := vector.GetStrVectorValues(expected)
			v2 := vector.GetStrVectorValues(got)
			for i, v := range v1 {
				if nulls.Contains(expected.Nsp, uint64(i)) {
					if !nulls.Contains(got.Nsp, uint64(i)) {
						return false
					}
				} else {
					if nulls.Contains(got.Nsp, uint64(i)) {
						return false
					}
					vv := v2[i]
					if v != vv {
						return false
					}
				}
			}
			return true
		} else {
			return reflect.DeepEqual(expected.Col, got.Col)
		}
	}
}
