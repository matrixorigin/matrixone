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
	"github.com/matrixorigin/matrixone/pkg/container/nulls"
	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/container/vector"
	"reflect"
)

func CompareVectors(expected *vector.Vector, got *vector.Vector) bool {
	if expected.IsScalar() {
		if !got.IsScalar() {
			return false
		}
		if expected.IsScalarNull() {
			return got.IsScalarNull()
		} else {
			switch expected.Typ.Oid {
			case types.T_char, types.T_varchar, types.T_json:
				if got.Typ.Oid != expected.Typ.Oid {
					return false
				}
				t1 := expected.Col.(*types.Bytes)
				t2 := expected.Col.(*types.Bytes)
				return reflect.DeepEqual(t1.Get(0), t2.Get(0))
			default:
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
		}
		switch expected.Typ.Oid {
		case types.T_char, types.T_varchar, types.T_json:
			if got.Typ.Oid != expected.Typ.Oid {
				return false
			}
			t1 := expected.Col.(*types.Bytes)
			t2 := expected.Col.(*types.Bytes)
			l1, l2 := len(t1.Lengths), len(t2.Lengths)
			if l1 != l2 {
				return false
			}
			for i := 0; i < l1; i++ {
				if nulls.Contains(expected.Nsp, uint64(i)) {
					continue
				}
				if !reflect.DeepEqual(t1.Get(0), t2.Get(0)) {
					return false
				}
			}
			return true
		default:
			return reflect.DeepEqual(expected.Col, got.Col)
		}
	}
}
