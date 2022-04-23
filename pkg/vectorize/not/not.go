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

package not

import "golang.org/x/exp/constraints"

var (
	Int8Not    = numericNot[int8]
	Int16Not   = numericNot[int16]
	Int32Not   = numericNot[int32]
	Int64Not   = numericNot[int64]
	Float32Not = numericNot[float32]
	Float64Not = numericNot[float64]
	Uint8Not   = numericNot[uint8]
	Uint16Not  = numericNot[uint16]
	Uint32Not  = numericNot[uint32]
	Uint64Not  = numericNot[uint64]
)

func numericNot[T constraints.Integer | constraints.Float](xs []T, rs []int8) []int8 {
	for i, x := range xs {
		if x == 0 {
			rs[i] = 1
		} else {
			rs[i] = 0
		}
	}
	return rs
}
