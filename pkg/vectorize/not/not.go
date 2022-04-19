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

var (
	Int8Not    func([]int8, []int8) []int8
	Int16Not   func([]int16, []int8) []int8
	Int32Not   func([]int32, []int8) []int8
	Int64Not   func([]int64, []int8) []int8
	Float32Not func([]float32, []int8) []int8
	Float64Not func([]float64, []int8) []int8
	Uint8Not   func([]uint8, []int8) []int8
	Uint16Not  func([]uint16, []int8) []int8
	Uint32Not  func([]uint32, []int8) []int8
	Uint64Not  func([]uint64, []int8) []int8
)

func init() {
	Int8Not = int8Not
	Int16Not = int16Not
	Int32Not = int32Not
	Int64Not = int64Not
	Float32Not = float32Not
	Float64Not = float64Not
	Uint8Not = uint8Not
	Uint16Not = uint16Not
	Uint32Not = uint32Not
	Uint64Not = uint64Not
}

func int8Not(xs []int8, rs []int8) []int8 {
	for i, x := range xs {
		if x == 0 {
			rs[i] = 1
		} else {
			rs[i] = 0
		}
	}
	return rs
}

func int16Not(xs []int16, rs []int8) []int8 {
	for i, x := range xs {
		if x == 0 {
			rs[i] = 1
		} else {
			rs[i] = 0
		}
	}
	return rs
}

func int32Not(xs []int32, rs []int8) []int8 {
	for i, x := range xs {
		if x == 0 {
			rs[i] = 1
		} else {
			rs[i] = 0
		}
	}
	return rs
}

func int64Not(xs []int64, rs []int8) []int8 {
	for i, x := range xs {
		if x == 0 {
			rs[i] = 1
		} else {
			rs[i] = 0
		}
	}
	return rs
}

func float32Not(xs []float32, rs []int8) []int8 {
	for i, x := range xs {
		if x == 0 {
			rs[i] = 1
		} else {
			rs[i] = 0
		}
	}
	return rs
}

func float64Not(xs []float64, rs []int8) []int8 {
	for i, x := range xs {
		if x == 0 {
			rs[i] = 1
		} else {
			rs[i] = 0
		}
	}
	return rs
}

func uint8Not(xs []uint8, rs []int8) []int8 {
	for i, x := range xs {
		if x == 0 {
			rs[i] = 1
		} else {
			rs[i] = 0
		}
	}
	return rs
}

func uint16Not(xs []uint16, rs []int8) []int8 {
	for i, x := range xs {
		if x == 0 {
			rs[i] = 1
		} else {
			rs[i] = 0
		}
	}
	return rs
}

func uint32Not(xs []uint32, rs []int8) []int8 {
	for i, x := range xs {
		if x == 0 {
			rs[i] = 1
		} else {
			rs[i] = 0
		}
	}
	return rs
}

func uint64Not(xs []uint64, rs []int8) []int8 {
	for i, x := range xs {
		if x == 0 {
			rs[i] = 1
		} else {
			rs[i] = 0
		}
	}
	return rs
}
