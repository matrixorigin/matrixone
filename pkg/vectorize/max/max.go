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

package max

import (
	"bytes"
	"matrixone/pkg/container/types"
	"matrixone/pkg/vectorize"
)

var (
	BoolMax        func([]bool) bool
	BoolMaxSels    func([]bool, []int64) bool
	Int8Max        func([]int8) int8
	Int8MaxSels    func([]int8, []int64) int8
	Int16Max       func([]int16) int16
	Int16MaxSels   func([]int16, []int64) int16
	Int32Max       func([]int32) int32
	Int32MaxSels   func([]int32, []int64) int32
	Int64Max       func([]int64) int64
	Int64MaxSels   func([]int64, []int64) int64
	Uint8Max       func([]uint8) uint8
	Uint8MaxSels   func([]uint8, []int64) uint8
	Uint16Max      func([]uint16) uint16
	Uint16MaxSels  func([]uint16, []int64) uint16
	Uint32Max      func([]uint32) uint32
	Uint32MaxSels  func([]uint32, []int64) uint32
	Uint64Max      func([]uint64) uint64
	Uint64MaxSels  func([]uint64, []int64) uint64
	Float32Max     func([]float32) float32
	Float32MaxSels func([]float32, []int64) float32
	Float64Max     func([]float64) float64
	Float64MaxSels func([]float64, []int64) float64
	StrMax         func(*types.Bytes) []byte
	StrMaxSels     func(*types.Bytes, []int64) []byte
)

func boolMax(xs []bool) bool {
	for _, x := range xs {
		if x == true {
			return true
		}
	}
	return false
}

func boolMaxSels(xs []bool, sels []int64) bool {
	for _, sel := range sels {
		if xs[sel] == true {
			return true
		}
	}
	return false
}

func maxGeneric[T vectorize.Numeric](xs []T) T {
	res := xs[0]
	for _, x := range xs {
		if x > res {
			res = x
		}
	}
	return res
}

func maxSelsGeneric[T vectorize.Numeric](xs []T, sels []int64) T {
	res := xs[sels[0]]
	for _, sel := range sels {
		x := xs[sel]
		if x > res {
			res = x
		}
	}
	return res
}

func strMax(xs *types.Bytes) []byte {
	res := xs.Get(0)
	for i, n := 0, len(xs.Offsets); i < n; i++ {
		x := xs.Get(int64(i))
		if bytes.Compare(x, res) > 0 {
			res = x
		}
	}
	return res
}

func strMaxSels(xs *types.Bytes, sels []int64) []byte {
	res := xs.Get(sels[0])
	for _, sel := range sels {
		x := xs.Get(sel)
		if bytes.Compare(x, res) > 0 {
			res = x
		}
	}
	return res
}
