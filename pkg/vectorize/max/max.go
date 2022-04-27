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

	"github.com/matrixorigin/matrixone/pkg/container/types"
	"golang.org/x/exp/constraints"
)

var (
	BoolMax     = boolMax
	BoolMaxSels = boolMaxSels

	Int8Max        = numericMax[int8]
	Int16Max       = numericMax[int16]
	Int32Max       = numericMax[int32]
	Int64Max       = numericMax[int64]
	Uint8Max       = numericMax[uint8]
	Uint16Max      = numericMax[uint16]
	Uint32Max      = numericMax[uint32]
	Uint64Max      = numericMax[uint64]
	Float32Max     = numericMax[float32]
	Float64Max     = numericMax[float64]
	Int8MaxSels    = numericMaxSels[int8]
	Int16MaxSels   = numericMaxSels[int16]
	Int32MaxSels   = numericMaxSels[int32]
	Int64MaxSels   = numericMaxSels[int64]
	Uint8MaxSels   = numericMaxSels[uint8]
	Uint16MaxSels  = numericMaxSels[uint16]
	Uint32MaxSels  = numericMaxSels[uint32]
	Uint64MaxSels  = numericMaxSels[uint64]
	Float32MaxSels = numericMaxSels[float32]
	Float64MaxSels = numericMaxSels[float64]

	StrMax     = strMax
	StrMaxSels = strMaxSels
)

func boolMax(xs []bool) bool {
	for _, x := range xs {
		if x {
			return true
		}
	}
	return false
}

func boolMaxSels(xs []bool, sels []int64) bool {
	for _, sel := range sels {
		if xs[sel] {
			return true
		}
	}
	return false
}

func numericMax[T constraints.Integer | constraints.Float](xs []T) T {
	res := xs[0]
	for _, x := range xs {
		if x > res {
			res = x
		}
	}
	return res
}

func numericMaxSels[T constraints.Integer | constraints.Float](xs []T, sels []int64) T {
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
