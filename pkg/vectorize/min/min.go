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

package min

import (
	"bytes"

	"github.com/matrixorigin/matrixone/pkg/container/types"
	"golang.org/x/exp/constraints"
)

var (
	BoolMin     = boolMin
	BoolMinSels = boolMinSels

	Int8Min        = numericMin[int8]
	Int16Min       = numericMin[int16]
	Int32Min       = numericMin[int32]
	Int64Min       = numericMin[int64]
	Uint8Min       = numericMin[uint8]
	Uint16Min      = numericMin[uint16]
	Uint32Min      = numericMin[uint32]
	Uint64Min      = numericMin[uint64]
	Float32Min     = numericMin[float32]
	Float64Min     = numericMin[float64]
	Int8MinSels    = numericMinSels[int8]
	Int16MinSels   = numericMinSels[int16]
	Int32MinSels   = numericMinSels[int32]
	Int64MinSels   = numericMinSels[int64]
	Uint8MinSels   = numericMinSels[uint8]
	Uint16MinSels  = numericMinSels[uint16]
	Uint32MinSels  = numericMinSels[uint32]
	Uint64MinSels  = numericMinSels[uint64]
	Float32MinSels = numericMinSels[float32]
	Float64MinSels = numericMinSels[float64]

	StrMin     = strMin
	StrMinSels = strMinSels
)

func boolMin(xs []bool) bool {
	for _, x := range xs {
		if !x {
			return false
		}
	}
	return true
}

func boolMinSels(xs []bool, sels []int64) bool {
	for _, sel := range sels {
		if !xs[sel] {
			return false
		}
	}
	return true
}

func numericMin[T constraints.Integer | constraints.Float](xs []T) T {
	res := xs[0]
	for _, x := range xs {
		if x < res {
			res = x
		}
	}
	return res
}

func numericMinSels[T constraints.Integer | constraints.Float](xs []T, sels []int64) T {
	res := xs[sels[0]]
	for _, sel := range sels {
		x := xs[sel]
		if x < res {
			res = x
		}
	}
	return res
}

func strMin(xs *types.Bytes) []byte {
	res := xs.Get(0)
	for i, n := 0, len(xs.Offsets); i < n; i++ {
		x := xs.Get(int64(i))
		if bytes.Compare(x, res) < 0 {
			res = x
		}
	}
	return res
}

func strMinSels(xs *types.Bytes, sels []int64) []byte {
	res := xs.Get(sels[0])
	for _, sel := range sels {
		x := xs.Get(sel)
		if bytes.Compare(x, res) < 0 {
			res = x
		}
	}
	return res
}
