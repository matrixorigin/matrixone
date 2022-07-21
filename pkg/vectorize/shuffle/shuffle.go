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

package shuffle

import (
	"github.com/matrixorigin/matrixone/pkg/container/types"
	"golang.org/x/exp/constraints"
)

var (
	BoolShuffle  = fixedLengthShuffle[bool]
	Int8Shuffle  = fixedLengthShuffle[int8]
	Int16Shuffle = fixedLengthShuffle[int16]
	Int32Shuffle = fixedLengthShuffle[int32]
	Int64Shuffle = fixedLengthShuffle[int64]

	Uint8Shuffle  = fixedLengthShuffle[uint8]
	Uint16Shuffle = fixedLengthShuffle[uint16]
	Uint32Shuffle = fixedLengthShuffle[uint32]
	Uint64Shuffle = fixedLengthShuffle[uint64]

	Float32Shuffle = fixedLengthShuffle[float32]
	Float64Shuffle = fixedLengthShuffle[float64]

	Decimal64Shuffle  = fixedLengthShuffle[types.Decimal64]
	Decimal128Shuffle = fixedLengthShuffle[types.Decimal128]

	DateShuffle      = fixedLengthShuffle[types.Date]
	DatetimeShuffle  = fixedLengthShuffle[types.Datetime]
	TimestampShuffle = fixedLengthShuffle[types.Timestamp]

	TupleShuffle = tupleShuffle

	StrShuffle = strShuffle
)

type fixedLength interface {
	constraints.Integer | constraints.Float | types.Decimal64 | types.Decimal128 | bool
}

func fixedLengthShuffle[T fixedLength](vs, ws []T, sels []int64) []T {
	for i, sel := range sels {
		ws[i] = vs[sel]
	}
	copy(vs, ws)
	return vs[:len(sels)]
}

func tupleShuffle(vs, ws [][]interface{}, sels []int64) [][]interface{} {
	for i, sel := range sels {
		ws[i] = vs[sel]
	}
	return ws[:len(sels)]
}

func strShuffle(vs *types.Bytes, os, ns []uint32, sels []int64) *types.Bytes {
	for i, sel := range sels {
		os[i] = vs.Offsets[sel]
		ns[i] = vs.Lengths[sel]
	}
	copy(vs.Offsets, os)
	copy(vs.Lengths, ns)
	vs.Offsets, vs.Lengths = vs.Offsets[:len(sels)], vs.Lengths[:len(sels)]
	return vs
}
