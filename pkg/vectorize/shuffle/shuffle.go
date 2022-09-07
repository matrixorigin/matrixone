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
)

var (
	BoolShuffle  = FixedLengthShuffle[bool]
	Int8Shuffle  = FixedLengthShuffle[int8]
	Int16Shuffle = FixedLengthShuffle[int16]
	Int32Shuffle = FixedLengthShuffle[int32]
	Int64Shuffle = FixedLengthShuffle[int64]

	Uint8Shuffle  = FixedLengthShuffle[uint8]
	Uint16Shuffle = FixedLengthShuffle[uint16]
	Uint32Shuffle = FixedLengthShuffle[uint32]
	Uint64Shuffle = FixedLengthShuffle[uint64]

	Float32Shuffle = FixedLengthShuffle[float32]
	Float64Shuffle = FixedLengthShuffle[float64]

	Decimal64Shuffle  = FixedLengthShuffle[types.Decimal64]
	Decimal128Shuffle = FixedLengthShuffle[types.Decimal128]

	DateShuffle      = FixedLengthShuffle[types.Date]
	DatetimeShuffle  = FixedLengthShuffle[types.Datetime]
	TimestampShuffle = FixedLengthShuffle[types.Timestamp]

	VarlenaShuffle = FixedLengthShuffle[types.Varlena]

	TupleShuffle = tupleShuffle
)

func FixedLengthShuffle[T types.FixedSizeT](vs, ws []T, sels []int64) []T {
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
