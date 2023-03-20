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

package neg

import (
	"github.com/matrixorigin/matrixone/pkg/container/types"
	"golang.org/x/exp/constraints"
)

var (
	Int8Neg    = NumericNeg[int8]
	Int16Neg   = NumericNeg[int16]
	Int32Neg   = NumericNeg[int32]
	Int64Neg   = NumericNeg[int64]
	Float32Neg = NumericNeg[float32]
	Float64Neg = NumericNeg[float64]
)

func NumericNeg[T constraints.Signed | constraints.Float](xs, rs []T) []T {
	for i, x := range xs {
		rs[i] = -x
	}
	return rs
}

func Decimal64Neg(xs, rs []types.Decimal64) []types.Decimal64 {
	for i, x := range xs {
		rs[i] = x.Minus()
	}
	return rs
}

func Decimal128Neg(xs, rs []types.Decimal128) []types.Decimal128 {
	for i, x := range xs {
		rs[i] = x.Minus()
	}
	return rs
}
