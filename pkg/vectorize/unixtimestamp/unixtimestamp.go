// Copyright 2022 Matrix Origin
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

package unixtimestamp

import (
	"github.com/matrixorigin/matrixone/pkg/container/types"
)

var (
	UnixTimestampToInt        func([]types.Timestamp, []int64) []int64
	UnixTimestampToFloat      func([]types.Timestamp, []float64) []float64
	UnixTimestampToDecimal128 func([]types.Timestamp, []types.Decimal128) ([]types.Decimal128, error)
)

func init() {
	UnixTimestampToInt = unixTimestampToInt
	UnixTimestampToFloat = unixTimestampToFloat
	UnixTimestampToDecimal128 = unixTimestampToDecimal128
}

func unixTimestampToInt(xs []types.Timestamp, rs []int64) []int64 {
	for i := range xs {
		rs[i] = xs[i].Unix()
	}
	return rs
}

func unixTimestampToFloat(xs []types.Timestamp, rs []float64) []float64 {
	for i := range xs {
		rs[i] = xs[i].UnixToFloat()
	}
	return rs
}

func unixTimestampToDecimal128(xs []types.Timestamp, rs []types.Decimal128) ([]types.Decimal128, error) {
	for i := range xs {
		res, err := xs[i].UnixToDecimal128()
		if err != nil {
			return nil, err
		}
		rs[i] = res
	}
	return rs, nil
}
