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
	"github.com/matrixorigin/matrixone/pkg/container/types"
	"math/rand"
)

var (
	boolType       = types.T_bool.ToType()
	int8Type       = types.T_int8.ToType()
	int16Type      = types.T_int16.ToType()
	int32Type      = types.T_int32.ToType()
	int64Type      = types.T_int64.ToType()
	uint8Type      = types.T_uint8.ToType()
	uint16Type     = types.T_uint16.ToType()
	uint32Type     = types.T_uint32.ToType()
	uint64Type     = types.T_uint64.ToType()
	float32Type    = types.T_float32.ToType()
	float64Type    = types.T_float64.ToType()
	charType       = types.T_char.ToType()
	varcharType    = types.T_varchar.ToType()
	decimal64Type  = types.T_decimal64.ToType()
	decimal128Type = types.T_decimal128.ToType()
	dateType       = types.T_date.ToType()
	datetimeType   = types.T_datetime.ToType()
	timestampType  = types.T_timestamp.ToType()
)

func MakeDecimal64Type(precision, scalar int32) types.Type {
	d64 := types.T_decimal64.ToType()
	d64.Scale = scalar
	d64.Width = precision
	return d64
}

func MakeDecimal128Type(precision, scalar int32) types.Type {
	d128 := types.T_decimal128.ToType()
	d128.Scale = scalar
	d128.Width = precision
	return d128
}

func MakeBatchZs(n int, random bool) []int64 {
	zs := make([]int64, n)
	if random {
		for i := 0; i < n; i++ {
			v := rand.Intn(5)
			zs[i] = int64(v) + 1
		}
	} else {
		for i := 0; i < n; i++ {
			zs[i] = 1
		}
	}
	return zs
}
