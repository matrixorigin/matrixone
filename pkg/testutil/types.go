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
	"math/rand"

	"github.com/matrixorigin/matrixone/pkg/container/types"
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
	binaryType     = types.T_binary.ToType()
	varbinaryType  = types.T_varbinary.ToType()
	decimal64Type  = types.T_decimal64.ToType()
	decimal128Type = types.T_decimal128.ToType()
	dateType       = types.T_date.ToType()
	timeType       = types.T_time.ToType()
	datetimeType   = types.T_datetime.ToType()
	timestampType  = types.T_timestamp.ToType()
	blobType       = types.T_blob.ToType()
	textType       = types.T_text.ToType()
	uuidType       = types.T_uuid.ToType()
	rowIdType      = types.T_Rowid.ToType()
	enumType       = types.T_enum.ToType()
)

func MakeDecimal64Type(width, scalar int32) types.Type {
	d64 := types.T_decimal64.ToType()
	d64.Scale = scalar
	d64.Width = width
	return d64
}

func MakeDecimal128Type(width, scalar int32) types.Type {
	d128 := types.T_decimal128.ToType()
	d128.Scale = scalar
	d128.Width = width
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
