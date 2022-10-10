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

package external

import (
	"github.com/matrixorigin/matrixone/pkg/common/moerr"
	"github.com/matrixorigin/matrixone/pkg/container/nulls"
	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/pb/plan"
	"github.com/stretchr/testify/require"
	"math"
	"strconv"
	"testing"
)

var (
	cols = []*plan.ColDef{
		{
			Typ: &plan.Type{
				Id: int32(types.T_bool),
			},
		},
		{
			Typ: &plan.Type{
				Id: int32(types.T_int8),
			},
		},
		{
			Typ: &plan.Type{
				Id: int32(types.T_int16),
			},
		},
		{
			Typ: &plan.Type{
				Id: int32(types.T_int32),
			},
		},
		{
			Typ: &plan.Type{
				Id: int32(types.T_int64),
			},
		},
		{
			Typ: &plan.Type{
				Id: int32(types.T_uint8),
			},
		},
		{
			Typ: &plan.Type{
				Id: int32(types.T_uint16),
			},
		},
		{
			Typ: &plan.Type{
				Id: int32(types.T_uint32),
			},
		},
		{
			Typ: &plan.Type{
				Id: int32(types.T_uint64),
			},
		},
		{
			Typ: &plan.Type{
				Id: int32(types.T_float32),
			},
		},
		{
			Typ: &plan.Type{
				Id: int32(types.T_float64),
			},
		},
		{
			Typ: &plan.Type{
				Id: int32(types.T_varchar),
			},
		},
		{
			Typ: &plan.Type{
				Id: int32(types.T_json),
			},
		},
		{
			Typ: &plan.Type{
				Id: int32(types.T_date),
			},
		},
		{
			Typ: &plan.Type{
				Id: int32(types.T_datetime),
			},
		},
		{
			Typ: &plan.Type{
				Id:    int32(types.T_decimal64),
				Width: 15,
				Scale: 0,
			},
		},
		{
			Typ: &plan.Type{
				Id:    int32(types.T_decimal128),
				Width: 17,
				Scale: 0,
			},
		},
		{
			Typ: &plan.Type{
				Id: int32(types.T_timestamp),
			},
		},
	}

	tests = [][]string{
		{"true", "false", "1", "0", "dad", "error"},
		{"1.1", "2", "3", "4", "256", "error"},
		{"1", "2.1", "3", "4", "999999999", "error"},
		{"1", "2", "3", "4", strconv.Itoa(math.MaxInt64), "error"},
		{"1", "2", "3", "4", "18446744073709551616", "error"},
		{"1", "2.1", "3", "4", "255", "success"},
		{"1", "2", "3", "4", "65535", "success"},
		{"1", "2", "3", "4", "4294967295", "success"},
		{"1", "2", "3", "4", "18446744073709551616", "error"},
		{"1.1", "1.2", "1.3", "1.4", "1.5", "success"},
		{"1.111", "1.2111", "1.31111", "1.411111", "1.5111111", "success"},
		{"12121", "dafege", "success"},
		{"{\"a\":1}", "{\"a\":2}", "{", "error"},
		{"2020-09-07", "2020-09-08", "success"},
		{"2020-09-07 00:00:00", "2020-09-08 00:00:00", "success"},
		{"1.111", "1.2111", "success"},
		{"1.111", "1.2111", "success"},
		{"2020-09-07 00:00:00", "2020-09-08 00:00:00", "2020/1212/", "error"},
	}
)

func TestExternal(t *testing.T) {
	var err error
	for i, test := range tests {
		err = nil
		id := types.T(cols[i].Typ.Id)
		status := test[len(test)-1]
		test = test[:len(test)-1]
		switch id {
		case types.T_bool:
			rs := make([]bool, len(test))
			test, err = TrimSpace(test)
			require.Nil(t, err)
			nsp := ParseNullFlagNormal(test, []string{"true"})

			InsertNsp(nsp, &nulls.Nulls{})
			rs, err = ParseBool(test, &nulls.Nulls{}, rs)
			if status == "success" {
				require.Nil(t, err)
				require.NotNil(t, rs)
			} else {
				require.NotNil(t, err)
			}
		case types.T_int8:
			rs := make([]int8, len(test))
			test, err = TrimSpace(test)
			require.Nil(t, err)
			ParseNullFlagNormal(test, []string{"1212"})
			rs, err = ParseInt8(test, &nulls.Nulls{}, rs)
			if status == "success" {
				require.Nil(t, err)
				require.NotNil(t, rs)
			} else {
				require.NotNil(t, err)
			}
		case types.T_int16:
			rs := make([]int16, len(test))
			rs, err = ParseInt16(test, &nulls.Nulls{}, rs)
			if status == "success" {
				require.Nil(t, err)
				require.NotNil(t, rs)
			} else {
				require.NotNil(t, err)
			}
		case types.T_int32:
			rs := make([]int32, len(test))
			rs, err = ParseInt32(test, &nulls.Nulls{}, rs)
			if status == "success" {
				require.Nil(t, err)
				require.NotNil(t, rs)
			} else {
				require.NotNil(t, err)
			}
		case types.T_int64:
			rs := make([]int64, len(test))
			rs, err = ParseInt64(test, &nulls.Nulls{}, rs)
			if status == "success" {
				require.Nil(t, err)
				require.NotNil(t, rs)
			} else {
				require.NotNil(t, err)
			}
		case types.T_uint8:
			rs := make([]uint8, len(test))
			rs, err = ParseUint8(test, &nulls.Nulls{}, rs)
			if status == "success" {
				require.Nil(t, err)
				require.NotNil(t, rs)
			} else {
				require.NotNil(t, err)
			}
		case types.T_uint16:
			rs := make([]uint16, len(test))
			rs, err = ParseUint16(test, &nulls.Nulls{}, rs)
			if status == "success" {
				require.Nil(t, err)
				require.NotNil(t, rs)
			} else {
				require.NotNil(t, err)
			}
		case types.T_uint32:
			rs := make([]uint32, len(test))
			rs, err = ParseUint32(test, &nulls.Nulls{}, rs)
			if status == "success" {
				require.Nil(t, err)
				require.NotNil(t, rs)
			} else {
				require.NotNil(t, err)
			}
		case types.T_uint64:
			rs := make([]uint64, len(test))
			rs, err = ParseUint64(test, &nulls.Nulls{}, rs)
			if status == "success" {
				require.Nil(t, err)
				require.NotNil(t, rs)
			} else {
				require.NotNil(t, err)
			}
		case types.T_float32:
			rs := make([]float32, len(test))
			rs, err = ParseFloat32(test, &nulls.Nulls{}, rs)
			if status == "success" {
				require.Nil(t, err)
				require.NotNil(t, rs)
			} else {
				require.NotNil(t, err)
			}
		case types.T_float64:
			rs := make([]float64, len(test))
			rs, err = ParseFloat64(test, &nulls.Nulls{}, rs)
			if status == "success" {
				require.Nil(t, err)
				require.NotNil(t, rs)
			} else {
				require.NotNil(t, err)
			}
		case types.T_decimal64:
			rs := make([]types.Decimal64, len(test))
			rs, err = ParseDecimal64(test, &nulls.Nulls{}, cols[i].Typ.Width, cols[i].Typ.Scale, rs)
			if status == "success" {
				require.Nil(t, err)
				require.NotNil(t, rs)
			} else {
				require.NotNil(t, err)
			}
		case types.T_decimal128:
			rs := make([]types.Decimal128, len(test))
			rs, err = ParseDecimal128(test, &nulls.Nulls{}, cols[i].Typ.Width, cols[i].Typ.Scale, rs)
			if status == "success" {
				require.Nil(t, err)
				require.NotNil(t, rs)
			} else {
				require.NotNil(t, err)
			}
		case types.T_char, types.T_varchar, types.T_blob:
			ParseNullFlagStrings(test, []string{"aad"})
			getNullFlag([]string{"aad"}, "aad")
			getNullFlag([]string{"aad"}, test[0])
			if status == "success" {
				require.Nil(t, err)
			} else {
				require.NotNil(t, err)
			}
		case types.T_json:
			rs := make([][]byte, len(test))
			rs, err = ParseJson(test, &nulls.Nulls{}, rs)
			if status == "success" {
				require.Nil(t, err)
				require.NotNil(t, rs)
			} else {
				require.NotNil(t, err)
			}
		case types.T_date:
			rs := make([]types.Date, len(test))
			rs, err = ParseDate(test, &nulls.Nulls{}, rs)
			if status == "success" {
				require.Nil(t, err)
				require.NotNil(t, rs)
			} else {
				require.NotNil(t, err)
			}
		case types.T_datetime:
			rs := make([]types.Datetime, len(test))
			rs, err = ParseDateTime(test, &nulls.Nulls{}, cols[i].Typ.Precision, rs)
			if status == "success" {
				require.Nil(t, err)
				require.NotNil(t, rs)
			} else {
				require.NotNil(t, err)
			}
		case types.T_timestamp:
			rs := make([]types.Timestamp, len(test))
			rs, err = ParseTimeStamp(test, &nulls.Nulls{}, cols[i].Typ.Precision, rs)
			if status == "success" {
				require.Nil(t, err)
				require.NotNil(t, rs)
			} else {
				require.NotNil(t, err)
			}
		default:
			err = moerr.NewNotSupported("the value type %d is not support now", id)
			require.NotNil(t, err)
		}
	}
}
