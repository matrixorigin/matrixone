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

package compute

import (
	"bytes"

	"github.com/matrixorigin/matrixone/pkg/container/types"
)

func CompareOrdered[T types.OrderedT](v1, v2 any) int64 {
	a, b := v1.(T), v2.(T)
	if a > b {
		return 1
	} else if a < b {
		return -1
	}
	return 0
}

func CompareBool(a, b bool) int64 {
	if a && b {
		return 0
	} else if !a && !b {
		return 0
	} else if a {
		return 1
	}
	return 0
}

func CompareBytes(a, b any) int64 {
	res := bytes.Compare(a.([]byte), b.([]byte))
	if res > 0 {
		return 1
	} else if res < 0 {
		return -1
	} else {
		return 0
	}
}

func CompareGeneric(a, b any, t types.Type) int64 {
	switch t.Oid {
	case types.T_bool:
		return CompareBool(a.(bool), b.(bool))
	case types.T_int8:
		return CompareOrdered[int8](a, b)
	case types.T_int16:
		return CompareOrdered[int16](a, b)
	case types.T_int32:
		return CompareOrdered[int32](a, b)
	case types.T_int64:
		return CompareOrdered[int64](a, b)
	case types.T_uint8:
		return CompareOrdered[uint8](a, b)
	case types.T_uint16:
		return CompareOrdered[uint16](a, b)
	case types.T_uint32:
		return CompareOrdered[uint32](a, b)
	case types.T_uint64:
		return CompareOrdered[uint64](a, b)
	case types.T_decimal64:
		return types.CompareDecimal64Decimal64Aligned(a.(types.Decimal64), b.(types.Decimal64))
	case types.T_decimal128:
		return types.CompareDecimal128Decimal128Aligned(a.(types.Decimal128), b.(types.Decimal128))
	case types.T_float32:
		return CompareOrdered[float32](a, b)
	case types.T_float64:
		return CompareOrdered[float64](a, b)
	case types.T_timestamp:
		return CompareOrdered[types.Timestamp](a, b)
	case types.T_date:
		return CompareOrdered[types.Date](a, b)
	case types.T_datetime:
		return CompareOrdered[types.Datetime](a, b)
	case types.T_char, types.T_varchar, types.T_blob, types.T_json:
		return CompareBytes(a, b)
	default:
		panic("unsupported type")
	}
}
