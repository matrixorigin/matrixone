// Copyright 2024 Matrix Origin
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

package aggexec

import "github.com/matrixorigin/matrixone/pkg/container/types"

// all the codes in this file were to new the multiple column aggregation executors.

func newMultiAggFuncExec(to types.Type) AggFuncExec {
	if to.IsVarlen() {
		return &multiAggFuncExec2{}
	}

	switch to.Oid {
	case types.T_bool:
		return &multiAggFuncExec1[bool]{}
	case types.T_int8:
		return &multiAggFuncExec1[int8]{}
	case types.T_int16:
		return &multiAggFuncExec1[int16]{}
	case types.T_int32:
		return &multiAggFuncExec1[int32]{}
	case types.T_int64:
		return &multiAggFuncExec1[int64]{}
	case types.T_uint8:
		return &multiAggFuncExec1[uint8]{}
	case types.T_uint16:
		return &multiAggFuncExec1[uint16]{}
	case types.T_uint32:
		return &multiAggFuncExec1[uint32]{}
	case types.T_uint64:
		return &multiAggFuncExec1[uint64]{}
	case types.T_float32:
		return &multiAggFuncExec1[float32]{}
	case types.T_float64:
		return &multiAggFuncExec1[float64]{}
	case types.T_decimal64:
		return &multiAggFuncExec1[types.Decimal64]{}
	case types.T_decimal128:
		return &multiAggFuncExec1[types.Decimal128]{}
	case types.T_date:
		return &multiAggFuncExec1[types.Date]{}
	case types.T_datetime:
		return &multiAggFuncExec1[types.Datetime]{}
	case types.T_time:
		return &multiAggFuncExec1[types.Time]{}
	case types.T_timestamp:
		return &multiAggFuncExec1[types.Timestamp]{}
	}
	panic("unsupported result type for multiAggFuncExec")
}
