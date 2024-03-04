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

import (
	"fmt"
	"github.com/matrixorigin/matrixone/pkg/container/types"
)

// all the codes in this file were to new the multiple column aggregation executors.

func newMultiAggFuncExec(
	mg AggMemoryManager, info multiAggInfo, implementationAllocator any) AggFuncExec {
	if info.retType.IsVarlen() {
		if g, ok := implementationAllocator.(func() MultiAggRetVar); ok {
			e := &multiAggFuncExec2{}
			e.init(mg, info, g)
			return e
		}
	} else {
		switch info.retType.Oid {
		case types.T_bool:
			if g, ok := implementationAllocator.(func() MultiAggRetFixed[bool]); ok {
				e := &multiAggFuncExec1[bool]{}
				e.init(mg, info, g)
				return e
			}
		case types.T_int8:
			if g, ok := implementationAllocator.(func() MultiAggRetFixed[int8]); ok {
				e := &multiAggFuncExec1[int8]{}
				e.init(mg, info, g)
				return e
			}
		case types.T_int16:
			if g, ok := implementationAllocator.(func() MultiAggRetFixed[int16]); ok {
				e := &multiAggFuncExec1[int16]{}
				e.init(mg, info, g)
				return e
			}
		case types.T_int32:
			if g, ok := implementationAllocator.(func() MultiAggRetFixed[int32]); ok {
				e := &multiAggFuncExec1[int32]{}
				e.init(mg, info, g)
				return e
			}
		case types.T_int64:
			if g, ok := implementationAllocator.(func() MultiAggRetFixed[int64]); ok {
				e := &multiAggFuncExec1[int64]{}
				e.init(mg, info, g)
				return e
			}
		case types.T_uint8:
			if g, ok := implementationAllocator.(func() MultiAggRetFixed[uint8]); ok {
				e := &multiAggFuncExec1[uint8]{}
				e.init(mg, info, g)
				return e
			}
		case types.T_uint16:
			if g, ok := implementationAllocator.(func() MultiAggRetFixed[uint16]); ok {
				e := &multiAggFuncExec1[uint16]{}
				e.init(mg, info, g)
				return e
			}
		case types.T_uint32:
			if g, ok := implementationAllocator.(func() MultiAggRetFixed[uint32]); ok {
				e := &multiAggFuncExec1[uint32]{}
				e.init(mg, info, g)
				return e
			}
		case types.T_uint64:
			if g, ok := implementationAllocator.(func() MultiAggRetFixed[uint64]); ok {
				e := &multiAggFuncExec1[uint64]{}
				e.init(mg, info, g)
				return e
			}
		case types.T_float32:
			if g, ok := implementationAllocator.(func() MultiAggRetFixed[float32]); ok {
				e := &multiAggFuncExec1[float32]{}
				e.init(mg, info, g)
				return e
			}
		case types.T_float64:
			if g, ok := implementationAllocator.(func() MultiAggRetFixed[float64]); ok {
				e := &multiAggFuncExec1[float64]{}
				e.init(mg, info, g)
				return e
			}
		case types.T_decimal64:
			if g, ok := implementationAllocator.(func() MultiAggRetFixed[types.Decimal64]); ok {
				e := &multiAggFuncExec1[types.Decimal64]{}
				e.init(mg, info, g)
				return e
			}
		case types.T_decimal128:
			if g, ok := implementationAllocator.(func() MultiAggRetFixed[types.Decimal128]); ok {
				e := &multiAggFuncExec1[types.Decimal128]{}
				e.init(mg, info, g)
				return e
			}
		case types.T_date:
			if g, ok := implementationAllocator.(func() MultiAggRetFixed[types.Date]); ok {
				e := &multiAggFuncExec1[types.Date]{}
				e.init(mg, info, g)
				return e
			}
		case types.T_datetime:
			if g, ok := implementationAllocator.(func() MultiAggRetFixed[types.Datetime]); ok {
				e := &multiAggFuncExec1[types.Datetime]{}
				e.init(mg, info, g)
				return e
			}
		case types.T_time:
			if g, ok := implementationAllocator.(func() MultiAggRetFixed[types.Time]); ok {
				e := &multiAggFuncExec1[types.Time]{}
				e.init(mg, info, g)
				return e
			}
		case types.T_timestamp:
			if g, ok := implementationAllocator.(func() MultiAggRetFixed[types.Timestamp]); ok {
				e := &multiAggFuncExec1[types.Timestamp]{}
				e.init(mg, info, g)
				return e
			}
		default:
			panic(fmt.Sprintf("unsupported parameter type for multiAggFuncExec, aggInfo: %s", info))
		}
	}

	panic(fmt.Sprintf("unexpected parameter to Init a multiAggFuncExec, aggInfo: %s", info))
}
