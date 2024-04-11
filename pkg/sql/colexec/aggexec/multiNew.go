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

func newMultiAggFuncExecRetFixed(
	mg AggMemoryManager, info multiAggInfo, impl multiColumnAggImplementation) AggFuncExec {

	switch info.retType.Oid {
	case types.T_bool:
		e := &multiAggFuncExec1[bool]{}
		e.init(mg, info, impl)
		return e
	case types.T_int8:
		e := &multiAggFuncExec1[int8]{}
		e.init(mg, info, impl)
		return e
	case types.T_int16:
		e := &multiAggFuncExec1[int16]{}
		e.init(mg, info, impl)
		return e
	case types.T_int32:
		e := &multiAggFuncExec1[int32]{}
		e.init(mg, info, impl)
		return e
	case types.T_int64:
		e := &multiAggFuncExec1[int64]{}
		e.init(mg, info, impl)
		return e
	case types.T_uint8:
		e := &multiAggFuncExec1[uint8]{}
		e.init(mg, info, impl)
		return e
	case types.T_uint16:
		e := &multiAggFuncExec1[uint16]{}
		e.init(mg, info, impl)
		return e
	case types.T_uint32:
		e := &multiAggFuncExec1[uint32]{}
		e.init(mg, info, impl)
		return e
	case types.T_uint64:
		e := &multiAggFuncExec1[uint64]{}
		e.init(mg, info, impl)
		return e
	case types.T_float32:
		e := &multiAggFuncExec1[float32]{}
		e.init(mg, info, impl)
		return e
	case types.T_float64:
		e := &multiAggFuncExec1[float64]{}
		e.init(mg, info, impl)
		return e
	case types.T_decimal64:
		e := &multiAggFuncExec1[types.Decimal64]{}
		e.init(mg, info, impl)
		return e
	case types.T_decimal128:
		e := &multiAggFuncExec1[types.Decimal128]{}
		e.init(mg, info, impl)
		return e
	case types.T_date:
		e := &multiAggFuncExec1[types.Date]{}
		e.init(mg, info, impl)
		return e
	case types.T_datetime:
		e := &multiAggFuncExec1[types.Datetime]{}
		e.init(mg, info, impl)
		return e
	case types.T_time:
		e := &multiAggFuncExec1[types.Time]{}
		e.init(mg, info, impl)
		return e
	case types.T_timestamp:
		e := &multiAggFuncExec1[types.Timestamp]{}
		e.init(mg, info, impl)
		return e
	}

	panic(fmt.Sprintf("unexpected parameter to Init a multiAggFuncExec, aggInfo: %s", info))
}
