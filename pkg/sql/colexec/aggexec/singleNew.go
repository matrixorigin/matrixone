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

// all the codes in this file were to make the single column aggregation executor from agg information.

func newSingleAggFuncExec1NewVersion(
	mg AggMemoryManager, info singleAggInfo, impl aggImplementation) AggFuncExec {
	switch info.retType.Oid {
	case types.T_bool:
		return newSingleAggFuncExec1NewVersionWithKnownResultType[bool](mg, info, impl)
	case types.T_int8:
		return newSingleAggFuncExec1NewVersionWithKnownResultType[int8](mg, info, impl)
	case types.T_int16:
		return newSingleAggFuncExec1NewVersionWithKnownResultType[int16](mg, info, impl)
	case types.T_int32:
		return newSingleAggFuncExec1NewVersionWithKnownResultType[int32](mg, info, impl)
	case types.T_int64:
		return newSingleAggFuncExec1NewVersionWithKnownResultType[int64](mg, info, impl)
	case types.T_uint8:
		return newSingleAggFuncExec1NewVersionWithKnownResultType[uint8](mg, info, impl)
	case types.T_uint16:
		return newSingleAggFuncExec1NewVersionWithKnownResultType[uint16](mg, info, impl)
	case types.T_uint32:
		return newSingleAggFuncExec1NewVersionWithKnownResultType[uint32](mg, info, impl)
	case types.T_uint64:
		return newSingleAggFuncExec1NewVersionWithKnownResultType[uint64](mg, info, impl)
	case types.T_float32:
		return newSingleAggFuncExec1NewVersionWithKnownResultType[float32](mg, info, impl)
	case types.T_float64:
		return newSingleAggFuncExec1NewVersionWithKnownResultType[float64](mg, info, impl)
	case types.T_decimal64:
		return newSingleAggFuncExec1NewVersionWithKnownResultType[types.Decimal64](mg, info, impl)
	case types.T_decimal128:
		return newSingleAggFuncExec1NewVersionWithKnownResultType[types.Decimal128](mg, info, impl)
	case types.T_date:
		return newSingleAggFuncExec1NewVersionWithKnownResultType[types.Date](mg, info, impl)
	case types.T_datetime:
		return newSingleAggFuncExec1NewVersionWithKnownResultType[types.Datetime](mg, info, impl)
	case types.T_time:
		return newSingleAggFuncExec1NewVersionWithKnownResultType[types.Time](mg, info, impl)
	case types.T_timestamp:
		return newSingleAggFuncExec1NewVersionWithKnownResultType[types.Timestamp](mg, info, impl)
	case types.T_bit:
		return newSingleAggFuncExec1NewVersionWithKnownResultType[uint64](mg, info, impl)
	case types.T_TS:
		return newSingleAggFuncExec1NewVersionWithKnownResultType[types.TS](mg, info, impl)
	case types.T_Rowid:
		return newSingleAggFuncExec1NewVersionWithKnownResultType[types.Rowid](mg, info, impl)
	case types.T_Blockid:
		return newSingleAggFuncExec1NewVersionWithKnownResultType[types.Blockid](mg, info, impl)
	case types.T_uuid:
		return newSingleAggFuncExec1NewVersionWithKnownResultType[types.Uuid](mg, info, impl)
	}
	panic(fmt.Sprintf("unsupported result type %s for single column agg executor1", info.retType))
}

func newSingleAggFuncExec1NewVersionWithKnownResultType[to types.FixedSizeTExceptStrType](mg AggMemoryManager, info singleAggInfo, impl aggImplementation) AggFuncExec {

	switch info.argType.Oid {
	case types.T_bool:
		e := &singleAggFuncExecNew1[bool, to]{}
		e.init(mg, info, impl)
		return e

	case types.T_bit:
		e := &singleAggFuncExecNew1[uint64, to]{}
		e.init(mg, info, impl)
		return e

	case types.T_int8:
		e := &singleAggFuncExecNew1[int8, to]{}
		e.init(mg, info, impl)
		return e

	case types.T_int16:
		e := &singleAggFuncExecNew1[int16, to]{}
		e.init(mg, info, impl)
		return e

	case types.T_int32:
		e := &singleAggFuncExecNew1[int32, to]{}
		e.init(mg, info, impl)
		return e

	case types.T_int64:
		e := &singleAggFuncExecNew1[int64, to]{}
		e.init(mg, info, impl)
		return e

	case types.T_uint8:
		e := &singleAggFuncExecNew1[uint8, to]{}
		e.init(mg, info, impl)
		return e

	case types.T_uint16:
		e := &singleAggFuncExecNew1[uint16, to]{}
		e.init(mg, info, impl)
		return e

	case types.T_uint32:
		e := &singleAggFuncExecNew1[uint32, to]{}
		e.init(mg, info, impl)
		return e

	case types.T_uint64:
		e := &singleAggFuncExecNew1[uint64, to]{}
		e.init(mg, info, impl)
		return e

	case types.T_float32:
		e := &singleAggFuncExecNew1[float32, to]{}
		e.init(mg, info, impl)
		return e

	case types.T_float64:
		e := &singleAggFuncExecNew1[float64, to]{}
		e.init(mg, info, impl)
		return e

	case types.T_decimal64:
		e := &singleAggFuncExecNew1[types.Decimal64, to]{}
		e.init(mg, info, impl)
		return e

	case types.T_decimal128:
		e := &singleAggFuncExecNew1[types.Decimal128, to]{}
		e.init(mg, info, impl)
		return e

	case types.T_date:
		e := &singleAggFuncExecNew1[types.Date, to]{}
		e.init(mg, info, impl)
		return e

	case types.T_datetime:
		e := &singleAggFuncExecNew1[types.Datetime, to]{}
		e.init(mg, info, impl)
		return e

	case types.T_time:
		e := &singleAggFuncExecNew1[types.Time, to]{}
		e.init(mg, info, impl)
		return e

	case types.T_timestamp:
		e := &singleAggFuncExecNew1[types.Timestamp, to]{}
		e.init(mg, info, impl)
		return e

	case types.T_TS:
		e := &singleAggFuncExecNew1[types.TS, to]{}
		e.init(mg, info, impl)
		return e

	case types.T_Rowid:
		e := &singleAggFuncExecNew1[types.Rowid, to]{}
		e.init(mg, info, impl)
		return e

	case types.T_Blockid:
		e := &singleAggFuncExecNew1[types.Rowid, to]{}
		e.init(mg, info, impl)
		return e

	case types.T_uuid:
		e := &singleAggFuncExecNew1[types.Uuid, to]{}
		e.init(mg, info, impl)
		return e
	}
	panic(fmt.Sprintf("unexpected parameter to Init a singleAggFuncExec1NewVersion, aggInfo: %s", info))
}

func newSingleAggFuncExec2NewVersion(
	mg AggMemoryManager, info singleAggInfo, impl aggImplementation) AggFuncExec {
	switch info.argType.Oid {
	case types.T_bool:
		e := &singleAggFuncExecNew2[bool]{}
		e.init(mg, info, impl)
		return e

	case types.T_bit:
		e := &singleAggFuncExecNew2[uint64]{}
		e.init(mg, info, impl)
		return e

	case types.T_int8:
		e := &singleAggFuncExecNew2[int8]{}
		e.init(mg, info, impl)
		return e

	case types.T_int16:
		e := &singleAggFuncExecNew2[int16]{}
		e.init(mg, info, impl)
		return e

	case types.T_int32:
		e := &singleAggFuncExecNew2[int32]{}
		e.init(mg, info, impl)
		return e

	case types.T_int64:
		e := &singleAggFuncExecNew2[int64]{}
		e.init(mg, info, impl)
		return e

	case types.T_uint8:
		e := &singleAggFuncExecNew2[uint8]{}
		e.init(mg, info, impl)
		return e

	case types.T_uint16:
		e := &singleAggFuncExecNew2[uint16]{}
		e.init(mg, info, impl)
		return e

	case types.T_uint32:
		e := &singleAggFuncExecNew2[uint32]{}
		e.init(mg, info, impl)
		return e

	case types.T_uint64:
		e := &singleAggFuncExecNew2[uint64]{}
		e.init(mg, info, impl)
		return e

	case types.T_float32:
		e := &singleAggFuncExecNew2[float32]{}
		e.init(mg, info, impl)
		return e

	case types.T_float64:
		e := &singleAggFuncExecNew2[float64]{}
		e.init(mg, info, impl)
		return e

	case types.T_decimal64:
		e := &singleAggFuncExecNew2[types.Decimal64]{}
		e.init(mg, info, impl)
		return e

	case types.T_decimal128:
		e := &singleAggFuncExecNew2[types.Decimal128]{}
		e.init(mg, info, impl)
		return e

	case types.T_date:
		e := &singleAggFuncExecNew2[types.Date]{}
		e.init(mg, info, impl)
		return e

	case types.T_datetime:
		e := &singleAggFuncExecNew2[types.Datetime]{}
		e.init(mg, info, impl)
		return e

	case types.T_time:
		e := &singleAggFuncExecNew2[types.Time]{}
		e.init(mg, info, impl)
		return e

	case types.T_timestamp:
		e := &singleAggFuncExecNew2[types.Timestamp]{}
		e.init(mg, info, impl)
		return e

	case types.T_TS:
		e := &singleAggFuncExecNew2[types.TS]{}
		e.init(mg, info, impl)
		return e

	case types.T_Rowid:
		e := &singleAggFuncExecNew2[types.Rowid]{}
		e.init(mg, info, impl)
		return e

	case types.T_Blockid:
		e := &singleAggFuncExecNew2[types.Blockid]{}
		e.init(mg, info, impl)
		return e

	case types.T_uuid:
		e := &singleAggFuncExecNew2[types.Uuid]{}
		e.init(mg, info, impl)
		return e
	}
	panic(fmt.Sprintf("unsupported parameter type %s for singleAggFuncExec2", info.argType))
}

func newSingleAggFuncExec3NewVersion(
	mg AggMemoryManager, info singleAggInfo, impl aggImplementation) AggFuncExec {
	switch info.retType.Oid {
	case types.T_bool:
		e := &singleAggFuncExecNew3[bool]{}
		e.init(mg, info, impl)
		return e
	case types.T_bit:
		e := &singleAggFuncExecNew3[uint64]{}
		e.init(mg, info, impl)
		return e
	case types.T_int8:
		e := &singleAggFuncExecNew3[int8]{}
		e.init(mg, info, impl)
		return e
	case types.T_int16:
		e := &singleAggFuncExecNew3[int16]{}
		e.init(mg, info, impl)
		return e
	case types.T_int32:
		e := &singleAggFuncExecNew3[int32]{}
		e.init(mg, info, impl)
		return e
	case types.T_int64:
		e := &singleAggFuncExecNew3[int64]{}
		e.init(mg, info, impl)
		return e
	case types.T_uint8:
		e := &singleAggFuncExecNew3[uint8]{}
		e.init(mg, info, impl)
		return e
	case types.T_uint16:
		e := &singleAggFuncExecNew3[uint16]{}
		e.init(mg, info, impl)
		return e
	case types.T_uint32:
		e := &singleAggFuncExecNew3[uint32]{}
		e.init(mg, info, impl)
		return e
	case types.T_uint64:
		e := &singleAggFuncExecNew3[uint64]{}
		e.init(mg, info, impl)
		return e
	case types.T_float32:
		e := &singleAggFuncExecNew3[float32]{}
		e.init(mg, info, impl)
		return e
	case types.T_float64:
		e := &singleAggFuncExecNew3[float64]{}
		e.init(mg, info, impl)
		return e
	case types.T_decimal64:
		e := &singleAggFuncExecNew3[types.Decimal64]{}
		e.init(mg, info, impl)
		return e
	case types.T_decimal128:
		e := &singleAggFuncExecNew3[types.Decimal128]{}
		e.init(mg, info, impl)
		return e
	case types.T_date:
		e := &singleAggFuncExecNew3[types.Date]{}
		e.init(mg, info, impl)
		return e
	case types.T_datetime:
		e := &singleAggFuncExecNew3[types.Datetime]{}
		e.init(mg, info, impl)
		return e
	case types.T_time:
		e := &singleAggFuncExecNew3[types.Time]{}
		e.init(mg, info, impl)
		return e
	case types.T_timestamp:
		e := &singleAggFuncExecNew3[types.Timestamp]{}
		e.init(mg, info, impl)
		return e
	case types.T_TS:
		e := &singleAggFuncExecNew3[types.TS]{}
		e.init(mg, info, impl)
		return e
	case types.T_Rowid:
		e := &singleAggFuncExecNew3[types.Rowid]{}
		e.init(mg, info, impl)
		return e
	case types.T_Blockid:
		e := &singleAggFuncExecNew3[types.Blockid]{}
		e.init(mg, info, impl)
		return e
	case types.T_uuid:
		e := &singleAggFuncExecNew3[types.Uuid]{}
		e.init(mg, info, impl)
		return e
	}
	panic(fmt.Sprintf("unsupported result type %s for singleAggFuncExec3", info.retType))
}

func newSingleAggFuncExec4NewVersion(
	mg AggMemoryManager, info singleAggInfo, impl aggImplementation) AggFuncExec {
	e := &singleAggFuncExecNew4{}
	e.init(mg, info, impl)
	return e
}
