// Copyright 2023 Matrix Origin
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//	http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.
package fuzzyfilter

import (
	"github.com/bits-and-blooms/bloom"
	"github.com/matrixorigin/matrixone/pkg/container/batch"
	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/container/vector"
	"github.com/matrixorigin/matrixone/pkg/vm/process"
)

type container struct {
	filter *bloom.BloomFilter
    collisionCnt int
	isCpk bool
	// refer func generateRbat
	rbat *batch.Batch
}

type Argument struct {
	ctr *container

	TblName string
	DbName  string
}

func (arg *Argument) Free(proc *process.Process, pipelineFailed bool) {
	ctr := arg.ctr
	if ctr != nil {
		ctr.filter = nil
		if ctr.rbat != nil {
			ctr.rbat.Clean(proc.GetMPool())
		}
	}
	ctr = nil
}

func getNonNullValue(col *vector.Vector, row uint32) any {

	switch col.GetType().Oid {
	case types.T_bool:
		return vector.GetFixedAt[bool](col, int(row))
	case types.T_int8:
		return vector.GetFixedAt[int8](col, int(row))
	case types.T_int16:
		return vector.GetFixedAt[int16](col, int(row))
	case types.T_int32:
		return vector.GetFixedAt[int32](col, int(row))
	case types.T_int64:
		return vector.GetFixedAt[int64](col, int(row))
	case types.T_uint8:
		return vector.GetFixedAt[uint8](col, int(row))
	case types.T_uint16:
		return vector.GetFixedAt[uint16](col, int(row))
	case types.T_uint32:
		return vector.GetFixedAt[uint32](col, int(row))
	case types.T_uint64:
		return vector.GetFixedAt[uint64](col, int(row))
	case types.T_decimal64:
		return vector.GetFixedAt[types.Decimal64](col, int(row))
	case types.T_decimal128:
		return vector.GetFixedAt[types.Decimal128](col, int(row))
	case types.T_uuid:
		return vector.GetFixedAt[types.Uuid](col, int(row))
	case types.T_float32:
		return vector.GetFixedAt[float32](col, int(row))
	case types.T_float64:
		return vector.GetFixedAt[float64](col, int(row))
	case types.T_date:
		return vector.GetFixedAt[types.Date](col, int(row))
	case types.T_time:
		return vector.GetFixedAt[types.Time](col, int(row))
	case types.T_datetime:
		return vector.GetFixedAt[types.Datetime](col, int(row))
	case types.T_timestamp:
		return vector.GetFixedAt[types.Timestamp](col, int(row))
	case types.T_enum:
		return vector.GetFixedAt[types.Enum](col, int(row))
	case types.T_TS:
		return vector.GetFixedAt[types.TS](col, int(row))
	case types.T_Rowid:
		return vector.GetFixedAt[types.Rowid](col, int(row))
	case types.T_Blockid:
		return vector.GetFixedAt[types.Blockid](col, int(row))
	case types.T_char, types.T_varchar, types.T_binary, types.T_varbinary, types.T_json, types.T_blob, types.T_text:
		return col.GetBytesAt(int(row))
	default:
		//return vector.ErrVecTypeNotSupport
		panic(any("No Support"))
	}
}
