// Copyright 2021-2024 Matrix Origin
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

package engine_util

import (
	"bytes"
	"fmt"

	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/fileservice"
	"github.com/matrixorigin/matrixone/pkg/pb/plan"
	"github.com/matrixorigin/matrixone/pkg/pb/timestamp"
	"github.com/matrixorigin/matrixone/pkg/sql/plan/function"
	"github.com/matrixorigin/matrixone/pkg/vm/engine"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/disttae/logtailreplay"
)

type MemPKFilter struct {
	op      int
	packed  [][]byte
	isVec   bool
	isValid bool
	TS      types.TS

	filterHint  engine.FilterHint
	SpecFactory func(f *MemPKFilter) logtailreplay.PrimaryKeyMatchSpec
}

func NewMemPKFilter(
	tableDef *plan.TableDef,
	ts timestamp.Timestamp,
	packerPool *fileservice.Pool[*types.Packer],
	basePKFilter BasePKFilter,
	filterHint engine.FilterHint,
) (filter MemPKFilter, err error) {

	filter.TS = types.TimestampToTS(ts)

	if !basePKFilter.Valid || tableDef.Pkey == nil {
		return
	}

	var lbVal, ubVal any
	var packed [][]byte
	var packer *types.Packer
	put := packerPool.Get(&packer)
	defer put.Put()

	if basePKFilter.Op != function.IN && basePKFilter.Op != function.PREFIX_IN {
		switch basePKFilter.Oid {
		case types.T_int8:
			lbVal = types.DecodeInt8(basePKFilter.LB)
			if len(basePKFilter.UB) > 0 {
				ubVal = types.DecodeInt8(basePKFilter.UB)
			}
		case types.T_int16:
			lbVal = types.DecodeInt16(basePKFilter.LB)
			if len(basePKFilter.UB) > 0 {
				ubVal = types.DecodeInt16(basePKFilter.UB)
			}
		case types.T_int32:
			lbVal = types.DecodeInt32(basePKFilter.LB)
			if len(basePKFilter.UB) > 0 {
				ubVal = types.DecodeInt32(basePKFilter.UB)
			}
		case types.T_int64:
			lbVal = types.DecodeInt64(basePKFilter.LB)
			if len(basePKFilter.UB) > 0 {
				ubVal = types.DecodeInt64(basePKFilter.UB)
			}
		case types.T_float32:
			lbVal = types.DecodeFloat32(basePKFilter.LB)
			if len(basePKFilter.UB) > 0 {
				ubVal = types.DecodeFloat32(basePKFilter.UB)
			}
		case types.T_float64:
			lbVal = types.DecodeFloat64(basePKFilter.LB)
			if len(basePKFilter.UB) > 0 {
				ubVal = types.DecodeFloat64(basePKFilter.UB)
			}
		case types.T_uint8:
			lbVal = types.DecodeUint8(basePKFilter.LB)
			if len(basePKFilter.UB) > 0 {
				ubVal = types.DecodeUint8(basePKFilter.UB)
			}
		case types.T_uint16:
			lbVal = types.DecodeUint16(basePKFilter.LB)
			if len(basePKFilter.UB) > 0 {
				ubVal = types.DecodeUint16(basePKFilter.UB)
			}
		case types.T_uint32:
			lbVal = types.DecodeUint32(basePKFilter.LB)
			if len(basePKFilter.UB) > 0 {
				ubVal = types.DecodeUint32(basePKFilter.UB)
			}
		case types.T_uint64:
			lbVal = types.DecodeUint64(basePKFilter.LB)
			if len(basePKFilter.UB) > 0 {
				ubVal = types.DecodeUint64(basePKFilter.UB)
			}
		case types.T_date:
			lbVal = types.DecodeDate(basePKFilter.LB)
			if len(basePKFilter.UB) > 0 {
				ubVal = types.DecodeDate(basePKFilter.UB)
			}
		case types.T_time:
			lbVal = types.DecodeTime(basePKFilter.LB)
			if len(basePKFilter.UB) > 0 {
				ubVal = types.DecodeTime(basePKFilter.UB)
			}
		case types.T_datetime:
			lbVal = types.DecodeDatetime(basePKFilter.LB)
			if len(basePKFilter.UB) > 0 {
				ubVal = types.DecodeDatetime(basePKFilter.UB)
			}
		case types.T_timestamp:
			lbVal = types.DecodeTimestamp(basePKFilter.LB)
			if len(basePKFilter.UB) > 0 {
				ubVal = types.DecodeTimestamp(basePKFilter.UB)
			}
		case types.T_decimal64:
			lbVal = types.DecodeDecimal64(basePKFilter.LB)
			if len(basePKFilter.UB) > 0 {
				ubVal = types.DecodeDecimal64(basePKFilter.UB)
			}
		case types.T_decimal128:
			lbVal = types.DecodeDecimal128(basePKFilter.LB)
			if len(basePKFilter.UB) > 0 {
				ubVal = types.DecodeDecimal128(basePKFilter.UB)
			}
		case types.T_varchar, types.T_char:
			lbVal = basePKFilter.LB
			ubVal = basePKFilter.UB
		case types.T_json:
			lbVal = types.DecodeJson(basePKFilter.LB)
			if len(basePKFilter.UB) > 0 {
				ubVal = types.DecodeJson(basePKFilter.UB)
			}
		case types.T_enum:
			lbVal = types.DecodeEnum(basePKFilter.LB)
			if len(basePKFilter.UB) > 0 {
				ubVal = types.DecodeEnum(basePKFilter.UB)
			}
		default:
			return
			//panic(basePKFilter.Oid.String())
		}
	}

	switch basePKFilter.Op {
	case function.EQUAL, function.PREFIX_EQ:
		packed = append(packed, logtailreplay.EncodePrimaryKey(lbVal, packer))
		if basePKFilter.Op == function.PREFIX_EQ {
			// TODO Remove this later
			// serial_full(secondary_index, primary_key|fake_pk) => varchar
			// prefix_eq expression only has the prefix(secondary index) in it.
			// there will have an extra zero after the `encodeStringType` done
			// this will violate the rule of prefix_eq, so remove this redundant zero here.
			//
			packed[0] = packed[0][0 : len(packed[0])-1]
		}
		filter.SetFullData(basePKFilter.Op, false, packed...)

	case function.IN, function.PREFIX_IN:
		packed = logtailreplay.EncodePrimaryKeyVector(basePKFilter.Vec, packer)

		if basePKFilter.Op == function.PREFIX_IN {
			for x := range packed {
				packed[x] = packed[x][0 : len(packed[x])-1]
			}
		}
		filter.SetFullData(basePKFilter.Op, true, packed...)

	case function.LESS_THAN, function.LESS_EQUAL, function.GREAT_THAN, function.GREAT_EQUAL:
		packed = append(packed, logtailreplay.EncodePrimaryKey(lbVal, packer))
		filter.SetFullData(basePKFilter.Op, false, packed...)

	case function.PREFIX_BETWEEN, function.BETWEEN,
		RangeLeftOpen, RangeRightOpen, RangeBothOpen:
		packed = append(packed, logtailreplay.EncodePrimaryKey(lbVal, packer))
		packed = append(packed, logtailreplay.EncodePrimaryKey(ubVal, packer))
		if basePKFilter.Op == function.PREFIX_BETWEEN {
			packed[0] = packed[0][0 : len(packed[0])-1]
			packed[1] = packed[1][0 : len(packed[1])-1]
		}
		filter.SetFullData(basePKFilter.Op, false, packed...)
	default:
		return
	}

	filter.tryConstructPrimaryKeyIndexIter(ts, tableDef.Name)

	filter.filterHint = filterHint

	return
}

func (f *MemPKFilter) InKind() (int, bool) {
	return len(f.packed), f.op == function.IN || f.op == function.PREFIX_IN
}

func (f *MemPKFilter) Must() bool {
	return f.filterHint.Must
}

func (f *MemPKFilter) String() string {
	var buf bytes.Buffer
	buf.WriteString(fmt.Sprintf("InMemPKFilter{op: %d, isVec: %v, isValid: %v vals: [", f.op, f.isVec, f.isValid))
	for x := range f.packed {
		buf.WriteString(fmt.Sprintf("%x, ", f.packed[x]))
	}
	buf.WriteString(fmt.Sprintf("][%d]}", len(f.packed)))
	return buf.String()
}

func (f *MemPKFilter) SetNull() {
	f.isValid = false
}

func (f *MemPKFilter) SetFullData(op int, isVec bool, val ...[]byte) {
	f.packed = append(f.packed, val...)
	f.op = op
	f.isVec = isVec
	f.isValid = true
}

func (f *MemPKFilter) tryConstructPrimaryKeyIndexIter(
	ts timestamp.Timestamp,
	tableName string,
) {
	if !f.isValid {
		return
	}

	switch f.op {
	case function.EQUAL:
		f.SpecFactory = func(f *MemPKFilter) logtailreplay.PrimaryKeyMatchSpec {
			return logtailreplay.Exact(f.packed[0])
		}

	case function.PREFIX_EQ:
		//spec = logtailreplay.Prefix(f.packed[0])
		f.SpecFactory = func(f *MemPKFilter) logtailreplay.PrimaryKeyMatchSpec {
			return logtailreplay.Prefix(f.packed[0])
		}

	case function.IN, function.PREFIX_IN:
		// // may be it's better to iterate rows instead.
		// if len(f.packed) > 128 {
		// 	return
		// }
		//spec = logtailreplay.InKind(f.packed, f.Op)
		f.SpecFactory = func(f *MemPKFilter) logtailreplay.PrimaryKeyMatchSpec {
			return logtailreplay.InKind(f.packed, f.op)
		}

	case function.LESS_EQUAL, function.LESS_THAN:
		//spec = logtailreplay.LessKind(f.packed[0], f.Op == function.LESS_EQUAL)
		f.SpecFactory = func(f *MemPKFilter) logtailreplay.PrimaryKeyMatchSpec {
			return logtailreplay.LessKind(f.packed[0], f.op == function.LESS_EQUAL)
		}

	case function.GREAT_EQUAL, function.GREAT_THAN:
		//spec = logtailreplay.GreatKind(f.packed[0], f.Op == function.GREAT_EQUAL)
		f.SpecFactory = func(f *MemPKFilter) logtailreplay.PrimaryKeyMatchSpec {
			return logtailreplay.GreatKind(f.packed[0], f.op == function.GREAT_EQUAL)
		}

	case function.BETWEEN, RangeLeftOpen, RangeRightOpen, RangeBothOpen, function.PREFIX_BETWEEN:
		var kind int
		switch f.op {
		case function.BETWEEN:
			kind = 0
		case RangeLeftOpen:
			kind = 1
		case RangeRightOpen:
			kind = 2
		case RangeBothOpen:
			kind = 3
		case function.PREFIX_BETWEEN:
			kind = 4
		}

		//spec = logtailreplay.BetweenKind(f.packed[0], f.packed[1], kind)
		f.SpecFactory = func(f *MemPKFilter) logtailreplay.PrimaryKeyMatchSpec {
			return logtailreplay.BetweenKind(f.packed[0], f.packed[1], kind)
		}
	}

}
