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

package disttae

import (
	"bytes"
	"fmt"

	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/fileservice"
	"github.com/matrixorigin/matrixone/pkg/pb/plan"
	"github.com/matrixorigin/matrixone/pkg/pb/timestamp"
	"github.com/matrixorigin/matrixone/pkg/sql/plan/function"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/disttae/logtailreplay"
)

type MemPKFilter struct {
	op      int
	packed  [][]byte
	isVec   bool
	isValid bool
	TS      types.TS

	SpecFactory func(f *MemPKFilter) logtailreplay.PrimaryKeyMatchSpec
}

func newMemPKFilter(
	tableDef *plan.TableDef,
	ts timestamp.Timestamp,
	packerPool *fileservice.Pool[*types.Packer],
	basePKFilter basePKFilter,
) (filter MemPKFilter, err error) {
	//defer func() {
	//	if filter.iter == nil {
	//		filter.isValid = true
	//		filter.iter = state.NewRowsIter(
	//			types.TimestampToTS(ts),
	//			nil,
	//			false,
	//		)
	//	}
	//}()

	filter.TS = types.TimestampToTS(ts)

	if !basePKFilter.valid || tableDef.Pkey == nil {
		return
	}

	var lbVal, ubVal any
	var packed [][]byte
	var packer *types.Packer
	put := packerPool.Get(&packer)
	defer put.Put()

	if basePKFilter.op != function.IN && basePKFilter.op != function.PREFIX_IN {
		switch basePKFilter.oid {
		case types.T_int8:
			lbVal = types.DecodeInt8(basePKFilter.lb)
			if len(basePKFilter.ub) > 0 {
				ubVal = types.DecodeInt8(basePKFilter.ub)
			}
		case types.T_int16:
			lbVal = types.DecodeInt16(basePKFilter.lb)
			if len(basePKFilter.ub) > 0 {
				ubVal = types.DecodeInt16(basePKFilter.ub)
			}
		case types.T_int32:
			lbVal = types.DecodeInt32(basePKFilter.lb)
			if len(basePKFilter.ub) > 0 {
				ubVal = types.DecodeInt32(basePKFilter.ub)
			}
		case types.T_int64:
			lbVal = types.DecodeInt64(basePKFilter.lb)
			if len(basePKFilter.ub) > 0 {
				ubVal = types.DecodeInt64(basePKFilter.ub)
			}
		case types.T_float32:
			lbVal = types.DecodeFloat32(basePKFilter.lb)
			if len(basePKFilter.ub) > 0 {
				ubVal = types.DecodeFloat32(basePKFilter.ub)
			}
		case types.T_float64:
			lbVal = types.DecodeFloat64(basePKFilter.lb)
			if len(basePKFilter.ub) > 0 {
				ubVal = types.DecodeFloat64(basePKFilter.ub)
			}
		case types.T_uint8:
			lbVal = types.DecodeUint8(basePKFilter.lb)
			if len(basePKFilter.ub) > 0 {
				ubVal = types.DecodeUint8(basePKFilter.ub)
			}
		case types.T_uint16:
			lbVal = types.DecodeUint16(basePKFilter.lb)
			if len(basePKFilter.ub) > 0 {
				ubVal = types.DecodeUint16(basePKFilter.ub)
			}
		case types.T_uint32:
			lbVal = types.DecodeUint32(basePKFilter.lb)
			if len(basePKFilter.ub) > 0 {
				ubVal = types.DecodeUint32(basePKFilter.ub)
			}
		case types.T_uint64:
			lbVal = types.DecodeUint64(basePKFilter.lb)
			if len(basePKFilter.ub) > 0 {
				ubVal = types.DecodeUint64(basePKFilter.ub)
			}
		case types.T_date:
			lbVal = types.DecodeDate(basePKFilter.lb)
			if len(basePKFilter.ub) > 0 {
				ubVal = types.DecodeDate(basePKFilter.ub)
			}
		case types.T_time:
			lbVal = types.DecodeTime(basePKFilter.lb)
			if len(basePKFilter.ub) > 0 {
				ubVal = types.DecodeTime(basePKFilter.ub)
			}
		case types.T_datetime:
			lbVal = types.DecodeDatetime(basePKFilter.lb)
			if len(basePKFilter.ub) > 0 {
				ubVal = types.DecodeDatetime(basePKFilter.ub)
			}
		case types.T_timestamp:
			lbVal = types.DecodeTimestamp(basePKFilter.lb)
			if len(basePKFilter.ub) > 0 {
				ubVal = types.DecodeTimestamp(basePKFilter.ub)
			}
		case types.T_decimal64:
			lbVal = types.DecodeDecimal64(basePKFilter.lb)
			if len(basePKFilter.ub) > 0 {
				ubVal = types.DecodeDecimal64(basePKFilter.ub)
			}
		case types.T_decimal128:
			lbVal = types.DecodeDecimal128(basePKFilter.lb)
			if len(basePKFilter.ub) > 0 {
				ubVal = types.DecodeDecimal128(basePKFilter.ub)
			}
		case types.T_varchar, types.T_char:
			lbVal = basePKFilter.lb
			ubVal = basePKFilter.ub
		case types.T_json:
			lbVal = types.DecodeJson(basePKFilter.lb)
			if len(basePKFilter.ub) > 0 {
				ubVal = types.DecodeJson(basePKFilter.ub)
			}
		case types.T_enum:
			lbVal = types.DecodeEnum(basePKFilter.lb)
			if len(basePKFilter.ub) > 0 {
				ubVal = types.DecodeEnum(basePKFilter.ub)
			}
		default:
			return
			//panic(basePKFilter.oid.String())
		}
	}

	switch basePKFilter.op {
	case function.EQUAL, function.PREFIX_EQ:
		packed = append(packed, logtailreplay.EncodePrimaryKey(lbVal, packer))
		if basePKFilter.op == function.PREFIX_EQ {
			// TODO Remove this later
			// serial_full(secondary_index, primary_key|fake_pk) => varchar
			// prefix_eq expression only has the prefix(secondary index) in it.
			// there will have an extra zero after the `encodeStringType` done
			// this will violate the rule of prefix_eq, so remove this redundant zero here.
			//
			packed[0] = packed[0][0 : len(packed[0])-1]
		}
		filter.SetFullData(basePKFilter.op, false, packed...)

	case function.IN, function.PREFIX_IN:
		packed = logtailreplay.EncodePrimaryKeyVector(basePKFilter.vec, packer)

		if basePKFilter.op == function.PREFIX_IN {
			for x := range packed {
				packed[x] = packed[x][0 : len(packed[x])-1]
			}
		}
		filter.SetFullData(basePKFilter.op, true, packed...)

	case function.LESS_THAN, function.LESS_EQUAL, function.GREAT_THAN, function.GREAT_EQUAL:
		packed = append(packed, logtailreplay.EncodePrimaryKey(lbVal, packer))
		filter.SetFullData(basePKFilter.op, false, packed...)

	case function.PREFIX_BETWEEN, function.BETWEEN, rangeLeftOpen, rangeRightOpen, rangeBothOpen:
		packed = append(packed, logtailreplay.EncodePrimaryKey(lbVal, packer))
		packed = append(packed, logtailreplay.EncodePrimaryKey(ubVal, packer))
		if basePKFilter.op == function.PREFIX_BETWEEN {
			packed[0] = packed[0][0 : len(packed[0])-1]
			packed[1] = packed[1][0 : len(packed[1])-1]
		}
		filter.SetFullData(basePKFilter.op, false, packed...)
	default:
		return
	}

	filter.tryConstructPrimaryKeyIndexIter(ts)
	return
}

func (f *MemPKFilter) String() string {
	var buf bytes.Buffer
	buf.WriteString(
		fmt.Sprintf("InMemPKFilter{op: %d, isVec: %v, isValid: %v, val: %v, data(len=%d)",
			f.op, f.isVec, f.isValid, f.packed, len(f.packed),
		))
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

func (f *MemPKFilter) tryConstructPrimaryKeyIndexIter(ts timestamp.Timestamp) {
	if !f.isValid {
		return
	}

	switch f.op {
	case function.EQUAL, function.PREFIX_EQ:
		//spec = logtailreplay.Prefix(f.packed[0])
		f.SpecFactory = func(f *MemPKFilter) logtailreplay.PrimaryKeyMatchSpec {
			return logtailreplay.Prefix(f.packed[0])
		}

	case function.IN, function.PREFIX_IN:
		// // may be it's better to iterate rows instead.
		// if len(f.packed) > 128 {
		// 	return
		// }
		//spec = logtailreplay.InKind(f.packed, f.op)
		f.SpecFactory = func(f *MemPKFilter) logtailreplay.PrimaryKeyMatchSpec {
			return logtailreplay.InKind(f.packed, f.op)
		}

	case function.LESS_EQUAL, function.LESS_THAN:
		//spec = logtailreplay.LessKind(f.packed[0], f.op == function.LESS_EQUAL)
		f.SpecFactory = func(f *MemPKFilter) logtailreplay.PrimaryKeyMatchSpec {
			return logtailreplay.LessKind(f.packed[0], f.op == function.LESS_EQUAL)
		}

	case function.GREAT_EQUAL, function.GREAT_THAN:
		//spec = logtailreplay.GreatKind(f.packed[0], f.op == function.GREAT_EQUAL)
		f.SpecFactory = func(f *MemPKFilter) logtailreplay.PrimaryKeyMatchSpec {
			return logtailreplay.GreatKind(f.packed[0], f.op == function.GREAT_EQUAL)
		}

	case function.BETWEEN, rangeLeftOpen, rangeRightOpen, rangeBothOpen, function.PREFIX_BETWEEN:
		var kind int
		switch f.op {
		case function.BETWEEN:
			kind = 0
		case rangeLeftOpen:
			kind = 1
		case rangeRightOpen:
			kind = 2
		case rangeBothOpen:
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
