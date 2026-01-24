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

package readutil

import (
	"bytes"
	"fmt"

	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/container/vector"
	"github.com/matrixorigin/matrixone/pkg/fileservice"
	"github.com/matrixorigin/matrixone/pkg/logutil"
	"github.com/matrixorigin/matrixone/pkg/objectio"
	"github.com/matrixorigin/matrixone/pkg/pb/plan"
	"github.com/matrixorigin/matrixone/pkg/pb/timestamp"
	"github.com/matrixorigin/matrixone/pkg/sql/plan/function"
	"github.com/matrixorigin/matrixone/pkg/vm/engine"
	"go.uber.org/zap"
)

type MemPKFilter struct {
	op      int
	packed  [][]byte
	inSet   map[string]struct{} // pre-built hashmap for IN filter
	isVec   bool
	isValid bool
	TS      types.TS

	exact struct {
		hit bool
	}

	filterHint engine.FilterHint
}

func (f *MemPKFilter) Valid() bool {
	return f.isValid
}

func (f *MemPKFilter) Op() int {
	return f.op
}

func (f *MemPKFilter) Keys() [][]byte {
	return f.packed
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

	// Currently only support single atomic filter in memory path.
	if len(basePKFilter.Disjuncts) > 0 {
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
		case types.T_varchar, types.T_char, types.T_binary:
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
		case types.T_uuid:
			lbVal = types.DecodeUuid(basePKFilter.LB)
			if len(basePKFilter.UB) > 0 {
				ubVal = types.DecodeUuid(basePKFilter.UB)
			}
		default:
			logutil.Warn("NewMemPKFilter skipped data type",
				zap.Int("expr op", basePKFilter.Op),
				zap.String("data type", basePKFilter.Oid.String()))
			return
			//panic(basePKFilter.Oid.String())
		}
	}

	switch basePKFilter.Op {
	case function.EQUAL, function.PREFIX_EQ:
		packed = append(packed, EncodePrimaryKey(lbVal, packer))
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
		packed = EncodePrimaryKeyVector(basePKFilter.Vec, packer)

		if basePKFilter.Op == function.PREFIX_IN {
			for x := range packed {
				packed[x] = packed[x][0 : len(packed[x])-1]
			}
		}
		filter.SetFullData(basePKFilter.Op, true, packed...)

	case function.LESS_THAN, function.LESS_EQUAL, function.GREAT_THAN, function.GREAT_EQUAL:
		packed = append(packed, EncodePrimaryKey(lbVal, packer))
		filter.SetFullData(basePKFilter.Op, false, packed...)

	case function.PREFIX_BETWEEN, function.BETWEEN,
		RangeLeftOpen, RangeRightOpen, RangeBothOpen:
		packed = append(packed, EncodePrimaryKey(lbVal, packer))
		packed = append(packed, EncodePrimaryKey(ubVal, packer))
		if basePKFilter.Op == function.PREFIX_BETWEEN {
			packed[0] = packed[0][0 : len(packed[0])-1]
			packed[1] = packed[1][0 : len(packed[1])-1]
		}
		filter.SetFullData(basePKFilter.Op, false, packed...)
	default:
		return
	}

	filter.filterHint = filterHint

	return
}

func (f *MemPKFilter) InKind() (int, bool) {
	return len(f.packed), f.op == function.IN || f.op == function.PREFIX_IN
}

func (f *MemPKFilter) Exact() (bool, bool) {
	return f.op == function.EQUAL && len(f.packed) == 1, f.exact.hit
}

func (f *MemPKFilter) RecordExactHit() bool {
	if ok, _ := f.Exact(); !ok {
		return false
	}

	f.exact.hit = true

	return true
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

func (f *MemPKFilter) FilterVector(
	vec *vector.Vector,
	packer *types.Packer,
	skipMask *objectio.Bitmap,
) {
	keys := EncodePrimaryKeyVector(vec, packer)

	// For IN filter, use hashmap for O(1) lookup
	if f.op == function.IN {
		// Lazy build hashmap on first use
		if f.inSet == nil && len(f.packed) > 0 {
			f.inSet = make(map[string]struct{}, len(f.packed))
			for _, k := range f.packed {
				f.inSet[string(k)] = struct{}{}
			}
		}
		for i := 0; i < len(keys); i++ {
			if _, ok := f.inSet[string(keys[i])]; !ok {
				skipMask.Add(uint64(i))
			}
		}
		return
	}

	// For PREFIX_IN with small list, use linear search
	if f.op == function.PREFIX_IN && len(f.packed) > 4 {
		return
	}

	for i := 0; i < len(keys); i++ {
		switch f.op {
		case function.EQUAL:
			if !bytes.Equal(keys[i], f.packed[0]) {
				skipMask.Add(uint64(i))
			}

		case function.PREFIX_EQ:
			if !bytes.HasPrefix(keys[i], f.packed[0]) {
				skipMask.Add(uint64(i))
			}

		case function.PREFIX_IN:
			in := false
			for _, k := range f.packed {
				if bytes.HasPrefix(keys[i], k) {
					in = true
					break
				}
			}
			if !in {
				skipMask.Add(uint64(i))
			}
		case function.BETWEEN:
			if !(bytes.Compare(keys[i], f.packed[0]) >= 0 && bytes.Compare(keys[i], f.packed[1]) <= 0) {
				skipMask.Add(uint64(i))
			}

		case RangeRightOpen:
			if !(bytes.Compare(keys[i], f.packed[0]) >= 0 && bytes.Compare(keys[i], f.packed[1]) < 0) {
				skipMask.Add(uint64(i))
			}

		case RangeLeftOpen:
			if !(bytes.Compare(keys[i], f.packed[0]) > 0 && bytes.Compare(keys[i], f.packed[1]) <= 0) {
				skipMask.Add(uint64(i))
			}

		case RangeBothOpen:
			if !(bytes.Compare(keys[i], f.packed[0]) > 0 && bytes.Compare(keys[i], f.packed[1]) < 0) {
				skipMask.Add(uint64(i))
			}

		case function.GREAT_EQUAL:
			if !(bytes.Compare(keys[i], f.packed[0]) >= 0) {
				skipMask.Add(uint64(i))
			}

		case function.GREAT_THAN:
			if !(bytes.Compare(keys[i], f.packed[0]) > 0) {
				skipMask.Add(uint64(i))
			}

		case function.LESS_EQUAL:
			if !(bytes.Compare(keys[i], f.packed[0]) <= 0) {
				skipMask.Add(uint64(i))
			}

		case function.LESS_THAN:
			if !(bytes.Compare(keys[i], f.packed[0]) < 0) {
				skipMask.Add(uint64(i))
			}

		default:
			// skip nothing
		}
	}
}
