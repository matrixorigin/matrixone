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
	"sort"

	"github.com/matrixorigin/matrixone/pkg/catalog"
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

// MemPKFilterSpec describes one atomic primary-key predicate. Multiple specs
// are OR-ed by partition-state iterators.
type MemPKFilterSpec struct {
	Op   int
	Keys [][]byte
}

type MemPKFilter struct {
	op        int
	packed    [][]byte
	inSet     map[string]struct{}
	prefixes  [][]byte
	disjuncts []MemPKFilter
	isVec     bool
	isValid   bool
	TS        types.TS

	exact struct {
		hit bool
	}

	FilterHint engine.FilterHint
	HasBF      bool
	BFSeqNum   int16
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

func (f *MemPKFilter) Specs() []MemPKFilterSpec {
	if !f.isValid {
		return nil
	}
	if len(f.disjuncts) == 0 {
		return []MemPKFilterSpec{{Op: f.op, Keys: f.packed}}
	}

	specs := make([]MemPKFilterSpec, 0, len(f.disjuncts))
	for idx := range f.disjuncts {
		specs = append(specs, f.disjuncts[idx].Specs()...)
	}
	return specs
}

func NewMemPKFilter(
	tableDef *plan.TableDef,
	ts timestamp.Timestamp,
	packerPool *fileservice.Pool[*types.Packer],
	basePKFilter BasePKFilter,
	filterHint engine.FilterHint,
) (filter MemPKFilter, err error) {

	filter.TS = types.TimestampToTS(ts)

	if !basePKFilter.Valid || tableDef == nil || tableDef.Pkey == nil || packerPool == nil {
		return
	}

	if len(basePKFilter.Disjuncts) > 0 {
		filter.disjuncts = make([]MemPKFilter, 0, len(basePKFilter.Disjuncts))
		for idx := range basePKFilter.Disjuncts {
			disjunct, err := NewMemPKFilter(
				tableDef,
				ts,
				packerPool,
				basePKFilter.Disjuncts[idx],
				engine.FilterHint{},
			)
			if err != nil {
				return MemPKFilter{}, err
			}
			if !disjunct.Valid() {
				return MemPKFilter{TS: filter.TS}, nil
			}
			filter.disjuncts = append(filter.disjuncts, disjunct)
		}
		filter.isValid = len(filter.disjuncts) > 0
		filter.isVec = true
		filter.setFilterHint(tableDef, filterHint)
		return
	}
	if !validBlockPKSearchFilter(basePKFilter) {
		return
	}

	var lbVal, ubVal any
	var packed [][]byte
	var packer *types.Packer
	put := packerPool.Get(&packer)
	defer put.Put()

	if basePKFilter.Op != function.IN && basePKFilter.Op != function.PREFIX_IN {
		switch basePKFilter.Oid {
		case types.T_bool:
			lbVal = types.DecodeBool(basePKFilter.LB)
			if len(basePKFilter.UB) > 0 {
				ubVal = types.DecodeBool(basePKFilter.UB)
			}
		case types.T_bit:
			lbVal = types.DecodeUint64(basePKFilter.LB)
			if len(basePKFilter.UB) > 0 {
				ubVal = types.DecodeUint64(basePKFilter.UB)
			}
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
		case types.T_year:
			lbVal = types.DecodeMoYear(basePKFilter.LB)
			if len(basePKFilter.UB) > 0 {
				ubVal = types.DecodeMoYear(basePKFilter.UB)
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
		case types.T_decimal256:
			lbVal = types.DecodeDecimal256(basePKFilter.LB)
			if len(basePKFilter.UB) > 0 {
				ubVal = types.DecodeDecimal256(basePKFilter.UB)
			}
		case types.T_varchar, types.T_char, types.T_binary, types.T_varbinary:
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
		RangeLeftOpen, RangeRightOpen, RangeBothOpen,
		PrefixRangeLeftOpen, PrefixRangeRightOpen, PrefixRangeBothOpen:
		packed = append(packed, EncodePrimaryKey(lbVal, packer))
		packed = append(packed, EncodePrimaryKey(ubVal, packer))
		if basePKFilter.Op == function.PREFIX_BETWEEN ||
			basePKFilter.Op == PrefixRangeLeftOpen ||
			basePKFilter.Op == PrefixRangeRightOpen ||
			basePKFilter.Op == PrefixRangeBothOpen {
			packed[0] = packed[0][0 : len(packed[0])-1]
			packed[1] = packed[1][0 : len(packed[1])-1]
		}
		filter.SetFullData(basePKFilter.Op, false, packed...)
	default:
		return
	}

	filter.setFilterHint(tableDef, filterHint)

	return
}

func (f *MemPKFilter) setFilterHint(tableDef *plan.TableDef, filterHint engine.FilterHint) {
	f.FilterHint = filterHint
	if f.FilterHint.BF != nil && f.FilterHint.BF.Valid() {
		f.HasBF = true
		f.BFSeqNum = -1
		// For IVF entries table, use __mo_index_pri_col for BF filtering.
		for _, col := range tableDef.Cols {
			if col.Name == catalog.IndexTablePrimaryColName {
				f.BFSeqNum = int16(col.Seqnum)
				break
			}
		}
		// For fulltext index table, use doc_id column for BF filtering.
		if f.BFSeqNum == -1 && catalog.IsFullTextIndexTableType(tableDef.TableType, tableDef.Name) {
			for _, col := range tableDef.Cols {
				if col.Name == catalog.FullTextIndex_TabCol_Id {
					f.BFSeqNum = int16(col.Seqnum)
					break
				}
			}
		}
	}
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
	return f.FilterHint.Must
}

func (f *MemPKFilter) String() string {
	var buf bytes.Buffer
	if len(f.disjuncts) > 0 {
		buf.WriteString(fmt.Sprintf("InMemPKFilter{disjuncts: %d, isValid: %v}", len(f.disjuncts), f.isValid))
		return buf.String()
	}
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
	f.packed = append(f.packed[:0], val...)
	f.op = op
	f.isVec = isVec
	f.isValid = true
	f.inSet = nil
	f.prefixes = nil
	f.disjuncts = nil

	if op == function.IN || op == function.PREFIX_IN {
		sort.Slice(f.packed, func(i, j int) bool {
			return bytes.Compare(f.packed[i], f.packed[j]) < 0
		})
	}
	if op == function.IN {
		f.inSet = make(map[string]struct{}, len(f.packed))
		for _, key := range f.packed {
			f.inSet[string(key)] = struct{}{}
		}
	}
	if op == function.PREFIX_IN {
		f.prefixes = make([][]byte, 0, len(f.packed))
		for _, prefix := range f.packed {
			if len(f.prefixes) > 0 && bytes.HasPrefix(prefix, f.prefixes[len(f.prefixes)-1]) {
				continue
			}
			f.prefixes = append(f.prefixes, prefix)
		}
	}
}

func (f *MemPKFilter) matches(key []byte) bool {
	if len(f.disjuncts) > 0 {
		for idx := range f.disjuncts {
			if f.disjuncts[idx].matches(key) {
				return true
			}
		}
		return false
	}

	switch f.op {
	case function.EQUAL:
		return len(f.packed) == 1 && bytes.Equal(key, f.packed[0])
	case function.PREFIX_EQ:
		return len(f.packed) == 1 && bytes.HasPrefix(key, f.packed[0])
	case function.IN:
		_, ok := f.inSet[string(key)]
		return ok
	case function.PREFIX_IN:
		idx := sort.Search(len(f.prefixes), func(idx int) bool {
			return bytes.Compare(f.prefixes[idx], key) > 0
		}) - 1
		return idx >= 0 && bytes.HasPrefix(key, f.prefixes[idx])
	case function.BETWEEN:
		return len(f.packed) == 2 && bytes.Compare(key, f.packed[0]) >= 0 && bytes.Compare(key, f.packed[1]) <= 0
	case RangeRightOpen:
		return len(f.packed) == 2 && bytes.Compare(key, f.packed[0]) >= 0 && bytes.Compare(key, f.packed[1]) < 0
	case RangeLeftOpen:
		return len(f.packed) == 2 && bytes.Compare(key, f.packed[0]) > 0 && bytes.Compare(key, f.packed[1]) <= 0
	case RangeBothOpen:
		return len(f.packed) == 2 && bytes.Compare(key, f.packed[0]) > 0 && bytes.Compare(key, f.packed[1]) < 0
	case function.PREFIX_BETWEEN:
		return len(f.packed) == 2 && types.PrefixCompare(key, f.packed[0]) >= 0 && types.PrefixCompare(key, f.packed[1]) <= 0
	case PrefixRangeLeftOpen:
		return len(f.packed) == 2 && types.PrefixCompare(key, f.packed[0]) > 0 && types.PrefixCompare(key, f.packed[1]) <= 0
	case PrefixRangeRightOpen:
		return len(f.packed) == 2 && types.PrefixCompare(key, f.packed[0]) >= 0 && types.PrefixCompare(key, f.packed[1]) < 0
	case PrefixRangeBothOpen:
		return len(f.packed) == 2 && types.PrefixCompare(key, f.packed[0]) > 0 && types.PrefixCompare(key, f.packed[1]) < 0
	case function.GREAT_EQUAL:
		return len(f.packed) == 1 && bytes.Compare(key, f.packed[0]) >= 0
	case function.GREAT_THAN:
		return len(f.packed) == 1 && bytes.Compare(key, f.packed[0]) > 0
	case function.LESS_EQUAL:
		return len(f.packed) == 1 && bytes.Compare(key, f.packed[0]) <= 0
	case function.LESS_THAN:
		return len(f.packed) == 1 && bytes.Compare(key, f.packed[0]) < 0
	default:
		return true
	}
}

func (f *MemPKFilter) FilterVector(
	vec *vector.Vector,
	packer *types.Packer,
	skipMask *objectio.Bitmap,
) {
	keys := EncodePrimaryKeyVector(vec, packer)
	for i := 0; i < len(keys); i++ {
		if !f.matches(keys[i]) {
			skipMask.Add(uint64(i))
		}
	}
}
