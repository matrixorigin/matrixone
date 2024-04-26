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

package merge

import (
	"bytes"
	"cmp"
	"fmt"
	"math"
	"slices"

	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/logutil"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/catalog"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/iface/txnif"
)

var _ Policy = (*overlap)(nil)

type overlapConfig struct {
	lessThan, greaterThan any
}

type overlap struct {
	id     uint64
	schema *catalog.Schema

	inspector *overlapInspector
	config    *overlapConfig
}

func NewOverlapPolicy() Policy {
	return &overlap{
		inspector: &overlapInspector{
			intervals: make([]*entryInterval, 0),
		},
	}
}

// impl Policy for Basic
func (o *overlap) OnObject(obj *catalog.ObjectEntry) {
	/*	if obj.GetOriginSize() <= 110*common.Const1MBytes {
		return
	}*/
	sortKeyZonemap := obj.GetSortKeyZonemap()
	if sortKeyZonemap == nil {
		return
	}
	o.inspector.add(obj)
	if o.schema.Name == "bmsql_new_order" {
		minv := sortKeyZonemap.GetMinBuf()
		maxv := sortKeyZonemap.GetMaxBuf()
		mint, _, _, _ := types.DecodeTuple(minv)
		maxt, _, _, _ := types.DecodeTuple(maxv)
		logutil.Infof("Mergeblocks %d %s %s", obj.SortHint, mint.ErrString(nil), maxt.ErrString(nil))
	}
}

func (o *overlap) SetConfig(*catalog.TableEntry, func() txnif.AsyncTxn, any) {}
func (o *overlap) GetConfig(*catalog.TableEntry) any                         { return nil }

func (o *overlap) Revise(_, mem int64) ([]*catalog.ObjectEntry, TaskHostKind) {
	set := o.inspector.analyze()
	if len(set.entries) <= 1 {
		return nil, TaskHostDN
	}
	set.entries = controlMem(set.entries, mem)
	return set.entries, TaskHostDN
}

func (o *overlap) ResetForTable(entry *catalog.TableEntry) {
	o.id = entry.ID
	o.schema = entry.GetLastestSchemaLocked()
	if o.schema.HasSortKey() {
		o.inspector.reset(o.schema.GetSingleSortKeyType().Oid)
	}
}

type entryInterval struct {
	min, max any
	entry    *catalog.ObjectEntry
}

func newEntryInterval(entry *catalog.ObjectEntry) *entryInterval {
	return &entryInterval{
		entry: entry,
		min:   entry.GetSortKeyZonemap().GetMin(),
		max:   entry.GetSortKeyZonemap().GetMax(),
	}
}

type overlapInspector struct {
	t         types.T
	intervals []*entryInterval
}

func (oi *overlapInspector) reset(t types.T) {
	oi.t = t
	oi.intervals = oi.intervals[:0]
}

func (oi *overlapInspector) add(entry *catalog.ObjectEntry) {
	oi.intervals = append(oi.intervals, newEntryInterval(entry))
}

func (oi *overlapInspector) sortIntervals() {
	slices.SortFunc(oi.intervals, func(a, b *entryInterval) int {
		if c := compare(oi.t, a.min, b.min); c != 0 {
			return c
		}
		return compare(oi.t, a.max, b.max)
	})
}

type entrySet struct {
	entries  []*catalog.ObjectEntry
	maxValue any
}

func (s *entrySet) reset(t types.T) {
	clear(s.entries)
	s.maxValue = minValue(t)
}

func (s *entrySet) add(t types.T, entry *entryInterval) {
	s.entries = append(s.entries, entry.entry)
	if compare(t, s.maxValue, entry.max) < 0 {
		s.maxValue = entry.max
	}
}

func (oi *overlapInspector) analyze() entrySet {
	oi.sortIntervals()

	set := entrySet{entries: make([]*catalog.ObjectEntry, 0), maxValue: minValue(oi.t)}
	for _, interval := range oi.intervals {
		interval := interval
		logutil.Infof("Mergeblocks %v %v", interval.min, interval.max)
		if len(set.entries) == 0 || compare(oi.t, set.maxValue, interval.min) < 0 {
			set.add(oi.t, interval)
		} else if len(set.entries) == 1 {
			set.reset(oi.t)
			set.add(oi.t, interval)
		} else {
			return set
		}
	}
	return entrySet{}
}

func compare(t types.T, a, b any) int {
	switch t {
	case types.T_bit:
		return cmp.Compare(a.(uint64), b.(uint64))
	case types.T_int8:
		return cmp.Compare(a.(int8), b.(int8))
	case types.T_int16:
		return cmp.Compare(a.(int16), b.(int16))
	case types.T_int32:
		return cmp.Compare(a.(int32), b.(int32))
	case types.T_int64:
		return cmp.Compare(a.(int64), b.(int64))
	case types.T_uint8:
		return cmp.Compare(a.(uint8), b.(uint8))
	case types.T_uint16:
		return cmp.Compare(a.(uint16), b.(uint16))
	case types.T_uint32:
		return cmp.Compare(a.(uint32), b.(uint32))
	case types.T_uint64:
		return cmp.Compare(a.(uint64), b.(uint64))
	case types.T_float32:
		return cmp.Compare(a.(float32), b.(float32))
	case types.T_float64:
		return cmp.Compare(a.(float64), b.(float64))
	case types.T_date:
		return cmp.Compare(a.(types.Date), b.(types.Date))
	case types.T_time:
		return cmp.Compare(a.(types.Time), b.(types.Time))
	case types.T_datetime:
		return cmp.Compare(a.(types.Datetime), b.(types.Datetime))
	case types.T_timestamp:
		return cmp.Compare(a.(types.Timestamp), b.(types.Timestamp))
	case types.T_enum:
		return cmp.Compare(a.(types.Enum), b.(types.Enum))
	case types.T_decimal64:
		return a.(types.Decimal64).Compare(b.(types.Decimal64))
	case types.T_decimal128:
		return a.(types.Decimal128).Compare(b.(types.Decimal128))
	case types.T_uuid:
		return a.(types.Uuid).Compare(b.(types.Uuid))
	case types.T_TS:
		ts1 := a.(types.TS)
		ts2 := b.(types.TS)
		return ts1.Compare(&ts2)
	case types.T_Rowid:
		return a.(types.Rowid).Compare(b.(types.Rowid))
	case types.T_Blockid:
		return a.(types.Blockid).Compare(b.(types.Blockid))
	case types.T_char, types.T_varchar, types.T_json,
		types.T_binary, types.T_varbinary, types.T_blob, types.T_text:
		return bytes.Compare(a.([]byte), b.([]byte))
	default:
		panic(fmt.Sprintf("unsupported type: %v", t))
	}
}

func minValue(t types.T) any {
	switch t {
	case types.T_bit:
		return 0
	case types.T_int8:
		return math.MinInt8
	case types.T_int16:
		return math.MinInt16
	case types.T_int32:
		return math.MinInt32
	case types.T_int64:
		return math.MinInt64
	case types.T_uint8:
		return 0
	case types.T_uint16:
		return 0
	case types.T_uint32:
		return 0
	case types.T_uint64:
		return 0
	case types.T_float32:
		return -math.MaxFloat32
	case types.T_float64:
		return -math.MaxFloat64
	case types.T_date:
		return types.Date(math.MinInt32)
	case types.T_time:
		return types.Time(math.MinInt64)
	case types.T_datetime:
		return types.Datetime(math.MinInt64)
	case types.T_timestamp:
		return types.Timestamp(math.MinInt64)
	case types.T_enum:
		return types.Enum(0)
	case types.T_decimal64:
		return types.Decimal64Min
	case types.T_decimal128:
		return types.Decimal128Min
	case types.T_uuid:
		return types.Uuid{}
	case types.T_TS:
		return types.TS{}
	case types.T_Rowid:
		return types.Rowid{}
	case types.T_Blockid:
		return types.Blockid{}
	case types.T_char, types.T_varchar, types.T_json,
		types.T_binary, types.T_varbinary, types.T_blob, types.T_text:
		return []byte{}
	default:
		panic(fmt.Sprintf("unsupported type: %v", t))
	}
}
