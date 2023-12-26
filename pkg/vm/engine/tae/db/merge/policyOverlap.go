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
	"fmt"
	"sort"

	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/logutil"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/catalog"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/iface/txnif"
)

var _ Policy = (*Overlap)(nil)

type Overlap struct {
	id       uint64
	schema   *catalog.Schema
	analyzer *OverlapInspector
}

func NewOverlapPolicy() *Overlap {
	return &Overlap{
		analyzer: &OverlapInspector{},
	}
}

// impl Policy for Basic
func (o *Overlap) OnObject(obj *catalog.ObjectEntry) {
	sortKeyZonemap := obj.Stat.GetSortKeyZonemap()
	if sortKeyZonemap != nil {
		o.analyzer.push(sortKeyZonemap.GetMin(), true, obj)
		o.analyzer.push(sortKeyZonemap.GetMax(), false, obj)

		if o.schema.Name == "bmsql_new_order" {
			minv := sortKeyZonemap.GetMinBuf()
			maxv := sortKeyZonemap.GetMaxBuf()
			mint, _, _ := types.DecodeTuple(minv)
			maxt, _, _ := types.DecodeTuple(maxv)
			logutil.Infof("Mergeblocks %d %s %s", obj.SortHint, mint.ErrString(nil), maxt.ErrString(nil))
		}
	}
}

func (o *Overlap) SetConfig(*catalog.TableEntry, func() txnif.AsyncTxn, any) {}
func (o *Overlap) GetConfig(*catalog.TableEntry) any                         { return nil }

func (o *Overlap) Revise(cpu, mem int64) []*catalog.ObjectEntry {
	o.analyzer.analyze(o.schema.GetSingleSortKeyType().Oid, o.schema.Name)
	return nil
}

func (o *Overlap) ResetForTable(entry *catalog.TableEntry) {
	o.id = entry.ID
	o.schema = entry.GetLastestSchema()
	o.analyzer.reset()
}

type OverlapUnit struct {
	point  any
	isOpen bool
	entry  *catalog.ObjectEntry
}

type OverlapInspector struct {
	units []OverlapUnit
}

func (oi *OverlapInspector) reset() {
	oi.units = oi.units[:0]
}

func (oi *OverlapInspector) push(point any, isOpen bool, entry *catalog.ObjectEntry) {
	oi.units = append(oi.units, OverlapUnit{point, isOpen, entry})
}

func (oi *OverlapInspector) analyze(t types.T, name string) {
	sort.Slice(oi.units, func(i, j int) bool {
		switch t {
		case types.T_int8:
			return oi.units[i].point.(int8) < oi.units[j].point.(int8)
		case types.T_int16:
			return oi.units[i].point.(int16) < oi.units[j].point.(int16)
		case types.T_int32:
			return oi.units[i].point.(int32) < oi.units[j].point.(int32)
		case types.T_int64:
			return oi.units[i].point.(int64) < oi.units[j].point.(int64)
		case types.T_uint8:
			return oi.units[i].point.(uint8) < oi.units[j].point.(uint8)
		case types.T_uint16:
			return oi.units[i].point.(uint16) < oi.units[j].point.(uint16)
		case types.T_uint32:
			return oi.units[i].point.(uint32) < oi.units[j].point.(uint32)
		case types.T_uint64:
			return oi.units[i].point.(uint64) < oi.units[j].point.(uint64)
		case types.T_float32:
			return oi.units[i].point.(float32) < oi.units[j].point.(float32)
		case types.T_float64:
			return oi.units[i].point.(float64) < oi.units[j].point.(float64)
		case types.T_date:
			return oi.units[i].point.(types.Date) < oi.units[j].point.(types.Date)
		case types.T_time:
			return oi.units[i].point.(types.Time) < oi.units[j].point.(types.Time)
		case types.T_datetime:
			return oi.units[i].point.(types.Datetime) < oi.units[j].point.(types.Datetime)
		case types.T_timestamp:
			return oi.units[i].point.(types.Timestamp) < oi.units[j].point.(types.Timestamp)
		case types.T_enum:
			return oi.units[i].point.(types.Enum) < oi.units[j].point.(types.Enum)
		case types.T_decimal64:
			return oi.units[i].point.(types.Decimal64).Less(oi.units[j].point.(types.Decimal64))
		case types.T_decimal128:
			return oi.units[i].point.(types.Decimal128).Less(oi.units[j].point.(types.Decimal128))
		case types.T_uuid:
			return oi.units[i].point.(types.Uuid).Lt(oi.units[j].point.(types.Uuid))
		case types.T_TS:
			return oi.units[i].point.(types.TS).Less(oi.units[j].point.(types.TS))
		case types.T_Rowid:
			return oi.units[i].point.(types.Rowid).Less(oi.units[j].point.(types.Rowid))
		case types.T_Blockid:
			return oi.units[i].point.(types.Blockid).Compare(oi.units[j].point.(types.Blockid)) < 0
		case types.T_char, types.T_varchar, types.T_json,
			types.T_binary, types.T_varbinary, types.T_blob, types.T_text:
			return bytes.Compare(oi.units[i].point.([]byte), oi.units[j].point.([]byte)) < 0
		default:
			panic(fmt.Sprintf("unsupported type: %v", t))
		}
	})

	overlapDepth := 0
	maxDepth := 0
	overlapSet := make(map[uint64]bool, 0)
	maxSet := make([]uint64, 0)
	for _, unit := range oi.units {
		if unit.isOpen {
			overlapDepth++
			overlapSet[unit.entry.SortHint] = true
		} else {
			overlapDepth--
			if _, ok := overlapSet[unit.entry.SortHint]; ok {
				delete(overlapSet, unit.entry.SortHint)
			} else {
				panic("unmatched close")
			}
		}
		if overlapDepth > maxDepth {
			maxDepth = overlapDepth
			maxSet = maxSet[:0]
			for k := range overlapSet {
				maxSet = append(maxSet, k)
			}
		}
	}

	if len(oi.units) > 2 {
		sort.Slice(maxSet, func(i, j int) bool { return maxSet[i] < maxSet[j] })
		logutil.Infof("Mergeblocks %s overlap (%d/%d) %v", name, len(maxSet), len(oi.units)/2, maxSet)
	}
}
