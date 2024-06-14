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

package merge

import (
	"fmt"
	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/catalog"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/common"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/compute"
	"math"
	"slices"
)

type multiObjConfig struct {
	maxObjs            int
	maxOSizeMergedObjs int
}

type multiObjPolicy struct {
	config  *multiObjConfig
	objects []*catalog.ObjectEntry
}

func (m *multiObjConfig) adjust() {
	if m.maxObjs == 0 {
		m.maxObjs = common.DefaultMaxMergeObjN
	}
	if m.maxOSizeMergedObjs == 0 {
		m.maxOSizeMergedObjs = common.DefaultMaxOsizeObjMB * common.Const1MBytes
	}
}

func newMultiObjPolicy(config *multiObjConfig) *multiObjPolicy {
	if config == nil {
		config = &multiObjConfig{}
	}
	config.adjust()
	return &multiObjPolicy{
		config:  config,
		objects: make([]*catalog.ObjectEntry, 0, config.maxObjs),
	}
}

func (m *multiObjPolicy) OnObject(obj *catalog.ObjectEntry) {
	m.objects = append(m.objects, obj)
}

func (m *multiObjPolicy) OnPostTable() {
	if len(m.objects) < 2 {
		return
	}
	if !m.objects[0].GetSortKeyZonemap().IsInited() {
		return
	}
	t := m.objects[0].GetSortKeyZonemap().GetType()

	slices.SortFunc(m.objects, func(a, b *catalog.ObjectEntry) int {
		zmA := a.GetSortKeyZonemap()
		zmB := b.GetSortKeyZonemap()
		if c := compute.CompareGeneric(zmA.GetMin(), zmB.GetMin(), t); c != 0 {
			return c
		}
		return compute.CompareGeneric(zmA.GetMax(), zmB.GetMax(), t)
	})

	set := entrySet{entries: make([]*catalog.ObjectEntry, 0), maxValue: minValue(t)}
	for _, obj := range m.objects {
		zm := obj.GetSortKeyZonemap()
		if len(set.entries) == 0 || compute.CompareGeneric(set.maxValue, zm.GetMin(), t) > 0 {
			set.add(t, obj)
		} else if len(set.entries) == 1 {
			set.reset(t)
			set.add(t, obj)
		} else {
			break
		}
	}
	if len(set.entries) > m.config.maxObjs {
		m.objects = set.entries[:m.config.maxObjs]
	}
	m.objects = set.entries
}

func (m *multiObjPolicy) Revise(cpu, mem int64) ([]*catalog.ObjectEntry, TaskHostKind) {
	if len(m.objects) < 2 {
		return nil, TaskHostDN
	}
	if !m.objects[0].GetSortKeyZonemap().IsInited() {
		revisedObj := make([]*catalog.ObjectEntry, len(m.objects))
		copy(revisedObj, m.objects)
		return revisedObj, TaskHostDN
	}

	t := m.objects[0].GetSortKeyZonemap().GetType()

	slices.SortFunc(m.objects, func(a, b *catalog.ObjectEntry) int {
		zmA := a.GetSortKeyZonemap()
		zmB := b.GetSortKeyZonemap()
		if c := compute.CompareGeneric(zmA.GetMin(), zmB.GetMin(), t); c != 0 {
			return c
		}
		return compute.CompareGeneric(zmA.GetMax(), zmB.GetMax(), t)
	})

	set := entrySet{entries: make([]*catalog.ObjectEntry, 0), maxValue: minValue(t)}
	for _, obj := range m.objects {
		zm := obj.GetSortKeyZonemap()
		if len(set.entries) == 0 || compute.CompareGeneric(set.maxValue, zm.GetMin(), t) > 0 {
			set.add(t, obj)
		} else if len(set.entries) == 1 {
			set.reset(t)
			set.add(t, obj)
		} else {
			break
		}
	}
	if len(set.entries) > m.config.maxObjs {
		m.objects = set.entries[:m.config.maxObjs]
	}
	m.objects = set.entries
	objs := m.controlMem(m.objects, mem)
	if len(objs) < 2 {
		return nil, TaskHostDN
	}
	revisedObj := make([]*catalog.ObjectEntry, len(objs))
	copy(revisedObj, objs)
	return revisedObj, TaskHostDN
}

func (m *multiObjPolicy) Clear() {
	m.objects = m.objects[:0]
}

func (m *multiObjPolicy) ObjCnt() int {
	return len(m.objects)
}

type entryInterval struct {
	min, max any
	entry    *catalog.ObjectEntry
}

type entrySet struct {
	entries  []*catalog.ObjectEntry
	maxValue any
	size     int
}

func (s *entrySet) reset(t types.T) {
	s.entries = s.entries[:0]
	s.maxValue = minValue(t)
	s.size = 0
}

func (s *entrySet) add(t types.T, obj *catalog.ObjectEntry) {
	s.entries = append(s.entries, obj)
	s.size += obj.GetOriginSize()
	zmMax := obj.GetSortKeyZonemap().GetMax()
	if compute.CompareGeneric(s.maxValue, zmMax, t) < 0 {
		s.maxValue = zmMax
	}
}

func minValue(t types.T) any {
	switch t {
	case types.T_bit:
		return 0
	case types.T_int8:
		return int8(math.MinInt8)
	case types.T_int16:
		return int16(math.MinInt16)
	case types.T_int32:
		return int32(math.MinInt32)
	case types.T_int64:
		return int64(math.MinInt64)
	case types.T_uint8:
		return uint8(0)
	case types.T_uint16:
		return uint16(0)
	case types.T_uint32:
		return uint32(0)
	case types.T_uint64:
		return uint64(0)
	case types.T_float32:
		return float32(-math.MaxFloat32)
	case types.T_float64:
		return float64(-math.MaxFloat64)
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

func (m *multiObjPolicy) controlMem(objs []*catalog.ObjectEntry, mem int64) []*catalog.ObjectEntry {
	if mem > constMaxMemCap {
		mem = constMaxMemCap
	}

	needPopout := func(ss []*catalog.ObjectEntry) bool {
		osize, esize, _ := estimateMergeConsume(ss)
		if esize > int(2*mem/3) {
			return true
		}

		if len(ss) < 2 {
			return false
		}
		// make object averaged size
		return osize > m.config.maxOSizeMergedObjs
	}
	for needPopout(objs) {
		objs = objs[:len(objs)-1]
	}

	return objs
}
