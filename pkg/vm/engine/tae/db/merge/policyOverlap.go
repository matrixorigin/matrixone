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
	"cmp"
	"fmt"
	"math"
	"slices"

	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/catalog"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/compute"
)

var _ policy = (*objOverlapPolicy)(nil)

type objOverlapPolicy struct {
	objects []*catalog.ObjectEntry

	overlappingObjsSet [][]*catalog.ObjectEntry
}

func newObjOverlapPolicy() *objOverlapPolicy {
	return &objOverlapPolicy{
		objects:            make([]*catalog.ObjectEntry, 0),
		overlappingObjsSet: make([][]*catalog.ObjectEntry, 0),
	}
}

func (m *objOverlapPolicy) onObject(obj *catalog.ObjectEntry, config *BasicPolicyConfig) bool {
	if obj.IsTombstone {
		return false
	}
	if obj.OriginSize() < config.ObjectMinOsize {
		return false
	}
	if !obj.SortKeyZoneMap().IsInited() {
		return false
	}
	m.objects = append(m.objects, obj)
	return true
}

func (m *objOverlapPolicy) revise(cpu, mem int64, config *BasicPolicyConfig) ([]*catalog.ObjectEntry, TaskHostKind) {
	if len(m.objects) < 2 {
		return nil, TaskHostDN
	}
	if cpu > 90 {
		return nil, TaskHostDN
	}
	objs, taskHostKind := m.reviseDataObjs(config)
	objs = controlMem(objs, mem)
	if len(objs) > 1 {
		return objs, taskHostKind
	}
	return nil, TaskHostDN
}

func (m *objOverlapPolicy) reviseDataObjs(config *BasicPolicyConfig) ([]*catalog.ObjectEntry, TaskHostKind) {
	t := m.objects[0].SortKeyZoneMap().GetType()
	slices.SortFunc(m.objects, func(a, b *catalog.ObjectEntry) int {
		zmA := a.SortKeyZoneMap()
		zmB := b.SortKeyZoneMap()
		if c := zmA.CompareMin(zmB); c != 0 {
			return c
		}
		return zmA.CompareMax(zmB)
	})
	set := entrySet{entries: make([]*catalog.ObjectEntry, 0), maxValue: minValue(t)}
	for _, obj := range m.objects {
		if len(set.entries) == 0 {
			set.add(t, obj)
			continue
		}

		if compute.CompareGeneric(set.maxValue, obj.SortKeyZoneMap().GetMin(), t) > 0 {
			// zm is overlapped
			set.add(t, obj)
			continue
		}

		// obj is not added in the set.
		// either dismiss the set or add the set in m.overlappingObjsSet
		if len(set.entries) > 1 {
			objs := make([]*catalog.ObjectEntry, len(set.entries))
			copy(objs, set.entries)
			m.overlappingObjsSet = append(m.overlappingObjsSet, objs)
		}

		set.reset(t)
		set.add(t, obj)
	}
	// there is still more than one entry in set.
	if len(set.entries) > 1 {
		objs := make([]*catalog.ObjectEntry, len(set.entries))
		copy(objs, set.entries)
		m.overlappingObjsSet = append(m.overlappingObjsSet, objs)
		set.reset(t)
	}
	if len(m.overlappingObjsSet) == 0 {
		return nil, TaskHostDN
	}

	slices.SortFunc(m.overlappingObjsSet, func(a, b []*catalog.ObjectEntry) int {
		return cmp.Compare(len(a), len(b))
	})

	// get the overlapping set with most objs.
	objs := m.overlappingObjsSet[len(m.overlappingObjsSet)-1]
	if len(objs) < 2 {
		return nil, TaskHostDN
	}
	if len(objs) > config.MergeMaxOneRun {
		objs = objs[:config.MergeMaxOneRun]
	}
	return objs, TaskHostCN
}

func (m *objOverlapPolicy) resetForTable(*catalog.TableEntry) {
	m.objects = m.objects[:0]
	m.overlappingObjsSet = m.overlappingObjsSet[:0]
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
	zmMax := obj.SortKeyZoneMap().GetMax()
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
