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
	"slices"

	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/catalog"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/common"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/compute"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/index"
)

var _ policy = (*objOverlapPolicy)(nil)

type objOverlapPolicy struct {
	objects     []*catalog.ObjectEntry
	objectsSize int

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
	if m.objectsSize > 10*common.DefaultMaxOsizeObjMB*common.Const1MBytes {
		return false
	}
	m.objects = append(m.objects, obj)
	m.objectsSize += int(obj.OriginSize())
	return true
}

func (m *objOverlapPolicy) revise(cpu, mem int64, config *BasicPolicyConfig) []reviseResult {
	if len(m.objects) < 2 {
		return nil
	}
	if cpu > 80 {
		return nil
	}
	objs, taskHostKind := m.reviseDataObjs(config)
	objs = controlMem(objs, mem)
	if len(objs) > 1 {
		return []reviseResult{{objs, taskHostKind}}
	}
	return nil
}

func (m *objOverlapPolicy) reviseDataObjs(config *BasicPolicyConfig) ([]*catalog.ObjectEntry, TaskHostKind) {
	slices.SortFunc(m.objects, func(a, b *catalog.ObjectEntry) int {
		zmA := a.SortKeyZoneMap()
		zmB := b.SortKeyZoneMap()
		if c := zmA.CompareMin(zmB); c != 0 {
			return c
		}
		return zmA.CompareMax(zmB)
	})
	set := entrySet{entries: make([]*catalog.ObjectEntry, 0), maxValue: []byte{}}
	for _, obj := range m.objects {
		if len(set.entries) == 0 {
			set.add(obj)
			continue
		}

		if zm := obj.SortKeyZoneMap(); index.StrictlyCompareZmMaxAndMin(set.maxValue, zm.GetMinBuf(), zm.GetType(), zm.GetScale(), zm.GetScale()) > 0 {
			// zm is overlapped
			set.add(obj)
			continue
		}

		// obj is not added in the set.
		// either dismiss the set or add the set in m.overlappingObjsSet
		if len(set.entries) > 1 {
			objs := make([]*catalog.ObjectEntry, len(set.entries))
			copy(objs, set.entries)
			m.overlappingObjsSet = append(m.overlappingObjsSet, objs)
		}

		set.reset()
		set.add(obj)
	}
	// there is still more than one entry in set.
	if len(set.entries) > 1 {
		objs := make([]*catalog.ObjectEntry, len(set.entries))
		copy(objs, set.entries)
		m.overlappingObjsSet = append(m.overlappingObjsSet, objs)
		set.reset()
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
	return objs, TaskHostDN
}

func (m *objOverlapPolicy) resetForTable(*catalog.TableEntry) {
	m.objects = m.objects[:0]
	m.overlappingObjsSet = m.overlappingObjsSet[:0]
	m.objectsSize = 0
}

type entrySet struct {
	entries  []*catalog.ObjectEntry
	maxValue []byte
	size     int
}

func (s *entrySet) reset() {
	s.entries = s.entries[:0]
	s.maxValue = []byte{}
	s.size = 0
}

func (s *entrySet) add(obj *catalog.ObjectEntry) {
	s.entries = append(s.entries, obj)
	s.size += int(obj.OriginSize())
	if zm := obj.SortKeyZoneMap(); len(s.maxValue) == 0 ||
		compute.Compare(s.maxValue, zm.GetMaxBuf(), zm.GetType(), zm.GetScale(), zm.GetScale()) < 0 {
		s.maxValue = zm.GetMaxBuf()
	}
}
