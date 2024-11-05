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

	"github.com/matrixorigin/matrixone/pkg/objectio"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/catalog"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/compute"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/index"
)

var _ policy = (*objOverlapPolicy)(nil)

var levels = [6]int{
	1, 2, 4, 16, 64, 256,
}

type objOverlapPolicy struct {
	leveledObjects [6][]*catalog.ObjectEntry

	segments           map[objectio.Segmentid]map[*catalog.ObjectEntry]struct{}
	overlappingObjsSet [][]*catalog.ObjectEntry
}

func newObjOverlapPolicy() *objOverlapPolicy {
	return &objOverlapPolicy{
		overlappingObjsSet: make([][]*catalog.ObjectEntry, 0),
		segments:           make(map[objectio.Segmentid]map[*catalog.ObjectEntry]struct{}),
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
	if m.segments[obj.ObjectName().SegmentId()] == nil {
		m.segments[obj.ObjectName().SegmentId()] = make(map[*catalog.ObjectEntry]struct{})
	}
	m.segments[obj.ObjectName().SegmentId()][obj] = struct{}{}
	return true
}

func (m *objOverlapPolicy) revise(cpu, mem int64, config *BasicPolicyConfig) []reviseResult {
	for _, objects := range m.segments {
		segLevel := segLevel(len(objects))
		for obj := range objects {
			m.leveledObjects[segLevel] = append(m.leveledObjects[segLevel], obj)
		}
	}

	reviseResults := make([]reviseResult, len(levels))
	for i := range len(levels) {
		if len(m.leveledObjects[i]) < 2 {
			continue
		}

		if cpu > 80 {
			continue
		}

		m.overlappingObjsSet = m.overlappingObjsSet[:0]

		objs, taskHostKind := m.reviseLeveledObjs(i)
		if ok, eSize := controlMem(objs, mem); ok && len(objs) > 1 {
			reviseResults[i] = reviseResult{slices.Clone(objs), taskHostKind}
			mem -= eSize
		}
	}
	return reviseResults
}

func (m *objOverlapPolicy) reviseLeveledObjs(level int) ([]*catalog.ObjectEntry, TaskHostKind) {
	slices.SortFunc(m.leveledObjects[level], func(a, b *catalog.ObjectEntry) int {
		zmA := a.SortKeyZoneMap()
		zmB := b.SortKeyZoneMap()
		if c := zmA.CompareMin(zmB); c != 0 {
			return c
		}
		return zmA.CompareMax(zmB)
	})
	set := entrySet{entries: make([]*catalog.ObjectEntry, 0), maxValue: []byte{}}
	for _, obj := range m.leveledObjects[level] {
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

	if level < 2 && len(objs) > levels[3] {
		objs = objs[:levels[3]]
	}

	return objs, TaskHostDN
}

func (m *objOverlapPolicy) resetForTable(*catalog.TableEntry, *BasicPolicyConfig) {
	m.overlappingObjsSet = m.overlappingObjsSet[:0]
	for i := range m.leveledObjects {
		m.leveledObjects[i] = m.leveledObjects[i][:0]
	}
	clear(m.segments)
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

func segLevel(length int) int {
	l := 5
	for i, level := range levels {
		if length < level {
			l = i - 1
			break
		}
	}
	return l
}
