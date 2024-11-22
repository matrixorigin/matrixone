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
	leveledObjects [len(levels)][]*catalog.ObjectEntry

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
	if !obj.SortKeyZoneMap().IsInited() {
		return false
	}
	if m.segments[obj.ObjectName().SegmentId()] == nil {
		m.segments[obj.ObjectName().SegmentId()] = make(map[*catalog.ObjectEntry]struct{})
	}
	m.segments[obj.ObjectName().SegmentId()][obj] = struct{}{}
	return true
}

func (m *objOverlapPolicy) revise(rc *resourceController, config *BasicPolicyConfig) []reviseResult {
	for _, objects := range m.segments {
		l := segLevel(len(objects))
		for obj := range objects {
			m.leveledObjects[l] = append(m.leveledObjects[l], obj)
		}
	}

	reviseResults := make([]reviseResult, len(levels))
	for i := range 4 {
		if len(m.leveledObjects[i]) < 2 {
			continue
		}

		m.overlappingObjsSet = m.overlappingObjsSet[:0]
		revisedResults := m.reviseLeveledObjs(i)
		for _, result := range revisedResults {
			if len(result.objs) < 2 {
				continue
			}

			if result.kind == taskHostDN {
				if rc.resourceAvailable(result.objs) {
					rc.reserveResources(result.objs)
				} else {
					continue
				}
			}
			reviseResults[i] = result
		}
	}
	return reviseResults
}

func (m *objOverlapPolicy) reviseLeveledObjs(level int) []reviseResult {
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

		// obj has no overlap with the set.
		// if the set has more than one objects, add the set to m.overlappingObjsSet.
		// else dismiss it.
		if len(set.entries) > 1 {
			m.overlappingObjsSet = append(m.overlappingObjsSet, set.cloneEntries())
		}

		set.reset()
		set.add(obj)
	}
	// there is still more than one entry in set.
	if len(set.entries) > 1 {
		m.overlappingObjsSet = append(m.overlappingObjsSet, set.cloneEntries())
		set.reset()
	}
	if len(m.overlappingObjsSet) == 0 {
		return nil
	}

	slices.SortFunc(m.overlappingObjsSet, func(a, b []*catalog.ObjectEntry) int {
		return cmp.Compare(len(a), len(b))
	})

	revisedResults := make([]reviseResult, 0, len(m.overlappingObjsSet))
	// get the overlapping set with most objs.
	for i := len(m.overlappingObjsSet) - 1; i >= 0; i-- {
		objs := m.overlappingObjsSet[i]
		if len(objs) < 2 || originalRows(objs) < 8192*len(objs) {
			continue
		}
		if level < 3 && len(objs) > levels[3] {
			objs = objs[:levels[3]]
		}
		revisedResults = append(revisedResults, reviseResult{
			objs: slices.Clone(objs),
			kind: taskHostCN,
		})
	}

	return revisedResults
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

func (s *entrySet) cloneEntries() []*catalog.ObjectEntry {
	clone := make([]*catalog.ObjectEntry, len(s.entries))
	copy(clone, s.entries)
	return clone
}

func segLevel(length int) int {
	l := len(levels) - 1
	for i, level := range levels {
		if length < level {
			l = i - 1
			break
		}
	}
	return l
}
