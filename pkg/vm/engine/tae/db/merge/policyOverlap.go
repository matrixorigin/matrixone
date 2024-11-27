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
		revisedResults := []reviseResult{{
			objs: objectsWithMaximumOverlaps(m.leveledObjects[i]),
			kind: taskHostDN,
		}}
		for _, result := range revisedResults {
			if len(result.objs) < 2 {
				continue
			}

			if result.kind == taskHostDN {
				if rc.cpuPercent > 80 {
					continue
				}

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

func (m *objOverlapPolicy) resetForTable(*catalog.TableEntry, *BasicPolicyConfig) {
	m.overlappingObjsSet = m.overlappingObjsSet[:0]
	for i := range m.leveledObjects {
		m.leveledObjects[i] = m.leveledObjects[i][:0]
	}
	clear(m.segments)
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

type endPoint struct {
	val []byte
	s   int

	obj *catalog.ObjectEntry
}

func objectsWithMaximumOverlaps(objects []*catalog.ObjectEntry) []*catalog.ObjectEntry {
	if len(objects) < 2 {
		return nil
	}
	points := make([]endPoint, 0, len(objects))
	for _, obj := range objects {
		zm := obj.SortKeyZoneMap()
		points = append(points, endPoint{val: zm.GetMinBuf(), obj: obj, s: 1})
		points = append(points, endPoint{val: zm.GetMaxBuf(), obj: obj, s: -1})
	}
	t := objects[0].SortKeyZoneMap().GetType()
	if t.FixedLength() > 0 {
		slices.SortFunc(points, func(a, b endPoint) int {
			c := compute.Compare(a.val, b.val, t,
				a.obj.SortKeyZoneMap().GetScale(), b.obj.SortKeyZoneMap().GetScale())
			if c != 0 {
				return c
			}
			if a.s == 1 {
				// left node is first
				return -1
			}
			return 1
		})
	} else {
		slices.SortFunc(points, func(a, b endPoint) int {
			if a.s == b.s {
				c := compute.Compare(a.val, b.val, t,
					a.obj.SortKeyZoneMap().GetScale(), b.obj.SortKeyZoneMap().GetScale())
				if c != 0 {
					return c
				}
				if a.s == 1 {
					// left node is first
					return -1
				}
				return 1
			}
			if a.s == -1 {
				return index.StrictlyCompareZmMaxAndMin(b.val, a.val, t,
					b.obj.SortKeyZoneMap().GetScale(), a.obj.SortKeyZoneMap().GetScale())
			}
			return index.StrictlyCompareZmMaxAndMin(a.val, b.val, t,
				a.obj.SortKeyZoneMap().GetScale(), b.obj.SortKeyZoneMap().GetScale())
		})
	}

	globalMax, tmpMax := 0, 0
	res := make([]*catalog.ObjectEntry, 0, len(objects))
	tmp := make(map[*catalog.ObjectEntry]struct{})
	for _, p := range points {
		tmpMax += p.s
		if p.s == 1 {
			tmp[p.obj] = struct{}{}
		} else {
			delete(tmp, p.obj)
		}
		if tmpMax > globalMax {
			globalMax = tmpMax
			res = res[:0]
			for obj := range tmp {
				res = append(res, obj)
			}
		}
	}
	if len(res) < 2 {
		return nil
	}
	return res
}
