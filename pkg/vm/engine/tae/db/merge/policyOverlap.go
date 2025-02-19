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
)

var _ policy = (*objOverlapPolicy)(nil)

var levels = [6]int{
	1, 2, 4, 16, 64, 256,
}

type objOverlapPolicy struct {
	leveledObjects [len(levels)][]*catalog.ObjectEntry

	segments map[objectio.Segmentid]map[*catalog.ObjectEntry]struct{}

	config *BasicPolicyConfig
}

func newObjOverlapPolicy() *objOverlapPolicy {
	return &objOverlapPolicy{
		segments: make(map[objectio.Segmentid]map[*catalog.ObjectEntry]struct{}),
	}
}

func (m *objOverlapPolicy) onObject(obj *catalog.ObjectEntry) bool {
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

func (m *objOverlapPolicy) revise(rc *resourceController) []reviseResult {
	for _, objects := range m.segments {
		l := segLevel(len(objects))
		for obj := range objects {
			m.leveledObjects[l] = append(m.leveledObjects[l], obj)
		}
	}

	reviseResults := make([]reviseResult, 0, len(levels))
	for i := range 4 {
		objects := m.leveledObjects[i]
		if len(objects) < 2 {
			continue
		}

		points := makeEndPoints(objects)
		res := objectsWithGivenOverlaps(points, 5)
		if len(res) == 0 {
			viewed := make(map[*catalog.ObjectEntry]struct{})
			tmp := make([]*catalog.ObjectEntry, 0)
			sum := uint32(0)
			for _, p := range points {
				if _, ok := viewed[p.obj]; ok {
					continue
				}
				viewed[p.obj] = struct{}{}
				if p.obj.OriginSize() < m.config.MaxOsizeMergedObj || sum < m.config.MaxOsizeMergedObj {
					sum += p.obj.OriginSize()
					tmp = append(tmp, p.obj)
				} else {
					if len(tmp) > 1 {
						reviseResults = append(reviseResults, reviseResult{objs: removeOversize(tmp), kind: taskHostDN})
					}
					tmp = tmp[:0]
					sum = 0
				}
			}
		}

		for _, objs := range res {
			objs = removeOversize(objs)
			if len(objs) < 2 || score(objs) < 1.1 {
				continue
			}
			result := reviseResult{objs: objs, kind: taskHostDN}
			if rc.cpuPercent > 80 {
				continue
			}

			for !rc.resourceAvailable(result.objs) && len(result.objs) > 1 {
				result.objs = result.objs[:len(result.objs)-1]
			}
			if len(result.objs) < 2 {
				continue
			}
			if len(result.objs) > 30 {
				result.objs = result.objs[:30]
			}

			rc.reserveResources(result.objs)
			reviseResults = append(reviseResults, result)
		}
	}
	return reviseResults
}

func (m *objOverlapPolicy) resetForTable(entry *catalog.TableEntry, config *BasicPolicyConfig) {
	for i := range m.leveledObjects {
		m.leveledObjects[i] = m.leveledObjects[i][:0]
	}
	m.config = config
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

func objectsWithGivenOverlaps(points []endPoint, overlaps int) [][]*catalog.ObjectEntry {
	globalMax := 0

	res := make([][]*catalog.ObjectEntry, 0)
	tmp := make(map[*catalog.ObjectEntry]struct{})
	for {
		objs := make([]*catalog.ObjectEntry, 0, len(points)/2)
		clear(tmp)
		for _, p := range points {
			if p.s == 1 {
				tmp[p.obj] = struct{}{}
			} else {
				delete(tmp, p.obj)
			}
			if len(tmp) > globalMax {
				globalMax = len(tmp)
				objs = objs[:0]
				for obj := range tmp {
					objs = append(objs, obj)
				}
			}
		}
		if len(objs) < overlaps {
			return res
		}
		res = append(res, objs)
		points = slices.DeleteFunc(points, func(point endPoint) bool {
			return slices.Contains(objs, point.obj)
		})
		globalMax = 0
	}
}

func makeEndPoints(objects []*catalog.ObjectEntry) []endPoint {
	points := make([]endPoint, 0, 2*len(objects))
	for _, obj := range objects {
		zm := obj.SortKeyZoneMap()
		points = append(points, endPoint{val: zm.GetMinBuf(), s: 1, obj: obj})
		points = append(points, endPoint{val: zm.GetMaxBuf(), s: -1, obj: obj})
	}
	slices.SortFunc(points, func(a, b endPoint) int {
		c := compute.Compare(a.val, b.val, objects[0].SortKeyZoneMap().GetType(),
			a.obj.SortKeyZoneMap().GetScale(), b.obj.SortKeyZoneMap().GetScale())
		if c != 0 {
			return c
		}
		// left node is first
		return -a.s
	})
	return points
}
