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
	"bytes"
	"maps"
	"slices"

	"github.com/matrixorigin/matrixone/pkg/objectio"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/catalog"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/common"
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
	if rc.cpuPercent > 80 {
		return nil
	}

	for _, objects := range m.segments {
		l := segLevel(len(objects))
		for obj := range objects {
			m.leveledObjects[l] = append(m.leveledObjects[l], obj)
		}
	}

	reviseResults := make([]reviseResult, 0, len(levels))
	for i := range 4 {
		if len(m.leveledObjects[i]) < 2 {
			continue
		}

		points := makeEndPoints(m.leveledObjects[i])
		if res := objectsWithGivenOverlaps(points, 5); len(res) != 0 {
			for _, objs := range res {
				objs = removeOversize(objs)
				if len(objs) < 2 || score(objs) < 1.1 {
					continue
				}
				result := reviseResult{objs: objs, kind: taskHostDN}
				reviseResults = append(reviseResults, result)
			}
		} else {
			viewed := make(map[*catalog.ObjectEntry]struct{})
			tmp := make([]*catalog.ObjectEntry, 0)
			sum := uint32(0)
			for _, p := range points {
				if _, ok := viewed[p.obj]; ok {
					continue
				}
				viewed[p.obj] = struct{}{}
				if p.obj.OriginSize() < m.config.MaxOsizeMergedObj/2 || sum < m.config.MaxOsizeMergedObj/2 {
					sum += p.obj.OriginSize()
					tmp = append(tmp, p.obj)
				} else {
					if len(tmp) > 1 {
						reviseResults = append(reviseResults, reviseResult{objs: removeOversize(slices.Clone(tmp)), kind: taskHostDN})
					}
					tmp = tmp[:0]
					sum = 0
				}
			}
			if len(tmp) > 200 { // let the little objs accumulate to a certain number
				reviseResults = append(reviseResults, reviseResult{objs: removeOversize(slices.Clone(tmp)), kind: taskHostDN})
			}
		}
	}

	for i := range reviseResults {
		for !rc.resourceAvailable(reviseResults[i].objs) && len(reviseResults[i].objs) > 1 {
			reviseResults[i].objs = reviseResults[i].objs[:len(reviseResults[i].objs)/2]
		}
		if len(reviseResults[i].objs) < 2 {
			continue
		}

		rc.reserveResources(reviseResults[i].objs)
	}
	return slices.DeleteFunc(reviseResults, func(result reviseResult) bool {
		return len(result.objs) < 2
	})
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
	s   bool

	obj *catalog.ObjectEntry
}

func objectsWithGivenOverlaps(points []endPoint, overlaps int) [][]*catalog.ObjectEntry {
	res := make([][]*catalog.ObjectEntry, 0)
	tmp := make(map[*catalog.ObjectEntry]struct{})
	for {
		clear(tmp)
		count := 0
		globalMax := 0
		for _, p := range points {
			if p.s {
				count++
			} else {
				count--
			}
			if count > globalMax {
				globalMax = count
			}
		}
		for _, p := range points {
			if p.s {
				tmp[p.obj] = struct{}{}
			} else {
				delete(tmp, p.obj)
			}
			if len(tmp) == globalMax {
				break
			}
		}

		if len(tmp) > 1 {
			res = append(res, slices.Collect(maps.Keys(tmp)))
		}
		if len(tmp) < overlaps {
			return res
		}

		points = slices.DeleteFunc(points, func(point endPoint) bool {
			_, ok := tmp[point.obj]
			return ok
		})
	}
}

func makeEndPoints(objects []*catalog.ObjectEntry) []endPoint {
	points := make([]endPoint, 0, 2*len(objects))
	for _, obj := range objects {
		zm := obj.SortKeyZoneMap()
		if obj.OriginSize() >= common.DefaultMinOsizeQualifiedMB*common.Const1MBytes {
			if !zm.IsString() {
				if zm.GetMin() == zm.GetMax() {
					continue
				}
			} else {
				if zm.MaxTruncated() {
					continue
				}
				minBuf := zm.GetMinBuf()
				maxBuf := zm.GetMaxBuf()
				if len(minBuf) == len(maxBuf) && len(minBuf) == 30 {
					copiedMin := make([]byte, len(minBuf))
					copiedMax := make([]byte, len(maxBuf))

					copy(copiedMin, minBuf)
					copy(copiedMax, maxBuf)
					for i := 29; i >= 0; i-- {
						if copiedMax[i] != 0 {
							copiedMax[i]--
							break
						}
						copiedMax[i]--
					}
					if bytes.Compare(copiedMin, copiedMax) == 0 {
						continue
					}
				}
			}
		}
		points = append(points, endPoint{val: zm.GetMinBuf(), s: true, obj: obj})
		points = append(points, endPoint{val: zm.GetMaxBuf(), s: false, obj: obj})
	}
	slices.SortFunc(points, func(a, b endPoint) int {
		c := compute.Compare(a.val, b.val, objects[0].SortKeyZoneMap().GetType(),
			a.obj.SortKeyZoneMap().GetScale(), b.obj.SortKeyZoneMap().GetScale())
		if c != 0 {
			return c
		}
		// left node is first
		if a.s {
			return -1
		}
		return 1
	})
	return points
}
