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
	"fmt"
	"maps"
	"slices"

	"github.com/matrixorigin/matrixone/pkg/logutil"
	"github.com/matrixorigin/matrixone/pkg/objectio"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/catalog"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/common"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/compute"

	"go.uber.org/zap"
)

var _ policy = (*objOverlapPolicy)(nil)

var levels = [6]int{
	1, 2, 4, 16, 64, 256,
}

type objOverlapPolicy struct {
	leveledObjects [len(levels)][]*catalog.ObjectEntry

	segments map[objectio.Segmentid]map[*catalog.ObjectEntry]struct{}

	config *BasicPolicyConfig
	tid    uint64
	name   string
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
	if m.segments[obj.ObjectName().SegmentId()] == nil {
		m.segments[obj.ObjectName().SegmentId()] = make(map[*catalog.ObjectEntry]struct{})
	}
	m.segments[obj.ObjectName().SegmentId()][obj] = struct{}{}
	return true
}

func IsSameSegment(objs []*catalog.ObjectEntry) bool {
	if len(objs) < 2 {
		return true
	}
	segId := objs[0].ObjectName().SegmentId()
	for _, obj := range objs {
		if !obj.ObjectName().SegmentId().Eq(segId) {
			return false
		}
	}
	return true
}

func hasHundredSmallObjs(objs []*catalog.ObjectEntry, small uint32) bool {
	if len(objs) < 100 {
		return false
	}
	cnt := 0
	for _, obj := range objs {
		if obj.OriginSize() < small {
			cnt++
		}
		if cnt > 100 {
			return true
		}
	}
	return false
}

func isAllGreater(objs []*catalog.ObjectEntry, size uint32) bool {
	for _, obj := range objs {
		if obj.OriginSize() < size {
			return false
		}
	}
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

	checkNonoverlapObj := func(objs []*catalog.ObjectEntry, note string) {
		tmp := make([]*catalog.ObjectEntry, 0)
		sum := 0
		for _, obj := range objs {
			if obj.OriginSize() < m.config.MaxOsizeMergedObj/2 || sum < int(m.config.MaxOsizeMergedObj)/2 {
				sum += int(obj.OriginSize())
				tmp = append(tmp, obj)
			} else {
				if len(tmp) > 1 {
					reviseResults = append(reviseResults, reviseResult{objs: removeOversize(slices.Clone(tmp)), kind: taskHostDN, note: note})
				}
				tmp = tmp[:0]
				sum = 0
			}
		}
		if len(tmp) > 200 { // let the little objs accumulate to a certain number
			reviseResults = append(reviseResults, reviseResult{objs: removeOversize(slices.Clone(tmp)), kind: taskHostDN, note: "end: " + note})
		}
	}

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
				if i >= 1 && IsSameSegment(objs) {
					continue
				}
				result := reviseResult{objs: objs, kind: taskHostDN, note: "overlap"}
				reviseResults = append(reviseResults, result)
			}
		} else if len(points) > 0 { // zm is inited
			viewed := make(map[*catalog.ObjectEntry]struct{})
			tmp := make([]*catalog.ObjectEntry, 0)
			for _, p := range points {
				if _, ok := viewed[p.obj]; ok {
					continue
				}
				viewed[p.obj] = struct{}{}
				tmp = append(tmp, p.obj)
			}
			checkNonoverlapObj(tmp, "nonoverlap")
		} else { // no sort key
			checkNonoverlapObj(m.leveledObjects[i], "noSortKey")
		}

		if (len(reviseResults) == 0 || (len(reviseResults) == 1 && len(reviseResults[0].objs) == 2)) && hasHundredSmallObjs(m.leveledObjects[i], m.config.MaxOsizeMergedObj/2) {
			// try merge small objs
			var oldtask reviseResult
			if len(reviseResults) == 1 {
				oldtask = reviseResults[0]
				reviseResults = reviseResults[:0]
			}
			checkNonoverlapObj(m.leveledObjects[i], "check")
			// restore the old task
			if len(reviseResults) == 0 {
				reviseResults = append(reviseResults, oldtask)
			}
		}
	}

	for i := range reviseResults {
		original := len(reviseResults[i].objs)
		for !rc.resourceAvailable(reviseResults[i].objs) && len(reviseResults[i].objs) > 1 {
			reviseResults[i].objs = reviseResults[i].objs[:len(reviseResults[i].objs)/2]
		}
		if original-len(reviseResults[i].objs) > 100 {
			tablename := "unknown"
			if len(reviseResults[i].objs) > 0 {
				tablename = fmt.Sprintf("%v-%v", m.tid, m.name)
			}
			logutil.Info("MergeExecutorEvent",
				zap.String("event", "Popout"),
				zap.String("table", tablename),
				zap.Int("original", original),
				zap.Int("revised", len(reviseResults[i].objs)),
				zap.String("createNote", string(reviseResults[i].note)),
				zap.String("avail", common.HumanReadableBytes(int(rc.availableMem()))))
		}

		if len(reviseResults[i].objs) < 2 {
			continue
		}

		if original-len(reviseResults[i].objs) > 0 && isAllGreater(reviseResults[i].objs, m.config.ObjectMinOsize) {
			// avoid zm infinited loop
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
	if entry != nil {
		m.tid = entry.ID
		m.name = entry.GetLastestSchema(false).Name
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

		if len(tmp) < overlaps {
			return res
		}

		if len(tmp) > 1 {
			res = append(res, slices.Collect(maps.Keys(tmp)))
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
		if !zm.IsInited() {
			continue
		}
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
					if bytes.Equal(copiedMin, copiedMax) {
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
