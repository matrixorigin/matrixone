// Copyright 2024 Matrix Origin
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
	"context"
	"fmt"
	"sort"

	"github.com/matrixorigin/matrixone/pkg/common/moerr"
	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/objectio"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/common"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/compute"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/index"
	"github.com/openacid/slimarray/polyfit"
	"github.com/tidwall/btree"
)

var (
	DefaultOverlapOpts = &OverlapOpts{
		MinPointDepthPerCluster: 3,
		FitPolynomialDegree:     0,
		NoFurtherStat:           true,
	}
)

const (
	MAX_LV_COUNT = 8
	MAX_LV       = MAX_LV_COUNT - 1
)

// |---------|
// |---------|
//	|---------------| --- object A
//	             |---------|
//	            |---------|
//
// objectA overlap = 4
// objectA depth = 3
// we can see that generally, the overlap is greater or equal to the depth
//   if the overlap is at the same level as the depth, it indicates objects stack intensively at one point
//   if the overlap is greater than the depth, it indicates objects stack broadly at multiple points
// This enlightens us that if overlap >> depth, we'd better to gather multiple tasks
//   but for now, we don't collect multiple tasks with respect the caqse that overlap >> depth.
//   conservatively, we only collect multiple tasks when we find disjoint clusters, these clusters are not overlapped at all.

type objOverlapRecord struct {
	overlap int
	depth   int
}

type pointEvent struct {
	open  []int  // indices of statsList, the `val` opens these objects
	close []int  // indices of statsList, the `val` closes these objects
	val   []byte // zm map value
}

func (p *pointEvent) String() string {
	return fmt.Sprintf("%x open: %v, close: %v", p.val, p.open, p.close)
}

type clusterInfo struct {
	idx        int
	pointDepth int
}

type OverlapStats struct {
	AvgPointDepth          float64
	AvgOverlapCnt          float64
	Clusters               []clusterInfo
	ConstantObj            int
	UniniteddObj           int
	ScanObj                int
	PolynomialCoefficients []float64

	// intermediate results
	PointEvents  *btree.BTreeG[*pointEvent]
	StatsRecords []objOverlapRecord
}

func (o *OverlapStats) String() string {
	pointsCnt := 0
	if o.PointEvents != nil {
		pointsCnt = o.PointEvents.Len()
	}
	coffs := ""
	if len(o.PolynomialCoefficients) > 0 {
		processed := o.ScanObj - o.ConstantObj - o.UniniteddObj
		coffs = fmt.Sprintf("%v-%d", o.PolynomialCoefficients, processed)
	}
	s := fmt.Sprintf("AvgPointDepth: %.2f, AvgOverlapCnt: %.2f, "+
		"Obj(s,c,u): %d-%d-%d, EventsCnt: %d, Clusters: %v",
		o.AvgPointDepth,
		o.AvgOverlapCnt,
		o.ScanObj,
		o.ConstantObj,
		o.UniniteddObj,
		pointsCnt,
		o.Clusters,
	)
	if coffs != "" {
		s += fmt.Sprintf(", PolynomialCoefficients: %s", coffs)
	}
	return s
}

// GetScaleAndType returns the scale and type of the statsList,
// the first valid scale and type will be returned
func GetScaleAndType(statsList []*objectio.ObjectStats) (scale int32, typ types.T) {
	for _, stats := range statsList {
		zm := stats.SortKeyZoneMap()
		if zm == nil || !zm.IsInited() {
			continue
		}
		scale = zm.GetScale()
		typ = zm.GetType()
		break
	}
	return
}

func MakePointEventsSortedMap(
	ctx context.Context,
	statsList []*objectio.ObjectStats,
) (*btree.BTreeG[*pointEvent], int, int, error) {

	scale, typ := GetScaleAndType(statsList)
	if typ == types.T_any {
		return nil, 0, 0, moerr.NewInternalError(ctx, "no valid intervals with initialized zonemaps")
	}

	events := btree.NewBTreeG(func(a *pointEvent, b *pointEvent) bool {
		return compute.Compare(a.val, b.val, typ, scale, scale) < 0
	})

	constants := 0
	uninitialized := 0

	key := &pointEvent{val: []byte{}}
	for i, stats := range statsList {
		zm := stats.SortKeyZoneMap()
		if zm == nil || !zm.IsInited() {
			uninitialized++
			continue
		}

		if IsConstantObj(stats) {
			constants++
			continue
		}

		key.val = zm.GetMinBuf()
		if event, ok := events.Get(key); ok {
			event.open = append(event.open, i)
		} else {
			events.Set(&pointEvent{open: []int{i}, val: key.val})
		}
		key.val = zm.GetMaxBuf()
		if event, ok := events.Get(key); ok {
			event.close = append(event.close, i)
		} else {
			events.Set(&pointEvent{close: []int{i}, val: key.val})
		}
	}
	return events, constants, uninitialized, nil
}

func IsConstantObj(stats *objectio.ObjectStats) bool {
	if stats.OriginSize() < common.DefaultMinOsizeQualifiedBytes {
		return false
	}
	return IsConstantZM(stats.SortKeyZoneMap())
}

func IsConstantZM(zm index.ZM) bool {
	if zm == nil || !zm.IsInited() {
		return false
	}
	// the max is 0xfffffffff..., which has no valuable information,
	// so we treat it as constant and it is not a good candidate for merge
	if zm.MaxTruncated() {
		return true
	}

	minBuf, maxBuf := zm.GetMinBuf(), zm.GetMaxBuf()
	if zm.IsString() && len(minBuf) == 30 && len(maxBuf) == 30 {
		var diffi int
		for diffi = 0; diffi < 30; diffi++ {
			if minBuf[diffi] != maxBuf[diffi] {
				break
			}
		}
		if diffi == 30 {
			return true
		}

		// if maxBuf = minBuf + 1, we also treat it as constant object
		// If the logic of bumping one is removed,
		// the following code should be removed
		for i := 29; i >= 0; i-- {
			x := minBuf[i] + 1
			if x != 0 {
				return i == diffi && x == maxBuf[i]
			}
			if maxBuf[i] != 0 {
				return false
			}
		}
	}

	// for those non-string types, or string without any truncation,
	// if the min and max are the same,
	// it is a constant object and not a good candidate for merge
	return compute.Compare(
		zm.GetMinBuf(), zm.GetMaxBuf(),
		zm.GetType(), zm.GetScale(), zm.GetScale(),
	) == 0
}

type OverlapOpts struct {
	NoFurtherStat           bool // set by inner coding
	MinPointDepthPerCluster int
	FitPolynomialDegree     int // 0 means no polynomial fitting, max is 5
}

func (o *OverlapOpts) String() string {
	return fmt.Sprintf("OverlapOpts{MinPD: %d, Degree: %d, NoStat: %t}",
		o.MinPointDepthPerCluster, o.FitPolynomialDegree, o.NoFurtherStat)
}

func (o *OverlapOpts) Clone() *OverlapOpts {
	return &OverlapOpts{
		MinPointDepthPerCluster: o.MinPointDepthPerCluster,
		FitPolynomialDegree:     o.FitPolynomialDegree,
		NoFurtherStat:           o.NoFurtherStat,
	}
}

func NewOverlapOptions() *OverlapOpts {
	return DefaultOverlapOpts.Clone()
}

func (o *OverlapOpts) WithMinPointDepthPerCluster(minPointDepthPerCluster int) *OverlapOpts {
	o.MinPointDepthPerCluster = minPointDepthPerCluster
	return o
}

func (o *OverlapOpts) WithFitPolynomialDegree(fitPolynomialDegree int) *OverlapOpts {
	o.FitPolynomialDegree = fitPolynomialDegree
	return o
}

func (o *OverlapOpts) WithFurtherStat(furtherStat bool) *OverlapOpts {
	o.NoFurtherStat = !furtherStat
	return o
}

// CalculateOverlapStats calculates average overlap depth and average overlap count
func CalculateOverlapStats(
	ctx context.Context,
	statsList []*objectio.ObjectStats,
	opts *OverlapOpts,
) (ret *OverlapStats, err error) {
	if len(statsList) == 0 {
		return nil, nil
	}

	ret = &OverlapStats{}
	events, constantObjCnt, uninitialized, err := MakePointEventsSortedMap(ctx, statsList)
	if err != nil {
		return nil, err
	}
	ret.ConstantObj = constantObjCnt
	ret.UniniteddObj = uninitialized
	ret.ScanObj = len(statsList)
	ret.PointEvents = events
	ret.StatsRecords = make([]objOverlapRecord, len(statsList))

	openingObjs := make(map[int]objOverlapRecord)
	openAndCloseInSamePoint := make([]int, 0)

	idx := -1
	maxpd := 0
	maxidx := 0
	iter := events.Iter()
	defer iter.Release()
	for iter.Next() {
		idx++
		event := iter.Item()

		// remove the closing objects from openingObjs first,
		// avoiding the impact of point stack on point depth.
		//   point stack: a value point is responsible for multiple objects' opening and closing.
		for _, idx := range event.close {
			record, exists := openingObjs[idx]
			if exists {
				ret.StatsRecords[idx] = record
				delete(openingObjs, idx)
			} else {
				openAndCloseInSamePoint = append(openAndCloseInSamePoint, idx)
			}
		}

		// if this point event does not open any objects,
		// it won't upate any existing information
		// and the openAndCloseInSamePoint MUST be empty, no need to check
		if len(event.open) == 0 {
			continue
		}

		// if there are no opening objects, we can determine a cluster
		if len(openingObjs) == 0 {
			if maxpd >= opts.MinPointDepthPerCluster {
				ret.Clusters = append(ret.Clusters,
					clusterInfo{idx: maxidx, pointDepth: maxpd},
				)
			}
			maxpd = 0
		}

		// for the current value point, the pointDepth is the number of objects
		// that are unclosed plus the number of new opening objects
		pointDepth := len(event.open) + len(openingObjs)

		// update the max point depth and index
		if pointDepth > maxpd {
			maxpd = pointDepth
			maxidx = idx
		}

		for key, record := range openingObjs {
			newRecord := objOverlapRecord{
				// new objects are added to the overlap
				overlap: record.overlap + len(event.open),
				// the depth is the max value considering all points contained in the object
				depth: max(record.depth, pointDepth),
			}
			openingObjs[key] = newRecord
		}

		// add new objects to openingObjs
		for _, idx := range event.open {
			// overlap = pointDepth - 1, self is not included
			openingObjs[idx] = objOverlapRecord{
				overlap: pointDepth - 1,
				depth:   pointDepth,
			}
		}

		// handle special case: open and close in the same point
		for _, idx := range openAndCloseInSamePoint {
			record := openingObjs[idx]
			ret.StatsRecords[idx] = record
			delete(openingObjs, idx)
		}
		if len(openAndCloseInSamePoint) > 0 {
			openAndCloseInSamePoint = openAndCloseInSamePoint[:0]
		}
	}

	// handle the last cluster
	if maxpd >= opts.MinPointDepthPerCluster {
		ret.Clusters = append(ret.Clusters,
			clusterInfo{idx: maxidx, pointDepth: maxpd},
		)
	}

	if len(openingObjs) > 0 {
		err = moerr.NewInternalError(ctx, "some objects are not closed")
		return
	}

	if opts.NoFurtherStat {
		return
	}

	processed := ret.ScanObj - ret.ConstantObj - ret.UniniteddObj
	totalOverlap := 0
	totalPointDepth := 0
	for _, record := range ret.StatsRecords {
		totalOverlap += record.overlap
		totalPointDepth += record.depth
	}
	ret.AvgPointDepth = float64(totalPointDepth) / float64(processed)
	ret.AvgOverlapCnt = float64(totalOverlap) / float64(processed)

	// Skip polynomial fitting if we don't have enough data points
	if opts.FitPolynomialDegree > 0 && len(ret.StatsRecords) > opts.FitPolynomialDegree {
		pointDepths := make([]float64, len(ret.StatsRecords))
		xValues := make([]float64, len(ret.StatsRecords))
		for i, record := range ret.StatsRecords {
			pointDepths[i] = float64(record.depth)
			xValues[i] = float64(i)
		}
		p := polyfit.NewFit(xValues, pointDepths, opts.FitPolynomialDegree)
		coffs := p.Solve()
		ret.PolynomialCoefficients = coffs
	}

	return
}

func splitTasksOnSpan(
	statsList []*objectio.ObjectStats,
	candidatesIdx []int,
	opts *OverlapOpts,
	overlapStats *OverlapStats,
	level int8,
) (tasks []mergeTask) {
	records := overlapStats.StatsRecords

	// sort by overlap ratio descending
	sort.Slice(candidatesIdx, func(i, j int) bool {
		r1, r2 := records[candidatesIdx[i]], records[candidatesIdx[j]]
		overlapRatio1 := float64(r1.overlap) / float64(r1.depth)
		overlapRatio2 := float64(r2.overlap) / float64(r2.depth)
		return overlapRatio1 > overlapRatio2
	})

	pushToRationLessThan := func(idx int, ration float64) int {
		for ; idx < len(candidatesIdx); idx++ {
			record := records[candidatesIdx[idx]]
			spanRatio := float64(record.overlap) / float64(record.depth)
			if spanRatio < ration {
				break
			}
		}
		return idx
	}
	notesFmt := []string{
		"pointDepth %v >= %v, wide span",
		"pointDepth %v >= %v, medium span",
		"pointDepth %v >= %v, narrow span",
	}
	start := 0
	pos := 0
	for i, ration := range []float64{10.0, 2.0, -1.0} {
		pos = pushToRationLessThan(pos, ration)
		if cnt := pos - start; cnt >= opts.MinPointDepthPerCluster {
			stats := make([]*objectio.ObjectStats, 0, cnt)
			for k := start; k < pos; k++ {
				stats = append(stats, statsList[candidatesIdx[k]])
			}

			note := fmt.Sprintf(
				notesFmt[i], cnt, opts.MinPointDepthPerCluster,
			)

			lv := level
			// for medium & wide span, try to keep them in the current lv
			if i < 2 && lv >= 2 {
				lv -= 1
				note += "(keep in current level)"
			}
			tasks = append(tasks, mergeTask{
				objs:        stats,
				note:        note,
				level:       lv,
				kind:        taskHostDN,
				isTombstone: false,
			})
			// wider span take higher priority to be merged,
			// because they are about to produce thinner span.
			break
		}
		start = pos
	}

	return
}

func GatherOverlapMergeTasks(
	ctx context.Context,
	statsList []*objectio.ObjectStats,
	overlapOpts *OverlapOpts,
	level int8,
) ([]mergeTask, error) {
	if len(statsList) == 0 {
		return nil, nil
	}
	opts := overlapOpts
	if !opts.NoFurtherStat {
		opts = overlapOpts.Clone().WithFurtherStat(false)
	}
	overlapStats, err := CalculateOverlapStats(ctx, statsList, opts)
	if err != nil {
		return nil, err
	}
	events := overlapStats.PointEvents
	openingObjs := make(map[int]*objectio.ObjectStats)
	iter := events.Iter()
	defer iter.Release()
	idx := -1

	groupidx := 0
	group := len(overlapStats.Clusters)
	targetedStats := make([]mergeTask, 0, group)
	closeAndOpenInSamePoint := make([]int, 0)
	for iter.Next() {
		idx++
		event := iter.Item()
		for _, idx := range event.close {
			if _, ok := openingObjs[idx]; ok {
				delete(openingObjs, idx)
			} else {
				closeAndOpenInSamePoint = append(closeAndOpenInSamePoint, idx)
			}
		}
		for _, idx := range event.open {
			openingObjs[idx] = statsList[idx]
		}

		pd := len(openingObjs)

		if groupidx < group &&
			pd == overlapStats.Clusters[groupidx].pointDepth &&
			idx == overlapStats.Clusters[groupidx].idx {
			keys := make([]int, 0, len(openingObjs))
			for k := range openingObjs {
				keys = append(keys, k)
			}
			targetedStats = append(targetedStats, splitTasksOnSpan(
				statsList,
				keys,
				overlapOpts,
				overlapStats,
				level,
			)...)
			groupidx++
		}

		for _, idx := range closeAndOpenInSamePoint {
			delete(openingObjs, idx)
		}
		if len(closeAndOpenInSamePoint) > 0 {
			closeAndOpenInSamePoint = closeAndOpenInSamePoint[:0]
		}
	}
	return targetedStats, nil
}
