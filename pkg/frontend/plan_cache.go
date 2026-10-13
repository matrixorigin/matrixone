// Copyright 2021 Matrix Origin
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

package frontend

import (
	"container/list"

	"github.com/matrixorigin/matrixone/pkg/pb/timestamp"
	"github.com/matrixorigin/matrixone/pkg/sql/parsers/tree"
	"github.com/matrixorigin/matrixone/pkg/sql/plan"
)

type cachedPlan struct {
	sql               string
	stmts             []tree.Statement
	plans             []*plan.Plan
	planSnapshotTS    []timestamp.Timestamp
	protocolVersion   int64
	statsVersions     map[optimizerStatsTableKey]uint64
	planStatsVersions []map[optimizerStatsTableKey]uint64
	invalid           bool
}

// planCache uses LRU to cache plan for the same sql
type planCache struct {
	capacity  int
	lruList   *list.List
	cachePool map[string]*list.Element
}

func newPlanCache(capacity int) *planCache {
	return &planCache{
		capacity: capacity,
	}
}

func freeStmts(stmts []tree.Statement) {
	for i, stmt := range stmts {
		if stmt == nil {
			continue
		}
		stmt.Free()
		stmts[i] = nil
	}
}

func (pc *planCache) cache(sql string, stmts []tree.Statement, plans []*plan.Plan, versions ...int64) {
	// Legacy internal callers get a conservative oldest-possible binding. The
	// production cache path always supplies the actual generation snapshot.
	pc.cacheWithPlanSnapshotsAndStatsVersions(
		sql, stmts, plans, make([]timestamp.Timestamp, len(plans)),
		make([]map[optimizerStatsTableKey]uint64, len(plans)), versions...)
}

func (pc *planCache) cacheWithPlanSnapshotsAndStatsVersions(
	sql string,
	stmts []tree.Statement,
	plans []*plan.Plan,
	planSnapshotTS []timestamp.Timestamp,
	planStatsVersions []map[optimizerStatsTableKey]uint64,
	versions ...int64,
) {
	protocolVersion := currentProtocolVersion(nil)
	if len(versions) > 0 {
		protocolVersion = versions[0]
	}
	if pc.cachePool == nil {
		pc.cachePool = make(map[string]*list.Element)
		pc.lruList = list.New()
	}
	if len(stmts) != len(plans) || len(planSnapshotTS) != len(plans) ||
		len(planStatsVersions) != len(plans) {
		freeStmts(stmts)
		return
	}
	statsVersions, versionsConsistent := aggregatePlanStatsVersions(planStatsVersions)
	if !versionsConsistent {
		freeStmts(stmts)
		return
	}
	for i := range stmts {
		if plans[i] == nil {
			// can not cache and clean all stmts
			freeStmts(stmts)
			return
		}
	}
	if element, ok := pc.cachePool[sql]; ok {
		freeStmts(element.Value.(*cachedPlan).stmts)
		element.Value = &cachedPlan{
			sql:               sql,
			stmts:             stmts,
			plans:             plans,
			planSnapshotTS:    planSnapshotTS,
			protocolVersion:   protocolVersion,
			statsVersions:     statsVersions,
			planStatsVersions: clonePlanStatsVersions(planStatsVersions),
		}
		pc.lruList.MoveToFront(element)
		return
	}
	element := pc.lruList.PushFront(&cachedPlan{
		sql:               sql,
		stmts:             stmts,
		plans:             plans,
		planSnapshotTS:    planSnapshotTS,
		protocolVersion:   protocolVersion,
		statsVersions:     statsVersions,
		planStatsVersions: clonePlanStatsVersions(planStatsVersions),
	})
	pc.cachePool[sql] = element
	if pc.lruList.Len() > pc.capacity {
		toRemove := pc.lruList.Back()
		pc.lruList.Remove(toRemove)
		delete(pc.cachePool, toRemove.Value.(*cachedPlan).sql)
		freeStmts(toRemove.Value.(*cachedPlan).stmts)
	}
}

func cloneStatsVersions(versions map[optimizerStatsTableKey]uint64) map[optimizerStatsTableKey]uint64 {
	if len(versions) == 0 {
		return nil
	}
	cloned := make(map[optimizerStatsTableKey]uint64, len(versions))
	for key, version := range versions {
		cloned[key] = version
	}
	return cloned
}

func planStatsVersionsFromAggregate(
	planCount int,
	versions map[optimizerStatsTableKey]uint64,
) []map[optimizerStatsTableKey]uint64 {
	perPlan := make([]map[optimizerStatsTableKey]uint64, planCount)
	if planCount > 0 {
		perPlan[0] = versions
	}
	return perPlan
}

func clonePlanStatsVersions(
	versions []map[optimizerStatsTableKey]uint64,
) []map[optimizerStatsTableKey]uint64 {
	cloned := make([]map[optimizerStatsTableKey]uint64, len(versions))
	for i := range versions {
		cloned[i] = cloneStatsVersions(versions[i])
	}
	return cloned
}

func aggregatePlanStatsVersions(
	versions []map[optimizerStatsTableKey]uint64,
) (map[optimizerStatsTableKey]uint64, bool) {
	var aggregated map[optimizerStatsTableKey]uint64
	for _, planVersions := range versions {
		if len(planVersions) == 0 {
			continue
		}
		if aggregated == nil {
			aggregated = make(map[optimizerStatsTableKey]uint64)
		}
		if !mergeOptimizerStatsVersions(aggregated, planVersions) {
			return nil, false
		}
	}
	return aggregated, true
}

func mergeOptimizerStatsVersions(dst, src map[optimizerStatsTableKey]uint64) bool {
	for key, version := range src {
		if prior, exists := dst[key]; exists && prior != version {
			return false
		}
		dst[key] = version
	}
	return true
}

func (pc *planCache) remove(sql string) {
	if pc.cachePool == nil {
		return
	}
	element, ok := pc.cachePool[sql]
	if !ok {
		return
	}
	pc.lruList.Remove(element)
	delete(pc.cachePool, sql)
	freeStmts(element.Value.(*cachedPlan).stmts)
}

// get gets a cached plan by its sql
func (pc *planCache) get(sql string) *cachedPlan {
	if pc.cachePool == nil {
		return nil
	}
	if element, ok := pc.cachePool[sql]; ok {
		cp := element.Value.(*cachedPlan)
		if cp.invalid || len(cp.planSnapshotTS) != len(cp.plans) ||
			len(cp.planStatsVersions) != len(cp.plans) {
			pc.remove(sql)
			return nil
		}
		pc.lruList.MoveToFront(element)
		return cp
	}
	return nil
}

func (pc *planCache) isCached(sql string) bool {
	if pc.cachePool == nil {
		return false
	}
	element, isCached := pc.cachePool[sql]
	if !isCached {
		return false
	}
	cached := element.Value.(*cachedPlan)
	return !cached.invalid && len(cached.planSnapshotTS) == len(cached.plans) &&
		len(cached.planStatsVersions) == len(cached.plans)
}

func (pc *planCache) updatePlanGeneration(
	sql string,
	index int,
	expectedPlan *plan.Plan,
	newPlan *plan.Plan,
	planSnapshotTS timestamp.Timestamp,
	statsVersions map[optimizerStatsTableKey]uint64,
) bool {
	if pc.cachePool == nil || newPlan == nil {
		return false
	}
	element, ok := pc.cachePool[sql]
	if !ok {
		return false
	}
	cached := element.Value.(*cachedPlan)
	if cached.invalid || index < 0 || index >= len(cached.plans) ||
		len(cached.planSnapshotTS) != len(cached.plans) ||
		len(cached.planStatsVersions) != len(cached.plans) ||
		cached.plans[index] != expectedPlan {
		return false
	}
	updatedPlanStatsVersions := clonePlanStatsVersions(cached.planStatsVersions)
	updatedPlanStatsVersions[index] = cloneStatsVersions(statsVersions)
	aggregated, versionsConsistent := aggregatePlanStatsVersions(updatedPlanStatsVersions)
	if !versionsConsistent {
		return false
	}
	cached.plans[index] = newPlan
	cached.planSnapshotTS[index] = planSnapshotTS
	cached.statsVersions = aggregated
	cached.planStatsVersions = updatedPlanStatsVersions
	pc.lruList.MoveToFront(element)
	return true
}

func (pc *planCache) invalidatePlanGeneration(
	sql string,
	index int,
	expectedPlan *plan.Plan,
) {
	if pc.cachePool == nil {
		return
	}
	element, ok := pc.cachePool[sql]
	if !ok {
		return
	}
	cached := element.Value.(*cachedPlan)
	if index < 0 || index >= len(cached.plans) || cached.plans[index] != expectedPlan {
		return
	}
	// Do not remove the entry here: its AST can still be borrowed by the
	// wrapper that discovered the stale generation. The next lookup removes it
	// after that wrapper has completed its lifecycle.
	cached.invalid = true
}

func (pc *planCache) clean() {
	if pc.lruList == nil {
		pc.cachePool = nil
		return
	}
	for pc.lruList.Len() > 0 {
		toRemove := pc.lruList.Front()
		pc.lruList.Remove(toRemove)
		delete(pc.cachePool, toRemove.Value.(*cachedPlan).sql)
		freeStmts(toRemove.Value.(*cachedPlan).stmts)
	}
	pc.lruList = nil
	pc.cachePool = nil
}
