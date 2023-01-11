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

	"github.com/matrixorigin/matrixone/pkg/sql/parsers/tree"
	"github.com/matrixorigin/matrixone/pkg/sql/plan"
)

type cachedPlan struct {
	sql   string
	stmts []*tree.Statement
	plans []*plan.Plan
}

// planCache uses LRU to cache plan for the same sql
type planCache struct {
	capacity  int
	lruList   *list.List
	cachePool map[string]*list.Element
}

func newPlanCache(capacity int) *planCache {
	return &planCache{
		capacity:  capacity,
		lruList:   list.New(),
		cachePool: make(map[string]*list.Element),
	}
}

func (pc *planCache) cache(sql string, stmts []*tree.Statement, plans []*plan.Plan) {
	element := pc.lruList.PushFront(&cachedPlan{sql: sql, stmts: stmts, plans: plans})
	pc.cachePool[sql] = element
	if pc.lruList.Len() > pc.capacity {
		toRemove := pc.lruList.Back()
		pc.lruList.Remove(toRemove)
		delete(pc.cachePool, toRemove.Value.(*cachedPlan).sql)
	}
}

// get gets a cached plan by its sql
func (pc *planCache) get(sql string) *cachedPlan {
	if element, ok := pc.cachePool[sql]; ok {
		pc.lruList.MoveToFront(element)
		cp := element.Value.(*cachedPlan)
		return cp
	}
	return nil
}

func (pc *planCache) isCached(sql string) bool {
	_, isCached := pc.cachePool[sql]
	return isCached
}

func (pc *planCache) clean() {
	pc.lruList = list.New()
	pc.cachePool = make(map[string]*list.Element)
}
