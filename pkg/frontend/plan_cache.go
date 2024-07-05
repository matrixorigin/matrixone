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
	"crypto/sha256"
	"encoding/hex"
	"github.com/matrixorigin/matrixone/pkg/sql/parsers/tree"
	"github.com/matrixorigin/matrixone/pkg/sql/plan"
)

type cachedPlan struct {
	sql   string
	stmts []tree.Statement
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
		capacity: capacity,
	}
}

func (pc *planCache) cache(sql string, stmts []tree.Statement, plans []*plan.Plan) {
	if pc.cachePool == nil {
		pc.cachePool = make(map[string]*list.Element)
		pc.lruList = list.New()
	}
	for i := range stmts {
		if plans[i] == nil {
			// can not cache and clean all stmts
			for _, stmt := range stmts {
				if stmt != nil {
					stmt.Free()
				}
			}
			return
		}
	}
	sql256 := pc.hashString(sql)
	element := pc.lruList.PushFront(&cachedPlan{sql: sql256, stmts: stmts, plans: plans})
	pc.cachePool[sql256] = element
	if pc.lruList.Len() > pc.capacity {
		toRemove := pc.lruList.Back()
		toRemoveStmts := toRemove.Value.(*cachedPlan).stmts
		for _, stmt := range toRemoveStmts {
			stmt.Free()
		}

		pc.lruList.Remove(toRemove)
		delete(pc.cachePool, toRemove.Value.(*cachedPlan).sql)
	}
}

// get gets a cached plan by its sql
func (pc *planCache) get(sql string) *cachedPlan {
	if pc.cachePool == nil {
		return nil
	}
	sql256 := pc.hashString(sql)
	if element, ok := pc.cachePool[sql256]; ok {
		pc.lruList.MoveToFront(element)
		cp := element.Value.(*cachedPlan)
		return cp
	}
	return nil
}

func (pc *planCache) isCached(sql string) bool {
	if pc.cachePool == nil {
		return false
	}
	sql256 := pc.hashString(sql)
	_, isCached := pc.cachePool[sql256]
	return isCached
}

func (pc *planCache) clean() {
	if pc.lruList != nil {
		for i := 0; i < pc.lruList.Len(); i++ {
			toRemove := pc.lruList.Front()
			toRemoveStmts := toRemove.Value.(*cachedPlan).stmts
			for _, stmt := range toRemoveStmts {
				stmt.Free()
			}
		}
	}
	pc.lruList = nil
	pc.cachePool = nil
}

func (pc *planCache) hashString(sql string) string {
	hash := sha256.New()
	hash.Write([]byte(sql))
	return hex.EncodeToString(hash.Sum(nil))
}
