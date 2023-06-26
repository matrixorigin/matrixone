// Copyright 2021 - 2023 Matrix Origin
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

package cache

import (
	"fmt"
	"sync"

	"github.com/matrixorigin/matrixone/pkg/pb/timestamp"
)

const (
	// defaultDeletedTableSize is the default size of deleted table slice.
	defaultDeletedTableSize = 256
)

// tableGuard helps to track the deleted tables. It is global unique and is
// kept in CatalogCache, which is kept in disttae.Engine.
// Every transaction keeps a table map internally and an index of deleteTables.
// If a table is dropped by another transaction, the transaction should update
// its table map.
type tableGuard struct {
	mu struct {
		sync.RWMutex
		// If a table is dropped, it is pushed into this slice.
		deletedTables []*TableItem
		// The number of deleted tables which are garbage collected.
		gcCount int
	}
}

// newTableGuard creates a new tableGuard instance.
func newTableGuard() *tableGuard {
	t := &tableGuard{}
	t.mu.deletedTables = make([]*TableItem, 0, defaultDeletedTableSize)
	return t
}

// pushDeletedTable pushes a dropped table item.
func (g *tableGuard) pushDeletedTable(item *TableItem) {
	g.mu.Lock()
	defer g.mu.Unlock()
	if !item.deleted {
		return
	}
	g.mu.deletedTables = append(g.mu.deletedTables, item)
}

// getDeletedTables returns the deleted tables which are in range [cachedIndex:] and the
// table's timestamp is less than ts. The cachedIndex is the index for the original slice.
func (g *tableGuard) getDeletedTables(cachedIndex int, ts timestamp.Timestamp) []*TableItem {
	g.mu.RLock()

	// We get the tables from the next of cached index, so move the index to next.
	// The deleted tables may be garbage collected, and the number is gcCount.
	// The cachedIndex value is the index to the original slice which is created
	// at the beginning. The "begin" value is the index to the current slice after
	// calculating.
	begin := cachedIndex + 1 - g.mu.gcCount

	// If "begin" is less than 0, the GC job is not working properly.
	if begin < 0 {
		g.mu.RUnlock()
		panic(fmt.Sprintf("Cannot get the correct index, please check the GC job. "+
			"The cached index is %d, gc count is %d", cachedIndex, g.mu.gcCount))
	}

	if begin >= len(g.mu.deletedTables) {
		g.mu.RUnlock()
		return nil
	}

	items := make([]*TableItem, 0, len(g.mu.deletedTables[begin:]))
	for _, t := range g.mu.deletedTables[begin:] {
		if t.Ts.Less(ts) {
			items = append(items, t)
		}
	}
	g.mu.RUnlock()
	return items
}

// getDeletedTableIndex returns the max index. If the slice is empty, returns -1.
// There is no need to get the index by timestamp, because the transaction must
// happen after the logtail which contains the table dropping.
func (g *tableGuard) getDeletedTableIndex() int {
	g.mu.RLock()
	defer g.mu.RUnlock()
	return len(g.mu.deletedTables) - 1 + g.mu.gcCount
}

// GC do the garbage collection job for deletedTables slice.
// It gets the first item whose timestamp is not less than ts,
// then delete item from the first one to that item. This will
// cause some items are not removed because the items in the
// slice are not sorted by timestamp, but it is OK because it
// is just GC. They will be removed the next time.
func (g *tableGuard) GC(ts timestamp.Timestamp) {
	gcIndex := -1
	g.mu.RLock()
	for idx, item := range g.mu.deletedTables {
		if item.Ts.GreaterEq(ts) {
			break
		}
		gcIndex = idx
	}
	g.mu.RUnlock()
	if gcIndex < 0 {
		return
	}
	gcIndex++
	g.mu.Lock()
	defer g.mu.Unlock()
	g.mu.deletedTables = g.mu.deletedTables[gcIndex:]
	g.mu.gcCount += gcIndex
}
