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
	"sync"

	"github.com/matrixorigin/matrixone/pkg/pb/timestamp"
)

// const (
// 	// defaultDeletedTableSize is the default size of deleted table slice.
// 	defaultDeletedTableSize = 256
// )

// tableGuard helps to track the deleted tables. It is global unique and is
// kept in CatalogCache, which is kept in disttae.Engine.
// Every transaction keeps a table map internally and an index of deleteTables.
// If a table is dropped by another transaction, the transaction should update
// its table map.
type tableGuard struct {
	mu struct {
		sync.RWMutex
		schema_version map[TableKey]*TableVersion
	}
}

// newTableGuard creates a new tableGuard instance.
func newTableGuard() *tableGuard {
	t := &tableGuard{}
	t.mu.schema_version = make(map[TableKey]*TableVersion)
	return t
}

// setSchemaVersion
func (g *tableGuard) setSchemaVersion(name TableKey, val *TableVersion) {
	g.mu.Lock()
	defer g.mu.Unlock()
	g.mu.schema_version[name] = val
}

// getSchemaVersion
func (g *tableGuard) getSchemaVersion(name TableKey) *TableVersion {
	g.mu.Lock()
	defer g.mu.Unlock()
	return g.mu.schema_version[name]
}

// GC do the garbage collection job for deletedTables slice.
// It gets the first item whose timestamp is not less than ts,
// then delete item from the first one to that item. This will
// cause some items are not removed because the items in the
// slice are not sorted by timestamp, but it is OK because it
// is just GC. They will be removed the next time.
func (g *tableGuard) GC(ts timestamp.Timestamp) {
	g.mu.Lock()
	defer g.mu.Unlock()
	g.mu.schema_version = make(map[TableKey]*TableVersion)
}
