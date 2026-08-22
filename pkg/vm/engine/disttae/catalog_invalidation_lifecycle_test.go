// Copyright 2026 Matrix Origin
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

package disttae

import (
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"github.com/matrixorigin/matrixone/pkg/vm/engine/disttae/cache"
)

func TestFinishCatalogReloadTerminalOutcomesExactlyOnce(t *testing.T) {
	catalogCache := cache.NewCatalog()
	catalogCache.EnableCatalogInvalidationAttribution()
	engine := &Engine{}
	engine.catalog.Store(catalogCache)
	txn := &Transaction{engine: engine}
	pending := &sync.Map{}
	txn.catalogInvalidations.Store(pending)

	outcomes := []cache.CatalogInvalidationOutcome{
		cache.CatalogInvalidationSuccess,
		cache.CatalogInvalidationError,
		cache.CatalogInvalidationMiss,
	}
	for index, outcome := range outcomes {
		key := tableKey{accountId: 1, databaseId: uint64(index + 1), dbName: "db", name: "t"}
		pending.Store(key, time.Now())
		txn.finishCatalogReload(key, outcome)
		txn.finishCatalogReload(key, outcome)
	}

	report := catalogCache.SnapshotCatalogInvalidationReport()
	require.Equal(t, uint64(3), report.RCTableCacheReload.Count)
	require.Equal(t, uint64(1), report.RCTableCacheReload.Success)
	require.Equal(t, uint64(1), report.RCTableCacheReload.Error)
	require.Equal(t, uint64(1), report.RCTableCacheReload.Miss)
}

func TestFinishCatalogReloadConcurrentTerminalIsExactlyOnce(t *testing.T) {
	catalogCache := cache.NewCatalog()
	catalogCache.EnableCatalogInvalidationAttribution()
	engine := &Engine{}
	engine.catalog.Store(catalogCache)
	txn := &Transaction{engine: engine}
	pending := &sync.Map{}
	txn.catalogInvalidations.Store(pending)
	key := tableKey{accountId: 1, databaseId: 1, dbName: "db", name: "t"}
	pending.Store(key, time.Now())

	var wg sync.WaitGroup
	for range 2 {
		wg.Add(1)
		go func() {
			defer wg.Done()
			txn.finishCatalogReload(key, cache.CatalogInvalidationSuccess)
		}()
	}
	wg.Wait()

	require.Equal(t, uint64(1), catalogCache.SnapshotCatalogInvalidationReport().RCTableCacheReload.Count)
}
