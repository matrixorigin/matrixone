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

package cache

import (
	"fmt"
	"runtime"
	"sync"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/pb/timestamp"
)

// legacyHasNewerVersionForTest mirrors the exact lookup before the probe pool
// change. It is intentionally kept in the test package so the benchmark can
// compare the production path with its pre-change allocation behavior.
func legacyHasNewerVersionForTest(cc *CatalogCache, qry *TableChangeQuery) bool {
	var find bool
	if qry.DatabaseName != "" {
		key := &DatabaseItem{
			AccountId: qry.AccountId,
			Name:      qry.DatabaseName,
			Ts:        types.MaxTs().ToTimestamp(),
		}
		cc.databases.data.Ascend(key, func(item *DatabaseItem) bool {
			if item.AccountId != qry.AccountId || item.Name != qry.DatabaseName {
				return false
			}
			if item.Ts.Greater(qry.Ts) && (item.deleted || item.Id != qry.DatabaseId) {
				find = true
			}
			return false
		})
		if find {
			return true
		}
	}
	if qry.Name == "" {
		if qry.DatabaseId == 0 {
			cc.tableChange.RLock()
			latest := cc.tableChange.byAccount[tableChangeBucket(qry.AccountId)]
			cc.tableChange.RUnlock()
			return latest.Greater(qry.Ts)
		}
		return false
	}

	key := &TableItem{
		AccountId:  qry.AccountId,
		DatabaseId: qry.DatabaseId,
		Name:       qry.Name,
		Ts:         types.MaxTs().ToTimestamp(),
	}
	cc.tables.data.Ascend(key, func(item *TableItem) bool {
		if item.AccountId != qry.AccountId || item.DatabaseId != qry.DatabaseId || item.Name != qry.Name {
			return false
		}
		if item.Ts.Greater(qry.Ts) {
			if item.deleted || item.Id != qry.TableId || item.Version > qry.Version {
				find = true
			}
		}
		return false
	})
	return find
}

func newProbeOracleCatalog() *CatalogCache {
	cc := NewCatalog()
	cc.databases.data.Set(&DatabaseItem{
		AccountId: 1,
		Name:      "db",
		Id:        11,
		Ts:        timestamp.Timestamp{PhysicalTime: 200},
	})
	cc.databases.data.Set(&DatabaseItem{
		AccountId: 2,
		Name:      "db",
		Id:        21,
		Ts:        timestamp.Timestamp{PhysicalTime: 300},
	})
	cc.setTableItem(&TableItem{
		AccountId:  1,
		DatabaseId: 11,
		Name:       "t",
		Id:         101,
		Version:    3,
		Ts:         timestamp.Timestamp{PhysicalTime: 200},
	}, true)
	cc.setTableItem(&TableItem{
		AccountId:  1,
		DatabaseId: 12,
		Name:       "other",
		Id:         102,
		Version:    1,
		Ts:         timestamp.Timestamp{PhysicalTime: 250},
	}, true)
	cc.setTableItem(&TableItem{
		AccountId:  2,
		DatabaseId: 21,
		Name:       "t",
		Id:         201,
		Version:    2,
		Ts:         timestamp.Timestamp{PhysicalTime: 300},
	}, true)
	return cc
}

func TestHasNewerVersionMatchesLegacyExactOracle(t *testing.T) {
	cc := newProbeOracleCatalog()
	queries := []TableChangeQuery{
		{AccountId: 1, DatabaseId: 11, DatabaseName: "db", Name: "t", TableId: 101, Version: 3, Ts: timestamp.Timestamp{PhysicalTime: 200}},
		{AccountId: 1, DatabaseId: 10, DatabaseName: "db", Name: "t", TableId: 101, Version: 3, Ts: timestamp.Timestamp{PhysicalTime: 100}},
		{AccountId: 1, DatabaseId: 11, Name: "t", TableId: 101, Version: 2, Ts: timestamp.Timestamp{PhysicalTime: 100}},
		{AccountId: 1, DatabaseId: 11, Name: "t", TableId: 101, Version: 3, Ts: timestamp.Timestamp{PhysicalTime: 250}},
		{AccountId: 1, DatabaseId: 12, Name: "other", TableId: 102, Version: 1, Ts: timestamp.Timestamp{PhysicalTime: 200}},
		{AccountId: 1, DatabaseId: 0, Ts: timestamp.Timestamp{PhysicalTime: 100}},
		{AccountId: 2, DatabaseId: 0, Ts: timestamp.Timestamp{PhysicalTime: 100}},
		{AccountId: 1, DatabaseId: 11, Ts: timestamp.Timestamp{PhysicalTime: 100}},
		{AccountId: 3, DatabaseId: 0, Ts: timestamp.Timestamp{PhysicalTime: 100}},
	}

	for i := range queries {
		query := queries[i]
		want := legacyHasNewerVersionForTest(cc, &query)
		got := cc.HasNewerVersion(&query)
		require.Equal(t, want, got, "query %d: %+v", i, query)
	}

	// A fixed, deterministic sequence exercises identity, timestamp, and
	// account-bucket boundaries without relying on timing or random retries.
	seed := uint64(0x27235)
	for i := 0; i < 256; i++ {
		seed = seed*6364136223846793005 + 1
		query := TableChangeQuery{
			AccountId:  uint32(seed % 4),
			DatabaseId: (seed >> 8) % 32,
			TableId:    (seed >> 16) % 256,
			Version:    uint32(seed >> 24),
			Ts:         timestamp.Timestamp{PhysicalTime: int64(seed % 400)},
		}
		if seed&1 == 0 {
			query.Name = "t"
		} else if seed&2 == 0 {
			query.Name = "other"
		}
		if seed&4 != 0 {
			query.DatabaseName = "db"
		}
		want := legacyHasNewerVersionForTest(cc, &query)
		got := cc.HasNewerVersion(&query)
		require.Equal(t, want, got, "seeded query %d: %+v", i, query)
	}
}

func TestHasNewerVersionMatchesLegacyAfterMutationReplayAndGC(t *testing.T) {
	cc := NewCatalog()
	query := &TableChangeQuery{
		AccountId:  1,
		DatabaseId: 10,
		Name:       "t",
		TableId:    100,
		Version:    1,
	}

	check := func(step string) {
		t.Helper()
		want := legacyHasNewerVersionForTest(cc, query)
		require.Equal(t, want, cc.HasNewerVersion(query), step)
	}

	// These mutations model the replay order for create, alter, truncate,
	// drop/recreate, and the later GC pass. Every step uses the same exact
	// oracle as the pre-pool implementation.
	cc.setTableItem(&TableItem{
		AccountId: 1, DatabaseId: 10, Name: "t", Id: 100, Version: 1,
		Ts: timestamp.Timestamp{PhysicalTime: 10},
	}, true)
	check("create")
	cc.setTableItem(&TableItem{
		AccountId: 1, DatabaseId: 10, Name: "t", Id: 100, Version: 2,
		Ts: timestamp.Timestamp{PhysicalTime: 20},
	}, true)
	check("alter")
	cc.setTableItem(&TableItem{
		AccountId: 1, DatabaseId: 10, Name: "t", Id: 100, Version: 3,
		Ts: timestamp.Timestamp{PhysicalTime: 25},
	}, true)
	check("truncate")
	cc.setTableItem(&TableItem{
		AccountId: 1, DatabaseId: 10, Name: "t", Id: 100, Version: 3,
		deleted: true,
		Ts:      timestamp.Timestamp{PhysicalTime: 30},
	}, false)
	check("drop")
	cc.setTableItem(&TableItem{
		AccountId: 1, DatabaseId: 10, Name: "t", Id: 101, Version: 1,
		Ts: timestamp.Timestamp{PhysicalTime: 40},
	}, true)
	check("recreate")
	cc.GC(timestamp.Timestamp{PhysicalTime: 25})
	check("gc")
}

func TestHasNewerVersionTableHasNoSteadyStateAllocations(t *testing.T) {
	negativeCatalog, negativeQuery := newProbeBenchmarkCatalog(16, false, false)
	positiveCatalog, positiveQuery := newProbeBenchmarkCatalog(16, false, true)

	negativeCatalog.HasNewerVersion(negativeQuery)
	positiveCatalog.HasNewerVersion(positiveQuery)

	var got bool
	negativeAllocs := testing.AllocsPerRun(100, func() {
		got = negativeCatalog.HasNewerVersion(negativeQuery)
	})
	require.False(t, got)
	require.Zero(t, negativeAllocs)

	positiveAllocs := testing.AllocsPerRun(100, func() {
		got = positiveCatalog.HasNewerVersion(positiveQuery)
	})
	require.True(t, got)
	require.Zero(t, positiveAllocs)

	coldCatalog, coldQuery := newProbeBenchmarkCatalog(16, false, false)
	coldAllocs := testing.AllocsPerRun(1, func() {
		got = coldCatalog.HasNewerVersion(coldQuery)
	})
	runtime.GC()
	postGCAllocs := testing.AllocsPerRun(1, func() {
		got = coldCatalog.HasNewerVersion(coldQuery)
	})
	t.Logf("table cold allocs=%v post-GC allocs=%v", coldAllocs, postGCAllocs)
}

func TestHasNewerVersionDatabaseHasNoSteadyStateAllocations(t *testing.T) {
	negativeCatalog, negativeQuery := newProbeBenchmarkCatalog(16, true, false)
	positiveCatalog, positiveQuery := newProbeBenchmarkCatalog(16, true, true)

	negativeCatalog.HasNewerVersion(negativeQuery)
	positiveCatalog.HasNewerVersion(positiveQuery)

	var got bool
	negativeAllocs := testing.AllocsPerRun(100, func() {
		got = negativeCatalog.HasNewerVersion(negativeQuery)
	})
	require.False(t, got)
	require.Zero(t, negativeAllocs)

	positiveAllocs := testing.AllocsPerRun(100, func() {
		got = positiveCatalog.HasNewerVersion(positiveQuery)
	})
	require.True(t, got)
	require.Zero(t, positiveAllocs)

	coldCatalog, coldQuery := newProbeBenchmarkCatalog(16, true, false)
	coldAllocs := testing.AllocsPerRun(1, func() {
		got = coldCatalog.HasNewerVersion(coldQuery)
	})
	runtime.GC()
	postGCAllocs := testing.AllocsPerRun(1, func() {
		got = coldCatalog.HasNewerVersion(coldQuery)
	})
	t.Logf("database cold allocs=%v post-GC allocs=%v", coldAllocs, postGCAllocs)
}

func TestHasNewerVersionProbeReleaseClearsExactObject(t *testing.T) {
	cc := NewCatalog()
	tableProbe := &TableItem{
		AccountId: 1, DatabaseId: 2, Name: "long-table-name",
		DatabaseName: "db", CPKey: []byte("retained-key"),
	}
	releaseTableQueryProbe(&cc.tableQueryProbePool, tableProbe)
	require.Equal(t, TableItem{}, *tableProbe)

	databaseProbe := &DatabaseItem{
		AccountId: 1, Name: "long-database-name", CPKey: []byte("retained-key"),
	}
	releaseDatabaseQueryProbe(&cc.databaseQueryProbePool, databaseProbe)
	require.Equal(t, DatabaseItem{}, *databaseProbe)
}

func TestHasNewerVersionProbeReuseAcrossGC(t *testing.T) {
	cc := newProbeOracleCatalog()
	queries := []*TableChangeQuery{
		{AccountId: 1, DatabaseId: 11, DatabaseName: "a-very-long-database-name", Name: "a-very-long-table-name", TableId: 101, Version: 1, Ts: timestamp.Timestamp{PhysicalTime: 100}},
		{AccountId: 2, DatabaseId: 21, DatabaseName: "db", Name: "t", TableId: 201, Version: 2, Ts: timestamp.Timestamp{PhysicalTime: 300}},
		{AccountId: 1, DatabaseId: 11, Name: "t", TableId: 101, Version: 3, Ts: timestamp.Timestamp{PhysicalTime: 200}},
	}
	tableCount := cc.tables.data.Len()
	databaseCount := cc.databases.data.Len()
	for i := 0; i < 32; i++ {
		for _, query := range queries {
			want := legacyHasNewerVersionForTest(cc, query)
			require.Equal(t, want, cc.HasNewerVersion(query))
		}
		runtime.GC()
	}
	require.Equal(t, tableCount, cc.tables.data.Len())
	require.Equal(t, databaseCount, cc.databases.data.Len())
}

func TestHasNewerVersionProbeConcurrentReaders(t *testing.T) {
	cc := newProbeOracleCatalog()
	queries := []*TableChangeQuery{
		{AccountId: 1, DatabaseId: 11, Name: "t", TableId: 101, Version: 3, Ts: timestamp.Timestamp{PhysicalTime: 200}},
		{AccountId: 1, DatabaseId: 12, Name: "other", TableId: 102, Version: 0, Ts: timestamp.Timestamp{PhysicalTime: 100}},
		{AccountId: 2, DatabaseId: 21, DatabaseName: "db", Name: "t", TableId: 201, Version: 2, Ts: timestamp.Timestamp{PhysicalTime: 300}},
		{AccountId: 7, DatabaseId: 0, Ts: timestamp.Timestamp{PhysicalTime: 100}},
	}
	want := make([]bool, len(queries))
	for i, query := range queries {
		want[i] = legacyHasNewerVersionForTest(cc, query)
	}

	start := make(chan struct{})
	failures := make(chan string, 64)
	var wg sync.WaitGroup
	for worker := 0; worker < 64; worker++ {
		wg.Add(1)
		go func(worker int) {
			defer wg.Done()
			<-start
			for i := 0; i < 256; i++ {
				index := (worker + i) % len(queries)
				if want[index] != cc.HasNewerVersion(queries[index]) {
					failures <- fmt.Sprintf("worker %d query %d returned an unexpected result", worker, index)
					return
				}
			}
		}(worker)
	}
	close(start)
	wg.Wait()
	select {
	case failure := <-failures:
		t.Fatal(failure)
	default:
	}
}

func newProbeBenchmarkCatalog(history int, database, changed bool) (*CatalogCache, *TableChangeQuery) {
	cc := NewCatalog()
	if database {
		for i := 1; i <= history; i++ {
			cc.databases.data.Set(&DatabaseItem{
				AccountId: 1,
				Name:      "db",
				Id:        uint64(i),
				Ts:        timestamp.Timestamp{PhysicalTime: int64(i)},
			})
		}
		query := &TableChangeQuery{
			AccountId:    1,
			DatabaseId:   uint64(history),
			DatabaseName: "db",
			Ts:           timestamp.Timestamp{PhysicalTime: int64(history)},
		}
		if changed {
			query.DatabaseId = uint64(history - 1)
			query.Ts = timestamp.Timestamp{PhysicalTime: int64(history - 1)}
		}
		return cc, query
	}

	for i := 1; i <= history; i++ {
		cc.tables.data.Set(&TableItem{
			AccountId:  1,
			DatabaseId: 2,
			Name:       "t",
			Id:         3,
			Version:    uint32(i),
			Ts:         timestamp.Timestamp{PhysicalTime: int64(i)},
		})
	}
	query := &TableChangeQuery{
		AccountId:  1,
		DatabaseId: 2,
		Name:       "t",
		TableId:    3,
		Version:    uint32(history),
		Ts:         timestamp.Timestamp{PhysicalTime: int64(history)},
	}
	if changed {
		query.Version = uint32(history - 1)
		query.Ts = timestamp.Timestamp{PhysicalTime: int64(history - 1)}
	}
	return cc, query
}

func BenchmarkHasNewerVersionTable(b *testing.B) {
	benchmarkHasNewerVersionTable(b, false)
}

func BenchmarkHasNewerVersionLegacyTable(b *testing.B) {
	benchmarkHasNewerVersionTable(b, true)
}

func benchmarkHasNewerVersionTable(b *testing.B, legacy bool) {
	for _, history := range []int{1, 16, 256, 4096} {
		for _, changed := range []bool{false, true} {
			state := "warmed-negative"
			if changed {
				state = "changed"
			}
			b.Run(fmt.Sprintf("history=%d/state=%s", history, state), func(b *testing.B) {
				cc, query := newProbeBenchmarkCatalog(history, false, changed)
				b.ReportAllocs()
				b.ResetTimer()
				for i := 0; i < b.N; i++ {
					if legacy {
						_ = legacyHasNewerVersionForTest(cc, query)
					} else {
						_ = cc.HasNewerVersion(query)
					}
				}
			})
		}
	}
}

func BenchmarkHasNewerVersionDatabase(b *testing.B) {
	benchmarkHasNewerVersionDatabase(b, false)
}

func BenchmarkHasNewerVersionLegacyDatabase(b *testing.B) {
	benchmarkHasNewerVersionDatabase(b, true)
}

func benchmarkHasNewerVersionDatabase(b *testing.B, legacy bool) {
	for _, history := range []int{1, 16, 256, 4096} {
		for _, changed := range []bool{false, true} {
			state := "warmed-negative"
			if changed {
				state = "changed"
			}
			b.Run(fmt.Sprintf("history=%d/state=%s", history, state), func(b *testing.B) {
				cc, query := newProbeBenchmarkCatalog(history, true, changed)
				b.ReportAllocs()
				b.ResetTimer()
				for i := 0; i < b.N; i++ {
					if legacy {
						_ = legacyHasNewerVersionForTest(cc, query)
					} else {
						_ = cc.HasNewerVersion(query)
					}
				}
			})
		}
	}
}
