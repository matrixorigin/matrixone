// Copyright 2026 Matrix Origin
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
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
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/matrixorigin/matrixone/pkg/common/identifier"
	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/pb/timestamp"
)

func TestFoldedDatabaseLookupHonorsSnapshotAndAmbiguity(t *testing.T) {
	cc := NewCatalog()
	makeItem := func(name string, id uint64, at int64, deleted bool) *DatabaseItem {
		return &DatabaseItem{
			AccountId: 7, Name: name, Id: id,
			Ts: timestamp.Timestamp{PhysicalTime: at}, deleted: deleted,
		}
	}
	cc.setDatabaseItem(makeItem("Foo", 10, 100, false), false)
	cc.setDatabaseItem(makeItem("foo", 11, 200, false), false)

	lookup := func(at int64) (DatabaseItem, bool, bool) {
		query := DatabaseItem{
			AccountId: 7, Name: "FOO", Ts: timestamp.Timestamp{PhysicalTime: at},
		}
		found, ambiguous := lookupFoldedDatabase(cc, &query)
		return query, found, ambiguous
	}
	beforeCollision, found, ambiguous := lookup(150)
	require.True(t, found)
	require.False(t, ambiguous)
	require.Equal(t, "Foo", beforeCollision.Name)
	require.Equal(t, uint64(10), beforeCollision.Id)

	_, found, ambiguous = lookup(250)
	require.False(t, found)
	require.True(t, ambiguous)

	cc.setDatabaseItem(makeItem("foo", 11, 300, true), false)
	afterDrop, found, ambiguous := lookup(350)
	require.True(t, found)
	require.False(t, ambiguous)
	require.Equal(t, "Foo", afterDrop.Name)

	exact := &DatabaseItem{AccountId: 7, Name: "FOO", Ts: timestamp.Timestamp{PhysicalTime: 350}}
	require.False(t, cc.GetDatabase(exact), "mode-0 exact lookup must stay case-sensitive")
	otherTenant := &DatabaseItem{AccountId: 8, Name: "FOO", Ts: timestamp.Timestamp{PhysicalTime: 350}}
	found, ambiguous = lookupFoldedDatabase(cc, otherTenant)
	require.False(t, found)
	require.False(t, ambiguous)
}

func TestFoldedTableLookupPreservesPhysicalName(t *testing.T) {
	cc := NewCatalog()
	cc.setTableItem(&TableItem{
		AccountId: 7, DatabaseId: 9, Name: "MixT", DatabaseName: "Qa",
		Id: 100, Ts: timestamp.Timestamp{PhysicalTime: 100},
	}, false)
	query := &TableItem{
		AccountId: 7, DatabaseId: 9, Name: "mixt",
		Ts: timestamp.Timestamp{PhysicalTime: 150},
	}
	found, ambiguous := lookupFoldedTable(cc, query)
	require.True(t, found)
	require.False(t, ambiguous)
	require.Equal(t, "MixT", query.Name)
	require.Equal(t, "Qa", query.DatabaseName)
	require.Equal(t, uint64(100), query.Id)

	cc.setTableItem(&TableItem{
		AccountId: 7, DatabaseId: 9, Name: "mixt", DatabaseName: "Qa",
		Id: 101, Ts: timestamp.Timestamp{PhysicalTime: 200},
	}, false)
	query = &TableItem{
		AccountId: 7, DatabaseId: 9, Name: "MIXT",
		Ts: timestamp.Timestamp{PhysicalTime: 250},
	}
	found, ambiguous = lookupFoldedTable(cc, query)
	require.False(t, found)
	require.True(t, ambiguous)
}

func TestFoldedCatalogVisitsPhysicalNameOrder(t *testing.T) {
	require.Equal(t, "k", identifier.Fold("K"))
	cc := NewCatalog()
	for i, name := range []string{"K", "k", "K"} {
		cc.setDatabaseItem(&DatabaseItem{
			AccountId: 7, Name: name, Id: uint64(i + 1),
			Ts: timestamp.Timestamp{PhysicalTime: 10},
		}, false)
		cc.setTableItem(&TableItem{
			AccountId: 7, DatabaseId: 9, Name: name, Id: uint64(i + 1),
			Ts: timestamp.Timestamp{PhysicalTime: 10},
		}, false)
	}

	snapshot := timestamp.Timestamp{PhysicalTime: 20}
	var databases []string
	cc.VisitFoldedDatabases(7, "K", snapshot, func(item *DatabaseItem) bool {
		databases = append(databases, item.Name)
		return true
	})
	require.Equal(t, []string{"K", "k", "K"}, databases)

	var tables []string
	cc.VisitFoldedTables(7, 9, "k", snapshot, func(item *TableItem) bool {
		tables = append(tables, item.Name)
		return len(tables) < 2
	})
	require.Equal(t, []string{"K", "k"}, tables)
}

// The canonical spelling and a mixed-case spelling must remain separate
// physical candidates through replacement, tombstone retirement, and GC.
// This is the contract for both a full and a sparse folded-name index.
func TestFoldedCatalogCanonicalVariantVisibilityAcrossGC(t *testing.T) {
	cc := NewCatalog()
	cc.UpdateDuration(types.TS{}, types.MaxTs())
	for _, event := range []struct {
		name    string
		id      uint64
		at      int64
		deleted bool
	}{
		{"foo", 10, 10, false},
		{"Foo", 20, 20, false},
		{"foo", 10, 30, true},
		{"foo", 30, 40, false},
		{"Foo", 20, 50, true},
	} {
		ts := timestamp.Timestamp{PhysicalTime: event.at}
		cc.setDatabaseItem(&DatabaseItem{
			AccountId: 7, Name: event.name, Id: event.id,
			Ts: ts, deleted: event.deleted,
		}, false)
		cc.setTableItem(&TableItem{
			AccountId: 7, DatabaseId: 9, Name: event.name, Id: event.id,
			Ts: ts, deleted: event.deleted,
		}, false)
	}

	check := func(at int64, wantName string, wantID uint64, wantAmbiguous bool) {
		t.Helper()
		snapshot := timestamp.Timestamp{PhysicalTime: at}
		require.True(t, cc.CanServe(types.TimestampToTS(snapshot)))
		db := &DatabaseItem{AccountId: 7, Name: "FOO", Ts: snapshot}
		found, ambiguous := lookupFoldedDatabase(cc, db)
		require.Equal(t, wantAmbiguous, ambiguous)
		require.Equal(t, !wantAmbiguous, found)
		if found {
			require.Equal(t, wantName, db.Name)
			require.Equal(t, wantID, db.Id)
		}
		table := &TableItem{AccountId: 7, DatabaseId: 9, Name: "FOO", Ts: snapshot}
		found, ambiguous = lookupFoldedTable(cc, table)
		require.Equal(t, wantAmbiguous, ambiguous)
		require.Equal(t, !wantAmbiguous, found)
		if found {
			require.Equal(t, wantName, table.Name)
			require.Equal(t, wantID, table.Id)
		}
	}

	check(15, "foo", 10, false)
	check(25, "", 0, true)
	check(35, "Foo", 20, false)
	check(45, "", 0, true)
	check(55, "foo", 30, false)

	cc.GC(timestamp.Timestamp{PhysicalTime: 46})
	require.False(t, cc.CanServe(types.BuildTS(45, 0)))
	check(47, "", 0, true)
	check(55, "foo", 30, false)

	cc.GC(timestamp.Timestamp{PhysicalTime: 56})
	check(57, "foo", 30, false)
	mode0 := &DatabaseItem{AccountId: 7, Name: "Foo", Ts: timestamp.Timestamp{PhysicalTime: 57}}
	require.False(t, cc.GetDatabase(mode0), "mode-0 exact lookup must still see the tombstone")
}

func BenchmarkCatalogDatabaseNameLookup(b *testing.B) {
	for _, size := range []int{1, 100, 10000} {
		cc := NewCatalog()
		for i := 0; i < size-1; i++ {
			cc.setDatabaseItem(&DatabaseItem{
				AccountId: 7,
				Name:      fmt.Sprintf("Db_%05d", i),
				Id:        uint64(i + 10),
				Ts:        timestamp.Timestamp{PhysicalTime: 100},
			}, false)
		}
		cc.setDatabaseItem(&DatabaseItem{
			AccountId: 7, Name: "TargetDB", Id: uint64(size + 10),
			Ts: timestamp.Timestamp{PhysicalTime: 100},
		}, false)
		b.Run(fmt.Sprintf("exact_%d", size), func(b *testing.B) {
			b.ReportAllocs()
			b.ResetTimer()
			for i := 0; i < b.N; i++ {
				query := DatabaseItem{
					AccountId: 7, Name: "TargetDB",
					Ts: timestamp.Timestamp{PhysicalTime: 150},
				}
				if !cc.GetDatabase(&query) {
					b.Fatal("exact database lookup lost target")
				}
			}
		})
		b.Run(fmt.Sprintf("mode2_%d", size), func(b *testing.B) {
			b.ReportAllocs()
			b.ResetTimer()
			for i := 0; i < b.N; i++ {
				query := DatabaseItem{
					AccountId: 7, Name: "targetdb",
					Ts: timestamp.Timestamp{PhysicalTime: 150},
				}
				found, ambiguous := lookupFoldedDatabase(cc, &query)
				if !found || ambiguous || query.Name != "TargetDB" {
					b.Fatal("folded database lookup lost target")
				}
			}
		})
	}
}

func BenchmarkCatalogTableNameLookup(b *testing.B) {
	for _, size := range []int{1, 100, 10000} {
		cc := NewCatalog()
		for i := 0; i < size-1; i++ {
			cc.setTableItem(&TableItem{
				AccountId: 7, DatabaseId: 9, DatabaseName: "Qa",
				Name: fmt.Sprintf("Tbl_%05d", i), Id: uint64(i + 10),
				Ts: timestamp.Timestamp{PhysicalTime: 100},
			}, false)
		}
		cc.setTableItem(&TableItem{
			AccountId: 7, DatabaseId: 9, DatabaseName: "Qa",
			Name: "TargetTbl", Id: uint64(size + 10),
			Ts: timestamp.Timestamp{PhysicalTime: 100},
		}, false)
		b.Run(fmt.Sprintf("exact_%d", size), func(b *testing.B) {
			b.ReportAllocs()
			b.ResetTimer()
			for i := 0; i < b.N; i++ {
				query := TableItem{
					AccountId: 7, DatabaseId: 9, Name: "TargetTbl",
					Ts: timestamp.Timestamp{PhysicalTime: 150},
				}
				if !cc.GetTable(&query) {
					b.Fatal("exact table lookup lost target")
				}
			}
		})
		b.Run(fmt.Sprintf("mode2_%d", size), func(b *testing.B) {
			b.ReportAllocs()
			b.ResetTimer()
			for i := 0; i < b.N; i++ {
				query := TableItem{
					AccountId: 7, DatabaseId: 9, Name: "targettbl",
					Ts: timestamp.Timestamp{PhysicalTime: 150},
				}
				found, ambiguous := lookupFoldedTable(cc, &query)
				if !found || ambiguous || query.Name != "TargetTbl" {
					b.Fatal("folded table lookup lost target")
				}
			}
		})
	}
}

// Include the catalog write path in mode-0/1 performance checks. Lowercase
// names are the common mode-1 case; mixed spelling exercises mode-2 storage.
func BenchmarkCatalogTableVersionSet(b *testing.B) {
	for _, mixed := range []bool{false, true} {
		label := "lowercase"
		if mixed {
			label = "mixed"
		}
		b.Run(label, func(b *testing.B) {
			cc := NewCatalog()
			const size = 10000
			names := make([]string, size)
			for i := range names {
				prefix := "table"
				if mixed {
					prefix = "Table"
				}
				names[i] = fmt.Sprintf("%s_%05d", prefix, i)
				cc.setTableItem(&TableItem{
					AccountId: 7, DatabaseId: 9, Name: names[i], Id: uint64(i + 10),
					Ts: timestamp.Timestamp{PhysicalTime: 100},
				}, false)
			}
			b.ReportAllocs()
			b.ResetTimer()
			for i := 0; i < b.N; i++ {
				slot := i % size
				cc.setTableItem(&TableItem{
					AccountId: 7, DatabaseId: 9, Name: names[slot], Id: uint64(slot + 10),
					Ts: timestamp.Timestamp{PhysicalTime: 100},
				}, false)
			}
		})
	}
}

// Measure the actual retained catalog heap, including exact and sparse trees.
// Run with -benchtime=1x; this is a heap delta, not an allocation rate.
func BenchmarkCatalogFoldedIndexRetainedBytes(b *testing.B) {
	for _, mixed := range []bool{false, true} {
		label, prefix := "lowercase", "table"
		if mixed {
			label, prefix = "mixed", "Table"
		}
		b.Run(label, func(b *testing.B) {
			const size = 10000
			names := make([]string, size)
			for i := range names {
				names[i] = fmt.Sprintf("%s_%05d", prefix, i)
			}
			runtime.GC()
			var before, after runtime.MemStats
			runtime.ReadMemStats(&before)
			cc := NewCatalog()
			for i, name := range names {
				cc.setTableItem(&TableItem{
					AccountId: 7, DatabaseId: 9, Name: name, Id: uint64(i + 10),
					Ts: timestamp.Timestamp{PhysicalTime: 100},
				}, false)
			}
			runtime.GC()
			runtime.ReadMemStats(&after)
			runtime.KeepAlive(cc)
			runtime.KeepAlive(names)
			b.ReportMetric(float64(int64(after.HeapAlloc)-int64(before.HeapAlloc))/size, "retained-B/name")
		})
	}
}

// Tests and benchmarks collapse the visitor output; production overlays its
// transaction operations before deciding uniqueness in disttae.
func lookupFoldedTable(cc *CatalogCache, tbl *TableItem) (found, ambiguous bool) {
	cc.VisitFoldedTables(tbl.AccountId, tbl.DatabaseId, tbl.Name, tbl.Ts,
		func(item *TableItem) bool {
			if found {
				ambiguous = true
				return false
			}
			copyTableItem(tbl, item)
			found = true
			return true
		})
	return found && !ambiguous, ambiguous
}

func lookupFoldedDatabase(cc *CatalogCache, db *DatabaseItem) (found, ambiguous bool) {
	cc.VisitFoldedDatabases(db.AccountId, db.Name, db.Ts,
		func(item *DatabaseItem) bool {
			if found {
				ambiguous = true
				return false
			}
			copyDatabaseItem(db, item)
			found = true
			return true
		})
	return found && !ambiguous, ambiguous
}
