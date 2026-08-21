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
	"bytes"
	"encoding/json"
	"strconv"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"github.com/matrixorigin/matrixone/pkg/pb/timestamp"
)

func TestCatalogInvalidationAttributionDisabledByDefault(t *testing.T) {
	cc := NewCatalog()
	cc.HasNewerVersionFor(&TableChangeQuery{AccountId: 1, Name: "t"}, CatalogInvalidationConsumerPreparedPlan)
	report := cc.SnapshotCatalogInvalidationReport()
	require.False(t, report.Enabled)
	require.Empty(t, report.Consumers)
}

func TestCatalogInvalidationAttributionDifferentialAndCollision(t *testing.T) {
	cc := NewCatalog()
	cc.EnableCatalogInvalidationAttribution()
	cc.setTableItem(&TableItem{
		AccountId: 1, DatabaseId: 10, Name: "t", Id: 20, Version: 3,
		Ts: timestamp.Timestamp{PhysicalTime: 200},
	}, true)

	changed := &TableChangeQuery{
		AccountId: 1, DatabaseId: 10, Name: "t", TableId: 20, Version: 2,
		Ts: timestamp.Timestamp{PhysicalTime: 100},
	}
	require.True(t, cc.HasNewerVersionFor(changed, CatalogInvalidationConsumerPreparedPlan))

	otherAccount := &TableChangeQuery{
		AccountId: 2, DatabaseId: 10, Name: "t", TableId: 20, Version: 2,
		Ts: timestamp.Timestamp{PhysicalTime: 100},
	}
	require.False(t, cc.HasNewerVersionFor(otherAccount, CatalogInvalidationConsumerRCTableCache))

	collision := *otherAccount
	collision.AccountId = 1 + tableChangeBucketCount
	require.False(t, cc.HasNewerVersionFor(&collision, CatalogInvalidationConsumerRCTableCache))

	report := cc.SnapshotCatalogInvalidationReport()
	require.True(t, report.Enabled)
	require.Equal(t, uint64(1), report.Consumers["prepared_plan"].Checks)
	require.Equal(t, uint64(1), report.Consumers["prepared_plan"].StableChecks)
	require.Zero(t, report.Consumers["prepared_plan"].InconclusiveChecks)
	require.Zero(t, report.Consumers["prepared_plan"].PreciseFalseNegatives)
	require.Equal(t, uint64(1), report.Consumers["rc_table_cache"].BucketFalsePositives)
	require.Zero(t, report.Consumers["rc_table_cache"].PreciseFalsePositives)
}

func TestCatalogInvalidationConcurrentMutationIsInconclusive(t *testing.T) {
	cc := NewCatalog()
	cc.EnableCatalogInvalidationAttribution()
	cc.setTableItem(&TableItem{
		AccountId: 1, DatabaseId: 10, Name: "t", Id: 20, Version: 1,
		Ts: timestamp.Timestamp{PhysicalTime: 100},
	}, true)
	query := &TableChangeQuery{
		AccountId: 1, DatabaseId: 10, DatabaseName: "db", Name: "t", TableId: 20,
		Version: 1, Ts: timestamp.Timestamp{PhysicalTime: 200},
	}

	endMutation := cc.attribution.beginMutation()
	defer endMutation()
	require.False(t, cc.HasNewerVersionFor(query, CatalogInvalidationConsumerPreparedPlan))

	counter := cc.SnapshotCatalogInvalidationReport().Consumers["prepared_plan"]
	require.Equal(t, uint64(1), counter.Checks)
	require.Zero(t, counter.StableChecks)
	require.Equal(t, uint64(1), counter.InconclusiveChecks)
	require.Zero(t, counter.PreciseFalseNegatives)
}

func TestCatalogInvalidationShadowIgnoresReplayAndReportsMetadata(t *testing.T) {
	cc := NewCatalog()
	cc.EnableCatalogInvalidationAttribution()
	cc.SetCatalogInvalidationReportMetadata(CatalogInvalidationReportMetadata{
		MatrixONESHA: "test-sha", Config: "test-config", Window: "test-window", Integrity: "complete",
	})
	a := cc.attribution
	a.observeTable(1, 10, "db", "t", 20, 3, timestamp.Timestamp{PhysicalTime: 200}, false)
	a.observeTable(1, 10, "db", "t", 20, 2, timestamp.Timestamp{PhysicalTime: 100}, false)
	require.True(t, a.preciseDecision(&TableChangeQuery{
		AccountId: 1, DatabaseId: 10, DatabaseName: "db", Name: "t", TableId: 20, Version: 2,
		Ts: timestamp.Timestamp{PhysicalTime: 150},
	}))

	var buf bytes.Buffer
	require.NoError(t, cc.WriteCatalogInvalidationReport(&buf))
	var report CatalogInvalidationReport
	require.NoError(t, json.Unmarshal(buf.Bytes(), &report))
	require.Equal(t, "test-sha", report.Metadata.MatrixONESHA)
	require.Equal(t, "complete", report.Metadata.Integrity)
	require.Len(t, report.PreparedPlanRebuild.Buckets, len(catalogLatencyBounds)+1)
}

func TestCatalogInvalidationShadowDatabaseRecreation(t *testing.T) {
	cc := NewCatalog()
	cc.EnableCatalogInvalidationAttribution()
	cc.attribution.observeDatabase(1, "db", 20, timestamp.Timestamp{PhysicalTime: 200}, false)
	require.True(t, cc.attribution.preciseDecision(&TableChangeQuery{
		AccountId: 1, DatabaseId: 10, DatabaseName: "db", Name: "missing",
		Ts: timestamp.Timestamp{PhysicalTime: 100},
	}))
	cc.attribution.observeDatabase(1, "db", 20, timestamp.Timestamp{PhysicalTime: 100}, false)
	require.False(t, cc.attribution.preciseDecision(&TableChangeQuery{
		AccountId: 1, DatabaseId: 20, DatabaseName: "db", Name: "missing",
		Ts: timestamp.Timestamp{PhysicalTime: 300},
	}))
}

func TestCatalogInvalidationShadowEqualTimestampConflictIsConservative(t *testing.T) {
	cc := NewCatalog()
	cc.EnableCatalogInvalidationAttribution()
	a := cc.attribution
	firstTS := timestamp.Timestamp{PhysicalTime: 200}
	a.observeTable(1, 10, "db", "t", 20, 3, firstTS, false)
	a.observeTable(1, 10, "db", "t", 21, 1, firstTS, false)
	a.observeTable(1, 10, "db", "t", 20, 3, timestamp.Timestamp{PhysicalTime: 300}, false)
	require.True(t, a.preciseDecision(&TableChangeQuery{
		AccountId: 1, DatabaseId: 10, DatabaseName: "db", Name: "t", TableId: 20, Version: 3,
		Ts: timestamp.Timestamp{PhysicalTime: 250},
	}))
}

func TestCatalogInvalidationDifferentialMatrix(t *testing.T) {
	assertTable := func(t *testing.T, cc *CatalogCache, query *TableChangeQuery) {
		t.Helper()
		exact := cc.hasNewerVersion(query)
		precise := cc.attribution.preciseDecision(query)
		require.Equal(t, exact, precise, "query=%+v", query)
	}

	t.Run("table lifecycle", func(t *testing.T) {
		cc := NewCatalog()
		cc.EnableCatalogInvalidationAttribution()
		cc.setTableItem(&TableItem{
			AccountId: 1, DatabaseId: 10, DatabaseName: "db", Name: "t", Id: 20,
			Version: 1, Ts: timestamp.Timestamp{PhysicalTime: 100},
		}, true)
		assertTable(t, cc, &TableChangeQuery{
			AccountId: 1, DatabaseId: 10, DatabaseName: "db", Name: "t", TableId: 20,
			Version: 1, Ts: timestamp.Timestamp{PhysicalTime: 200},
		})

		cc.setTableItem(&TableItem{
			AccountId: 1, DatabaseId: 10, DatabaseName: "db", Name: "t", Id: 20,
			Version: 2, Ts: timestamp.Timestamp{PhysicalTime: 300},
		}, true)
		assertTable(t, cc, &TableChangeQuery{
			AccountId: 1, DatabaseId: 10, DatabaseName: "db", Name: "t", TableId: 20,
			Version: 1, Ts: timestamp.Timestamp{PhysicalTime: 200},
		})

		cc.setTableItem(&TableItem{
			AccountId: 1, DatabaseId: 10, DatabaseName: "db", Name: "t", Id: 20,
			deleted: true, Ts: timestamp.Timestamp{PhysicalTime: 400},
		}, false)
		cc.setTableItem(&TableItem{
			AccountId: 1, DatabaseId: 10, DatabaseName: "db", Name: "t", Id: 21,
			Version: 1, Ts: timestamp.Timestamp{PhysicalTime: 500},
		}, true)
		assertTable(t, cc, &TableChangeQuery{
			AccountId: 1, DatabaseId: 10, DatabaseName: "db", Name: "t", TableId: 20,
			Version: 2, Ts: timestamp.Timestamp{PhysicalTime: 350},
		})

		cc.setTableItem(&TableItem{
			AccountId: 1, DatabaseId: 10, DatabaseName: "db", Name: "old", Id: 20,
			deleted: true, Ts: timestamp.Timestamp{PhysicalTime: 600},
		}, false)
		cc.setTableItem(&TableItem{
			AccountId: 1, DatabaseId: 10, DatabaseName: "db", Name: "new", Id: 21,
			Version: 1, Ts: timestamp.Timestamp{PhysicalTime: 700},
		}, true)
		assertTable(t, cc, &TableChangeQuery{
			AccountId: 1, DatabaseId: 10, DatabaseName: "db", Name: "old", TableId: 20,
			Version: 2, Ts: timestamp.Timestamp{PhysicalTime: 550},
		})
	})

	t.Run("database identity and empty recreation", func(t *testing.T) {
		cc := NewCatalog()
		cc.EnableCatalogInvalidationAttribution()
		cc.databases.data.Set(&DatabaseItem{
			AccountId: 1, Name: "db", Id: 20, Ts: timestamp.Timestamp{PhysicalTime: 200},
		})
		cc.attribution.observeDatabase(1, "db", 20, timestamp.Timestamp{PhysicalTime: 200}, false)
		require.True(t, cc.hasNewerVersion(&TableChangeQuery{
			AccountId: 1, DatabaseId: 10, DatabaseName: "db", Ts: timestamp.Timestamp{PhysicalTime: 100},
		}))
		require.True(t, cc.attribution.preciseDecision(&TableChangeQuery{
			AccountId: 1, DatabaseId: 10, DatabaseName: "db", Ts: timestamp.Timestamp{PhysicalTime: 100},
		}))
		cc.databases.data.Set(&DatabaseItem{
			AccountId: 1, Name: "db", Id: 21, deleted: true, Ts: timestamp.Timestamp{PhysicalTime: 300},
		})
		cc.attribution.observeDatabase(1, "db", 21, timestamp.Timestamp{PhysicalTime: 300}, true)
		cc.databases.data.Set(&DatabaseItem{
			AccountId: 1, Name: "db", Id: 22, Ts: timestamp.Timestamp{PhysicalTime: 400},
		})
		cc.attribution.observeDatabase(1, "db", 22, timestamp.Timestamp{PhysicalTime: 400}, false)
		require.True(t, cc.hasNewerVersion(&TableChangeQuery{
			AccountId: 1, DatabaseId: 20, DatabaseName: "db", Ts: timestamp.Timestamp{PhysicalTime: 250},
		}))
		require.True(t, cc.attribution.preciseDecision(&TableChangeQuery{
			AccountId: 1, DatabaseId: 20, DatabaseName: "db", Ts: timestamp.Timestamp{PhysicalTime: 250},
		}))
	})

	t.Run("same account and collision", func(t *testing.T) {
		cc := NewCatalog()
		cc.EnableCatalogInvalidationAttribution()
		cc.setTableItem(&TableItem{
			AccountId: 1, DatabaseId: 10, DatabaseName: "db", Name: "other", Id: 30,
			Ts: timestamp.Timestamp{PhysicalTime: 500},
		}, true)
		query := &TableChangeQuery{
			AccountId: 1, DatabaseId: 10, DatabaseName: "db", Name: "target", TableId: 40,
			Ts: timestamp.Timestamp{PhysicalTime: 400},
		}
		require.False(t, cc.hasNewerVersion(query))
		require.False(t, cc.attribution.preciseDecision(query))
		require.True(t, cc.bucketHasNewerVersion(query))
		collision := *query
		collision.AccountId += tableChangeBucketCount
		require.False(t, cc.hasNewerVersion(&collision))
		require.False(t, cc.attribution.preciseDecision(&collision))
		require.True(t, cc.bucketHasNewerVersion(&collision))
	})

	t.Run("timestamp boundaries and gc", func(t *testing.T) {
		cc := NewCatalog()
		cc.EnableCatalogInvalidationAttribution()
		cc.setTableItem(&TableItem{
			AccountId: 1, DatabaseId: 10, DatabaseName: "db", Name: "t", Id: 20,
			Version: 3, Ts: timestamp.Timestamp{PhysicalTime: 500},
		}, true)
		for _, tc := range []struct {
			name string
			ts   int64
			ver  uint32
			want bool
		}{
			{name: "older changed", ts: 499, ver: 2, want: true},
			{name: "equal", ts: 500, ver: 3, want: false},
			{name: "newer", ts: 501, ver: 3, want: false},
		} {
			t.Run(tc.name, func(t *testing.T) {
				query := &TableChangeQuery{
					AccountId: 1, DatabaseId: 10, DatabaseName: "db", Name: "t", TableId: 20,
					Version: tc.ver, Ts: timestamp.Timestamp{PhysicalTime: tc.ts},
				}
				require.Equal(t, tc.want, cc.hasNewerVersion(query))
				require.Equal(t, tc.want, cc.attribution.preciseDecision(query))
			})
		}
		cc.setTableItem(&TableItem{
			AccountId: 1, DatabaseId: 10, DatabaseName: "db", Name: "t", Id: 20,
			Version: 2, Ts: timestamp.Timestamp{PhysicalTime: 400},
		}, true)
		cc.GC(timestamp.Timestamp{PhysicalTime: 600})
		require.False(t, cc.attribution.preciseDecision(&TableChangeQuery{
			AccountId: 1, DatabaseId: 10, DatabaseName: "db", Name: "t", TableId: 20,
			Version: 3, Ts: timestamp.Timestamp{PhysicalTime: 500},
		}))
	})
}

func TestCatalogInvalidationDifferentialRandomizedSequences(t *testing.T) {
	const seed uint64 = 27235
	state := seed
	next := func(limit uint64) uint64 {
		state = state*6364136223846793005 + 1442695040888963407
		return state % limit
	}

	cc := NewCatalog()
	cc.EnableCatalogInvalidationAttribution()
	for step := 1; step <= 4096; step++ {
		account := uint32(1 + next(4))
		databaseID := uint64(10 + next(4))
		name := "t" + strconv.FormatUint(next(4), 10)
		ts := timestamp.Timestamp{PhysicalTime: int64(step)}
		if next(3) != 0 {
			cc.setTableItem(&TableItem{
				AccountId: account, DatabaseId: databaseID, DatabaseName: "db",
				Name: name, Id: uint64(100 + next(8)), Version: uint32(1 + next(4)),
				Ts: ts, deleted: next(5) == 0,
			}, true)
			continue
		}
		query := &TableChangeQuery{
			AccountId: account, DatabaseId: databaseID, DatabaseName: "db", Name: name,
			TableId: uint64(100 + next(8)), Version: uint32(1 + next(4)), Ts: ts,
		}
		exact := cc.hasNewerVersion(query)
		precise := cc.attribution.preciseDecision(query)
		require.Equal(t, exact, precise, "seed=%d step=%d query=%+v", seed, step, query)
	}
}

func TestCatalogInvalidationShadowCapFailsClosed(t *testing.T) {
	cc := NewCatalog()
	cc.EnableCatalogInvalidationAttribution()
	for i := 0; i < catalogShadowEntryLimit+1; i++ {
		cc.attribution.observeTable(
			uint32(i), uint64(i), "db", "t", uint64(i+1), 1,
			timestamp.Timestamp{PhysicalTime: int64(i + 1)}, false,
		)
	}
	require.True(t, cc.attribution.shadowOverflow)
	require.True(t, cc.attribution.preciseDecision(&TableChangeQuery{AccountId: 999999, Name: "unseen"}))
	require.True(t, cc.SnapshotCatalogInvalidationReport().Shadow.Overflow)
}

func TestCatalogInvalidationLatencyHistogramIsBounded(t *testing.T) {
	var h catalogLatencyHistogram
	for i := 0; i < 32; i++ {
		h.observe(time.Duration(i+1) * time.Microsecond)
	}
	require.Equal(t, uint64(32), h.count)
	require.Greater(t, h.quantile(0.99), int64(0))
	// The histogram retains fixed buckets rather than one sample per event.
	require.Len(t, h.bucket, len(catalogLatencyBounds)+1)
}

func TestCatalogInvalidationPreciseNoChangeDoesNotAllocate(t *testing.T) {
	cc := NewCatalog()
	cc.EnableCatalogInvalidationAttribution()
	cc.attribution.observeTable(1, 10, "db", "t", 20, 3, timestamp.Timestamp{PhysicalTime: 100}, false)
	query := &TableChangeQuery{
		AccountId: 1, DatabaseId: 10, DatabaseName: "db", Name: "t", TableId: 20, Version: 3,
		Ts: timestamp.Timestamp{PhysicalTime: 300},
	}
	allocs := testing.AllocsPerRun(100, func() {
		cc.attribution.preciseDecision(query)
	})
	require.Zero(t, allocs)
}

func BenchmarkCatalogInvalidationOracles(b *testing.B) {
	for _, history := range []int{1, 16, 256, 4096} {
		for _, changed := range []bool{false, true} {
			state := "warmed-negative"
			if changed {
				state = "changed"
			}
			query := &TableChangeQuery{
				AccountId: 1, DatabaseId: 10, DatabaseName: "db", Name: "t", TableId: uint64(history),
				Version: uint32(history), Ts: timestamp.Timestamp{PhysicalTime: int64(history)},
			}
			if changed {
				query.Version--
				query.Ts.PhysicalTime--
			} else {
				query.Ts.PhysicalTime++
			}
			seed := func(cc *CatalogCache) {
				for i := 0; i < history; i++ {
					cc.setTableItem(&TableItem{
						AccountId: 1, DatabaseId: 10, DatabaseName: "db", Name: "t",
						Id: uint64(i + 1), Version: uint32(i + 1),
						Ts: timestamp.Timestamp{PhysicalTime: int64(i + 1)},
					}, true)
				}
			}
			b.Run("history="+strconv.Itoa(history)+"/"+state+"/exact", func(b *testing.B) {
				cc := NewCatalog()
				seed(cc)
				b.ReportAllocs()
				b.ResetTimer()
				for i := 0; i < b.N; i++ {
					cc.HasNewerVersion(query)
				}
			})

			b.Run("history="+strconv.Itoa(history)+"/"+state+"/disabled-wrapper", func(b *testing.B) {
				cc := NewCatalog()
				cc.attribution = nil
				seed(cc)
				b.ReportAllocs()
				b.ResetTimer()
				for i := 0; i < b.N; i++ {
					cc.HasNewerVersionFor(query, CatalogInvalidationConsumerPreparedPlan)
				}
			})

			b.Run("history="+strconv.Itoa(history)+"/"+state+"/bucket", func(b *testing.B) {
				cc := NewCatalog()
				seed(cc)
				b.ReportAllocs()
				b.ResetTimer()
				for i := 0; i < b.N; i++ {
					cc.bucketHasNewerVersion(query)
				}
			})

			b.Run("history="+strconv.Itoa(history)+"/"+state+"/precise", func(b *testing.B) {
				cc := NewCatalog()
				cc.EnableCatalogInvalidationAttribution()
				seed(cc)
				b.ReportAllocs()
				b.ResetTimer()
				for i := 0; i < b.N; i++ {
					cc.attribution.preciseDecision(query)
				}
			})
		}
	}
}
