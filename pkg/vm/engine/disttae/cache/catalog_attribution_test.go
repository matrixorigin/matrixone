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
	require.Zero(t, report.Consumers["prepared_plan"].PreciseFalseNegatives)
	require.Equal(t, uint64(1), report.Consumers["rc_table_cache"].BucketFalsePositives)
	require.Zero(t, report.Consumers["rc_table_cache"].PreciseFalsePositives)
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
	query := &TableChangeQuery{
		AccountId: 1, DatabaseId: 10, DatabaseName: "db", Name: "t", TableId: 20, Version: 3,
		Ts: timestamp.Timestamp{PhysicalTime: 1000},
	}
	for _, history := range []int{1, 16, 256, 4096} {
		b.Run("history="+strconv.Itoa(history)+"/exact", func(b *testing.B) {
			cc := NewCatalog()
			for i := 0; i < history; i++ {
				cc.setTableItem(&TableItem{
					AccountId: 1, DatabaseId: uint64(100 + i), Name: "history", Id: uint64(i + 1),
					Ts: timestamp.Timestamp{PhysicalTime: int64(i + 1)},
				}, true)
			}
			b.ReportAllocs()
			b.ResetTimer()
			for i := 0; i < b.N; i++ {
				cc.HasNewerVersion(query)
			}
		})

		b.Run("history="+strconv.Itoa(history)+"/bucket", func(b *testing.B) {
			cc := NewCatalog()
			for i := 0; i < history; i++ {
				cc.setTableItem(&TableItem{
					AccountId: 1, DatabaseId: uint64(100 + i), Name: "history", Id: uint64(i + 1),
					Ts: timestamp.Timestamp{PhysicalTime: int64(i + 1)},
				}, true)
			}
			b.ReportAllocs()
			b.ResetTimer()
			for i := 0; i < b.N; i++ {
				cc.bucketHasNewerVersion(query)
			}
		})

		b.Run("history="+strconv.Itoa(history)+"/precise", func(b *testing.B) {
			cc := NewCatalog()
			cc.EnableCatalogInvalidationAttribution()
			for i := 0; i < history; i++ {
				cc.attribution.observeTable(
					1, 10, "db", "history", uint64(i+1), 1,
					timestamp.Timestamp{PhysicalTime: int64(i + 1)}, false,
				)
			}
			b.ReportAllocs()
			b.ResetTimer()
			for i := 0; i < b.N; i++ {
				cc.attribution.preciseDecision(query)
			}
		})
	}
}
