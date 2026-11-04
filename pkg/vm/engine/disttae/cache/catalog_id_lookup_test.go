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
	"sync"
	"testing"

	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/pb/timestamp"
	"github.com/stretchr/testify/require"
)

func TestGetTablesByIdAndTimeSnapshotVisibility(t *testing.T) {
	cc := NewCatalog()
	cc.UpdateDuration(types.TS{}, types.MaxTs())
	item := func(aid uint32, db, id uint64, name string, ts int64, version uint32, deleted bool) *TableItem {
		return &TableItem{AccountId: aid, DatabaseId: db, Id: id, Name: name,
			Ts: timestamp.Timestamp{PhysicalTime: ts}, Version: version, deleted: deleted}
	}
	old := item(1, 10, 101, "a", 10, 1, false)
	recreated := item(1, 10, 102, "a", 30, 1, false)
	altered := item(1, 10, 102, "a", 40, 2, false)
	truncated := item(1, 10, 103, "a", 50, 1, false)
	b := item(1, 10, 201, "b", 10, 1, false)
	view := item(1, 10, 202, "view", 20, 1, false)
	view.Kind = "v"
	otherDB := item(1, 20, 301, "a", 10, 1, false)
	otherTenant := item(2, 10, 401, "a", 10, 1, false)
	for _, value := range []*TableItem{
		old, item(1, 10, 101, "a", 20, 0, true), recreated,
		item(1, 10, 102, "a", 40, 0, true), altered,
		item(1, 10, 102, "a", 50, 0, true), truncated,
		b, item(1, 10, 201, "b", 60, 0, true), view, otherDB, otherTenant,
	} {
		cc.setTableItem(value, false)
	}
	ids := []uint64{103, 101, 999, 102, 301, 201, 201, 202, 401}
	for _, tc := range []struct {
		name string
		aid  uint32
		db   uint64
		ts   int64
		want []*TableItem
	}{
		{"before-create", 1, 10, 9, []*TableItem{nil, nil, nil, nil, nil, nil, nil, nil, nil}},
		{"created", 1, 10, 10, []*TableItem{nil, old, nil, nil, nil, b, b, nil, nil}},
		{"dropped", 1, 10, 20, []*TableItem{nil, nil, nil, nil, nil, b, b, view, nil}},
		{"recreated", 1, 10, 30, []*TableItem{nil, nil, nil, recreated, nil, b, b, view, nil}},
		{"same-timestamp-alter", 1, 10, 40, []*TableItem{nil, nil, nil, altered, nil, b, b, view, nil}},
		{"same-timestamp-truncate", 1, 10, 50, []*TableItem{truncated, nil, nil, nil, nil, b, b, view, nil}},
		{"latest-drop", 1, 10, 60, []*TableItem{truncated, nil, nil, nil, nil, nil, nil, view, nil}},
		{"wildcard-database", 1, 0, 40, []*TableItem{nil, nil, nil, altered, otherDB, b, b, view, nil}},
		{"other-database", 1, 20, 40, []*TableItem{nil, nil, nil, nil, otherDB, nil, nil, nil, nil}},
		{"other-tenant", 2, 10, 40, []*TableItem{nil, nil, nil, nil, nil, nil, nil, nil, otherTenant}},
		{"absent-scope", 3, 0, 40, []*TableItem{nil, nil, nil, nil, nil, nil, nil, nil, nil}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			ts := timestamp.Timestamp{PhysicalTime: tc.ts}
			got := cc.GetTablesByIdAndTime(tc.aid, tc.db, ids, ts)
			require.Len(t, got, len(ids))
			for i := range got {
				require.Same(t, tc.want[i], got[i], "id=%d", ids[i])
				require.Same(t, cc.GetTableByIdAndTime(tc.aid, tc.db, ids[i], ts), got[i])
			}
		})
	}
	t.Run("empty-and-singleton", func(t *testing.T) {
		ts := timestamp.Timestamp{PhysicalTime: 40}
		require.Nil(t, cc.GetTablesByIdAndTime(1, 10, nil, ts))
		require.Nil(t, cc.GetTablesByIdAndTime(1, 10, []uint64{}, ts))
		require.Equal(t, []*TableItem{altered}, cc.GetTablesByIdAndTime(1, 10, []uint64{102}, ts))
		require.Equal(t, []*TableItem{nil}, cc.GetTablesByIdAndTime(1, 10, []uint64{101}, ts))
		require.Equal(t, []*TableItem{altered, altered}, cc.GetTablesByIdAndTime(1, 10, []uint64{102, 102}, ts))
	})
	t.Run("GC-retained-boundary", func(t *testing.T) {
		cc.GC(timestamp.Timestamp{PhysicalTime: 45})
		require.False(t, cc.CanServe(types.BuildTS(40, 0)))
		require.True(t, cc.CanServe(types.BuildTS(45, 0)))
		for _, ts := range []int64{45, 50, 60} {
			stamp := timestamp.Timestamp{PhysicalTime: ts}
			got := cc.GetTablesByIdAndTime(1, 0, []uint64{101, 102, 103, 201, 202, 301}, stamp)
			want := []*TableItem{nil, altered, nil, b, view, otherDB}
			if ts >= 50 {
				want[1], want[2] = nil, truncated
			}
			if ts >= 60 {
				want[3] = nil
			}
			for i := range got {
				require.Same(t, want[i], got[i], "ts=%d index=%d", ts, i)
			}
		}
	})
}

func TestGetTablesByIdAndTimeConcurrentWriterAndGC(t *testing.T) {
	cc := NewCatalog()
	cc.UpdateDuration(types.TS{}, types.MaxTs())
	old := &TableItem{AccountId: 1, DatabaseId: 10, Id: 101, Name: "a", Ts: timestamp.Timestamp{PhysicalTime: 10}}
	visible := &TableItem{AccountId: 1, DatabaseId: 10, Id: 102, Name: "a", Ts: timestamp.Timestamp{PhysicalTime: 40}, Version: 2}
	for _, item := range []*TableItem{
		old,
		{AccountId: 1, DatabaseId: 10, Id: 101, Name: "a", Ts: timestamp.Timestamp{PhysicalTime: 30}, deleted: true},
		{AccountId: 1, DatabaseId: 10, Id: 102, Name: "a", Ts: timestamp.Timestamp{PhysicalTime: 30}},
		visible,
	} {
		cc.setTableItem(item, false)
	}
	ids := []uint64{101, 102, 103, 999, 102}
	want := []*TableItem{nil, visible, nil, nil, visible}
	stamp := timestamp.Timestamp{PhysicalTime: 50}
	start := make(chan struct{})
	ready := make(chan struct{}, 3)
	var wg sync.WaitGroup
	wg.Add(3)
	go func() {
		defer wg.Done()
		ready <- struct{}{}
		<-start
		for i := int64(60); i < 124; i++ {
			cc.setTableItem(&TableItem{AccountId: 1, DatabaseId: 10, Id: 103, Name: "a", Ts: timestamp.Timestamp{PhysicalTime: i}}, false)
		}
	}()
	go func() {
		defer wg.Done()
		ready <- struct{}{}
		<-start
		for range 16 {
			cc.GC(stamp)
		}
	}()
	go func() {
		defer wg.Done()
		ready <- struct{}{}
		<-start
		for range 256 {
			got := cc.GetTablesByIdAndTime(1, 10, ids, stamp)
			for i := range want {
				if got[i] != want[i] {
					t.Errorf("retained snapshot id=%d: got %p, want %p", ids[i], got[i], want[i])
					return
				}
			}
		}
	}()
	for range 3 {
		<-ready
	}
	close(start)
	wg.Wait()
	require.True(t, cc.CanServe(types.BuildTS(50, 0)))
	require.Equal(t, want, cc.GetTablesByIdAndTime(1, 10, ids, stamp))
}

var tableIDLookupBenchmarkSink []*TableItem

func BenchmarkGetTablesByIdAndTime(b *testing.B) {
	for _, names := range []int{64, 4096} {
		cc := NewCatalog()
		for i := 0; i < names; i++ {
			for v := int64(1); v <= 4; v++ {
				cc.setTableItem(&TableItem{AccountId: 1, DatabaseId: 10, Name: fmt.Sprintf("t%08d", i),
					Id: uint64(i + 1000), Ts: timestamp.Timestamp{PhysicalTime: v}}, false)
			}
		}
		for _, asked := range []int{1, 64} {
			ids := make([]uint64, asked)
			for i := range ids {
				ids[i] = uint64(1<<40) + uint64(i)
			}
			for _, mode := range []string{"scalar", "batch"} {
				b.Run(fmt.Sprintf("names=%d/asked=%d/%s", names, asked, mode), func(b *testing.B) {
					stamp := timestamp.Timestamp{PhysicalTime: 100}
					b.ReportAllocs()
					for i := 0; i < b.N; i++ {
						if mode == "batch" {
							tableIDLookupBenchmarkSink = cc.GetTablesByIdAndTime(1, 10, ids, stamp)
						} else {
							out := make([]*TableItem, len(ids))
							for j, id := range ids {
								out[j] = cc.GetTableByIdAndTime(1, 10, id, stamp)
							}
							tableIDLookupBenchmarkSink = out
						}
					}
					b.StopTimer()
					for _, item := range tableIDLookupBenchmarkSink {
						if item != nil {
							b.Fatal("missing ID unexpectedly found")
						}
					}
				})
			}
		}
	}
}
