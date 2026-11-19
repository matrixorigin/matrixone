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

package mpool

import (
	"encoding/json"
	"fmt"
	"sync"
	"testing"

	"github.com/stretchr/testify/require"
)

// Sum across call sites so the oracle does not depend on stack formatting.
func detailTotals(t *testing.T, mp *MPool) map[string][2]int64 {
	t.Helper()
	var report struct {
		Details map[string]map[string][2]int64 `json:"detailed_alloc"`
	}
	require.NoError(t, json.Unmarshal([]byte(mp.ReportJson()), &report))
	if report.Details == nil {
		return nil
	}
	totals := make(map[string][2]int64, len(report.Details))
	for kind, sites := range report.Details {
		var total [2]int64
		for _, info := range sites {
			total[0] += info[0]
			total[1] += info[1]
		}
		totals[kind] = total
	}
	return totals
}

func allocFreeDetailBuffers(t *testing.T, mp *MPool) {
	t.Helper()
	for _, offHeap := range []bool{false, true} {
		buffer, err := mp.Alloc(64, offHeap)
		require.NoError(t, err)
		mp.Free(buffer)
	}
}

func TestDetailRecordingLifecycle(t *testing.T) {
	mp := MustNew(t.Name())
	t.Cleanup(func() { DeleteMPool(mp) })

	allocFreeDetailBuffers(t, mp)
	require.Nil(t, detailTotals(t, mp))
	require.Equal(t, "ok", MPoolControl(mp.tag, "enable_detail"))
	empty := map[string][2]int64{
		"alloc": {}, "free": {}, "on_heap_alloc": {}, "on_heap_free": {},
	}
	require.Equal(t, empty, detailTotals(t, mp))
	allocFreeDetailBuffers(t, mp)
	want := map[string][2]int64{
		"alloc": {1, 64}, "free": {1, 64},
		"on_heap_alloc": {1, 64}, "on_heap_free": {1, 64},
	}
	require.Equal(t, want, detailTotals(t, mp))

	require.Equal(t, "ok", MPoolControl(mp.tag, "enable_detail"))
	require.Equal(t, want, detailTotals(t, mp), "repeated enable retains history")
	allocFreeDetailBuffers(t, mp)
	for kind := range want {
		want[kind] = [2]int64{2, 128}
	}
	require.Equal(t, want, detailTotals(t, mp))

	retired := mp.details.Load()
	require.Equal(t, "ok", MPoolControl(mp.tag, "disable_detail"))
	require.Equal(t, "ok", MPoolControl(mp.tag, "disable_detail"))
	allocFreeDetailBuffers(t, mp)
	require.Nil(t, detailTotals(t, mp))
	require.Equal(t, "ok", MPoolControl(mp.tag, "enable_detail"))
	require.Equal(t, empty, detailTotals(t, mp), "re-enable starts fresh")

	// An in-flight reader can finish on its retained object after withdrawal.
	retired.recordAlloc("retired", 64)
	retired.recordFree("retired", 64)
	retired.recordOnHeapAlloc("retired", 64)
	retired.recordOnHeapFree("retired", 64)
	require.Equal(t, empty, detailTotals(t, mp), "retired readers cannot contaminate the new generation")
	require.Zero(t, mp.CurrNB())
	bytes, objects := mp.OnHeapOutstanding()
	require.Zero(t, bytes)
	require.Zero(t, objects)
}

func TestDetailRecordingConcurrentEnable(t *testing.T) {
	mp := MustNew(t.Name())
	t.Cleanup(func() { DeleteMPool(mp) })
	mp.EnableDetailRecording()
	allocFreeDetailBuffers(t, mp)
	before := detailTotals(t, mp)

	start := make(chan struct{})
	var wait sync.WaitGroup
	for range 4 {
		wait.Add(1)
		go func() {
			defer wait.Done()
			<-start
			for range 16 {
				if result := MPoolControl(mp.tag, "enable_detail"); result != "ok" {
					t.Errorf("enable detail: %s", result)
				}
			}
		}()
	}
	close(start)
	wait.Wait()
	require.Equal(t, before, detailTotals(t, mp), "concurrent enables retain existing history")
}

func TestDetailRecordingConcurrentToggle(t *testing.T) {
	registry, account := newTestAllocationAccount(t, 64, 1)
	t.Cleanup(func() { finalizeTestAllocationAccount(t, registry, account) })
	mp := MustNew(t.Name())
	t.Cleanup(func() { DeleteMPool(mp) })

	const rounds = 100
	start := make(chan struct{})
	var wait sync.WaitGroup
	for kind := range 3 {
		wait.Add(1)
		go func() {
			defer wait.Done()
			<-start
			for range rounds {
				var buffer []byte
				var err error
				if kind == 2 {
					buffer, err = mp.AllocAccounted(64, account, testAllocationOwner, testAllocationSite)
				} else {
					buffer, err = mp.Alloc(64, kind == 1)
				}
				if err != nil {
					t.Errorf("allocation kind %d: %v", kind, err)
					return
				}
				buffer[0] = byte(kind)
				mp.Free(buffer)
			}
		}()
	}
	// Two enablers also contend for publication when the pool is disabled.
	for _, command := range []string{"enable_detail", "enable_detail", "disable_detail"} {
		wait.Add(1)
		go func() {
			defer wait.Done()
			<-start
			for range rounds {
				if result := MPoolControl(mp.tag, command); result != "ok" {
					t.Errorf("%s: %s", command, result)
				}
			}
		}()
	}
	wait.Add(1)
	go func() {
		defer wait.Done()
		<-start
		for range rounds {
			for _, report := range []string{mp.ReportJson(), ReportMemUsage(mp.tag)} {
				if !json.Valid([]byte(report)) {
					t.Errorf("invalid memory report: %s", report)
				}
			}
		}
	}()
	close(start)
	wait.Wait()

	// Detail counts across toggles are best-effort; actual ownership must balance.
	require.Equal(t, int64(2*rounds), mp.Stats().NumAlloc.Load())
	require.Equal(t, int64(2*rounds), mp.Stats().NumFree.Load())
	require.Zero(t, mp.CurrNB())
	bytes, objects := mp.OnHeapOutstanding()
	require.Zero(t, bytes)
	require.Zero(t, objects)
	require.Zero(t, account.Snapshot().Used)
	require.Zero(t, registry.LiveAllocationMetadata())

	// A stable generation after the competition must still record every path.
	mp.DisableDetailRecording()
	require.Nil(t, detailTotals(t, mp))
	mp.EnableDetailRecording()
	allocFreeDetailBuffers(t, mp)
	buffer, err := mp.AllocAccounted(64, account, testAllocationOwner, testAllocationSite)
	require.NoError(t, err)
	mp.Free(buffer)
	require.Equal(t, map[string][2]int64{
		"alloc": {2, 128}, "free": {2, 128},
		"on_heap_alloc": {1, 64}, "on_heap_free": {1, 64},
	}, detailTotals(t, mp))
	require.Zero(t, mp.CurrNB())
	require.Zero(t, account.Snapshot().Used)
	require.Zero(t, registry.LiveAllocationMetadata())
}

func TestDetailRecordingCrossPoolFree(t *testing.T) {
	for _, offHeap := range []bool{false, true} {
		t.Run(fmt.Sprintf("offheap=%t", offHeap), func(t *testing.T) {
			owner := MustNew(t.Name() + "-owner")
			t.Cleanup(func() { DeleteMPool(owner) })
			freer := MustNew(t.Name() + "-freer")
			t.Cleanup(func() { DeleteMPool(freer) })
			owner.EnableDetailRecording()
			buffer, err := owner.Alloc(64, offHeap)
			require.NoError(t, err)
			freer.Free(buffer)

			want := map[string][2]int64{
				"alloc": {}, "free": {}, "on_heap_alloc": {}, "on_heap_free": {},
			}
			if offHeap {
				want["alloc"], want["free"] = [2]int64{1, 64}, [2]int64{1, 64}
			} else {
				want["on_heap_alloc"], want["on_heap_free"] = [2]int64{1, 64}, [2]int64{1, 64}
			}
			require.Equal(t, want, detailTotals(t, owner))
			require.Nil(t, detailTotals(t, freer))
			require.Zero(t, owner.CurrNB())
			bytes, objects := owner.OnHeapOutstanding()
			require.Zero(t, bytes)
			require.Zero(t, objects)
			require.Equal(t, int64(1), freer.Stats().NumCrossPoolFree.Load())
		})
	}
}
