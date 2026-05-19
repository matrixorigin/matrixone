// Copyright 2024 Matrix Origin
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

package fileservice

import (
	"context"
	"os"
	"runtime"
	"runtime/debug"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/matrixorigin/matrixone/pkg/common/malloc"
)

// TestConcurrentIOAllocatorPressure tests the ioAllocator under concurrent pressure.
// Models the TPCC OOM scenario (issue #24348):
// 1000 terminals × FOR UPDATE → TableScan → DiskCache.Read → prepareData → ioAllocator().Allocate
func TestConcurrentIOAllocatorPressure(t *testing.T) {
	const (
		numGoroutines = 1000
		blockSize     = 65536                 // 64KB per block
		holdDuration  = 50 * time.Millisecond // time data is held during scan
		testDuration  = 5 * time.Second
	)

	alloc := ioAllocator()

	var (
		totalAllocs  atomic.Int64
		peakInFlight atomic.Int64
		curInFlight  atomic.Int64
		stopCh       = make(chan struct{})
		wg           sync.WaitGroup
	)

	wg.Add(1)
	go func() {
		defer wg.Done()
		for {
			select {
			case <-stopCh:
				return
			case <-time.After(10 * time.Millisecond):
				cur := curInFlight.Load()
				for {
					old := peakInFlight.Load()
					if cur <= old {
						break
					}
					if peakInFlight.CompareAndSwap(old, cur) {
						break
					}
				}
			}
		}
	}()

	for i := 0; i < numGoroutines; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for {
				select {
				case <-stopCh:
					return
				default:
				}

				slice, dec, err := alloc.Allocate(uint64(blockSize), malloc.NoHints)
				if err != nil {
					t.Error(err)
					return
				}
				_ = slice
				curInFlight.Add(1)
				totalAllocs.Add(1)

				time.Sleep(holdDuration)

				dec.Deallocate()
				curInFlight.Add(-1)
			}
		}()
	}

	time.Sleep(testDuration)
	close(stopCh)
	wg.Wait()

	peakMB := float64(peakInFlight.Load()) * float64(blockSize) / 1024 / 1024

	t.Logf("Results (hold=%v):", holdDuration)
	t.Logf("  Goroutines: %d, Block size: %d KB", numGoroutines, blockSize/1024)
	t.Logf("  Total allocs: %d", totalAllocs.Load())
	t.Logf("  Peak in-flight: %d (= %.1f MB)", peakInFlight.Load(), peakMB)

	s3Latency := 200 * time.Millisecond
	scaleFactor := float64(s3Latency) / float64(holdDuration)
	t.Logf("\n  === Projected OOM Model (S3 latency=%v) ===", s3Latency)
	t.Logf("  Scale factor: %.1fx", scaleFactor)
	t.Logf("  Projected peak (single block): %.0f MB", peakMB*scaleFactor)
	t.Logf("  Projected peak (10 blocks/scan): %.0f MB", peakMB*scaleFactor*10)
}

// TestLockOpHoldingCacheDataOOM reproduces the exact OOM mechanism from issue #24348:
//
// Path: LockOp.callNonBlocking → TableScan → DiskCache.Read → setCachedData
//
//	→ constructorFactory → AllocateCacheDataWithHint → memoryCacheAllocator.Allocate
//
// The key insight: after setCachedData allocates CachedData, it gets stored in memory cache
// (Retain, refs=2), then MustVectorTo does zero-copy UnmarshalBinary (referencing CachedData.Bytes()),
// then Union copies data to outputBat via mpool. The CachedData is released after BlockDataReadInner.
//
// BUT the mpool-allocated batch data is held on the goroutine stack during lockWithRetry.
// 1000 concurrent transactions × N blocks/scan × ~683KB/block = OOM.
//
// This test simulates: allocate cache data (same allocator as crash path), hold for lock-wait
// duration to model transaction-bound lifetime.
//
// Run with: MO_TEST_OOM_REPRO=1 go test -run TestLockOpHoldingCacheDataOOM -v -timeout 120s
func TestLockOpHoldingCacheDataOOM(t *testing.T) {
	if os.Getenv("MO_TEST_OOM_REPRO") == "" {
		t.Skip("Set MO_TEST_OOM_REPRO=1 to run OOM reproduction test")
	}

	const (
		numGoroutines = 1000
		// Each FOR UPDATE scan reads ~10 blocks of decompressed column data
		// In TPCC stock table: ~683KB per block (from EXPLAIN ANALYZE ReadSize)
		blocksPerScan = 10
		blockSize     = 700 * 1024 // ~683KB rounded up

		// Lock wait time: simulates distributed lock contention
		// In production: lockWithRetry blocks 200ms-2s waiting for lock service
		lockWaitTime = 500 * time.Millisecond

		testDuration = 30 * time.Second
	)

	// Use the EXACT same allocator as the crash path:
	// S3FS.AllocateCacheDataWithHint → DefaultCacheDataAllocator() → bytesAllocator{memoryCacheAllocator()}
	alloc := memoryCacheAllocator()

	runtime.GC()
	var memBefore runtime.MemStats
	runtime.ReadMemStats(&memBefore)

	var (
		peakBytes  atomic.Int64
		curBytes   atomic.Int64
		totalScans atomic.Int64
		stopCh     = make(chan struct{})
		wg         sync.WaitGroup
	)

	// Peak tracker
	wg.Add(1)
	go func() {
		defer wg.Done()
		for {
			select {
			case <-stopCh:
				return
			case <-time.After(50 * time.Millisecond):
				cur := curBytes.Load()
				for {
					old := peakBytes.Load()
					if cur <= old {
						break
					}
					if peakBytes.CompareAndSwap(old, cur) {
						break
					}
				}
			}
		}
	}()

	// Progress reporter
	wg.Add(1)
	go func() {
		defer wg.Done()
		ticker := time.NewTicker(3 * time.Second)
		defer ticker.Stop()
		for {
			select {
			case <-stopCh:
				return
			case <-ticker.C:
				t.Logf("  in-flight=%d MB  peak=%d MB  scans=%d",
					curBytes.Load()/1024/1024,
					peakBytes.Load()/1024/1024,
					totalScans.Load())
			}
		}
	}()

	// Simulate 1000 concurrent FOR UPDATE transactions
	for i := 0; i < numGoroutines; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			ctx := context.Background()
			for {
				select {
				case <-stopCh:
					return
				default:
				}

				// === Phase 1: TableScan (allocate cache data for each block) ===
				// Models: readBlockData → LoadColumnsData → ReadOneBlock → S3FS.Read
				//   → DiskCache.Read → setCachedData → constructorFactory
				//   → AllocateCacheDataWithHint (THIS is the 225 GiB allocation in crash)
				decs := make([]malloc.Deallocator, 0, blocksPerScan)
				scanSize := int64(0)

				for b := 0; b < blocksPerScan; b++ {
					// This is exactly: memoryCacheAllocator().Allocate()
					// Same as: bytesAllocator.allocateCacheData → MetricsAllocator.Allocate
					slice, dec, err := alloc.Allocate(uint64(blockSize), malloc.NoClear)
					if err != nil {
						t.Error(err)
						return
					}
					// Touch memory to ensure it's actually mapped
					_ = ctx
					for j := 0; j < len(slice); j += 4096 {
						slice[j] = byte(j)
					}
					decs = append(decs, dec)
					scanSize += int64(blockSize)
					curBytes.Add(int64(blockSize))
				}

				// === Phase 2: Lock Wait (hold data while waiting for distributed lock) ===
				// Models: performLock → doLock → lockWithRetry → lockService.Lock(ctx, ...)
				// In production, this blocks 200ms-2s waiting for lock service response
				// During this time, the batch (containing mpool copies of cached data) is live
				time.Sleep(lockWaitTime)

				totalScans.Add(1)

				// === Phase 3: Release (transaction completes or batch is consumed) ===
				for _, dec := range decs {
					dec.Deallocate()
				}
				curBytes.Add(-scanSize)
			}
		}()
	}

	time.Sleep(testDuration)
	close(stopCh)
	wg.Wait()

	runtime.GC()
	var memAfter runtime.MemStats
	runtime.ReadMemStats(&memAfter)

	peakMB := peakBytes.Load() / 1024 / 1024
	expectedMB := int64(numGoroutines) * int64(blocksPerScan) * int64(blockSize) / 1024 / 1024

	t.Logf("\n=== OOM Reproduction Results ===")
	t.Logf("  Params: %d goroutines × %d blocks/scan × %d KB/block × %v lock_wait",
		numGoroutines, blocksPerScan, blockSize/1024, lockWaitTime)
	t.Logf("  Peak in-flight: %d MB", peakMB)
	t.Logf("  Theoretical max: %d MB (all goroutines holding simultaneously)", expectedMB)
	t.Logf("  Memory growth: Sys=%dMB HeapInuse=%dMB",
		(memAfter.Sys-memBefore.Sys)/1024/1024,
		(memAfter.HeapInuse-memBefore.HeapInuse)/1024/1024)
	t.Logf("\n  === Why this causes OOM ===")
	t.Logf("  Production scenario: 1000 TPCC terminals all do FOR UPDATE simultaneously")
	t.Logf("  Each scan: ~%d blocks × %d KB = %d KB held during lock wait",
		blocksPerScan, blockSize/1024, blocksPerScan*blockSize/1024)
	t.Logf("  All concurrent: %d × %d KB = %d MB",
		numGoroutines, blocksPerScan*blockSize/1024, expectedMB)
	t.Logf("  With S3 latency adding more in-flight time, actual peak exceeds CN memory limit")

	if peakMB > int64(float64(expectedMB)*0.5) {
		t.Logf("  CONFIRMED: Peak (%d MB) shows high concurrent memory pressure", peakMB)
	}
}

// TestMultiBlockScanOOMReproduction reproduces the exact OOM mechanism:
// each goroutine simulates a table scan that reads MULTIPLE blocks sequentially,
// holding all block data in memory until the scan completes.
// With S3 latency, the hold time per scan scales linearly with blocks-per-scan.
//
// Run with: MO_TEST_OOM_REPRO=1 go test -run TestMultiBlockScanOOMReproduction -v -timeout 60s
func TestMultiBlockScanOOMReproduction(t *testing.T) {
	if os.Getenv("MO_TEST_OOM_REPRO") == "" {
		t.Skip("Set MO_TEST_OOM_REPRO=1 to run OOM reproduction test")
	}

	const (
		numGoroutines   = 1000
		blockSize       = 65536
		blocksPerScan   = 10                    // blocks read per scan operation
		ioLatencyPerBlk = 20 * time.Millisecond // S3 read latency per block
		testDuration    = 30 * time.Second
	)

	alloc := ioAllocator()

	runtime.GC()
	var memBefore runtime.MemStats
	runtime.ReadMemStats(&memBefore)

	var (
		peakBytes  atomic.Int64
		curBytes   atomic.Int64
		totalScans atomic.Int64
		stopCh     = make(chan struct{})
		wg         sync.WaitGroup
	)

	wg.Add(1)
	go func() {
		defer wg.Done()
		for {
			select {
			case <-stopCh:
				return
			case <-time.After(50 * time.Millisecond):
				cur := curBytes.Load()
				for {
					old := peakBytes.Load()
					if cur <= old {
						break
					}
					if peakBytes.CompareAndSwap(old, cur) {
						break
					}
				}
			}
		}
	}()

	wg.Add(1)
	go func() {
		defer wg.Done()
		ticker := time.NewTicker(2 * time.Second)
		defer ticker.Stop()
		for {
			select {
			case <-stopCh:
				return
			case <-ticker.C:
				t.Logf("  in-flight=%d MB  peak=%d MB  scans=%d",
					curBytes.Load()/1024/1024,
					peakBytes.Load()/1024/1024,
					totalScans.Load())
			}
		}
	}()

	for i := 0; i < numGoroutines; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for {
				select {
				case <-stopCh:
					return
				default:
				}

				// Simulate a scan: read multiple blocks, hold all until scan completes
				decs := make([]malloc.Deallocator, 0, blocksPerScan)
				scanSize := int64(0)

				for b := 0; b < blocksPerScan; b++ {
					slice, dec, err := alloc.Allocate(uint64(blockSize), malloc.NoHints)
					if err != nil {
						return
					}
					_ = slice
					decs = append(decs, dec)
					scanSize += int64(blockSize)
					curBytes.Add(int64(blockSize))

					// S3 read latency per block
					time.Sleep(ioLatencyPerBlk)
				}

				totalScans.Add(1)

				// Release all blocks after scan completes
				for _, dec := range decs {
					dec.Deallocate()
				}
				curBytes.Add(-scanSize)
			}
		}()
	}

	time.Sleep(testDuration)
	close(stopCh)
	wg.Wait()

	runtime.GC()
	var memAfter runtime.MemStats
	runtime.ReadMemStats(&memAfter)

	t.Logf("\nResults:")
	t.Logf("  Peak in-flight = %d MB", peakBytes.Load()/1024/1024)
	t.Logf("  Params: %d goroutines × %d blocks/scan × %d KB/block × %v latency/block",
		numGoroutines, blocksPerScan, blockSize/1024, ioLatencyPerBlk)
	t.Logf("  Expected max: %d goroutines × %d blocks × %d KB = %d MB",
		numGoroutines, blocksPerScan, blockSize/1024,
		numGoroutines*blocksPerScan*blockSize/1024/1024)
	t.Logf("  Memory growth: Sys=%dMB HeapInuse=%dMB",
		(memAfter.Sys-memBefore.Sys)/1024/1024,
		(memAfter.HeapInuse-memBefore.HeapInuse)/1024/1024)
}

// TestTPCCRssPressureSimplifiedRepro is a small, single-process reproduction
// for the TPCC failure shape: many concurrent sessions hold Go heap and
// off-heap buffers at the same time, then GC leaves a large HeapIdle/RSS gap.
//
// It is skipped by default. Scale the env vars up to intentionally hit a small
// container limit, or keep them small to observe the RSS behavior safely.
//
// Example:
//
//	MO_TEST_RSS_REPRO=1 \
//	MO_TEST_RSS_WORKERS=64 \
//	MO_TEST_RSS_HEAP_MB=16 \
//	MO_TEST_RSS_OFFHEAP_MB=8 \
//	go test ./pkg/fileservice -run TestTPCCRssPressureSimplifiedRepro -v -count=1
func TestTPCCRssPressureSimplifiedRepro(t *testing.T) {
	if os.Getenv("MO_TEST_RSS_REPRO") == "" {
		t.Skip("Set MO_TEST_RSS_REPRO=1 to run simplified RSS/OOM reproduction")
	}

	workers := envInt("MO_TEST_RSS_WORKERS", 64)
	heapMB := envInt("MO_TEST_RSS_HEAP_MB", 16)
	offheapMB := envInt("MO_TEST_RSS_OFFHEAP_MB", 8)
	hold := time.Duration(envInt("MO_TEST_RSS_HOLD_MS", 3000)) * time.Millisecond
	if limitMB := envInt("MO_TEST_RSS_GOMEMLIMIT_MB", 0); limitMB > 0 {
		old := debug.SetMemoryLimit(int64(limitMB) << 20)
		defer debug.SetMemoryLimit(old)
	}

	t.Logf("params: workers=%d heap=%dMiB/worker offheap=%dMiB/worker hold=%v",
		workers, heapMB, offheapMB, hold)
	logRSSAndHeap(t, "before")

	alloc := memoryCacheAllocator()
	var (
		ready sync.WaitGroup
		done  sync.WaitGroup
		stop  = make(chan struct{})
		sink  atomic.Uint64
	)
	ready.Add(workers)
	done.Add(workers)
	for i := 0; i < workers; i++ {
		go func(worker int) {
			defer done.Done()

			heap := make([]byte, heapMB<<20)
			touchBytes(heap, byte(worker), &sink)

			offheap, dec, err := alloc.Allocate(uint64(offheapMB<<20), malloc.NoClear)
			if err != nil {
				t.Errorf("offheap allocate failed: %v", err)
				ready.Done()
				return
			}
			touchBytes(offheap, byte(worker+1), &sink)

			ready.Done()
			<-stop

			runtime.KeepAlive(heap)
			runtime.KeepAlive(offheap)
			dec.Deallocate()
		}(i)
	}
	ready.Wait()
	logRSSAndHeap(t, "all workers allocated")

	time.Sleep(hold)
	close(stop)
	done.Wait()
	t.Logf("touch checksum=%d", sink.Load())

	runtime.GC()
	logRSSAndHeap(t, "after runtime.GC")

	debug.FreeOSMemory()
	logRSSAndHeap(t, "after FreeOSMemory")
}

func envInt(name string, fallback int) int {
	value := os.Getenv(name)
	if value == "" {
		return fallback
	}
	n, err := strconv.Atoi(value)
	if err != nil || n < 0 {
		return fallback
	}
	return n
}

func touchBytes(bs []byte, seed byte, sink *atomic.Uint64) {
	var sum uint64
	for i := 0; i < len(bs); i += 4096 {
		bs[i] = seed + byte(i)
		sum += uint64(bs[i])
	}
	if len(bs) > 0 {
		bs[len(bs)-1] = seed
		sum += uint64(bs[len(bs)-1])
	}
	sink.Add(sum)
}

func logRSSAndHeap(t *testing.T, label string) {
	t.Helper()
	var stats runtime.MemStats
	runtime.ReadMemStats(&stats)
	rss := currentRSSBytes()
	t.Logf("%s: rss=%dMiB heapAlloc=%dMiB heapSys=%dMiB heapIdle=%dMiB heapReleased=%dMiB heapInuse=%dMiB numGC=%d",
		label,
		rss>>20,
		stats.HeapAlloc>>20,
		stats.HeapSys>>20,
		stats.HeapIdle>>20,
		stats.HeapReleased>>20,
		stats.HeapInuse>>20,
		stats.NumGC,
	)
}

func currentRSSBytes() uint64 {
	data, err := os.ReadFile("/proc/self/statm")
	if err != nil {
		return 0
	}
	fields := strings.Fields(string(data))
	if len(fields) < 2 {
		return 0
	}
	pages, err := strconv.ParseUint(fields[1], 10, 64)
	if err != nil {
		return 0
	}
	return pages * uint64(os.Getpagesize())
}
