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
