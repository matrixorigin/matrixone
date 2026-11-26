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

package fileservice

import (
	"bytes"
	"context"
	"fmt"
	"slices"
	"sync"
	"testing"
	"time"

	"github.com/matrixorigin/matrixone/pkg/util/toml"
	"github.com/stretchr/testify/require"
)

func TestCloseCancelsUnclaimedPrefetchPublication(t *testing.T) {
	for _, tc := range []struct {
		name string
		size int64
	}{
		{name: "ordinary_async_entry", size: 64 << 10},
		{name: "file_backed_range", size: 144 << 10},
		{name: "memory_backed_range", size: 64 << 10},
	} {
		t.Run(tc.name, func(t *testing.T) {
			ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
			defer cancel()
			fs, _, data := newPrefetchRangeFS(t, 0, 256<<10)
			d := fs.diskCache
			d.memoryPrefetchMax = 128 << 10
			<-d.load.done

			atPublication := make(chan struct{})
			resume := make(chan struct{})
			release := sync.OnceFunc(func() { close(resume) })
			defer release()
			d.hooks.beforeFilePublication = func() {
				close(atPublication)
				<-resume
			}
			if tc.name == "ordinary_async_entry" {
				vec := &IOVector{FilePath: "object", Entries: []IOEntry{{Size: tc.size, Data: data[:tc.size]}}}
				require.NoError(t, d.Update(ctx, vec, true))
			} else {
				require.NoError(t, fs.PrefetchObject(ctx, ObjectPrefetch{FilePath: "object", Size: tc.size,
					Ranges: []PrefetchRange{{Size: tc.size}}}))
			}
			select {
			case <-atPublication:
			case <-ctx.Done():
				t.Fatal(ctx.Err())
			}
			canceled := d.async.ctx.Done()
			if tc.name != "ordinary_async_entry" {
				objects := &d.prefetchObjects
				objects.mu.Lock()
				active := slices.Clone(objects.m[d.objectKey("object")])
				objects.mu.Unlock()
				require.Len(t, active, 1)
				switch flight := active[0].(type) {
				case *memoryPrefetchObject:
					canceled = flight.ctx.Done()
				case *filePrefetchObject:
					canceled = flight.ctx.Done()
				default:
					t.Fatalf("unexpected range owner %T", flight)
				}
			}

			closed := make(chan struct{})
			go func() {
				d.Close(ctx)
				close(closed)
			}()
			select {
			case <-canceled:
			case <-ctx.Done():
				t.Fatal(ctx.Err())
			}
			release()
			select {
			case <-closed:
				require.NoError(t, ctx.Err())
			case <-ctx.Done():
				t.Fatal(ctx.Err())
			}

			path := d.pathForIOEntry("object", IOEntry{Size: tc.size})
			require.NoFileExists(t, path, "close canceled the flight before it could claim publication")
		})
	}
}

func TestSplitReadReservesFirstConnectionForNeededPart(t *testing.T) {
	for _, offset := range []int64{0, 128 << 10} {
		name := "needed_prefix"
		if offset > 0 {
			name = "needed_suffix"
		}
		t.Run(name, func(t *testing.T) {
			ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
			defer cancel()
			fs, _, data := newObjectSplitFS(t, objectSplitConfig{parts: 2, min: 128 << 10}, 300<<10)
			d := fs.diskCache
			<-d.load.done
			limitRoom(t, fs.server, 1, 1)

			opened := make(chan int64, 2)
			storage := newBlackBoxStorage(fs.server.raw, func(_ string, start int64) bool {
				opened <- start
				return true
			})
			fs.server.raw = storage
			unblock := sync.OnceFunc(func() { close(storage.gate) })
			defer unblock()

			result := make(chan error, 1)
			go func() {
				vec := &IOVector{FilePath: "object", Entries: []IOEntry{{Offset: offset, Size: 16}}}
				defer vec.Release()
				err := fs.Read(ctx, vec)
				if err == nil && !bytes.Equal(data[offset:offset+16], vec.Entries[0].Data) {
					err = fmt.Errorf("incorrect bytes at offset %d", offset)
				}
				result <- err
			}()

			var first int64
			select {
			case first = <-opened:
			case <-ctx.Done():
				t.Fatal(ctx.Err())
			}
			require.Equal(t, 1, heldRoom(fs.server))
			unblock()
			require.NoError(t, <-result)
			require.Zero(t, heldRoom(fs.server))
			require.Equal(t, offset, first, "with room for one the read's part must start first")
		})
	}
}

func TestFileRangeRespectsZeroExtraWait(t *testing.T) {
	for _, tc := range []struct {
		name   string
		policy Policy
	}{
		{name: "bypass_disk_reads", policy: SkipDiskCacheReads},
		{name: "can_read_disk_cache"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			rangeCosts(t, 0.001, 100<<20)
			downloadsRate(t, 1024)
			ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
			defer cancel()
			fs, _, data := newPrefetchRangeFS(t, 0, 256<<10)
			d := fs.diskCache
			d.memoryPrefetchMax = 128 << 10
			d.downloadMaxExtraWait = 0
			<-d.load.done

			open := make(chan struct{})
			opened := sync.OnceFunc(func() { close(open) })
			storage := newBlackBoxStorage(fs.server.raw, func(_ string, offset int64) bool {
				if offset != 0 {
					return false
				}
				opened()
				return true
			})
			fs.server.raw = storage
			unblock := sync.OnceFunc(func() { close(storage.gate) })
			defer unblock()
			const size = 192 << 10
			require.NoError(t, fs.PrefetchObject(ctx, ObjectPrefetch{FilePath: "object", Size: size,
				Ranges: []PrefetchRange{{Size: size}}}))
			select {
			case <-open:
			case <-ctx.Done():
				t.Fatal(ctx.Err())
			}
			download, _ := d.fileDownload(d.objectKey("object"), 16, 80)
			require.NotNil(t, download)
			_, eta := download.estimate(16, 80)
			require.Greater(t, eta, rangeReadCosts.cost(64))

			vec := &IOVector{FilePath: "object", Policy: ExactColumnsRead | SkipDiskCacheWrites | tc.policy,
				Entries: []IOEntry{{Offset: 16, Size: 64}}}
			defer vec.Release()
			readDone := make(chan error, 1)
			readFinished := make(chan struct{})
			go func() {
				defer close(readFinished)
				readDone <- fs.Read(ctx, vec)
			}()
			defer func() {
				unblock()
				<-readFinished
			}()
			var queuedWaiters int
			require.Eventually(t, func() bool {
				select {
				case <-readFinished:
					return true
				default:
				}
				download.mu.Lock()
				queuedWaiters = len(download.waiters)
				download.mu.Unlock()
				return queuedWaiters > 0
			}, 4*time.Second, time.Millisecond)

			unblock()
			require.NoError(t, <-readDone)
			require.Equal(t, data[16:80], vec.Entries[0].Data)
			require.Zero(t, queuedWaiters, "a read joined the file prefetch despite its ETA exceeding the zero extra-wait budget")
		})
	}
}

func TestFlightBudgetDoesNotOverflow(t *testing.T) {
	for _, tc := range []struct {
		name  string
		limit toml.ByteSize
	}{
		{name: "default_sized_limit", limit: DefaultDiskPrefetchMemoryMax},
		{name: "large_limit", limit: 1 << 57},
	} {
		t.Run(tc.name, func(t *testing.T) {
			prior := memoryAvailable
			memoryAvailable = func() (uint64, bool) { return 1 << 30, true }
			t.Cleanup(func() { memoryAvailable = prior })
			cfg := CacheConfig{DiskPrefetchMemoryMax: &tc.limit}
			fs, _, _ := newPrefetchRangeFS(t, 0, 1)
			fs.diskCache.memoryPrefetchMax = cfg.diskPrefetchMemoryMax()
			require.EqualValues(t, (uint64(1)<<30)/100*5, fs.diskCache.flightBytes(), "the measured ceiling should constrain an oversized configured ceiling")
		})
	}
}

// Overlapping exact reads larger than disk-prefetch-memory-max share one file-backed
// prefetch object (decision 2026-11-23).
func TestExactReadsShareLargeRange(t *testing.T) {
	for _, tc := range []struct {
		name string
		size int64
	}{
		{name: "memory_sized_range", size: 64 << 10},
		{name: "file_sized_range", size: 128<<10 + 1},
	} {
		t.Run(tc.name, func(t *testing.T) {
			ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
			defer cancel()
			fs, _, data := newPrefetchRangeFS(t, 0, 256<<10)
			fs.diskCache.memoryPrefetchMax = 128 << 10
			// A read joins a FilePrefetchObject only within the extra wait of a range read;
			// the stalled download here is joined only under a budget that covers it.
			fs.diskCache.downloadMaxExtraWait = time.Hour
			<-fs.diskCache.load.done

			// Exact column reads skip the grid. Use distinct start offsets so the
			// identical-request I/O merger leaves deduplication to the range owner.
			started := make(chan struct{}, 2)
			gate := newBlackBoxStorage(fs.server.raw, func(string, int64) bool {
				select {
				case started <- struct{}{}:
				default:
				}
				return true
			})
			fs.server.raw = gate
			unblock := sync.OnceFunc(func() { close(gate.gate) })
			defer unblock()
			read := func(offset, size int64, result chan<- error) {
				vec := &IOVector{FilePath: "object", Policy: ExactColumnsRead,
					Entries: []IOEntry{{Offset: offset, Size: size}}}
				defer vec.Release()
				err := fs.Read(ctx, vec)
				if err == nil && !bytes.Equal(data[offset:offset+size], vec.Entries[0].Data) {
					err = fmt.Errorf("incorrect bytes at offset %d", offset)
				}
				result <- err
			}
			results := make(chan error, 2)
			go read(0, tc.size, results)
			select {
			case <-started:
			case <-ctx.Done():
				t.Fatal(ctx.Err())
			}
			go read(1, tc.size-1, results)
			// If the second reader opens its own range we have the counterexample.
			// Otherwise, observe it waiting for bytes on the registered range.
			require.Eventually(t, func() bool {
				if _, _, reads := gate.counts("object"); reads == 2 {
					return true
				}
				objects := &fs.diskCache.prefetchObjects
				objects.mu.Lock()
				defer objects.mu.Unlock()
				for _, object := range objects.m[fs.diskCache.objectKey("object")] {
					var waiters int
					switch flight := object.(type) {
					case *memoryPrefetchObject:
						flight.mu.Lock()
						waiters = len(flight.waiters)
						flight.mu.Unlock()
					case *filePrefetchObject:
						if download := flight.joinableDownload(); download != nil {
							download.mu.Lock()
							waiters = len(download.waiters)
							download.mu.Unlock()
						}
					}
					if waiters == 2 {
						return true
					}
				}
				return false
			}, 4*time.Second, time.Millisecond)
			unblock()
			for range 2 {
				require.NoError(t, <-results)
			}
			_, _, reads := gate.counts("object")
			require.Equal(t, 1, reads, "overlapping exact reads should share a registered download")
		})
	}
}
