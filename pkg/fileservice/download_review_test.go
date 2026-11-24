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
				active := slices.Clone(objects.m[d.pathForFile("object")])
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
			bystanders := d.conns.max - 1
			d.conns.held.Store(bystanders)

			opened := make(chan int64, 2)
			storage := newBlackBoxStorage(fs.storage, func(_ string, start int64) bool {
				opened <- start
				return true
			})
			fs.storage = storage
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
			require.Equal(t, d.conns.max, d.conns.held.Load())
			unblock()
			require.NoError(t, <-result)
			require.Equal(t, bystanders, d.conns.held.Load())
			require.Equal(t, offset, first, "with one connection available the read's part must start first")
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
			storage := newBlackBoxStorage(fs.storage, func(_ string, offset int64) bool {
				if offset != 0 {
					return false
				}
				opened()
				return true
			})
			fs.storage = storage
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
			download, _ := d.fileDownload(d.pathForFile("object"), 16, 80)
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
