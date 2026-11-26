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
	"context"
	"io"
	"sync"
	"testing"
	"time"

	"github.com/matrixorigin/matrixone/pkg/fileservice/fscache"
	"github.com/stretchr/testify/require"
)

func TestPrefetchBufferReleasedBeforeDecode(t *testing.T) {
	for _, blockDecode := range []bool{false, true} {
		name := "completed_decode"
		if blockDecode {
			name = "blocked_decode_after_copy"
		}
		t.Run(name, func(t *testing.T) {
			const budget = 64 << 10
			const size = 4 << 10
			fs, _, data := newPrefetchRangeFS(t, 0, 64<<10)
			d := fs.diskCache
			<-d.load.done
			d.prefetchFlightBytes = 2 * budget
			ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
			defer cancel()
			require.NoError(t, fs.Write(ctx, IOVector{FilePath: "bystander", Policy: SkipAllCache,
				Entries: []IOEntry{{Size: size, Data: data[:size]}}}))
			storage := newBlackBoxStorage(fs.server.raw, func(key string, _ int64) bool { return key == fs.pathToKey("object") })
			fs.server.raw = storage
			resumeStorage := sync.OnceFunc(func() { close(storage.gate) })
			defer resumeStorage()
			require.NoError(t, fs.PrefetchObject(ctx, ObjectPrefetch{FilePath: "object", Size: size,
				Ranges: []PrefetchRange{{Size: size}}}))
			require.Eventually(t, func() bool {
				cur, _, _ := storage.counts(fs.pathToKey("object"))
				return cur == 1
			}, time.Second, time.Millisecond)

			decoding := make(chan struct{})
			resumeDecode := make(chan struct{})
			unblock := sync.OnceFunc(func() { close(resumeDecode) })
			if !blockDecode {
				unblock()
			}
			vec := &IOVector{FilePath: "object", Policy: ExactColumnsRead | SkipMemoryCache,
				Entries: []IOEntry{{Size: size, ToCacheData: func(ctx context.Context, r io.Reader, data []byte, alloc CacheDataAllocator) (fscache.Data, error) {
					close(decoding)
					<-resumeDecode
					return CacheOriginalData(ctx, r, data, alloc)
				}}}}
			defer vec.Release()
			done := make(chan struct{})
			var readErr error
			go func() {
				defer close(done)
				readErr = fs.Read(ctx, vec)
			}()
			defer func() {
				unblock()
				resumeStorage()
				<-done
				require.NoError(t, readErr)
			}()
			require.Eventually(t, func() bool {
				return memoryWaiters(fs, d.objectKey("object")) == 1
			}, time.Second, time.Millisecond, "the read should be joined to the prefetch")
			resumeStorage()
			select {
			case <-decoding:
			case <-ctx.Done():
				t.Fatal("the reader did not reach the decoder")
			}
			waitPrefetcherIdle(t, fs)
			d.prefetchObjects.reads.Wait()
			if !blockDecode {
				<-done
			}

			// The rest of the budget is held, so the bystander gets a buffer only once the
			// prefetch released its own.
			holdForegroundBuffer(t, fs, budget)
			other := &IOVector{FilePath: "bystander", Policy: ExactColumnsRead | SkipMemoryCache | SkipDiskCacheWrites,
				Entries: []IOEntry{{Size: size}}}
			defer other.Release()
			otherDone := make(chan struct{})
			var otherErr error
			go func() {
				defer close(otherDone)
				otherErr = fs.Read(ctx, other)
			}()
			defer func() {
				unblock()
				<-otherDone
				require.NoError(t, otherErr)
				require.Equal(t, data[:size], other.Entries[0].Data)
			}()
			var charged int64
			require.Eventually(t, func() bool {
				select {
				case <-otherDone:
					return true
				default:
				}
				f := &d.prefetchObjects
				f.mu.Lock()
				defer f.mu.Unlock()
				for o := range f.waiting {
					if target := o.target(); target.filePath == "bystander" && target.state == objectWaitingBuffer {
						charged = f.bytes
						return true
					}
				}
				return false
			}, time.Second, time.Millisecond)
			require.LessOrEqual(t, charged, int64(budget), "the bystander should finish while the decoder runs on its own copy")
		})
	}
}
