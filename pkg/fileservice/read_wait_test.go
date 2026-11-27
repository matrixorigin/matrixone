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
	"strings"
	"testing"
	"testing/synctest"
	"time"

	"github.com/matrixorigin/matrixone/pkg/defines"
	"github.com/matrixorigin/matrixone/pkg/fileservice/fscache"
	"github.com/stretchr/testify/require"
)

func TestS3FSReadWaitExcludesCacheAndDecode(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		const (
			requestTime = 2 * time.Millisecond
			bodyTime    = 3 * time.Millisecond
			decodeTime  = 7 * time.Millisecond
		)
		payload := strings.NewReader("data")
		storage := &readTestObjectStorage{open: func(context.Context, string, *int64, *int64, *ObjectIdentity) (io.ReadCloser, error) {
			time.Sleep(requestTime)
			return io.NopCloser(readerFunc(func(p []byte) (int, error) {
				if payload.Len() > 0 {
					time.Sleep(bodyTime)
				}
				return payload.Read(p)
			})), nil
		}}
		fs := &S3FS{name: "test", storage: storage, ioMerger: NewIOMerger()}
		ctx, tracker := WithReadWaitTracking(t.Context())
		vec := &IOVector{FilePath: "object", Entries: []IOEntry{{
			Size: -1,
			ToCacheData: func(ctx context.Context, r io.Reader, data []byte, alloc CacheDataAllocator) (fscache.Data, error) {
				time.Sleep(decodeTime)
				return CacheOriginalData(ctx, r, data, alloc)
			},
		}}}
		defer vec.Release()

		start := time.Now()
		require.NoError(t, fs.Read(ctx, vec))
		require.Equal(t, "data", string(vec.Entries[0].CachedData.Bytes()))
		require.Equal(t, requestTime+bodyTime+decodeTime, time.Since(start))
		require.Equal(t, requestTime+bodyTime, tracker.Elapsed())

		cache := &callbackVectorCache{onRead: func(v *IOVector) {
			v.Entries[0].Data = []byte("data")
			v.Entries[0].done = true
		}}
		cached := &IOVector{FilePath: "object", Caches: []IOVectorCache{cache}, Entries: []IOEntry{{Size: 4}}}
		defer cached.Release()
		require.NoError(t, fs.Read(ctx, cached))
		require.Equal(t, "data", string(cached.Entries[0].Data))
		require.Equal(t, requestTime+bodyTime, tracker.Elapsed())
	})
}

func TestS3FSReadWaitIncludesCancelledRequest(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		const waitTime = 5 * time.Millisecond
		storage := &readTestObjectStorage{open: func(ctx context.Context, _ string, _, _ *int64, _ *ObjectIdentity) (io.ReadCloser, error) {
			<-ctx.Done()
			return nil, ctx.Err()
		}}
		fs := &S3FS{name: "test", storage: storage, ioMerger: NewIOMerger()}
		ctx, cancel := context.WithTimeout(t.Context(), waitTime)
		defer cancel()
		ctx, tracker := WithReadWaitTracking(ctx)
		vec := &IOVector{FilePath: "object", Entries: []IOEntry{{Size: 4}}}
		defer vec.Release()

		require.ErrorIs(t, fs.Read(ctx, vec), context.DeadlineExceeded)
		require.Equal(t, waitTime, tracker.Elapsed())
		require.ErrorIs(t, fs.Read(ctx, vec), context.DeadlineExceeded)
		require.Equal(t, waitTime, tracker.Elapsed(), "an expired request must return before storage")
	})
}

func TestConfiguredFileServiceReadWait(t *testing.T) {
	for _, backend := range []string{memFileServiceBackend, diskETLFileServiceBackend, diskTmpFileServiceBackend} {
		t.Run(backend, func(t *testing.T) {
			ctx := t.Context()
			fs, err := NewFileService(ctx, Config{
				Name: defines.SharedFileServiceName, Backend: backend, DataDir: t.TempDir(),
				Cache: DisabledCacheConfig,
			}, nil)
			require.NoError(t, err)
			t.Cleanup(func() { fs.Close(ctx) })
			require.NoError(t, fs.Write(ctx, IOVector{
				FilePath: "object", Entries: []IOEntry{{Size: 4, Data: []byte("data")}},
			}))

			ctx, tracker := WithReadWaitTracking(ctx)
			var beforeDecode time.Duration
			vec := &IOVector{FilePath: "object", Entries: []IOEntry{{
				Offset: 1,
				Size:   3,
				ToCacheData: func(ctx context.Context, r io.Reader, data []byte, alloc CacheDataAllocator) (fscache.Data, error) {
					beforeDecode = tracker.Elapsed()
					return CacheOriginalData(ctx, r, data, alloc)
				},
			}}}
			defer vec.Release()
			require.NoError(t, fs.Read(ctx, vec))
			require.Equal(t, "ata", string(vec.Entries[0].CachedData.Bytes()))
			require.Equal(t, beforeDecode, tracker.Elapsed())
			if backend == memFileServiceBackend {
				require.Zero(t, tracker.Elapsed())
			} else {
				require.Positive(t, tracker.Elapsed())
			}
		})
	}
}
