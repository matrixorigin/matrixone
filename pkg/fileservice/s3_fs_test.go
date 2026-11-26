// Copyright 2022 Matrix Origin
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
	"encoding/csv"
	"encoding/xml"
	"errors"
	"io"
	"net/http"
	"net/http/httptest"
	"net/url"
	"os"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"testing/synctest"
	"time"

	"github.com/aliyun/aliyun-oss-go-sdk/oss"
	"github.com/matrixorigin/matrixone/pkg/common/moerr"
	"github.com/matrixorigin/matrixone/pkg/fileservice/fscache"
	"github.com/matrixorigin/matrixone/pkg/logutil"
	"github.com/matrixorigin/matrixone/pkg/perfcounter"
	"github.com/matrixorigin/matrixone/pkg/queryservice/client"
	"github.com/matrixorigin/matrixone/pkg/util/toml"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/tencentyun/cos-go-sdk-v5"
	"go.uber.org/zap"
)

type objectStorageReadRange struct {
	key string
	min *int64
	max *int64
}

type testObjectCopyStorage struct {
	dummyObjectStorage
	src     ObjectStorage
	srcKey  string
	dstKey  string
	exists  bool
	err     error
	copyErr error
	copies  int
}

func (s *testObjectCopyStorage) Exists(context.Context, string) (bool, error) {
	return s.exists, s.err
}

func (s *testObjectCopyStorage) CopyObject(
	_ context.Context,
	src ObjectStorage,
	srcKey string,
	dstKey string,
) (bool, error) {
	s.copies++
	s.src = src
	s.srcKey = srcKey
	s.dstKey = dstKey
	return true, s.copyErr
}

func TestS3FSCopyObject(t *testing.T) {
	ctx := context.Background()
	srcStorage := &testObjectCopyStorage{}
	dstStorage := &testObjectCopyStorage{}
	src := &S3FS{name: "src", keyPrefix: "cluster", storage: srcStorage, rawStorage: srcStorage}
	dst := &S3FS{name: "dst", keyPrefix: "fixture", storage: dstStorage, rawStorage: dstStorage}

	copied, err := dst.CopyObject(ctx, src, "objects/a", "backup/objects/a")
	require.NoError(t, err)
	require.True(t, copied)
	require.Same(t, srcStorage, dstStorage.src)
	require.Equal(t, "cluster/objects/a", dstStorage.srcKey)
	require.Equal(t, "fixture/backup/objects/a", dstStorage.dstKey)

	sub := SubPath(dst, "table")
	copied, err = sub.(ObjectCopier).CopyObject(ctx, src, "objects/a", "objects/a")
	require.NoError(t, err)
	require.True(t, copied)
	require.Equal(t, "fixture/table/objects/a", dstStorage.dstKey)

	srcServices, err := NewFileServices("src", src)
	require.NoError(t, err)
	dstServices, err := NewFileServices("dst", dst)
	require.NoError(t, err)
	copied, err = dstServices.CopyObject(ctx, srcServices, "objects/a", "objects/b")
	require.NoError(t, err)
	require.True(t, copied)
	require.Equal(t, "cluster/objects/a", dstStorage.srcKey)
	require.Equal(t, "fixture/objects/b", dstStorage.dstKey)

	noCopier := &S3FS{name: "dst", storage: dummyObjectStorage{}}
	copied, err = noCopier.CopyObject(ctx, src, "objects/a", "objects/b")
	require.NoError(t, err)
	require.False(t, copied)

	dstStorage.exists = true
	_, err = dst.CopyObject(ctx, src, "objects/a", "objects/b")
	require.Error(t, err)
	dstStorage.exists = false
	dstStorage.err = errors.New("exists failed")
	_, err = dst.CopyObject(ctx, src, "objects/a", "objects/b")
	require.ErrorContains(t, err, "exists failed")
	dstStorage.err = nil
	dstStorage.copyErr = errors.New("copy failed")
	_, err = dst.CopyObject(ctx, src, "objects/a", "objects/b")
	require.ErrorContains(t, err, "copy failed")
	dstStorage.copyErr = context.DeadlineExceeded
	canceledCtx, cancel := context.WithCancel(ctx)
	cancel()
	copies := dstStorage.copies
	_, err = dst.CopyObject(canceledCtx, src, "objects/a", "objects/b")
	require.ErrorIs(t, err, context.Canceled)
	require.Equal(t, copies, dstStorage.copies)

	_, err = dst.CopyObject(ctx, src, "~~", "objects/b")
	require.Error(t, err)
	_, err = dst.CopyObject(ctx, src, "objects/a", "~~")
	require.Error(t, err)
}

func TestObjectCopyCapabilityFallbacks(t *testing.T) {
	ctx := context.Background()
	plain := dummyFileService{name: "plain"}
	services, err := NewFileServices("plain", plain)
	require.NoError(t, err)

	// A FileServices destination without the optional copy capability must let
	// callers fall back to streaming instead of treating it as an error.
	copied, err := services.CopyObject(ctx, plain, "source", "destination")
	require.NoError(t, err)
	require.False(t, copied)

	_, err = services.CopyObject(ctx, plain, "source", "~~")
	require.Error(t, err)
	_, err = services.CopyObject(ctx, plain, "source", "missing:destination")
	require.Error(t, err)

	sub := SubPath(plain, "prefix").(ObjectCopier)
	copied, err = sub.CopyObject(ctx, plain, "source", "destination")
	require.NoError(t, err)
	require.False(t, copied)
	_, err = sub.CopyObject(ctx, plain, "source", "~~")
	require.Error(t, err)

	source, _, err := resolveS3CopySource(plain, "source")
	require.NoError(t, err)
	require.Nil(t, source)
	_, _, err = resolveS3CopySource(SubPath(plain, "prefix"), "~~")
	require.Error(t, err)
	_, _, err = resolveS3CopySource(services, "missing:source")
	require.Error(t, err)
}

func TestObjectCopyRejectsIncompatibleEndpoints(t *testing.T) {
	copied, err := (&AwsSDKv2{endpoint: "https://s3-b.example.com"}).CopyObject(
		context.Background(), &AwsSDKv2{endpoint: "https://s3-a.example.com"}, "src", "dst",
	)
	require.NoError(t, err)
	require.False(t, copied)

	copied, err = (&MinioSDK{endpoint: "minio-b:9000"}).CopyObject(
		context.Background(), &MinioSDK{endpoint: "minio-a:9000"}, "src", "dst",
	)
	require.NoError(t, err)
	require.False(t, copied)

	copied, err = (&AliyunSDK{endpoint: "oss-b.example.com"}).CopyObject(
		context.Background(), &AliyunSDK{endpoint: "oss-a.example.com"}, "src", "dst",
	)
	require.NoError(t, err)
	require.False(t, copied)
}

func TestObjectCopyRejectsDifferentCredentialDomains(t *testing.T) {
	left := newObjectStorageCopyCredentialDomain("left-id", "left-secret")
	right := newObjectStorageCopyCredentialDomain("right-id", "right-secret")

	copied, err := (&AwsSDKv2{
		endpoint: "https://s3.example.com", copyCredentialDomain: left,
	}).CopyObject(context.Background(), &AwsSDKv2{
		endpoint: "https://s3.example.com", copyCredentialDomain: right,
	}, "src", "dst")
	require.NoError(t, err)
	require.False(t, copied)

	copied, err = (&MinioSDK{
		endpoint: "minio.example.com", copyCredentialDomain: left,
	}).CopyObject(context.Background(), &MinioSDK{
		endpoint: "minio.example.com", copyCredentialDomain: right,
	}, "src", "dst")
	require.NoError(t, err)
	require.False(t, copied)

	copied, err = (&AliyunSDK{
		endpoint: "oss.example.com", copyCredentialDomain: left,
	}).CopyObject(context.Background(), &AliyunSDK{
		endpoint: "oss.example.com", copyCredentialDomain: right,
	}, "src", "dst")
	require.NoError(t, err)
	require.False(t, copied)

	copied, err = (&QCloudSDK{copyCredentialDomain: left}).CopyObject(
		context.Background(), &QCloudSDK{copyCredentialDomain: right}, "src", "dst",
	)
	require.NoError(t, err)
	require.False(t, copied)
}

func TestObjectStorageCopyCredentialDomain(t *testing.T) {
	first := newObjectStorageCopyCredentialDomain("id", "secret", "token")
	require.True(t, first.matches(newObjectStorageCopyCredentialDomain("id", "secret", "token")))
	require.False(t, first.matches(newObjectStorageCopyCredentialDomain("id", "other", "token")))
	require.False(t, first.matches(newObjectStorageCopyCredentialDomain("", "", "")))
}

type readRangeRecordingObjectStorage struct {
	ObjectStorage
	mu    sync.Mutex
	reads []objectStorageReadRange
}

type blockingReadObjectStorage struct {
	ObjectStorage
	readStarted chan struct{}
	releaseRead chan struct{}
	readCount   atomic.Int64
}

type generatedRangeObjectStorage struct {
	dummyObjectStorage
	mu          sync.Mutex
	reads       []objectStorageReadRange
	readStarted chan struct{}
	readOnce    sync.Once
}

type notifyReadObjectStorage struct {
	ObjectStorage
	reads      atomic.Int64
	secondRead chan struct{}
}

type heldWriter struct {
	once    sync.Once
	started chan struct{}
	release chan struct{}
}

type blockingDataCache struct {
	fscache.DataCache
	updateStarted chan struct{}
	releaseUpdate chan struct{}
	updateCount   atomic.Int64
}

func (b *blockingReadObjectStorage) Read(ctx context.Context, key string, min *int64, max *int64) (io.ReadCloser, error) {
	if b.readCount.Add(1) == 1 {
		close(b.readStarted)
		select {
		case <-ctx.Done():
			return nil, ctx.Err()
		case <-b.releaseRead:
		}
	}
	return b.ObjectStorage.Read(ctx, key, min, max)
}

func (b *blockingDataCache) Set(ctx context.Context, key fscache.CacheKey, data fscache.Data) error {
	if b.updateCount.Add(1) == 1 {
		close(b.updateStarted)
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-b.releaseUpdate:
		}
	}
	return b.DataCache.Set(ctx, key, data)
}

func (g *generatedRangeObjectStorage) Read(
	_ context.Context,
	key string,
	min *int64,
	max *int64,
) (io.ReadCloser, error) {
	if g.readStarted != nil {
		g.readOnce.Do(func() { close(g.readStarted) })
	}
	g.mu.Lock()
	g.reads = append(g.reads, objectStorageReadRange{
		key: key,
		min: cloneInt64Pointer(min),
		max: cloneInt64Pointer(max),
	})
	g.mu.Unlock()
	if min == nil || max == nil || *max < *min {
		return nil, errors.New("expected a bounded range read")
	}
	return io.NopCloser(bytes.NewReader(make([]byte, *max-*min))), nil
}

func (s *notifyReadObjectStorage) Read(ctx context.Context, key string, min, max *int64) (io.ReadCloser, error) {
	if s.reads.Add(1) == 2 {
		close(s.secondRead)
	}
	return s.ObjectStorage.Read(ctx, key, min, max)
}

func (g *generatedRangeObjectStorage) readRanges() []objectStorageReadRange {
	g.mu.Lock()
	defer g.mu.Unlock()
	return append([]objectStorageReadRange(nil), g.reads...)
}

func (w *heldWriter) Write(p []byte) (int, error) {
	w.once.Do(func() { close(w.started) })
	<-w.release
	return len(p), nil
}

func (r *readRangeRecordingObjectStorage) Read(ctx context.Context, key string, min *int64, max *int64) (io.ReadCloser, error) {
	r.mu.Lock()
	r.reads = append(r.reads, objectStorageReadRange{
		key: key,
		min: cloneInt64Pointer(min),
		max: cloneInt64Pointer(max),
	})
	r.mu.Unlock()
	return r.ObjectStorage.Read(ctx, key, min, max)
}

func (r *readRangeRecordingObjectStorage) lastRead() (objectStorageReadRange, bool) {
	r.mu.Lock()
	defer r.mu.Unlock()
	if len(r.reads) == 0 {
		return objectStorageReadRange{}, false
	}
	return r.reads[len(r.reads)-1], true
}

func cloneInt64Pointer(v *int64) *int64 {
	if v == nil {
		return nil
	}
	ret := *v
	return &ret
}

func TestS3FS(
	t *testing.T,
) {
	t.Run("default policy", func(t *testing.T) {
		testS3FS(t, 0)
	})
	t.Run("skip full file preloads", func(t *testing.T) {
		testS3FS(t, SkipFullFilePreloads)
	})
}

func testS3FS(
	t *testing.T,
	policy Policy,
) {

	getArgs := func(t *testing.T, name string) ObjectStorageArguments {
		return ObjectStorageArguments{
			Name:      name,
			Endpoint:  "disk",
			Bucket:    t.TempDir(),
			KeyPrefix: time.Now().Format("2006-01-02.15:04:05.000000"),
		}
	}

	t.Run("file service", func(t *testing.T) {
		testFileService(t, policy, func(name string) FileService {
			ctx := context.Background()
			fs, err := NewS3FS(
				ctx,
				getArgs(t, name),
				DisabledCacheConfig,
				nil,
				true,
				false,
			)
			assert.Nil(t, err)

			// to test continuation
			switch storage := fs.storage.(type) {
			case *AwsSDKv2:
				storage.listMaxKeys = 5
			}

			return fs
		})
	})

	t.Run("list root", func(t *testing.T) {
		ctx := context.Background()
		fs, err := NewS3FS(
			ctx,
			getArgs(t, "test"),
			DisabledCacheConfig,
			nil,
			true,
			false,
		)
		assert.Nil(t, err)
		var counterSet, counterSet2 perfcounter.CounterSet
		ctx = perfcounter.WithCounterSet(ctx, &counterSet)
		ctx = perfcounter.WithCounterSet(ctx, &counterSet2)

		err = fs.Write(ctx, IOVector{
			FilePath: "foo",
			Entries: []IOEntry{
				{
					Data: []byte("foo"),
					Size: 3,
				},
			},
		})
		assert.Nil(t, err)
		// The disk-backed test object store is not a physical S3 boundary, so it
		// must not publish S3 accepted-byte facts.
		assert.Zero(t, counterSet.FileService.S3WriteSize.Load())
		assert.Zero(t, counterSet2.FileService.S3WriteSize.Load())

		entries, err := SortedList(fs.List(ctx, ""))
		assert.Nil(t, err)

		assert.True(t, len(entries) > 0)
		assert.True(t, counterSet.FileService.S3.List.Load() > 0)
		assert.True(t, counterSet2.FileService.S3.List.Load() > 0)
	})

	t.Run("mem caching file service", func(t *testing.T) {
		testCachingFileService(t, func() CachingFileService {
			ctx := context.Background()
			fs, err := NewS3FS(
				ctx,
				getArgs(t, "test"),
				CacheConfig{
					MemoryCapacity: ptrTo[toml.ByteSize](128 * 1024),
				},
				nil,
				false,
				false,
			)
			assert.Nil(t, err)
			return fs
		})
	})

	t.Run("disk caching file service", func(t *testing.T) {
		testCachingFileService(t, func() CachingFileService {
			ctx := context.Background()
			fs, err := NewS3FS(
				ctx,
				getArgs(t, "name"),
				CacheConfig{
					MemoryCapacity: ptrTo[toml.ByteSize](1),
					DiskCapacity:   ptrTo[toml.ByteSize](128 * 1024),
					DiskPath:       ptrTo(t.TempDir()),
				},
				nil,
				false,
				false,
			)
			assert.Nil(t, err)
			return fs
		})
	})

	t.Run("mem and disk caching file service", func(t *testing.T) {
		testCachingFileService(t, func() CachingFileService {
			ctx := context.Background()
			fs, err := NewS3FS(
				ctx,
				getArgs(t, "test"),
				CacheConfig{
					MemoryCapacity: ptrTo[toml.ByteSize](128 * 1024),
					DiskCapacity:   ptrTo[toml.ByteSize](128 * 1024),
					DiskPath:       ptrTo(t.TempDir()),
				},
				nil,
				false,
				false,
			)
			assert.Nil(t, err)
			return fs
		})
	})

}

func TestDynamicS3(t *testing.T) {
	ctx := WithSQLFileAccess(context.Background(), "sys", "moadmin")
	testFileService(t, 0, func(name string) FileService {
		buf := new(strings.Builder)
		w := csv.NewWriter(buf)
		err := w.Write([]string{
			"s3",
			"disk",
			"",
			t.TempDir(),
			"",
			"",
			time.Now().Format("2006-01-02.15:04:05.000000"),
			name,
		})
		assert.Nil(t, err)
		w.Flush()
		fs, path, err := GetForETL(ctx, nil, JoinPath(
			buf.String(),
			"foo/bar/baz",
		))
		assert.Nil(t, err)
		assert.Equal(t, path, "foo/bar/baz")
		return fs
	})
}

func TestDynamicS3Opts(t *testing.T) {
	ctx := WithSQLFileAccess(context.Background(), "sys", "moadmin")
	testFileService(t, 0, func(name string) FileService {
		buf := new(strings.Builder)
		w := csv.NewWriter(buf)
		err := w.Write([]string{
			"s3-opts",
			"endpoint=disk",
			"bucket=" + t.TempDir(),
			"prefix=" + time.Now().Format("2006-01-02.15:04:05.000000"),
			"name=" + name,
		})
		assert.Nil(t, err)
		w.Flush()
		fs, path, err := GetForETL(ctx, nil, JoinPath(
			buf.String(),
			"foo/bar/baz",
		))
		assert.Nil(t, err)
		assert.Equal(t, path, "foo/bar/baz")
		return fs
	})
}

func TestDynamicS3NoKey(t *testing.T) {
	ctx := WithParallelMode(WithSQLFileAccess(context.Background(), "sys", "moadmin"), ParallelForce)
	prefix := "etl-prefix"
	buf := new(strings.Builder)
	w := csv.NewWriter(buf)
	err := w.Write([]string{
		"s3-no-key",
		"disk",
		"",
		t.TempDir(),
		prefix,
		"s3-nokey",
	})
	assert.Nil(t, err)
	w.Flush()

	fs, path, err := GetForETL(ctx, nil, JoinPath(buf.String(), "foo.txt"))
	assert.Nil(t, err)
	assert.Equal(t, "foo.txt", path)
	s3fs, ok := fs.(*S3FS)
	assert.True(t, ok)
	assert.Equal(t, ParallelForce, s3fs.parallelMode)
	assert.Equal(t, prefix, s3fs.keyPrefix)
	assert.IsType(t, &diskObjectStorage{}, baseStorage(t, s3fs))
}

func TestDynamicMinio(t *testing.T) {
	ctx := WithParallelMode(WithSQLFileAccess(context.Background(), "sys", "moadmin"), ParallelForce)
	buf := new(strings.Builder)
	w := csv.NewWriter(buf)
	err := w.Write([]string{
		"minio",
		"http://127.0.0.1:9000",
		"us-east-1",
		"bucket",
		"ak",
		"sk",
		"pref",
		"minio-etl",
	})
	assert.Nil(t, err)
	w.Flush()

	fs, path, err := GetForETL(ctx, nil, JoinPath(buf.String(), "bar.txt"))
	assert.Nil(t, err)
	assert.Equal(t, "bar.txt", path)
	s3fs, ok := fs.(*S3FS)
	assert.True(t, ok)
	assert.Equal(t, ParallelForce, s3fs.parallelMode)
	assert.Equal(t, "pref", s3fs.keyPrefix)
	assert.IsType(t, &MinioSDK{}, baseStorage(t, s3fs))
}

func baseStorage(t *testing.T, fs *S3FS) ObjectStorage {
	trace, ok := fs.storage.(*objectStorageHTTPTrace)
	assert.True(t, ok)
	metrics, ok := trace.upstream.(*objectStorageMetrics)
	assert.True(t, ok)
	sem, ok := metrics.upstream.(*objectStorageSemaphore)
	assert.True(t, ok)
	return sem.upstream
}

func BenchmarkS3FS(b *testing.B) {
	cacheDir := b.TempDir()
	b.ResetTimer()

	ctx := context.Background()
	benchmarkFileService(ctx, b, func() FileService {
		fs, err := NewS3FS(
			ctx,
			ObjectStorageArguments{
				Name:      "bench",
				Endpoint:  "disk",
				IsMinio:   false,
				Region:    "",
				Bucket:    b.TempDir(),
				KeyPrefix: time.Now().Format("2006-01-02.15:04:05.000000"),
				KeyID:     "",
				KeySecret: "",
			},
			CacheConfig{
				DiskPath: ptrTo(cacheDir),
			},
			nil,
			true,
			false,
		)
		assert.Nil(b, err)
		return fs
	})
}

func TestS3FSWithSubPath(t *testing.T) {
	testFileService(t, 0, func(name string) FileService {
		ctx := context.Background()
		fs, err := NewS3FS(
			ctx,
			ObjectStorageArguments{
				Name:      name,
				Endpoint:  "disk",
				Bucket:    t.TempDir(),
				KeyPrefix: time.Now().Format("2006-01-02.15:04:05.000000"),
			},
			DisabledCacheConfig,
			nil,
			true,
			false,
		)
		assert.Nil(t, err)
		return SubPath(fs, "foo/")
	})
}

func BenchmarkS3ConcurrentRead(b *testing.B) {
	ctx := context.Background()

	fs, err := NewS3FS(
		ctx,
		ObjectStorageArguments{
			Name:      "bench",
			Endpoint:  "disk",
			Bucket:    b.TempDir(),
			KeyPrefix: time.Now().Format("2006-01-02.15:04:05.000000"),
		},
		DisabledCacheConfig,
		nil,
		true,
		false,
	)
	if err != nil {
		b.Fatal(err)
	}
	if fs == nil {
		b.Fatal(err)
	}

	vector := IOVector{
		FilePath: "foo",
		Entries: []IOEntry{
			{
				Size: 3,
				Data: []byte("foo"),
			},
		},
	}
	err = fs.Write(ctx, vector)
	if err != nil {
		b.Fatal(err)
	}

	b.ResetTimer()

	b.RunParallel(func(pb *testing.PB) {
		sem := make(chan struct{}, 128)
		for pb.Next() {
			sem <- struct{}{}
			go func() {
				defer func() {
					<-sem
				}()
				err := fs.Read(ctx, &IOVector{
					FilePath: "foo",
					Entries: []IOEntry{
						{
							Size: 3,
						},
					},
				})
				if err != nil {
					panic(err)
				}
			}()
		}
		for i := 0; i < cap(sem); i++ {
			sem <- struct{}{}
		}
	})

}

func TestSequentialS3Read(t *testing.T) {
	ctx := context.Background()

	fs, err := NewS3FS(
		ctx,
		ObjectStorageArguments{
			Name:      "bench",
			Endpoint:  "disk",
			Bucket:    t.TempDir(),
			KeyPrefix: time.Now().Format("2006-01-02.15:04:05.000000"),
		},
		DisabledCacheConfig,
		nil,
		true,
		false,
	)
	if err != nil {
		t.Fatal(err)
	}
	if fs == nil {
		t.Fatal(err)
	}

	vector := IOVector{
		FilePath: "foo",
		Entries: []IOEntry{
			{
				Size: 3,
				Data: []byte("foo"),
			},
		},
	}
	err = fs.Write(ctx, vector)
	if err != nil {
		t.Fatal(err)
	}

	for i := 0; i < 128; i++ {
		err := fs.Read(ctx, &IOVector{
			FilePath: "foo",
			Entries: []IOEntry{
				{
					Size: 3,
				},
			},
		})
		if err != nil {
			t.Fatal(err)
		}
	}

}

func TestS3RestoreFromCache(t *testing.T) {
	t.Skip("no longer valid since we delete cache files when calling Delete")
	ctx := context.Background()

	cacheDir := t.TempDir()
	fs, err := NewS3FS(
		ctx,
		ObjectStorageArguments{
			Name:      "s3",
			Endpoint:  "disk",
			Bucket:    t.TempDir(),
			KeyPrefix: time.Now().Format("2006-01-02.15:04:05.000000"),
		},
		CacheConfig{
			DiskPath: ptrTo(cacheDir),
		},
		nil,
		false,
		false,
	)
	assert.Nil(t, err)

	// write file
	err = fs.Write(ctx, IOVector{
		FilePath: "foo/bar",
		Entries: []IOEntry{
			{
				Size: 3,
				Data: []byte("foo"),
			},
		},
	})
	assert.Nil(t, err)

	// write file without full file cache
	err = fs.Write(ctx, IOVector{
		FilePath: "quux",
		Entries: []IOEntry{
			{
				Size: 3,
				Data: []byte("foo"),
			},
		},
		Policy: SkipFullFilePreloads,
	})
	assert.Nil(t, err)
	err = fs.Read(ctx, &IOVector{
		FilePath: "quux",
		Entries: []IOEntry{
			{
				Size: 3,
			},
		},
	})
	assert.Nil(t, err)

	err = fs.Delete(ctx, "foo/bar")
	assert.Nil(t, err)

	logutil.Info("cache dir", zap.Any("dir", cacheDir))

	counterSet := new(perfcounter.CounterSet)
	ctx = perfcounter.WithCounterSet(ctx, counterSet)
	fs.restoreFromDiskCache(ctx)

	if n := counterSet.FileService.S3.Put.Load(); n != 1 {
		t.Fatalf("got %v", n)
	}

	vec := &IOVector{
		FilePath: "foo/bar",
		Entries: []IOEntry{
			{
				Size: -1,
			},
		},
	}
	err = fs.Read(ctx, vec)
	assert.Nil(t, err)
	assert.Equal(t, []byte("foo"), vec.Entries[0].Data)

}

func TestS3PrefetchFile(t *testing.T) {
	ctx := context.Background()
	var pcSet perfcounter.CounterSet
	ctx = perfcounter.WithCounterSet(ctx, &pcSet)

	cacheDir := t.TempDir()
	fs, err := NewS3FS(
		ctx,
		ObjectStorageArguments{
			Name:      "s3",
			Endpoint:  "disk",
			Bucket:    t.TempDir(),
			KeyPrefix: time.Now().Format("2006-01-02.15:04:05.000000"),
		},
		CacheConfig{
			DiskPath:     ptrTo(cacheDir),
			DiskCapacity: ptrTo[toml.ByteSize](1 << 30),
		},
		nil,
		false,
		false,
	)
	assert.Nil(t, err)
	defer fs.Close(ctx)

	data := bytes.Repeat([]byte("abcd"), 2<<20)

	// write file
	err = fs.Write(ctx, IOVector{
		FilePath: "foo/bar",
		Entries: []IOEntry{
			{
				Size: int64(len(data)),
				Data: data,
			},
		},
		Policy: SkipDiskCache | SkipMemoryCache,
	})
	assert.Nil(t, err)

	// preload
	err = fs.PrefetchFile(ctx, "foo/bar")
	assert.Nil(t, err)
	err = fs.PrefetchFile(ctx, "foo/bar")
	assert.Nil(t, err)
	fs.FlushCache(ctx)

	// read
	lastHit := int64(0)
	for i := 1; i < len(data); i += len(data) / 1000 {
		vec := &IOVector{
			FilePath: "foo/bar",
			Entries: []IOEntry{
				{
					Size: int64(i),
				},
			},
		}
		err = fs.Read(ctx, vec)
		assert.Nil(t, err)
		assert.Equal(t, data[:i], vec.Entries[0].Data)
		assert.Equal(t, lastHit+1, pcSet.FileService.Cache.Disk.Hit.Load())
		vec.Release()
		lastHit++
	}

}

func TestS3PrefetchFileDoesNotWaitForAsyncDiskFinalize(t *testing.T) {
	ctx := context.Background()
	fs, err := NewS3FS(
		ctx,
		ObjectStorageArguments{
			Name:      "s3",
			Endpoint:  "disk",
			Bucket:    t.TempDir(),
			KeyPrefix: time.Now().Format("2006-01-02.15:04:05.000000"),
		},
		CacheConfig{
			DiskPath:     ptrTo(t.TempDir()),
			DiskCapacity: ptrTo[toml.ByteSize](1 << 20),
		},
		nil,
		false,
		false,
	)
	require.NoError(t, err)
	require.NoError(t, fs.Write(ctx, IOVector{
		FilePath: "foo/bar",
		Entries: []IOEntry{{
			Size: 11,
			Data: []byte("hello world"),
		}},
		Policy: SkipDiskCache | SkipMemoryCache,
	}))

	syncStarted, unblock := installBlockedDiskCacheFileSync(fs.diskCache)
	t.Cleanup(func() {
		unblock()
		fs.Close(ctx)
	})

	prefetchDone := make(chan error, 1)
	go func() {
		prefetchDone <- fs.PrefetchFile(ctx, "foo/bar")
	}()
	select {
	case <-syncStarted:
	case <-time.After(diskCacheLifecycleTestTimeout):
		t.Fatal("async prefetch finalizer did not start")
	}
	select {
	case err := <-prefetchDone:
		require.NoError(t, err)
	case <-time.After(diskCacheLifecycleTestTimeout):
		unblock()
		t.Fatalf("prefetch waited for disk finalization: %v", <-prefetchDone)
	}

	diskPath := fs.diskCache.pathForFile("foo/bar")
	_, err = os.Stat(diskPath)
	require.ErrorIs(t, err, os.ErrNotExist)
	require.True(t, fs.diskCache.isUpdating(diskPath))

	unblock()
	flushCtx, cancel := context.WithTimeout(ctx, diskCacheLifecycleTestTimeout)
	defer cancel()
	fs.FlushCache(flushCtx)
	require.NoError(t, flushCtx.Err())
	data, err := os.ReadFile(diskPath)
	require.NoError(t, err)
	require.Equal(t, []byte("hello world"), data)
}

func TestS3FSWriteDiskCacheFinalizeMode(t *testing.T) {
	for _, test := range []struct {
		name  string
		async bool
	}{
		{name: "async", async: true},
		{name: "sync", async: false},
	} {
		t.Run(test.name, func(t *testing.T) {
			ctx := context.Background()
			fs, err := NewS3FS(
				ctx,
				ObjectStorageArguments{
					Name:      "s3",
					Endpoint:  "disk",
					Bucket:    t.TempDir(),
					KeyPrefix: time.Now().Format("2006-01-02.15:04:05.000000"),
				},
				CacheConfig{
					DiskPath:     ptrTo(t.TempDir()),
					DiskCapacity: ptrTo[toml.ByteSize](1 << 20),
				},
				nil,
				false,
				false,
			)
			require.NoError(t, err)
			fs.SetAsyncUpdate(test.async)

			syncStarted, unblock := installBlockedDiskCacheFileSync(fs.diskCache)
			t.Cleanup(func() {
				unblock()
				fs.Close(ctx)
			})

			writeDone := make(chan error, 1)
			go func() {
				writeDone <- fs.Write(ctx, IOVector{
					FilePath: "foo/bar",
					Entries: []IOEntry{{
						Size: 11,
						Data: []byte("hello world"),
					}},
				})
			}()
			select {
			case <-syncStarted:
			case <-time.After(diskCacheLifecycleTestTimeout):
				t.Fatal("disk-cache finalization did not start")
			}

			diskPath := fs.diskCache.pathForFile("foo/bar")
			require.True(t, fs.diskCache.isUpdating(diskPath))
			if test.async {
				select {
				case err := <-writeDone:
					require.NoError(t, err)
				case <-time.After(diskCacheLifecycleTestTimeout):
					unblock()
					t.Fatalf("async write waited for disk finalization: %v", <-writeDone)
				}
				_, err = os.Stat(diskPath)
				require.ErrorIs(t, err, os.ErrNotExist)
			} else {
				select {
				case err := <-writeDone:
					t.Fatalf("synchronous write returned before disk finalization: %v", err)
				case <-time.After(50 * time.Millisecond):
				}
			}

			unblock()
			if !test.async {
				require.NoError(t, <-writeDone)
			}
			flushCtx, cancel := context.WithTimeout(ctx, diskCacheLifecycleTestTimeout)
			defer cancel()
			fs.FlushCache(flushCtx)
			require.NoError(t, flushCtx.Err())
			data, err := os.ReadFile(diskPath)
			require.NoError(t, err)
			require.Equal(t, []byte("hello world"), data)
		})
	}
}

func TestS3FSFullObjectDiskCacheFillDoesNotRetainWholeObjectBuffer(t *testing.T) {
	ctx := context.Background()
	var pcSet perfcounter.CounterSet
	ctx = perfcounter.WithCounterSet(ctx, &pcSet)

	fs, err := NewS3FS(
		ctx,
		ObjectStorageArguments{
			Name:      "s3",
			Endpoint:  "disk",
			Bucket:    t.TempDir(),
			KeyPrefix: time.Now().Format("2006-01-02.15:04:05.000000"),
		},
		CacheConfig{
			DiskPath:     ptrTo(t.TempDir()),
			DiskCapacity: ptrTo[toml.ByteSize](1 << 30),
		},
		nil,
		false,
		false,
	)
	assert.Nil(t, err)
	defer fs.Close(ctx)

	data := bytes.Repeat([]byte("abcd"), 1<<18)
	err = fs.Write(ctx, IOVector{
		FilePath: "foo/bar",
		Entries: []IOEntry{
			{
				Size: int64(len(data)),
				Data: data,
			},
		},
		Policy: SkipDiskCache | SkipMemoryCache,
	})
	assert.Nil(t, err)

	vec := &IOVector{
		FilePath: "foo/bar",
		Entries: []IOEntry{
			{
				Offset: 12345,
				Size:   7,
			},
		},
	}
	err = fs.Read(ctx, vec)
	assert.Nil(t, err)
	assert.Equal(t, data[12345:12352], vec.Entries[0].Data)
	assert.Less(t, cap(vec.Entries[0].Data), len(data)/16)
	assert.Equal(t, int64(1), pcSet.FileService.S3.Get.Load())
	vec.Release()

	// A read in async-update mode guarantees cache admission, not publication.
	// Wait on the explicit barrier before asserting that a later read hits disk.
	flushCtx, cancel := context.WithTimeout(ctx, diskCacheLifecycleTestTimeout)
	defer cancel()
	fs.FlushCache(flushCtx)
	require.NoError(t, flushCtx.Err())

	hitVec := &IOVector{
		FilePath: "foo/bar",
		Entries: []IOEntry{
			{
				Offset: 54321,
				Size:   11,
			},
		},
	}
	err = fs.Read(ctx, hitVec)
	assert.Nil(t, err)
	assert.Equal(t, data[54321:54332], hitVec.Entries[0].Data)
	assert.Equal(t, int64(1), pcSet.FileService.S3.Get.Load())
	assert.Equal(t, int64(1), pcSet.FileService.Cache.Disk.Hit.Load())
	hitVec.Release()
}

func TestS3FSFullObjectDiskCacheFillDoesNotReturnWholeObjectDataWithCachedData(t *testing.T) {
	ctx := context.Background()
	var pcSet perfcounter.CounterSet
	ctx = perfcounter.WithCounterSet(ctx, &pcSet)

	fs, err := NewS3FS(
		ctx,
		ObjectStorageArguments{
			Name:      "s3",
			Endpoint:  "disk",
			Bucket:    t.TempDir(),
			KeyPrefix: time.Now().Format("2006-01-02.15:04:05.000000"),
		},
		CacheConfig{
			DiskPath:     ptrTo(t.TempDir()),
			DiskCapacity: ptrTo[toml.ByteSize](1 << 30),
		},
		nil,
		false,
		false,
	)
	assert.Nil(t, err)
	defer fs.Close(ctx)

	data := bytes.Repeat([]byte("abcd"), 1<<18)
	err = fs.Write(ctx, IOVector{
		FilePath: "foo/bar",
		Entries: []IOEntry{
			{
				Size: int64(len(data)),
				Data: data,
			},
		},
		Policy: SkipDiskCache | SkipMemoryCache,
	})
	assert.Nil(t, err)

	vec := &IOVector{
		FilePath: "foo/bar",
		Entries: []IOEntry{
			{
				Size:        int64(len(data)),
				ToCacheData: CacheOriginalData,
			},
		},
	}
	err = fs.Read(ctx, vec)
	assert.Nil(t, err)
	assert.NotNil(t, vec.Entries[0].CachedData)
	if vec.Entries[0].CachedData != nil {
		assert.Equal(t, data, vec.Entries[0].CachedData.Bytes())
	}
	assert.Nil(t, vec.Entries[0].Data)
	assert.Equal(t, int64(1), pcSet.FileService.S3.Get.Load())
	vec.Release()
}

type diskCacheWaitLocker struct {
	sync.Mutex
	unlocked chan struct{}
}

func (l *diskCacheWaitLocker) Unlock() {
	l.Mutex.Unlock()
	select {
	case l.unlocked <- struct{}{}:
	default:
	}
}

func TestS3FSRangeCacheWaitStopsOnCancel(t *testing.T) {
	for _, tc := range []struct {
		name   string
		policy Policy
	}{
		{name: "reading the cache entry"},
		{name: "filling the cache entry", policy: SkipDiskCacheReads},
	} {
		t.Run(tc.name, func(t *testing.T) {
			ctx := t.Context()
			store, err := NewS3FS(
				ctx,
				ObjectStorageArguments{Name: "s3", Endpoint: "disk", Bucket: t.TempDir()},
				CacheConfig{DiskPath: ptrTo(t.TempDir()), DiskCapacity: ptrTo[toml.ByteSize](1 << 20)},
				nil, false, false,
			)
			require.NoError(t, err)
			defer store.Close(ctx)
			store.SetAsyncUpdate(false) // exercise caller cancellation while a synchronous fill waits

			const path = "history/object"
			require.NoError(t, store.Write(ctx, IOVector{
				FilePath: path,
				Entries:  []IOEntry{{Size: 5, Data: []byte("abcde")}},
				Policy:   SkipAllCache,
			}))

			cache := store.diskCache
			locker := &diskCacheWaitLocker{unlocked: make(chan struct{}, 1)}
			cache.updatingPaths.Cond = sync.NewCond(locker)
			vector := &IOVector{
				FilePath: path,
				Entries:  []IOEntry{{Offset: 1, Size: 3}},
				Policy:   tc.policy | SkipFullFilePreloads,
			}
			diskPath := cache.pathForIOEntry(path, vector.Entries[0])
			release := holdDiskCacheUpdate(t, cache, diskPath)
			<-locker.unlocked
			readCtx, cancel := context.WithCancel(ctx)
			done := make(chan struct{})
			var readErr error
			go func() {
				defer close(done)
				readErr = store.Read(readCtx, vector)
			}()
			defer func() {
				cancel()
				release()
				<-done
				vector.Release()
			}()

			select {
			case <-locker.unlocked:
			case <-done:
				t.Fatalf("read returned before encountering the held range: %v", readErr)
			case <-time.After(2 * time.Second):
				t.Fatal("read did not reach the held disk-cache range")
			}
			cancel()
			select {
			case <-done:
			case <-time.After(2 * time.Second):
				t.Fatal("cancellation left the read waiting for the cache writer")
			}
			if tc.policy.Any(SkipDiskCacheReads) {
				require.NoError(t, readErr)
				require.Equal(t, []byte("bcd"), vector.Entries[0].Data)
			} else {
				require.ErrorIs(t, readErr, context.Canceled)
			}
			require.True(t, cache.isUpdating(diskPath), "cancelling a waiter must not release the writer")
		})
	}
}

func TestS3FSRangeReadSkipsFullObjectDiskCacheUpdate(t *testing.T) {
	ctx := context.Background()
	fs, err := NewS3FS(
		ctx,
		ObjectStorageArguments{
			Name:      "s3",
			Endpoint:  "disk",
			Bucket:    t.TempDir(),
			KeyPrefix: time.Now().Format("2006-01-02.15:04:05.000000"),
		},
		CacheConfig{
			DiskPath:     ptrTo(t.TempDir()),
			DiskCapacity: ptrTo[toml.ByteSize](1 << 30),
		},
		nil,
		false,
		false,
	)
	assert.Nil(t, err)
	defer fs.Close(ctx)

	data := bytes.Repeat([]byte("abcd"), 1<<10)
	err = fs.Write(ctx, IOVector{
		FilePath: "foo/bar",
		Entries: []IOEntry{
			{
				Size: int64(len(data)),
				Data: data,
			},
		},
		Policy: SkipDiskCache | SkipMemoryCache,
	})
	assert.Nil(t, err)

	updatingPath := fs.diskCache.pathForFile("foo/bar")
	doneUpdate := holdDiskCacheUpdate(t, fs.diskCache, updatingPath)
	released := false
	release := func() {
		if !released {
			doneUpdate()
			released = true
		}
	}
	defer release()

	fullObjectVec := &IOVector{
		FilePath: "foo/bar",
		Entries: []IOEntry{
			{
				Size: int64(len(data)),
			},
		},
	}
	doneMerge, waitMerge := fs.ioMerger.Merge(fs.readMergeKey(fullObjectVec), maxIOWaitDuration)
	assert.NotNil(t, doneMerge)
	assert.Nil(t, waitMerge)
	releasedMerge := false
	releaseMerge := func() {
		if !releasedMerge {
			doneMerge()
			releasedMerge = true
		}
	}
	defer releaseMerge()

	type readResult struct {
		data []byte
		err  error
	}
	readDone := make(chan readResult, 1)
	go func() {
		vec := &IOVector{
			FilePath: "foo/bar",
			Entries: []IOEntry{
				{
					Offset: 123,
					Size:   7,
				},
			},
		}
		err := fs.Read(ctx, vec)
		defer vec.Release()
		if err != nil {
			readDone <- readResult{err: err}
			return
		}
		readDone <- readResult{data: append([]byte(nil), vec.Entries[0].Data...)}
	}()

	select {
	case result := <-readDone:
		assert.Nil(t, result.err)
		assert.Equal(t, data[123:130], result.data)
		assert.True(t, fs.diskCache.isUpdating(updatingPath))
	case <-time.After(2 * time.Second):
		releaseMerge()
		release()
		result := <-readDone
		t.Fatalf("range read waited for full-object disk cache update or io merge: %v", result.err)
	}
}

func TestS3FSRangeReadSkipsFullObjectIOMergeBeforeDiskCacheUpdate(t *testing.T) {
	ctx := context.Background()
	fs, err := NewS3FS(
		ctx,
		ObjectStorageArguments{
			Name:      "s3",
			Endpoint:  "disk",
			Bucket:    t.TempDir(),
			KeyPrefix: time.Now().Format("2006-01-02.15:04:05.000000"),
		},
		CacheConfig{
			DiskPath:     ptrTo(t.TempDir()),
			DiskCapacity: ptrTo[toml.ByteSize](1 << 30),
		},
		nil,
		false,
		false,
	)
	assert.Nil(t, err)
	defer fs.Close(ctx)

	data := bytes.Repeat([]byte("abcd"), 1<<10)
	err = fs.Write(ctx, IOVector{
		FilePath: "foo/bar",
		Entries: []IOEntry{
			{
				Size: int64(len(data)),
				Data: data,
			},
		},
		Policy: SkipDiskCache | SkipMemoryCache,
	})
	assert.Nil(t, err)

	recorder := &readRangeRecordingObjectStorage{
		ObjectStorage: fs.storage,
	}
	fs.storage = recorder

	fullObjectVec := &IOVector{
		FilePath: "foo/bar",
		Entries: []IOEntry{
			{
				Size: int64(len(data)),
			},
		},
	}
	doneMerge, waitMerge := fs.ioMerger.Merge(fs.readMergeKey(fullObjectVec), maxIOWaitDuration)
	assert.NotNil(t, doneMerge)
	assert.Nil(t, waitMerge)
	releasedMerge := false
	releaseMerge := func() {
		if !releasedMerge {
			doneMerge()
			releasedMerge = true
		}
	}
	defer releaseMerge()

	type readResult struct {
		data []byte
		err  error
	}
	readDone := make(chan readResult, 1)
	go func() {
		vec := &IOVector{
			FilePath: "foo/bar",
			Entries: []IOEntry{
				{
					Offset: 123,
					Size:   7,
				},
			},
		}
		err := fs.Read(ctx, vec)
		defer vec.Release()
		if err != nil {
			readDone <- readResult{err: err}
			return
		}
		readDone <- readResult{data: append([]byte(nil), vec.Entries[0].Data...)}
	}()

	select {
	case result := <-readDone:
		assert.Nil(t, result.err)
		assert.Equal(t, data[123:130], result.data)
		assert.True(t, fs.ioMerger.IsMerging(fs.readMergeKey(fullObjectVec)))
		assert.False(t, fs.diskCache.isUpdating(fs.diskCache.pathForFile("foo/bar")))
		read, ok := recorder.lastRead()
		assert.True(t, ok)
		if assert.NotNil(t, read.min) {
			assert.Equal(t, int64(123), *read.min)
		}
		if assert.NotNil(t, read.max) {
			assert.Equal(t, int64(130), *read.max)
		}
	case <-time.After(2 * time.Second):
		releaseMerge()
		result := <-readDone
		t.Fatalf("range read waited for full-object io merge before disk cache update: %v", result.err)
	}
}

func TestS3FSExpensiveRangeWaitsForFullObjectMergeUntilContextDeadline(t *testing.T) {
	ctx := context.Background()
	originalShortWait := shortIOWaitDuration
	shortIOWaitDuration = time.Millisecond
	t.Cleanup(func() {
		shortIOWaitDuration = originalShortWait
	})

	fs, err := NewS3FS(
		ctx,
		ObjectStorageArguments{
			Name:      "s3",
			Endpoint:  "disk",
			Bucket:    t.TempDir(),
			KeyPrefix: time.Now().Format("2006-01-02.15:04:05.000000"),
		},
		CacheConfig{
			DiskPath:     ptrTo(t.TempDir()),
			DiskCapacity: ptrTo[toml.ByteSize](1 << 30),
		},
		nil,
		false,
		false,
	)
	require.NoError(t, err)

	storage := &blockingReadObjectStorage{
		ObjectStorage: fs.storage,
		readStarted:   make(chan struct{}),
		releaseRead:   make(chan struct{}),
	}
	fs.storage = storage
	vector := &IOVector{
		FilePath: "foo/bar",
		Entries: []IOEntry{
			{Offset: 0, Size: 1 << 20},
			{Offset: 32 << 20, Size: 1 << 20},
		},
	}
	doneMerge, waitMerge := fs.ioMerger.Merge(fs.readMergeKey(vector), maxIOWaitDuration)
	require.NotNil(t, doneMerge)
	require.Nil(t, waitMerge)
	releaseMerge := sync.OnceFunc(doneMerge)
	releaseRead := sync.OnceFunc(func() { close(storage.releaseRead) })
	readCtx, cancel := context.WithTimeout(context.Background(), 100*time.Millisecond)
	t.Cleanup(func() {
		cancel()
		releaseRead()
		releaseMerge()
		vector.Release()
		fs.Close(ctx)
	})

	err = fs.Read(readCtx, vector)
	require.ErrorIs(t, err, context.DeadlineExceeded)
	require.Equal(t, int64(0), storage.readCount.Load())
}

func TestS3FSExpensiveRangeHasBoundedPerEntryFallback(t *testing.T) {
	ctx := context.Background()
	originalShortWait := shortIOWaitDuration
	originalMaxWait := maxIOWaitDuration
	shortIOWaitDuration = time.Millisecond
	maxIOWaitDuration = 5 * time.Millisecond
	t.Cleanup(func() {
		shortIOWaitDuration = originalShortWait
		maxIOWaitDuration = originalMaxWait
	})

	fs, err := NewS3FS(
		ctx,
		ObjectStorageArguments{
			Name:      "s3",
			Endpoint:  "disk",
			Bucket:    t.TempDir(),
			KeyPrefix: time.Now().Format("2006-01-02.15:04:05.000000"),
		},
		CacheConfig{
			DiskPath:     ptrTo(t.TempDir()),
			DiskCapacity: ptrTo[toml.ByteSize](1 << 30),
		},
		nil,
		false,
		false,
	)
	require.NoError(t, err)

	storage := &generatedRangeObjectStorage{}
	fs.storage = storage
	vector := &IOVector{
		FilePath: "foo/bar",
		Entries: []IOEntry{
			{Offset: 0, Size: 1},
			{Offset: 9 << 20, Size: 1},
		},
	}
	doneMerge, waitMerge := fs.ioMerger.Merge(fs.readMergeKey(vector), maxIOWaitDuration)
	require.NotNil(t, doneMerge)
	require.Nil(t, waitMerge)
	releaseMerge := sync.OnceFunc(doneMerge)
	t.Cleanup(func() {
		releaseMerge()
		vector.Release()
		fs.Close(ctx)
	})

	readDone := make(chan error, 1)
	go func() {
		readDone <- fs.Read(context.Background(), vector)
	}()
	select {
	case err := <-readDone:
		require.NoError(t, err)
	case <-time.After(time.Second):
		releaseMerge()
		select {
		case <-readDone:
		case <-time.After(time.Second):
			t.Fatal("follower did not terminate after releasing the merge leader")
		}
		t.Fatal("follower with no caller deadline did not reach a bounded fallback")
	}

	reads := storage.readRanges()
	require.Len(t, reads, 2)
	require.Equal(t, int64(0), *reads[0].min)
	require.Equal(t, int64(1), *reads[0].max)
	require.Equal(t, int64(9<<20), *reads[1].min)
	require.Equal(t, int64(9<<20)+1, *reads[1].max)
}

func TestS3FSCacheFillDoesNotWaitForNonProducingLeader(t *testing.T) {
	ctx := context.Background()
	fs, err := NewS3FS(
		ctx,
		ObjectStorageArguments{
			Name:      "s3",
			Endpoint:  "disk",
			Bucket:    t.TempDir(),
			KeyPrefix: time.Now().Format("2006-01-02.15:04:05.000000"),
		},
		CacheConfig{
			DiskPath:     ptrTo(t.TempDir()),
			DiskCapacity: ptrTo[toml.ByteSize](1 << 30),
		},
		nil,
		false,
		false,
	)
	require.NoError(t, err)
	safeToRelease := true
	t.Cleanup(func() {
		if safeToRelease {
			fs.Close(ctx)
		}
	})

	data := []byte{1, 2}
	require.NoError(t, fs.Write(ctx, IOVector{
		FilePath: "foo/bar",
		Entries:  []IOEntry{{Size: int64(len(data)), Data: data}},
		Policy:   SkipAllCache,
	}))
	storage := &notifyReadObjectStorage{ObjectStorage: fs.storage, secondRead: make(chan struct{})}
	fs.storage = storage

	writer := &heldWriter{
		started: make(chan struct{}),
		release: make(chan struct{}),
	}
	releaseWriter := sync.OnceFunc(func() { close(writer.release) })
	leaderVector := &IOVector{
		FilePath: "foo/bar",
		Entries: []IOEntry{{
			Offset:        0,
			Size:          1,
			WriterForRead: writer,
		}},
	}
	followerVector := &IOVector{
		FilePath: "foo/bar",
		Entries: []IOEntry{
			{Offset: 0, Size: 1},
			{Offset: 1, Size: 1},
		},
	}
	t.Cleanup(func() {
		if safeToRelease {
			followerVector.Release()
		}
	})
	require.False(t, fs.readMergeKey(leaderVector).CacheFill)
	require.True(t, fs.readMergeKey(followerVector).CacheFill)
	require.True(t, fs.readMergeKey(leaderVector).FullObject)
	require.True(t, fs.readMergeKey(followerVector).FullObject)
	require.NotEqual(t, fs.readMergeKey(leaderVector), fs.readMergeKey(followerVector))

	leaderDone := make(chan error, 1)
	go func() { leaderDone <- fs.Read(ctx, leaderVector) }()
	t.Cleanup(func() {
		releaseWriter()
		select {
		case err := <-leaderDone:
			assert.NoError(t, err)
		case <-time.After(10 * time.Second):
			safeToRelease = false
			t.Error("non-cache-producing leader did not terminate")
		}
	})
	select {
	case <-writer.started:
	case <-time.After(5 * time.Second):
		t.Fatal("non-cache-producing leader did not reach its writer")
	}
	require.False(t, fs.ioMerger.IsMerging(fs.readMergeKey(leaderVector)))
	require.EqualValues(t, 1, storage.reads.Load())

	followerCtx, cancelFollower := context.WithCancel(ctx)
	followerResult := make(chan error, 1)
	followerDone := make(chan struct{})
	go func() {
		defer close(followerDone)
		followerResult <- fs.Read(followerCtx, followerVector)
	}()
	t.Cleanup(func() {
		cancelFollower()
		releaseWriter()
		select {
		case <-followerDone:
		case <-time.After(10 * time.Second):
			safeToRelease = false
			t.Error("cache-producing follower did not exit during cleanup")
		}
	})
	select {
	case <-storage.secondRead:
	case <-time.After(10 * time.Second):
		releaseWriter()
		cancelFollower()
		select {
		case <-followerDone:
		case <-time.After(10 * time.Second):
			t.Fatal("cache-producing follower did not terminate after leader release")
		}
		t.Fatal("cache-producing follower waited for a non-cache-producing leader")
	}
	select {
	case <-followerDone:
		require.NoError(t, <-followerResult)
	case <-time.After(10 * time.Second):
		t.Fatal("cache-producing follower did not finish its independent read")
	}
	require.Equal(t, []byte{1}, followerVector.Entries[0].Data)
	require.Equal(t, []byte{2}, followerVector.Entries[1].Data)
}

func TestS3FSRangeMergeWaitHasBoundedFallback(t *testing.T) {
	ctx := context.Background()
	originalMaxWait := maxIOWaitDuration
	maxIOWaitDuration = 5 * time.Millisecond
	t.Cleanup(func() { maxIOWaitDuration = originalMaxWait })

	fs, err := NewS3FS(
		ctx,
		ObjectStorageArguments{
			Name:      "s3",
			Endpoint:  "disk",
			Bucket:    t.TempDir(),
			KeyPrefix: time.Now().Format("2006-01-02.15:04:05.000000"),
		},
		CacheConfig{
			DiskPath:     ptrTo(t.TempDir()),
			DiskCapacity: ptrTo[toml.ByteSize](1 << 30),
		},
		nil,
		false,
		false,
	)
	require.NoError(t, err)
	// This virtual clock measures the merger wait. An async disk worker
	// created inside the bubble would wait on channels owned outside it.
	fs.SetAsyncUpdate(false)
	safeToRelease := true
	t.Cleanup(func() {
		if safeToRelease {
			fs.Close(ctx)
		}
	})
	synctest.Test(t, func(t *testing.T) {
		storage := &generatedRangeObjectStorage{readStarted: make(chan struct{})}
		fs.storage = storage
		vector := &IOVector{
			FilePath: "foo/bar",
			Policy:   SkipFullFilePreloads,
			Entries:  []IOEntry{{Offset: 0, Size: 1}},
		}
		defer func() {
			if safeToRelease {
				vector.Release()
			}
		}()
		finishMerge, waitMerge := fs.ioMerger.Merge(fs.readMergeKey(vector), maxIOWaitDuration)
		require.NotNil(t, finishMerge)
		require.Nil(t, waitMerge)
		releaseMerge := sync.OnceFunc(finishMerge)
		defer releaseMerge()

		readCtx, cancelRead := context.WithCancel(context.Background())
		readResult := make(chan error, 1)
		readDone := make(chan struct{})
		go func() {
			defer close(readDone)
			readResult <- fs.Read(readCtx, vector)
		}()
		defer func() {
			cancelRead()
			releaseMerge()
			select {
			case <-readDone:
			case <-time.After(10 * time.Second):
				safeToRelease = false
				t.Error("range follower did not exit during cleanup")
			}
		}()

		// The leader stays blocked throughout. Virtual time proves S3FS uses
		// the configured bound instead of merely completing within a loose
		// wall-clock watchdog under a favorable scheduler.
		synctest.Wait()
		time.Sleep(maxIOWaitDuration - time.Millisecond)
		synctest.Wait()
		select {
		case <-storage.readStarted:
			t.Fatal("range follower read before the merge wait expired")
		case <-readDone:
			t.Fatal("range follower finished before the merge wait expired")
		default:
		}
		time.Sleep(time.Millisecond)
		synctest.Wait()
		select {
		case <-storage.readStarted:
		default:
			t.Fatal("range follower did not start reading when the merge wait expired")
		}
		select {
		case <-readDone:
			require.NoError(t, <-readResult)
		case <-time.After(10 * time.Second):
			t.Fatal("range follower reached storage but did not finish")
		}
		reads := storage.readRanges()
		require.Len(t, reads, 1)
		require.Equal(t, int64(0), *reads[0].min)
		require.Equal(t, int64(1), *reads[0].max)
	})
}

func TestS3FSSingleEntryFallbackCoalescesExactRange(t *testing.T) {
	ctx := context.Background()
	originalShortWait := shortIOWaitDuration
	shortIOWaitDuration = time.Millisecond
	t.Cleanup(func() { shortIOWaitDuration = originalShortWait })

	fs, err := NewS3FS(
		ctx,
		ObjectStorageArguments{
			Name:      "s3",
			Endpoint:  "disk",
			Bucket:    t.TempDir(),
			KeyPrefix: time.Now().Format("2006-01-02.15:04:05.000000"),
		},
		CacheConfig{
			MemoryCapacity: ptrTo[toml.ByteSize](1 << 20),
			DiskPath:       ptrTo(t.TempDir()),
			DiskCapacity:   ptrTo[toml.ByteSize](1 << 30),
		},
		nil,
		false,
		false,
	)
	require.NoError(t, err)
	require.NotNil(t, fs.memCache)
	t.Cleanup(func() { fs.Close(ctx) })

	rangeStorage := &generatedRangeObjectStorage{}
	storage := &blockingReadObjectStorage{
		ObjectStorage: rangeStorage,
		readStarted:   make(chan struct{}),
		releaseRead:   make(chan struct{}),
	}
	fs.storage = storage

	newVector := func() *IOVector {
		return &IOVector{
			FilePath: "foo/bar",
			Entries: []IOEntry{{
				Offset:      123,
				Size:        7,
				ToCacheData: CacheOriginalData,
			}},
		}
	}
	fullObjectVector := newVector()
	fullObjectDone, fullObjectWait := fs.ioMerger.Merge(
		fs.readMergeKey(fullObjectVector), maxIOWaitDuration,
	)
	require.NotNil(t, fullObjectDone)
	require.Nil(t, fullObjectWait)
	releaseFullObject := sync.OnceFunc(fullObjectDone)
	releaseStorage := sync.OnceFunc(func() { close(storage.releaseRead) })
	t.Cleanup(func() {
		releaseStorage()
		releaseFullObject()
	})

	leaderVector := newVector()
	leaderDone := make(chan error, 1)
	go func() { leaderDone <- fs.Read(ctx, leaderVector) }()
	select {
	case <-storage.readStarted:
	case <-time.After(5 * time.Second):
		t.Fatal("single-range leader did not reach object storage")
	}

	const followers = 16
	followerCtx, cancelFollowers := context.WithTimeout(ctx, 100*time.Millisecond)
	defer cancelFollowers()
	followerDone := make(chan error, followers)
	for range followers {
		go func() {
			vector := newVector()
			defer vector.Release()
			followerDone <- fs.Read(followerCtx, vector)
		}()
	}
	for range followers {
		select {
		case err := <-followerDone:
			require.ErrorIs(t, err, context.DeadlineExceeded)
		case <-time.After(5 * time.Second):
			t.Fatal("single-range follower did not honor caller cancellation")
		}
	}
	require.Equal(t, int64(1), storage.readCount.Load())

	releaseStorage()
	select {
	case err := <-leaderDone:
		require.NoError(t, err)
	case <-time.After(5 * time.Second):
		t.Fatal("single-range leader did not terminate")
	}
	require.Equal(t, []byte(make([]byte, 7)), leaderVector.Entries[0].Data)
	leaderVector.Release()

	cachedVector := newVector()
	require.NoError(t, fs.Read(ctx, cachedVector))
	require.NotNil(t, cachedVector.Entries[0].CachedData)
	require.Equal(t, []byte(make([]byte, 7)), cachedVector.Entries[0].CachedData.Bytes())
	cachedVector.Release()
	require.Equal(t, int64(1), storage.readCount.Load())
	require.Len(t, rangeStorage.readRanges(), 1)
}

func TestS3FSSingleEntryFallbackDoesNotMergeDifferentRanges(t *testing.T) {
	ctx := context.Background()
	originalShortWait := shortIOWaitDuration
	shortIOWaitDuration = time.Millisecond
	t.Cleanup(func() { shortIOWaitDuration = originalShortWait })

	fs, err := NewS3FS(
		ctx,
		ObjectStorageArguments{
			Name:      "s3",
			Endpoint:  "disk",
			Bucket:    t.TempDir(),
			KeyPrefix: time.Now().Format("2006-01-02.15:04:05.000000"),
		},
		CacheConfig{
			MemoryCapacity: ptrTo[toml.ByteSize](1 << 20),
			DiskPath:       ptrTo(t.TempDir()),
			DiskCapacity:   ptrTo[toml.ByteSize](1 << 30),
		},
		nil,
		false,
		false,
	)
	require.NoError(t, err)
	t.Cleanup(func() { fs.Close(ctx) })

	rangeStorage := &generatedRangeObjectStorage{}
	storage := &blockingReadObjectStorage{
		ObjectStorage: rangeStorage,
		readStarted:   make(chan struct{}),
		releaseRead:   make(chan struct{}),
	}
	fs.storage = storage

	newVector := func(offset int64) *IOVector {
		return &IOVector{
			FilePath: "foo/bar",
			Entries: []IOEntry{{
				Offset:      offset,
				Size:        7,
				ToCacheData: CacheOriginalData,
			}},
		}
	}
	fullObjectDone, fullObjectWait := fs.ioMerger.Merge(
		fs.readMergeKey(newVector(123)), maxIOWaitDuration,
	)
	require.NotNil(t, fullObjectDone)
	require.Nil(t, fullObjectWait)
	releaseFullObject := sync.OnceFunc(fullObjectDone)
	releaseStorage := sync.OnceFunc(func() { close(storage.releaseRead) })
	t.Cleanup(func() {
		releaseStorage()
		releaseFullObject()
	})

	first := newVector(123)
	firstDone := make(chan error, 1)
	go func() { firstDone <- fs.Read(ctx, first) }()
	select {
	case <-storage.readStarted:
	case <-time.After(5 * time.Second):
		t.Fatal("first exact range did not reach object storage")
	}

	second := newVector(456)
	secondDone := make(chan error, 1)
	go func() { secondDone <- fs.Read(ctx, second) }()
	select {
	case err := <-secondDone:
		require.NoError(t, err)
	case <-time.After(5 * time.Second):
		t.Fatal("different exact range waited for an unrelated leader")
	}
	require.Equal(t, int64(2), storage.readCount.Load())

	releaseStorage()
	select {
	case err := <-firstDone:
		require.NoError(t, err)
	case <-time.After(5 * time.Second):
		t.Fatal("first exact range did not terminate")
	}
	first.Release()
	second.Release()
}

func TestS3FSRangeReadSkipsPrefetchFullObjectIOMerge(t *testing.T) {
	ctx := context.Background()
	fs, err := NewS3FS(
		ctx,
		ObjectStorageArguments{
			Name:      "s3",
			Endpoint:  "disk",
			Bucket:    t.TempDir(),
			KeyPrefix: time.Now().Format("2006-01-02.15:04:05.000000"),
		},
		CacheConfig{
			DiskPath:     ptrTo(t.TempDir()),
			DiskCapacity: ptrTo[toml.ByteSize](1 << 30),
		},
		nil,
		false,
		false,
	)
	assert.Nil(t, err)
	defer fs.Close(ctx)

	data := bytes.Repeat([]byte("abcd"), 1<<10)
	err = fs.Write(ctx, IOVector{
		FilePath: "foo/bar",
		Entries: []IOEntry{
			{
				Size: int64(len(data)),
				Data: data,
			},
		},
		Policy: SkipDiskCache | SkipMemoryCache,
	})
	assert.Nil(t, err)

	doneMerge, waitMerge := fs.ioMerger.Merge(IOMergeKey{
		Path:       "foo/bar",
		FullObject: true,
		CacheFill:  true,
	}, maxIOWaitDuration)
	assert.NotNil(t, doneMerge)
	assert.Nil(t, waitMerge)
	releasedMerge := false
	releaseMerge := func() {
		if !releasedMerge {
			doneMerge()
			releasedMerge = true
		}
	}
	defer releaseMerge()

	type readResult struct {
		data []byte
		err  error
	}
	readDone := make(chan readResult, 1)
	go func() {
		vec := &IOVector{
			FilePath: "foo/bar",
			Entries: []IOEntry{
				{
					Offset: 5,
					Size:   9,
				},
			},
		}
		err := fs.Read(ctx, vec)
		defer vec.Release()
		if err != nil {
			readDone <- readResult{err: err}
			return
		}
		readDone <- readResult{data: append([]byte(nil), vec.Entries[0].Data...)}
	}()

	select {
	case result := <-readDone:
		assert.Nil(t, result.err)
		assert.Equal(t, data[5:14], result.data)
	case <-time.After(2 * time.Second):
		releaseMerge()
		result := <-readDone
		t.Fatalf("range read waited for prefetch full-object io merge: %v", result.err)
	}
}

func TestS3FSReadFullObjectToDiskCacheStreamingDoesNotOpenReaderWhenCacheExists(t *testing.T) {
	ctx := context.Background()
	cache, err := NewDiskCache(ctx, t.TempDir(), fscache.ConstCapacity(1<<20), nil, false, nil, "")
	assert.Nil(t, err)
	defer cache.Close(ctx)

	err = cache.SetFile(ctx, "foo/bar", func(context.Context) (io.ReadCloser, error) {
		return io.NopCloser(bytes.NewReader([]byte("hello world"))), nil
	})
	assert.Nil(t, err)

	fs := &S3FS{
		diskCache: cache,
	}
	vector := &IOVector{
		FilePath: "foo/bar",
		Entries: []IOEntry{
			{
				Offset: 1,
				Size:   4,
			},
		},
	}

	getReaderCalls := 0
	done, err := fs.readFullObjectToDiskCacheStreaming(
		ctx,
		vector,
		"foo/bar",
		func(context.Context, *int64, *int64) (io.ReadCloser, error) {
			getReaderCalls++
			return io.NopCloser(bytes.NewReader([]byte("backend"))), nil
		},
	)
	assert.Nil(t, err)
	assert.False(t, done)
	assert.Zero(t, getReaderCalls)
}

// Count bytes consumed, not just the ranges passed to ObjectStorage.Read.
// The fixture drives prefetch and foreground reads serially.
type cacheFillReadMeter struct {
	ObjectStorage
	bytes         atomic.Int64
	opens, closes int
	beforeOpen    func() error
	afterClose    func()
}

func (m *cacheFillReadMeter) Read(ctx context.Context, key string, min, max *int64) (io.ReadCloser, error) {
	if m.beforeOpen != nil {
		if err := m.beforeOpen(); err != nil {
			return nil, err
		}
	}
	r, err := m.ObjectStorage.Read(ctx, key, min, max)
	if err != nil {
		return nil, err
	}
	m.opens++
	return &readCloser{r: &countingReader{R: r, C: &m.bytes}, closeFunc: func() error {
		m.closes++
		err := r.Close()
		if m.afterClose != nil {
			m.afterClose()
		}
		return err
	}}, nil
}

func TestS3FSReadAfterMissRacesFullFilePublication(t *testing.T) {
	const far = 9 << 20
	data := make([]byte, far+4096)
	for i := range data {
		data[i] = byte(i*31 + i/251)
	}
	injected := errors.New("second range read failed")
	for _, tc := range []struct {
		name                    string
		second, size, wantBytes int64
		wantOpens               int
		wantErr                 error
	}{
		{"many-published", far, 1, 0, 0, nil},
		{"many-missing", far, 1, far - 4096 + 1, 1, nil},
		{"partly-published", far, 17, 17, 1, nil},
		{"sparse", far, 17, 30, 2, nil},
		{"dense", 4112, 17, 33, 1, nil},
		{"to-end", far, -1, 4096, 1, nil},
		{"second-range-error", far, 17, 13, 1, injected},
		{"cancel-between-ranges", far, 17, 13, 1, context.Canceled},
	} {
		t.Run(tc.name, func(t *testing.T) {
			ctx, cancel := context.WithCancel(t.Context())
			defer cancel()
			var counter perfcounter.CounterSet
			ctx = perfcounter.WithCounterSet(ctx, &counter)
			fs, err := NewS3FS(ctx, ObjectStorageArguments{Name: "s3", Endpoint: "disk", Bucket: t.TempDir()}, CacheConfig{
				DiskPath: ptrTo(t.TempDir()), DiskCapacity: ptrTo[toml.ByteSize](32 << 20), MemoryCapacity: ptrTo[toml.ByteSize](0),
			}, nil, false, true)
			require.NoError(t, err)
			defer fs.Close(t.Context())
			const path = "object"
			require.NoError(t, fs.Write(ctx, IOVector{FilePath: path, Entries: []IOEntry{{Size: int64(len(data)), Data: data}}, Policy: SkipDiskCache | SkipMemoryCache}))
			m := &cacheFillReadMeter{ObjectStorage: fs.storage}
			fs.storage = m
			v := &IOVector{FilePath: path, Entries: []IOEntry{{Offset: 4096, Size: 13}, {Offset: tc.second, Size: tc.size}}}
			defer v.Release()
			if tc.size < 0 {
				v.Entries = v.Entries[1:]
			}
			if strings.HasPrefix(tc.name, "many-") {
				v.Entries = nil
				for i := int64(0); i < 64; i++ {
					v.Entries = append(v.Entries, IOEntry{Offset: 4096 + i*(far-4096)/63, Size: 1})
				}
			}
			// Preserve a real partial cache hit in the sparse fallback.
			if tc.name == "sparse" {
				hit := IOEntry{Offset: 100, Size: 3, Data: data[100:103]}
				require.NoError(t, fs.diskCache.Update(ctx, &IOVector{FilePath: path, Entries: []IOEntry{hit}}, false))
				v.Entries = append([]IOEntry{{Offset: hit.Offset, Size: hit.Size}}, v.Entries...)
			}
			// Disk lookup precedes remote lookup. Publish synchronously at this
			// boundary: the foreground request missed the file but its SetFile
			// will find it. No sleeps or production scheduling hooks are needed.
			readDone := make(chan struct{})
			var updateDone chan struct{}
			var updateGuardExpired bool
			finishRead := sync.OnceFunc(func() {
				close(readDone)
				if updateDone != nil {
					<-updateDone
				}
			})
			defer finishRead()
			fs.remoteCache = NewRemoteCache(nil, func() client.KeyRouter[fscache.CacheKey] {
				require.NoError(t, fs.PrefetchFile(ctx, path))
				fs.diskCache.Flush(ctx)
				require.Equal(t, int64(len(data)), m.bytes.Load())
				require.Equal(t, 1, m.opens)
				require.Equal(t, 1, m.closes)
				if tc.name == "many-published" && updateDone == nil {
					// This update started after the initial disk lookup. Reusing
					// the full file must not wait for an unrelated entry writer.
					entryPath := fs.diskCache.pathForIOEntry(path, v.Entries[0])
					finish := holdDiskCacheUpdate(t, fs.diskCache, entryPath)
					updateDone = make(chan struct{})
					go func() {
						defer close(updateDone)
						defer finish()
						timer := time.NewTimer(5 * time.Second)
						defer timer.Stop()
						select {
						case <-readDone:
						case <-timer.C:
							updateGuardExpired = true
						}
					}()
				}
				if tc.name != "many-published" {
					// A short local file cannot satisfy the requested ranges. SetFile
					// still skips it; the foreground request must fall back to storage.
					usable := int64(3)
					if tc.name == "partly-published" {
						usable = 4109
					} // covers only the first missing entry
					require.NoError(t, os.Truncate(fs.diskCache.pathForFile(path), usable))
				}
				if tc.wantErr == context.Canceled {
					m.afterClose = cancel
					m.beforeOpen = func() error {
						require.NoError(t, ctx.Err(), "must not attempt another read after cancellation")
						return nil
					}
				} else if tc.wantErr != nil {
					m.beforeOpen = func() error {
						if m.opens == 2 {
							return injected
						}
						return nil
					}
				}
				return nil
			})
			err = fs.Read(ctx, v)
			finishRead()
			require.False(t, updateGuardExpired, "full file recheck waited for an entry update")
			if tc.wantErr == nil {
				require.NoError(t, err)
				for _, e := range v.Entries {
					require.Equal(t, data[e.Offset:e.Offset+e.Size], e.Data)
				}
			} else {
				require.ErrorIs(t, err, tc.wantErr)
				require.Equal(t, data[4096:4109], v.Entries[0].Data)
				require.Empty(t, v.Entries[1].Data)
			}
			require.Equal(t, tc.wantBytes, m.bytes.Load()-int64(len(data)), "foreground bytes consumed")
			require.Equal(t, tc.wantOpens, m.opens-1)
			require.Equal(t, m.opens, m.closes)
			if tc.name == "many-published" {
				require.NotNil(t, updateDone, "fixture must park an entry update")
				require.EqualValues(t, 64, counter.FileService.DiskReadSize.Load())
				require.Zero(t, counter.FileService.S3ReadSize.Load())
			}
			if tc.name == "partly-published" {
				require.EqualValues(t, 13, counter.FileService.DiskReadSize.Load())
				require.EqualValues(t, 17, counter.FileService.S3ReadSize.Load())
			}
			warm := &IOVector{FilePath: path, Entries: []IOEntry{{Offset: 0, Size: 3}}}
			defer warm.Release()
			require.NoError(t, fs.Read(t.Context(), warm))
			require.Equal(t, data[:3], warm.Entries[0].Data)
			require.Equal(t, tc.wantOpens, m.opens-1, "a new read should hit the published file")
		})
	}
}

// Count the HTTP body written by the endpoint, including bytes the SDK drains
// on Close, rather than bytes returned by ObjectStorage.Read to S3FS.
type rangeResponseWriter struct {
	http.ResponseWriter
	n      int64
	status int
}

func (w *rangeResponseWriter) WriteHeader(status int) {
	w.status = status
	w.ResponseWriter.WriteHeader(status)
}

func (w *rangeResponseWriter) Write(p []byte) (int, error) {
	n, err := w.ResponseWriter.Write(p)
	w.n += int64(n)
	return n, err
}

func TestS3FSSkippedFillHTTPRanges(t *testing.T) {
	const far = 9 << 20
	data := make([]byte, 16<<20)
	data[0], data[far] = 'a', 'b'
	for _, backend := range []string{"minio", "aws", "cos", "oss"} {
		for _, mode := range []string{"sparse", "at-eof", "past-eof-fixed", "not-found"} {
			t.Run(backend+"/"+mode, func(t *testing.T) {
				var mu sync.Mutex
				var ranges []string
				var statuses []int
				var bodyBytes int64
				srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
					w.Header().Set("ETag", `"test"`)
					out := &rangeResponseWriter{ResponseWriter: w}
					defer func() {
						if r.Method == http.MethodGet {
							mu.Lock()
							defer mu.Unlock()
							ranges = append(ranges, r.Header.Get("Range"))
							statuses = append(statuses, out.status)
							bodyBytes += out.n
						}
					}()
					if mode == "not-found" || mode == "at-eof" || mode == "past-eof-fixed" {
						w.Header().Set("Content-Type", "application/xml")
						if mode == "not-found" {
							out.WriteHeader(http.StatusNotFound)
							_, _ = io.WriteString(out, `<Error><Code>NoSuchKey</Code><Message>Not found</Message></Error>`)
						} else {
							out.WriteHeader(http.StatusRequestedRangeNotSatisfiable)
							_, _ = io.WriteString(out, `<Error><Code>InvalidRange</Code><Message>Outside object</Message></Error>`)
						}
						return
					}
					http.ServeContent(out, r, "object", time.Unix(1, 0), bytes.NewReader(data))
				}))
				defer srv.Close()
				ctx := t.Context()
				fs, err := NewS3FS(ctx, ObjectStorageArguments{
					Name: "s3", Endpoint: srv.URL, Bucket: "bucket", Region: "us-east-1", IsMinio: backend == "minio",
					NoBucketValidation: true, KeyID: "id", KeySecret: "secret",
				}, CacheConfig{
					DiskPath: ptrTo(t.TempDir()), DiskCapacity: ptrTo[toml.ByteSize](32 << 20), MemoryCapacity: ptrTo[toml.ByteSize](0),
				}, nil, false, true)
				require.NoError(t, err)
				defer fs.Close(ctx)
				// The production router recognizes vendor hostnames; point the real
				// clients directly at the loopback endpoint instead.
				switch backend {
				case "cos":
					u, err := url.Parse(srv.URL)
					require.NoError(t, err)
					c := cos.NewClient(&cos.BaseURL{BucketURL: u}, srv.Client())
					c.Conf.RetryOpt.Count = 0
					fs.storage = &QCloudSDK{client: c}
				case "oss":
					c, err := oss.New(srv.URL, "id", "secret", oss.UseCname(true), oss.HTTPClient(srv.Client()))
					require.NoError(t, err)
					b, err := c.Bucket("bucket")
					require.NoError(t, err)
					fs.storage = &AliyunSDK{bucket: b}
				}
				fs.remoteCache = NewRemoteCache(nil, func() client.KeyRouter[fscache.CacheKey] {
					require.NoError(t, fs.diskCache.SetFile(ctx, "object", func(context.Context) (io.ReadCloser, error) { return io.NopCloser(bytes.NewReader(nil)), nil }))
					return nil
				})
				v := IOVector{FilePath: "object", Entries: []IOEntry{{Offset: 0, Size: 1}, {Offset: far, Size: 1}}}
				defer v.Release()
				if mode == "at-eof" || mode == "not-found" {
					v.Entries = []IOEntry{{Offset: int64(len(data)), Size: -1}}
				} else if mode == "past-eof-fixed" {
					v.Entries = []IOEntry{{Offset: int64(len(data)) + 1, Size: 1}}
				}
				err = fs.Read(ctx, &v)
				srv.Close() // join handlers before inspecting byte counts
				switch mode {
				case "sparse":
					require.NoError(t, err)
					require.Equal(t, []byte{'a'}, v.Entries[0].Data)
					require.Equal(t, []byte{'b'}, v.Entries[1].Data)
					require.Equal(t, int64(2), bodyBytes, "HTTP response body bytes")
					require.Equal(t, []string{"bytes=0-0", "bytes=9437184-9437184"}, ranges)
				case "at-eof":
					require.Equal(t, []int{http.StatusRequestedRangeNotSatisfiable}, statuses)
					require.True(t, moerr.IsMoErrCode(err, moerr.ErrEmptyRange), "%v", err)
				case "past-eof-fixed":
					require.Equal(t, []int{http.StatusRequestedRangeNotSatisfiable}, statuses)
					require.True(t, moerr.IsMoErrCode(err, moerr.ErrUnexpectedEOF), "%v", err)
				case "not-found":
					require.True(t, moerr.IsMoErrCode(err, moerr.ErrFileNotFound), "%v", err)
				}
			})
		}
	}
}

func TestS3FSReadFullObjectToDiskCacheStreamingDoesNotWaitForAsyncFinalize(t *testing.T) {
	ctx := context.Background()
	cache, err := NewDiskCache(ctx, t.TempDir(), fscache.ConstCapacity(1<<20), nil, false, nil, "")
	require.NoError(t, err)

	syncStarted, unblock := installBlockedDiskCacheFileSync(cache)
	t.Cleanup(func() {
		unblock()
		cache.Close(ctx)
	})

	s3 := &S3FS{
		diskCache:   cache,
		asyncUpdate: true,
	}
	vector := &IOVector{
		FilePath: "foo/bar",
		Entries: []IOEntry{{
			Offset: 1,
			Size:   4,
		}},
	}
	type result struct {
		done bool
		err  error
	}
	readDone := make(chan result, 1)
	go func() {
		done, err := s3.readFullObjectToDiskCacheStreaming(
			ctx,
			vector,
			"foo/bar",
			func(context.Context, *int64, *int64) (io.ReadCloser, error) {
				return io.NopCloser(bytes.NewReader([]byte("hello world"))), nil
			},
		)
		readDone <- result{done: done, err: err}
	}()

	select {
	case <-syncStarted:
	case <-time.After(diskCacheLifecycleTestTimeout):
		t.Fatal("async full-object finalizer did not start")
	}
	select {
	case result := <-readDone:
		require.NoError(t, result.err)
		require.True(t, result.done)
	case <-time.After(diskCacheLifecycleTestTimeout):
		unblock()
		result := <-readDone
		t.Fatalf("full-object read waited for disk finalization: %v", result.err)
	}
	require.Equal(t, []byte("ello"), vector.Entries[0].Data)
	// The streaming helper fills Data but leaves done unchanged for its caller,
	// matching the existing full-object streaming contract.
	require.False(t, vector.Entries[0].done)
	require.True(t, cache.isUpdating(cache.pathForFile("foo/bar")))

	unblock()
	flushCtx, cancel := context.WithTimeout(ctx, diskCacheLifecycleTestTimeout)
	defer cancel()
	cache.Flush(flushCtx)
	require.NoError(t, flushCtx.Err())
}

func TestS3FSReadIOMergerReleaseMatchesDiskFinalizeMode(t *testing.T) {
	for _, test := range []struct {
		name  string
		async bool
	}{
		{name: "async", async: true},
		{name: "sync", async: false},
	} {
		t.Run(test.name, func(t *testing.T) {
			ctx := context.Background()
			fs, err := NewS3FS(
				ctx,
				ObjectStorageArguments{
					Name:      "s3",
					Endpoint:  "disk",
					Bucket:    t.TempDir(),
					KeyPrefix: time.Now().Format("2006-01-02.15:04:05.000000"),
				},
				CacheConfig{
					DiskPath:     ptrTo(t.TempDir()),
					DiskCapacity: ptrTo[toml.ByteSize](1 << 20),
				},
				nil,
				false,
				false,
			)
			require.NoError(t, err)
			require.NoError(t, fs.Write(ctx, IOVector{
				FilePath: "foo/bar",
				Entries: []IOEntry{{
					Size: 11,
					Data: []byte("hello world"),
				}},
				Policy: SkipDiskCache | SkipMemoryCache,
			}))
			fs.SetAsyncUpdate(test.async)

			syncStarted, unblock := installBlockedDiskCacheFileSync(fs.diskCache)
			t.Cleanup(func() {
				unblock()
				fs.Close(ctx)
			})

			vector := &IOVector{
				FilePath: "foo/bar",
				Entries:  []IOEntry{{Offset: 1, Size: 4}},
			}
			t.Cleanup(vector.Release)
			mergeKey := fs.readMergeKey(vector)
			readDone := make(chan error, 1)
			go func() { readDone <- fs.Read(ctx, vector) }()
			select {
			case <-syncStarted:
			case <-time.After(diskCacheLifecycleTestTimeout):
				t.Fatal("full-object disk-cache finalization did not start")
			}

			if test.async {
				select {
				case err := <-readDone:
					require.NoError(t, err)
				case <-time.After(diskCacheLifecycleTestTimeout):
					unblock()
					t.Fatalf("async full-object read waited for disk finalization: %v", <-readDone)
				}
				require.False(t, fs.ioMerger.IsMerging(mergeKey))
				require.True(t, fs.diskCache.isUpdating(fs.diskCache.pathForFile("foo/bar")))
			} else {
				require.True(t, fs.ioMerger.IsMerging(mergeKey))
				select {
				case err := <-readDone:
					t.Fatalf("synchronous full-object read returned before disk finalization: %v", err)
				case <-time.After(50 * time.Millisecond):
				}
			}

			unblock()
			if !test.async {
				require.NoError(t, <-readDone)
			}
			require.Equal(t, []byte("ello"), vector.Entries[0].Data)
			flushCtx, cancel := context.WithTimeout(ctx, diskCacheLifecycleTestTimeout)
			defer cancel()
			fs.FlushCache(flushCtx)
			require.NoError(t, flushCtx.Err())
			require.False(t, fs.ioMerger.IsMerging(mergeKey))
		})
	}
}

func TestS3FSRangeFollowerDoesNotWaitForAsyncDiskFinalize(t *testing.T) {
	originalShortWait := shortIOWaitDuration
	shortIOWaitDuration = 5 * time.Millisecond
	t.Cleanup(func() { shortIOWaitDuration = originalShortWait })

	var pcSet perfcounter.CounterSet
	ctx := perfcounter.WithCounterSet(context.Background(), &pcSet)
	fs, err := NewS3FS(
		ctx,
		ObjectStorageArguments{
			Name:      "s3",
			Endpoint:  "disk",
			Bucket:    t.TempDir(),
			KeyPrefix: time.Now().Format("2006-01-02.15:04:05.000000"),
		},
		CacheConfig{
			DiskPath:     ptrTo(t.TempDir()),
			DiskCapacity: ptrTo[toml.ByteSize](1 << 20),
		},
		nil,
		false,
		false,
	)
	require.NoError(t, err)
	require.NoError(t, fs.Write(ctx, IOVector{
		FilePath: "foo/bar",
		Entries:  []IOEntry{{Size: 11, Data: []byte("hello world")}},
		Policy:   SkipDiskCache | SkipMemoryCache,
	}))

	syncStarted, unblock := installBlockedDiskCacheFileSync(fs.diskCache)
	t.Cleanup(func() {
		unblock()
		fs.Close(ctx)
	})

	newVector := func() *IOVector {
		return &IOVector{
			FilePath: "foo/bar",
			Entries:  []IOEntry{{Offset: 1, Size: 4}},
			Policy:   SkipFullFilePreloads,
		}
	}
	leader := newVector()
	defer leader.Release()
	require.NoError(t, fs.Read(ctx, leader))
	require.Equal(t, []byte("ello"), leader.Entries[0].Data)
	select {
	case <-syncStarted:
	case <-time.After(diskCacheLifecycleTestTimeout):
		t.Fatal("async range finalizer did not start")
	}

	follower := newVector()
	defer follower.Release()
	followerDone := make(chan error, 1)
	go func() { followerDone <- fs.Read(ctx, follower) }()
	select {
	case err := <-followerDone:
		require.NoError(t, err)
	case <-time.After(diskCacheLifecycleTestTimeout):
		unblock()
		<-followerDone
		t.Fatal("range follower waited for async disk finalization")
	}
	require.Equal(t, []byte("ello"), follower.Entries[0].Data)
	require.Equal(t, int64(2), pcSet.FileService.S3.Get.Load())
	diskPath := fs.diskCache.pathForIOEntry("foo/bar", follower.Entries[0])
	require.True(t, fs.diskCache.isUpdating(diskPath))

	unblock()
	flushCtx, cancel := context.WithTimeout(ctx, time.Second)
	defer cancel()
	fs.FlushCache(flushCtx)
	require.NoError(t, flushCtx.Err())
}

func TestS3FSReadFullObjectToDiskCacheStreamingReturnsReaderError(t *testing.T) {
	ctx := context.Background()
	cache, err := NewDiskCache(ctx, t.TempDir(), fscache.ConstCapacity(1<<20), nil, false, nil, "")
	assert.Nil(t, err)
	defer cache.Close(ctx)

	fs := &S3FS{
		diskCache: cache,
	}
	vector := &IOVector{
		FilePath: "foo/bar",
		Entries: []IOEntry{
			{
				Offset: 1,
				Size:   3,
			},
		},
	}
	readerErr := errors.New("reader failed")

	done, err := fs.readFullObjectToDiskCacheStreaming(
		ctx,
		vector,
		"foo/bar",
		func(context.Context, *int64, *int64) (io.ReadCloser, error) {
			return &errorReadCloser{
				reader: bytes.NewReader([]byte("hello world")),
				err:    readerErr,
			}, nil
		},
	)

	assert.True(t, done)
	assert.ErrorIs(t, err, readerErr)
	assert.False(t, vector.Entries[0].done)
	assert.Nil(t, vector.Entries[0].Data)
	assert.Equal(t, int64(3), vector.Entries[0].Size)
}

func TestS3FSReadFullObjectToDiskCacheStreamingReadToEnd(t *testing.T) {
	ctx := context.Background()
	cache, err := NewDiskCache(ctx, t.TempDir(), fscache.ConstCapacity(1<<20), nil, false, nil, "")
	assert.Nil(t, err)
	defer cache.Close(ctx)

	fs := &S3FS{
		diskCache: cache,
	}
	vector := &IOVector{
		FilePath: "foo/bar",
		Entries: []IOEntry{
			{
				Offset: 6,
				Size:   -1,
			},
		},
	}

	done, err := fs.readFullObjectToDiskCacheStreaming(
		ctx,
		vector,
		"foo/bar",
		func(context.Context, *int64, *int64) (io.ReadCloser, error) {
			return io.NopCloser(bytes.NewReader([]byte("hello world"))), nil
		},
	)

	assert.True(t, done)
	assert.Nil(t, err)
	assert.False(t, vector.Entries[0].done)
	assert.Equal(t, []byte("world"), vector.Entries[0].Data)
	assert.Equal(t, int64(5), vector.Entries[0].Size)
}

func TestS3FSReadFullObjectToDiskCacheStreamingUnexpectedEOF(t *testing.T) {
	ctx := context.Background()
	cache, err := NewDiskCache(ctx, t.TempDir(), fscache.ConstCapacity(1<<20), nil, false, nil, "")
	assert.Nil(t, err)
	defer cache.Close(ctx)

	fs := &S3FS{
		diskCache: cache,
	}
	vector := &IOVector{
		FilePath: "foo/bar",
		Entries: []IOEntry{
			{
				Offset: 6,
				Size:   20,
			},
		},
	}

	done, err := fs.readFullObjectToDiskCacheStreaming(
		ctx,
		vector,
		"foo/bar",
		func(context.Context, *int64, *int64) (io.ReadCloser, error) {
			return io.NopCloser(bytes.NewReader([]byte("hello world"))), nil
		},
	)

	assert.True(t, done)
	assert.True(t, moerr.IsMoErrCode(err, moerr.ErrUnexpectedEOF))
	assert.False(t, vector.Entries[0].done)
	assert.Nil(t, vector.Entries[0].Data)
	assert.Equal(t, int64(20), vector.Entries[0].Size)
}

func TestS3FSShouldStreamFullObjectToDiskCacheExclusions(t *testing.T) {
	ctx := context.Background()
	cache, err := NewDiskCache(ctx, t.TempDir(), fscache.ConstCapacity(1<<20), nil, false, nil, "")
	assert.Nil(t, err)
	defer cache.Close(ctx)

	fs := &S3FS{
		diskCache: cache,
	}

	assert.False(t, fs.shouldStreamFullObjectToDiskCache(&IOVector{
		FilePath: "foo/bar",
		Entries: []IOEntry{
			{
				Size: 0,
			},
		},
	}))

	buf := new(bytes.Buffer)
	assert.False(t, fs.shouldStreamFullObjectToDiskCache(&IOVector{
		FilePath: "foo/bar",
		Entries: []IOEntry{
			{
				Size:          3,
				WriterForRead: buf,
			},
		},
	}))

	var reader io.ReadCloser
	assert.False(t, fs.shouldStreamFullObjectToDiskCache(&IOVector{
		FilePath: "foo/bar",
		Entries: []IOEntry{
			{
				Size:              3,
				ReadCloserForRead: &reader,
			},
		},
	}))
}

func TestS3FSWillCacheFullObject(t *testing.T) {
	ctx := context.Background()
	cache, err := NewDiskCache(ctx, t.TempDir(), fscache.ConstCapacity(1<<20), nil, false, nil, "")
	require.NoError(t, err)
	t.Cleanup(func() { cache.Close(ctx) })
	fs := &S3FS{diskCache: cache}

	writer := new(bytes.Buffer)
	var reader io.ReadCloser
	tests := []struct {
		name   string
		vector *IOVector
		want   bool
	}{
		{
			name:   "ordinary entry",
			vector: &IOVector{Entries: []IOEntry{{Size: 1}}},
			want:   true,
		},
		{
			name:   "writer only",
			vector: &IOVector{Entries: []IOEntry{{Size: 1, WriterForRead: writer}}},
		},
		{
			name:   "read closer only",
			vector: &IOVector{Entries: []IOEntry{{Size: 1, ReadCloserForRead: &reader}}},
		},
		{
			name: "mixed ordinary and writer",
			vector: &IOVector{Entries: []IOEntry{
				{Size: 1, WriterForRead: writer},
				{Offset: 1, Size: 1},
			}},
			want: true,
		},
		{
			name: "zero-sized entry prevents publication",
			vector: &IOVector{Entries: []IOEntry{
				{Size: 1},
				{Offset: 1, Size: 0},
			}},
		},
		{
			name: "disk cache writes disabled",
			vector: &IOVector{
				Policy:  SkipDiskCacheWrites,
				Entries: []IOEntry{{Size: 1}},
			},
		},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			require.Equal(t, test.want, fs.willCacheFullObject(test.vector))
		})
	}
}

type errorReadCloser struct {
	reader *bytes.Reader
	err    error
}

func (e *errorReadCloser) Read(p []byte) (int, error) {
	n, err := e.reader.Read(p)
	if err != nil {
		return n, e.err
	}
	return n, nil
}

func (e *errorReadCloser) Close() error {
	return nil
}

type S3CredentialTestCase struct {
	Skip bool
	ObjectStorageArguments
}

var s3CredentialTestCases = func() []S3CredentialTestCase {
	content, err := os.ReadFile("s3_fs_test_new.xml")
	if os.IsNotExist(err) {
		return nil
	}
	if err != nil {
		panic(err)
	}
	var spec struct {
		XMLName xml.Name               `xml:"Spec"`
		Cases   []S3CredentialTestCase `xml:"Case"`
	}
	err = xml.Unmarshal(content, &spec)
	if err != nil {
		panic(err)
	}
	return spec.Cases
}()

func TestNewS3FSFromSpec(t *testing.T) {
	if len(s3CredentialTestCases) == 0 {
		t.Skip("no case")
	}

	for _, kase := range s3CredentialTestCases {
		if kase.Skip {
			continue
		}

		t.Run(kase.Name, func(t *testing.T) {

			ctx := context.Background()
			fs, err := NewS3FS(
				ctx,
				kase.ObjectStorageArguments,
				DisabledCacheConfig,
				nil,
				true,
				false,
			)
			assert.Nil(t, err)
			_ = fs

		})

		t.Run(kase.Name+" bad bucket", func(t *testing.T) {

			args := kase.ObjectStorageArguments
			args.Bucket = args.Bucket + "foobarbaz"
			ctx := context.Background()
			_, err := NewS3FS(
				ctx,
				args,
				DisabledCacheConfig,
				nil,
				true,
				false,
			)
			if err == nil {
				t.Fatal("should fail")
			}

		})
	}

}

func TestNewS3NoDefaultCredential(t *testing.T) {
	ctx := context.Background()
	_, err := NewS3FS(
		ctx,
		ObjectStorageArguments{
			Endpoint: "aliyuncs.com",
		},
		DisabledCacheConfig,
		nil,
		true,
		true,
	)
	assert.True(t, moerr.IsMoErrCode(err, moerr.ErrInvalidInput))
	assert.True(t, strings.Contains(err.Error(), "no valid credentials"))
}

func TestS3FSIOMerger(t *testing.T) {
	ctx := context.Background()

	fs, err := NewS3FS(
		context.Background(),
		ObjectStorageArguments{
			Endpoint: "disk",
			Bucket:   t.TempDir(),
		},
		CacheConfig{
			MemoryCapacity: ptrTo(toml.ByteSize(1 << 20)),
		},
		nil,
		false,
		false,
	)
	require.NoError(t, err)

	err = fs.Write(ctx, IOVector{
		FilePath: "foo",
		Policy:   SkipDiskCache, // skip disk cache
		Entries: []IOEntry{
			{
				Data: []byte("foo"),
				Size: 3,
			},
		},
	})
	require.NoError(t, err)

	cache := &blockingDataCache{
		DataCache:     fs.memCache.cache,
		updateStarted: make(chan struct{}),
		releaseUpdate: make(chan struct{}),
	}
	fs.memCache.cache = cache
	storage := &blockingReadObjectStorage{
		ObjectStorage: fs.storage,
		readStarted:   make(chan struct{}),
		releaseRead:   make(chan struct{}),
	}
	fs.storage = storage
	readCtx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	releaseRead := sync.OnceFunc(func() { close(storage.releaseRead) })
	releaseUpdate := sync.OnceFunc(func() { close(cache.releaseUpdate) })
	var wg sync.WaitGroup
	t.Cleanup(func() {
		cancel()
		releaseRead()
		releaseUpdate()
		wg.Wait()
		fs.Close(ctx)
	})

	newReadVector := func() *IOVector {
		return &IOVector{
			FilePath: "foo",
			Policy:   SkipFullFilePreloads,
			Entries: []IOEntry{
				{
					Size:        3,
					ToCacheData: CacheOriginalData,
				},
			},
		}
	}
	read := func(ctx context.Context) error {
		vec := newReadVector()
		defer vec.Release()
		if err := fs.Read(ctx, vec); err != nil {
			return err
		}
		if !bytes.Equal([]byte("foo"), vec.Entries[0].CachedData.Bytes()) {
			return errors.New("unexpected read data")
		}
		return nil
	}

	results := make(chan error, 2)
	startRead := func(ctx context.Context) {
		wg.Add(1)
		go func() {
			defer wg.Done()
			results <- read(ctx)
		}()
	}
	waitForSignal := func(ch <-chan struct{}, name string) {
		t.Helper()
		select {
		case <-ch:
		case <-readCtx.Done():
			t.Fatalf("timed out waiting for %s: %v", name, readCtx.Err())
		}
	}

	startRead(readCtx)
	waitForSignal(storage.readStarted, "object read")
	releaseRead()
	waitForSignal(cache.updateStarted, "cache update")

	// The cache update is deliberately blocked. The merge must still be active
	// at this point, otherwise a waiter can become a second merge leader before
	// the first leader's cache contents are visible. This assertion is
	// deterministically false with defer done() at the Merge call site.
	require.True(t, fs.ioMerger.IsMerging(fs.readMergeKey(newReadVector())))

	waiterCtx := WithEventLogger(readCtx)
	startRead(waiterCtx)
	waiterLogger := waiterCtx.Value(EventLoggerKey).(*eventLogger)
	require.Eventually(t, func() bool {
		waiterLogger.mu.Lock()
		defer waiterLogger.mu.Unlock()
		for _, ev := range *waiterLogger.events {
			if ev.ev == str_ioMerger_Merge_wait {
				return true
			}
		}
		return false
	}, 5*time.Second, time.Millisecond)

	releaseUpdate()
	require.NoError(t, <-results)
	require.NoError(t, <-results)
	require.Equal(t, int64(1), storage.readCount.Load())
}

func BenchmarkS3FSAllocateCacheData(b *testing.B) {
	ctx := context.Background()

	fs, err := NewS3FS(
		context.Background(),
		ObjectStorageArguments{
			Name:      "s3",
			Endpoint:  "disk",
			Bucket:    b.TempDir(),
			KeyPrefix: time.Now().Format("2006-01-02.15:04:05.000000"),
		},
		CacheConfig{
			MemoryCapacity: ptrTo[toml.ByteSize](128 * 1024),
		},
		nil,
		false,
		false,
	)
	assert.Nil(b, err)
	defer fs.Close(ctx)

	b.ResetTimer()
	b.RunParallel(func(pb *testing.PB) {
		for pb.Next() {
			data := fs.AllocateCacheData(ctx, 42)
			data.Release()
		}
	})
}

func TestS3FSFromSpecs(t *testing.T) {

	for _, args := range objectStorageArgumentsForTest("test", t) {

		t.Run(args.Name, func(t *testing.T) {

			testFileServiceWithContext(t, 0, func(ctx context.Context, name string) FileService {
				args.Name = name
				args.KeyPrefix = time.Now().Format("2006-01-02.15:04:05.000000")

				fs, err := NewS3FS(
					ctx,
					args,
					DisabledCacheConfig,
					nil,
					true,
					false,
				)
				if err != nil {
					logutil.Error("S3FS errror",
						zap.Any("args", args),
						zap.Any("error", err),
					)
					t.Fatal(err)
				}

				return fs
			})

		})

	}

}

func TestS3FSCloseDrainsDiskBeforeDirectoryReuse(t *testing.T) {
	for _, mode := range []string{"active decode", "retry after expired close"} {
		t.Run(mode, func(t *testing.T) {
			ctx, cancel := context.WithTimeout(t.Context(), diskCacheLifecycleTestTimeout)
			defer cancel()
			args := ObjectStorageArguments{Name: "s3", Endpoint: "disk", Bucket: t.TempDir()}
			config := CacheConfig{
				MemoryCapacity: ptrTo[toml.ByteSize](1 << 20),
				DiskCapacity:   ptrTo[toml.ByteSize](1 << 20),
				DiskPath:       ptrTo(t.TempDir()),
			}
			openFS := func() *S3FS {
				fs, err := NewS3FS(ctx, args, config, nil, false, false)
				require.NoError(t, err)
				t.Cleanup(func() { fs.Close(context.Background()) })
				return fs
			}

			old := openFS()
			if mode == "active decode" {
				// Hold the same scope that S3FS.Read releases after a shared decode.
				require.True(t, old.decodedReads.beginRead())
				t.Cleanup(old.decodedReads.endRead)
			}
			started, release := installBlockedDiskCacheFileSync(old.diskCache)
			t.Cleanup(release)
			require.NoError(t, old.Write(ctx, IOVector{FilePath: "obj", Policy: SkipAllCache, Entries: []IOEntry{{Size: 3, Data: []byte("old")}}}))
			require.NoError(t, old.PrefetchFile(ctx, "obj"))
			select {
			case <-started:
			case <-ctx.Done():
				t.Fatal("old file sync did not start")
			}
			if mode == "retry after expired close" {
				expired, stop := context.WithDeadline(ctx, time.Unix(1, 0))
				defer stop()
				old.Close(expired)
			}
			wait := &waitStartedContext{Context: ctx, started: make(chan struct{})}
			done := make(chan struct{})
			go func() { old.Close(wait); close(done) }()
			select {
			case <-wait.started:
				release()
			case <-done:
				t.Fatal("Close returned with an old disk publication still blocked")
			case <-ctx.Done():
				t.Fatal("Close did not reach its disk drain")
			}
			select {
			case <-done:
			case <-ctx.Done():
				t.Fatal("Close did not join the released worker")
			}
			require.NoError(t, ctx.Err())

			next := openFS()
			next.SetAsyncUpdate(false)
			require.NoError(t, next.Delete(ctx, "obj"))
			require.NoError(t, next.Write(ctx, IOVector{FilePath: "obj", Entries: []IOEntry{{Size: 3, Data: []byte("new")}}}))
			for _, policy := range []Policy{SkipAllCache, SkipMemoryCache} {
				v := &IOVector{FilePath: "obj", Policy: policy, Entries: []IOEntry{{Size: 3}}}
				t.Cleanup(v.Release)
				require.NoError(t, next.Read(ctx, v))
				require.Equal(t, "new", string(v.Entries[0].Data))
				if policy == SkipMemoryCache {
					require.Same(t, next.diskCache, v.Entries[0].fromCache)
				}
			}
		})
	}
}

func TestS3FSPrefetchCanonicalObject(t *testing.T) {
	for _, path := range []string{"foo/bar", "replay:foo/bar"} {
		t.Run(path, func(t *testing.T) {
			fs := newReplayWriteTestFS(t)
			ctx := t.Context()
			require.NoError(t, fs.Write(ctx, IOVector{
				FilePath: "replay:foo/bar", Policy: SkipAllCache, Entries: []IOEntry{{Size: 3, Data: []byte("abc")}},
			}))
			require.NoError(t, fs.PrefetchFile(ctx, path))
			require.Equal(t, []byte("abc"), readReplayCache(t, fs, "foo/bar"))
			require.True(t, moerr.IsMoErrCode(fs.PrefetchFile(ctx, "replay:missing"), moerr.ErrFileNotFound))
			require.True(t, moerr.IsMoErrCode(fs.PrefetchFile(ctx, "elsewhere:foo/bar"), moerr.ErrWrongService))
		})
	}
}

type cancelingReadWriter struct {
	bytes.Buffer
	cancel context.CancelFunc
	err    error
}

func (w *cancelingReadWriter) Write(p []byte) (int, error) {
	defer w.cancel()
	if w.err != nil {
		return 0, w.err
	}
	return w.Buffer.Write(p)
}

func TestS3FSBufferedReadCancellationAfterDelivery(t *testing.T) {
	for _, tc := range []struct {
		name      string
		writerErr error
		missing   bool
	}{
		{name: "delivered"},
		{name: "writer failed", writerErr: io.ErrClosedPipe},
		{name: "source missing", missing: true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			fs := newReplayWriteTestFS(t)
			ctx, cancel := context.WithCancel(t.Context())
			defer cancel()
			if !tc.missing {
				require.NoError(t, fs.Write(ctx, IOVector{FilePath: "obj", Policy: SkipAllCache, Entries: []IOEntry{{Size: 3, Data: []byte("abc")}}}))
			}
			w := &cancelingReadWriter{cancel: cancel, err: tc.writerErr}
			v := &IOVector{FilePath: "obj", Entries: []IOEntry{{Size: 1}, {Offset: 1, Size: 2, WriterForRead: w}}}
			defer v.ReleaseReadResultOnError()
			err := fs.Read(ctx, v)
			if tc.missing {
				require.True(t, moerr.IsMoErrCode(err, moerr.ErrFileNotFound))
				require.NoError(t, ctx.Err(), "writer must not run on source failure")
				return
			}
			require.Equal(t, "a", string(v.Entries[0].Data))
			require.ErrorIs(t, ctx.Err(), context.Canceled)
			if tc.writerErr != nil {
				require.ErrorIs(t, err, tc.writerErr)
				return
			}
			require.NoError(t, err)
			require.Equal(t, "bc", w.String())
		})
	}
}
