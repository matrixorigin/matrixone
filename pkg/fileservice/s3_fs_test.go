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
	"os"
	"path"
	"runtime"
	"slices"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/matrixorigin/matrixone/pkg/common/moerr"
	"github.com/matrixorigin/matrixone/pkg/fileservice/fscache"
	"github.com/matrixorigin/matrixone/pkg/logutil"
	"github.com/matrixorigin/matrixone/pkg/perfcounter"
	"github.com/matrixorigin/matrixone/pkg/util/toml"
	"github.com/prometheus/client_golang/prometheus/testutil"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
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
	_, err = noCopier.CopyObject(ctx, src, "", "objects/b")
	require.True(t, moerr.IsMoErrCode(err, moerr.ErrFileNotFound), "got %v", err)
	_, err = noCopier.CopyObject(ctx, src, "objects/a", "")
	require.True(t, moerr.IsMoErrCode(err, moerr.ErrFileNotFound), "got %v", err)

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
	_, err = dst.CopyObject(ctx, src, "", "objects/b")
	require.True(t, moerr.IsMoErrCode(err, moerr.ErrFileNotFound), "got %v", err)
	_, err = dst.CopyObject(ctx, src, "objects/a", "")
	require.True(t, moerr.IsMoErrCode(err, moerr.ErrFileNotFound), "got %v", err)
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

func TestIsS3BackedFileServiceResolvesWrappers(t *testing.T) {
	s3 := &S3FS{name: "archive"}
	local := dummyFileService{name: "local"}
	services, err := NewFileServices("local", local, s3)
	require.NoError(t, err)

	require.True(t, IsS3BackedFileService(services, "archive:input.arrow"))
	require.True(t, IsS3BackedFileService(SubPath(s3, "tenant"), "input.arrow"))
	require.False(t, IsS3BackedFileService(services, "local:input.arrow"))
	require.False(t, IsS3BackedFileService(services, "missing:input.arrow"))
	require.False(t, IsS3BackedFileService(nil, "archive:input.arrow"))
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
	mu    sync.Mutex
	reads []objectStorageReadRange
}

type controlledRangeObjectStorage struct {
	dummyObjectStorage
	data            []byte
	followerStarted chan struct{}
	releaseFollower chan struct{}
	followerOnce    sync.Once
	readCount       atomic.Int64
}

type heldWriter struct {
	once    sync.Once
	started chan struct{}
	release chan struct{}
}

type blockingDataCache struct {
	memoryCacheBacking
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

func (b *blockingDataCache) Set(ctx context.Context, key fscache.CacheKey, data fscache.Data) (bool, error) {
	if b.updateCount.Add(1) == 1 {
		close(b.updateStarted)
		select {
		case <-ctx.Done():
			return false, ctx.Err()
		case <-b.releaseUpdate:
		}
	}
	return b.memoryCacheBacking.Set(ctx, key, data)
}

func (g *generatedRangeObjectStorage) Read(
	_ context.Context,
	key string,
	min *int64,
	max *int64,
) (io.ReadCloser, error) {
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

func (g *generatedRangeObjectStorage) readRanges() []objectStorageReadRange {
	g.mu.Lock()
	defer g.mu.Unlock()
	return append([]objectStorageReadRange(nil), g.reads...)
}

func (c *controlledRangeObjectStorage) Read(
	ctx context.Context,
	key string,
	min *int64,
	max *int64,
) (io.ReadCloser, error) {
	if c.readCount.Add(1) > 1 {
		c.followerOnce.Do(func() { close(c.followerStarted) })
		select {
		case <-ctx.Done():
			return nil, ctx.Err()
		case <-c.releaseFollower:
		}
	}
	start := int64(0)
	if min != nil {
		start = *min
	}
	end := int64(len(c.data))
	if max != nil {
		end = *max
	}
	if start < 0 || end < start || end > int64(len(c.data)) {
		return nil, errors.New("expected a valid bounded range read")
	}
	return io.NopCloser(bytes.NewReader(c.data[start:end])), nil
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
	// Exercise representative cache reads without repeatedly rereading the
	// whole object. The old 1000-point ramp performed about 4 GiB of reads for
	// an 8 MiB fixture while checking the same disk-cache hit invariant.
	readSizes := []int{
		1,
		4 << 10,
		_ReadCoalesceSize,
		1 << 20,
		len(data) / 2,
		len(data) - 1,
		len(data),
	}
	lastHit := int64(0)
	for _, size := range readSizes {
		vec := &IOVector{
			FilePath: "foo/bar",
			Entries: []IOEntry{
				{
					Size: int64(size),
				},
			},
		}
		err = fs.Read(ctx, vec)
		assert.Nil(t, err)
		assert.Equal(t, data[:size], vec.Entries[0].Data)
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

// The full-object default lets later reads reuse one disk-cache artifact.
// Without a disk cache, the default must not expand a bounded entry.
func TestS3FSReadPolicyPreservesFullObjectDefault(t *testing.T) {
	for _, tc := range []struct {
		name         string
		policy       Policy
		disk, memory bool
		noCache      bool
		min          int64
		max          *int64
	}{
		{name: "default full object", disk: true, min: 0},
		{name: "explicit range", disk: true, policy: SkipFullFilePreloads, min: 123, max: ptrTo(int64(130))},
		{name: "zero cache capacities", min: 123, max: ptrTo(int64(130))},
		{name: "memory cache only", memory: true, min: 123, max: ptrTo(int64(130))},
		{name: "cache initialization disabled", disk: true, noCache: true, min: 123, max: ptrTo(int64(130))},
		{name: "disk writes disabled", disk: true, policy: SkipDiskCacheWrites, min: 123, max: ptrTo(int64(130))},
		{name: "disk reads disabled", disk: true, policy: SkipDiskCacheReads, min: 123, max: ptrTo(int64(130))},
	} {
		t.Run(tc.name, func(t *testing.T) {
			ctx := context.Background()
			config := CacheConfig{
				MemoryCapacity: ptrTo[toml.ByteSize](0),
				DiskCapacity:   ptrTo[toml.ByteSize](0),
				DiskPath:       ptrTo(t.TempDir()),
			}
			if tc.disk {
				config.DiskCapacity = ptrTo[toml.ByteSize](1 << 20)
			}
			if tc.memory {
				config.MemoryCapacity = ptrTo[toml.ByteSize](1 << 20)
			}
			fs, err := NewS3FS(
				ctx,
				ObjectStorageArguments{
					Name: "s3", Endpoint: "disk", Bucket: t.TempDir(),
				},
				config,
				nil, tc.noCache, false,
			)
			require.NoError(t, err)
			t.Cleanup(func() { fs.Close(ctx) })

			data := bytes.Repeat([]byte("abcd"), 64)
			require.NoError(t, fs.Write(ctx, IOVector{
				FilePath: "foo/bar",
				Entries:  []IOEntry{{Size: int64(len(data)), Data: data}},
				Policy:   SkipDiskCache | SkipMemoryCache,
			}))

			recorder := &readRangeRecordingObjectStorage{ObjectStorage: fs.storage}
			fs.storage = recorder
			vec := &IOVector{
				FilePath: "foo/bar",
				Policy:   tc.policy,
				Entries:  []IOEntry{{Offset: 123, Size: 7}},
			}
			t.Cleanup(vec.Release)
			require.NoError(t, fs.Read(ctx, vec))
			require.Equal(t, data[123:130], vec.Entries[0].Data)
			require.Equal(t, tc.policy, vec.Policy, "range selection must not mutate caller policy")

			read, ok := recorder.lastRead()
			require.True(t, ok)
			require.NotNil(t, read.min)
			require.Equal(t, tc.min, *read.min)
			if tc.max == nil {
				require.Nil(t, read.max)
			} else {
				require.Equal(t, *tc.max, *read.max)
			}
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
	doneUpdate := fs.diskCache.startUpdate(updatingPath)
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
	t.Cleanup(func() { fs.Close(ctx) })

	data := make([]byte, 1025)
	data[0] = 1
	data[len(data)-1] = 2
	storage := &controlledRangeObjectStorage{
		data:            data,
		followerStarted: make(chan struct{}),
		releaseFollower: make(chan struct{}),
	}
	releaseFollower := sync.OnceFunc(func() { close(storage.releaseFollower) })
	fs.storage = storage
	fs.rawStorage = storage
	t.Cleanup(releaseFollower)

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
			{Offset: int64(len(data) - 1), Size: 1},
		},
	}
	t.Cleanup(followerVector.Release)
	require.False(t, fs.readMergeKey(leaderVector).CacheFill)
	require.True(t, fs.readMergeKey(followerVector).CacheFill)
	require.NotEqual(t, fs.readMergeKey(leaderVector), fs.readMergeKey(followerVector))

	leaderDone := make(chan error, 1)
	go func() { leaderDone <- fs.Read(ctx, leaderVector) }()
	t.Cleanup(func() {
		releaseWriter()
		select {
		case err := <-leaderDone:
			require.NoError(t, err)
		case <-time.After(5 * time.Second):
			t.Error("non-cache-producing leader did not terminate")
		}
	})
	select {
	case <-writer.started:
	case <-time.After(5 * time.Second):
		t.Fatal("non-cache-producing leader did not reach its writer")
	}
	require.False(t, fs.ioMerger.IsMerging(fs.readMergeKey(leaderVector)))

	followerDone := make(chan error, 1)
	go func() { followerDone <- fs.Read(ctx, followerVector) }()
	select {
	case <-storage.followerStarted:
	case err := <-followerDone:
		require.NoError(t, err)
		t.Fatal("cache-producing follower completed before the controlled storage read was observed")
	case <-time.After(5 * time.Second):
		releaseWriter()
		releaseFollower()
		t.Fatal("cache-producing follower did not start storage read while non-cache-producing leader was blocked")
	}
	releaseFollower()
	select {
	case err := <-followerDone:
		require.NoError(t, err)
	case <-time.After(5 * time.Second):
		releaseWriter()
		t.Fatal("cache-producing follower did not terminate after storage release")
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
	t.Cleanup(func() { fs.Close(ctx) })
	generatedStorage := &generatedRangeObjectStorage{}
	storage := &blockingReadObjectStorage{
		ObjectStorage: generatedStorage,
		readStarted:   make(chan struct{}),
		releaseRead:   make(chan struct{}),
	}
	fs.storage = storage
	vector := &IOVector{
		FilePath: "foo/bar",
		Policy:   SkipFullFilePreloads | SkipDiskCacheWrites,
		Entries:  []IOEntry{{Offset: 0, Size: 1}},
	}
	t.Cleanup(vector.Release)
	finishMerge, waitMerge := fs.ioMerger.Merge(fs.readMergeKey(vector), maxIOWaitDuration)
	require.NotNil(t, finishMerge)
	require.Nil(t, waitMerge)
	releaseMerge := sync.OnceFunc(finishMerge)
	t.Cleanup(releaseMerge)
	releaseRead := sync.OnceFunc(func() { close(storage.releaseRead) })
	t.Cleanup(releaseRead)

	readCtx := WithEventLogger(context.Background())
	waiterLogger := readCtx.Value(EventLoggerKey).(*eventLogger)
	readDone := make(chan error, 1)
	readExited := make(chan struct{})
	go func() {
		defer close(readExited)
		readDone <- fs.Read(readCtx, vector)
	}()
	t.Cleanup(func() {
		releaseRead()
		releaseMerge()
		select {
		case <-readExited:
		case <-time.After(5 * time.Second):
			t.Error("range follower goroutine did not terminate")
		}
	})
	require.Eventually(t, func() bool {
		waiterLogger.mu.Lock()
		defer waiterLogger.mu.Unlock()
		for _, ev := range *waiterLogger.events {
			if ev.ev == str_ioMerger_Merge_wait {
				return true
			}
		}
		return false
	}, 5*time.Second, time.Millisecond,
		"range follower did not join the held merge generation")
	select {
	case <-storage.readStarted:
		require.True(t, fs.ioMerger.IsMerging(fs.readMergeKey(vector)),
			"the timed-out follower must bypass, rather than replace, the held generation")
	case err := <-readDone:
		require.NoError(t, err)
		t.Fatal("range follower completed before its fallback storage read was observed")
	case <-time.After(5 * time.Second):
		releaseMerge()
		releaseRead()
		t.Fatal("range follower did not bypass the timed-out merge generation")
	}
	releaseRead()
	select {
	case err := <-readDone:
		require.NoError(t, err)
	case <-time.After(5 * time.Second):
		releaseMerge()
		t.Fatal("range follower did not terminate after its fallback storage read was released")
	}
	reads := generatedStorage.readRanges()
	require.Len(t, reads, 1)
	require.Equal(t, int64(0), *reads[0].min)
	require.Equal(t, int64(1), *reads[0].max)
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

func TestS3FSSingleEntryRangeMergeKeyValidation(t *testing.T) {
	fs := &S3FS{memCache: &MemCache{}}
	cacheableEntry := func(offset int64) IOEntry {
		return IOEntry{
			Offset:      offset,
			Size:        7,
			ToCacheData: CacheOriginalData,
		}
	}

	t.Run("skips completed entries", func(t *testing.T) {
		completed := cacheableEntry(10)
		completed.done = true
		vector := &IOVector{
			FilePath: "foo/bar",
			Entries:  []IOEntry{completed, cacheableEntry(123)},
		}

		key, ok := fs.singleEntryRangeMergeKey(vector)
		require.True(t, ok)
		require.Equal(t, IOMergeKey{
			Path:      "foo/bar",
			Offset:    123,
			End:       130,
			CacheFill: true,
		}, key)
	})

	t.Run("rejects an already completed vector", func(t *testing.T) {
		completed := cacheableEntry(10)
		completed.done = true

		_, ok := fs.singleEntryRangeMergeKey(&IOVector{
			FilePath: "foo/bar",
			Entries:  []IOEntry{completed},
		})
		require.False(t, ok)
	})

	t.Run("rejects multiple unfinished entries", func(t *testing.T) {
		_, ok := fs.singleEntryRangeMergeKey(&IOVector{
			FilePath: "foo/bar",
			Entries: []IOEntry{
				cacheableEntry(10),
				cacheableEntry(20),
			},
		})
		require.False(t, ok)
	})
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
	// Flush waits for the async finalizer to publish the cache file.  Keep a
	// generous bounded deadline here: the finalizer is deliberately blocked
	// above, and a loaded CI worker may need more than one scheduler quantum
	// after it is released.  The test still fails fast on a genuinely stuck
	// finalizer while avoiding a false timeout caused by CI contention.
	flushCtx, cancel := context.WithTimeout(ctx, diskCacheLifecycleTestTimeout)
	defer cancel()
	fs.FlushCache(flushCtx)
	require.NoError(t, flushCtx.Err())
	require.Eventually(t, func() bool {
		return !fs.diskCache.isUpdating(diskPath)
	}, diskCacheLifecycleTestTimeout, time.Millisecond,
		"async range finalizer did not release its update reservation")
	require.FileExists(t, diskPath)
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

// streamFill runs readFullObjectToDiskCacheStreaming of foo/bar bytes 1-3 in the
// background with an async disk cache, reading the object from getReader.
func streamFill(
	t *testing.T,
	ctx context.Context,
	getReader func(context.Context, *int64, *int64) (io.ReadCloser, error),
) (*DiskCache, *IOVector, chan error) {
	cache, err := NewDiskCache(context.Background(), t.TempDir(), fscache.ConstCapacity(1<<20), nil, false, nil, "")
	require.NoError(t, err)
	t.Cleanup(func() { cache.Close(context.Background()) })
	s3 := &S3FS{diskCache: cache, asyncUpdate: true}
	vector := &IOVector{FilePath: "foo/bar", Entries: []IOEntry{{Offset: 1, Size: 3}}}
	done := make(chan error, 1)
	go func() {
		ok, err := s3.readFullObjectToDiskCacheStreaming(ctx, vector, "foo/bar", getReader)
		if err == nil && !ok {
			err = errors.New("not done")
		}
		done <- err
	}()
	return cache, vector, done
}

func waitResult(t *testing.T, done chan error) error {
	select {
	case err := <-done:
		return err
	case <-time.After(diskCacheLifecycleTestTimeout):
		t.Fatal("read did not return")
		return nil
	}
}

// A reader whose ranges are captured returns while the rest of the object still
// downloads, and the download then caches the whole object.
func TestS3FSReadFullObjectToDiskCacheStreamingReturnsOnceCaptured(t *testing.T) {
	ctx := context.Background()
	pr, pw := io.Pipe()
	release := make(chan struct{})
	go func() {
		_, _ = pw.Write([]byte("hello"))
		<-release
		_, _ = pw.Write([]byte(" world"))
		_ = pw.Close()
	}()
	cache, vector, done := streamFill(t, ctx, func(context.Context, *int64, *int64) (io.ReadCloser, error) {
		return pr, nil
	})

	require.NoError(t, waitResult(t, done))
	require.Equal(t, []byte("ell"), vector.Entries[0].Data)
	require.True(t, cache.isUpdating(cache.pathForFile("foo/bar")))

	close(release)
	flushCtx, cancel := context.WithTimeout(ctx, diskCacheLifecycleTestTimeout)
	defer cancel()
	cache.Flush(flushCtx)
	require.True(t, cache.cache.Contains(cache.pathForFile("foo/bar")))
}

// A download that fails after the ranges were captured does not fail the read, and
// caches nothing.
func TestS3FSReadFullObjectToDiskCacheStreamingErrorAfterCapture(t *testing.T) {
	ctx := context.Background()
	cache, vector, done := streamFill(t, ctx, func(context.Context, *int64, *int64) (io.ReadCloser, error) {
		return &errorReadCloser{reader: bytes.NewReader([]byte("hello world")), err: errors.New("reader failed")}, nil
	})

	require.NoError(t, waitResult(t, done))
	require.Equal(t, []byte("ell"), vector.Entries[0].Data)
	flushCtx, cancel := context.WithTimeout(ctx, diskCacheLifecycleTestTimeout)
	defer cancel()
	cache.Flush(flushCtx)
	require.False(t, cache.cache.Contains(cache.pathForFile("foo/bar")))
}

// Cancelling the read before its ranges are captured cancels the download.
func TestS3FSReadFullObjectToDiskCacheStreamingCancelBeforeCapture(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	pr, pw := io.Pipe()
	go func() { _, _ = pw.Write([]byte("h")) }()
	cache, _, done := streamFill(t, ctx, func(fillCtx context.Context, _ *int64, _ *int64) (io.ReadCloser, error) {
		go func() {
			<-fillCtx.Done()
			_ = pw.CloseWithError(fillCtx.Err())
		}()
		return pr, nil
	})

	cancel()
	require.ErrorIs(t, waitResult(t, done), context.Canceled)
	require.False(t, cache.cache.Contains(cache.pathForFile("foo/bar")))
}

// With every background slot taken, a reader waits for its download as before.
func TestS3FSReadFullObjectToDiskCacheStreamingWaitsWhenBackgroundFull(t *testing.T) {
	require.Eventually(t, func() bool { return len(backgroundFills) == 0 },
		diskCacheLifecycleTestTimeout, time.Millisecond, "fills of earlier tests released their slots")
	for i := 0; i < cap(backgroundFills); i++ {
		backgroundFills <- struct{}{}
	}
	t.Cleanup(func() {
		for i := 0; i < cap(backgroundFills); i++ {
			<-backgroundFills
		}
	})
	ctx := context.Background()
	pr, pw := io.Pipe()
	release := make(chan struct{})
	go func() {
		_, _ = pw.Write([]byte("hello"))
		<-release
		_, _ = pw.Write([]byte(" world"))
		_ = pw.Close()
	}()
	_, vector, done := streamFill(t, ctx, func(context.Context, *int64, *int64) (io.ReadCloser, error) {
		return pr, nil
	})

	select {
	case err := <-done:
		t.Fatalf("read returned before its download ended: %v", err)
	case <-time.After(50 * time.Millisecond):
	}
	close(release)
	require.NoError(t, waitResult(t, done))
	require.Equal(t, []byte("ell"), vector.Entries[0].Data)
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
		memoryCacheBacking: fs.memCache.cache,
		updateStarted:      make(chan struct{}),
		releaseUpdate:      make(chan struct{}),
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

func TestNewS3FSCacheInitializationFailureRollsBackMemoryCache(t *testing.T) {
	ctx := context.Background()
	cachePath := t.TempDir() + "/not-a-directory"
	require.NoError(t, os.WriteFile(cachePath, nil, 0o644))
	name := t.Name()

	fs, err := NewS3FS(
		ctx,
		ObjectStorageArguments{
			Name:     name,
			Endpoint: "disk",
			Bucket:   t.TempDir(),
		},
		CacheConfig{
			MemoryCapacity: ptrTo(toml.ByteSize(1 << 20)),
			DiskCapacity:   ptrTo(toml.ByteSize(1 << 20)),
			DiskPath:       &cachePath,
		},
		nil,
		false,
		false,
	)
	require.Nil(t, fs)
	require.Error(t, err)

	registered := false
	allMemoryCaches.Range(func(_, value any) bool {
		if value.(memoryCacheRegistration).name == name {
			registered = true
			return false
		}
		return true
	})
	require.False(t, registered)
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

	benchmarkFileServiceAllocateCacheData(b, fs.AllocateCacheData, 42, 1)
}

func BenchmarkS3FSAllocateCacheDataHighCardinality(b *testing.B) {
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

	benchmarkFileServiceAllocateCacheData(b, fs.AllocateCacheData, 1, 1024)
}

func TestS3FSFromSpecs(t *testing.T) {

	for _, args := range objectStorageArgumentsForTest("test", t) {

		t.Run(args.Name, func(t *testing.T) {
			csPrefix := args.Name == "cs-s3"
			fixturePrefix := args.KeyPrefix
			if csPrefix {
				prepareCITestStorage(t, args)
			}

			testFileServiceWithContext(t, 0, func(ctx context.Context, name string) FileService {
				args.Name = name
				if csPrefix {
					args.KeyPrefix = path.Join(fixturePrefix, time.Now().Format("2006-01-02.15:04:05.000000"))
				} else {
					args.KeyPrefix = time.Now().Format("2006-01-02.15:04:05.000000")
				}

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

func TestS3FSReadWrapperOwnership(t *testing.T) {
	t.Run("stream-retry", func(t *testing.T) {
		var opens int
		var closes [2]int
		closeErr := errors.New("provider close failed")
		source := &readTestObjectStorage{open: func(ctx context.Context, key string, min, max *int64, expected *ObjectIdentity) (io.ReadCloser, error) {
			require.Equal(t, true, ctx.Value(httpTraceKey{}))
			require.Equal(t, "key", key)
			require.Nil(t, expected)
			return newRetryableReader(func(offset int64) (io.ReadCloser, error) {
				i := opens
				opens++
				require.Equal(t, int64(0), offset)
				if i == 0 {
					return &readCloser{r: readerFunc(func([]byte) (int, error) { return 0, io.ErrUnexpectedEOF }), closeFunc: func() error { closes[0]++; return nil }}, nil
				}
				return &readCloser{r: strings.NewReader("abc"), closeFunc: func() error { closes[1]++; return closeErr }}, nil
			}, 0, func(err error) bool { return errors.Is(err, io.ErrUnexpectedEOF) })
		}}
		sem := newObjectStorageSemaphore(source, 1)
		metrics := newObjectStorageMetrics(sem, t.Name())
		fs := &S3FS{name: "test", storage: newObjectStorageHTTPTrace(metrics), ioMerger: NewIOMerger()}
		activeBefore := testutil.ToFloat64(metrics.numActiveRead)
		var reader io.ReadCloser
		vector := IOVector{FilePath: "test:key", Policy: SkipAllCache | SkipFullFilePreloads, Entries: []IOEntry{{Size: 3, ReadCloserForRead: &reader}}}
		t.Cleanup(vector.Release)
		require.NoError(t, fs.Read(t.Context(), &vector))
		require.NotNil(t, reader)
		t.Cleanup(func() { _ = reader.Close() })
		data, err := io.ReadAll(reader)
		require.NoError(t, err)
		require.Equal(t, "abc", string(data))
		require.Equal(t, 2, opens)
		require.Empty(t, sem.semaphore)
		require.Equal(t, activeBefore+1, testutil.ToFloat64(metrics.numActiveRead))
		require.ErrorIs(t, reader.Close(), closeErr)
		require.ErrorIs(t, reader.Close(), closeErr)
		require.Equal(t, [2]int{1, 1}, closes)
		require.Equal(t, activeBefore, testutil.ToFloat64(metrics.numActiveRead))
	})
	t.Run("conditional-lease", func(t *testing.T) {
		var closes int
		closeErr := error(ErrObjectChanged)
		source := &readTestObjectStorage{open: func(ctx context.Context, key string, min, max *int64, expected *ObjectIdentity) (io.ReadCloser, error) {
			require.Equal(t, true, ctx.Value(httpTraceKey{}))
			require.Equal(t, "key", key)
			require.Equal(t, int64(1), *min)
			require.Equal(t, int64(4), *max)
			require.Equal(t, &ObjectIdentity{ETag: "version1", Size: 8}, expected)
			return &readCloser{r: strings.NewReader("abc"), closeFunc: func() error { closes++; return closeErr }}, nil
		}}
		sem := newObjectStorageSemaphore(source, 1)
		metrics := newObjectStorageMetrics(sem, t.Name())
		fs := &S3FS{name: "test", storage: newObjectStorageHTTPTrace(metrics)}
		activeBefore := testutil.ToFloat64(metrics.numActiveRead)
		reader := NewLeasedRangeReader(fs).(ConditionalLeasedRangeReader)
		identity, err := reader.StatIdentity(t.Context(), "test:key")
		require.NoError(t, err)
		admission := new(testRangeAdmission)
		lease, err := reader.ReadRangeLeaseWithIdentity(t.Context(), "test:key", 1, 3, identity, admission)
		require.Nil(t, lease)
		require.ErrorIs(t, err, ErrObjectChanged)
		require.Zero(t, admission.committed.Load())
		require.Equal(t, int64(3), admission.aborted.Load())
		require.Empty(t, sem.semaphore)
		require.Equal(t, activeBefore, testutil.ToFloat64(metrics.numActiveRead))
		require.Equal(t, 1, closes)
		closeErr = nil
		lease, err = reader.ReadRangeLeaseWithIdentity(t.Context(), "test:key", 1, 3, identity, admission)
		require.NoError(t, err)
		t.Cleanup(lease.Release)
		require.Equal(t, "abc", string(lease.Bytes()))
		require.Equal(t, int64(3), admission.committed.Load())
		require.Equal(t, 2, closes)
		lease.Release()
		require.Equal(t, int64(3), admission.released.Load())
		require.Empty(t, sem.semaphore)
		require.Equal(t, activeBefore, testutil.ToFloat64(metrics.numActiveRead))
	})
}

// With every background slot taken, cancelling a reader that waits for its download
// cancels the download and returns.
func TestS3FSReadFullObjectToDiskCacheStreamingCancelWhileWaiting(t *testing.T) {
	require.Eventually(t, func() bool { return len(backgroundFills) == 0 },
		diskCacheLifecycleTestTimeout, time.Millisecond, "fills of earlier tests released their slots")
	for i := 0; i < cap(backgroundFills); i++ {
		backgroundFills <- struct{}{}
	}
	t.Cleanup(func() {
		for i := 0; i < cap(backgroundFills); i++ {
			<-backgroundFills
		}
	})
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	pr, pw := io.Pipe()
	go func() { _, _ = pw.Write([]byte("hello")) }()
	cache, _, done := streamFill(t, ctx, func(fillCtx context.Context, _ *int64, _ *int64) (io.ReadCloser, error) {
		go func() {
			<-fillCtx.Done()
			_ = pw.CloseWithError(fillCtx.Err())
		}()
		return pr, nil
	})

	select {
	case err := <-done:
		t.Fatalf("read returned before its download ended: %v", err)
	case <-time.After(50 * time.Millisecond):
	}
	cancel()
	require.Error(t, waitResult(t, done))
	require.False(t, cache.cache.Contains(cache.pathForFile("foo/bar")))
}

// With every background slot taken, a download that fails after the reader captured its
// bytes does not fail the read.
func TestS3FSReadFullObjectToDiskCacheStreamingErrorAfterCaptureWhenBackgroundFull(t *testing.T) {
	require.Eventually(t, func() bool { return len(backgroundFills) == 0 },
		diskCacheLifecycleTestTimeout, time.Millisecond, "fills of earlier tests released their slots")
	for i := 0; i < cap(backgroundFills); i++ {
		backgroundFills <- struct{}{}
	}
	t.Cleanup(func() {
		for i := 0; i < cap(backgroundFills); i++ {
			<-backgroundFills
		}
	})
	pr, pw := io.Pipe()
	go func() { _, _ = pw.Write([]byte("hello")) }()
	_, vector, done := streamFill(t, context.Background(), func(context.Context, *int64, *int64) (io.ReadCloser, error) {
		return pr, nil
	})

	select {
	case err := <-done:
		t.Fatalf("read returned before its download ended: %v", err)
	case <-time.After(50 * time.Millisecond):
	}
	require.NoError(t, pw.CloseWithError(errors.New("source failed")))
	require.NoError(t, waitResult(t, done))
	require.Equal(t, []byte("ell"), vector.Entries[0].Data)
}

// A download continuing after its reader returned keeps the reader's deadline and ends
// when the disk cache closes.
func TestS3FSReadFullObjectToDiskCacheStreamingBackgroundEndsOnClose(t *testing.T) {
	require.Eventually(t, func() bool { return len(backgroundFills) == 0 },
		diskCacheLifecycleTestTimeout, time.Millisecond, "fills of earlier tests released their slots")
	ctx, cancel := context.WithTimeout(context.Background(), time.Hour)
	defer cancel()
	deadline, _ := ctx.Deadline()
	pr, pw := io.Pipe()
	go func() { _, _ = pw.Write([]byte("hello")) }()
	fillDeadline := make(chan time.Time, 1)
	fillDone := make(chan struct{})
	cache, vector, done := streamFill(t, ctx, func(fillCtx context.Context, _ *int64, _ *int64) (io.ReadCloser, error) {
		d, _ := fillCtx.Deadline()
		fillDeadline <- d
		go func() {
			<-fillCtx.Done()
			close(fillDone)
			_ = pw.CloseWithError(fillCtx.Err())
		}()
		return pr, nil
	})

	require.NoError(t, waitResult(t, done))
	require.Equal(t, []byte("ell"), vector.Entries[0].Data)
	require.Equal(t, deadline, <-fillDeadline)
	cache.Close(ctx)
	select {
	case <-fillDone:
	case <-time.After(diskCacheLifecycleTestTimeout):
		t.Fatal("the download outlived the disk cache")
	}
}

func TestS3FSOrdinarySparseRangeRead(t *testing.T) {
	for _, tc := range []struct {
		name    string
		offsets []int64
		reads   int
	}{
		{"sparse", []int64{0, 16 << 20}, 2},
		{"adjacent", []int64{0, 4096}, 1},
		{"exact span threshold", []int64{0, minExpensiveRangeReadSpan - 4096}, 1},
		{"above span threshold", []int64{0, minExpensiveRangeReadSpan - 4095}, 2},
		{"out of order", []int64{16 << 20, 0}, 1},
		{"four sparse", []int64{0, 16 << 20, 32 << 20, 48 << 20}, 4},
		{"five sparse", []int64{0, 16 << 20, 32 << 20, 48 << 20, 64 << 20}, 1},
	} {
		t.Run(tc.name, func(t *testing.T) {
			ctx := t.Context()
			fs, err := NewS3FS(ctx, ObjectStorageArguments{Name: "sparse", Endpoint: "disk", Bucket: t.TempDir()}, CacheConfig{}, nil, true, false)
			require.NoError(t, err)
			t.Cleanup(func() { fs.Close(context.Background()) })
			storage := &generatedRangeObjectStorage{}
			fs.storage = storage
			vector := &IOVector{FilePath: "foo", Policy: SkipAllCache}
			for _, offset := range tc.offsets {
				vector.Entries = append(vector.Entries, IOEntry{Offset: offset, Size: 4096})
			}
			t.Cleanup(vector.Release)
			require.NoError(t, fs.Read(ctx, vector))
			reads := storage.readRanges()
			require.Len(t, reads, tc.reads)
			if tc.reads > 1 {
				for i, read := range reads {
					require.Equal(t, tc.offsets[i], *read.min)
					require.Equal(t, tc.offsets[i]+4096, *read.max)
				}
			}
			for _, entry := range vector.Entries {
				require.Equal(t, make([]byte, 4096), entry.Data)
			}
		})
	}
}

var errTestHDFSSeek = errors.New("test HDFS seek beyond EOF")

// hdfsSeekReader models the bounds check in colinmarc/hdfs FileReader.Seek.
type hdfsSeekReader struct {
	size, offset int64
}

func (r *hdfsSeekReader) Seek(offset int64, whence int) (int64, error) {
	if whence != io.SeekStart || offset < 0 || offset > r.size {
		return r.offset, errTestHDFSSeek
	}
	r.offset = offset
	return offset, nil
}

func (r *hdfsSeekReader) Read(p []byte) (int, error) {
	if r.offset == r.size {
		return 0, io.EOF
	}
	n := int(min(int64(len(p)), r.size-r.offset))
	clear(p[:n])
	r.offset += int64(n)
	return n, nil
}

type hdfsSeekStorage struct {
	dummyObjectStorage
	size   int64
	ranges [][2]int64
	closes int
}

func (s *hdfsSeekStorage) Read(_ context.Context, key string, min, max *int64) (io.ReadCloser, error) {
	s.ranges = append(s.ranges, [2]int64{*min, *max})
	reader, err := readObjectRange(&hdfsSeekReader{size: s.size}, key, min, max)
	if err != nil {
		s.closes++ // HDFS.Read closes its opened file on an error.
		return nil, err
	}
	return &readCloser{r: reader, closeFunc: func() error { s.closes++; return nil }}, nil
}

func TestS3FSOrdinarySparseHDFSSeekBounds(t *testing.T) {
	const far = int64(16 << 20)
	for _, tc := range []struct {
		name       string
		hdfs       bool
		size       int64
		offsets    [2]int64
		wantRanges [][2]int64
		wantErr    error
		short      bool
	}{
		{"later beyond EOF", true, 8192, [2]int64{0, far}, [][2]int64{{0, far + 4096}}, nil, true},
		{"first beyond EOF", true, 8192, [2]int64{far, 2 * far}, [][2]int64{{far, 2*far + 4096}}, errTestHDFSSeek, false},
		{"first at EOF", true, 8192, [2]int64{8192, far}, [][2]int64{{8192, far + 4096}}, nil, true},
		{"success", true, far + 4096, [2]int64{0, far}, [][2]int64{{0, far + 4096}}, nil, false},
		{"non-HDFS control", false, 8192, [2]int64{0, far}, [][2]int64{{0, 4096}, {far, far + 4096}}, errTestHDFSSeek, false},
	} {
		t.Run(tc.name, func(t *testing.T) {
			ctx := t.Context()
			fs, err := NewS3FS(ctx, ObjectStorageArguments{Name: "sparse", Endpoint: "disk", Bucket: t.TempDir()}, CacheConfig{}, nil, true, false)
			require.NoError(t, err)
			t.Cleanup(func() { fs.Close(context.Background()) })
			storage := &hdfsSeekStorage{size: tc.size}
			fs.storage = storage
			if tc.hdfs {
				fs.rawStorage = &HDFS{}
			} else {
				fs.rawStorage = storage
			}
			cache := &publicationVectorCache{}
			vector := &IOVector{FilePath: "foo", Policy: SkipAllCache, Caches: []IOVectorCache{cache}, Entries: []IOEntry{
				{Offset: tc.offsets[0], Size: 4096}, {Offset: tc.offsets[1], Size: 4096},
			}}
			t.Cleanup(vector.Release)
			err = fs.Read(ctx, vector)
			switch {
			case tc.short:
				require.True(t, moerr.IsMoErrCode(err, moerr.ErrUnexpectedEOF), "%v", err)
			case tc.wantErr != nil:
				require.ErrorIs(t, err, tc.wantErr)
			default:
				require.NoError(t, err)
				for _, entry := range vector.Entries {
					require.Equal(t, make([]byte, 4096), entry.Data)
				}
			}
			require.Equal(t, tc.wantRanges, storage.ranges)
			require.Equal(t, len(tc.wantRanges), storage.closes)
			if err != nil {
				require.Zero(t, cache.updates.Load())
			} else {
				require.EqualValues(t, 1, cache.updates.Load())
			}
		})
	}
	t.Run("partial cache with two misses", func(t *testing.T) {
		ctx := t.Context()
		fs, err := NewS3FS(ctx, ObjectStorageArguments{Name: "sparse", Endpoint: "disk", Bucket: t.TempDir()}, CacheConfig{}, nil, true, false)
		require.NoError(t, err)
		t.Cleanup(func() { fs.Close(context.Background()) })
		storage := &hdfsSeekStorage{size: 8192}
		fs.storage, fs.rawStorage = storage, &HDFS{}
		hit := &sparseFirstEntryCache{}
		miss := &publicationVectorCache{}
		vector := &IOVector{FilePath: "foo", Policy: SkipAllCache, Caches: []IOVectorCache{hit, miss}, Entries: []IOEntry{
			{Offset: 0, Size: 4096}, {Offset: 4096, Size: 4096}, {Offset: far, Size: 4096},
		}}
		t.Cleanup(vector.Release)
		err = fs.Read(ctx, vector)
		require.True(t, moerr.IsMoErrCode(err, moerr.ErrUnexpectedEOF), "%v", err)
		require.Equal(t, []byte("cached"), vector.Entries[0].Data)
		require.Equal(t, [][2]int64{{4096, far + 4096}}, storage.ranges)
		require.Equal(t, 1, storage.closes)
		require.EqualValues(t, 1, hit.reads.Load())
		require.EqualValues(t, 1, miss.reads.Load())
		require.Zero(t, hit.updates.Load())
		require.Zero(t, miss.updates.Load())
	})
}

type sparseFailureStorage struct {
	dummyObjectStorage
	calls int
	err   error
}

func (s *sparseFailureStorage) Read(_ context.Context, _ string, min, max *int64) (io.ReadCloser, error) {
	s.calls++
	if s.calls == 2 {
		return nil, s.err
	}
	return io.NopCloser(bytes.NewReader(make([]byte, *max-*min))), nil
}

type rangeStatusError struct{}

func (rangeStatusError) Error() string       { return "range beyond object" }
func (rangeStatusError) HTTPStatusCode() int { return 416 }

func TestS3FSSparseRangeFailure(t *testing.T) {
	ctx := t.Context()
	fs, err := NewS3FS(ctx, ObjectStorageArguments{Name: "sparse", Endpoint: "disk", Bucket: t.TempDir()}, CacheConfig{}, nil, true, false)
	require.NoError(t, err)
	t.Cleanup(func() { fs.Close(context.Background()) })
	storage := &sparseFailureStorage{err: rangeStatusError{}}
	fs.storage = storage
	cache := &publicationVectorCache{}
	vector := &IOVector{FilePath: "foo", Policy: SkipAllCache, Caches: []IOVectorCache{cache}, Entries: []IOEntry{
		{Offset: 0, Size: 4096}, {Offset: 16 << 20, Size: 4096},
	}}
	t.Cleanup(vector.Release)
	err = fs.Read(ctx, vector)
	require.True(t, moerr.IsMoErrCode(err, moerr.ErrUnexpectedEOF), "%v", err)
	require.Equal(t, 2, storage.calls)
	require.Zero(t, cache.updates.Load())
}

func TestS3FSSparseFailureDoesNotPopulateMemoryCache(t *testing.T) {
	ctx := t.Context()
	fs, err := NewS3FS(ctx, ObjectStorageArguments{Name: "sparse", Endpoint: "disk", Bucket: t.TempDir()},
		CacheConfig{MemoryCapacity: ptrTo[toml.ByteSize](1 << 20)}, nil, false, false)
	require.NoError(t, err)
	fs.SetAsyncUpdate(false)
	t.Cleanup(func() { fs.Close(context.Background()) })
	storage := &sparseFailureStorage{err: rangeStatusError{}}
	fs.storage = storage
	entry := func(offset int64) IOEntry {
		return IOEntry{Offset: offset, Size: 4096, ToCacheData: CacheOriginalData}
	}
	vector := &IOVector{FilePath: "foo", Entries: []IOEntry{entry(0), entry(16 << 20)}}
	t.Cleanup(vector.Release)
	require.Error(t, fs.Read(ctx, vector))
	cached := &IOVector{FilePath: "foo", Entries: []IOEntry{entry(0)}}
	t.Cleanup(cached.Release)
	require.NoError(t, fs.ReadCache(ctx, cached))
	require.False(t, cached.allDone())
}

func TestS3FSSparseFailureDoesNotPopulateDiskCache(t *testing.T) {
	ctx := t.Context()
	fs, err := NewS3FS(ctx, ObjectStorageArguments{Name: "sparse", Endpoint: "disk", Bucket: t.TempDir()},
		CacheConfig{DiskCapacity: ptrTo[toml.ByteSize](1 << 20), DiskPath: ptrTo(t.TempDir())}, nil, false, false)
	require.NoError(t, err)
	fs.SetAsyncUpdate(false)
	t.Cleanup(func() { fs.Close(context.Background()) })
	fs.diskCache.prefetchRange = prefetchRangeConfig{}
	fs.storage = &sparseFailureStorage{err: rangeStatusError{}}
	entry := func(offset int64) IOEntry { return IOEntry{Offset: offset, Size: 4096, ToCacheData: CacheOriginalData} }
	vector := &IOVector{FilePath: "foo", Policy: SkipFullFilePreloads, Entries: []IOEntry{entry(0), entry(16 << 20)}}
	t.Cleanup(vector.Release)
	require.Error(t, fs.Read(ctx, vector))
	cached := &IOVector{FilePath: "foo", Policy: SkipFullFilePreloads, Entries: []IOEntry{entry(0)}}
	t.Cleanup(cached.Release)
	require.NoError(t, readCache(ctx, fs.diskCache, cached))
	require.False(t, cached.allDone())
}

func TestS3FSSparsePreservesConverterError(t *testing.T) {
	ctx := t.Context()
	fs, err := NewS3FS(ctx, ObjectStorageArguments{Name: "sparse", Endpoint: "disk", Bucket: t.TempDir()}, CacheConfig{}, nil, true, false)
	require.NoError(t, err)
	t.Cleanup(func() { fs.Close(context.Background()) })
	fs.storage = &generatedRangeObjectStorage{}
	vector := &IOVector{FilePath: "foo", Policy: SkipAllCache, Entries: []IOEntry{
		{Offset: 0, Size: 4096},
		{Offset: 16 << 20, Size: 4096, ToCacheData: func(context.Context, io.Reader, []byte, CacheDataAllocator) (fscache.Data, error) {
			return nil, rangeStatusError{}
		}},
	}}
	t.Cleanup(vector.Release)
	err = fs.Read(ctx, vector)
	status, ok := objectStorageStatusCode(err)
	require.True(t, ok, "%v", err)
	require.Equal(t, 416, status)
}

type sparsePatternReader struct{ value byte }

func (r sparsePatternReader) Read(p []byte) (int, error) {
	for i := range p {
		p[i] = r.value
	}
	return len(p), nil
}

type sparseTrackingStorage struct {
	dummyObjectStorage
	// mu guards the counters, which a read's prefetch object may update after the read.
	mu                  sync.Mutex
	opens, closes       int
	offsets             []int64
	setFileEvents       int
	bytes               int64
	firstErr, secondErr error
	shortSecond         bool
	bodyErrSecond       bool
	cancel              context.CancelFunc
}

func (s *sparseTrackingStorage) Read(ctx context.Context, _ string, min, max *int64) (io.ReadCloser, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.opens == 0 {
		s.setFileEvents = countLoggedEvents(ctx, str_disk_cache_setfile_begin)
	}
	s.opens++
	s.offsets = append(s.offsets, *min)
	if s.opens == 1 && s.firstErr != nil {
		return nil, s.firstErr
	}
	if s.opens == 2 && s.secondErr != nil {
		return nil, s.secondErr
	}
	n := *max - *min
	if s.opens == 2 && s.shortSecond {
		n--
	}
	s.bytes += n
	var body io.Reader = io.LimitReader(sparsePatternReader{value: byte(*min/(16<<20) + 1)}, n)
	if s.opens == 2 && s.bodyErrSecond {
		body = readerFunc(func([]byte) (int, error) { return 0, rangeStatusError{} })
	}
	return &readCloser{r: body, closeFunc: func() error {
		s.mu.Lock()
		defer s.mu.Unlock()
		s.closes++
		if s.cancel != nil {
			s.cancel()
			s.cancel = nil
		}
		return nil
	}}, nil
}

// counts returns the opens, closes and opened offsets so far.
func (s *sparseTrackingStorage) counts() (opens, closes int, offsets []int64) {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.opens, s.closes, slices.Clone(s.offsets)
}

type sparseReleaseCountingData struct {
	fscache.Data
	releases *int
}

func (d *sparseReleaseCountingData) Release() { *d.releases++; d.Data.Release() }

func TestS3FSSparseReadLifecycle(t *testing.T) {
	for _, tc := range []struct {
		name                string
		firstErr, secondErr error
		short, cancel       bool
		opens, closes       int
	}{
		{name: "first 416", firstErr: rangeStatusError{}, opens: 1},
		{name: "second 416", secondErr: rangeStatusError{}, opens: 2, closes: 1},
		{name: "short body", short: true, opens: 2, closes: 2},
		{name: "cancel between opens", cancel: true, opens: 1, closes: 1},
	} {
		t.Run(tc.name, func(t *testing.T) {
			ctx, cancel := context.WithCancel(t.Context())
			defer cancel()
			fs, err := NewS3FS(ctx, ObjectStorageArguments{Name: "sparse", Endpoint: "disk", Bucket: t.TempDir()}, CacheConfig{}, nil, true, false)
			require.NoError(t, err)
			t.Cleanup(func() { fs.Close(context.Background()) })
			storage := &sparseTrackingStorage{firstErr: tc.firstErr, secondErr: tc.secondErr, shortSecond: tc.short}
			if tc.cancel {
				storage.cancel = cancel
			}
			fs.storage = storage
			releases := 0
			convert := func(ctx context.Context, r io.Reader, data []byte, allocator CacheDataAllocator) (fscache.Data, error) {
				d, err := CacheOriginalData(ctx, r, data, allocator)
				if err != nil {
					return nil, err
				}
				return &sparseReleaseCountingData{Data: d, releases: &releases}, nil
			}
			vector := &IOVector{FilePath: "foo", Policy: SkipAllCache, Entries: []IOEntry{
				{Offset: 0, Size: 4096, ToCacheData: convert}, {Offset: 16 << 20, Size: 4096},
			}}
			err = fs.Read(ctx, vector)
			require.Error(t, err)
			if tc.cancel {
				require.ErrorIs(t, err, context.Canceled)
			} else if tc.firstErr != nil {
				status, ok := objectStorageStatusCode(err)
				require.True(t, ok)
				require.Equal(t, 416, status)
			} else {
				require.True(t, moerr.IsMoErrCode(err, moerr.ErrUnexpectedEOF), "%v", err)
			}
			require.Equal(t, tc.opens, storage.opens)
			require.Equal(t, tc.closes, storage.closes)
			vector.ReleaseReadResultOnError()
			if tc.firstErr == nil {
				require.Equal(t, 1, releases)
			} else {
				require.Zero(t, releases)
			}
		})
	}
}

type sparseFirstEntryCache struct{ publicationVectorCache }

func (c *sparseFirstEntryCache) Read(_ context.Context, vector *IOVector) error {
	c.reads.Add(1)
	vector.Entries[0].Data = []byte("cached")
	vector.Entries[0].done = true
	return nil
}

func TestS3FSSparsePartialCacheAndStreaming(t *testing.T) {
	ctx := t.Context()
	fs, err := NewS3FS(ctx, ObjectStorageArguments{Name: "sparse", Endpoint: "disk", Bucket: t.TempDir()}, CacheConfig{}, nil, true, false)
	require.NoError(t, err)
	t.Cleanup(func() { fs.Close(context.Background()) })
	t.Run("two misses in a larger vector", func(t *testing.T) {
		storage := &sparseTrackingStorage{}
		fs.storage = storage
		cache := &callbackVectorCache{onRead: func(vector *IOVector) {
			for pos := range vector.Entries[:4] {
				vector.Entries[pos].Data = []byte("cached")
				vector.Entries[pos].done = true
			}
		}}
		// Cached entries include a streaming destination and a descending offset.
		// The decision uses only the two buffered misses.
		var cachedOutput bytes.Buffer
		vector := &IOVector{FilePath: "foo", Policy: SkipAllCache, Caches: []IOVectorCache{cache}, Entries: []IOEntry{
			{Offset: 64 << 20, Size: 4096},
			{Offset: 0, Size: 4096, WriterForRead: &cachedOutput},
			{Offset: 4096, Size: 4096},
			{Offset: 8192, Size: 4096},
			{Offset: 16 << 20, Size: 4096},
			{Offset: 32 << 20, Size: 4096},
		}}
		t.Cleanup(vector.Release)
		require.NoError(t, fs.Read(ctx, vector))
		for _, entry := range vector.Entries[:4] {
			require.Equal(t, []byte("cached"), entry.Data)
		}
		require.Equal(t, bytes.Repeat([]byte{2}, 4096), vector.Entries[4].Data)
		require.Equal(t, bytes.Repeat([]byte{3}, 4096), vector.Entries[5].Data)
		require.Equal(t, 2, storage.opens)
		require.Equal(t, 2, storage.closes)
		require.Equal(t, int64(8192), storage.bytes)
	})
	t.Run("one remaining miss", func(t *testing.T) {
		storage := &sparseTrackingStorage{}
		fs.storage = storage
		vector := &IOVector{FilePath: "foo", Policy: SkipAllCache, Caches: []IOVectorCache{&sparseFirstEntryCache{}}, Entries: []IOEntry{
			{Offset: 0, Size: 4096}, {Offset: 16 << 20, Size: 4096},
		}}
		t.Cleanup(vector.Release)
		require.NoError(t, fs.Read(ctx, vector))
		require.Equal(t, 1, storage.opens)
		require.Equal(t, bytes.Repeat([]byte{2}, 4096), vector.Entries[1].Data)
		require.Equal(t, int64(4096), storage.bytes)
	})
	t.Run("success updates original vector", func(t *testing.T) {
		storage := &sparseTrackingStorage{}
		fs.storage = storage
		cache := &publicationVectorCache{}
		buf := make([]byte, 4096)
		vector := &IOVector{FilePath: "foo", Policy: SkipAllCache, Caches: []IOVectorCache{cache}, Entries: []IOEntry{
			{Offset: 0, Size: 4096, Data: buf}, {Offset: 16 << 20, Size: 4096},
		}}
		t.Cleanup(vector.Release)
		require.NoError(t, fs.Read(ctx, vector))
		require.Same(t, vector, cache.last)
		require.Equal(t, int64(1), cache.updates.Load())
		require.Same(t, &buf[0], &vector.Entries[0].Data[0])
	})
	t.Run("mixed read closer", func(t *testing.T) {
		storage := &sparseTrackingStorage{}
		fs.storage = storage
		var reader io.ReadCloser
		vector := &IOVector{FilePath: "foo", Policy: SkipAllCache, Entries: []IOEntry{
			{Offset: 0, Size: 4096}, {Offset: 16 << 20, Size: 4096, ReadCloserForRead: &reader},
		}}
		t.Cleanup(vector.Release)
		require.NoError(t, fs.Read(ctx, vector))
		require.Equal(t, 1, storage.opens)
		data, err := io.ReadAll(reader)
		require.NoError(t, err)
		require.NoError(t, reader.Close())
		require.Len(t, data, 4096)
		require.Equal(t, 1, storage.closes)
	})
	t.Run("mixed writer", func(t *testing.T) {
		storage := &sparseTrackingStorage{}
		fs.storage = storage
		var output bytes.Buffer
		vector := &IOVector{FilePath: "foo", Policy: SkipAllCache, Entries: []IOEntry{
			{Offset: 0, Size: 4096}, {Offset: 16 << 20, Size: 4096, WriterForRead: &output},
		}}
		t.Cleanup(vector.Release)
		require.NoError(t, fs.Read(ctx, vector))
		require.Equal(t, 1, storage.opens)
		require.Equal(t, 1, storage.closes)
		require.Equal(t, 4096, output.Len())
	})
}

type sparseFailingReadCache struct{ publicationVectorCache }

func (*sparseFailingReadCache) Read(context.Context, *IOVector) error {
	return errors.New("cache read failed")
}

func TestS3FSFailedCacheReadDoesNotUpdateEarlierCache(t *testing.T) {
	ctx := t.Context()
	fs, err := NewS3FS(ctx, ObjectStorageArguments{Name: "sparse", Endpoint: "disk", Bucket: t.TempDir()}, CacheConfig{}, nil, true, false)
	require.NoError(t, err)
	t.Cleanup(func() { fs.Close(context.Background()) })
	first := &publicationVectorCache{}
	vector := &IOVector{FilePath: "foo", Policy: SkipAllCache, Caches: []IOVectorCache{first, &sparseFailingReadCache{}}, Entries: []IOEntry{{Offset: 0, Size: 4096}}}
	require.ErrorContains(t, fs.Read(ctx, vector), "cache read failed")
	require.Zero(t, first.updates.Load())
}

type sparseFailingUpdateCache struct{ publicationVectorCache }

func (c *sparseFailingUpdateCache) Update(ctx context.Context, vector *IOVector, async bool) error {
	_ = c.publicationVectorCache.Update(ctx, vector, async)
	return errors.New("cache update failed")
}

func TestS3FSCacheUpdateErrorDoesNotAffectReadOrOtherUpdates(t *testing.T) {
	ctx := t.Context()
	fs, err := NewS3FS(ctx, ObjectStorageArguments{Name: "sparse", Endpoint: "disk", Bucket: t.TempDir()},
		CacheConfig{MemoryCapacity: ptrTo[toml.ByteSize](1 << 20)}, nil, false, false)
	require.NoError(t, err)
	fs.SetAsyncUpdate(false)
	t.Cleanup(func() { fs.Close(context.Background()) })
	fs.storage = &generatedRangeObjectStorage{}
	first := &publicationVectorCache{}
	failing := &sparseFailingUpdateCache{}
	vector := &IOVector{FilePath: "foo", Policy: SkipFullFilePreloads, Caches: []IOVectorCache{first, failing}, Entries: []IOEntry{
		{Offset: 0, Size: 4096, ToCacheData: CacheOriginalData},
	}}
	t.Cleanup(vector.Release)
	require.NoError(t, fs.Read(ctx, vector))
	require.Equal(t, make([]byte, 4096), vector.Entries[0].Data)
	require.Equal(t, int64(1), failing.updates.Load())
	require.Equal(t, int64(1), first.updates.Load())

	cached := &IOVector{FilePath: "foo", Entries: []IOEntry{{Offset: 0, Size: 4096, ToCacheData: CacheOriginalData}}}
	t.Cleanup(cached.Release)
	require.NoError(t, fs.ReadCache(ctx, cached))
	require.True(t, cached.allDone())
}

func TestS3FSFullCacheHitUpdatesOnlyEarlierMisses(t *testing.T) {
	ctx := t.Context()
	fs, err := NewS3FS(ctx, ObjectStorageArguments{Name: "sparse", Endpoint: "disk", Bucket: t.TempDir()}, CacheConfig{}, nil, true, false)
	require.NoError(t, err)
	t.Cleanup(func() { fs.Close(context.Background()) })
	first := &publicationVectorCache{}
	hit := &sparseFirstEntryCache{}
	last := &publicationVectorCache{}
	vector := &IOVector{FilePath: "foo", Caches: []IOVectorCache{first, hit, last}, Entries: []IOEntry{{Offset: 0, Size: 6}}}
	t.Cleanup(vector.Release)
	require.NoError(t, fs.Read(ctx, vector))
	require.Equal(t, []byte("cached"), vector.Entries[0].Data)
	require.Equal(t, int64(1), first.updates.Load())
	require.Zero(t, hit.updates.Load())
	require.Zero(t, last.reads.Load())
	require.Zero(t, last.updates.Load())
}

// callbackVectorCache exposes cache read and update boundaries to lifecycle tests.
type callbackVectorCache struct {
	publicationVectorCache
	onRead   func(*IOVector)
	onUpdate func()
}

func (c *callbackVectorCache) Read(_ context.Context, v *IOVector) error {
	if c.onRead != nil {
		c.onRead(v)
	}
	return nil
}

func (c *callbackVectorCache) Update(_ context.Context, _ *IOVector, _ bool) error {
	if c.onUpdate != nil {
		c.onUpdate()
	}
	return nil
}

func TestS3FSCacheReadUnwindDoesNotPublish(t *testing.T) {
	for _, cause := range []string{"panic", "goexit"} {
		t.Run(cause, func(t *testing.T) {
			ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
			defer cancel()
			fs, err := NewS3FS(ctx, ObjectStorageArguments{Name: "unwind", Endpoint: "disk", Bucket: t.TempDir()}, CacheConfig{}, nil, true, false)
			require.NoError(t, err)
			t.Cleanup(func() { fs.Close(context.Background()) })
			first := &publicationVectorCache{}
			entered := make(chan struct{})
			second := &callbackVectorCache{onRead: func(v *IOVector) {
				close(entered)
				v.Entries[0].Data = []byte("partial")
				v.Entries[0].done = true
				if cause == "panic" {
					panic("cache read panic")
				}
				runtime.Goexit()
			}}
			vector := &IOVector{FilePath: "foo", Caches: []IOVectorCache{first, second}, Entries: []IOEntry{{Size: 7}, {Offset: 7, Size: 1}}}
			result := make(chan any, 1)
			finished := make(chan struct{})
			returned := make(chan error, 1)
			go func() {
				defer close(finished)
				defer func() { result <- recover() }()
				returned <- fs.Read(ctx, vector)
			}()
			t.Cleanup(func() { cancel(); <-finished; vector.Release() })
			select {
			case got := <-result:
				if cause == "panic" {
					require.Equal(t, "cache read panic", got)
				} else {
					require.Nil(t, got)
				}
			case <-ctx.Done():
				t.Fatal("read did not unwind")
			}
			<-finished
			select {
			case <-entered:
			default:
				t.Fatal("target cache callback was not reached")
			}
			select {
			case err := <-returned:
				t.Fatalf("Read returned normally: %v", err)
			default:
			}
			require.Zero(t, first.updates.Load())
		})
	}
}

func TestS3FSCacheUpdateUnwindRunsRemainingUpdates(t *testing.T) {
	for _, cause := range []string{"panic", "goexit"} {
		t.Run(cause, func(t *testing.T) {
			ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
			defer cancel()
			fs, err := NewS3FS(ctx, ObjectStorageArguments{Name: "unwind", Endpoint: "disk", Bucket: t.TempDir()}, CacheConfig{}, nil, false, false)
			require.NoError(t, err)
			t.Cleanup(func() { fs.Close(context.Background()) })
			fs.storage = &generatedRangeObjectStorage{}
			calls := make(chan string, 6)
			entered := make(chan struct{})
			var key IOMergeKey
			mk := func(name string) *callbackVectorCache {
				return &callbackVectorCache{onUpdate: func() {
					if !fs.ioMerger.IsMerging(key) {
						calls <- "merge-already-released"
					}
					calls <- name
					if name == "C" {
						close(entered)
						if cause == "panic" {
							panic("cache update panic")
						}
						runtime.Goexit()
					}
				}}
			}
			vector := &IOVector{FilePath: "foo", Policy: SkipFullFilePreloads, Caches: []IOVectorCache{mk("A"), mk("B"), mk("C")}, Entries: []IOEntry{{Size: 4096}}}
			key = fs.readMergeKey(vector)
			result := make(chan any, 1)
			finished := make(chan struct{})
			returned := make(chan error, 1)
			go func() {
				defer close(finished)
				defer func() { result <- recover() }()
				returned <- fs.Read(ctx, vector)
			}()
			t.Cleanup(func() { cancel(); <-finished; vector.Release() })
			select {
			case got := <-result:
				if cause == "panic" {
					require.Equal(t, "cache update panic", got)
				} else {
					require.Nil(t, got)
				}
			case <-ctx.Done():
				t.Fatal("read did not unwind")
			}
			<-finished
			select {
			case <-entered:
			default:
				t.Fatal("target cache callback was not reached")
			}
			select {
			case err := <-returned:
				t.Fatalf("Read returned normally: %v", err)
			default:
			}
			close(calls)
			var got []string
			for call := range calls {
				got = append(got, call)
			}
			require.Equal(t, []string{"C", "B", "A"}, got)
			require.False(t, fs.ioMerger.IsMerging(key))
		})
	}
}

func TestS3FSCacheUpdatesPreserveRetryOrder(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	var eventsMu sync.Mutex
	var events []string
	record := func(event string) { eventsMu.Lock(); defer eventsMu.Unlock(); events = append(events, event) }
	ctx = OnDiskCacheWritten(ctx, func(string, IOEntry) { record("disk") })
	ctx = WithEventLogger(ctx)
	logger := ctx.Value(EventLoggerKey).(*eventLogger)
	fs, err := NewS3FS(ctx, ObjectStorageArguments{Name: "retry", Endpoint: "disk", Bucket: t.TempDir()}, CacheConfig{MemoryCapacity: ptrTo[toml.ByteSize](1 << 20), DiskCapacity: ptrTo[toml.ByteSize](1 << 20), DiskPath: ptrTo(t.TempDir()), CacheCallbacks: CacheCallbacks{PostSet: []CacheCallbackFunc{func(fscache.CacheKey, fscache.Data) { record("memory") }}}}, nil, false, false)
	require.NoError(t, err)
	t.Cleanup(func() { fs.Close(context.Background()) })
	fs.SetAsyncUpdate(false)
	fs.diskCache.prefetchRange = prefetchRangeConfig{}
	fs.storage = &generatedRangeObjectStorage{}
	order := make(chan []string, 1)
	first := &callbackVectorCache{onUpdate: func() {
		record("vector1")
		logger.mu.Lock()
		defer logger.mu.Unlock()
		var got []string
		for _, ev := range *logger.events {
			switch ev.ev {
			case str_update_disk_cache_Caches_begin:
				got = append(got, "disk")
			case str_update_memory_cache_begin:
				got = append(got, "memory")
			case str_update_vector_Caches_begin:
				got = append(got, "vector")
			}
		}
		order <- got
	}}
	vector := &IOVector{FilePath: "foo", Policy: SkipFullFilePreloads, Caches: []IOVectorCache{first, &callbackVectorCache{onUpdate: func() { record("vector2") }}}, Entries: []IOEntry{{Size: 4096, ToCacheData: CacheOriginalData}}}
	t.Cleanup(vector.Release)
	done, _ := fs.ioMerger.merge(fs.readMergeKey(vector))
	require.NotNil(t, done)
	release := sync.OnceFunc(done)
	result := make(chan error, 1)
	finished := make(chan struct{})
	go func() { defer close(finished); result <- fs.Read(ctx, vector) }()
	t.Cleanup(func() { release(); cancel(); <-finished })
	for {
		logger.mu.Lock()
		found := false
		if logger.events != nil {
			for _, ev := range *logger.events {
				if ev.ev == str_ioMerger_Merge_wait {
					found = true
					break
				}
			}
		}
		logger.mu.Unlock()
		if found {
			break
		}
		select {
		case <-ctx.Done():
			t.Fatal("read did not join seeded generation")
		default:
			runtime.Gosched()
		}
	}
	release()
	select {
	case err := <-result:
		require.NoError(t, err)
	case <-ctx.Done():
		t.Fatal("read did not finish")
	}
	<-finished
	eventsMu.Lock()
	gotEvents := append([]string(nil), events...)
	eventsMu.Unlock()
	require.Equal(t, []string{"disk", "memory", "vector2", "vector1"}, gotEvents)
	for _, cache := range []IOVectorCache{fs.memCache, fs.diskCache} {
		cached := &IOVector{FilePath: "foo", Entries: []IOEntry{{Size: 4096, ToCacheData: CacheOriginalData}}}
		t.Cleanup(cached.Release)
		require.NoError(t, cache.Read(ctx, cached))
		require.True(t, cached.allDone())
		require.Equal(t, make([]byte, 4096), cached.Entries[0].CachedData.Bytes())
	}
	select {
	case got := <-order:
		require.Equal(t, []string{"disk", "memory", "disk", "memory", "vector", "vector"}, got)
	case <-ctx.Done():
		t.Fatal("missing cache update")
	}
}

// Exercise both real disk-cache fallback paths while another fill owns the file.
func TestS3FSSparseDiskFallbackErrors(t *testing.T) {
	for _, mode := range []string{"download declined", "fill admission skipped"} {
		for _, tc := range []struct {
			name                    string
			first, second           int64
			firstErr, secondErr     error
			short, cancel, hit      bool
			stream, writer, convert bool
			sameOffset, bodyError   bool
			unexpected              bool
			opens, closes           int
			offsets                 []int64
		}{
			{name: "first 416", first: 9 << 20, second: 25 << 20, firstErr: rangeStatusError{}, opens: 1},
			{name: "later 416", first: 9 << 20, second: 25 << 20, secondErr: rangeStatusError{}, unexpected: true, opens: 2, closes: 1},
			{name: "lower 416", first: 25 << 20, second: 9 << 20, secondErr: rangeStatusError{}, opens: 2, closes: 1},
			{name: "cached first 416", first: 9 << 20, second: 25 << 20, firstErr: rangeStatusError{}, hit: true, opens: 1},
			{name: "cached later 416", first: 9 << 20, second: 25 << 20, secondErr: rangeStatusError{}, hit: true, unexpected: true, opens: 2, closes: 1},
			{name: "same offset 416", first: 9 << 20, second: 9 << 20, secondErr: rangeStatusError{}, sameOffset: true, opens: 2, closes: 1},
			{name: "short", first: 9 << 20, second: 25 << 20, short: true, unexpected: true, opens: 2, closes: 2},
			{name: "body 416", first: 9 << 20, second: 25 << 20, bodyError: true, opens: 2, closes: 2},
			{name: "cancel", first: 9 << 20, second: 25 << 20, cancel: true, opens: 1, closes: 1},
			{name: "converter 416", first: 9 << 20, second: 25 << 20, convert: true, opens: 2, closes: 2},
			{name: "stream first", first: 9 << 20, second: 25 << 20, stream: true, secondErr: rangeStatusError{}, opens: 2, closes: 1},
			{name: "writer first", first: 9 << 20, second: 25 << 20, writer: true, secondErr: rangeStatusError{}, opens: 2, closes: 1},
		} {
			// Streaming requests do not enter the full-file streaming fill path.
			if mode == "fill admission skipped" && (tc.stream || tc.writer) {
				continue
			}
			t.Run(mode+"/"+tc.name, func(t *testing.T) {
				ctx, cancel := context.WithCancel(WithEventLogger(t.Context()))
				defer cancel()
				rangeReadsCost(t, time.Nanosecond)
				var disk *DiskCache
				if mode == "download declined" {
					disk, _, _ = downloading(t)
					disk.downloadMaxExtraWait = 0
				} else {
					var err error
					disk, err = NewDiskCache(ctx, t.TempDir(), fscache.ConstCapacity(1<<20), nil, false, nil, "")
					require.NoError(t, err)
					t.Cleanup(func() { disk.Close(context.Background()) })
					path := disk.pathForFile("obj")
					require.True(t, disk.reserveAsyncFileFinalize(ctx, path, nil))
					t.Cleanup(func() { disk.releaseAsyncFileFinalizeReservation(path) })
				}
				fs, err := NewS3FS(ctx, ObjectStorageArguments{Name: "sparse", Endpoint: "disk", Bucket: t.TempDir()}, CacheConfig{}, nil, true, false)
				require.NoError(t, err)
				disk.prefetchRange, disk.memoryPrefetchMax = prefetchRangeConfig{}, 0
				fs.diskCache = disk
				fs.SetAsyncUpdate(true)
				t.Cleanup(func() { fs.diskCache = nil; fs.Close(context.Background()) })
				storage := &sparseTrackingStorage{firstErr: tc.firstErr, secondErr: tc.secondErr, shortSecond: tc.short, bodyErrSecond: tc.bodyError}
				if tc.cancel {
					storage.cancel = cancel
				}
				fs.storage = storage
				cache := &publicationVectorCache{}
				hitCache := &sparseFirstEntryCache{}
				first := IOEntry{Offset: tc.first, Size: 4096}
				releases := 0
				if !tc.stream && !tc.writer {
					first.ToCacheData = func(ctx context.Context, r io.Reader, data []byte, allocator CacheDataAllocator) (fscache.Data, error) {
						d, err := CacheOriginalData(ctx, r, data, allocator)
						if err != nil {
							return nil, err
						}
						return &sparseReleaseCountingData{Data: d, releases: &releases}, nil
					}
				}
				var stream io.ReadCloser
				t.Cleanup(func() {
					if stream != nil {
						_ = stream.Close()
					}
				})
				if tc.stream {
					first.ReadCloserForRead = &stream
				}
				var output bytes.Buffer
				if tc.writer {
					first.WriterForRead = &output
				}
				second := IOEntry{Offset: tc.second, Size: 4096}
				if tc.convert {
					second.ToCacheData = func(context.Context, io.Reader, []byte, CacheDataAllocator) (fscache.Data, error) {
						return nil, rangeStatusError{}
					}
				}
				vector := &IOVector{FilePath: "obj", Caches: []IOVectorCache{cache}, Entries: []IOEntry{first, second}}
				if tc.hit {
					vector.Caches = []IOVectorCache{hitCache, cache}
					vector.Entries = append([]IOEntry{{Offset: 0, Size: 4096}}, vector.Entries...)
				}
				if tc.sameOffset {
					vector.Entries = append(vector.Entries, IOEntry{Offset: 25 << 20, Size: 4096})
				}
				t.Cleanup(vector.ReleaseReadResultOnError)
				err = fs.Read(ctx, vector)
				if stream != nil {
					closeErr := stream.Close()
					stream = nil
					require.NoError(t, closeErr)
				}
				// A read past disk-prefetch-memory-max reads its ranges through prefetch
				// objects of its own, which end after the read returns.
				wantOpens, wantCloses := tc.opens, tc.closes
				if tc.cancel {
					// The first range's object closes its reader, canceling the read, before
					// or after the second range is opened; every opened reader is closed.
					require.Eventually(t, func() bool {
						opens, closes, _ := storage.counts()
						return opens >= 1 && opens == closes
					}, 10*time.Second, time.Millisecond)
					wantOpens, _, _ = storage.counts()
					wantCloses = wantOpens
					if err == nil {
						require.Equal(t, 2, wantOpens, "a read that got all its ranges opened both")
					}
				}
				require.Eventually(t, func() bool {
					opens, closes, _ := storage.counts()
					return opens == wantOpens && closes == wantCloses
				}, 10*time.Second, time.Millisecond)
				opens, closes, offsets := storage.counts()
				unexpected := tc.unexpected
				if tc.sameOffset && moerr.IsMoErrCode(err, moerr.ErrUnexpectedEOF) {
					// The second entry shared the first's prefetch object, so the next read
					// of storage is the 416 past it.
					unexpected, tc.offsets = true, []int64{9 << 20, 25 << 20}
				}
				switch {
				case tc.cancel:
					if err != nil {
						require.ErrorIs(t, err, context.Canceled)
					}
				case unexpected:
					require.True(t, moerr.IsMoErrCode(err, moerr.ErrUnexpectedEOF), "%v", err)
				default:
					status, ok := objectStorageStatusCode(err)
					require.True(t, ok, "%v", err)
					require.Equal(t, 416, status)
				}
				require.Equal(t, mode == "download declined", vector.downloadDeclined)
				require.Equal(t, wantOpens, opens)
				require.Equal(t, wantCloses, closes)
				wantOffsets := []int64{tc.first}
				if wantOpens == 2 {
					wantOffsets = append(wantOffsets, tc.second)
				}
				if tc.offsets != nil {
					wantOffsets = tc.offsets
				}
				require.Equal(t, wantOffsets, offsets)
				if mode == "fill admission skipped" {
					require.Equal(t, 1, storage.setFileEvents)
				}
				if err == nil {
					// A read that got all its ranges updates its caller's cache.
					require.EqualValues(t, 1, cache.updates.Load())
				} else {
					require.Zero(t, cache.updates.Load())
				}
				require.Zero(t, hitCache.updates.Load())
				vector.ReleaseReadResultOnError()
				if tc.firstErr == nil && !tc.stream && !tc.writer {
					require.Equal(t, 1, releases)
				} else {
					require.Zero(t, releases)
				}
			})
		}
	}
}
