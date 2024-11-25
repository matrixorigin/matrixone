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
	"os"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/matrixorigin/matrixone/pkg/common/moerr"
	"github.com/matrixorigin/matrixone/pkg/logutil"
	"github.com/matrixorigin/matrixone/pkg/perfcounter"
	"github.com/matrixorigin/matrixone/pkg/util/toml"
	"github.com/stretchr/testify/assert"
	"go.uber.org/zap"
)

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
	ctx := context.Background()
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
	ctx := context.Background()
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
				Bucket:    b.TempDir(),
				KeyPrefix: time.Now().Format("2006-01-02.15:04:05.000000"),
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
	assert.Equal(t, int64(0), pcSet.FileService.Cache.Disk.WriteFile.Load())

	// preload
	err = fs.PrefetchFile(ctx, "foo/bar")
	assert.Nil(t, err)
	assert.Equal(t, int64(1), pcSet.FileService.Cache.Disk.WriteFile.Load())
	err = fs.PrefetchFile(ctx, "foo/bar")
	assert.Nil(t, err)
	assert.Equal(t, int64(1), pcSet.FileService.Cache.Disk.WriteFile.Load())

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
			DiskPath:       ptrTo(t.TempDir()),
			DiskCapacity:   ptrTo(toml.ByteSize(10 * (1 << 20))),
			CheckOverlaps:  false,
		},
		nil,
		false,
		false,
	)
	assert.Nil(t, err)
	defer fs.Close(ctx)

	var counterSet perfcounter.CounterSet
	ctx = perfcounter.WithCounterSet(context.Background(), &counterSet)

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
	assert.Nil(t, err)

	nThreads := 256
	wg := new(sync.WaitGroup)
	wg.Add(nThreads)
	for range nThreads {
		go func() {
			defer wg.Done()
			vec := &IOVector{
				FilePath: "foo",
				Entries: []IOEntry{
					{
						Size:        3,
						ToCacheData: CacheOriginalData,
					},
				},
			}
			err := fs.Read(ctx, vec)
			assert.Nil(t, err)
			assert.Equal(t, []byte("foo"), vec.Entries[0].CachedData.Bytes())
			vec.Release()
		}()
	}
	wg.Wait()

	assert.Equal(t, int64(1), counterSet.FileService.S3.Put.Load())
	assert.True(t, counterSet.FileService.S3.Get.Load() < int64(nThreads))

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

			testFileService(t, 0, func(name string) FileService {
				args.Name = name
				args.KeyPrefix = time.Now().Format("2006-01-02.15:04:05.000000")

				fs, err := NewS3FS(
					context.Background(),
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
