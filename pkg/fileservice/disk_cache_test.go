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
	"crypto/rand"
	"fmt"
	"io"
	"io/fs"
	mrand "math/rand"
	"path/filepath"
	"strconv"
	"testing"

	"github.com/matrixorigin/matrixone/pkg/perfcounter"
	"github.com/stretchr/testify/assert"
)

func TestDiskCache(t *testing.T) {
	dir := t.TempDir()
	ctx := context.Background()

	// counter
	numWritten := 0
	ctx = OnDiskCacheWritten(ctx, func(path string, entry IOEntry) {
		numWritten++
	})

	// new
	cache, err := NewDiskCache(ctx, dir, 1<<20, nil)
	assert.Nil(t, err)

	// update
	testUpdate := func(cache *DiskCache) {
		vec := &IOVector{
			FilePath: "foo",
			Entries: []IOEntry{
				{
					Offset: 0,
					Size:   1,
					Data:   []byte("a"),
				},
				// no data
				{
					Offset: 98,
					Size:   0,
				},
				// size unknown
				{
					Offset: 9999,
					Size:   -1,
					Data:   []byte("abc"),
				},
			},
		}
		err = cache.Update(ctx, vec, false)
		assert.Nil(t, err)
	}
	testUpdate(cache)

	assert.Equal(t, 1, numWritten)

	// update again
	testUpdate(cache)

	assert.Equal(t, 1, numWritten)

	// read
	testRead := func(cache *DiskCache) {
		buf := new(bytes.Buffer)
		var r io.ReadCloser
		vec := &IOVector{
			FilePath: "foo",
			Entries: []IOEntry{
				// written data
				{
					Offset:            0,
					Size:              1,
					WriterForRead:     buf,
					ReadCloserForRead: &r,
				},
				// not exists
				{
					Offset: 1,
					Size:   1,
				},
				// bad offset
				{
					Offset: 999,
					Size:   1,
				},
			},
		}
		err = cache.Read(ctx, vec)
		assert.Nil(t, err)
		assert.NotNil(t, r)
		defer r.Close()
		assert.True(t, vec.Entries[0].done)
		assert.Equal(t, []byte("a"), vec.Entries[0].Data)
		assert.Equal(t, []byte("a"), buf.Bytes())
		bs, err := io.ReadAll(r)
		assert.Nil(t, err)
		assert.Equal(t, []byte("a"), bs)
		assert.False(t, vec.Entries[1].done)
		assert.False(t, vec.Entries[2].done)
	}
	testRead(cache)

	// read again
	testRead(cache)

	// new cache instance and read
	cache, err = NewDiskCache(ctx, dir, 1<<20, nil)
	assert.Nil(t, err)
	testRead(cache)

	assert.Equal(t, 1, numWritten)

	// new cache instance and update
	cache, err = NewDiskCache(ctx, dir, 1<<20, nil)
	assert.Nil(t, err)
	testUpdate(cache)

	assert.Equal(t, 1, numWritten)

	// delete file
	err = cache.DeletePaths(ctx, []string{"foo"})
	assert.Nil(t, err)
}

func TestDiskCacheWriteAgain(t *testing.T) {

	dir := t.TempDir()
	ctx := context.Background()
	var counterSet perfcounter.CounterSet
	ctx = perfcounter.WithCounterSet(ctx, &counterSet)

	cache, err := NewDiskCache(ctx, dir, 4096, nil)
	assert.Nil(t, err)

	// update
	err = cache.Update(ctx, &IOVector{
		FilePath: "foo",
		Entries: []IOEntry{
			{
				Size: 3,
				Data: []byte("foo"),
			},
		},
	}, false)
	assert.Nil(t, err)
	assert.Equal(t, int64(1), counterSet.FileService.Cache.Disk.WriteFile.Load())
	assert.Equal(t, int64(0), counterSet.FileService.Cache.Disk.Evict.Load())

	// update another entry
	err = cache.Update(ctx, &IOVector{
		FilePath: "foo",
		Entries: []IOEntry{
			{
				Size:   3,
				Data:   []byte("foo"),
				Offset: 99,
			},
		},
	}, false)
	assert.Nil(t, err)
	assert.Equal(t, int64(2), counterSet.FileService.Cache.Disk.WriteFile.Load())
	assert.Equal(t, int64(1), counterSet.FileService.Cache.Disk.Evict.Load())

	// update again, should write cache file again
	err = cache.Update(ctx, &IOVector{
		FilePath: "foo",
		Entries: []IOEntry{
			{
				Size: 3,
				Data: []byte("foo"),
			},
		},
	}, false)
	assert.Nil(t, err)
	assert.Equal(t, int64(3), counterSet.FileService.Cache.Disk.WriteFile.Load())
	assert.Equal(t, int64(2), counterSet.FileService.Cache.Disk.Evict.Load())

	err = cache.Read(ctx, &IOVector{
		FilePath: "foo",
		Entries: []IOEntry{
			{
				Size: 3,
			},
		},
	})
	assert.Nil(t, err)
	assert.Equal(t, int64(1), counterSet.FileService.Cache.Disk.Hit.Load())

}

func TestDiskCacheFileCache(t *testing.T) {
	dir := t.TempDir()
	ctx := context.Background()
	cache, err := NewDiskCache(ctx, dir, 1<<20, nil)
	assert.Nil(t, err)

	vector := IOVector{
		FilePath: "foo",
		Entries: []IOEntry{
			{
				Offset: 0,
				Size:   3,
				Data:   []byte("foo"),
			},
			{
				Offset: 3,
				Size:   3,
				Data:   []byte("bar"),
			},
		},
	}

	data, err := io.ReadAll(newIOEntriesReader(ctx, vector.Entries))
	assert.Nil(t, err)
	err = cache.SetFile(ctx, vector.FilePath, func(context.Context) (io.ReadCloser, error) {
		return io.NopCloser(bytes.NewReader(data)), nil
	})
	assert.Nil(t, err)

	readVector := &IOVector{
		FilePath: "foo",
		Entries: []IOEntry{
			{
				Offset: 0,
				Size:   2,
			},
			{
				Offset: 2,
				Size:   2,
			},
			{
				Offset: 4,
				Size:   2,
			},
		},
	}
	err = cache.Read(ctx, readVector)
	assert.Nil(t, err)
	assert.Equal(t, []byte("fo"), readVector.Entries[0].Data)
	assert.Equal(t, []byte("ob"), readVector.Entries[1].Data)
	assert.Equal(t, []byte("ar"), readVector.Entries[2].Data)

}

func TestDiskCacheDirSize(t *testing.T) {
	ctx := context.Background()
	var counter perfcounter.CounterSet
	ctx = perfcounter.WithCounterSet(ctx, &counter)

	dir := t.TempDir()
	capacity := 1 << 20
	cache, err := NewDiskCache(ctx, dir, capacity, nil)
	assert.Nil(t, err)

	data := bytes.Repeat([]byte("a"), capacity/128)
	for i := 0; i < capacity/len(data)*64; i++ {
		err := cache.Update(ctx, &IOVector{
			FilePath: fmt.Sprintf("%v", i),
			Entries: []IOEntry{
				{
					Offset: 0,
					Size:   int64(len(data)),
					Data:   data,
				},
			},
		}, false)
		assert.Nil(t, err)
		size := dirSize(dir)
		assert.LessOrEqual(t, size, capacity)
	}
	assert.True(t, counter.FileService.Cache.Disk.Evict.Load() > 0)
}

func dirSize(path string) (ret int) {
	if err := filepath.WalkDir(path, func(path string, d fs.DirEntry, err error) error {
		if err != nil {
			return err
		}
		if d.IsDir() {
			return nil
		}
		stat, err := d.Info()
		if err != nil {
			return err
		}
		ret += int(fileSize(stat))
		return nil
	}); err != nil {
		panic(err)
	}
	return
}

func benchmarkDiskCacheWriteThenRead(
	b *testing.B,
	size int64,
) {
	b.Helper()

	b.SetBytes(size)
	data := bytes.Repeat([]byte("a"), int(size))

	dir := b.TempDir()
	ctx := context.Background()

	cache, err := NewDiskCache(
		ctx,
		dir,
		10<<30,
		nil,
	)
	if err != nil {
		b.Fatal(err)
	}

	b.ResetTimer()

	b.RunParallel(func(pb *testing.PB) {
		buf := make([]byte, 0, size)
		for pb.Next() {

			path := strconv.FormatInt(mrand.Int63(), 10)

			// update
			err := cache.Update(
				ctx,
				&IOVector{
					FilePath: path,
					Entries: []IOEntry{
						{
							Size: size,
							Data: data,
						},
					},
				},
				false,
			)
			if err != nil {
				b.Fatal(err)
			}

			// read
			vec := &IOVector{
				FilePath: path,
				Entries: []IOEntry{
					{
						Size: size,
						Data: buf,
					},
				},
			}
			err = cache.Read(
				ctx,
				vec,
			)
			if err != nil {
				b.Fatal(err)
			}
			if !bytes.Equal(vec.Entries[0].Data, data) {
				b.Fatal()
			}

		}
	})

}

func BenchmarkDiskCacheWriteThenRead(b *testing.B) {
	b.Run("4K", func(b *testing.B) {
		benchmarkDiskCacheWriteThenRead(b, 4096)
	})
	b.Run("1M", func(b *testing.B) {
		benchmarkDiskCacheWriteThenRead(b, 1<<20)
	})
	b.Run("16M", func(b *testing.B) {
		benchmarkDiskCacheWriteThenRead(b, 16<<20)
	})
}

func benchmarkDiskCacheReadRandomOffsetAtLargeFile(
	b *testing.B,
	fileSize int64,
	readSize int64,
) {
	b.Helper()

	b.SetBytes(readSize)
	data := make([]byte, fileSize)
	_, err := rand.Read(data)
	if err != nil {
		b.Fatal(err)
	}

	dir := b.TempDir()
	ctx := context.Background()

	cache, err := NewDiskCache(
		ctx,
		dir,
		8<<30,
		nil,
	)
	if err != nil {
		b.Fatal(err)
	}

	err = cache.SetFile(ctx, "foo", func(ctx context.Context) (io.ReadCloser, error) {
		return io.NopCloser(bytes.NewReader(data)), nil
	})
	if err != nil {
		b.Fatal(err)
	}

	b.ResetTimer()

	b.RunParallel(func(pb *testing.PB) {
		buf := make([]byte, 0, readSize)
		for pb.Next() {

			// read
			offset := mrand.Intn(int(fileSize - readSize))
			vec := &IOVector{
				FilePath: "foo",
				Entries: []IOEntry{
					{
						Offset: int64(offset),
						Size:   readSize,
						Data:   buf,
					},
				},
			}
			err = cache.Read(
				ctx,
				vec,
			)
			if err != nil {
				b.Fatal(err)
			}
			if !bytes.Equal(vec.Entries[0].Data, data[offset:offset+int(readSize)]) {
				b.Fatal()
			}

		}
	})

}

func BenchmarkDiskCacheReadRandomOffsetAtLargeFile(b *testing.B) {
	b.Run("4K", func(b *testing.B) {
		benchmarkDiskCacheReadRandomOffsetAtLargeFile(b, 1<<30, 4096)
	})
	b.Run("1M", func(b *testing.B) {
		benchmarkDiskCacheReadRandomOffsetAtLargeFile(b, 1<<30, 1<<20)
	})
	b.Run("16M", func(b *testing.B) {
		benchmarkDiskCacheReadRandomOffsetAtLargeFile(b, 1<<30, 16<<20)
	})
}
