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
	"fmt"
	"io"
	"sync"
	"testing"
	"time"

	"github.com/matrixorigin/matrixone/pkg/perfcounter"
	"github.com/stretchr/testify/assert"
)

func TestDiskCache(t *testing.T) {
	dir := t.TempDir()
	ctx := context.Background()

	// new
	cache, err := NewDiskCache(dir, 1024, time.Second, 1, nil)
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

	// update again
	testUpdate(cache)

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
	cache, err = NewDiskCache(dir, 1024, time.Second, 1, nil)
	assert.Nil(t, err)
	testRead(cache)

	// new cache instance and update
	cache, err = NewDiskCache(dir, 1024, time.Second, 1, nil)
	assert.Nil(t, err)
	testUpdate(cache)

}

func TestDiskCachePreload(t *testing.T) {
	dir := t.TempDir()
	ctx := context.Background()
	var counterSet perfcounter.CounterSet
	ctx = perfcounter.WithCounterSet(ctx, &counterSet)

	// new
	cache, err := NewDiskCache(dir, 1024, time.Second, 1, nil)
	assert.Nil(t, err)

	// set content
	err = cache.SetFileContent(ctx, "foo", func(_ context.Context, vec *IOVector) error {
		_, err := vec.Entries[0].WriterForRead.Write([]byte("foo"))
		if err != nil {
			return err
		}
		return nil
	})
	assert.Nil(t, err)
	assert.Equal(t, int64(1), counterSet.FileService.Cache.Disk.SetFileContent.Load())
	assert.Equal(t, int64(0), counterSet.FileService.Cache.Disk.OpenFile.Load())
	assert.Equal(t, int64(0), counterSet.FileService.Cache.Disk.StatFile.Load())

	// get content
	r, err := cache.GetFileContent(ctx, "foo", 0)
	assert.Nil(t, err)
	data, err := io.ReadAll(r)
	assert.Nil(t, err)
	assert.Equal(t, []byte("foo"), data)
	assert.Equal(t, int64(1), counterSet.FileService.Cache.Disk.GetFileContent.Load())
	assert.Equal(t, int64(1), counterSet.FileService.Cache.Disk.OpenFile.Load())
	assert.Equal(t, int64(0), counterSet.FileService.Cache.Disk.StatFile.Load())

	r, err = cache.GetFileContent(ctx, "foo", 1)
	assert.Nil(t, err)
	data, err = io.ReadAll(r)
	assert.Nil(t, err)
	assert.Equal(t, []byte("oo"), data)
	assert.Equal(t, int64(2), counterSet.FileService.Cache.Disk.GetFileContent.Load())
	assert.Equal(t, int64(2), counterSet.FileService.Cache.Disk.OpenFile.Load())
	assert.Equal(t, int64(0), counterSet.FileService.Cache.Disk.StatFile.Load())

	// read
	vec := &IOVector{
		FilePath: "foo",
		Entries: []IOEntry{
			{
				Offset: 0,
				Size:   3,
			},
			{
				Offset: 1,
				Size:   2,
			},
			{
				Offset: 2,
				Size:   1,
			},
		},
	}
	assert.Nil(t, cache.Read(ctx, vec))
	assert.Equal(t, []byte("foo"), vec.Entries[0].Data)
	assert.Equal(t, []byte("oo"), vec.Entries[1].Data)
	assert.Equal(t, []byte("o"), vec.Entries[2].Data)
	assert.Equal(t, int64(3), counterSet.FileService.Cache.Disk.OpenFile.Load())
	assert.Equal(t, int64(0), counterSet.FileService.Cache.Disk.StatFile.Load())

	// update
	assert.Nil(t, cache.Update(ctx, vec, false))
	assert.Equal(t, int64(3), counterSet.FileService.Cache.Disk.OpenFile.Load())
	assert.Equal(t, int64(1), counterSet.FileService.Cache.Disk.StatFile.Load())

	// read and update again
	assert.Nil(t, cache.Read(ctx, vec))
	assert.Equal(t, []byte("foo"), vec.Entries[0].Data)
	assert.Equal(t, []byte("oo"), vec.Entries[1].Data)
	assert.Equal(t, []byte("o"), vec.Entries[2].Data)
	assert.Equal(t, int64(4), counterSet.FileService.Cache.Disk.OpenFile.Load())
	assert.Equal(t, int64(1), counterSet.FileService.Cache.Disk.StatFile.Load())
	assert.Nil(t, cache.Update(ctx, vec, false))
	assert.Equal(t, int64(4), counterSet.FileService.Cache.Disk.OpenFile.Load())
	assert.Equal(t, int64(1), counterSet.FileService.Cache.Disk.StatFile.Load())

}

func TestDiskCacheConcurrentSetFileContent(t *testing.T) {
	dir := t.TempDir()
	ctx := context.Background()
	var counterSet perfcounter.CounterSet
	ctx = perfcounter.WithCounterSet(ctx, &counterSet)

	// new
	cache, err := NewDiskCache(dir, 1024, time.Second, 1, nil)
	assert.Nil(t, err)

	n := 128
	wg := new(sync.WaitGroup)
	wg.Add(n)
	for i := 0; i < n; i++ {
		go func() {
			defer wg.Done()
			err := cache.SetFileContent(ctx, "foo", func(_ context.Context, vec *IOVector) error {
				_, err := vec.Entries[0].WriterForRead.Write([]byte("foo"))
				if err != nil {
					return err
				}
				return nil
			})
			assert.Nil(t, err)
		}()
	}
	wg.Wait()

	assert.Equal(t, int64(1), counterSet.FileService.Cache.Disk.SetFileContent.Load())
}

func TestDiskCacheEviction(t *testing.T) {
	dir := t.TempDir()
	ctx := context.Background()
	var counterSet perfcounter.CounterSet
	ctx = perfcounter.WithCounterSet(ctx, &counterSet)

	// new
	cache, err := NewDiskCache(dir, 3, time.Second, 1, nil)
	assert.Nil(t, err)

	n := 128
	wg := new(sync.WaitGroup)
	wg.Add(n)
	for i := 0; i < n; i++ {
		i := i
		go func() {
			defer wg.Done()
			err := cache.SetFileContent(ctx, fmt.Sprintf("%v", i), func(_ context.Context, vec *IOVector) error {
				_, err := vec.Entries[0].WriterForRead.Write([]byte("foo"))
				if err != nil {
					return err
				}
				return nil
			})
			assert.Nil(t, err)
		}()
	}
	wg.Wait()

	assert.Equal(t, int64(128), counterSet.FileService.Cache.Disk.SetFileContent.Load())

	cache.evict(ctx)
	assert.True(t, counterSet.FileService.Cache.Disk.Evict.Load() > 0)
}

func TestImmediatelyEviction(t *testing.T) {
	dir := t.TempDir()
	ctx := context.Background()
	var counterSet perfcounter.CounterSet
	ctx = perfcounter.WithCounterSet(ctx, &counterSet)

	evictInterval := time.Hour * 1
	cache, err := NewDiskCache(dir, 3, evictInterval, 0.8, nil)
	assert.Nil(t, err)

	err = cache.SetFileContent(ctx, "a", func(_ context.Context, vec *IOVector) error {
		_, err := vec.Entries[0].WriterForRead.Write([]byte("foo"))
		if err != nil {
			return err
		}
		return nil
	})
	assert.Nil(t, err)

	err = cache.SetFileContent(ctx, "b", func(_ context.Context, vec *IOVector) error {
		_, err := vec.Entries[0].WriterForRead.Write([]byte("foo"))
		if err != nil {
			return err
		}
		return nil
	})
	assert.Nil(t, err)

	assert.Equal(t, int64(1), counterSet.FileService.Cache.Disk.EvictImmediately.Load())
}
