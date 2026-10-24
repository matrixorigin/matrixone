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
	"errors"
	"fmt"
	"io"
	"io/fs"
	"math/rand"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/matrixorigin/matrixone/pkg/common/moerr"
	"github.com/matrixorigin/matrixone/pkg/fileservice/fscache"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func newTestIndexFileCache(t *testing.T, dir string, capacity int64) *IndexFileCache {
	t.Helper()
	c, err := NewIndexFileCache(context.Background(), dir, fscache.ConstCapacity(capacity), "")
	require.NoError(t, err)
	<-c.disk.load.done
	t.Cleanup(func() { c.Close(context.Background()) })
	return c
}

func indexCacheBytes(name string, content []byte) IndexCacheFile {
	return IndexCacheFile{
		Name: name,
		Open: func(context.Context) (io.ReadCloser, error) {
			return io.NopCloser(bytes.NewReader(content)), nil
		},
	}
}

func indexCacheEntry(data []byte) []IndexCacheFile {
	return []IndexCacheFile{
		indexCacheBytes("data", data),
		indexCacheBytes("manifest", []byte(fmt.Sprintf("len=%d", len(data)))),
	}
}

func readAllFile(t *testing.T, f *os.File) []byte {
	t.Helper()
	_, err := f.Seek(0, io.SeekStart)
	require.NoError(t, err)
	b, err := io.ReadAll(f)
	require.NoError(t, err)
	return b
}

func closeFiles(files []*os.File) {
	for _, f := range files {
		_ = f.Close()
	}
}

func requireIndexCacheMiss(t *testing.T, c *IndexFileCache, name string) {
	t.Helper()
	files, err := c.Open(context.Background(), name, "data", "manifest")
	require.Nil(t, files)
	require.True(t, moerr.IsMoErrCode(err, moerr.ErrFileNotFound), "want miss, got %v", err)
}

// listCacheDirs returns entry, temp and trash directories under root.
func listCacheDirs(t *testing.T, root string) (entries, temps, trash []string) {
	t.Helper()
	require.NoError(t, filepath.WalkDir(root, func(path string, d fs.DirEntry, err error) error {
		if err != nil || !d.IsDir() {
			return err
		}
		switch name := d.Name(); {
		case strings.HasSuffix(name, cacheDirTrashSuffix):
			trash = append(trash, path)
			return fs.SkipDir
		case strings.HasSuffix(name, cacheDirTempSuffix):
			temps = append(temps, path)
			return fs.SkipDir
		case strings.HasSuffix(name, cacheDirSuffix):
			entries = append(entries, path)
			return fs.SkipDir
		}
		return nil
	}))
	return
}

func evictIndexCache(c *IndexFileCache) {
	done := make(chan int64, 1)
	c.disk.cache.Evict(context.Background(), done, 0)
	<-done
}

func TestIndexFileCachePutOpen(t *testing.T) {
	ctx := context.Background()
	c := newTestIndexFileCache(t, t.TempDir(), 1<<30)
	data := bytes.Repeat([]byte("x"), 10000)

	written, err := c.Put(ctx, "1/abcd/0/sum", indexCacheEntry(data))
	require.NoError(t, err)
	require.True(t, written)

	files, err := c.Open(ctx, "1/abcd/0/sum", "data", "manifest")
	require.NoError(t, err)
	defer closeFiles(files)
	require.Equal(t, data, readAllFile(t, files[0]))
	require.Equal(t, []byte("len=10000"), readAllFile(t, files[1]))

	written, err = c.Put(ctx, "1/abcd/0/sum", indexCacheEntry([]byte("other")))
	require.NoError(t, err)
	require.False(t, written, "an existing entry is never rewritten")

	requireIndexCacheMiss(t, c, "1/abcd/0/other")
	_, err = c.Open(ctx, "1/abcd/0/sum", "data", "absent")
	require.True(t, moerr.IsMoErrCode(err, moerr.ErrFileNotFound))
}

func TestIndexFileCacheRejectsInvalidNames(t *testing.T) {
	ctx := context.Background()
	c := newTestIndexFileCache(t, t.TempDir(), 1<<30)
	for _, name := range []string{"", ".", "..", "../x", "/abs", "a/b" + cacheDirSuffix + "/c",
		"a" + cacheDirTempSuffix, "a" + cacheDirTrashSuffix, "a" + cacheFileSuffix} {
		_, err := c.Put(ctx, name, indexCacheEntry([]byte("x")))
		require.Error(t, err, name)
		_, err = c.Open(ctx, name, "data")
		require.Error(t, err, name)
		require.Error(t, c.Delete(ctx, name), name)
	}
	for _, files := range [][]IndexCacheFile{
		nil,
		{indexCacheBytes("", nil)},
		{indexCacheBytes("a/b", nil)},
		{indexCacheBytes("..", nil)},
		{indexCacheBytes("data", nil), indexCacheBytes("data", nil)},
	} {
		_, err := c.Put(ctx, "ok", files)
		require.Error(t, err)
	}
	_, err := c.Open(ctx, "ok", "../data")
	require.Error(t, err)
}

func TestIndexFileCacheSourceErrorLeavesNothing(t *testing.T) {
	ctx := context.Background()
	dir := t.TempDir()
	c := newTestIndexFileCache(t, dir, 1<<30)
	boom := errors.New("boom")

	for _, files := range [][]IndexCacheFile{
		{indexCacheBytes("data", []byte("x")), {Name: "manifest", Open: func(context.Context) (io.ReadCloser, error) {
			return nil, boom
		}}},
		{{Name: "data", Open: func(context.Context) (io.ReadCloser, error) {
			return io.NopCloser(io.MultiReader(bytes.NewReader([]byte("part")), indexCacheErrReader{boom})), nil
		}}},
	} {
		written, err := c.Put(ctx, "e", files)
		require.ErrorIs(t, err, boom)
		require.False(t, written)
		requireIndexCacheMiss(t, c, "e")
	}
	entries, temps, trash := listCacheDirs(t, dir)
	require.Empty(t, entries)
	require.Empty(t, temps)
	require.Empty(t, trash)
}

type indexCacheErrReader struct{ err error }

func (r indexCacheErrReader) Read([]byte) (int, error) { return 0, r.err }

func TestIndexFileCacheCanceledPutLeavesNothing(t *testing.T) {
	dir := t.TempDir()
	c := newTestIndexFileCache(t, dir, 1<<30)
	ctx, cancel := context.WithCancel(context.Background())
	c.disk.beforeFilePublication = cancel
	written, err := c.Put(ctx, "e", indexCacheEntry([]byte("x")))
	require.ErrorIs(t, err, context.Canceled)
	require.False(t, written)
	c.disk.beforeFilePublication = nil
	requireIndexCacheMiss(t, c, "e")
	entries, temps, _ := listCacheDirs(t, dir)
	require.Empty(t, entries)
	require.Empty(t, temps)
}

func TestIndexFileCacheEntryInvisibleBeforePublication(t *testing.T) {
	ctx := context.Background()
	c := newTestIndexFileCache(t, t.TempDir(), 1<<30)
	diskPath, err := c.disk.pathForDir("e")
	require.NoError(t, err)
	var sawMiss bool
	c.disk.beforeFilePublication = func() {
		_, statErr := os.Stat(diskPath)
		sawMiss = os.IsNotExist(statErr)
	}
	written, err := c.Put(ctx, "e", indexCacheEntry([]byte("x")))
	require.NoError(t, err)
	require.True(t, written)
	require.True(t, sawMiss, "the entry must not exist under its name before publication")
}

func TestIndexFileCacheEvictedWhileOpenStaysReadable(t *testing.T) {
	ctx := context.Background()
	dir := t.TempDir()
	data := bytes.Repeat([]byte("a"), 64<<10)
	c := newTestIndexFileCache(t, dir, 200<<10)

	_, err := c.Put(ctx, "first", indexCacheEntry(data))
	require.NoError(t, err)
	files, err := c.Open(ctx, "first", "data")
	require.NoError(t, err)
	defer closeFiles(files)

	for i := 0; i < 8; i++ {
		_, err := c.Put(ctx, fmt.Sprintf("other-%d", i), indexCacheEntry(data))
		require.NoError(t, err)
	}
	evictIndexCache(c)

	requireIndexCacheMiss(t, c, "first")
	require.Equal(t, data, readAllFile(t, files[0]), "an evicted entry stays readable through an open handle")

	entries, _, trash := listCacheDirs(t, dir)
	require.Empty(t, trash)
	var total int64
	for _, e := range entries {
		require.True(t, c.disk.cache.Contains(e), e)
		size, err := cacheDirSize(e)
		require.NoError(t, err)
		total += size
	}
	require.LessOrEqual(t, total, int64(200<<10))
}

func TestIndexFileCacheDelete(t *testing.T) {
	ctx := context.Background()
	dir := t.TempDir()
	c := newTestIndexFileCache(t, dir, 1<<30)
	data := []byte("payload")
	_, err := c.Put(ctx, "e", indexCacheEntry(data))
	require.NoError(t, err)
	files, err := c.Open(ctx, "e", "data")
	require.NoError(t, err)
	defer closeFiles(files)

	require.NoError(t, c.Delete(ctx, "e"))
	requireIndexCacheMiss(t, c, "e")
	require.Equal(t, data, readAllFile(t, files[0]))
	require.NoError(t, c.Delete(ctx, "e"), "deleting a missing entry is not an error")

	written, err := c.Put(ctx, "e", indexCacheEntry([]byte("new")))
	require.NoError(t, err)
	require.True(t, written)
	files2, err := c.Open(ctx, "e", "data")
	require.NoError(t, err)
	defer closeFiles(files2)
	require.Equal(t, []byte("new"), readAllFile(t, files2[0]))

	entries, temps, trash := listCacheDirs(t, dir)
	require.Len(t, entries, 1)
	require.Empty(t, temps)
	require.Empty(t, trash)
}

func TestIndexFileCacheReusedAfterRestart(t *testing.T) {
	ctx := context.Background()
	dir := t.TempDir()
	c, err := NewIndexFileCache(ctx, dir, fscache.ConstCapacity(1<<30), "")
	require.NoError(t, err)
	<-c.disk.load.done
	data := []byte("persisted")
	_, err = c.Put(ctx, "1/ab/0/sum", indexCacheEntry(data))
	require.NoError(t, err)
	diskPath, err := c.disk.pathForDir("1/ab/0/sum")
	require.NoError(t, err)
	c.Close(ctx)

	parent := filepath.Dir(diskPath)
	oldTemp := filepath.Join(parent, "old"+cacheDirTempSuffix)
	newTemp := filepath.Join(parent, "new"+cacheDirTempSuffix)
	leftTrash := filepath.Join(parent, "x"+cacheDirTrashSuffix)
	for _, d := range []string{oldTemp, newTemp, leftTrash} {
		require.NoError(t, os.MkdirAll(d, 0755))
		require.NoError(t, os.WriteFile(filepath.Join(d, "data"), []byte("junk"), 0644))
	}
	old := time.Now().Add(-9 * time.Hour)
	require.NoError(t, os.Chtimes(oldTemp, old, old))

	c2 := newTestIndexFileCache(t, dir, 1<<30)
	require.True(t, c2.disk.cache.Contains(diskPath), "restart load must index entry directories")
	files, err := c2.Open(ctx, "1/ab/0/sum", "data", "manifest")
	require.NoError(t, err)
	defer closeFiles(files)
	require.Equal(t, data, readAllFile(t, files[0]))

	_, err = os.Stat(oldTemp)
	require.True(t, os.IsNotExist(err), "old temp directory is removed")
	_, err = os.Stat(newTemp)
	require.NoError(t, err, "recent temp directory may belong to a writer and is kept")
	_, err = os.Stat(leftTrash)
	require.True(t, os.IsNotExist(err), "trash is removed")
}

func TestIndexFileCacheTwoInstancesShareDirectory(t *testing.T) {
	ctx := context.Background()
	dir := t.TempDir()
	c1 := newTestIndexFileCache(t, dir, 1<<30)
	c2 := newTestIndexFileCache(t, dir, 1<<30)
	data := []byte("shared")

	_, err := c1.Put(ctx, "e", indexCacheEntry(data))
	require.NoError(t, err)
	files, err := c2.Open(ctx, "e", "data")
	require.NoError(t, err)
	require.Equal(t, data, readAllFile(t, files[0]))
	closeFiles(files)
	diskPath, err := c2.disk.pathForDir("e")
	require.NoError(t, err)
	require.True(t, c2.disk.cache.Contains(diskPath), "a hit indexes the entry in the reading instance")

	written, err := c2.Put(ctx, "e", indexCacheEntry([]byte("other")))
	require.NoError(t, err)
	require.False(t, written)

	require.NoError(t, c1.Delete(ctx, "e"))
	requireIndexCacheMiss(t, c2, "e")

	var wg sync.WaitGroup
	results := make([]bool, 2)
	for i, c := range []*IndexFileCache{c1, c2} {
		wg.Add(1)
		go func(i int, c *IndexFileCache) {
			defer wg.Done()
			written, err := c.Put(ctx, "race", indexCacheEntry(data))
			assert.NoError(t, err)
			results[i] = written
		}(i, c)
	}
	wg.Wait()
	require.NotEqual(t, results[0], results[1], "exactly one instance publishes the entry")
	for _, c := range []*IndexFileCache{c1, c2} {
		files, err := c.Open(ctx, "race", "data", "manifest")
		require.NoError(t, err)
		require.Equal(t, data, readAllFile(t, files[0]))
		closeFiles(files)
	}
	_, temps, trash := listCacheDirs(t, dir)
	require.Empty(t, temps)
	require.Empty(t, trash)
}

func TestIndexFileCacheConcurrentOperations(t *testing.T) {
	ctx := context.Background()
	dir := t.TempDir()
	c := newTestIndexFileCache(t, dir, 96<<10)
	names := []string{"a", "b", "c", "d", "e", "f"}
	payload := func(name string) []byte { return bytes.Repeat([]byte(name), 16<<10) }

	var wg sync.WaitGroup
	for w := 0; w < 8; w++ {
		wg.Add(1)
		go func(seed int64) {
			defer wg.Done()
			r := rand.New(rand.NewSource(seed))
			for i := 0; i < 200; i++ {
				name := names[r.Intn(len(names))]
				switch r.Intn(4) {
				case 0, 1:
					_, err := c.Put(ctx, name, indexCacheEntry(payload(name)))
					assert.NoError(t, err)
				case 2:
					files, err := c.Open(ctx, name, "data", "manifest")
					if err != nil {
						assert.True(t, moerr.IsMoErrCode(err, moerr.ErrFileNotFound), err)
						continue
					}
					assert.Equal(t, payload(name), readAllFile(t, files[0]))
					assert.Equal(t, []byte(fmt.Sprintf("len=%d", len(payload(name)))), readAllFile(t, files[1]))
					closeFiles(files)
				case 3:
					assert.NoError(t, c.Delete(ctx, name))
				}
			}
		}(int64(w))
	}
	wg.Wait()
	evictIndexCache(c)

	entries, temps, trash := listCacheDirs(t, dir)
	require.Empty(t, temps)
	require.Empty(t, trash)
	for _, e := range entries {
		require.True(t, c.disk.cache.Contains(e), "entry on disk but not in the cache: %s", e)
	}
	for _, name := range names {
		diskPath, err := c.disk.pathForDir(name)
		require.NoError(t, err)
		if c.disk.cache.Contains(diskPath) {
			_, err := os.Stat(diskPath)
			require.NoError(t, err, "entry in the cache but not on disk: %s", diskPath)
		}
	}
}

// An entry replaced while it is being opened yields its own files or a miss, never a mix.
func TestIndexFileCacheOpenReturnsOneGeneration(t *testing.T) {
	ctx := context.Background()
	c := newTestIndexFileCache(t, t.TempDir(), 1<<30)
	oldData := []byte("old")
	_, err := c.Put(ctx, "e", indexCacheEntry(oldData))
	require.NoError(t, err)

	replaced := false
	c.disk.beforeCacheDirFileOpen = func() {
		if replaced {
			return
		}
		replaced = true
		require.NoError(t, c.Delete(ctx, "e"))
		written, err := c.Put(ctx, "e", indexCacheEntry([]byte("newer content")))
		require.NoError(t, err)
		require.True(t, written)
	}
	files, err := c.Open(ctx, "e", "data", "manifest")
	require.True(t, replaced)
	if err != nil {
		require.True(t, moerr.IsMoErrCode(err, moerr.ErrFileNotFound), err)
		return
	}
	defer closeFiles(files)
	require.Equal(t, oldData, readAllFile(t, files[0]))
	require.Equal(t, []byte("len=3"), readAllFile(t, files[1]), "manifest must come from the same entry as data")
}

func TestDiskCacheIgnoresDirectoryEntriesWithoutIndexCache(t *testing.T) {
	ctx := context.Background()
	dir := t.TempDir()
	entry := filepath.Join(dir, "abc", "x"+cacheDirSuffix)
	require.NoError(t, os.MkdirAll(entry, 0755))
	require.NoError(t, os.WriteFile(filepath.Join(entry, "data"), []byte("x"), 0644))

	cache, err := NewDiskCache(ctx, dir, fscache.ConstCapacity(1<<20), nil, false, nil, "")
	require.NoError(t, err)
	defer cache.Close(ctx)
	require.False(t, cache.cache.Contains(entry), "a table-data cache never indexes a directory")
	_, err = os.Stat(filepath.Join(entry, "data"))
	require.True(t, os.IsNotExist(err), "a table-data cache removes unknown files, as before")
}
