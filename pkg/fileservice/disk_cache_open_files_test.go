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
	"os"
	"path/filepath"
	"sync"
	"testing"

	"github.com/matrixorigin/matrixone/pkg/fileservice/fscache"
	"github.com/stretchr/testify/require"
)

func readTestEntry(t *testing.T, ctx context.Context, cache *DiskCache, path string, size int64) ([]byte, bool) {
	t.Helper()
	vec := &IOVector{FilePath: path, Entries: []IOEntry{{Offset: 0, Size: size}}}
	require.NoError(t, cache.Read(ctx, vec))
	defer vec.Release()
	if !vec.Entries[0].done {
		return nil, false
	}
	return append([]byte(nil), vec.Entries[0].Data...), true
}

// A read keeps the cache file it opened open, and the next read of the file reads
// through it instead of opening it again.
func TestDiskCacheReadKeepsFileOpen(t *testing.T) {
	ctx := context.Background()
	cache, err := NewDiskCache(ctx, t.TempDir(), fscache.ConstCapacity(1<<20), nil, false, nil, "")
	require.NoError(t, err)
	defer cache.Close(ctx)
	data := []byte("0123456789")
	require.NoError(t, cache.Update(ctx, &IOVector{FilePath: "foo", Entries: []IOEntry{{Offset: 0, Size: 10, Data: data}}}, false))
	diskPath := cache.pathForIOEntry("foo", IOEntry{Offset: 0, Size: 10})

	got, ok := readTestEntry(t, ctx, cache, "foo", 10)
	require.True(t, ok)
	require.Equal(t, data, got)
	cache.updatingPaths.L.Lock()
	kept := cache.updatingPaths.openFiles.m[diskPath]
	cache.updatingPaths.L.Unlock()
	require.NotNil(t, kept, "the read keeps its file open")
	require.Zero(t, kept.refs)

	got, ok = readTestEntry(t, ctx, cache, "foo", 10)
	require.True(t, ok)
	require.Equal(t, data, got)
	cache.updatingPaths.L.Lock()
	defer cache.updatingPaths.L.Unlock()
	require.Same(t, kept, cache.updatingPaths.openFiles.m[diskPath], "the next read reads the kept file")
	require.Zero(t, kept.refs)
}

// A delete of a cached object drops and closes the files kept open for it; the cache's
// close closes the others.
func TestDiskCacheKeptFileClosedOnDeleteAndClose(t *testing.T) {
	ctx := context.Background()
	cache, err := NewDiskCache(ctx, t.TempDir(), fscache.ConstCapacity(1<<20), nil, false, nil, "")
	require.NoError(t, err)
	for _, p := range []string{"foo", "bar"} {
		require.NoError(t, cache.Update(ctx, &IOVector{FilePath: p, Entries: []IOEntry{{Offset: 0, Size: 3, Data: []byte(p)}}}, false))
		_, ok := readTestEntry(t, ctx, cache, p, 3)
		require.True(t, ok)
	}
	keptOf := func(p string) *openCacheFile {
		cache.updatingPaths.L.Lock()
		defer cache.updatingPaths.L.Unlock()
		return cache.updatingPaths.openFiles.m[cache.pathForIOEntry(p, IOEntry{Offset: 0, Size: 3})]
	}
	foo, bar := keptOf("foo"), keptOf("bar")
	require.NotNil(t, foo)
	require.NotNil(t, bar)

	require.NoError(t, cache.DeletePaths(ctx, []string{"foo"}))
	require.Nil(t, keptOf("foo"))
	_, err = foo.file.Stat()
	require.ErrorIs(t, err, os.ErrClosed)
	_, ok := readTestEntry(t, ctx, cache, "foo", 3)
	require.False(t, ok, "a deleted entry is not read through its kept file")

	cache.Close(ctx)
	_, err = bar.file.Stat()
	require.ErrorIs(t, err, os.ErrClosed)
}

// Past the most files kept, the least recently taken file no read holds is closed for a
// new one; with every file held, a new file is not kept.
func TestOpenFilePoolLimit(t *testing.T) {
	dir := t.TempDir()
	open := func(name string) *os.File {
		f, err := os.Create(filepath.Join(dir, name))
		require.NoError(t, err)
		return f
	}
	o := openFilePool{limit: 2}
	a, closeA := o.keepLocked("a", open("a"))
	require.Nil(t, closeA)
	b, _ := o.keepLocked("b", open("b"))
	o.releaseLocked(a)
	o.releaseLocked(b)
	require.NotNil(t, o.takeLocked("a"))
	o.releaseLocked(a)

	c, closeFile := o.keepLocked("c", open("c"))
	require.NotNil(t, c)
	require.Same(t, b.file, closeFile, "the least recently taken file is closed")
	require.NoError(t, closeFile.Close())
	require.Nil(t, o.m["b"])

	require.NotNil(t, o.takeLocked("a"))
	d := open("d")
	defer d.Close()
	kept, closeFile := o.keepLocked("d", d)
	require.Nil(t, kept, "with every file held, none is kept")
	require.Nil(t, closeFile)

	// A dropped file a read holds is closed when the read releases it.
	require.Nil(t, o.releaseLocked(c))
	dropped := o.dropAllLocked()
	require.Equal(t, []*os.File{c.file}, dropped)
	require.NoError(t, c.file.Close())
	require.Same(t, a.file, o.releaseLocked(a))
	require.NoError(t, a.file.Close())
}

// Reads of an entry at once share its kept file and read its bytes while its object is
// deleted and cached again.
func TestDiskCacheConcurrentReadsOfKeptFile(t *testing.T) {
	ctx := context.Background()
	cache, err := NewDiskCache(ctx, t.TempDir(), fscache.ConstCapacity(1<<20), nil, false, nil, "")
	require.NoError(t, err)
	defer cache.Close(ctx)
	data := []byte("0123456789abcdef")
	update := func() {
		require.NoError(t, cache.Update(ctx, &IOVector{FilePath: "foo", Entries: []IOEntry{{Offset: 0, Size: 16, Data: data}}}, false))
	}
	update()
	var wg sync.WaitGroup
	stop := make(chan struct{})
	for i := 0; i < 8; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for {
				select {
				case <-stop:
					return
				default:
				}
				if got, ok := readTestEntry(t, ctx, cache, "foo", 16); ok {
					require.Equal(t, data, got)
				}
			}
		}()
	}
	for i := 0; i < 50; i++ {
		require.NoError(t, cache.DeletePaths(ctx, []string{"foo"}))
		update()
	}
	close(stop)
	wg.Wait()
}

// A read that finds no file at a path notes it missing and does not open it again; a write
// of the path forgets that, so the next read finds the file.
func TestDiskCacheMissingPathForgottenOnWrite(t *testing.T) {
	ctx := context.Background()
	cache, err := NewDiskCache(ctx, t.TempDir(), fscache.ConstCapacity(1<<20), nil, false, nil, "")
	require.NoError(t, err)
	defer cache.Close(ctx)
	diskPath := cache.pathForIOEntry("foo", IOEntry{Offset: 0, Size: 3})
	_, ok := readTestEntry(t, ctx, cache, "foo", 3)
	require.False(t, ok)
	cache.updatingPaths.L.Lock()
	require.True(t, cache.updatingPaths.openFiles.missingLocked(diskPath))
	cache.updatingPaths.L.Unlock()

	require.NoError(t, cache.Update(ctx, &IOVector{FilePath: "foo", Entries: []IOEntry{{Offset: 0, Size: 3, Data: []byte("abc")}}}, false))
	got, ok := readTestEntry(t, ctx, cache, "foo", 3)
	require.True(t, ok)
	require.Equal(t, []byte("abc"), got)
}
