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

package indexfile

import (
	"context"
	"os"
	"sync"
	"sync/atomic"
	"testing"

	"github.com/matrixorigin/matrixone/pkg/fileservice"
	"github.com/stretchr/testify/require"
)

// countingFS counts the reads of objects.
type countingFS struct {
	fileservice.FileService
	reads atomic.Int64
}

func (c *countingFS) Read(ctx context.Context, vector *fileservice.IOVector) error {
	c.reads.Add(1)
	return c.FileService.Read(ctx, vector)
}

func newObjectFS(t *testing.T, size int) (*countingFS, []byte) {
	mem, err := fileservice.NewMemoryFS("mem", fileservice.DisabledCacheConfig, nil)
	require.NoError(t, err)
	data := make([]byte, size)
	for i := range data {
		data[i] = byte(i*13 + i/997)
	}
	require.NoError(t, mem.Write(context.Background(), fileservice.IOVector{FilePath: "object", Entries: []fileservice.IOEntry{{Size: int64(size), Data: data}}}))
	return &countingFS{FileService: mem}, data
}

// An object is read once into the cache, and later opens are served from it.
func TestOpenObjectCachesIt(t *testing.T) {
	fs, data := newObjectFS(t, 300<<10)
	cache := newCache(t, t.TempDir())
	ctx := context.Background()
	f, err := OpenObject(ctx, cache, fs, "object", int64(len(data)))
	require.NoError(t, err)
	require.Equal(t, data, readFile(t, f))
	require.NoError(t, f.Close())
	require.Equal(t, int64(1), fs.reads.Load())

	f, err = OpenObject(ctx, cache, fs, "object", 0)
	require.NoError(t, err)
	require.Equal(t, data, readFile(t, f))
	require.NoError(t, f.Close())
	require.Equal(t, int64(1), fs.reads.Load(), "a cached object is not read again")
}

// Concurrent opens of an object read it once.
func TestOpenObjectReadsOnceForConcurrentOpens(t *testing.T) {
	fs, data := newObjectFS(t, 300<<10)
	cache := newCache(t, t.TempDir())
	var wg sync.WaitGroup
	for range 16 {
		wg.Add(1)
		go func() {
			defer wg.Done()
			f, err := OpenObject(context.Background(), cache, fs, "object", 0)
			require.NoError(t, err)
			require.Equal(t, data, readFile(t, f))
			require.NoError(t, f.Close())
		}()
	}
	wg.Wait()
	require.Equal(t, int64(1), fs.reads.Load())
}

// A large object is read as ranges at once, each written at its offset.
func TestOpenObjectReadsParts(t *testing.T) {
	fs, data := newObjectFS(t, objectPartMin+3<<10)
	cache := newCache(t, t.TempDir())
	f, err := OpenObject(context.Background(), cache, fs, "object", int64(len(data)))
	require.NoError(t, err)
	defer f.Close()
	require.Equal(t, data, readFile(t, f))
	require.Equal(t, int64(objectParts), fs.reads.Load())
}

// A cached entry of another size than the object's is read again.
func TestOpenObjectRereadsEntryOfOtherSize(t *testing.T) {
	fs, data := newObjectFS(t, 300<<10)
	cache := newCache(t, t.TempDir())
	ctx := context.Background()
	tmp, remove, err := cache.CreateTempFile()
	require.NoError(t, err)
	defer remove()
	_, err = tmp.Write(data[:1000])
	require.NoError(t, err)
	require.NoError(t, tmp.Close())
	_, err = cache.Put(ctx, fileservice.IndexObjectEntry("object"), []fileservice.IndexCacheFile{{Name: fileservice.IndexObjectFile, Path: tmp.Name()}})
	require.NoError(t, err)

	f, err := OpenObject(ctx, cache, fs, "object", int64(len(data)))
	require.NoError(t, err)
	defer f.Close()
	require.Equal(t, data, readFile(t, f))
	require.Equal(t, int64(1), fs.reads.Load())
}

// An object that cannot be read is an error, and nothing is cached.
func TestOpenObjectOfMissingObject(t *testing.T) {
	fs, _ := newObjectFS(t, 1000)
	cache := newCache(t, t.TempDir())
	_, err := OpenObject(context.Background(), cache, fs, "missing", 0)
	require.Error(t, err)
	_, err = cache.Open(context.Background(), fileservice.IndexObjectEntry("missing"), fileservice.IndexObjectFile)
	require.Error(t, err)
	_, statErr := os.Stat(cache.Path())
	require.NoError(t, statErr)
}
