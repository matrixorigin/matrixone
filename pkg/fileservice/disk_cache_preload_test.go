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
	"fmt"
	"io"
	"testing"
	"time"

	"github.com/matrixorigin/matrixone/pkg/fileservice/fscache"
	"github.com/stretchr/testify/require"
)

type fakeClock struct{ t time.Time }

func (c *fakeClock) now() time.Time { return c.t }

func newTestPreloadUsage() (*preloadUsage, *fakeClock) {
	clock := &fakeClock{t: time.Unix(1000, 0)}
	p := newPreloadUsage()
	p.now = clock.now
	return p, clock
}

// evict caches a whole-object file of size bytes, reads served bytes from it and evicts it.
func evictWithShare(p *preloadUsage, i int, served, size int64) {
	path := fmt.Sprintf("/cache/00%d/full%d.mofscache", i%10, i)
	p.cached(path)
	p.read(path, served)
	p.removed(path, size)
}

func TestPreloadUsageThrashingNeedsLowShareOverMinSamples(t *testing.T) {
	p, _ := newTestPreloadUsage()
	for i := 0; i < preloadUsageMinSamples-1; i++ {
		evictWithShare(p, i, 0, 100)
		require.False(t, p.isThrashing(), "fewer than %d evictions", preloadUsageMinSamples)
	}
	evictWithShare(p, preloadUsageMinSamples, 0, 100)
	require.True(t, p.isThrashing())
}

func TestPreloadUsageWellReadFilesNeverThrash(t *testing.T) {
	p, _ := newTestPreloadUsage()
	for i := 0; i < 100; i++ {
		evictWithShare(p, i, 90, 100)
	}
	require.False(t, p.isThrashing())
}

func TestPreloadUsageShareIsCappedAtOne(t *testing.T) {
	p, _ := newTestPreloadUsage()
	for i := 0; i < 20; i++ {
		evictWithShare(p, i, 0, 100)
	}
	require.True(t, p.isThrashing())
	// Files read many times over must not pull the average above 1.
	for i := 20; i < 23; i++ {
		evictWithShare(p, i, 1000, 100)
	}
	require.LessOrEqual(t, p.average, 1.0)
}

func TestPreloadUsageStaysOnWhileEvictionsContinue(t *testing.T) {
	p, clock := newTestPreloadUsage()
	for i := 0; i < 10; i++ {
		evictWithShare(p, i, 0, 100)
	}
	require.True(t, p.isThrashing())
	// With narrow reads no longer filling whole objects, the remaining evictions
	// are well-read files and range files; neither turns thrashing off.
	for i := 10; i < 100; i++ {
		clock.t = clock.t.Add(time.Second)
		evictWithShare(p, i, 100, 100)
		p.removed(fmt.Sprintf("/cache/001/range%d.mofscache", i), 10)
		require.True(t, p.isThrashing())
	}
}

func TestPreloadUsageTurnsOffAfterQuietPeriod(t *testing.T) {
	p, clock := newTestPreloadUsage()
	for i := 0; i < 10; i++ {
		evictWithShare(p, i, 0, 100)
	}
	require.True(t, p.isThrashing())
	clock.t = clock.t.Add(preloadQuietPeriod)
	require.True(t, p.isThrashing(), "the quiet period has not passed")
	clock.t = clock.t.Add(time.Second)
	require.False(t, p.isThrashing())
	// Turning off restarts the evidence: it takes the minimum samples again.
	for i := 10; i < 10+preloadUsageMinSamples-1; i++ {
		evictWithShare(p, i, 0, 100)
	}
	require.False(t, p.isThrashing())
}

func TestPreloadUsageExplicitDeleteIsNotEviction(t *testing.T) {
	p, clock := newTestPreloadUsage()
	for i := 0; i < 10; i++ {
		evictWithShare(p, i, 0, 100)
	}
	require.True(t, p.isThrashing())
	samples := p.samples

	clock.t = clock.t.Add(preloadQuietPeriod / 2)
	path := "/cache/001/full-deleted.mofscache"
	p.cached(path)
	p.deleted(path)
	p.removed(path, 100)
	require.Equal(t, samples, p.samples)
	require.Empty(t, p.deleting)
	require.Empty(t, p.served)

	// The delete did not extend the quiet period.
	clock.t = clock.t.Add(preloadQuietPeriod/2 + time.Second)
	require.False(t, p.isThrashing())
}

func TestPreloadUsageIgnoresEntryFiles(t *testing.T) {
	p, _ := newTestPreloadUsage()
	p.cached("/cache/001/foo.mofscache")
	require.Empty(t, p.served)
}

func TestPreloadUsageNilIsSafe(t *testing.T) {
	var p *preloadUsage
	p.cached("/cache/001/full.mofscache")
	p.read("/cache/001/full.mofscache", 1)
	p.deleted("/cache/001/full.mofscache")
	p.removed("/cache/001/full.mofscache", 1)
	require.False(t, p.isThrashing())
}

type thrashingFS struct {
	FileService
	thrashing bool
}

func (f thrashingFS) fullFilePreloadsThrashing() bool { return f.thrashing }

func TestFullFilePreloadsSkipped(t *testing.T) {
	ctx := context.Background()
	with := func(p Policy) context.Context { return WithFileServicePolicy(ctx, p) }
	for _, tc := range []struct {
		name string
		ctx  context.Context
		fs   FileService
		want bool
	}{
		{"default policy", ctx, thrashingFS{thrashing: true}, false},
		{"skip policy", with(SkipFullFilePreloads), thrashingFS{}, true},
		{"narrow, cache keeps whole objects", with(NarrowRead), thrashingFS{}, false},
		{"narrow, cache thrashing", with(NarrowRead), thrashingFS{thrashing: true}, true},
		{"narrow, no disk cache signal", with(NarrowRead), nil, false},
	} {
		t.Run(tc.name, func(t *testing.T) {
			require.Equal(t, tc.want, FullFilePreloadsSkipped(tc.ctx, tc.fs))
		})
	}
}

// A real disk cache too small for the whole-object files it is given, read a few bytes
// each, reports thrashing; a deleted file does not count.
func TestDiskCacheReportsWholeObjectThrashing(t *testing.T) {
	ctx := context.Background()
	const fileSize = 4096
	cache, err := NewDiskCache(ctx, t.TempDir(), fscache.ConstCapacity(3*fileSize), nil, false, nil, "")
	require.NoError(t, err)
	defer cache.Close(ctx)

	content := bytes.Repeat([]byte("x"), fileSize)
	for i := 0; i < 40 && !cache.preloads.isThrashing(); i++ {
		name := fmt.Sprintf("obj%d", i)
		require.NoError(t, cache.SetFile(ctx, name, func(context.Context) (io.ReadCloser, error) {
			return io.NopCloser(bytes.NewReader(content)), nil
		}))
		vec := &IOVector{FilePath: name, Entries: []IOEntry{{Offset: 0, Size: 16}}}
		require.NoError(t, cache.Read(ctx, vec))
		require.True(t, vec.Entries[0].done)
		cache.Evict(ctx, nil)
	}
	require.True(t, cache.preloads.isThrashing())

	samples := cache.preloads.samples
	require.NoError(t, cache.DeletePaths(ctx, []string{"obj39", "obj38", "obj37"}))
	require.Equal(t, samples, cache.preloads.samples)
	require.Empty(t, cache.preloads.deleting)
}

func TestS3FSNarrowReadCachesRangesWhileThrashing(t *testing.T) {
	ctx := context.Background()
	s, err := NewS3FS(ctx, ObjectStorageArguments{Name: "s3", Endpoint: "disk", Bucket: t.TempDir()}, DisabledCacheConfig, nil, true, false)
	require.NoError(t, err)
	defer s.Close(ctx)
	require.NoError(t, s.Write(ctx, IOVector{FilePath: "foo", Entries: []IOEntry{{Size: 3, Data: []byte("abc")}}}))

	cache, err := NewDiskCache(ctx, t.TempDir(), fscache.ConstCapacity(1<<20), nil, false, nil, "")
	require.NoError(t, err)
	defer cache.Close(ctx)
	s.diskCache = cache

	read := func() Policy {
		vec := &IOVector{FilePath: "foo", Policy: NarrowRead, Entries: []IOEntry{{Offset: 1, Size: 1}}}
		require.NoError(t, s.Read(ctx, vec))
		require.Equal(t, []byte("b"), vec.Entries[0].Data)
		return vec.Policy
	}
	require.Equal(t, Policy(NarrowRead), read())
	require.False(t, FullFilePreloadsSkipped(WithFileServicePolicy(ctx, NarrowRead), s))

	cache.preloads.mu.Lock()
	cache.preloads.thrashing = true
	cache.preloads.lastEvict = cache.preloads.now()
	cache.preloads.mu.Unlock()
	require.Equal(t, Policy(NarrowRead|SkipFullFilePreloads), read())
	require.True(t, FullFilePreloadsSkipped(WithFileServicePolicy(ctx, NarrowRead), s))
}
