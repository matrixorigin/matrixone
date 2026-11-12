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

func roomFor() bool { return true }

func noRoom() bool { return false }

func newTestPreloadUsage() (*preloadUsage, *fakeClock) {
	clock := &fakeClock{t: time.Unix(1000, 0)}
	p := newPreloadUsage()
	p.now = clock.now
	return p, clock
}

// evict maps the i-th whole-object file of group to it, caches it, reads served
// bytes from it and evicts it.
func evict(p *preloadUsage, group uint64, i int, served, size int64) {
	path := fmt.Sprintf("/cache/00%d/full%d-%d.mofscache", i%10, group, i)
	p.narrowRead(path, group, roomFor)
	p.cached(path)
	p.read(path, served)
	p.removed(path, size)
}

func TestPreloadUsageGroupThrashesAfterMinSamples(t *testing.T) {
	p, _ := newTestPreloadUsage()
	for i := 0; i < preloadUsageMinSamples-1; i++ {
		evict(p, 1, i, 0, 100)
		require.False(t, p.narrowRead("/cache/x", 1, roomFor), "fewer than %d evictions", preloadUsageMinSamples)
	}
	evict(p, 1, preloadUsageMinSamples, 0, 100)
	require.True(t, p.narrowRead("/cache/x", 1, roomFor))
}

func TestPreloadUsagePartlyUsedGroupNeedsMoreSamples(t *testing.T) {
	p, _ := newTestPreloadUsage()
	for i := 0; i < preloadUsageMoreSamples-1; i++ {
		evict(p, 1, i, 20, 100)
		require.False(t, p.narrowRead("/cache/x", 1, roomFor), "a share of 0.2 over %d evictions", i+1)
	}
	evict(p, 1, preloadUsageMoreSamples, 20, 100)
	require.True(t, p.narrowRead("/cache/x", 1, roomFor))
}

func TestPreloadUsageGroupsAreSeparate(t *testing.T) {
	p, _ := newTestPreloadUsage()
	for i := 0; i < 20; i++ {
		evict(p, 1, i, 0, 100)  // the base table thrashes
		evict(p, 2, i, 90, 100) // the entries table is read well
	}
	require.True(t, p.narrowRead("/cache/x", 1, roomFor))
	require.False(t, p.narrowRead("/cache/y", 2, roomFor))
	require.False(t, p.narrowRead("/cache/z", 3, roomFor), "a group with no evictions")
}

func TestPreloadUsageIgnoresFilesOfNoNarrowRead(t *testing.T) {
	p, _ := newTestPreloadUsage()
	for i := 0; i < 20; i++ {
		path := fmt.Sprintf("/cache/001/full%d.mofscache", i)
		p.cached(path)
		p.removed(path, 100)
	}
	require.Empty(t, p.groups, "scans do not map their files to a group")
}

func TestPreloadUsageShareIsCappedAtOne(t *testing.T) {
	p, _ := newTestPreloadUsage()
	for i := 0; i < 20; i++ {
		evict(p, 1, i, 0, 100)
	}
	for i := 20; i < 23; i++ {
		evict(p, 1, i, 1000, 100)
	}
	require.LessOrEqual(t, p.groups[1].average, 1.0)
}

func TestPreloadUsageStaysOnWhileItsFilesAreEvicted(t *testing.T) {
	p, clock := newTestPreloadUsage()
	for i := 0; i < 10; i++ {
		evict(p, 1, i, 0, 100)
	}
	require.True(t, p.narrowRead("/cache/x", 1, roomFor))
	// Whole-object files of the group are still evicted, even well read: it stays on.
	for i := 10; i < 100; i++ {
		clock.t = clock.t.Add(time.Second)
		evict(p, 1, i, 100, 100)
		require.True(t, p.narrowRead("/cache/x", 1, roomFor))
	}
}

func TestPreloadUsageTurnsOffAfterItsQuietPeriod(t *testing.T) {
	p, clock := newTestPreloadUsage()
	for i := 0; i < 10; i++ {
		evict(p, 1, i, 0, 100)
		evict(p, 2, i, 0, 100)
	}
	require.True(t, p.narrowRead("/cache/x", 1, roomFor))
	require.True(t, p.narrowRead("/cache/y", 2, roomFor))
	// Group 2's files keep being evicted, from other files and range files of the
	// cache too; group 1's stop.
	for i := 0; i <= int(preloadQuietPeriod/time.Second); i++ {
		clock.t = clock.t.Add(time.Second)
		evict(p, 2, 100+i, 0, 100)
		p.cached("/cache/001/fullscan.mofscache")
		p.removed("/cache/001/fullscan.mofscache", 100)
		p.removed(fmt.Sprintf("/cache/001/%d-10obj.mofscache", i), 10)
	}
	require.False(t, p.narrowRead("/cache/x", 1, roomFor), "group 1 fits once its own files stop being evicted")
	require.True(t, p.narrowRead("/cache/y", 2, roomFor))
	// Turning off restarts the evidence: it takes the minimum samples again.
	for i := 10; i < 10+preloadUsageMinSamples-1; i++ {
		evict(p, 1, i, 0, 100)
	}
	require.False(t, p.narrowRead("/cache/x", 1, roomFor))
}

// A group that stopped being evicted only because it stopped filling whole objects
// keeps thrashing while the cache has no room for another whole object.
func TestPreloadUsageStaysOnWithoutRoom(t *testing.T) {
	p, clock := newTestPreloadUsage()
	for i := 0; i < preloadUsageMinSamples; i++ {
		evict(p, 1, i, 0, 100)
	}
	require.True(t, p.narrowRead("/cache/x", 1, noRoom))
	clock.t = clock.t.Add(preloadQuietPeriodMax + time.Second)
	require.True(t, p.narrowRead("/cache/x", 1, noRoom))
	require.False(t, p.narrowRead("/cache/x", 1, roomFor))
}

func TestPreloadUsageQuietPeriodDoublesWhenThrashingResumes(t *testing.T) {
	p, clock := newTestPreloadUsage()
	thrash := func() {
		for i := 0; i < preloadUsageMinSamples; i++ {
			evict(p, 1, i, 0, 100)
		}
		require.True(t, p.narrowRead("/cache/x", 1, roomFor))
	}
	end := func() {
		clock.t = clock.t.Add(p.groups[1].quiet + time.Second)
		require.False(t, p.narrowRead("/cache/x", 1, roomFor))
	}
	thrash()
	want := preloadQuietPeriod
	require.Equal(t, want, p.groups[1].quiet)
	for i := 0; i < 8; i++ {
		end()
		thrash() // right after it ended
		want = min(2*want, preloadQuietPeriodMax)
		require.Equal(t, want, p.groups[1].quiet)
	}
	require.Equal(t, preloadQuietPeriodMax, p.groups[1].quiet)

	// Thrashing long after it ended starts over.
	end()
	clock.t = clock.t.Add(2 * preloadQuietPeriodMax)
	thrash()
	require.Equal(t, preloadQuietPeriod, p.groups[1].quiet)
}

func TestPreloadUsageExplicitDeleteIsNotEviction(t *testing.T) {
	p, clock := newTestPreloadUsage()
	for i := 0; i < 10; i++ {
		evict(p, 1, i, 0, 100)
	}
	require.True(t, p.narrowRead("/cache/x", 1, roomFor))
	samples := p.groups[1].samples

	clock.t = clock.t.Add(preloadQuietPeriod / 2)
	path := "/cache/001/full-deleted.mofscache"
	p.narrowRead(path, 1, roomFor)
	p.cached(path)
	p.deleted(path)
	p.removed(path, 100)
	require.Equal(t, samples, p.groups[1].samples)
	require.Empty(t, p.deleting)
	require.Empty(t, p.served)

	// The delete did not extend the quiet period.
	clock.t = clock.t.Add(preloadQuietPeriod/2 + time.Second)
	require.Equal(t, preloadQuietPeriod, p.groups[1].quiet)
	require.False(t, p.narrowRead("/cache/x", 1, roomFor))
}

func TestPreloadUsageNoGroup(t *testing.T) {
	p, _ := newTestPreloadUsage()
	for i := 0; i < 20; i++ {
		evict(p, 0, i, 0, 100)
	}
	require.Empty(t, p.objects, "reads of no group are not mapped")
	require.Empty(t, p.groups)
	require.False(t, p.narrowRead("/cache/x", 0, roomFor))
}

func TestPreloadUsageEvictionForgetsTheObject(t *testing.T) {
	p, _ := newTestPreloadUsage()
	evict(p, 1, 0, 0, 100)
	require.Empty(t, p.objects, "an evicted file is mapped again by its next narrow read")
	require.Equal(t, 1, p.groups[1].samples)
}

func TestPreloadUsageObjectsAreBounded(t *testing.T) {
	p, _ := newTestPreloadUsage()
	for i := 0; i <= preloadObjectsMax; i++ {
		p.narrowRead(fmt.Sprintf("/cache/full%d", i), 1, roomFor)
	}
	require.Len(t, p.objects, preloadObjectsMax)
}

func TestPreloadUsageGroupsAreBounded(t *testing.T) {
	p, clock := newTestPreloadUsage()
	for g := uint64(1); g <= preloadGroupsMax; g++ {
		evict(p, g, 0, 0, 100)
	}
	for i := 1; i < preloadUsageMinSamples; i++ {
		evict(p, 1, i, 0, 100)
	}
	require.True(t, p.narrowRead("/cache/x", 1, roomFor))

	evict(p, preloadGroupsMax+1, 0, 0, 100)
	require.Len(t, p.groups, preloadGroupsMax, "no group is idle yet")
	require.NotContains(t, p.groups, uint64(preloadGroupsMax+1))

	for i := 0; i < 3; i++ {
		clock.t = clock.t.Add(preloadQuietPeriod - time.Second)
		evict(p, 1, preloadUsageMinSamples+i, 0, 100)
	}
	evict(p, preloadGroupsMax+1, 0, 0, 100)
	require.Len(t, p.groups, 2, "idle groups are forgotten")
	require.Contains(t, p.groups, uint64(preloadGroupsMax+1))
	require.True(t, p.narrowRead("/cache/x", 1, roomFor), "a thrashing group is kept")
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
	require.False(t, p.narrowRead("/cache/001/full.mofscache", 1, roomFor))
}

type skippedFS struct {
	FileService
	group uint64
}

func (f skippedFS) WholeObjectPreloadSkipped(_ string, group uint64) bool { return group == f.group }

func TestFullFilePreloadSkipped(t *testing.T) {
	fs := skippedFS{group: 7}
	thrashing := WithPreloadGroup(context.Background(), 7)
	other := WithPreloadGroup(context.Background(), 8)
	for _, tc := range []struct {
		name   string
		ctx    context.Context
		fs     FileService
		policy Policy
		want   bool
	}{
		{"default policy", thrashing, fs, 0, false},
		{"skip policy", other, fs, SkipFullFilePreloads, true},
		{"narrow, group keeps whole objects", other, fs, NarrowRead, false},
		{"narrow, group thrashing", thrashing, fs, NarrowRead, true},
		{"narrow, no group", context.Background(), fs, NarrowRead, false},
		{"narrow, no disk cache signal", thrashing, nil, NarrowRead, false},
	} {
		t.Run(tc.name, func(t *testing.T) {
			require.Equal(t, tc.want, FullFilePreloadSkipped(tc.ctx, tc.fs, tc.policy, "obj"))
		})
	}
	require.Equal(t, uint64(7), GetPreloadGroup(thrashing))
	require.Zero(t, GetPreloadGroup(context.Background()))
}

// A real disk cache too small for the whole-object files of a group's narrow reads,
// each read a few bytes, reports that group thrashing and no other; a deleted file
// does not count.
func TestDiskCacheReportsGroupThrashing(t *testing.T) {
	ctx := context.Background()
	const fileSize = 4096
	cache, err := NewDiskCache(ctx, t.TempDir(), fscache.ConstCapacity(3*fileSize), nil, false, nil, "")
	require.NoError(t, err)
	defer cache.Close(ctx)

	content := bytes.Repeat([]byte("x"), fileSize)
	for i := 0; i < 40 && !cache.preloads.narrowRead("probe", 1, roomFor); i++ {
		func() {
			name := fmt.Sprintf("obj%d", i)
			cache.preloads.narrowRead(cache.pathForFile(name), 1, roomFor)
			require.NoError(t, cache.SetFile(ctx, name, func(context.Context) (io.ReadCloser, error) {
				return io.NopCloser(bytes.NewReader(content)), nil
			}))
			vec := &IOVector{FilePath: name, Entries: []IOEntry{{Offset: 0, Size: 16}}}
			defer vec.Release()
			require.NoError(t, cache.Read(ctx, vec))
			require.True(t, vec.Entries[0].done)
			cache.Evict(ctx, nil)
		}()
	}
	require.True(t, cache.preloads.narrowRead("probe", 1, roomFor))
	require.False(t, cache.preloads.narrowRead("probe", 2, roomFor))

	samples := cache.preloads.groups[1].samples
	require.NoError(t, cache.DeletePaths(ctx, []string{"obj39", "obj38", "obj37"}))
	require.Equal(t, samples, cache.preloads.groups[1].samples)
	require.Empty(t, cache.preloads.deleting)
}

// A whole object, sized as the largest whole-object file cached so far, fits while it
// and the fills in progress stay within the capacity.
func TestDiskCacheWholeObjectFits(t *testing.T) {
	ctx := context.Background()
	const fileSize = 4096
	cache, err := NewDiskCache(ctx, t.TempDir(), fscache.ConstCapacity(4*fileSize), nil, false, nil, "")
	require.NoError(t, err)
	defer cache.Close(ctx)
	require.False(t, cache.wholeObjectFits(), "before any file, the estimate is larger than the cache")

	content := bytes.Repeat([]byte("x"), fileSize)
	set := func(name string) {
		require.NoError(t, cache.SetFile(ctx, name, func(context.Context) (io.ReadCloser, error) {
			return io.NopCloser(bytes.NewReader(content)), nil
		}))
	}
	for i := 0; i < 3; i++ {
		set(fmt.Sprintf("obj%d", i))
		require.True(t, cache.wholeObjectFits(), "%d files cached", i+1)
	}
	pr, pw := io.Pipe()
	filled := make(chan error, 1)
	go func() {
		filled <- cache.SetFile(ctx, "obj3", func(context.Context) (io.ReadCloser, error) { return pr, nil })
	}()
	_, err = pw.Write(content[:1])
	require.NoError(t, err)
	require.False(t, cache.wholeObjectFits(), "a fill in progress takes the last room")
	_, err = pw.Write(content[1:])
	require.NoError(t, err)
	require.NoError(t, pw.Close())
	require.NoError(t, <-filled)
	require.False(t, cache.wholeObjectFits(), "the cache is full")
}

func TestS3FSWholeObjectPreloadSkippedByGroup(t *testing.T) {
	ctx := context.Background()
	s, err := NewS3FS(ctx, ObjectStorageArguments{Name: "s3", Endpoint: "disk", Bucket: t.TempDir()}, DisabledCacheConfig, nil, true, false)
	require.NoError(t, err)
	defer s.Close(ctx)
	require.False(t, s.WholeObjectPreloadSkipped("foo", 1), "no disk cache")

	cache, err := NewDiskCache(ctx, t.TempDir(), fscache.ConstCapacity(1<<20), nil, false, nil, "")
	require.NoError(t, err)
	defer cache.Close(ctx)
	s.diskCache = cache

	require.False(t, s.WholeObjectPreloadSkipped("s3:foo", 1))
	require.Equal(t, uint64(1), cache.preloads.objects[cache.pathForFile("foo")], "the object is mapped to its group")
	cache.preloads.mu.Lock()
	cache.preloads.groups[1] = &groupUsage{thrashing: true, lastEvict: cache.preloads.now(), quiet: preloadQuietPeriod}
	cache.preloads.mu.Unlock()
	require.True(t, s.WholeObjectPreloadSkipped("foo", 1))
	require.False(t, s.WholeObjectPreloadSkipped("foo", 2))
	require.False(t, s.WholeObjectPreloadSkipped("other:foo", 1), "a path of another service")

	// A narrow read of a thrashing table caches ranges; other reads keep whole objects.
	require.NoError(t, s.Write(ctx, IOVector{FilePath: "foo", Entries: []IOEntry{{Size: 3, Data: []byte("abc")}}}))
	read := func(ctx context.Context, policy Policy) Policy {
		vec := &IOVector{FilePath: "foo", Policy: policy, Entries: []IOEntry{{Offset: 1, Size: 1}}}
		require.NoError(t, s.Read(ctx, vec))
		require.Equal(t, []byte("b"), vec.Entries[0].Data)
		return vec.Policy
	}
	thrashing := WithPreloadGroup(ctx, 1)
	require.Equal(t, Policy(NarrowRead|SkipFullFilePreloads), read(thrashing, NarrowRead))
	require.Equal(t, Policy(NarrowRead), read(WithPreloadGroup(ctx, 2), NarrowRead))
	require.Equal(t, Policy(NarrowRead), read(ctx, NarrowRead), "a read of no group")
	require.Equal(t, Policy(0), read(thrashing, 0), "a scan")
}
