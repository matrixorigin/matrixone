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
	"io"
	"slices"
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

// waitRoom waits for room of class c of g.
func waitRoom(g *governor, ctx context.Context, c requestClass) error {
	_, err := g.wait(ctx, c)
	return err
}

// prefetchObject queues the fills of req, whatever is cached, and waits until they end.
func (s *S3FS) prefetchObject(_ context.Context, req ObjectPrefetch) {
	path, err := parseFilePathAtService(req.FilePath, s.name)
	if err != nil || req.Size <= 0 {
		return
	}
	req.FilePath = path.File
	s.diskCache.addFills(req, false, s.source(req.FilePath))
	waitFillsEnd(s.diskCache, req.FilePath)
}

// waitFillsEnd waits until no fill of the object at filePath waits or runs.
func waitFillsEnd(d *DiskCache, filePath string) {
	for deadline := time.Now().Add(30 * time.Second); time.Now().Before(deadline); time.Sleep(time.Millisecond) {
		if !d.isQueued(filePath) && !d.fillsUnderWay(filePath) {
			return
		}
	}
}

// isQueued reports whether a fill of the object at filePath waits to start.
func (d *DiskCache) isQueued(filePath string) bool {
	f := &d.prefetchObjects
	f.mu.Lock()
	defer f.mu.Unlock()
	diskPath := d.objectKey(filePath)
	for _, index := range []bool{false, true} {
		if slices.ContainsFunc(f.fills[fillKey{diskPath: diskPath, index: index}], func(q *queuedFill) bool { return q.pos >= 0 }) {
			return true
		}
	}
	return false
}

// fillState is what tests observe of a queued fill.
type fillState struct {
	req        ObjectPrefetch
	start, end int64
	whole      bool
	scans      []*PrefetchScan
	joiners    int
}

// waitingFillsOf returns the fills of the object at filePath into the disk cache waiting to
// start.
func (d *DiskCache) waitingFillsOf(filePath string) []fillState {
	f := &d.prefetchObjects
	f.mu.Lock()
	defer f.mu.Unlock()
	var out []fillState
	for _, q := range f.fills[fillKey{diskPath: d.objectKey(filePath)}] {
		if q.pos >= 0 {
			out = append(out, fillState{req: q.req, start: q.start, end: q.end, whole: q.whole,
				scans: slices.Clone(q.scans), joiners: q.joiners})
		}
	}
	return out
}

// setFillsMax sets the most background fills of d run at once.
func (d *DiskCache) setFillsMax(n int) {
	f := &d.prefetchObjects
	f.mu.Lock()
	defer f.mu.Unlock()
	f.fillsMax = n
}

// busyStorage serves the object busy itself once gate is closed, and the other objects
// from its upstream.
type busyStorage struct {
	ObjectStorage
	key     string
	gate    chan struct{}
	reading chan struct{}
	once    sync.Once
}

func (b *busyStorage) Read(ctx context.Context, key string, min, max *int64) (io.ReadCloser, error) {
	if key != b.key {
		return b.ObjectStorage.Read(ctx, key, min, max)
	}
	b.once.Do(func() { close(b.reading) })
	select {
	case <-b.gate:
	case <-ctx.Done():
		return nil, ctx.Err()
	}
	return io.NopCloser(bytes.NewReader([]byte{1})), nil
}

// busyFills lets fs run one background fill at once, and runs one, of the object "busy",
// whose read of object storage waits until the returned function or the test's end.
func busyFills(t *testing.T, fs *S3FS) func() {
	b := &busyStorage{ObjectStorage: fs.governor.raw, key: fs.pathToKey("busy"),
		gate: make(chan struct{}), reading: make(chan struct{})}
	fs.governor.raw = b
	fs.diskCache.setFillsMax(1)
	require.NoError(t, fs.PrefetchObject(context.Background(), ObjectPrefetch{FilePath: "busy", Size: 1,
		Ranges: []PrefetchRange{{Offset: 0, Size: 1}}}))
	select {
	case <-b.reading:
	case <-time.After(5 * time.Second):
		t.Fatal("the busy fill did not start")
	}
	release := sync.OnceFunc(func() { close(b.gate) })
	t.Cleanup(release)
	return release
}

// waitFillsIdle waits until d has no fill waiting or running and no prefetch object.
func waitFillsIdle(t *testing.T, d *DiskCache) {
	t.Helper()
	f := &d.prefetchObjects
	require.Eventually(t, func() bool {
		f.mu.Lock()
		defer f.mu.Unlock()
		return len(f.fills) == 0 && len(f.waitingFills) == 0 && f.backgroundFills == 0 && len(f.m) == 0
	}, 10*time.Second, time.Millisecond)
}

// prefetchRanges queues the fills of ranges of the object, read through open when it is
// set, and waits until they end.
func prefetchRanges(fs *S3FS, ranges []PrefetchRange, open func(ctx context.Context, start, end int64) (io.ReadCloser, error)) {
	if open != nil {
		fs.governor.raw = openerStorage{ObjectStorage: fs.governor.raw, open: open}
	}
	var size int64
	for _, r := range ranges {
		size += r.Size
	}
	_ = fs.PrefetchObject(context.Background(), ObjectPrefetch{FilePath: "object", Size: size, Ranges: ranges})
	waitFillsEnd(fs.diskCache, "object")
}

// waitObjectsEnd waits until no prefetch object of the object at filePath is registered.
func waitObjectsEnd(d *DiskCache, filePath string) {
	f := &d.prefetchObjects
	diskPath := d.objectKey(filePath)
	for deadline := time.Now().Add(30 * time.Second); time.Now().Before(deadline); time.Sleep(time.Millisecond) {
		f.mu.Lock()
		n := len(f.m[diskPath])
		f.mu.Unlock()
		if n == 0 {
			return
		}
	}
}

// keyRecorder records the key of each read of object storage.
type keyRecorder struct {
	ObjectStorage
	mu   sync.Mutex
	keys []string
}

func (k *keyRecorder) Read(ctx context.Context, key string, min, max *int64) (io.ReadCloser, error) {
	k.mu.Lock()
	k.keys = append(k.keys, key)
	k.mu.Unlock()
	return k.ObjectStorage.Read(ctx, key, min, max)
}

// The waiting fills start one at a time in order: a larger share first, an unknown share as
// the whole, then the sooner due time, then arrival.
func TestFillsStartInOrder(t *testing.T) {
	fs, _, data := newPrefetchRangeFS(t, 0, 4<<10)
	ctx := context.Background()
	names := []string{"partial", "late", "unknown", "first", "second"}
	for _, name := range names {
		require.NoError(t, fs.Write(ctx, IOVector{FilePath: name, Entries: []IOEntry{{Size: 1 << 10, Data: data[:1<<10]}},
			Policy: SkipDiskCache | SkipMemoryCache}))
	}
	release := busyFills(t, fs)
	rec := &keyRecorder{ObjectStorage: fs.governor.raw}
	fs.governor.raw = rec
	add := func(name string, due int64, reads, blocks int) {
		require.NoError(t, fs.PrefetchObject(ctx, ObjectPrefetch{FilePath: name, Size: 1 << 10, Due: due, Reads: reads, Blocks: blocks,
			Ranges: []PrefetchRange{{Offset: 0, Size: 1 << 10}}}))
	}
	add("partial", 1, 1, 10)
	add("late", 30, 10, 10)
	add("unknown", 20, 0, 10)
	add("first", 10, 10, 10)
	add("second", 10, 10, 10)
	release()
	for _, name := range names {
		waitFillsEnd(fs.diskCache, name)
	}
	want := []string{"first", "second", "unknown", "late", "partial"}
	for i := range want {
		want[i] = fs.pathToKey(want[i])
	}
	rec.mu.Lock()
	defer rec.mu.Unlock()
	require.Equal(t, want, rec.keys)
}
