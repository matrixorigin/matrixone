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
	"io"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

// probeRead reads r of the object as a column read within a deadline of 2 s.
func probeRead(fs *S3FS, r PrefetchRange) ([]byte, error) {
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()
	return readColumnRange(ctx, fs, r)
}

// A column read of a range whose fill waits, while every background fill and all
// background room are held, joins the fill and is served by it: the object is read once.
// Control: a read of another range is read on its own.
func TestFillProbeReadJoinsWaitingFillWhileBackgroundRoomHeld(t *testing.T) {
	fs, _, data := newPrefetchRangeFS(t, 0, 300<<10)
	releaseBusy := probeBusy(t, fs)
	key := fs.pathToKey("object")
	g := newBlackBoxStorage(fs.server.raw, func(string, int64) bool { return false })
	fs.server.raw = g
	freeRoom := occupy(t, fs.server, backgroundRequest, fs.server.backgroundMax-1)
	defer freeRoom()
	r := PrefetchRange{Offset: 40 << 10, Size: 1 << 10}
	require.NoError(t, fs.PrefetchObject(context.Background(), ObjectPrefetch{FilePath: "object", Size: r.Size,
		Ranges: []PrefetchRange{r}, Scan: &PrefetchScan{}}))
	require.True(t, probeQueued(fs, "object"))

	got, err := probeRead(fs, r)
	require.NoError(t, err)
	require.Equal(t, data[r.Offset:r.Offset+r.Size], got)
	require.False(t, probeQueued(fs, "object"), "the read promoted the fill")
	freeRoom()
	releaseBusy()
	probeWaitIdle(t, fs)
	_, _, total := g.counts(key)
	require.Equal(t, 1, total, "the read is served by the fill it joined")

	control := PrefetchRange{Offset: 100 << 10, Size: 1 << 10}
	got, err = probeRead(fs, control)
	require.NoError(t, err)
	require.Equal(t, data[control.Offset:control.Offset+control.Size], got)
	_, _, total = g.counts(key)
	require.Equal(t, 2, total)
}

// A prefetch of bytes a running fill does not hold gets a fill of its own at once, read
// while the running fill still reads. Control: a prefetch of bytes the running fill holds
// reads nothing more.
func TestFillProbeOtherBytesOfRunningObjectStartAtOnce(t *testing.T) {
	fs, _, _ := newPrefetchRangeFS(t, 0, 300<<10)
	key := fs.pathToKey("object")
	g := newBlackBoxStorage(fs.server.raw, func(string, int64) bool { return true })
	fs.server.raw = g
	release := sync.OnceFunc(func() { close(g.gate) })
	defer release()
	ctx := context.Background()
	first := PrefetchRange{Offset: 0, Size: 1 << 10}
	require.NoError(t, fs.PrefetchObject(ctx, ObjectPrefetch{FilePath: "object", Size: first.Size, Ranges: []PrefetchRange{first}}))
	require.Eventually(t, func() bool { cur, _, _ := g.counts(key); return cur == 1 }, 5*time.Second, time.Millisecond)
	require.NoError(t, fs.PrefetchObject(ctx, ObjectPrefetch{FilePath: "object", Size: first.Size, Ranges: []PrefetchRange{first}}))
	require.Never(t, func() bool { cur, _, _ := g.counts(key); return cur > 1 }, 50*time.Millisecond, time.Millisecond,
		"bytes a running fill holds need nothing more")
	other := PrefetchRange{Offset: 100 << 10, Size: 1 << 10}
	require.NoError(t, fs.PrefetchObject(ctx, ObjectPrefetch{FilePath: "object", Size: other.Size, Ranges: []PrefetchRange{other}}))
	require.Eventually(t, func() bool { cur, _, _ := g.counts(key); return cur == 2 }, 2*time.Second, time.Millisecond,
		"other bytes of the object get a fill of their own at once")
	release()
	probeWaitIdle(t, fs)
	_, _, total := g.counts(key)
	require.Equal(t, 2, total)
}

// Two prefetches of the same bytes make one waiting fill with the sooner due time, the
// larger share and both scans: it stays live while one scan does, and is read once.
func TestFillProbeMergeOfSameBytes(t *testing.T) {
	fs, _, _ := newPrefetchRangeFS(t, 0, 300<<10)
	releaseBusy := probeBusy(t, fs)
	key := fs.pathToKey("object")
	g := newBlackBoxStorage(fs.server.raw, func(string, int64) bool { return false })
	fs.server.raw = g
	ctx := context.Background()
	r := PrefetchRange{Offset: 40 << 10, Size: 1 << 10}
	s1, s2 := &PrefetchScan{}, &PrefetchScan{}
	require.NoError(t, fs.PrefetchObject(ctx, ObjectPrefetch{FilePath: "object", Size: r.Size, Due: 30, Reads: 1, Blocks: 10,
		Ranges: []PrefetchRange{r}, Scan: s1}))
	require.NoError(t, fs.PrefetchObject(ctx, ObjectPrefetch{FilePath: "object", Size: r.Size, Due: 10, Reads: 5, Blocks: 10,
		Ranges: []PrefetchRange{r}, Scan: s2}))
	fills := probeWaiting(fs, "object")
	require.Len(t, fills, 1)
	require.Equal(t, int64(10), fills[0].due)
	require.Equal(t, 5, fills[0].share)
	require.ElementsMatch(t, []*PrefetchScan{s1, s2}, fills[0].scans)
	s1.End()
	releaseBusy()
	probeWaitIdle(t, fs)
	_, _, total := g.counts(key)
	require.Equal(t, 1, total, "the merged fill lives on its other scan and is read once")
}

// When the waiting background fills hold their most bytes, a new fill drops those whose
// scans ended first, then the last in order when it starts before it; one that starts
// after every fill, or is larger than the bound, is dropped and drops nothing.
func TestFillProbeByteBoundDropOrder(t *testing.T) {
	fs, _, _ := newPrefetchRangeFS(t, 0, 300<<10)
	probeBusy(t, fs)
	limit := probeQueueLimit(fs)
	q := limit / 4
	ctx := context.Background()
	add := func(name string, due int64, scan *PrefetchScan) {
		require.NoError(t, fs.PrefetchObject(ctx, ObjectPrefetch{FilePath: name, Size: q, Due: due,
			Ranges: []PrefetchRange{{Offset: 0, Size: q}}, Scan: scan}))
	}
	var scans []*PrefetchScan
	scan := func() *PrefetchScan {
		s := &PrefetchScan{}
		scans = append(scans, s)
		return s
	}
	defer func() {
		for _, s := range scans {
			s.End()
		}
	}()
	ended := scan()
	for _, f := range []struct {
		name string
		due  int64
		scan *PrefetchScan
	}{{"a", 10, scan()}, {"ended", 5, ended}, {"c", 20, scan()}, {"d", 25, scan()}} {
		add(f.name, f.due, f.scan)
	}
	ended.End()
	queued := func() []string {
		var out []string
		for _, name := range []string{"a", "ended", "c", "d", "e", "f", "g", "h"} {
			if probeQueued(fs, name) {
				out = append(out, name)
			}
		}
		return out
	}
	require.Equal(t, []string{"a", "ended", "c", "d"}, queued())
	add("e", 15, scan())
	require.Equal(t, []string{"a", "c", "d", "e"}, queued(), "the fill of an ended scan goes first")
	add("f", 30, scan())
	require.Equal(t, []string{"a", "c", "d", "e"}, queued(), "a fill after every fill is dropped")
	add("g", 1, scan())
	require.Equal(t, []string{"a", "c", "e", "g"}, queued(), "the last in order is dropped for one before it")
	require.NoError(t, fs.PrefetchObject(ctx, ObjectPrefetch{FilePath: "h", Size: limit + 1, Due: 0,
		Ranges: []PrefetchRange{{Offset: 0, Size: limit + 1}}, Scan: scan()}))
	require.Equal(t, []string{"a", "c", "e", "g"}, queued(), "a fill larger than the bound drops nothing")
}

// The end of a scan drops its waiting fill no read joined, which never reads; a fill a
// read joined after its scan ended is read and serves the read.
func TestFillProbeScanEndDropsUnjoinedFill(t *testing.T) {
	fs, _, data := newPrefetchRangeFS(t, 0, 300<<10)
	releaseBusy := probeBusy(t, fs)
	ctx := context.Background()
	unjoined, joined := PrefetchRange{Offset: 0, Size: 1 << 10}, PrefetchRange{Offset: 100 << 10, Size: 1 << 10}
	scan := &PrefetchScan{}
	require.NoError(t, fs.PrefetchObject(ctx, ObjectPrefetch{FilePath: "object", Size: 2 << 10,
		Ranges: []PrefetchRange{unjoined, joined}, Scan: scan}))
	starts := &startRecorder{ObjectStorage: fs.server.raw}
	fs.server.raw = starts
	scan.End()
	got, err := probeRead(fs, joined)
	require.NoError(t, err)
	require.Equal(t, data[joined.Offset:joined.Offset+joined.Size], got)
	releaseBusy()
	probeWaitIdle(t, fs)
	require.Equal(t, []int64{joined.Offset}, starts.offsets(), "the fill no read joined is dropped")
}

// startRecorder records the offset each range read of an object starts at.
type startRecorder struct {
	ObjectStorage
	mu     sync.Mutex
	starts []int64
}

func (s *startRecorder) Read(ctx context.Context, key string, min, max *int64) (io.ReadCloser, error) {
	var off int64
	if min != nil {
		off = *min
	}
	s.mu.Lock()
	s.starts = append(s.starts, off)
	s.mu.Unlock()
	return s.ObjectStorage.Read(ctx, key, min, max)
}

func (s *startRecorder) offsets() []int64 {
	s.mu.Lock()
	defer s.mu.Unlock()
	return append([]int64(nil), s.starts...)
}

// The buffers of memory fills never exceed the flight budget: background fills take at
// most half of it, and a foreground read gets a buffer and returns while they hold theirs.
// A read that joins a running background fill moves its buffer to the foreground charge.
func TestFillProbeFlightBytesWithinBudget(t *testing.T) {
	fs, _, data := newPrefetchRangeFS(t, 0, 300<<10)
	buf := flightBufSize(1 << 10)
	budget := 4 * buf
	fs.diskCache.prefetchFlightBytes = budget
	key := fs.pathToKey("object")
	g := newBlackBoxStorage(fs.server.raw, func(_ string, off int64) bool { return off < 200<<10 })
	fs.server.raw = g
	release := sync.OnceFunc(func() { close(g.gate) })
	defer release()
	var over atomic.Int64
	stop := make(chan struct{})
	sampled := make(chan struct{})
	go func() {
		defer close(sampled)
		for {
			select {
			case <-stop:
				return
			default:
			}
			if bytes, _ := probeFlightBytes(fs); bytes > budget {
				over.Store(bytes)
			}
			time.Sleep(50 * time.Microsecond)
		}
	}()
	ranges := columnRanges(6, 20<<10, 1<<10)
	require.NoError(t, fs.PrefetchObject(context.Background(), ObjectPrefetch{FilePath: "object", Size: 6 << 10,
		Ranges: ranges, Scan: &PrefetchScan{}}))
	require.Eventually(t, func() bool { cur, _, _ := g.counts(key); return cur == 2 }, 5*time.Second, time.Millisecond)
	require.Never(t, func() bool { cur, _, _ := g.counts(key); return cur > 2 }, 50*time.Millisecond, time.Millisecond,
		"background buffers take at most half the budget")
	bytes, background := probeFlightBytes(fs)
	require.Equal(t, 2*buf, bytes)
	require.Equal(t, 2*buf, background)

	bystander := PrefetchRange{Offset: 250 << 10, Size: 1 << 10}
	got, err := probeRead(fs, bystander)
	require.NoError(t, err, "a foreground read gets a buffer while background fills hold theirs")
	require.Equal(t, data[bystander.Offset:bystander.Offset+bystander.Size], got)

	joined := make(chan error, 1)
	go func() {
		_, err := probeRead(fs, ranges[0])
		joined <- err
	}()
	require.Eventually(t, func() bool { _, background := probeFlightBytes(fs); return background == buf }, 2*time.Second,
		time.Millisecond, "the joined fill's buffer moves to the foreground charge")
	release()
	require.NoError(t, <-joined)
	probeWaitIdle(t, fs)
	close(stop)
	<-sampled
	require.Zero(t, over.Load(), "the flight bytes went over the budget")
	bytes, background = probeFlightBytes(fs)
	require.Zero(t, bytes)
	require.Zero(t, background)
}

// A read that joins a waiting fill of a whole object large enough to split starts its
// first part at the block the read needs.
func TestFillProbeSplitStartsAtJoinedReadBlock(t *testing.T) {
	fs, _, data := newObjectSplitFS(t, objectSplitConfig{parts: 2, min: 128 << 10}, 300<<10)
	probeBusy(t, fs)
	starts := &startRecorder{ObjectStorage: fs.server.raw}
	fs.server.raw = starts
	require.NoError(t, fs.PrefetchObject(context.Background(), ObjectPrefetch{FilePath: "object", Size: int64(len(data))}))
	got, err := readFullObjectOf(fs, 100<<10, 10)
	require.NoError(t, err)
	require.Equal(t, data[100<<10:100<<10+10], got)
	require.Eventually(t, func() bool { return fs.diskCache.cache.Contains(fs.diskCache.wholePath("object", int64(len(data)))) },
		10*time.Second, time.Millisecond)
	offsets := starts.offsets()
	require.Len(t, offsets, 2)
	require.Equal(t, int64(64<<10), offsets[0], "the first part starts at the read's block")
}

// The fills of one object into the index-file cache and the disk cache are apart: a read
// of the disk cache joins only the disk cache's fill, and each cache is filled once.
func TestFillProbeIndexAndDiskFillsIndependent(t *testing.T) {
	fs, storage, data := newIndexObjectFS(t, true)
	releaseBusy := probeBusy(t, fs)
	ctx := context.Background()
	req := ObjectPrefetch{FilePath: "object", Size: int64(len(data))}
	require.NoError(t, fs.PrefetchObject(WithFileServicePolicy(ctx, IndexFileObject), req))
	require.NoError(t, fs.PrefetchObject(ctx, req))
	vec := &IOVector{FilePath: "object", Policy: SkipMemoryCache, Entries: []IOEntry{{Offset: 100, Size: 10}}}
	require.NoError(t, fs.Read(ctx, vec))
	require.Equal(t, data[100:110], vec.Entries[0].Data)
	vec.Release()
	require.True(t, probeQueued(fs, "object"), "the index-file cache's fill still waits")
	require.False(t, fs.indexObjectHeld("object"))
	releaseBusy()
	probeWaitIdle(t, fs)
	require.True(t, fs.indexObjectHeld("object"))
	require.True(t, fs.diskCache.cache.Contains(fs.diskCache.wholePath("object", int64(len(data)))))
	require.EqualValues(t, 2, storage.reads.Load(), "each cache is filled once")
}

// A delete of the object, or the closing of the file service, reaches its fills in each
// state: waiting, joined by a read and waiting for a buffer, and running. Afterwards no
// fill, buffer or room is held, and the joined read returns.
func TestFillProbeDeleteAndCloseReachEveryState(t *testing.T) {
	for _, event := range []string{"delete", "close"} {
		for _, state := range []string{"waiting", "promoted", "running"} {
			t.Run(event+"/"+state, func(t *testing.T) {
				fs, _, _ := newPrefetchRangeFS(t, 0, 300<<10)
				ctx := context.Background()
				r := PrefetchRange{Offset: 40 << 10, Size: 1 << 10}
				key := fs.pathToKey("object")
				g := newBlackBoxStorage(fs.server.raw, func(k string, _ int64) bool { return state == "running" && k == key })
				fs.server.raw = g
				defer sync.OnceFunc(func() { close(g.gate) })()
				joined := make(chan error, 1)
				freeBuffer := func() {}
				switch state {
				case "waiting":
					probeBusy(t, fs)
				case "promoted":
					probeBusy(t, fs)
					fs.diskCache.prefetchFlightBytes = 2 * flightBufSize(r.Size)
					freeBuffer = holdForegroundBuffer(t, fs, flightBufSize(r.Size))
				}
				baseRoom := heldRoom(fs.server)
				baseBytes, _ := probeFlightBytes(fs)
				require.NoError(t, fs.PrefetchObject(ctx, ObjectPrefetch{FilePath: "object", Size: r.Size,
					Ranges: []PrefetchRange{r}, Scan: &PrefetchScan{}}))
				switch state {
				case "waiting":
					require.True(t, probeQueued(fs, "object"))
				case "promoted":
					go func() {
						_, err := readColumnRange(ctx, fs, r)
						joined <- err
					}()
					require.Eventually(t, func() bool { return probeJoined(fs, "object") }, 2*time.Second, time.Millisecond)
				case "running":
					require.Eventually(t, func() bool { cur, _, _ := g.counts(key); return cur == 1 }, 5*time.Second, time.Millisecond)
				}
				if event == "delete" {
					require.NoError(t, fs.Delete(ctx, "object"))
				} else {
					fs.Close(ctx)
				}
				freeBuffer()
				if state == "promoted" {
					select {
					case <-joined:
					case <-time.After(2 * time.Second):
						t.Fatal("the joined read did not return")
					}
				}
				require.Eventually(t, func() bool {
					return !probeQueued(fs, "object") && !probeUnderWay(fs, "object")
				}, 2*time.Second, time.Millisecond)
				require.Eventually(t, func() bool {
					bytes, _ := probeFlightBytes(fs)
					return heldRoom(fs.server) <= baseRoom && bytes <= baseBytes
				}, 2*time.Second, time.Millisecond, "the fill holds no room and no buffer")
			})
		}
	}
}

// holdForegroundBuffer charges n bytes of the flight budget of fs, as a foreground flight
// does, until the returned function or the test's end.
func holdForegroundBuffer(t *testing.T, fs *S3FS, n int64) func() {
	f := &fs.diskCache.prefetchObjects
	f.mu.Lock()
	f.bytes += n
	f.mu.Unlock()
	free := sync.OnceFunc(func() { probeUncharge(fs, n) })
	t.Cleanup(free)
	return free
}
