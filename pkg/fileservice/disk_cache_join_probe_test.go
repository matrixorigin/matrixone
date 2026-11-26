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
	"slices"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

// jpRead is one range read of object storage.
type jpRead struct {
	key        string
	start, end int64 // end is -1 for a read to the end
}

// jpStall holds a range read after it delivered after bytes until release is closed, or
// ends it there, short, once short is set.
type jpStall struct {
	after   int64
	short   atomic.Bool
	release chan struct{}
	sent    atomic.Int64
}

// jpStorage records the range reads of object storage and holds those stall returns a
// stall for.
type jpStorage struct {
	ObjectStorage
	stall func(r jpRead) *jpStall

	mu    sync.Mutex
	reads []jpRead
}

func (s *jpStorage) Read(ctx context.Context, key string, from, to *int64) (io.ReadCloser, error) {
	r := jpRead{key: key, end: -1}
	if from != nil {
		r.start = *from
	}
	if to != nil {
		r.end = *to
	}
	s.mu.Lock()
	s.reads = append(s.reads, r)
	s.mu.Unlock()
	rc, err := s.ObjectStorage.Read(ctx, key, from, to)
	if err != nil {
		return nil, err
	}
	if s.stall == nil {
		return rc, nil
	}
	st := s.stall(r)
	if st == nil {
		return rc, nil
	}
	return &jpReader{r: rc, st: st, done: ctx.Done(), ctx: ctx}, nil
}

// readsOf returns the range reads of the object at key.
func (s *jpStorage) readsOf(key string) []jpRead {
	s.mu.Lock()
	defer s.mu.Unlock()
	var out []jpRead
	for _, r := range s.reads {
		if r.key == key {
			out = append(out, r)
		}
	}
	return out
}

// overlapping counts the range reads of key holding bytes of [lo, hi).
func (s *jpStorage) overlapping(key string, lo, hi int64) int {
	n := 0
	for _, r := range s.readsOf(key) {
		if r.start < hi && (r.end < 0 || lo < r.end) {
			n++
		}
	}
	return n
}

type jpReader struct {
	r    io.ReadCloser
	st   *jpStall
	done <-chan struct{}
	ctx  context.Context
}

func (g *jpReader) Read(p []byte) (int, error) {
	sent := g.st.sent.Load()
	if sent >= g.st.after {
		if g.st.short.Load() {
			return 0, io.EOF
		}
		select {
		case <-g.st.release:
		case <-g.done:
			return 0, g.ctx.Err()
		}
	} else if rest := g.st.after - sent; int64(len(p)) > rest {
		p = p[:rest]
	}
	n, err := g.r.Read(p)
	g.st.sent.Add(int64(n))
	return n, err
}

func (g *jpReader) Close() error { return g.r.Close() }

func newJPStall(after int64) *jpStall {
	return &jpStall{after: after, release: make(chan struct{})}
}

// jpFS returns a file service whose object "object" holds size bytes and "other" holds
// 64 KiB, read through a jpStorage.
func jpFS(t *testing.T, size int) (*S3FS, *jpStorage, []byte, []byte) {
	return jpUnitFS(t, 0, size)
}

// jpUnitFS is jpFS with prefetch ranges of unit bytes, none for 0.
func jpUnitFS(t *testing.T, unit int64, size int) (*S3FS, *jpStorage, []byte, []byte) {
	fs, _, data := newPrefetchRangeFS(t, unit, size)
	other := make([]byte, 64<<10)
	for i := range other {
		other[i] = byte(i * 13)
	}
	require.NoError(t, fs.Write(context.Background(), IOVector{FilePath: "other",
		Entries: []IOEntry{{Size: int64(len(other)), Data: other}}, Policy: SkipDiskCache | SkipMemoryCache}))
	s := &jpStorage{ObjectStorage: fs.server.raw}
	fs.server.raw = s
	return fs, s, data, other
}

type jpResult struct {
	data []byte
	err  error
}

// jpColumnRead reads r of the object at filePath as a column read under a deadline of
// timeout, in the background.
func jpColumnRead(fs *S3FS, filePath string, r PrefetchRange, timeout time.Duration) <-chan jpResult {
	done := make(chan jpResult, 1)
	go func() {
		ctx, cancel := context.WithTimeout(context.Background(), timeout)
		defer cancel()
		vec := &IOVector{FilePath: filePath, Policy: ColumnPrefetch | SkipFullFilePreloads | SkipMemoryCache,
			Entries: []IOEntry{{Offset: r.Offset, Size: r.Size}}}
		if err := fs.Read(ctx, vec); err != nil {
			done <- jpResult{err: err}
			return
		}
		got := slices.Clone(vec.Entries[0].Data)
		vec.Release()
		done <- jpResult{data: got}
	}()
	return done
}

func jpWait(t *testing.T, done <-chan jpResult, within time.Duration, what string) jpResult {
	t.Helper()
	select {
	case r := <-done:
		return r
	case <-time.After(within):
		t.Fatalf("%s did not return within %v", what, within)
		return jpResult{}
	}
}

// jpBystander reads the object "other" with a deadline of 2 s while a probe holds its
// stall, and requires its bytes.
func jpBystander(t *testing.T, fs *S3FS, other []byte) {
	t.Helper()
	r := PrefetchRange{Offset: 1000, Size: 100}
	got := jpWait(t, jpColumnRead(fs, "other", r, 2*time.Second), 3*time.Second, "the bystander read")
	require.NoError(t, got.err)
	require.Equal(t, other[r.Offset:r.Offset+r.Size], got.data)
}

// (a) A read of bytes a running background fill of the whole object writes late, behind
// a slow download, joins the fill: the object is read once. Control: a read of bytes the
// fill has written is served by them at once.
func TestJoinProbeReadJoinsSlowRunningWholeFill(t *testing.T) {
	const size = 4 << 20
	fs, s, data, other := jpFS(t, size)
	key := fs.pathToKey("object")
	st := newJPStall(512 << 10)
	releaseStall := sync.OnceFunc(func() { close(st.release) })
	defer releaseStall()
	var once sync.Once
	s.stall = func(r jpRead) *jpStall {
		var got *jpStall
		if r.key == key && r.start == 0 {
			once.Do(func() { got = st })
		}
		return got
	}
	require.NoError(t, fs.PrefetchObject(context.Background(), ObjectPrefetch{FilePath: "object", Size: size}))
	require.Eventually(t, func() bool { return st.sent.Load() >= st.after }, 5*time.Second, time.Millisecond)
	// The download's rate so far is its bytes over this wait.
	time.Sleep(200 * time.Millisecond)

	control := PrefetchRange{Offset: 1000, Size: 1 << 10}
	got := jpWait(t, jpColumnRead(fs, "object", control, 2*time.Second), 3*time.Second, "the control read")
	require.NoError(t, got.err)
	require.Equal(t, data[control.Offset:control.Offset+control.Size], got.data)
	require.Len(t, s.readsOf(key), 1, "the control read is served by the written bytes")

	r := PrefetchRange{Offset: 3 << 20, Size: 1 << 10}
	done := jpColumnRead(fs, "object", r, 10*time.Second)
	require.Never(t, func() bool { return len(done) > 0 }, 200*time.Millisecond, time.Millisecond,
		"the read waits for the fill it joined")
	require.Len(t, s.readsOf(key), 1, "the read does not read the bytes again")
	jpBystander(t, fs, other)
	releaseStall()
	got = jpWait(t, done, 5*time.Second, "the joined read")
	require.NoError(t, got.err)
	require.Equal(t, data[r.Offset:r.Offset+r.Size], got.data)
	probeWaitIdle(t, fs)
	require.Equal(t, 1, s.overlapping(key, r.Offset, r.Offset+r.Size), "the bytes are read once")
}

// (a) A read of bytes a running file-backed background fill of a range writes late joins
// it. Control: a read of bytes outside the fill reads them itself, at once.
func TestJoinProbeReadJoinsSlowRunningRangeFileFill(t *testing.T) {
	fs, s, data, other := jpFS(t, 6<<20)
	fs.diskCache.memoryPrefetchMax = 64 << 10
	key := fs.pathToKey("object")
	fill := PrefetchRange{Offset: 0, Size: 4 << 20}
	st := newJPStall(512 << 10)
	releaseStall := sync.OnceFunc(func() { close(st.release) })
	defer releaseStall()
	s.stall = func(r jpRead) *jpStall {
		if r.key == key && r.start == fill.Offset && r.end == fill.Offset+fill.Size {
			return st
		}
		return nil
	}
	require.NoError(t, fs.PrefetchObject(context.Background(), ObjectPrefetch{FilePath: "object", Size: fill.Size,
		Ranges: []PrefetchRange{fill}, Scan: &PrefetchScan{}}))
	require.Eventually(t, func() bool { return st.sent.Load() >= st.after }, 5*time.Second, time.Millisecond)
	time.Sleep(200 * time.Millisecond)

	control := PrefetchRange{Offset: 5 << 20, Size: 1 << 10}
	got := jpWait(t, jpColumnRead(fs, "object", control, 2*time.Second), 3*time.Second, "the control read")
	require.NoError(t, got.err)
	require.Equal(t, data[control.Offset:control.Offset+control.Size], got.data)
	require.Equal(t, 1, s.overlapping(key, control.Offset, control.Offset+control.Size))

	r := PrefetchRange{Offset: 3584 << 10, Size: 1 << 10}
	done := jpColumnRead(fs, "object", r, 10*time.Second)
	require.Never(t, func() bool { return len(done) > 0 }, 200*time.Millisecond, time.Millisecond,
		"the read waits for the fill it joined")
	jpBystander(t, fs, other)
	releaseStall()
	got = jpWait(t, done, 5*time.Second, "the joined read")
	require.NoError(t, got.err)
	require.Equal(t, data[r.Offset:r.Offset+r.Size], got.data)
	probeWaitIdle(t, fs)
	require.Equal(t, 1, s.overlapping(key, r.Offset, r.Offset+r.Size), "the bytes are read once")
}

// (b) A read of bytes two running fills and a gap between them hold joins both fills and
// reads only the gap. Control: a read of bytes neither fill holds reads them itself.
func TestJoinProbeReadJoinsTwoFillsAndFillsTheGap(t *testing.T) {
	fs, s, data, other := jpFS(t, 512<<10)
	key := fs.pathToKey("object")
	a, c := PrefetchRange{Offset: 0, Size: 64 << 10}, PrefetchRange{Offset: 128 << 10, Size: 64 << 10}
	stalls := map[int64]*jpStall{a.Offset: newJPStall(0), c.Offset: newJPStall(0)}
	release := sync.OnceFunc(func() {
		for _, st := range stalls {
			close(st.release)
		}
	})
	defer release()
	s.stall = func(r jpRead) *jpStall {
		for _, f := range []PrefetchRange{a, c} {
			if r.key == key && r.start == f.Offset && r.end == f.Offset+f.Size {
				return stalls[f.Offset]
			}
		}
		return nil
	}
	require.NoError(t, fs.PrefetchObject(context.Background(), ObjectPrefetch{FilePath: "object", Size: a.Size + c.Size,
		Ranges: []PrefetchRange{a, c}, Scan: &PrefetchScan{}}))
	require.Eventually(t, func() bool { return len(s.readsOf(key)) == 2 }, 5*time.Second, time.Millisecond)

	control := PrefetchRange{Offset: 300 << 10, Size: 1 << 10}
	got := jpWait(t, jpColumnRead(fs, "object", control, 2*time.Second), 3*time.Second, "the control read")
	require.NoError(t, got.err)
	require.Equal(t, data[control.Offset:control.Offset+control.Size], got.data)

	r := PrefetchRange{Offset: 0, Size: c.Offset + c.Size}
	done := jpColumnRead(fs, "object", r, 10*time.Second)
	require.Never(t, func() bool { return len(done) > 0 }, 200*time.Millisecond, time.Millisecond,
		"the read waits for the fills it joined")
	jpBystander(t, fs, other)
	release()
	got = jpWait(t, done, 5*time.Second, "the joined read")
	require.NoError(t, got.err)
	require.Equal(t, data[r.Offset:r.Offset+r.Size], got.data)
	probeWaitIdle(t, fs)
	require.Equal(t, 1, s.overlapping(key, a.Offset, a.Offset+a.Size), "the first fill's bytes are read once")
	require.Equal(t, 1, s.overlapping(key, c.Offset, c.Offset+c.Size), "the second fill's bytes are read once")
	require.Equal(t, 1, s.overlapping(key, a.Offset+a.Size, c.Offset), "the gap is read once")
	require.Contains(t, s.readsOf(key), jpRead{key: key, start: a.Offset + a.Size, end: c.Offset}, "only the gap is read")
}

// (b) A range read through prefetch ranges of bytes a running fill holds only part of a
// block of joins the fill for those bytes and reads only the rest of the unit. Control: a
// read of a unit no fill holds reads the whole unit.
func TestJoinProbePrefetchRangeReadSplitsBlockAtFill(t *testing.T) {
	const unit = 64 << 10
	fs, s, data, other := jpUnitFS(t, unit, 512<<10)
	key := fs.pathToKey("object")
	fill := PrefetchRange{Offset: 10 << 10, Size: 10 << 10}
	st := newJPStall(0)
	releaseStall := sync.OnceFunc(func() { close(st.release) })
	defer releaseStall()
	s.stall = func(r jpRead) *jpStall {
		if r.key == key && r.start == fill.Offset && r.end == fill.Offset+fill.Size {
			return st
		}
		return nil
	}
	require.NoError(t, fs.PrefetchObject(context.Background(), ObjectPrefetch{FilePath: "object", Size: fill.Size,
		Ranges: []PrefetchRange{fill}, Scan: &PrefetchScan{}}))
	require.Eventually(t, func() bool { return len(s.readsOf(key)) == 1 }, 5*time.Second, time.Millisecond)

	read := func(r PrefetchRange, timeout time.Duration) <-chan jpResult {
		done := make(chan jpResult, 1)
		go func() {
			ctx, cancel := context.WithTimeout(context.Background(), timeout)
			defer cancel()
			vec := &IOVector{FilePath: "object", Policy: SkipFullFilePreloads | SkipMemoryCache,
				Entries: []IOEntry{{Offset: r.Offset, Size: r.Size}}}
			if err := fs.Read(ctx, vec); err != nil {
				done <- jpResult{err: err}
				return
			}
			got := slices.Clone(vec.Entries[0].Data)
			vec.Release()
			done <- jpResult{data: got}
		}()
		return done
	}
	control := PrefetchRange{Offset: 4*unit + 100, Size: 100}
	got := jpWait(t, read(control, 2*time.Second), 3*time.Second, "the control read")
	require.NoError(t, got.err)
	require.Equal(t, data[control.Offset:control.Offset+control.Size], got.data)
	require.Contains(t, s.readsOf(key), jpRead{key: key, start: 4 * unit, end: 5 * unit}, "the control read reads its unit")

	r := PrefetchRange{Offset: 0, Size: 30 << 10}
	done := read(r, 10*time.Second)
	require.Never(t, func() bool { return len(done) > 0 }, 200*time.Millisecond, time.Millisecond,
		"the read waits for the fill it joined")
	jpBystander(t, fs, other)
	releaseStall()
	got = jpWait(t, done, 5*time.Second, "the joined read")
	require.NoError(t, got.err)
	require.Equal(t, data[r.Offset:r.Offset+r.Size], got.data)
	probeWaitIdle(t, fs)
	require.Equal(t, 1, s.overlapping(key, fill.Offset, fill.Offset+fill.Size), "the fill's bytes are read once")
	require.Equal(t, 1, s.overlapping(key, 0, fill.Offset), "the bytes before the fill are read once")
	require.Equal(t, 1, s.overlapping(key, fill.Offset+fill.Size, r.Offset+r.Size), "the bytes after the fill are read once")
}

// (c) A joined fill that ends short of its range leaves the read to read its own bytes:
// it returns them. Control: the same read with the fill not short is served by the fill.
func TestJoinProbeJoinedFillEndsShort(t *testing.T) {
	for _, short := range []bool{false, true} {
		name := map[bool]string{false: "control", true: "short"}[short]
		t.Run(name, func(t *testing.T) {
			fs, s, data, other := jpFS(t, 300<<10)
			key := fs.pathToKey("object")
			fill := PrefetchRange{Offset: 40 << 10, Size: 1 << 10}
			st := newJPStall(0)
			releaseStall := sync.OnceFunc(func() { close(st.release) })
			defer releaseStall()
			var once sync.Once
			s.stall = func(r jpRead) *jpStall {
				var got *jpStall
				if r.key == key && r.start == fill.Offset {
					once.Do(func() { got = st })
				}
				return got
			}
			require.NoError(t, fs.PrefetchObject(context.Background(), ObjectPrefetch{FilePath: "object", Size: fill.Size,
				Ranges: []PrefetchRange{fill}, Scan: &PrefetchScan{}}))
			require.Eventually(t, func() bool { return len(s.readsOf(key)) == 1 }, 5*time.Second, time.Millisecond)
			done := jpColumnRead(fs, "object", fill, 10*time.Second)
			require.Never(t, func() bool { return len(done) > 0 }, 100*time.Millisecond, time.Millisecond,
				"the read waits for the fill it joined")
			require.Len(t, s.readsOf(key), 1)
			jpBystander(t, fs, other)
			st.short.Store(short)
			releaseStall()
			got := jpWait(t, done, 5*time.Second, "the joined read")
			require.NoError(t, got.err)
			require.Equal(t, data[fill.Offset:fill.Offset+fill.Size], got.data)
			probeWaitIdle(t, fs)
			if !short {
				require.Len(t, s.readsOf(key), 1, "the fill serves the read")
			}
		})
	}
}

// (c) A delete of the object stops the fill a read joined: the read returns at once, and
// a read of the object written again gets its new bytes. Control: a read of another object
// is not affected.
func TestJoinProbeJoinedFillDeleted(t *testing.T) {
	fs, s, _, other := jpFS(t, 300<<10)
	key := fs.pathToKey("object")
	fill := PrefetchRange{Offset: 40 << 10, Size: 1 << 10}
	st := newJPStall(0)
	releaseStall := sync.OnceFunc(func() { close(st.release) })
	defer releaseStall()
	var once sync.Once
	s.stall = func(r jpRead) *jpStall {
		var got *jpStall
		if r.key == key && r.start == fill.Offset {
			once.Do(func() { got = st })
		}
		return got
	}
	ctx := context.Background()
	require.NoError(t, fs.PrefetchObject(ctx, ObjectPrefetch{FilePath: "object", Size: fill.Size,
		Ranges: []PrefetchRange{fill}, Scan: &PrefetchScan{}}))
	require.Eventually(t, func() bool { return len(s.readsOf(key)) == 1 }, 5*time.Second, time.Millisecond)
	done := jpColumnRead(fs, "object", fill, 10*time.Second)
	require.Never(t, func() bool { return len(done) > 0 }, 100*time.Millisecond, time.Millisecond)
	require.NoError(t, fs.Delete(ctx, "object"))
	jpWait(t, done, 2*time.Second, "the read of the deleted object")
	jpBystander(t, fs, other)

	fresh := make([]byte, 300<<10)
	for i := range fresh {
		fresh[i] = byte(i*3 + 1)
	}
	require.NoError(t, fs.Write(ctx, IOVector{FilePath: "object", Entries: []IOEntry{{Size: int64(len(fresh)), Data: fresh}},
		Policy: SkipDiskCache | SkipMemoryCache}))
	got := jpWait(t, jpColumnRead(fs, "object", fill, 2*time.Second), 3*time.Second, "the read of the new object")
	require.NoError(t, got.err)
	require.Equal(t, fresh[fill.Offset:fill.Offset+fill.Size], got.data)
}

// (d) A read that joins a waiting background fill makes it foreground: it runs while every
// background fill is held, before an earlier waiting background fill. Control: that
// earlier fill runs only once the background fills are free.
func TestJoinProbeJoinedWaitingFillRunsFirst(t *testing.T) {
	fs, s, data, _ := jpFS(t, 300<<10)
	releaseBusy := probeBusy(t, fs)
	key, otherKey := fs.pathToKey("object"), fs.pathToKey("other")
	ctx := context.Background()
	early := PrefetchRange{Offset: 0, Size: 1 << 10}
	require.NoError(t, fs.PrefetchObject(ctx, ObjectPrefetch{FilePath: "other", Size: early.Size, Due: 1,
		Ranges: []PrefetchRange{early}, Scan: &PrefetchScan{}}))
	late := PrefetchRange{Offset: 100 << 10, Size: 1 << 10}
	require.NoError(t, fs.PrefetchObject(ctx, ObjectPrefetch{FilePath: "object", Size: late.Size, Due: 100,
		Ranges: []PrefetchRange{late}, Scan: &PrefetchScan{}}))
	require.True(t, probeQueued(fs, "other"))
	require.True(t, probeQueued(fs, "object"))

	got := jpWait(t, jpColumnRead(fs, "object", late, 2*time.Second), 3*time.Second, "the joined read")
	require.NoError(t, got.err)
	require.Equal(t, data[late.Offset:late.Offset+late.Size], got.data)
	require.Len(t, s.readsOf(key), 1, "the joined fill serves the read")
	require.True(t, probeQueued(fs, "other"), "the earlier background fill still waits")
	require.Empty(t, s.readsOf(otherKey))
	releaseBusy()
	probeWaitIdle(t, fs)
	require.Len(t, s.readsOf(otherKey), 1)
}

// (e) A running fill of the object into the index-file cache, held, is not joined by a read
// of the disk cache, which reads its bytes itself; a running fill into the disk cache, held,
// is not joined by a read of the index-file cache. Control: each held fill still ends with
// its object cached.
func TestJoinProbeCachesNeverJoinEachOther(t *testing.T) {
	for _, held := range []string{"index", "disk"} {
		t.Run(held, func(t *testing.T) {
			fs, _, data := newIndexObjectFS(t, true)
			s := &jpStorage{ObjectStorage: fs.server.raw}
			fs.server.raw = s
			key := fs.pathToKey("object")
			st := newJPStall(0)
			releaseStall := sync.OnceFunc(func() { close(st.release) })
			defer releaseStall()
			var once sync.Once
			s.stall = func(r jpRead) *jpStall {
				var got *jpStall
				if r.key == key {
					once.Do(func() { got = st })
				}
				return got
			}
			ctx := context.Background()
			req := ObjectPrefetch{FilePath: "object", Size: int64(len(data))}
			readPolicy := Policy(IndexFileObject)
			if held == "index" {
				require.NoError(t, fs.PrefetchObject(WithFileServicePolicy(ctx, IndexFileObject), req))
				readPolicy = 0
			} else {
				require.NoError(t, fs.PrefetchObject(ctx, req))
			}
			require.Eventually(t, func() bool { return len(s.readsOf(key)) == 1 }, 5*time.Second, time.Millisecond)

			done := make(chan jpResult, 1)
			go func() {
				ctx, cancel := context.WithTimeout(ctx, 2*time.Second)
				defer cancel()
				vec := &IOVector{FilePath: "object", Policy: readPolicy | SkipFullFilePreloads | SkipMemoryCache, Entries: []IOEntry{{Offset: 100, Size: 10}}}
				if err := fs.Read(ctx, vec); err != nil {
					done <- jpResult{err: err}
					return
				}
				got := slices.Clone(vec.Entries[0].Data)
				vec.Release()
				done <- jpResult{data: got}
			}()
			got := jpWait(t, done, 3*time.Second, "the read of the other cache")
			require.NoError(t, got.err)
			require.Equal(t, data[100:110], got.data)
			require.GreaterOrEqual(t, len(s.readsOf(key)), 2, "the read reads its bytes itself")

			releaseStall()
			probeWaitIdle(t, fs)
			if held == "index" {
				require.Eventually(t, func() bool { return fs.indexObjectHeld("object") }, 5*time.Second, time.Millisecond)
			} else {
				require.Eventually(t, func() bool {
					return fs.diskCache.cache.Contains(fs.diskCache.wholePath("object", int64(len(data))))
				}, 5*time.Second, time.Millisecond)
			}
		})
	}
}
