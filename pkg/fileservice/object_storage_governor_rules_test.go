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
	"iter"
	"slices"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/matrixorigin/matrixone/pkg/common/moerr"
	"github.com/matrixorigin/matrixone/pkg/fileservice/fscache"
	"github.com/matrixorigin/matrixone/pkg/util/toml"
	"github.com/stretchr/testify/require"
)

// finishes runs f, failing the test when it does not return within 20s.
func finishes(t *testing.T, f func()) {
	t.Helper()
	done := make(chan struct{})
	go func() {
		defer close(done)
		f()
	}()
	select {
	case <-done:
	case <-time.After(20 * time.Second):
		t.Fatal("hold and wait: the call did not return")
	}
}

// openCountingStorage counts the requests open on its upstream at once: a read's from its
// call until its body ends or closes, any other request during its call. Each request
// takes delay first, and a read of a key gated accepts waits until gate is closed.
type openCountingStorage struct {
	ObjectStorage
	delay time.Duration
	gate  chan struct{}
	gated func(key string) bool

	cur, most, total atomic.Int64
	stats            atomic.Int64
}

func (o *openCountingStorage) open() func() {
	n := o.cur.Add(1)
	o.total.Add(1)
	for most := o.most.Load(); n > most && !o.most.CompareAndSwap(most, n); most = o.most.Load() {
	}
	time.Sleep(o.delay)
	return sync.OnceFunc(func() { o.cur.Add(-1) })
}

func (o *openCountingStorage) Read(ctx context.Context, key string, min, max *int64) (io.ReadCloser, error) {
	closed := o.open()
	if o.gated != nil && o.gated(key) {
		select {
		case <-o.gate:
		case <-ctx.Done():
			closed()
			return nil, ctx.Err()
		}
	}
	r, err := o.ObjectStorage.Read(ctx, key, min, max)
	if err != nil {
		closed()
		return nil, err
	}
	return &readCloser{
		r: readerFunc(func(b []byte) (int, error) {
			n, err := r.Read(b)
			if err != nil {
				closed()
			}
			return n, err
		}),
		closeFunc: func() error {
			defer closed()
			return r.Close()
		},
	}, nil
}

func (o *openCountingStorage) Write(ctx context.Context, key string, r io.Reader, size *int64, expire *time.Time) error {
	defer o.open()()
	return o.ObjectStorage.Write(ctx, key, r, size, expire)
}

func (o *openCountingStorage) Stat(ctx context.Context, key string) (int64, error) {
	o.stats.Add(1)
	defer o.open()()
	return o.ObjectStorage.Stat(ctx, key)
}

func (o *openCountingStorage) Exists(ctx context.Context, key string) (bool, error) {
	defer o.open()()
	return o.ObjectStorage.Exists(ctx, key)
}

func (o *openCountingStorage) Delete(ctx context.Context, keys ...string) error {
	defer o.open()()
	return o.ObjectStorage.Delete(ctx, keys...)
}

func (o *openCountingStorage) List(ctx context.Context, prefix string) iter.Seq2[*DirEntry, error] {
	return func(yield func(*DirEntry, error) bool) {
		closed := o.open()
		defer func() { closed() }()
		for e, err := range o.ObjectStorage.List(ctx, prefix) {
			closed()
			if !yield(e, err) {
				return
			}
			closed = o.open()
		}
	}
}

// newRoomFS returns an S3FS on a disk object storage whose governor has room free for
// room requests, at most half of it, or one, for background requests, the rest held until
// the test ends, and objects of size bytes written.
func newRoomFS(t *testing.T, room int, objects []string, size int) (*S3FS, []byte) {
	ctx := context.Background()
	fs, err := NewS3FS(ctx, ObjectStorageArguments{Name: "s3", Endpoint: "disk", Bucket: t.TempDir()}, CacheConfig{
		DiskPath: ptrTo(t.TempDir()), DiskCapacity: ptrTo[toml.ByteSize](256 << 20),
		MemoryCapacity: ptrTo[toml.ByteSize](0),
	}, nil, false, false)
	require.NoError(t, err)
	t.Cleanup(func() { fs.Close(ctx) })
	<-fs.diskCache.load.done
	data := make([]byte, size)
	for i := range data {
		data[i] = byte(i*7 + i/1021)
	}
	for _, name := range objects {
		require.NoError(t, fs.Write(ctx, IOVector{FilePath: name, Entries: []IOEntry{{Size: int64(len(data)), Data: data}}, Policy: SkipAllCache}))
	}
	limitRoom(t, fs.governor, room, max(room/2, 1))
	return fs, data
}

// newGovernedFS is newRoomFS whose requests are counted.
func newGovernedFS(t *testing.T, room int, objects []string, size int) (*S3FS, *openCountingStorage, []byte) {
	fs, data := newRoomFS(t, room, objects, size)
	counting := &openCountingStorage{ObjectStorage: fs.governor.raw, gate: make(chan struct{})}
	fs.governor.raw = counting
	return fs, counting, data
}

func names(prefix string, n int) []string {
	out := make([]string, n)
	for i := range out {
		out[i] = fmt.Sprintf("%s-%d", prefix, i)
	}
	return out
}

// openStream returns the stream of the bytes of filePath from offset, size bytes or to the
// end when size is -1.
func openStream(t *testing.T, ctx context.Context, fs FileService, filePath string, offset, size int64) io.ReadCloser {
	var r io.ReadCloser
	vec := &IOVector{FilePath: filePath, Policy: SkipAllCache, Entries: []IOEntry{{Offset: offset, Size: size, ReadCloserForRead: &r}}}
	require.NoError(t, fs.Read(ctx, vec))
	return r
}

// otherRequests makes a read, a write, a listing and a stat on fs.
func otherRequests(t *testing.T, ctx context.Context, fs FileService, data []byte, name string) {
	vec := &IOVector{FilePath: "b", Policy: SkipAllCache, Entries: []IOEntry{{Offset: 3, Size: 10}}}
	require.NoError(t, fs.Read(ctx, vec))
	require.Equal(t, data[3:13], vec.Entries[0].Data)
	require.NoError(t, fs.Write(ctx, IOVector{FilePath: name, Entries: []IOEntry{{Size: 4, Data: data[:4]}}}))
	for _, err := range fs.List(ctx, "") {
		require.NoError(t, err)
	}
	_, err := fs.StatFile(ctx, "b")
	require.NoError(t, err)
}

// Probe 1: a caller holding a stream unread makes other requests, then reads the stream.
func TestGovernorRuleStreamHeldWhileOtherRequests(t *testing.T) {
	for _, c := range []struct {
		name         string
		offset, size int64
	}{{"unknown size", 0, -1}, {"known size", 100, 200 << 10}, {"small", 7, 100}} {
		t.Run(c.name, func(t *testing.T) {
			fs, data := newRoomFS(t, 1, []string{"a", "b"}, 300<<10)
			fs.diskCache.memoryPrefetchMax = 64 << 10
			ctx := context.Background()
			finishes(t, func() {
				r := openStream(t, ctx, fs, "a", c.offset, c.size)
				otherRequests(t, ctx, fs, data, "new-"+strings.ReplaceAll(c.name, " ", "-"))
				got, err := io.ReadAll(r)
				require.NoError(t, err)
				want := data[c.offset:]
				if c.size > 0 {
					want = data[c.offset : c.offset+c.size]
				}
				require.Equal(t, want, got)
				require.NoError(t, r.Close())
			})
			require.Zero(t, heldRoom(fs.governor))
		})
	}
}

// streamCopiedIntoWrite copies a stream of a into a write of the object copy on fs, and
// reads copy back.
func streamCopiedIntoWrite(t *testing.T, fs FileService, data []byte) {
	ctx := context.Background()
	finishes(t, func() {
		r := openStream(t, ctx, fs, "a", 0, -1)
		require.NoError(t, fs.Write(ctx, IOVector{FilePath: "copy", Entries: []IOEntry{{Size: -1, ReaderForWrite: r}}}))
		require.NoError(t, r.Close())
		vec := &IOVector{FilePath: "copy", Policy: SkipAllCache, Entries: []IOEntry{{Size: -1}}}
		require.NoError(t, fs.Read(ctx, vec))
		require.Equal(t, data, vec.Entries[0].Data)
	})
}

// Probe 2: a stream of an object is the body of a write of another on the same file
// service, on the disk object storage, on object storage written through a temporary file,
// and on object storage written in parts.
func TestGovernorRuleStreamCopiedIntoWrite(t *testing.T) {
	t.Run("disk", func(t *testing.T) {
		fs, data := newRoomFS(t, 1, []string{"a"}, 300<<10)
		streamCopiedIntoWrite(t, fs, data)
	})
	t.Run("temporary file", func(t *testing.T) {
		fs, _, data := newGovernedFS(t, 1, []string{"a"}, 300<<10)
		streamCopiedIntoWrite(t, fs, data)
	})
	t.Run("parts", func(t *testing.T) {
		fs, data := newRoomFS(t, 1, []string{"a"}, int(minMultipartPartSize)*2+17)
		prev := writePartSize
		writePartSize = minMultipartPartSize
		t.Cleanup(func() { writePartSize = prev })
		storage := newPartStorage()
		parts := &partsOnDisk{ObjectStorage: fs.governor.raw, partStorage: storage}
		fs.governor.raw = parts
		streamCopiedIntoWrite(t, fs, data)
		require.Len(t, storage.parts, 3)
	})
}

// partsOnDisk is object storage that reads from its ObjectStorage and writes objects in
// parts into it.
type partsOnDisk struct {
	ObjectStorage
	*partStorage
}

func (p *partsOnDisk) Exists(ctx context.Context, key string) (bool, error) {
	return p.ObjectStorage.Exists(ctx, key)
}

func (p *partsOnDisk) Write(ctx context.Context, key string, r io.Reader, size *int64, expire *time.Time) error {
	return p.ObjectStorage.Write(ctx, key, r, size, expire)
}

func (p *partsOnDisk) completeMultipart(ctx context.Context, key, uploadID string, parts []multipartPart) error {
	if err := p.partStorage.completeMultipart(ctx, key, uploadID, parts); err != nil {
		return err
	}
	size := int64(len(p.done))
	return p.ObjectStorage.Write(ctx, key, bytes.NewReader(p.done), &size, nil)
}

type writerFunc func([]byte) (int, error)

func (f writerFunc) Write(b []byte) (int, error) { return f(b) }

// Probe 3: the writer of a read makes another read.
func TestGovernorRuleWriterForReadReads(t *testing.T) {
	fs, data := newRoomFS(t, 1, []string{"a", "b"}, 300<<10)
	ctx := context.Background()
	for _, size := range []int64{200 << 10, -1} {
		finishes(t, func() {
			var got bytes.Buffer
			w := writerFunc(func(b []byte) (int, error) {
				vec := &IOVector{FilePath: "b", Policy: SkipAllCache, Entries: []IOEntry{{Offset: 1, Size: 5}}}
				if err := fs.Read(ctx, vec); err != nil {
					return 0, err
				}
				return got.Write(b)
			})
			vec := &IOVector{FilePath: "a", Policy: SkipAllCache, Entries: []IOEntry{{Offset: 10, Size: size, WriterForRead: w}}}
			require.NoError(t, fs.Read(ctx, vec))
			want := data[10:]
			if size > 0 {
				want = data[10 : 10+size]
			}
			require.Equal(t, want, got.Bytes())
		})
	}
	require.Zero(t, heldRoom(fs.governor))
}

// Probe 4: the decoder of a read makes another read.
func TestGovernorRuleToCacheDataReads(t *testing.T) {
	fs, data := newRoomFS(t, 1, []string{"a", "b"}, 300<<10)
	ctx := context.Background()
	for _, policy := range []Policy{SkipAllCache, SkipMemoryCache, 0} {
		finishes(t, func() {
			vec := &IOVector{FilePath: "a", Policy: policy, Entries: []IOEntry{{Offset: 10, Size: 100,
				ToCacheData: func(ctx context.Context, r io.Reader, b []byte, allocator CacheDataAllocator) (fscache.Data, error) {
					other := &IOVector{FilePath: "b", Policy: SkipAllCache, Entries: []IOEntry{{Offset: 2, Size: 3}}}
					if err := fs.Read(ctx, other); err != nil {
						return nil, err
					}
					return allocator.CopyToCacheData(ctx, b), nil
				}}}}
			require.NoError(t, fs.Read(ctx, vec))
			require.Equal(t, data[10:110], vec.Entries[0].CachedData.Bytes())
			vec.Release()
		})
	}
}

// Probe 5: the body of a listing's loop reads and stats each listed file.
func TestGovernorRuleListBodyReads(t *testing.T) {
	fs, data := newRoomFS(t, 1, names("dir/object", 3), 4<<10)
	ctx := context.Background()
	finishes(t, func() {
		n := 0
		for entry, err := range fs.List(ctx, "dir") {
			require.NoError(t, err)
			vec := &IOVector{FilePath: "dir/" + entry.Name, Policy: SkipAllCache, Entries: []IOEntry{{Offset: 1, Size: 8}}}
			require.NoError(t, fs.Read(ctx, vec))
			require.Equal(t, data[1:9], vec.Entries[0].Data)
			_, err = fs.StatFile(ctx, "dir/"+entry.Name)
			require.NoError(t, err)
			n++
		}
		require.Equal(t, 3, n)
	})
	require.Zero(t, heldRoom(fs.governor))
}

// Probe 6: a whole-object fill of unknown size learns its size from object storage in a
// request of its own before it downloads, with room for one request.
func TestGovernorRuleUnknownSizeFillStats(t *testing.T) {
	fs, counting, data := newGovernedFS(t, 1, []string{"object"}, 300<<10)
	fs.SetAsyncUpdate(false)
	ctx := context.Background()
	finishes(t, func() {
		vec := &IOVector{FilePath: "object", Policy: SkipMemoryCache | SkipFullFilePreloads, Entries: []IOEntry{{Offset: 0, Size: 100}}}
		require.NoError(t, fs.Read(ctx, vec))
		vec.Release()
		fs.diskCache.prefetchObjects.reads.Wait()
		require.True(t, fs.diskCache.ranges.startsAt0(fs.diskCache.objectKey("object")))
		_, sized := fs.diskCache.prefetched.size(fs.diskCache.objectKey("object"))
		require.False(t, sized)
		stats := counting.stats.Load()
		require.NoError(t, fs.PrefetchFile(ctx, "object"))
		require.Equal(t, stats+1, counting.stats.Load(), "it stats")
	})
	_, cached := fs.diskCache.ranges.covering(fs.diskCache.objectKey("object"), 0, int64(len(data)))
	require.True(t, cached)
	require.Equal(t, int64(1), counting.most.Load())
	require.Zero(t, heldRoom(fs.governor))
}

// Probe 6: split whole-object reads of many objects at once with room for one request
// cache them all: each part is its own request.
func TestGovernorRuleSplitFills(t *testing.T) {
	objects := names("object", 6)
	fs, counting, data := newGovernedFS(t, 1, objects, 300<<10+17)
	fs.diskCache.objectSplit = objectSplitConfig{parts: 3, min: 128 << 10}
	ctx := context.Background()
	for _, name := range objects {
		fs.diskCache.prefetched.store(fs.diskCache.objectKey(name), int64(len(data)))
	}
	finishes(t, func() {
		var wg sync.WaitGroup
		for _, name := range objects {
			wg.Add(1)
			go func() {
				defer wg.Done()
				vec := &IOVector{FilePath: name, Policy: SkipMemoryCache, Entries: []IOEntry{{Offset: 250 << 10, Size: 10}}}
				defer vec.Release()
				require.NoError(t, fs.Read(ctx, vec))
				require.Equal(t, data[250<<10:250<<10+10], vec.Entries[0].Data)
			}()
		}
		wg.Wait()
	})
	require.Equal(t, int64(1), counting.most.Load())
	for _, name := range objects {
		require.Eventually(t, func() bool {
			return fs.diskCache.cache.Contains(fs.diskCache.wholePath(name, int64(len(data))))
		}, 5*time.Second, time.Millisecond, "%s is cached", name)
	}
	require.Eventually(t, func() bool { return heldRoom(fs.governor) == 0 }, 5*time.Second, time.Millisecond)
}

// Reads with every policy, whole-object and range prefetches, writes, listings, stats and
// deletes made at once through the public entry points never have more requests open than
// the governor's room, and free all of it.
func TestGovernorProbePeakOpensWithinRoom(t *testing.T) {
	const room = 4
	objects := names("object", 12)
	fs, counting, data := newGovernedFS(t, room, objects, 300<<10)
	counting.delay = time.Millisecond
	fs.diskCache.objectSplit = objectSplitConfig{parts: 2, min: 128 << 10}
	ctx, cancel := context.WithTimeout(context.Background(), time.Minute)
	defer cancel()
	policies := []Policy{0, SkipMemoryCache, SkipAllCache, SkipMemoryCache | SkipFullFilePreloads, ColumnPrefetch | SkipFullFilePreloads | SkipMemoryCache}
	var wg sync.WaitGroup
	errs := make(chan error, 256)
	run := func(f func() error) {
		wg.Add(1)
		go func() {
			defer wg.Done()
			if err := f(); err != nil {
				errs <- err
			}
		}()
	}
	for i, name := range objects {
		if i%3 == 0 {
			require.NoError(t, fs.PrefetchObject(ctx, ObjectPrefetch{FilePath: name, Size: int64(len(data)), Scan: &PrefetchScan{}}))
		} else if i%3 == 1 {
			require.NoError(t, fs.PrefetchObject(ctx, ObjectPrefetch{FilePath: name, Size: int64(len(data)),
				Ranges: columnRanges(6, 40<<10, 2<<10), Scan: &PrefetchScan{}}))
		}
		for j, policy := range policies {
			offset := int64((i*37 + j*53) % (len(data) - 64))
			run(func() error {
				vec := &IOVector{FilePath: name, Policy: policy, Entries: []IOEntry{{Offset: offset, Size: 64}}}
				defer vec.Release()
				if err := fs.Read(ctx, vec); err != nil {
					return err
				}
				if !bytes.Equal(data[offset:offset+64], vec.Entries[0].Data) {
					return moerr.NewInternalErrorNoCtxf("wrong bytes of %s at %d", name, offset)
				}
				return nil
			})
		}
		run(func() error {
			var r io.ReadCloser
			if err := fs.Read(ctx, &IOVector{FilePath: name, Policy: SkipAllCache, Entries: []IOEntry{{Size: -1, ReadCloserForRead: &r}}}); err != nil {
				return err
			}
			defer r.Close()
			got, err := io.ReadAll(r)
			if err == nil && !bytes.Equal(data, got) {
				err = moerr.NewInternalErrorNoCtxf("wrong stream of %s", name)
			}
			return err
		})
		run(func() error {
			_ = fs.PrefetchFile(ctx, name)
			return nil
		})
		run(func() error {
			_, err := fs.StatFile(ctx, name)
			return err
		})
	}
	for i := range 6 {
		run(func() error {
			return fs.Write(ctx, IOVector{FilePath: fmt.Sprintf("new-%d", i), Entries: []IOEntry{{Size: 1 << 10, Data: data[:1<<10]}}})
		})
		run(func() error {
			for _, err := range fs.List(ctx, "") {
				if err != nil {
					return err
				}
			}
			return nil
		})
	}
	wg.Wait()
	close(errs)
	for err := range errs {
		require.NoError(t, err)
	}
	for _, name := range objects[:4] {
		require.NoError(t, fs.Delete(ctx, name))
	}
	fs.diskCache.prefetchObjects.reads.Wait()
	require.Eventually(t, func() bool {
		return !slices.ContainsFunc(objects, fs.diskCache.fillsUnderWay) && heldRoom(fs.governor) == 0
	}, 10*time.Second, time.Millisecond)
	require.Positive(t, counting.total.Load())
	require.LessOrEqual(t, counting.most.Load(), int64(room), "peak requests open")
	require.Zero(t, counting.cur.Load())
	_, waiting := fs.governor.counts()
	require.Zero(t, waiting[foregroundRequest]+waiting[backgroundRequest])
}

// With background downloads stalled on all the room background may hold and more waiting,
// a foreground read of every kind with a deadline of 2s returns its bytes.
func TestGovernorProbeBystanderForegroundRead(t *testing.T) {
	const room = 8
	objects := append(names("prefetch", 12), "foreground")
	fs, counting, data := newGovernedFS(t, room, objects, 300<<10)
	foregroundKey := fs.pathToKey("foreground")
	counting.gated = func(key string) bool { return key != foregroundKey }
	defer close(counting.gate)
	ctx := context.Background()
	for i, name := range objects[:12] {
		req := ObjectPrefetch{FilePath: name, Size: int64(len(data)), Scan: &PrefetchScan{}}
		if i%2 == 1 {
			req.Ranges = columnRanges(6, 40<<10, 2<<10)
		}
		require.NoError(t, fs.PrefetchObject(ctx, req))
		go func() { _ = fs.PrefetchFile(ctx, name) }()
	}
	require.Eventually(t, func() bool {
		running, waiting := fs.governor.counts()
		return running[backgroundRequest] == fs.governor.backgroundMax && waiting[backgroundRequest] > 0
	}, 5*time.Second, time.Millisecond, "background holds its half and more waits")
	for _, policy := range []Policy{0, SkipMemoryCache, SkipAllCache, SkipMemoryCache | SkipFullFilePreloads,
		ColumnPrefetch | SkipFullFilePreloads | SkipMemoryCache, SkipAllCache | ExactColumnsRead} {
		readCtx, cancel := context.WithTimeout(ctx, 2*time.Second)
		vec := &IOVector{FilePath: "foreground", Policy: policy, Entries: []IOEntry{{Offset: 100 << 10, Size: 64}}}
		require.NoError(t, fs.Read(readCtx, vec), "policy %v", policy)
		require.Equal(t, data[100<<10:100<<10+64], vec.Entries[0].Data)
		vec.Release()
		cancel()
	}
	running, _ := fs.governor.counts()
	require.LessOrEqual(t, running[backgroundRequest], fs.governor.backgroundMax)
}
