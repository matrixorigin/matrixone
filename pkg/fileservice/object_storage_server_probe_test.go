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
	"net/http"
	"net/http/httptest"
	"runtime"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/matrixorigin/matrixone/pkg/util/toml"
	"github.com/stretchr/testify/require"
)

// Two file services that leave max-conns-per-host unset have pools of their own: with
// every connection of one held, a request of the other gets a connection at once.
// Control: a request of the saturated one waits.
func TestServerProbeOwnConnectionPools(t *testing.T) {
	release := make(chan struct{})
	var held atomic.Int64
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path == "/block" {
			held.Add(1)
			<-release
		}
		w.WriteHeader(http.StatusOK)
	}))
	defer server.Close()
	defer close(release)
	first := newHTTPClient(ObjectStorageArguments{})
	second := newHTTPClient(ObjectStorageArguments{})
	get := func(c *http.Client, path string, timeout time.Duration) error {
		ctx, cancel := context.WithTimeout(context.Background(), timeout)
		defer cancel()
		req, err := http.NewRequestWithContext(ctx, http.MethodGet, server.URL+path, nil)
		if err != nil {
			return err
		}
		resp, err := c.Do(req)
		if err != nil {
			return err
		}
		_, _ = io.Copy(io.Discard, resp.Body)
		return resp.Body.Close()
	}
	for range maxConnsPerHost {
		go func() { _ = get(first, "/block", time.Minute) }()
	}
	require.Eventually(t, func() bool { return held.Load() == int64(maxConnsPerHost) }, 10*time.Second, time.Millisecond)

	require.Error(t, get(first, "/ok", 200*time.Millisecond), "control: the saturated pool has no connection")
	start := time.Now()
	require.NoError(t, get(second, "/ok", 2*time.Second), "the other file service shares no connection")
	require.Less(t, time.Since(start), time.Second)
}

// orderStorage records the order in which reads of readKey and stats of statKey reach it.
type orderStorage struct {
	ObjectStorage
	readKey, statKey string
	mu               sync.Mutex
	order            []string
}

func (o *orderStorage) record(op string) {
	o.mu.Lock()
	o.order = append(o.order, op)
	o.mu.Unlock()
}

func (o *orderStorage) first() string {
	o.mu.Lock()
	defer o.mu.Unlock()
	if len(o.order) == 0 {
		return ""
	}
	return o.order[0]
}

func (o *orderStorage) Read(ctx context.Context, key string, min, max *int64) (io.ReadCloser, error) {
	if key == o.readKey {
		o.record("read")
	}
	return o.ObjectStorage.Read(ctx, key, min, max)
}

func (o *orderStorage) Stat(ctx context.Context, key string) (int64, error) {
	if key == o.statKey {
		o.record("stat")
		return 0, nil
	}
	return o.ObjectStorage.Stat(ctx, key)
}

// With the room full and the memory full, a foreground read of a column range a fill
// held in memory would read, and a background request that needs only room, both wait;
// when one room frees, the foreground read is admitted first, and, its buffer not
// fitting, reads through a file fill. Control: with room free, both run at once.
func TestServerProbeOneOrderForRoom(t *testing.T) {
	const size = 16 << 10
	fs, _, data := newPrefetchRangeFS(t, 0, 300<<10)
	probeSetMemory(fs, flightBufSize(size))
	order := &orderStorage{ObjectStorage: probeRaw(fs), readKey: fs.pathToKey("object"), statKey: fs.pathToKey("bg")}
	probeSetRaw(fs, order)
	ctx := context.Background()

	r := PrefetchRange{Offset: 200 << 10, Size: size}
	got, err := probeRead(fs, r)
	require.NoError(t, err, "control")
	require.Equal(t, data[r.Offset:r.Offset+r.Size], got)
	require.NoError(t, probeStat(withBackgroundRequests(ctx), fs, "bg"), "control")
	order.mu.Lock()
	order.order = nil
	order.mu.Unlock()

	freeMemory := holdForegroundBuffer(t, fs, flightBufSize(size))
	defer freeMemory()
	frees := probeFillRoom(t, fs)
	defer func() {
		for _, free := range frees {
			free()
		}
	}()
	statDone := make(chan error, 1)
	go func() { statDone <- probeStat(withBackgroundRequests(ctx), fs, "bg") }()
	require.Eventually(t, func() bool { _, waiting := probeCounts(fs); return waiting[backgroundRequest] == 1 },
		5*time.Second, time.Millisecond)
	r = PrefetchRange{Offset: 100 << 10, Size: size}
	readDone := make(chan error, 1)
	go func() {
		ctx, cancel := context.WithTimeout(ctx, 10*time.Second)
		defer cancel()
		b, err := readColumnRange(ctx, fs, r)
		if err == nil && !bytes.Equal(b, data[r.Offset:r.Offset+r.Size]) {
			err = fmt.Errorf("wrong bytes")
		}
		readDone <- err
	}()
	waitedInOrder := false
	for deadline := time.Now().Add(time.Second); time.Now().Before(deadline) && !waitedInOrder; time.Sleep(time.Millisecond) {
		_, waiting := probeCounts(fs)
		waitedInOrder = waiting[foregroundRequest] == 1
	}
	frees[0]()
	require.Eventually(t, func() bool { return order.first() != "" }, 5*time.Second, time.Millisecond)
	require.Equal(t, "read", order.first(), "the foreground read is admitted before the background request")
	require.True(t, waitedInOrder, "the foreground read waits for room among the requests")
	for _, free := range frees {
		free()
	}
	require.NoError(t, <-readDone)
	require.NoError(t, <-statDone)
}

// The memory a file service holds never passes its budget while reads, streams and
// background prefetches run at once; with the whole budget held, a read whose range would
// be held in memory reads it through a file fill at once, never waiting.
func TestServerProbeMemoryWithinBudget(t *testing.T) {
	const unit = 64 << 10
	fs, _, data := newPrefetchRangeFS(t, unit, 4<<20)
	budget := 4 * flightBufSize(unit)
	probeSetMemory(fs, budget)
	var peak atomic.Int64
	stop := make(chan struct{})
	sampled := make(chan struct{})
	go func() {
		defer close(sampled)
		for {
			if n := probeMemoryHeld(fs); n > peak.Load() {
				peak.Store(n)
			}
			select {
			case <-stop:
				return
			default:
				runtime.Gosched()
			}
		}
	}()
	ctx := context.Background()
	var wg sync.WaitGroup
	errs := make(chan error, 64)
	for i := range 16 {
		wg.Add(1)
		go func() {
			defer wg.Done()
			off := int64(i) * 200 << 10
			vec := &IOVector{FilePath: "object", Policy: SkipFullFilePreloads | SkipMemoryCache,
				Entries: []IOEntry{{Offset: off + 100, Size: 10}, {Offset: off + 2*unit + 7, Size: 20}}}
			cctx, cancel := context.WithTimeout(ctx, 30*time.Second)
			defer cancel()
			err := fs.Read(cctx, vec)
			if err == nil && (!bytes.Equal(vec.Entries[0].Data, data[off+100:off+110]) || !bytes.Equal(vec.Entries[1].Data, data[off+2*unit+7:off+2*unit+27])) {
				err = fmt.Errorf("wrong bytes at %d", off)
			}
			vec.Release()
			errs <- err
		}()
	}
	for i := range 4 {
		wg.Add(1)
		go func() {
			defer wg.Done()
			var rc io.ReadCloser
			off := int64(i) * 300 << 10
			vec := &IOVector{FilePath: "object", Policy: SkipAllCache,
				Entries: []IOEntry{{Offset: off, Size: unit, ReadCloserForRead: &rc}}}
			err := fs.Read(ctx, vec)
			if err == nil {
				var b []byte
				b, err = io.ReadAll(rc)
				_ = rc.Close()
				if err == nil && !bytes.Equal(b, data[off:off+unit]) {
					err = fmt.Errorf("wrong stream bytes at %d", off)
				}
			}
			errs <- err
		}()
	}
	ranges := make([]PrefetchRange, 8)
	for i := range ranges {
		ranges[i] = PrefetchRange{Offset: int64(i)*400<<10 + 50<<10, Size: 32 << 10}
	}
	require.NoError(t, fs.PrefetchObject(ctx, ObjectPrefetch{FilePath: "object", Size: 4 << 20, Ranges: ranges, Scan: &PrefetchScan{}}))
	wg.Wait()
	close(errs)
	for err := range errs {
		require.NoError(t, err)
	}
	waitFillsIdle(t, fs.diskCache)
	fs.diskCache.prefetchObjects.reads.Wait()
	close(stop)
	<-sampled
	require.LessOrEqual(t, peak.Load(), budget, "the memory held never passes the budget")
	require.Positive(t, peak.Load(), "some memory was held")

	free := holdForegroundBuffer(t, fs, budget-probeMemoryHeld(fs))
	defer free()
	r := PrefetchRange{Offset: 3<<20 + 11, Size: 8 << 10}
	start := time.Now()
	got, err := probeRead(fs, r)
	require.NoError(t, err, "a read whose buffer does not fit waits for no memory")
	require.Less(t, time.Since(start), time.Second)
	require.Equal(t, data[r.Offset:r.Offset+r.Size], got)
}

// Open streams that hold the whole memory budget and writes whose caller readers block,
// pipes whose producers wait on a read of the same file service, block neither a read nor
// another write from a caller's reader: each completes at once, while the streams keep
// their memory. Control: the same read and write with no stream open and no write
// blocked.
func TestServerProbeCallerMemoryBlocksNothing(t *testing.T) {
	const size = 1 << 10
	fs, _, data := newPrefetchRangeFS(t, 0, 300<<10)
	probeSetRaw(fs, newProbeParts(probeRaw(fs)))
	prevPart := writePartSize
	writePartSize = defaultParallelMultipartPartSize
	t.Cleanup(func() { writePartSize = prevPart })
	probeSetMemory(fs, 2*flightBufSize(size))
	ctx := context.Background()
	var seq atomic.Int64
	probeOps := func() error {
		r := PrefetchRange{Offset: 200<<10 + seq.Load(), Size: 8 << 10}
		got, err := probeRead(fs, r)
		if err != nil {
			return fmt.Errorf("read: %w", err)
		}
		if !bytes.Equal(got, data[r.Offset:r.Offset+r.Size]) {
			return fmt.Errorf("read: wrong bytes")
		}
		wctx, cancel := context.WithTimeout(ctx, 2*time.Second)
		defer cancel()
		name := fmt.Sprintf("written-%d", seq.Add(1))
		if err := fs.Write(wctx, IOVector{FilePath: name, Policy: SkipAllCache,
			Entries: []IOEntry{{Size: -1, ReaderForWrite: bytes.NewReader(data)}}}); err != nil {
			return fmt.Errorf("write: %w", err)
		}
		return nil
	}
	require.NoError(t, probeOps(), "control")

	streams := openProbeStreams(t, fs, 2, size)
	defer func() {
		for _, s := range streams {
			_ = s.Close()
		}
	}()
	held := probeMemoryHeld(fs)
	gate := make(chan struct{})
	openGate := sync.OnceFunc(func() { close(gate) })
	defer openGate()
	blocked := runtime.NumCPU() + 1
	written := make(chan error, blocked)
	for i := range blocked {
		pr, pw := io.Pipe()
		go func() {
			<-gate
			r := PrefetchRange{Offset: int64(i) * 10 << 10, Size: 4 << 10}
			b, err := probeRead(fs, r)
			if err != nil {
				_ = pw.CloseWithError(err)
				return
			}
			_, err = pw.Write(b)
			_ = pw.CloseWithError(err)
		}()
		go func() {
			written <- fs.Write(ctx, IOVector{FilePath: fmt.Sprintf("blocked-%d", i), Policy: SkipAllCache,
				Entries: []IOEntry{{Size: -1, ReaderForWrite: pr}}})
		}()
	}
	time.Sleep(100 * time.Millisecond)
	start := time.Now()
	require.NoError(t, probeOps(), "a read and a write run while caller readers block")
	require.Less(t, time.Since(start), 2*time.Second)
	require.Equal(t, held, probeMemoryHeld(fs), "the streams keep their memory")
	openGate()
	for range blocked {
		require.NoError(t, <-written)
	}
}

// zeroReader reads zeros.
type zeroReader struct{}

func (zeroReader) Read(p []byte) (int, error) {
	clear(p)
	return len(p), nil
}

// allocated returns the bytes f allocates.
func allocated(f func()) uint64 {
	runtime.GC()
	var before, after runtime.MemStats
	runtime.ReadMemStats(&before)
	f()
	runtime.ReadMemStats(&after)
	return after.TotalAlloc - before.TotalAlloc
}

// A write of bytes the caller holds in memory sends them as they are: with one put, and as
// a multipart upload, it allocates far less than their size.
func TestServerProbeInMemoryWriteCopiesNothing(t *testing.T) {
	fs, _, _ := newPrefetchRangeFS(t, 0, 1)
	probeSetRaw(fs, newProbeParts(probeRaw(fs)))
	prevPart := writePartSize
	writePartSize = 8 << 20
	t.Cleanup(func() { writePartSize = prevPart })
	ctx := context.Background()
	for _, size := range []int{4 << 20, 32 << 20} {
		data := bytes.Repeat([]byte{7}, size)
		n := allocated(func() {
			require.NoError(t, fs.Write(ctx, IOVector{FilePath: fmt.Sprintf("in-memory-%d", size), Policy: SkipAllCache,
				Entries: []IOEntry{{Size: int64(size), Data: data}}}))
		})
		require.Less(t, n, uint64(size/4), "a write of %d bytes allocated %d", size, n)
	}
}

// A write from a caller's reader of many parts reuses part buffers: it allocates far less
// than a part for each part, less than one part for each part with the race detector,
// which drops some of the buffers put back.
func TestServerProbePartBuffersReused(t *testing.T) {
	fs, _, _ := newPrefetchRangeFS(t, 0, 1)
	probeSetRaw(fs, newProbeParts(probeRaw(fs)))
	probeSetMemory(fs, 1<<30)
	prevPart := writePartSize
	writePartSize = minMultipartPartSize
	t.Cleanup(func() { writePartSize = prevPart })
	const parts = 24
	ctx := context.Background()
	write := func(name string) {
		require.NoError(t, fs.Write(ctx, IOVector{FilePath: name, Policy: SkipAllCache,
			Entries: []IOEntry{{Size: -1, ReaderForWrite: io.LimitReader(zeroReader{}, parts*minMultipartPartSize)}}}))
	}
	write("warm")
	n := allocated(func() { write("parts") })
	most := uint64(minMultipartPartSize / 2)
	if raceEnabled {
		most = minMultipartPartSize
	}
	require.Less(t, n/parts, most, "allocated %d for %d parts", n, parts)
}

// object-storage-memory sets the memory budget of the server; unset, the budget is 5% of
// the memory available. A budget that shrinks below the memory in use takes nothing back,
// and new memory is taken only once it fits again.
func TestServerProbeMemoryBudgetConfig(t *testing.T) {
	var args ObjectStorageArguments
	require.NoError(t, args.SetFromString([]string{"object-storage-memory=3MiB"}))
	require.Equal(t, toml.ByteSize(3<<20), probeArgsMemory(args))
	newFS := func(args ObjectStorageArguments) *S3FS {
		args.Name, args.Endpoint, args.Bucket = "s3", "disk", t.TempDir()
		fs, err := NewS3FS(context.Background(), args, CacheConfig{
			DiskPath: ptrTo(t.TempDir()), DiskCapacity: ptrTo[toml.ByteSize](64 << 20), MemoryCapacity: ptrTo[toml.ByteSize](0),
		}, nil, false, false)
		require.NoError(t, err)
		t.Cleanup(func() { fs.Close(context.Background()) })
		return fs
	}
	require.Equal(t, int64(3<<20), probeMemoryBudget(newFS(args)))

	prev := memoryAvailable
	t.Cleanup(func() { memoryAvailable = prev })
	memoryAvailable = func() (uint64, bool) { return 1 << 30, true }
	fs := newFS(ObjectStorageArguments{})
	require.Equal(t, int64((1<<30)/100*5), probeMemoryBudget(fs), "the default is 5% of the memory available")

	buf := flightBufSize(64 << 10)
	probeSetMemory(fs, 4*buf)
	free := holdForegroundBuffer(t, fs, 4*buf)
	probeSetMemory(fs, buf)
	require.Equal(t, 4*buf, probeMemoryHeld(fs), "a shrunk budget takes nothing back")
	require.False(t, probeMemoryFits(fs, buf), "new memory waits for none and is refused")
	free()
	require.True(t, probeMemoryFits(fs, buf), "memory is taken once it fits")
}
