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
	"errors"
	"fmt"
	"io"
	"math/rand/v2"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/matrixorigin/matrixone/pkg/common/moerr"
	"github.com/stretchr/testify/require"
)

// errStressInjected is the error of a read of object storage the stress storage fails.
var errStressInjected = errors.New("stress: injected storage failure")

// stressStorage delays reads of object storage by up to a millisecond, and fails a share
// of them, at open or part way through the body, while faults is set.
type stressStorage struct {
	ObjectStorage
	faults atomic.Bool
	seed   atomic.Uint64
}

func (s *stressStorage) rand() *rand.Rand {
	return rand.New(rand.NewPCG(s.seed.Add(1), 7))
}

func (s *stressStorage) Read(ctx context.Context, key string, min, max *int64) (io.ReadCloser, error) {
	r := s.rand()
	select {
	case <-time.After(time.Duration(r.IntN(1000)) * time.Microsecond):
	case <-ctx.Done():
		return nil, ctx.Err()
	}
	faults := s.faults.Load()
	if faults && r.IntN(20) == 0 {
		return nil, errStressInjected
	}
	rc, err := s.ObjectStorage.Read(ctx, key, min, max)
	if err != nil || !faults || r.IntN(20) != 0 {
		return rc, err
	}
	return &failingBody{ReadCloser: rc, left: r.IntN(64 << 10)}, nil
}

// failingBody fails after left bytes.
type failingBody struct {
	io.ReadCloser
	left int
}

func (b *failingBody) Read(p []byte) (int, error) {
	if b.left <= 0 {
		return 0, errStressInjected
	}
	if len(p) > b.left {
		p = p[:b.left]
	}
	n, err := b.ReadCloser.Read(p)
	b.left -= n
	return n, err
}

// stressContent is version v of object i, of size bytes.
func stressContent(i, v, size int) []byte {
	b := make([]byte, size)
	for j := range b {
		b[j] = byte(j*7 + j/509 + v*31 + i*13)
	}
	return b
}

// Workers read, prefetch, cancel, delete and rewrite objects at once through the public
// file-service calls, against few download connections, a tight buffer budget, small
// MemoryPrefetchObjects, split runs, and an object storage that delays and fails reads.
// Every read returns, with bytes of one version of its object or an error the workload
// explains; once the workers stop, no prefetch object, buffer, connection or prefetch
// stays, and every object reads its last version.
func TestPrefetchObjectsUnderStress(t *testing.T) {
	const (
		// A unit of four blocks makes runs of blocks a read takes no bytes of.
		unit    = 256 << 10
		objects = 6
		workers = 16
	)
	duration := 3 * time.Second
	if testing.Short() {
		duration = time.Second
	}
	sizes := []int{100<<10 + 3, 300 << 10, 700<<10 + 11, 1 << 20, 1500 << 10, 2<<20 + 5}
	fs, _, _ := newPrefetchRangeFS(t, unit, 1)
	d := fs.diskCache
	d.prefetchFlightBytes = 4 * prefetchBlockMax
	d.maxDownloadConns = 3
	d.memoryPrefetchMax = 256 << 10
	d.objectSplit = objectSplitConfig{parts: 2, min: 256 << 10}
	storage := &stressStorage{ObjectStorage: fs.storage}
	fs.storage = storage
	ctx := context.Background()

	// versions[i] is the newest version of object i written; a read may see any version
	// from the one current when it began on.
	var versions [objects]atomic.Int64
	var rewrite [objects]sync.Mutex
	name := func(i int) string { return fmt.Sprintf("object-%d", i) }
	write := func(i, v int) {
		require.NoError(t, fs.Write(ctx, IOVector{FilePath: name(i), Entries: []IOEntry{{Size: int64(sizes[i]), Data: stressContent(i, v, sizes[i])}},
			Policy: SkipDiskCache | SkipMemoryCache}))
	}
	for i := range objects {
		write(i, 0)
	}
	// matches reports whether data is bytes [off, off+len(data)) of a version of object i
	// from first on.
	matches := func(i, first int, off int64, data []byte) bool {
		for v := first; v <= int(versions[i].Load()); v++ {
			want := stressContent(i, v, sizes[i])[off : off+int64(len(data))]
			if string(want) == string(data) {
				return true
			}
		}
		return false
	}
	explained := func(err error) bool {
		return errors.Is(err, errStressInjected) || errors.Is(err, context.Canceled) ||
			errors.Is(err, context.DeadlineExceeded) || moerr.IsMoErrCode(err, moerr.ErrFileNotFound)
	}

	storage.faults.Store(true)
	stop := make(chan struct{})
	var wg sync.WaitGroup
	var failures atomic.Value
	fail := func(format string, args ...any) { failures.CompareAndSwap(nil, fmt.Sprintf(format, args...)) }
	var scans sync.Map
	for w := range workers {
		wg.Add(1)
		go func() {
			defer wg.Done()
			r := rand.New(rand.NewPCG(uint64(w), 11))
			for {
				select {
				case <-stop:
					return
				default:
				}
				// Half of the work is on one object, so reads and prefetches overlap.
				i := r.IntN(objects)
				if r.IntN(2) == 0 {
					i = 2
				}
				size := int64(sizes[i])
				switch op := r.IntN(100); {
				case op < 45:
					// A column read, or a read that may download the whole object, of
					// entries anywhere in the object, some under a short deadline.
					var policy Policy = SkipMemoryCache
					if op < 30 {
						policy |= ColumnPrefetch | SkipFullFilePreloads
					}
					entries := make([]IOEntry, 1+r.IntN(4))
					for e := range entries {
						n := 1 + r.Int64N(min(size, 200<<10))
						if r.IntN(2) == 0 {
							// A short entry across a block boundary.
							n = 1 + r.Int64N(256)
						}
						entries[e] = IOEntry{Offset: r.Int64N(size - n + 1), Size: n}
					}
					readCtx, cancel := context.WithCancel(ctx)
					if r.IntN(5) == 0 {
						readCtx, cancel = context.WithTimeout(ctx, time.Duration(r.IntN(3000))*time.Microsecond)
					}
					first := int(versions[i].Load())
					vec := &IOVector{FilePath: name(i), Policy: policy, Entries: entries}
					done := make(chan error, 1)
					go func() { done <- fs.Read(readCtx, vec) }()
					var err error
					select {
					case err = <-done:
					case <-time.After(diskCacheLifecycleTestTimeout):
						fail("a read of %s did not return", name(i))
						cancel()
						return
					}
					cancel()
					if err != nil {
						if !explained(err) {
							fail("read of %s: unexplained error %v", name(i), err)
						}
					} else {
						for e := range vec.Entries {
							if !matches(i, first, vec.Entries[e].Offset, vec.Entries[e].Data) {
								fail("read of %s at %d: bytes of no version", name(i), vec.Entries[e].Offset)
							}
						}
					}
					vec.Release()
				case op < 75:
					// A prefetch of ranges, or of the whole object, whose scan some
					// workers end soon after.
					scan := &PrefetchScan{}
					req := ObjectPrefetch{FilePath: name(i), Size: size, Scan: scan}
					if op < 65 {
						for range 1 + r.IntN(4) {
							n := 1 + r.Int64N(min(size, 600<<10))
							req.Ranges = append(req.Ranges, PrefetchRange{Offset: r.Int64N(size - n + 1), Size: n})
						}
					}
					if err := fs.PrefetchObject(ctx, req); err != nil {
						fail("prefetch of %s: %v", name(i), err)
					}
					if r.IntN(2) == 0 {
						scan.End()
					} else {
						scans.Store(scan, struct{}{})
					}
				case op < 80:
					// A delete of the object, then its next version.
					rewrite[i].Lock()
					if err := fs.Delete(ctx, name(i)); err != nil {
						fail("delete of %s: %v", name(i), err)
					}
					v := int(versions[i].Add(1))
					write(i, v)
					rewrite[i].Unlock()
				default:
					time.Sleep(time.Duration(r.IntN(500)) * time.Microsecond)
				}
			}
		}()
	}
	time.Sleep(duration)
	close(stop)
	wg.Wait()
	if msg := failures.Load(); msg != nil {
		t.Fatal(msg)
	}

	// Quiesce: end every scan, then nothing stays held.
	storage.faults.Store(false)
	scans.Range(func(k, _ any) bool {
		k.(*PrefetchScan).End()
		return true
	})
	require.Eventually(t, func() bool {
		fs.prefetcher.mu.Lock()
		defer fs.prefetcher.mu.Unlock()
		return len(fs.prefetcher.entries) == 0
	}, 10*time.Second, time.Millisecond, "every prefetch ends")
	f := &d.prefetchObjects
	f.reads.Wait()
	require.Eventually(t, func() bool {
		f.mu.Lock()
		defer f.mu.Unlock()
		return len(f.m) == 0 && f.bytes == 0
	}, 10*time.Second, time.Millisecond, "no prefetch object or buffer stays")
	require.Eventually(t, func() bool { return d.downloadConns.Load() == 0 }, 10*time.Second, time.Millisecond,
		"no connection stays held")

	// Every object reads its last version, whole and by columns.
	for i := range objects {
		want := stressContent(i, int(versions[i].Load()), sizes[i])
		for _, policy := range []Policy{SkipMemoryCache, ColumnPrefetch | SkipFullFilePreloads | SkipMemoryCache} {
			vec := &IOVector{FilePath: name(i), Policy: policy, Entries: []IOEntry{{Offset: 0, Size: int64(sizes[i])}}}
			require.NoError(t, fs.Read(ctx, vec))
			require.Equal(t, want, vec.Entries[0].Data, "%s reads its last version", name(i))
			vec.Release()
		}
	}
}
