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
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/matrixorigin/matrixone/pkg/common/moerr"
	"github.com/stretchr/testify/require"
)

// probeStorage holds the first read from offset at until gate is closed, then answers it
// through answer, given the reader of object storage; it counts the other reads.
type probeStorage struct {
	ObjectStorage
	at     int64
	gate   chan struct{}
	answer func(io.ReadCloser) (io.ReadCloser, error)
	hit    atomic.Bool
	others atomic.Int64
}

func (s *probeStorage) Read(ctx context.Context, key string, from, to *int64) (io.ReadCloser, error) {
	if from != nil && *from == s.at && s.hit.CompareAndSwap(false, true) {
		select {
		case <-s.gate:
		case <-ctx.Done():
			return nil, ctx.Err()
		}
		r, err := s.ObjectStorage.Read(ctx, key, from, to)
		if err != nil {
			return nil, err
		}
		return s.answer(r)
	}
	s.others.Add(1)
	return s.ObjectStorage.Read(ctx, key, from, to)
}

type readCloser2 struct {
	io.Reader
	io.Closer
}

// failAfter returns a reader of n bytes of r that then fails with err.
func failAfter(n int64, err error) func(io.ReadCloser) (io.ReadCloser, error) {
	return func(r io.ReadCloser) (io.ReadCloser, error) {
		return readCloser2{Reader: io.MultiReader(io.LimitReader(r, n), failingReader{err}), Closer: r}, nil
	}
}

// shortBy returns a reader of r without its last n bytes.
func shortBy(r PrefetchRange, n int64) func(io.ReadCloser) (io.ReadCloser, error) {
	return func(rc io.ReadCloser) (io.ReadCloser, error) {
		return readCloser2{Reader: io.LimitReader(rc, r.Size-n), Closer: rc}, nil
	}
}

type failingReader struct{ err error }

func (r failingReader) Read([]byte) (int, error) { return 0, r.err }

// joinedColumnRead prefetches r of the object, held in s, and returns what a column read of
// r that joins the prefetch returns once s answers it.
func joinedColumnRead(t *testing.T, fileBacked bool, r PrefetchRange, answer func(io.ReadCloser) (io.ReadCloser, error)) ([]byte, []byte, error, *probeStorage) {
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	t.Cleanup(cancel)
	fs, storage, data := newPrefetchRangeFS(t, 0, 300<<10)
	<-fs.diskCache.load.done
	if fileBacked {
		fs.diskCache.memoryPrefetchMax = r.Size - 1
		fs.diskCache.downloadMaxExtraWait = time.Hour
	}
	s := &probeStorage{ObjectStorage: storage, at: r.Offset, gate: make(chan struct{}), answer: answer}
	fs.storage = s
	release := sync.OnceFunc(func() { close(s.gate) })
	t.Cleanup(release)
	require.NoError(t, fs.PrefetchObject(ctx, ObjectPrefetch{FilePath: "object", Size: r.Size, Ranges: []PrefetchRange{r}}))
	require.Eventually(t, s.hit.Load, 5*time.Second, time.Millisecond)
	result := make(chan error, 1)
	var got []byte
	go func() {
		b, err := readColumnRange(ctx, fs, r)
		got = b
		result <- err
	}()
	diskPath := fs.diskCache.pathForFile("object")
	require.Eventually(t, func() bool {
		if !fileBacked {
			return memoryWaiters(fs, diskPath) == 1
		}
		o, _ := fs.diskCache.fileDownload(diskPath, r.Offset, r.Offset+r.Size)
		if o == nil {
			return false
		}
		o.mu.Lock()
		defer o.mu.Unlock()
		return len(o.waiters) == 1
	}, 5*time.Second, time.Millisecond, "the read joins the prefetch")
	release()
	err := <-result
	waitPrefetcherIdle(t, fs)
	return got, data[r.Offset : r.Offset+r.Size], err, s
}

var joinedKinds = []struct {
	name       string
	fileBacked bool
}{{name: "memory"}, {name: "file", fileBacked: true}}

// BC2: a read whose joined prefetch object fails with an unexpected end of its source gets
// the error a read of its own reports.
func TestJoinedReadReportsUnexpectedEOFAsOwnRead(t *testing.T) {
	r := PrefetchRange{Offset: 40 << 10, Size: 4 << 10}
	for _, k := range joinedKinds {
		t.Run(k.name, func(t *testing.T) {
			_, _, err, _ := joinedColumnRead(t, k.fileBacked, r, failAfter(100, io.ErrUnexpectedEOF))
			require.True(t, moerr.IsMoErrCode(err, moerr.ErrUnexpectedEOF), "got %v", err)
		})
	}
}

var _ = bytes.Equal

// BC1: a read whose joined prefetch object finds the object gone fails as its own read
// would, without reading object storage again.
func TestJoinedReadFailsOnMissingObject(t *testing.T) {
	r := PrefetchRange{Offset: 40 << 10, Size: 4 << 10}
	for _, k := range joinedKinds {
		t.Run(k.name, func(t *testing.T) {
			_, _, err, s := joinedColumnRead(t, k.fileBacked, r, func(rc io.ReadCloser) (io.ReadCloser, error) {
				_ = rc.Close()
				return nil, moerr.NewFileNotFoundNoCtx("object")
			})
			require.True(t, moerr.IsMoErrCode(err, moerr.ErrFileNotFound), "got %v", err)
			require.Zero(t, s.others.Load(), "the read does not read the object again")
		})
	}
}
