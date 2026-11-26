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
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

// heldTailStorage returns the first head bytes of a read at once and the rest once
// proceed is closed.
type heldTailStorage struct {
	ObjectStorage
	head    int
	proceed chan struct{}
}

func (s *heldTailStorage) Read(ctx context.Context, path string, min, max *int64) (io.ReadCloser, error) {
	r, err := s.ObjectStorage.Read(ctx, path, min, max)
	if err != nil {
		return nil, err
	}
	return &heldTailReader{ReadCloser: r, left: s.head, proceed: s.proceed, ctx: ctx}, nil
}

type heldTailReader struct {
	io.ReadCloser
	left    int
	proceed chan struct{}
	ctx     context.Context
}

func (r *heldTailReader) Read(p []byte) (int, error) {
	if r.left <= 0 {
		select {
		case <-r.proceed:
		case <-r.ctx.Done():
			return 0, r.ctx.Err()
		}
		return r.ReadCloser.Read(p)
	}
	n, err := r.ReadCloser.Read(p[:min(len(p), r.left)])
	r.left -= n
	return n, err
}

// firstWriteWriter signals first once it is written to.
type firstWriteWriter struct {
	mu    sync.Mutex
	buf   bytes.Buffer
	once  sync.Once
	first chan struct{}
}

func (w *firstWriteWriter) Write(p []byte) (int, error) {
	w.once.Do(func() { close(w.first) })
	w.mu.Lock()
	defer w.mu.Unlock()
	return w.buf.Write(p)
}

func (w *firstWriteWriter) bytes() []byte {
	w.mu.Lock()
	defer w.mu.Unlock()
	return bytes.Clone(w.buf.Bytes())
}

func TestStreamedRangeReadWritesBeforeRangeArrives(t *testing.T) {
	const size = minDiskPrefetchMemoryMax + 1<<20
	for _, tc := range []struct {
		name   string
		writer bool
	}{
		{name: "read_closer"},
		{name: "writer_for_read", writer: true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			fs, _, data := newPrefetchRangeFS(t, 0, size)
			fs.diskCache.memoryPrefetchMax = minDiskPrefetchMemoryMax
			<-fs.diskCache.load.done
			storage := &heldTailStorage{ObjectStorage: fs.server.raw, head: 64 << 10, proceed: make(chan struct{})}
			fs.server.raw = storage
			release := sync.OnceFunc(func() { close(storage.proceed) })
			defer release()
			ctx, cancel := context.WithTimeout(context.Background(), time.Minute)
			defer cancel()

			w := &firstWriteWriter{first: make(chan struct{})}
			var rc io.ReadCloser
			entry := IOEntry{Offset: 0, Size: size}
			if tc.writer {
				entry.WriterForRead = w
			} else {
				entry.ReadCloserForRead = &rc
			}
			vector := &IOVector{FilePath: "object", Policy: ExactColumnsRead | SkipMemoryCache, Entries: []IOEntry{entry}}
			done := make(chan error, 1)
			copied := make(chan error, 1)
			go func() {
				err := fs.Read(ctx, vector)
				if err == nil && rc != nil {
					go func() {
						defer rc.Close()
						_, err := io.Copy(w, rc)
						copied <- err
					}()
				} else {
					copied <- nil
				}
				done <- err
			}()
			if tc.writer {
				// The range is read before the writer runs.
				require.Never(t, func() bool {
					select {
					case <-w.first:
						return true
					default:
						return false
					}
				}, 100*time.Millisecond, time.Millisecond)
			} else {
				select {
				case <-w.first:
				case <-time.After(10 * time.Second):
					t.Fatal("the streamed entry got no bytes before the rest of its range arrived")
				}
			}
			release()
			require.NoError(t, <-done)
			require.NoError(t, <-copied)
			require.Equal(t, data, w.bytes())
		})
	}
}
