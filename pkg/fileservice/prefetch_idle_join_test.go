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
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

// slowCancelReader stalls until its context ends, signals it, and returns only once
// proceed is closed, as an HTTP body does while its connection is torn down.
// slowCancelReader returns from Read only once its context is canceled and proceed is
// closed.
type slowCancelReader struct {
	ctx      context.Context
	canceled chan struct{}
	proceed  chan struct{}
}

func (r *slowCancelReader) Read(p []byte) (int, error) {
	<-r.ctx.Done()
	close(r.canceled)
	<-r.proceed
	return 0, r.ctx.Err()
}

// A background flight nobody waits for times out; a read that joins it before it
// finished, and waits on it, must not fail with the idle-timeout error.
// A read that joined a flight stopping for the source's idle timeout reads its own bytes:
// it gets no timeout error.
func TestJoinedReadOfIdleStoppedFlightReadsItsBytes(t *testing.T) {
	shortIdle(t, 50*time.Millisecond)
	fs, _, data := newPrefetchRangeFS(t, 0, 64<<10)
	canceled := make(chan struct{})
	proceed := make(chan struct{})
	first := true
	open := func(ctx context.Context, start, end int64) (io.ReadCloser, error) {
		if first {
			first = false
			return io.NopCloser(&slowCancelReader{ctx: ctx, canceled: canceled, proceed: proceed}), nil
		}
		return io.NopCloser(bytes.NewReader(data[start:end])), nil
	}
	bg := make(chan error, 1)
	go func() {
		_, err := fs.diskCache.readRanges(context.Background(), "object", fs.diskCache.deletes.now(), SkipMemoryCacheWrites|SkipFullFilePreloads|ColumnPrefetch,
			[]PrefetchRange{{Offset: 0, Size: 4096}}, open)
		bg <- err
	}()
	<-canceled // the idle timer fired: no read waits for the flight

	readErr := make(chan error, 1)
	go func() {
		vec := &IOVector{FilePath: "object", Policy: SkipFullFilePreloads | SkipMemoryCache,
			Entries: []IOEntry{{Offset: 100, Size: 10}}}
		readErr <- fs.Read(context.Background(), vec)
	}()
	diskPath := fs.diskCache.pathForFile("object")
	require.Eventually(t, func() bool {
		f := &fs.diskCache.prefetchObjects
		f.mu.Lock()
		defer f.mu.Unlock()
		for _, o := range f.m[diskPath] {
			if fl, ok := o.(*memoryPrefetchObject); ok {
				fl.idle.mu.Lock()
				w := fl.idle.waiting
				fl.idle.mu.Unlock()
				if w > 0 {
					return true
				}
			}
		}
		return false
	}, 5*time.Second, time.Millisecond, "the read joins and waits on the flight")
	close(proceed)
	<-bg
	require.NoError(t, <-readErr, "a read that waits on a flight must not fail with the idle timeout")
}
