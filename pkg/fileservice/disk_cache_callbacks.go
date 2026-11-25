// Copyright 2022 Matrix Origin
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
	"time"
)

type DiskCacheCallbacks struct {
	OnWritten   []OnDiskCacheWrittenFunc
	OnEvict     []OnDiskCacheEvictFunc
	upstreamCtx context.Context
}

// OnDiskCacheWrittenFunc runs after a cache entry is published. Async disk-cache
// callbacks preserve submission order but are outside the Flush and Close
// completion barriers; their retained entry data remains charged to async
// admission until the callback returns, and an update that only callbacks
// returning can admit is dropped. A panic is logged and counted at the
// async goroutine boundary; later callbacks in the ordered chain still run.
type OnDiskCacheWrittenFunc = func(
	filePath string,
	entry IOEntry,
)

type OnDiskCacheEvictFunc = func(
	diskFilePath string,
)

type ctxKeyDiskCacheCallbacks struct{}

var CtxKeyDiskCacheCallbacks ctxKeyDiskCacheCallbacks

func OnDiskCacheWritten(ctx context.Context, fn OnDiskCacheWrittenFunc) (ret context.Context) {
	var callbacks *DiskCacheCallbacks
	v := ctx.Value(CtxKeyDiskCacheCallbacks)
	if v == nil {
		callbacks = new(DiskCacheCallbacks)
		callbacks.upstreamCtx = ctx
		ret = callbacks
	} else {
		callbacks = v.(*DiskCacheCallbacks)
		ret = ctx
	}
	if fn != nil {
		callbacks.OnWritten = append(callbacks.OnWritten, fn)
	}
	return
}

func OnDiskCacheEvict(ctx context.Context, fn OnDiskCacheEvictFunc) (ret context.Context) {
	var callbacks *DiskCacheCallbacks
	v := ctx.Value(CtxKeyDiskCacheCallbacks)
	if v == nil {
		callbacks = new(DiskCacheCallbacks)
		callbacks.upstreamCtx = ctx
		ret = callbacks
	} else {
		callbacks = v.(*DiskCacheCallbacks)
		ret = ctx
	}
	if fn != nil {
		callbacks.OnEvict = append(callbacks.OnEvict, fn)
	}
	return
}

var _ context.Context = new(DiskCacheCallbacks)

func (d *DiskCacheCallbacks) Deadline() (deadline time.Time, ok bool) {
	return d.upstreamCtx.Deadline()
}

func (d *DiskCacheCallbacks) Done() <-chan struct{} {
	return d.upstreamCtx.Done()
}

func (d *DiskCacheCallbacks) Err() error {
	return d.upstreamCtx.Err()
}

func (d *DiskCacheCallbacks) Value(key any) any {
	if key == CtxKeyDiskCacheCallbacks {
		return d
	}
	return d.upstreamCtx.Value(key)
}

// deferWriteCallbacks returns ctx, whose write callbacks record copies of the entries
// written instead of running; reserve, which reports whether a copy of n bytes, counted
// as a flight buffer of n bytes, fits in the copies d keeps, and reserves it; and run,
// which runs ctx's write callbacks on the copies, containing their panics, and releases
// them.
func (d *DiskCache) deferWriteCallbacks(ctx context.Context) (_ context.Context, reserve func(n int64) bool, run func()) {
	callbacks, _ := ctx.Value(CtxKeyDiskCacheCallbacks).(*DiskCacheCallbacks)
	if callbacks == nil || len(callbacks.OnWritten) == 0 {
		return ctx, func(int64) bool { return true }, func() {}
	}
	var entries []writtenEntry
	var reserved int64
	copies := &d.async.flightCopies
	reserve = func(n int64) bool {
		n = flightBufSize(n)
		for {
			cur := copies.Load()
			if cur+n > d.async.flightCopiesMax {
				return false
			}
			if copies.CompareAndSwap(cur, cur+n) {
				reserved += n
				return true
			}
		}
	}
	record := &DiskCacheCallbacks{
		OnWritten: []OnDiskCacheWrittenFunc{func(filePath string, entry IOEntry) {
			entry.Data = bytes.Clone(entry.Data)
			entries = append(entries, writtenEntry{filePath, entry})
		}},
		OnEvict:     callbacks.OnEvict,
		upstreamCtx: ctx,
	}
	return record, reserve, func() {
		c := &deferredCallbacks{d: d, fns: callbacks.OnWritten, entries: entries,
			release: func() { copies.Add(-reserved) }}
		c.run()
	}
}

// writtenEntry is an entry written, recorded for the write callbacks.
type writtenEntry struct {
	filePath string
	entry    IOEntry
}

// deferredCallbacks runs each of fns on each of entries, in order, then release.
type deferredCallbacks struct {
	d       *DiskCache
	fns     []OnDiskCacheWrittenFunc
	entries []writtenEntry
	// next is the index of the next call: fns[next%len(fns)] on entries[next/len(fns)].
	next    int
	release func()
}

// run runs the calls from next, then release. A callback that exits its goroutine
// continues the calls on a new one.
func (c *deferredCallbacks) run() {
	exited := true
	defer func() {
		if exited {
			go c.run()
		}
	}()
	for c.next < len(c.entries)*len(c.fns) {
		k := c.next
		c.next++
		w, i := c.entries[k/len(c.fns)], k%len(c.fns)
		c.d.runAsyncUpdateCallback(w.filePath, "", i, c.fns[i], w.entry)
	}
	exited = false
	c.release()
}
