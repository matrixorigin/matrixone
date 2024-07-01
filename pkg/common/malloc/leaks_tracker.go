// Copyright 2024 Matrix Origin
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

package malloc

import (
	"fmt"
	"io"
	"runtime"
	"sync"
	"sync/atomic"
)

type LeaksTracker struct {
	infos sync.Map // stacktrace id -> *TrackInfo
}

type trackInfo struct {
	allocate   *ShardedCounter[uint64, atomic.Uint64, *atomic.Uint64]
	deallocate *ShardedCounter[uint64, atomic.Uint64, *atomic.Uint64]
}

func newTrackInfo() *trackInfo {
	return &trackInfo{
		allocate:   NewShardedCounter[uint64, atomic.Uint64](runtime.GOMAXPROCS(0)),
		deallocate: NewShardedCounter[uint64, atomic.Uint64](runtime.GOMAXPROCS(0)),
	}
}

func (t *LeaksTracker) allocate(id StacktraceID) {
	v, ok := t.infos.Load(id)
	if ok {
		info := v.(*trackInfo)
		info.allocate.Add(1)
		return
	}

	v, _ = t.infos.LoadOrStore(id, newTrackInfo())
	v.(*trackInfo).allocate.Add(1)
}

func (t *LeaksTracker) deallocate(id StacktraceID) {
	v, ok := t.infos.Load(id)
	if ok {
		info := v.(*trackInfo)
		info.deallocate.Add(1)
		return
	}

	v, _ = t.infos.LoadOrStore(id, newTrackInfo())
	v.(*trackInfo).deallocate.Add(1)
}

func (t *LeaksTracker) ReportLeaks(w io.Writer) (leaks bool) {
	t.infos.Range(func(k, v any) bool {
		stacktraceID := k.(StacktraceID)
		info := v.(*trackInfo)

		allocate := info.allocate.Load()
		deallocate := info.deallocate.Load()
		if allocate > deallocate {
			fmt.Fprintf(w, "missing free: %s\n", stacktraceID)
			leaks = true
		} else if deallocate > allocate {
			fmt.Fprintf(w, "excessive free: %s\n", stacktraceID)
			leaks = true
		}

		return true
	})

	return leaks
}
