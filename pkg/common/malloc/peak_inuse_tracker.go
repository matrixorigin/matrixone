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
	"sync/atomic"
	"time"
)

type PeakInuseTracker struct {
	ptr atomic.Pointer[peakInuseInfo]
}

type peakInuseInfo struct {
	Data      peakInuseData
	Snapshots struct {
		Malloc      peakInuseData
		Session     peakInuseData
		IO          peakInuseData
		MemoryCache peakInuseData
		Hashmap     peakInuseData
	}
}

type peakInuseData struct {
	Malloc      peakInuseValue
	Session     peakInuseValue
	IO          peakInuseValue
	MemoryCache peakInuseValue
	Hashmap     peakInuseValue
}

type peakInuseValue struct {
	Value uint64
	Time  time.Time
}

var GlobalPeakInuseTracker = func() *PeakInuseTracker {
	ret := new(PeakInuseTracker)
	ret.ptr.Store(&peakInuseInfo{})
	return ret
}()

func (p *PeakInuseTracker) UpdateMalloc(n uint64) {
	for {
		// read
		ptr := p.ptr.Load()
		if n <= ptr.Data.Malloc.Value {
			return
		}
		// copy
		newData := *ptr
		newData.Data.Malloc.Value = n
		newData.Data.Malloc.Time = time.Now()
		newData.Snapshots.Malloc = newData.Data
		// update
		if p.ptr.CompareAndSwap(ptr, &newData) {
			return
		}
	}
}

func (p *PeakInuseTracker) UpdateSession(n uint64) {
	for {
		// read
		ptr := p.ptr.Load()
		if n <= ptr.Data.Session.Value {
			return
		}
		// copy
		newData := *ptr
		newData.Data.Session.Value = n
		newData.Data.Session.Time = time.Now()
		newData.Snapshots.Session = newData.Data
		// update
		if p.ptr.CompareAndSwap(ptr, &newData) {
			return
		}
	}
}

func (p *PeakInuseTracker) UpdateIO(n uint64) {
	for {
		// read
		ptr := p.ptr.Load()
		if n <= ptr.Data.IO.Value {
			return
		}
		// copy
		newData := *ptr
		newData.Data.IO.Value = n
		newData.Data.IO.Time = time.Now()
		newData.Snapshots.IO = newData.Data
		// update
		if p.ptr.CompareAndSwap(ptr, &newData) {
			return
		}
	}
}

func (p *PeakInuseTracker) UpdateMemoryCache(n uint64) {
	for {
		// read
		ptr := p.ptr.Load()
		if n <= ptr.Data.MemoryCache.Value {
			return
		}
		// copy
		newData := *ptr
		newData.Data.MemoryCache.Value = n
		newData.Data.MemoryCache.Time = time.Now()
		newData.Snapshots.MemoryCache = newData.Data
		// update
		if p.ptr.CompareAndSwap(ptr, &newData) {
			return
		}
	}
}

func (p *PeakInuseTracker) UpdateHashmap(n uint64) {
	for {
		// read
		ptr := p.ptr.Load()
		if n <= ptr.Data.Hashmap.Value {
			return
		}
		// copy
		newData := *ptr
		newData.Data.Hashmap.Value = n
		newData.Data.Hashmap.Time = time.Now()
		newData.Snapshots.Hashmap = newData.Data
		// update
		if p.ptr.CompareAndSwap(ptr, &newData) {
			return
		}
	}
}
