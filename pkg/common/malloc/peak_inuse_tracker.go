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
	"encoding/json"
	"sync/atomic"
	"time"
)

type PeakInuseTracker struct {
	ptr atomic.Pointer[peakInuseInfo]
}

type peakInuseInfo struct {
	Peak      peakInuseData
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

func NewPeakInuseTracker() *PeakInuseTracker {
	ret := new(PeakInuseTracker)
	ret.ptr.Store(&peakInuseInfo{})
	return ret
}

func (p *PeakInuseTracker) MarshalJSON() ([]byte, error) {
	return json.Marshal(*p.ptr.Load())
}

var GlobalPeakInuseTracker = NewPeakInuseTracker()

func (p *PeakInuseTracker) UpdateMalloc(n uint64) {
	for {
		// read
		ptr := p.ptr.Load()
		if n <= ptr.Peak.Malloc.Value {
			return
		}
		// copy
		newData := *ptr
		newData.Peak.Malloc.Value = n
		newData.Peak.Malloc.Time = time.Now()
		newData.Snapshots.Malloc = newData.Peak
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
		if n <= ptr.Peak.Session.Value {
			return
		}
		// copy
		newData := *ptr
		newData.Peak.Session.Value = n
		newData.Peak.Session.Time = time.Now()
		newData.Snapshots.Session = newData.Peak
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
		if n <= ptr.Peak.IO.Value {
			return
		}
		// copy
		newData := *ptr
		newData.Peak.IO.Value = n
		newData.Peak.IO.Time = time.Now()
		newData.Snapshots.IO = newData.Peak
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
		if n <= ptr.Peak.MemoryCache.Value {
			return
		}
		// copy
		newData := *ptr
		newData.Peak.MemoryCache.Value = n
		newData.Peak.MemoryCache.Time = time.Now()
		newData.Snapshots.MemoryCache = newData.Peak
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
		if n <= ptr.Peak.Hashmap.Value {
			return
		}
		// copy
		newData := *ptr
		newData.Peak.Hashmap.Value = n
		newData.Peak.Hashmap.Time = time.Now()
		newData.Snapshots.Hashmap = newData.Peak
		// update
		if p.ptr.CompareAndSwap(ptr, &newData) {
			return
		}
	}
}
