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
	"maps"
	"runtime/metrics"
	"sync/atomic"
	"time"
)

type PeakInuseTracker struct {
	ptr atomic.Pointer[peakInuseData]
}

type peakInuseData struct {
	Malloc         peakInuseValue
	Session        peakInuseValue
	IO             peakInuseValue
	MemoryCache    peakInuseValue
	Hashmap        peakInuseValue
	GoMetrics      map[string]peakInuseValue
	RSS            peakInuseValue
	VMS            peakInuseValue
	EstimatedGoRSS peakInuseValue
}

type peakInuseValue struct {
	Value uint64
	Time  time.Time
}

func NewPeakInuseTracker() *PeakInuseTracker {
	ret := new(PeakInuseTracker)
	ret.ptr.Store(&peakInuseData{
		GoMetrics: make(map[string]peakInuseValue),
	})
	return ret
}

func (p *peakInuseData) Copy() *peakInuseData {
	ret := *p
	return &ret
}

func (p *PeakInuseTracker) MarshalJSON() ([]byte, error) {
	return json.Marshal(*p.ptr.Load())
}

var GlobalPeakInuseTracker = NewPeakInuseTracker()

func (p *PeakInuseTracker) UpdateMalloc(n uint64) {
	for {
		// read
		ptr := p.ptr.Load()
		if n <= ptr.Malloc.Value {
			return
		}
		// copy
		newData := ptr.Copy()
		newData.Malloc.Value = n
		newData.Malloc.Time = time.Now()
		// update
		if p.ptr.CompareAndSwap(ptr, newData) {
			return
		}
	}
}

func (p *PeakInuseTracker) UpdateSession(n uint64) {
	for {
		// read
		ptr := p.ptr.Load()
		if n <= ptr.Session.Value {
			return
		}
		// copy
		newData := ptr.Copy()
		newData.Session.Value = n
		newData.Session.Time = time.Now()
		// update
		if p.ptr.CompareAndSwap(ptr, newData) {
			return
		}
	}
}

func (p *PeakInuseTracker) UpdateIO(n uint64) {
	for {
		// read
		ptr := p.ptr.Load()
		if n <= ptr.IO.Value {
			return
		}
		// copy
		newData := ptr.Copy()
		newData.IO.Value = n
		newData.IO.Time = time.Now()
		// update
		if p.ptr.CompareAndSwap(ptr, newData) {
			return
		}
	}
}

func (p *PeakInuseTracker) UpdateMemoryCache(n uint64) {
	for {
		// read
		ptr := p.ptr.Load()
		if n <= ptr.MemoryCache.Value {
			return
		}
		// copy
		newData := ptr.Copy()
		newData.MemoryCache.Value = n
		newData.MemoryCache.Time = time.Now()
		// update
		if p.ptr.CompareAndSwap(ptr, newData) {
			return
		}
	}
}

func (p *PeakInuseTracker) UpdateHashmap(n uint64) {
	for {
		// read
		ptr := p.ptr.Load()
		if n <= ptr.Hashmap.Value {
			return
		}
		// copy
		newData := ptr.Copy()
		newData.Hashmap.Value = n
		newData.Hashmap.Time = time.Now()
		// update
		if p.ptr.CompareAndSwap(ptr, newData) {
			return
		}
	}
}

func (p *PeakInuseTracker) UpdateGoMetrics(sample metrics.Sample) {
	for {
		// read
		ptr := p.ptr.Load()
		if sample.Value.Uint64() <= ptr.GoMetrics[sample.Name].Value {
			return
		}
		// copy
		newData := ptr.Copy()
		newData.GoMetrics = maps.Clone(ptr.GoMetrics)
		newData.GoMetrics[sample.Name] = peakInuseValue{
			Value: sample.Value.Uint64(),
			Time:  time.Now(),
		}
		// update
		if p.ptr.CompareAndSwap(ptr, newData) {
			return
		}
	}
}

func (p *PeakInuseTracker) UpdateRSS(n uint64) {
	for {
		// read
		ptr := p.ptr.Load()
		if n <= ptr.RSS.Value {
			return
		}
		// copy
		newData := ptr.Copy()
		newData.RSS.Value = n
		newData.RSS.Time = time.Now()
		// update
		if p.ptr.CompareAndSwap(ptr, newData) {
			return
		}
	}
}

func (p *PeakInuseTracker) UpdateVMS(n uint64) {
	for {
		// read
		ptr := p.ptr.Load()
		if n <= ptr.VMS.Value {
			return
		}
		// copy
		newData := ptr.Copy()
		newData.VMS.Value = n
		newData.VMS.Time = time.Now()
		// update
		if p.ptr.CompareAndSwap(ptr, newData) {
			return
		}
	}
}

func (p *PeakInuseTracker) UpdateEstimatedGoRSS(n uint64) {
	for {
		// read
		ptr := p.ptr.Load()
		if n <= ptr.EstimatedGoRSS.Value {
			return
		}
		// copy
		newData := ptr.Copy()
		newData.EstimatedGoRSS.Value = n
		newData.EstimatedGoRSS.Time = time.Now()
		// update
		if p.ptr.CompareAndSwap(ptr, newData) {
			return
		}
	}
}
