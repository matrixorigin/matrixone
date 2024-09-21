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
		Malloc  peakInuseData
		Session peakInuseData
	}
}

type peakInuseData struct {
	Malloc  peakInuseValue
	Session peakInuseValue
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
