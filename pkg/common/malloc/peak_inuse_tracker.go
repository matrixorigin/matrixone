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
	"sync/atomic"
	"time"
)

type PeakInuseTracker struct {
	ptr atomic.Pointer[map[string]peakInuseValue]
}

type peakInuseValue struct {
	Value uint64
	Time  time.Time
}

func NewPeakInuseTracker() *PeakInuseTracker {
	ret := new(PeakInuseTracker)
	m := make(map[string]peakInuseValue)
	ret.ptr.Store(&m)
	return ret
}

func (p *PeakInuseTracker) MarshalJSON() ([]byte, error) {
	return json.Marshal(*p.ptr.Load())
}

var GlobalPeakInuseTracker = NewPeakInuseTracker()

func (p *PeakInuseTracker) Update(key string, n uint64) {
	for {
		// read
		ptr := p.ptr.Load()
		if n <= (*ptr)[key].Value {
			return
		}
		// copy
		newData := maps.Clone(*ptr)
		newData[key] = peakInuseValue{
			Value: n,
			Time:  time.Now(),
		}
		// update
		if p.ptr.CompareAndSwap(ptr, &newData) {
			return
		}
	}
}
