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

import "math"

func (i *IOVector) allDone() bool {
	for _, entry := range i.Entries {
		if !entry.done {
			return false
		}
	}
	return true
}

func (i *IOVector) Release() {
	for _, entry := range i.Entries {
		if entry.CachedData != nil {
			entry.CachedData.Release()
		}
		for _, fn := range entry.releaseFuncs {
			fn()
		}
	}
}

func (i *IOVector) readRange() (min *int64, max *int64, readFull bool) {
	readFull = i.Policy.CacheFullFile() &&
		!i.Policy.Any(SkipDiskCache)

	if readFull {
		// full range
		min = ptrTo[int64](0)
		max = (*int64)(nil)

	} else {
		// minimal range
		min = ptrTo(int64(math.MaxInt))
		max = ptrTo(int64(0))
		for _, entry := range i.Entries {
			entry := entry
			if entry.done {
				continue
			}
			if entry.Offset < *min {
				min = &entry.Offset
			}
			if entry.Size < 0 {
				entry.Size = 0
				max = nil
			}
			if max != nil {
				if end := entry.Offset + entry.Size; end > *max {
					max = &end
				}
			}
		}
	}

	return
}
