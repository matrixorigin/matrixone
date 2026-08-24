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

const (
	minExpensiveRangeReadSpan        = int64(8 << 20)
	maxMinimalRangeReadAmplification = int64(8)
)

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
		if entry.releaseData != nil {
			entry.releaseData()
		}
	}
}

func (i *IOVector) ReleaseReadResultOnError() {
	for idx := range i.Entries {
		entry := &i.Entries[idx]
		if entry.CachedData != nil {
			entry.CachedData.Release()
			entry.CachedData = nil
		}
		if entry.done && entry.releaseData != nil {
			entry.releaseData()
			entry.releaseData = nil
		}
		entry.done = false
		entry.fromCache = nil
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
		min, max = i.readMinimalRange()
	}

	return
}

func (i *IOVector) readMinimalRange() (min *int64, max *int64) {
	min = ptrTo(int64(math.MaxInt))
	max = ptrTo(int64(0))
	for _, entry := range i.Entries {
		if entry.done {
			continue
		}
		if entry.Offset < *min {
			min = &entry.Offset
		}
		if entry.Size < 0 {
			max = nil
		}
		if max != nil {
			if end := entry.Offset + entry.Size; end > *max {
				max = &end
			}
		}
	}
	return
}

// expensiveMinimalRangeRead reports whether collapsing the unfinished entries
// into one range would fetch substantially more data than the caller requested.
// Unknown or invalid ranges keep the existing fallback behavior.
func (i *IOVector) expensiveMinimalRangeRead() (logicalBytes, spanBytes int64, expensive bool) {
	minOffset := int64(math.MaxInt64)
	maxEnd := int64(0)
	n := 0
	for _, entry := range i.Entries {
		if entry.done {
			continue
		}
		if entry.Offset < 0 || entry.Size <= 0 || entry.Offset > math.MaxInt64-entry.Size {
			return 0, 0, false
		}
		if logicalBytes > math.MaxInt64-entry.Size {
			return 0, 0, false
		}
		n++
		logicalBytes += entry.Size
		minOffset = min(minOffset, entry.Offset)
		maxEnd = max(maxEnd, entry.Offset+entry.Size)
	}
	if n < 2 || minOffset == math.MaxInt64 || maxEnd < minOffset {
		return logicalBytes, 0, false
	}
	spanBytes = maxEnd - minOffset
	if spanBytes <= minExpensiveRangeReadSpan ||
		logicalBytes > math.MaxInt64/maxMinimalRangeReadAmplification {
		return logicalBytes, spanBytes, false
	}
	return logicalBytes, spanBytes,
		spanBytes > logicalBytes*maxMinimalRangeReadAmplification
}

func (i *IOVector) size() *int64 {
	if len(i.Entries) == 0 {
		return nil
	}
	var ret int64
	for _, entry := range i.Entries {
		if entry.Size < 0 {
			return nil
		}
		ret = max(ret, entry.Offset+entry.Size)
	}
	return &ret
}
