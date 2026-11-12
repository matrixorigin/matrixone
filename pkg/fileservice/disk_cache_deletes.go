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
	"context"
	"sync"
	"sync/atomic"
)

// objectDeletesMax is the most deleted objects remembered; past it they are forgotten.
const objectDeletesMax = 1 << 16

// objectDeletes counts the deletes of objects, so an entry read before its object was
// deleted is not cached after the delete.
type objectDeletes struct {
	count atomic.Uint64
	mu    sync.Mutex
	// last holds, by object disk path, the count after the object's last delete.
	last map[string]uint64
	// floor is the count when last was cleared: earlier deletes are forgotten.
	floor uint64
}

func (o *objectDeletes) now() uint64 {
	return o.count.Load()
}

// deleted counts a delete of the object at diskPath.
func (o *objectDeletes) deleted(diskPath string) {
	o.mu.Lock()
	defer o.mu.Unlock()
	if len(o.last) >= objectDeletesMax {
		clear(o.last)
		o.floor = o.count.Load()
	}
	if o.last == nil {
		o.last = make(map[string]uint64)
	}
	o.last[diskPath] = o.count.Add(1)
}

// since reports whether the object at diskPath may have been deleted after the count
// was start.
func (o *objectDeletes) since(diskPath string, start uint64) bool {
	if o.count.Load() == start {
		return false
	}
	o.mu.Lock()
	defer o.mu.Unlock()
	if start < o.floor {
		return true
	}
	return o.last[diskPath] > start
}

// readDeletes returns the mark of an entry read now: 1 plus the delete count.
func (d *DiskCache) readDeletes() uint64 {
	return d.deletes.now() + 1
}

// retractDeleted removes the entry just cached at diskPath, and reports so, when the
// object at filePath was deleted after the entry was read. A delete counted before the
// check removes the entry here; one counted after it finds the entry among the object's
// cached files.
func (d *DiskCache) retractDeleted(ctx context.Context, filePath, diskPath string, entry IOEntry) (bool, error) {
	if entry.readDeletes == 0 {
		return false, nil
	}
	path, err := ParsePath(filePath)
	if err != nil {
		return false, nil
	}
	if !d.deletes.since(d.pathForFile(path.File), entry.readDeletes-1) {
		return false, nil
	}
	return true, d.removeRangeFile(ctx, diskPath)
}
