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
	"hash/maphash"
	"sync"
	"sync/atomic"
)

// objectDeletesMax is the most deleted objects remembered; past it they are forgotten.
const objectDeletesMax = 1 << 16

// objectDeletesStripes is the number of locks the publications and deletes of objects
// share by their disk paths.
const objectDeletesStripes = 64

// objectDeletes counts the deletes of objects, so an entry read before its object was
// deleted is not cached after the delete.
type objectDeletes struct {
	count atomic.Uint64
	mu    sync.Mutex
	// last holds, by object disk path, the count after the object's last delete.
	last map[string]uint64
	// floor is the count when last was cleared: earlier deletes are forgotten.
	floor uint64
	// stripes are held shared by the publications of entries and exclusively by the
	// counting of deletes, by object disk path.
	stripes [objectDeletesStripes]sync.RWMutex
}

var objectDeletesSeed = maphash.MakeSeed()

func (o *objectDeletes) stripe(diskPath string) *sync.RWMutex {
	return &o.stripes[maphash.String(objectDeletesSeed, diskPath)%objectDeletesStripes]
}

func (o *objectDeletes) now() uint64 {
	return o.count.Load()
}

// deleted counts a delete of the object at diskPath, after the publications of its
// entries under way end.
func (o *objectDeletes) deleted(diskPath string) {
	stripe := o.stripe(diskPath)
	stripe.Lock()
	defer stripe.Unlock()
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

// publishUnlessDeleted runs write, which caches entry once the claim it is given holds,
// with a claim that also fails when the object at filePath was deleted after the entry
// was read. A delete is counted after the publications claimed end, so its removal of the
// object's cached files finds them.
func (d *DiskCache) publishUnlessDeleted(
	filePath string,
	entry IOEntry,
	claim func() bool,
	write func(claim func() bool) (bool, error),
) (bool, error) {
	if entry.readDeletes == 0 {
		return write(claim)
	}
	path, err := ParsePath(filePath)
	if err != nil {
		return write(claim)
	}
	objectPath := d.pathForFile(path.File)
	stripe := d.deletes.stripe(objectPath)
	locked, deleted := false, false
	written, err := write(func() bool {
		if claim != nil && !claim() {
			return false
		}
		stripe.RLock()
		locked = true
		deleted = d.deletes.since(objectPath, entry.readDeletes-1)
		return !deleted
	})
	if locked {
		stripe.RUnlock()
	}
	if deleted {
		return false, nil
	}
	return written, err
}
