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
	"sync"
	"sync/atomic"

	"github.com/matrixorigin/matrixone/pkg/common/moerr"
)

var errObjectSizeUnknown = moerr.NewInternalErrorNoCtx("object size unknown")

// prefetchedObjectsMax is the most object sizes kept; past it they are dropped.
const prefetchedObjectsMax = 1 << 16

// prefetchedObjects holds, by disk path, the size of each object a prefetch told of.
type prefetchedObjects struct {
	mu sync.Mutex
	m  map[string]int64
	// deletes counts the objects deleted; a prefetch queued before a delete does not
	// store its size.
	deletes atomic.Uint64
}

// forget drops the size of the object at diskPath, which is deleted.
func (o *prefetchedObjects) forget(diskPath string) {
	o.mu.Lock()
	defer o.mu.Unlock()
	o.deletes.Add(1)
	delete(o.m, diskPath)
}

// storeCurrent stores the size of the object at diskPath unless an object was deleted
// since the delete count was deletes.
func (o *prefetchedObjects) storeCurrent(diskPath string, size int64, deletes uint64) {
	o.mu.Lock()
	defer o.mu.Unlock()
	if o.deletes.Load() == deletes {
		o.storeLocked(diskPath, size)
	}
}

func (o *prefetchedObjects) store(diskPath string, size int64) {
	o.mu.Lock()
	defer o.mu.Unlock()
	o.storeLocked(diskPath, size)
}

func (o *prefetchedObjects) storeLocked(diskPath string, size int64) {
	if o.m == nil || len(o.m) >= prefetchedObjectsMax {
		o.m = make(map[string]int64)
	}
	o.m[diskPath] = size
}

func (o *prefetchedObjects) size(diskPath string) (int64, bool) {
	o.mu.Lock()
	defer o.mu.Unlock()
	size, ok := o.m[diskPath]
	return size, ok
}
