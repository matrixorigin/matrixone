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
	m sync.Map
	n atomic.Int64
	// mu orders forget and storeCurrent.
	mu sync.Mutex
	// deletes counts the objects deleted; a prefetch queued before a delete does not
	// store its size.
	deletes atomic.Uint64
}

// forget drops the size of the object at diskPath, which is deleted.
func (o *prefetchedObjects) forget(diskPath string) {
	o.mu.Lock()
	defer o.mu.Unlock()
	o.deletes.Add(1)
	if _, loaded := o.m.LoadAndDelete(diskPath); loaded {
		o.n.Add(-1)
	}
}

// storeCurrent stores the size of the object at diskPath unless an object was deleted
// since the delete count was deletes.
func (o *prefetchedObjects) storeCurrent(diskPath string, size int64, deletes uint64) {
	o.mu.Lock()
	defer o.mu.Unlock()
	if o.deletes.Load() == deletes {
		o.store(diskPath, size)
	}
}

func (o *prefetchedObjects) store(diskPath string, size int64) {
	if o.n.Load() >= prefetchedObjectsMax {
		o.m.Clear()
		o.n.Store(0)
	}
	if _, loaded := o.m.Swap(diskPath, size); !loaded {
		o.n.Add(1)
	}
}

func (o *prefetchedObjects) size(diskPath string) (int64, bool) {
	if o.n.Load() == 0 {
		return 0, false
	}
	v, ok := o.m.Load(diskPath)
	if !ok {
		return 0, false
	}
	return v.(int64), true
}
