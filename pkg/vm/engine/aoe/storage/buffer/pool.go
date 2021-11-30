// Copyright 2021 Matrix Origin
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

package buf

import (
	"errors"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/aoe/storage/common"
	"sync/atomic"
	// log "github.com/sirupsen/logrus"
)

type SimpleMemoryPool struct {
	Capacity uint64
	Usage    uint64
}

func NewSimpleMemoryPool(capacity uint64) IMemoryPool {
	pool := &SimpleMemoryPool{
		Capacity: capacity,
	}
	return pool
}

func (pool *SimpleMemoryPool) GetCapacity() uint64 {
	return atomic.LoadUint64(&(pool.Capacity))
}

func (pool *SimpleMemoryPool) SetCapacity(capacity uint64) error {
	if capacity < atomic.LoadUint64(&(pool.Capacity)) {
		return errors.New("logic error")
	}
	atomic.StoreUint64(&(pool.Capacity), capacity)
	return nil
}

func (pool *SimpleMemoryPool) GetUsage() uint64 {
	return atomic.LoadUint64(&(pool.Usage))
}

// Only for temp test
func (pool *SimpleMemoryPool) Alloc(vf common.IVFile, useCompress bool, constructor MemoryNodeConstructor) (node IMemoryNode) {
	var size uint64
	if useCompress {
		size = uint64(vf.Stat().Size())
	} else {
		size = uint64(vf.Stat().OriginSize())
	}
	capacity := atomic.LoadUint64(&(pool.Capacity))
	currsize := atomic.LoadUint64(&(pool.Usage))
	postsize := size + currsize
	if postsize > capacity {
		return nil
	}
	for !atomic.CompareAndSwapUint64(&(pool.Usage), currsize, postsize) {
		currsize = atomic.LoadUint64(&(pool.Usage))
		postsize += currsize + size
		if postsize > capacity {
			return nil
		}
	}
	return constructor(vf, useCompress, WithFreeWithPool(pool))
}

func (pool *SimpleMemoryPool) Free(size uint64) {
	if size == 0 {
		return
	}
	usagesize := atomic.AddUint64(&(pool.Usage), ^uint64(size-1))
	if usagesize > pool.Capacity {
		panic("")
	}
}
