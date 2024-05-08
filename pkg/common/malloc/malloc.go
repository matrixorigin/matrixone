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

package malloc

import (
	"runtime"
	"time"
	"unsafe"

	"github.com/matrixorigin/matrixone/pkg/logutil"
	"go.uber.org/zap"
)

const (
	maxBufferSize   = 1 << 31
	minClassSize    = 128
	maxClassSize    = 1 << 20
	classSizeFactor = 1.5
)

var (
	numShards        = runtime.GOMAXPROCS(-1)
	evictAllDuration = time.Hour * 7
	minEvictInterval = time.Millisecond * 100

	bufferedObjectsPerClass = func() int {
		n := maxBufferSize / numShards / len(classSizes) / ((minClassSize + maxClassSize) / 2)
		if n < 8 {
			n = 8
		}
		logutil.Info("malloc",
			zap.Any("max buffer size", maxBufferSize),
			zap.Any("num shards", numShards),
			zap.Any("classes", len(classSizes)),
			zap.Any("min class size", minClassSize),
			zap.Any("max class size", maxClassSize),
			zap.Any("buffer objects per class", n),
		)
		return n
	}()

	classSizes = func() (ret []int) {
		for size := minClassSize; size <= maxClassSize; size = int(float64(size) * classSizeFactor) {
			ret = append(ret, size)
		}
		return
	}()

	shards = func() (ret [][]chan *Handle) {
		for i := 0; i < numShards; i++ {
			var shard []chan *Handle
			for range classSizes {
				shard = append(shard, make(chan *Handle, bufferedObjectsPerClass))
			}
			ret = append(ret, shard)
		}
		return
	}()
)

func init() {
	// evict
	go func() {
		evictOne := func() {
			for _, shard := range shards {
				for i := len(shard) - 1; i >= 0; i-- {
					select {
					case <-shard[i]:
						return
					default:
					}
				}
			}
		}
		interval := evictAllDuration / time.Duration(numShards*len(classSizes)*bufferedObjectsPerClass)
		if interval < minEvictInterval {
			interval = minEvictInterval
		}
		for range time.NewTicker(interval).C {
			evictOne()
		}
	}()
}

func requestSizeToClass(size int) int {
	for class, classSize := range classSizes {
		if classSize >= size {
			return class
		}
	}
	return -1
}

func classAllocate(class int) *Handle {
	pid := runtime_procPin()
	runtime_procUnpin()
	if pid >= len(shards) {
		pid = 0
	}
	select {
	case handle := <-shards[pid][class]:
		clear(unsafe.Slice((*byte)(handle.ptr), classSizes[handle.class]))
		return handle
	default:
		slice := make([]byte, classSizes[class])
		return &Handle{
			ptr:   unsafe.Pointer(unsafe.SliceData(slice)),
			class: class,
		}
	}
}

func Alloc(n int) (unsafe.Pointer, *Handle) {
	if n == 0 {
		return nil, dumbHandle
	}
	class := requestSizeToClass(n)
	if class == -1 {
		return unsafe.Pointer(unsafe.SliceData(make([]byte, n))), dumbHandle
	}
	handle := classAllocate(class)
	return handle.ptr, handle
}

func AllocTyped[T any](target **T) *Handle {
	var t T
	size := unsafe.Sizeof(t)
	class := requestSizeToClass(int(size))
	if class == -1 {
		*target = new(T)
		return dumbHandle
	}
	handle := classAllocate(class)
	*target = (*T)(handle.ptr)
	return handle
}

//go:linkname runtime_procPin runtime.procPin
func runtime_procPin() int

//go:linkname runtime_procUnpin runtime.procUnpin
func runtime_procUnpin() int
