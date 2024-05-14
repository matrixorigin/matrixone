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
	"sync/atomic"
	"unsafe"

	"github.com/matrixorigin/matrixone/pkg/logutil"
	"go.uber.org/zap"
)

const (
	maxBufferSize   = 1 << 30
	minClassSize    = 128
	maxClassSize    = 1 << 20
	classSizeFactor = 1.5
	numShards       = 16
)

type Shard struct {
	numAlloc atomic.Int64
	numFree  atomic.Int64
	pools    []chan *Handle
}

var (
	bufferedObjectsPerClass = func() int {
		n := maxBufferSize / numShards / classSumSize
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

	classSumSize = func() (ret int) {
		for _, size := range classSizes {
			ret += size
		}
		return
	}()

	shards = func() (ret []Shard) {
		ret = make([]Shard, numShards)
		for i := 0; i < numShards; i++ {
			for range classSizes {
				ret[i].pools = append(
					ret[i].pools,
					make(chan *Handle, bufferedObjectsPerClass),
				)
			}
		}
		return
	}()
)

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
	shard := pid % numShards
	select {
	case handle := <-shards[shard].pools[class]:
		shards[shard].numAlloc.Add(1)
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

//go:linkname runtime_procPin runtime.procPin
func runtime_procPin() int

//go:linkname runtime_procUnpin runtime.procUnpin
func runtime_procUnpin() int
