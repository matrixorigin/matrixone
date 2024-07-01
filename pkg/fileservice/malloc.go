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

package fileservice

import (
	"sync"

	"github.com/matrixorigin/matrixone/pkg/common/malloc"
	metric "github.com/matrixorigin/matrixone/pkg/util/metric/v2"
)

// delay initialization of global allocator
// ugly, but we tend to read malloc config from files instead of env vars
var getDefaultAllocator = func() func() malloc.Allocator {
	var allocator malloc.Allocator
	var initOnce sync.Once
	return func() malloc.Allocator {
		initOnce.Do(func() {
			allocator = malloc.GetDefault(nil)
		})
		return allocator
	}
}()

var getMemoryCacheAllocator = func() func() malloc.Allocator {
	var allocator malloc.Allocator
	var initOnce sync.Once
	return func() malloc.Allocator {
		initOnce.Do(func() {
			allocator = malloc.NewMetricsAllocator(
				getDefaultAllocator(),
				metric.MallocCounterMemoryCacheAllocateBytes,
				metric.MallocCounterMemoryCacheFreeBytes,
				metric.MallocCounterMemoryCacheInuseBytes,
			)
		})
		return allocator
	}
}()

var getBytesAllocator = func() func() malloc.Allocator {
	var allocator malloc.Allocator
	var initOnce sync.Once
	return func() malloc.Allocator {
		initOnce.Do(func() {
			allocator = malloc.NewMetricsAllocator(
				getDefaultAllocator(),
				metric.MallocCounterBytesAllocateBytes,
				metric.MallocCounterBytesFreeBytes,
				metric.MallocCounterBytesInuseBytes,
			)
		})
		return allocator
	}
}()

var getIOAllocator = func() func() malloc.Allocator {
	var allocator malloc.Allocator
	var initOnce sync.Once
	return func() malloc.Allocator {
		initOnce.Do(func() {
			allocator = malloc.NewMetricsAllocator(
				getDefaultAllocator(),
				metric.MallocCounterIOAllocateBytes,
				metric.MallocCounterIOFreeBytes,
				metric.MallocCounterIOInuseBytes,
			)
		})
		return allocator
	}
}()
