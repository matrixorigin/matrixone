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

package mpool

import (
	"sync"

	"github.com/matrixorigin/matrixone/pkg/common/malloc"
	metric "github.com/matrixorigin/matrixone/pkg/util/metric/v2"
)

var allocator = sync.OnceValue(func() *malloc.ManagedAllocator[malloc.Allocator] {
	// default
	allocator := malloc.GetDefault(nil)
	allocator = malloc.NewMetricsAllocator(
		allocator,
		metric.MallocCounter.WithLabelValues("mpool-allocate"),
		metric.MallocGauge.WithLabelValues("mpool-inuse"),
		metric.MallocCounter.WithLabelValues("mpool-allocate-objects"),
		metric.MallocGauge.WithLabelValues("mpool-inuse-objects"),
	)
	//TODO peak in-use
	// managed
	return malloc.NewManagedAllocator(allocator)
})

func MakeBytes(size int) ([]byte, error) {
	return allocator().Allocate(uint64(size), malloc.NoHints)
}

func FreeBytes(bs []byte) {
	allocator().Deallocate(bs, malloc.NoHints)
}
