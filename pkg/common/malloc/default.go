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

package malloc

import (
	"io"
	"net/http"
	"os"
	"runtime"
	"strings"
	"sync"

	metric "github.com/matrixorigin/matrixone/pkg/util/metric/v2"
)

var defaultAllocator Allocator

var setDefaultAllocatorOnce sync.Once

func GetDefault(defaultConfig *Config) Allocator {
	setDefaultAllocatorOnce.Do(func() {
		defaultAllocator = newDefault(defaultConfig)
	})
	return defaultAllocator
}

func newDefault(delta *Config) (allocator Allocator) {

	// config
	config := *defaultConfig.Load()
	if delta != nil {
		config = patchConfig(config, *delta)
	}

	// debug
	if os.Getenv("MO_MALLOC_DEBUG") != "" {
		config.CheckFraction = ptrTo(uint32(1))
		config.FullStackFraction = ptrTo(uint32(1))
		config.EnableMetrics = ptrTo(true)
	}

	// profile
	defer func() {
		if config.FullStackFraction != nil && *config.FullStackFraction > 0 {
			allocator = NewProfileAllocator(
				allocator,
				globalProfiler,
				*config.FullStackFraction,
			)
		}
	}()

	// checked
	defer func() {
		if config.CheckFraction != nil && *config.CheckFraction > 0 {
			allocator = NewRandomAllocator(
				allocator,
				NewCheckedAllocator(allocator),
				*config.CheckFraction,
			)
		}
	}()

	if config.Allocator == nil {
		config.Allocator = ptrTo("mmap")
	}

	switch strings.ToLower(*config.Allocator) {

	case "c":
		// c allocator
		allocator = NewCAllocator()
		if config.EnableMetrics != nil && *config.EnableMetrics {
			allocator = NewMetricsAllocator(
				allocator,
				metric.MallocCounterAllocateBytes,
				metric.MallocGaugeInuseBytes,
				metric.MallocCounterAllocateObjects,
				metric.MallocGaugeInuseObjects,
			)
		}
		return allocator

	case "go":
		// go allocator
		return NewShardedAllocator(
			runtime.GOMAXPROCS(0),
			func() Allocator {
				var ret Allocator
				ret = NewClassAllocator(NewFixedSizeSyncPoolAllocator)
				if config.EnableMetrics != nil && *config.EnableMetrics {
					ret = NewMetricsAllocator(
						ret,
						metric.MallocCounterAllocateBytes,
						metric.MallocGaugeInuseBytes,
						metric.MallocCounterAllocateObjects,
						metric.MallocGaugeInuseObjects,
					)
				}
				return ret
			},
		)

	case "mmap":
		// mmap allocator
		return NewShardedAllocator(
			runtime.GOMAXPROCS(0),
			func() Allocator {
				var ret Allocator
				ret = NewClassAllocator(NewFixedSizeMmapAllocator)
				if config.EnableMetrics != nil && *config.EnableMetrics {
					ret = NewMetricsAllocator(
						ret,
						metric.MallocCounterAllocateBytes,
						metric.MallocGaugeInuseBytes,
						metric.MallocCounterAllocateObjects,
						metric.MallocGaugeInuseObjects,
					)
				}
				return ret
			},
		)

	default:
		panic("unknown allocator: " + *config.Allocator)
	}
}

var globalProfiler = NewProfiler[HeapSampleValues]()

func init() {
	http.HandleFunc("/debug/malloc", func(w http.ResponseWriter, req *http.Request) {
		globalProfiler.Write(w)
	})
}

func WriteProfileData(w io.Writer) error {
	return globalProfiler.Write(w)
}
