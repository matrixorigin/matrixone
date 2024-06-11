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
	"net/http"
	"os"
	"runtime"
	"strings"
)

func NewDefault(config *Config) (allocator Allocator) {
	if config == nil {
		c := *defaultConfig.Load()
		config = &c
	}

	defer func() {
		if config.FullStackFraction != nil && *config.FullStackFraction > 0 {
			allocator = NewProfileAllocator(
				allocator,
				globalProfiler,
				*config.FullStackFraction,
			)
		}
	}()

	switch strings.TrimSpace(strings.ToLower(os.Getenv("MO_MALLOC"))) {

	case "c":
		allocator = NewCAllocator()
		if config.EnableMetrics != nil && *config.EnableMetrics {
			allocator = NewMetricsAllocator(allocator)
		}
		return allocator

	case "old":
		return NewShardedAllocator(
			runtime.GOMAXPROCS(0),
			func() Allocator {
				var ret Allocator
				ret = NewPureGoClassAllocator(256 * MB)
				if config.EnableMetrics != nil && *config.EnableMetrics {
					ret = NewMetricsAllocator(ret)
				}
				return ret
			},
		)

	default:
		return NewShardedAllocator(
			runtime.GOMAXPROCS(0),
			func() Allocator {
				var ret Allocator
				ret = NewClassAllocator(config.CheckFraction)
				if config.EnableMetrics != nil && *config.EnableMetrics {
					ret = NewMetricsAllocator(ret)
				}
				return ret
			},
		)

	}
}

var globalProfiler = NewProfiler[HeapSampleValues]()

func init() {
	http.HandleFunc("/debug/pprof/malloc", func(w http.ResponseWriter, req *http.Request) {
		globalProfiler.Write(w)
	})
}
