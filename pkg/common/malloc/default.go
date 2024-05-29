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
	"os"
	"runtime"
	"strings"
)

func NewDefault(config *Config) (allocator Allocator) {
	if config == nil {
		c := *defaultConfig.Load()
		config = &c
	}

	var metrics Metrics
	if config.EnableMetrics {
		go metrics.startExport()
	}

	switch strings.TrimSpace(strings.ToLower(os.Getenv("MO_MALLOC"))) {

	case "c":
		allocator = NewCAllocator()
		if config.EnableMetrics {
			allocator = NewMetricsAllocator(allocator, &metrics)
		}
		return allocator

	case "old":
		return NewShardedAllocator(
			runtime.GOMAXPROCS(0),
			func() Allocator {
				var ret Allocator
				ret = NewPureGoClassAllocator(256 * MB)
				if config.EnableMetrics {
					ret = NewMetricsAllocator(ret, &metrics)
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
				if config.EnableMetrics {
					ret = NewMetricsAllocator(ret, &metrics)
				}
				return ret
			},
		)

	}
}
