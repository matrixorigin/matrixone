// Copyright 2026 Matrix Origin
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package cache

import "github.com/matrixorigin/matrixone/pkg/common/system"

const fallbackCacheBytes int64 = 256 << 20

// Zero means automatic sizing. A retention budget above physical capacity
// cannot reclaim historical generations before the host is exhausted. Leave
// most host memory for SQL execution, file caches, and allocator overhead.
func automaticHostLimit(total, cgroup uint64) int64 {
	if cgroup > 0 && (total == 0 || cgroup < total) {
		total = cgroup
	}
	if total == 0 {
		return fallbackCacheBytes
	}
	return int64(max(uint64(1), min(total/4, uint64(absoluteHostCacheCeiling))))
}

func automaticDeviceCapacity(countDevices func() (int, error), totalMem func(int) (uint64, error)) int64 {
	count, err := countDevices()
	if err != nil || count <= 0 {
		return fallbackCacheBytes
	}
	var total uint64
	for device := 0; device < count; device++ {
		n, err := totalMem(device)
		if err != nil || n == 0 {
			return fallbackCacheBytes
		}
		// Saturate before summing physical devices, not query simulation aliases.
		total += min(n/2, uint64(absoluteDeviceCacheCeiling)-total)
	}
	return int64(max(total, 1))
}

func (c *VectorIndexCache) defaultLimits() caps {
	c.defaultLimitOnce.Do(func() {
		c.defaultLimit = caps{
			host:   automaticHostLimit(system.MemoryTotal(), system.CgroupMemoryLimit()),
			device: automaticDeviceLimit(),
		}
	})
	return c.defaultLimit
}
