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

package cache

import (
	"github.com/matrixorigin/matrixone/pkg/common/moerr"
	"github.com/matrixorigin/matrixone/pkg/common/system"
)

// automaticCachePercent is the share of physical capacity an unconfigured cache may retain.
//
// It is deliberately HIGH. The budget's job is to bound how MANY indexes stay hot, not to
// reserve headroom: an arrival that does not fit is refused outright (see makeRoom), so a
// conservative fraction does not protect memory -- it just refuses the largest indexes a
// deployment legitimately wants to cache. A 1M-row CAGRA index is ~7.8 GB; at half of an 8 GB
// card it could never be admitted at all.
const automaticCachePercent = 90

// automaticHostLimit derives the host budget from what the machine actually has, preferring a
// cgroup limit when it is the tighter of the two.
//
// There is NO fallback. A budget invented when the input is missing would be a number
// describing no real machine, and the governor refuses arrivals that exceed the budget -- so
// guessing low silently fails queries and guessing high silently over-commits. Neither is
// better than reporting that the sizing input is unavailable and letting the operator set the
// variable.
func automaticHostLimit(total, cgroup uint64) (int64, error) {
	if cgroup > 0 && (total == 0 || cgroup < total) {
		total = cgroup
	}
	if total == 0 {
		return 0, moerr.NewInternalErrorNoCtx("cannot size the index cache: host memory is " +
			"unknown (/proc/meminfo and the cgroup limit both report 0); " +
			"set max_index_cache_size explicitly")
	}
	share := total / 100 * automaticCachePercent
	return int64(max(uint64(1), min(share, uint64(absoluteHostCacheCeiling)))), nil
}

// automaticDeviceCapacity sums the per-device share across the GPUs this CN can see.
//
// No GPU is NOT an error: the device arena simply does not apply, so it gets no budget and
// enforce skips it. A GPU that exists but cannot be queried IS an error, for the same reason
// automaticHostLimit refuses to guess.
func automaticDeviceCapacity(countDevices func() (int, error), totalMem func(int) (uint64, error)) (int64, error) {
	count, err := countDevices()
	if err != nil {
		return 0, moerr.NewInternalErrorNoCtxf("cannot size the GPU index cache: counting "+
			"devices failed (%v); set max_gpu_index_cache_size explicitly", err)
	}
	if count <= 0 {
		return 0, nil
	}
	var total uint64
	for device := 0; device < count; device++ {
		n, err := totalMem(device)
		if err != nil || n == 0 {
			return 0, moerr.NewInternalErrorNoCtxf("cannot size the GPU index cache: device %d "+
				"reports no capacity (%v); set max_gpu_index_cache_size explicitly", device, err)
		}
		// Saturate before summing physical devices, not query simulation aliases.
		share := n / 100 * automaticCachePercent
		total += min(share, uint64(absoluteDeviceCacheCeiling)-total)
	}
	return int64(max(total, 1)), nil
}

// defaultLimits is the automatic budget for this machine, derived once, with the two arenas'
// errors kept SEPARATE.
//
// Separate because they are independent failures with independent blast radii: a GPU probe that
// fails says nothing about host memory, and joining them would let an unreadable card refuse
// every hnsw and fulltext2 load on a CN whose RAM is perfectly well known. limits() surfaces an
// arena's error only when that arena actually needs deriving.
//
// Memoized because probing does not become more likely to succeed on the next miss, and
// retrying per load would put a failing syscall on the query path. That is only acceptable
// because the remedy works: setting the arena's variable makes limits() stop consulting this
// at all, so an operator recovers without restarting the CN.
func (c *VectorIndexCache) defaultLimits() (caps, error, error) {
	c.defaultLimitOnce.Do(func() {
		host, herr := automaticHostLimit(system.MemoryTotal(), system.CgroupMemoryLimit())
		device, derr := automaticDeviceLimit()
		c.defaultLimit = caps{host: host, device: device}
		c.defaultLimitHostErr, c.defaultLimitDeviceErr = herr, derr
	})
	return c.defaultLimit, c.defaultLimitHostErr, c.defaultLimitDeviceErr
}
