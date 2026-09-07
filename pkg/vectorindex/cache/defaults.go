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
// reserve headroom: idle entries are reclaimed and only an arrival that would displace a live
// query is refused (see makeRoom). A 1M-row CAGRA index is ~7.8 GB; at half of an 8 GB card it
// could never coexist with another index even though it remains usable as a sole occupant.
const automaticCachePercent = 90

// automaticHostLimit derives the host budget from what the machine actually has, preferring a
// cgroup limit when it is the tighter of the two.
//
// There is NO guessed fallback. A budget invented when the input is missing would be a number
// describing no real machine, and the governor cannot safely decide whether an arrival fits --
// so the sizing error names the variable the operator can set. A cgroup v1 unlimited sentinel is
// normalized before this calculation and never becomes an effectively infinite budget.
func automaticHostLimit(total, cgroup uint64) (int64, error) {
	total = system.NormalizeMemoryCapacity(total)
	cgroup = system.NormalizeMemoryCapacity(cgroup)
	if cgroup > 0 && (total == 0 || cgroup < total) {
		total = cgroup
	}
	if total == 0 {
		return 0, moerr.NewInternalErrorNoCtx("cannot size the index cache: host memory is " +
			"unknown (/proc/meminfo and the cgroup limit both report 0); " +
			"set max_index_cache_size explicitly")
	}
	share := total / 100 * automaticCachePercent
	return int64(max(uint64(1), min(share, uint64(maxRepresentableBudget)))), nil
}

// automaticDeviceCapacity sums the per-device share across the GPUs this CN can see.
//
// No GPU is NOT an error: the device arena simply does not apply, so it gets no budget and
// enforce skips it. A GPU that exists but cannot be queried IS an error, for the same reason
// automaticHostLimit refuses to guess.
func automaticDeviceCapacity(countDevices func() (int, error), totalMem func(int) (uint64, error)) (int64, error) {
	perCard, err := automaticDeviceCapacityPerCard(countDevices, totalMem)
	if err != nil {
		return 0, err
	}
	var total int64
	for _, share := range perCard {
		total += min(share, maxRepresentableBudget-total)
	}
	if len(perCard) == 0 {
		return 0, nil
	}
	return max(total, 1), nil
}

// automaticDeviceCapacityPerCard is the derived budget for EACH GPU.
//
// Per card, not just summed, because that is the shape placement has: a SINGLE_GPU index lives
// on one device, so what bounds it is that device's memory, and the other cards' free VRAM is
// irrelevant to whether it fits.
func automaticDeviceCapacityPerCard(countDevices func() (int, error), totalMem func(int) (uint64, error)) (map[int]int64, error) {
	count, err := countDevices()
	if err != nil {
		return nil, moerr.NewInternalErrorNoCtxf("cannot size the GPU index cache: counting "+
			"devices failed (%v); set max_gpu_index_cache_size explicitly", err)
	}
	if count <= 0 {
		return nil, nil
	}
	perCard := make(map[int]int64, count)
	for device := 0; device < count; device++ {
		n, err := totalMem(device)
		if err != nil || n == 0 {
			return nil, moerr.NewInternalErrorNoCtxf("cannot size the GPU index cache: device %d "+
				"reports no capacity (%v); set max_gpu_index_cache_size explicitly", device, err)
		}
		share := n / 100 * automaticCachePercent
		perCard[device] = int64(min(share, uint64(maxRepresentableBudget)))
	}
	return perCard, nil
}

// defaultLimits is the automatic budget for this machine, with the two arenas'
// errors kept SEPARATE. Host sizing is deliberately refreshed on every call:
// cgroup limits can change while the CN is alive, and keeping the first value
// forever would let a warm cache exceed a newly lowered memory limit. Device
// probing is still memoized because GPU capacity does not change at runtime.
//
// Separate because they are independent failures with independent blast radii: a GPU probe that
// fails says nothing about host memory, and joining them would let an unreadable card refuse
// every hnsw and fulltext2 load on a CN whose RAM is perfectly well known. limits() surfaces an
// arena's error only when that arena actually needs deriving.
func (g *VectorIndexGovernor) defaultLimits() (caps, error, error) {
	g.defaultLimitMu.Lock()
	defer g.defaultLimitMu.Unlock()

	host, herr := automaticHostLimit(system.MemoryTotal(), system.CgroupMemoryLimit())
	g.defaultLimit.host = host
	g.defaultLimitHostErr = herr
	if !g.defaultLimitDeviceReady {
		device, derr := automaticDeviceLimit()
		g.defaultLimit.device = device
		g.defaultLimitDeviceErr = derr
		g.defaultLimitPerCard, _ = automaticDeviceLimitPerCard()
		g.defaultLimitDeviceReady = true
	}
	return g.defaultLimit, g.defaultLimitHostErr, g.defaultLimitDeviceErr
}

// devicePerCardCaps is the derived budget for each GPU, or nil when there is none to derive.
// Per card because that is the shape placement has; see enforceDevicePlacement.
func (g *VectorIndexGovernor) devicePerCardCaps() map[int]int64 {
	g.defaultLimitMu.Lock()
	defer g.defaultLimitMu.Unlock()
	if !g.defaultLimitDeviceReady {
		g.defaultLimit.device, g.defaultLimitDeviceErr = automaticDeviceLimit()
		g.defaultLimitPerCard, _ = automaticDeviceLimitPerCard()
		g.defaultLimitDeviceReady = true
	}
	return g.defaultLimitPerCard
}
