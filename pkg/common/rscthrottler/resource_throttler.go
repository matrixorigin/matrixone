// Copyright 2025 Matrix Origin
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//	http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package rscthrottler

import (
	"context"
	"fmt"
	"math"
	"os"
	"runtime/debug"
	"sync"
	"sync/atomic"
	"time"

	"github.com/KimMachineGun/automemlimit/memlimit"
	"github.com/matrixorigin/matrixone/pkg/common/mpool"
	"github.com/matrixorigin/matrixone/pkg/logutil"
	"github.com/matrixorigin/matrixone/pkg/objectio"
	metric "github.com/matrixorigin/matrixone/pkg/util/metric/v2"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/common"
	"github.com/shirou/gopsutil/v3/process"
	"go.uber.org/zap"
)

const (
	refreshMaxInterval     = time.Second * 10
	rssScavengeInterval    = time.Minute
	rssCacheEvictTimeout   = time.Second * 10
	rssScavengeVisibleRate = 0.70
	rssPressureSoftEnter   = 0.85
	rssPressureHardEnter   = 0.92
	rssPressureHardExit    = 0.88
	rssPressureSoftExit    = 0.80
	rssCacheSoftTarget     = int64(80)
	rssCacheHardTarget     = int64(50)

	MemoryThrottlerLogHeader = "MemoryThrottler"
)

var freeOSMemory = debug.FreeOSMemory

type rssPressureState int64

const (
	rssPressureNone rssPressureState = iota
	rssPressureSoft
	rssPressureHard
)

func (s rssPressureState) String() string {
	switch s {
	case rssPressureSoft:
		return "soft"
	case rssPressureHard:
		return "hard"
	default:
		return "none"
	}
}

type RSCThrottler interface {
	Refresh()
	PrintUsage()
	Acquire(int64) (int64, bool)
	Release(int64) int64
	Available() int64
}

type memThrottler struct {
	limit atomic.Int64
	rss   atomic.Int64
	// rssReservedBase records the reserved bytes that were already reflected in
	// the latest rss sample. S3 admission can then add only the delta above this
	// base, avoiding the rss+reserved double-count that caused #24881 while
	// still keeping a synchronous physical-memory backstop for new growth.
	rssReservedBase atomic.Int64
	// rssMpoolLiveBase records the mpool live-byte baseline associated with
	// rssReservedBase. As off-heap live bytes rise back toward this baseline,
	// previously released S3 pages are being reused and the covered stale
	// portion of rssReservedBase must shrink accordingly.
	rssMpoolLiveBase atomic.Int64
	reserved         atomic.Int64
	proc             *process.Process

	cgroup atomic.Uint64
	total  atomic.Uint64

	actualTotalMemory atomic.Uint64

	name      string
	limitRate float64

	lastRefresh        atomic.Int64
	lastRSSScavenge    atomic.Int64
	lastRSSCacheEvict  atomic.Int64
	lastRSSCacheTarget atomic.Int64
	rssPressureState   atomic.Int64
	rssPressureGen     atomic.Int64
	rssScavengeMu      sync.Mutex

	mergeAvailDebounce atomic.Int64

	options struct {
		acquirePolicy func(*memThrottler, int64) (int64, bool)

		constLimit int64

		// if false, the acquiring fails if
		// the wanted memory exceeds the available.
		allowOutOfMemoryAcquire bool

		specializedForMerge bool

		enableRSSScavenging   bool
		rssCacheEvictor       func(ctx context.Context, targetPercent int64)
		rssCacheTargetSetter  func(targetPercent int64)
		rssCacheTargetClearer func()
	}
}

func (m *memThrottler) String() string {
	return fmt.Sprintf(
		"{%s: limit=%s, total=%s, available=%s, cgroup=%s, rss=%s, pinned=%s, pinnedRate=%f, limitRate=%f, isConstLimit=%v}",
		m.name,
		common.HumanReadableBytes(int(m.limit.Load())),
		common.HumanReadableBytes(int(m.total.Load())),
		common.HumanReadableBytes(int(m.Available())),
		common.HumanReadableBytes(int(m.cgroup.Load())),
		common.HumanReadableBytes(int(m.rss.Load())),
		common.HumanReadableBytes(int(m.reserved.Load())),
		m.pinnedRate(),
		m.limitRate,
		m.options.constLimit > 0,
	)
}

func (m *memThrottler) pinnedRate() float64 {
	limit := float64(m.limit.Load())
	pinned := float64(m.reserved.Load())

	return pinned / limit
}

func (m *memThrottler) Refresh() {
	m.refresh(false)
}

func (m *memThrottler) ForceRefresh() {
	m.refresh(true)
}

func (m *memThrottler) refresh(force bool) {
	last := m.lastRefresh.Load()
	now := time.Now().UnixNano()

	if !force && time.Duration(now-last) <= refreshMaxInterval {
		return
	}

	m.lastRefresh.Store(now)

	var (
		err            error
		cgroup         uint64
		info           *process.MemoryInfoStat
		total          uint64
		reservedBefore int64
	)

	defer func() {
		logutil.Info(
			fmt.Sprintf("%s-Refresh", MemoryThrottlerLogHeader),
			zap.String("detail", m.String()),
			zap.Error(err),
		)
	}()

	total = objectio.TotalMem()
	cgroup, err = memlimit.FromCgroup()

	if cgroup != 0 && cgroup < total {
		m.actualTotalMemory.Store(cgroup)
	} else if total != 0 {
		m.actualTotalMemory.Store(total)
	} else {
		m.actualTotalMemory.Store(math.MaxInt64)
		logutil.Info(
			fmt.Sprintf("%s-Refresh", MemoryThrottlerLogHeader),
			zap.String("err", "cannot get the total memory, unlimited"),
		)
	}

	// if the const limit option is set, we should not change the limit.
	if m.options.constLimit < 0 {
		m.limit.Store(int64(float64(m.actualTotalMemory.Load()) * m.limitRate))
	}

	if m.proc == nil {
		m.proc, _ = process.NewProcess(int32(os.Getpid()))
	}

	prevRSS := m.rss.Load()
	prevBase := m.rssReservedBase.Load()
	prevLiveBase := m.rssMpoolLiveBase.Load()
	reservedBefore = m.reserved.Load()
	if reservedBefore < 0 {
		reservedBefore = 0
	}
	if info, err = m.proc.MemoryInfo(); err == nil {
		rss := int64(info.RSS)
		m.rss.Store(rss)

		reservedAfter := m.reserved.Load()
		if reservedAfter < 0 {
			reservedAfter = 0
		}

		currentLive := mpool.GlobalStats().NumCurrBytes.Load()
		// Sample the S3 reservation base conservatively from the same refresh
		// window as RSS. Using the smaller of the before/after counters avoids
		// treating a concurrent new acquire as already reflected in the RSS
		// sample. Any stale covered portion then decays not only when RSS drops
		// but also when off-heap live bytes rise back toward the earlier live
		// baseline, which means the freed S3 pages are being reused by other
		// work and can no longer be treated as reusable S3 headroom.
		sampledReserved := min(reservedBefore, reservedAfter)
		base, liveBase := nextCNFlushS3RSSState(
			prevRSS, prevBase, prevLiveBase, rss, currentLive, sampledReserved,
		)
		m.rssReservedBase.Store(base)
		m.rssMpoolLiveBase.Store(liveBase)
		m.tryScavengeRSS(now, rss)
	}

	m.cgroup.Store(cgroup)
	m.total.Store(total)
}

func (m *memThrottler) tryScavengeRSS(now int64, rss int64) {
	if !m.options.enableRSSScavenging {
		return
	}
	actualMaxMemory := int64(m.actualTotalMemory.Load())
	if actualMaxMemory <= 0 || actualMaxMemory == math.MaxInt64 {
		return
	}
	visible := m.reserved.Load() + mpool.GlobalStats().NumCurrBytes.Load()
	if visible < 0 {
		visible = 0
	}
	rssRate := float64(rss) / float64(actualMaxMemory)

	var (
		prevState              rssPressureState
		nextState              rssPressureState
		cacheTargetPercent     int64
		shouldEvictCache       bool
		shouldFreeOSMemory     bool
		shouldSetCacheTarget   bool
		resetCacheTargetFirst  bool
		shouldClearCacheTarget bool
		generation             int64
	)

	m.rssScavengeMu.Lock()
	prevState = rssPressureState(m.rssPressureState.Load())
	nextState = nextRSSPressureState(prevState, rssRate)
	cacheTargetPercent = rssPressureCacheTarget(nextState)

	if nextState != prevState {
		generation = m.rssPressureGen.Add(1)
		m.rssPressureState.Store(int64(nextState))
	} else {
		generation = m.rssPressureGen.Load()
	}

	if cacheTargetPercent > 0 && m.options.rssCacheTargetSetter != nil {
		shouldSetCacheTarget = true
		resetCacheTargetFirst = prevState == rssPressureHard && nextState == rssPressureSoft
	}
	if nextState == rssPressureNone && prevState != rssPressureNone {
		m.lastRSSCacheTarget.Store(0)
		shouldClearCacheTarget = true
	}

	if cacheTargetPercent > 0 && m.options.rssCacheEvictor != nil {
		lastCacheEvict := m.lastRSSCacheEvict.Load()
		lastTarget := m.lastRSSCacheTarget.Load()
		cacheEvictExpired := time.Duration(now-lastCacheEvict) > rssScavengeInterval
		cacheEvictEscalated := lastTarget == 0 || cacheTargetPercent < lastTarget
		cachePressureEntered := prevState == rssPressureNone && nextState != rssPressureNone
		cachePressureDowngraded := prevState == rssPressureHard && nextState == rssPressureSoft
		if cachePressureDowngraded {
			m.lastRSSCacheTarget.Store(cacheTargetPercent)
		}
		if !cachePressureDowngraded && (cachePressureEntered || cacheEvictExpired || cacheEvictEscalated) {
			m.lastRSSCacheEvict.Store(now)
			m.lastRSSCacheTarget.Store(cacheTargetPercent)
			shouldEvictCache = true
		}
	}

	if shouldClearCacheTarget && m.options.rssCacheTargetClearer != nil {
		m.options.rssCacheTargetClearer()
	}
	if shouldSetCacheTarget {
		if resetCacheTargetFirst && m.options.rssCacheTargetClearer != nil {
			m.options.rssCacheTargetClearer()
		}
		m.options.rssCacheTargetSetter(cacheTargetPercent)
	}
	m.rssScavengeMu.Unlock()

	needFreeOSMemory := nextState != rssPressureNone &&
		float64(visible) < float64(rss)*rssScavengeVisibleRate
	if needFreeOSMemory {
		last := m.lastRSSScavenge.Load()
		if time.Duration(now-last) > rssScavengeInterval &&
			m.lastRSSScavenge.CompareAndSwap(last, now) {
			shouldFreeOSMemory = true
		}
	}
	if !shouldFreeOSMemory && !shouldEvictCache && !shouldClearCacheTarget && nextState == prevState {
		return
	}
	metric.FSCachePressureTriggerCounter.Inc()
	logutil.Info(
		fmt.Sprintf("%s-RSSScavenge", MemoryThrottlerLogHeader),
		zap.String("rss", common.HumanReadableBytes(int(rss))),
		zap.String("visible", common.HumanReadableBytes(int(visible))),
		zap.String("actual-total-memory", common.HumanReadableBytes(int(actualMaxMemory))),
		zap.Bool("free-os-memory", shouldFreeOSMemory),
		zap.Bool("evict-cache", shouldEvictCache),
		zap.Bool("clear-cache-target", shouldClearCacheTarget),
		zap.Int64("cache-target-percent", cacheTargetPercent),
		zap.String("pressure-state", nextState.String()),
		zap.String("previous-pressure-state", prevState.String()),
		zap.String("detail", m.String()),
	)
	if shouldEvictCache {
		evictor := m.options.rssCacheEvictor
		free := freeOSMemory
		go func(targetPercent int64, gen int64) {
			if m.rssPressureGen.Load() != gen {
				return
			}
			evictCtx, cancel := context.WithTimeout(context.Background(), rssCacheEvictTimeout)
			defer cancel()
			evictor(evictCtx, targetPercent)
			free()
		}(cacheTargetPercent, generation)
	}
	if shouldFreeOSMemory {
		freeOSMemory()
	}
}

func nextRSSPressureState(prev rssPressureState, rssRate float64) rssPressureState {
	switch prev {
	case rssPressureHard:
		if rssRate <= rssPressureSoftExit {
			return rssPressureNone
		}
		if rssRate <= rssPressureHardExit {
			return rssPressureSoft
		}
		return rssPressureHard
	case rssPressureSoft:
		if rssRate >= rssPressureHardEnter {
			return rssPressureHard
		}
		if rssRate <= rssPressureSoftExit {
			return rssPressureNone
		}
		return rssPressureSoft
	default:
		if rssRate >= rssPressureHardEnter {
			return rssPressureHard
		}
		if rssRate >= rssPressureSoftEnter {
			return rssPressureSoft
		}
		return rssPressureNone
	}
}

func rssPressureCacheTarget(state rssPressureState) int64 {
	switch state {
	case rssPressureSoft:
		return rssCacheSoftTarget
	case rssPressureHard:
		return rssCacheHardTarget
	default:
		return 0
	}
}

/*
		| -------- actual max memory  -----|
		|***RSS****|                       |
									limit
							|--------------|  case 1

    	        |--------------------------|  case 2
							limit

 |-----------------------------------------|  case 3
				   limit
*/

func (m *memThrottler) Available() int64 {
	//avail := m.limit.Load() - m.rss.Load() - m.reserved.Load()

	var (
		avail    int64
		limit    = m.limit.Load()
		rss      = m.rss.Load()
		reserved = m.reserved.Load()

		actualMaxMemory = int64(m.actualTotalMemory.Load())
	)

	if m.options.specializedForMerge {
		now := time.Now().UnixNano()
		if time.Duration(now-m.mergeAvailDebounce.Load()) > time.Second {
			m.mergeAvailDebounce.Store(now)
			if m.proc == nil {
				m.proc, _ = process.NewProcess(int32(os.Getpid()))
			}

			if info, err := m.proc.MemoryInfo(); err == nil {
				rss = int64(info.RSS)
				m.rss.Store(rss)
			}
		}

		return max(0, limit-rss-reserved)
	}

	if actualMaxMemory-rss >= limit {
		avail = limit - reserved
	} else {
		avail = actualMaxMemory - rss - reserved
	}

	return max(0, avail)
}

func (m *memThrottler) PrintUsage() {
	logutil.Info(
		fmt.Sprintf("%s-Usage", MemoryThrottlerLogHeader),
		zap.String("detail", m.String()),
	)
}

// Acquire requires memory from memThrottler,
// it returns (new available, true) if success, or
// (available, false).
func (m *memThrottler) Acquire(ask int64) (int64, bool) {

	m.Refresh()

	var (
		left    int64
		granted bool
	)

	if m.options.acquirePolicy != nil {
		left, granted = m.options.acquirePolicy(m, ask)
	} else {
		left, granted = defaultAcquirePolicy(m, ask)
	}

	if !granted {
		logutil.Info(
			fmt.Sprintf("%s-Acquire", MemoryThrottlerLogHeader),
			zap.String("err", "out of available"),
			zap.String("ask", common.HumanReadableBytes(int(ask))),
			zap.String("detail", m.String()),
		)
	}

	return left, granted
}

func (m *memThrottler) Release(mem int64) int64 {
	for {
		currReserved := m.reserved.Load()
		newReserved := currReserved - mem
		if newReserved < 0 {
			newReserved = 0
		}
		if m.reserved.CompareAndSwap(currReserved, newReserved) {
			return m.Available()
		}
	}
}

// NewMemThrottler creates a new throttler for the `name`.
//
// you can use the limit rate to limit the max memory the `name` can
// pin. the max memory will be limitRate * totalFreeMemoryOfTheSystem.
//
// besides, you also can set the constLimit in the options, if done,
// the max memory will be the constLimit.
//
// another more flexible way to manage the max memory and allocation is the
// acquirePoly, you can use it in the options.
func NewMemThrottler(
	name string,
	limitRate float64,
	opts ...MemThrottlerOption,
) RSCThrottler {

	throttler := &memThrottler{
		limitRate: limitRate,
		name:      name,
	}

	throttler.fillDefaults()

	for _, opt := range opts {
		opt(throttler)
	}

	if throttler.options.constLimit > 0 {
		throttler.limit.Store(throttler.options.constLimit)
	}

	throttler.Refresh()

	return throttler
}

func (m *memThrottler) fillDefaults() {
	m.options.constLimit = -1
	m.options.allowOutOfMemoryAcquire = false
}

type MemThrottlerOption func(throttler *memThrottler)

func WithAllowOutOfLimitAcquire() MemThrottlerOption {
	return func(throttler *memThrottler) {
		throttler.options.allowOutOfMemoryAcquire = true
	}
}

func WithConstLimit(constLimit int64) MemThrottlerOption {
	return func(throttler *memThrottler) {
		throttler.options.constLimit = constLimit
	}
}

func WithSpecializedForMerge() MemThrottlerOption {
	return func(throttler *memThrottler) {
		throttler.options.specializedForMerge = true
	}
}

func WithRSSScavenging() MemThrottlerOption {
	return func(throttler *memThrottler) {
		throttler.options.enableRSSScavenging = true
	}
}

func WithRSSCacheEvictor(evictor func(ctx context.Context, targetPercent int64)) MemThrottlerOption {
	return func(throttler *memThrottler) {
		throttler.options.rssCacheEvictor = evictor
	}
}

func WithRSSCachePressureTarget(
	setter func(targetPercent int64),
	clearer func(),
) MemThrottlerOption {
	return func(throttler *memThrottler) {
		throttler.options.rssCacheTargetSetter = setter
		throttler.options.rssCacheTargetClearer = clearer
	}
}

func WithAcquirePolicy(
	policy func(*memThrottler, int64) (int64, bool),
) MemThrottlerOption {
	return func(throttler *memThrottler) {
		throttler.options.acquirePolicy = policy
	}
}

func defaultAcquirePolicy(m *memThrottler, ask int64) (int64, bool) {
	for {
		avail := m.Available()
		if !m.options.allowOutOfMemoryAcquire && avail < ask {
			return avail, false
		}

		currReserved := m.reserved.Load()
		if currReserved < 0 {
			currReserved = 0
		}
		newReserved := currReserved + ask
		if m.reserved.CompareAndSwap(currReserved, newReserved) {
			return avail - ask, true
		}
	}
}

const (
	// cnFlushS3PinnedRejectRate is the hard ceiling on pinnedRate
	// (reserved / limit) for the CN S3 write throttler.
	//
	// Removing the old RSS-based physical-memory gate (#24892) relies on
	// this ceiling to bound S3 write-buffer growth: reserved is the only
	// signal that is both responsive (it drops immediately on flush, unlike
	// RSS, which lags behind frees because the allocator pools the memory
	// until FreeOSMemory runs) and not double-counted. For the ceiling to
	// actually cap physical memory it must be enforced on every grant, not
	// only as a soft entry check.
	cnFlushS3PinnedRejectRate = 0.80

	// cnFlushS3PinnedSmallBatchRate is the pinnedRate above which only small
	// batches may be pinned.
	cnFlushS3PinnedSmallBatchRate = 0.50

	// cnFlushS3SmallBatchMaxAsk is the largest ask allowed once pinnedRate is
	// at or above cnFlushS3PinnedSmallBatchRate.
	cnFlushS3SmallBatchMaxAsk = mpool.MB * 10
)

func AcquirePolicyForCNFlushS3(
	throttler *memThrottler,
	ask int64,
) (int64, bool) {
	return acquireWithinRSSLimit(throttler, ask)
}

func currentCNFlushS3RSSCovered(base, liveBase, reserved, currentLive int64) int64 {
	if base < 0 {
		base = 0
	}
	if liveBase < currentLive {
		liveBase = currentLive
	}
	if reserved < 0 {
		reserved = 0
	}

	liveOverlap := min(base, reserved)
	staleReusable := max(0, liveBase-currentLive)
	return min(base, liveOverlap+staleReusable)
}

func nextCNFlushS3RSSState(
	prevRSS, prevBase, prevLiveBase, rss, currentLive, sampledReserved int64,
) (base int64, liveBase int64) {
	if currentLive < 0 {
		currentLive = 0
	}
	if sampledReserved < 0 {
		sampledReserved = 0
	}

	carried := currentCNFlushS3RSSCovered(prevBase, prevLiveBase, sampledReserved, currentLive)
	staleReusable := max(0, carried-sampledReserved)
	if rssDrop := prevRSS - rss; rssDrop > 0 {
		staleReusable = max(0, staleReusable-rssDrop)
	}

	base = sampledReserved + staleReusable
	if base > rss {
		base = rss
	}
	liveBase = currentLive + staleReusable
	return
}

func cnFlushS3PhysicalAvailable(throttler *memThrottler, reserved int64) int64 {
	total := int64(throttler.actualTotalMemory.Load())
	if total <= 0 || total == math.MaxInt64 {
		return math.MaxInt64
	}
	base := throttler.rssReservedBase.Load()
	liveBase := throttler.rssMpoolLiveBase.Load()
	currentLive := mpool.GlobalStats().NumCurrBytes.Load()
	covered := currentCNFlushS3RSSCovered(base, liveBase, reserved, currentLive)
	used := throttler.rss.Load()
	if delta := reserved - covered; delta > 0 {
		used += delta
	}

	return max(0, total-used)
}

func cnFlushS3ProjectedPhysicalUsed(throttler *memThrottler, reserved int64) int64 {
	base := throttler.rssReservedBase.Load()
	liveBase := throttler.rssMpoolLiveBase.Load()
	currentLive := mpool.GlobalStats().NumCurrBytes.Load()
	covered := currentCNFlushS3RSSCovered(base, liveBase, reserved, currentLive)
	used := throttler.rss.Load()
	if delta := reserved - covered; delta > 0 {
		used += delta
	}

	return used
}

func (m *memThrottler) ShouldRefreshBeforeRelease() bool {
	reserved := m.reserved.Load()
	if reserved <= 0 {
		return false
	}
	currentLive := mpool.GlobalStats().NumCurrBytes.Load()
	covered := currentCNFlushS3RSSCovered(
		m.rssReservedBase.Load(),
		m.rssMpoolLiveBase.Load(),
		reserved,
		currentLive,
	)
	return covered < reserved
}

func acquireWithinRSSLimit(throttler *memThrottler, ask int64) (int64, bool) {
	limit := throttler.limit.Load()
	smallBatchCap := int64(float64(limit) * cnFlushS3PinnedSmallBatchRate)
	hardCap := int64(float64(limit) * cnFlushS3PinnedRejectRate)
	total := int64(throttler.actualTotalMemory.Load())

	for {
		observed := throttler.reserved.Load()
		if observed < 0 {
			// Release clamps atomically, so negative reserved should not occur in
			// normal operation. Self-heal it defensively to keep the throttler
			// robust against transient corruption or tests that seed negatives.
			if !throttler.reserved.CompareAndSwap(observed, 0) {
				continue
			}
			observed = 0
		}
		reserved := observed

		if !throttler.options.allowOutOfMemoryAcquire {
			if reserved >= hardCap {
				// almost exhausted the memory, cannot pin more batch memory.
				return 0, false
			}
			if reserved >= smallBatchCap && ask >= cnFlushS3SmallBatchMaxAsk {
				// once pinned more than half, only allow pinning small batch.
				return 0, false
			}
		}

		// Pool check: use limit - reserved directly instead of calling
		// Available(). The shared Available() subtracts RSS from the
		// pool headroom (actualMaxMemory - rss - reserved), which
		// double-counts S3 write buffers already reflected in RSS.
		// The pool only needs to bound forward-looking reservations
		// against the throttler limit.
		//
		avail := limit - reserved
		if !throttler.options.allowOutOfMemoryAcquire && avail < ask {
			return avail, false
		}

		newReserved := reserved + ask
		// Physical-memory backstop: the latest RSS sample already includes the
		// S3 bytes recorded in rssReservedBase, so only reserved growth above
		// that base should consume additional physical headroom. This avoids the
		// rss+reserved double-count that broke ForceRefresh() while still
		// rejecting new S3 growth when non-S3 RSS has already eaten the cgroup.
		if !throttler.options.allowOutOfMemoryAcquire &&
			total > 0 && total != math.MaxInt64 &&
			cnFlushS3ProjectedPhysicalUsed(throttler, newReserved) > total {
			return min(avail, cnFlushS3PhysicalAvailable(throttler, reserved)), false
		}
		if !throttler.options.allowOutOfMemoryAcquire && newReserved > hardCap {
			return max(0, hardCap-reserved), false
		}

		if throttler.reserved.CompareAndSwap(observed, newReserved) {
			return limit - newReserved, true
		}
	}
}

func AcquirePolicyForDataBranch(
	throttler *memThrottler,
	ask int64,
) (int64, bool) {

	total := int64(throttler.actualTotalMemory.Load())
	if total > 0 && throttler.limitRate > 0 {
		used := throttler.rss.Load() + ask
		if float64(used) > float64(total)*throttler.limitRate {
			return 0, false
		}
	}

	return defaultAcquirePolicy(throttler, ask)
}
