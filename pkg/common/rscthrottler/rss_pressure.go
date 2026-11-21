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
	"math"
	"path"
	"runtime/debug"
	"strings"
	"sync"
	"time"

	"github.com/matrixorigin/matrixone/pkg/common/moerr"
	"github.com/matrixorigin/matrixone/pkg/fileservice"
	"github.com/matrixorigin/matrixone/pkg/logutil"
	metric "github.com/matrixorigin/matrixone/pkg/util/metric/v2"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/common"
	"go.uber.org/zap"
)

const (
	rssScavengeInterval    = time.Minute
	rssCacheEvictTimeout   = 10 * time.Second
	rssPressureTTL         = 2 * time.Minute
	rssScavengeVisibleRate = 0.70
	rssPressureSoftEnter   = 0.85
	rssPressureHardEnter   = 0.92
	rssPressureHardExit    = 0.88
	rssPressureSoftExit    = 0.80
	rssCacheSoftTarget     = int64(80)
	rssCacheHardTarget     = int64(50)
	rssCachePressureOwner  = "process-rss"
)

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

var processRSSPressure = rssPressureController{
	setTarget: func(percent int64, until time.Time) {
		fileservice.SetMemoryCachePressureTargetPercentByOwner(rssCachePressureOwner, percent, until)
	},
	clearTarget: func() {
		fileservice.ClearMemoryCachePressureTargetByOwner(rssCachePressureOwner)
	},
	evict: func(ctx context.Context, percent int64) {
		fileservice.EvictMemoryCachesToCapacityPercent(ctx, percent)
	},
	free: debug.FreeOSMemory,
}

// RSSPressureSource registers the lifetime of a throttler's process RSS reports.
// Closing it leaves other reporters and their shared recovery work active.
type RSSPressureSource struct {
	controller *rssPressureController
}

// NewRSSPressureSource attaches a reporter to process-wide RSS recovery.
func NewRSSPressureSource() *RSSPressureSource {
	return processRSSPressure.attach()
}

type rssPressureLimit struct {
	bytes uint64
	// Logical cgroup v2 hierarchy paths, independent of mount aliases.
	// Other sources leave both empty.
	readRoot   string
	knownRoot  string
	incomplete bool // The limit is only an upper bound when a level could not be read.
}

func cgroupPathContains(root, group string) bool {
	return root == "/" || group == root || strings.HasPrefix(group, root+"/")
}

// coveringCgroupRoot keeps one bound for the lease, even when the process
// reports from different subtrees. It never forgets an observed ancestor.
func coveringCgroupRoot(root, other string) string {
	if root == "" {
		return other
	}
	if other == "" {
		return root
	}
	for !cgroupPathContains(root, other) {
		root = path.Dir(root)
	}
	return root
}

type rssPressureController struct {
	// Order RSS measurements without blocking reporter detach or cache work.
	sampleMu sync.Mutex
	mu       sync.Mutex
	sources  map[*RSSPressureSource]struct{}
	// A successful RSS capture can rule out an older recovery decision
	// even when its own ceiling cannot be resolved.
	lastRSSCapture time.Time
	lastSample     time.Time
	lastEvict      time.Time
	lastFree       time.Time
	expires        time.Time
	leaseRoot      string
	state          rssPressureState
	pending        bool
	running        bool
	cancelEviction context.CancelFunc
	freeRunning    bool

	setTarget   func(int64, time.Time)
	clearTarget func()
	evict       func(context.Context, int64)
	free        func()
}

func (c *rssPressureController) attach() *RSSPressureSource {
	c.mu.Lock()
	defer c.mu.Unlock()
	if c.sources == nil {
		c.sources = make(map[*RSSPressureSource]struct{})
	}
	source := &RSSPressureSource{controller: c}
	c.sources[source] = struct{}{}
	return source
}

// Close stops reports and cancels recovery on last detach. The process worker
// retains admitted callbacks until they return.
func (s *RSSPressureSource) Close() {
	if s == nil {
		return
	}
	c := s.controller
	c.mu.Lock()
	defer c.mu.Unlock()
	if _, ok := c.sources[s]; !ok {
		return
	}
	delete(c.sources, s)
	if len(c.sources) != 0 {
		return
	}
	if c.state != rssPressureNone {
		c.clearTarget()
	}
	c.resetEviction()
	c.state = rssPressureNone
	c.lastRSSCapture = time.Time{}
	c.lastSample = time.Time{}
	c.lastEvict = time.Time{}
	c.expires = time.Time{}
	c.leaseRoot = ""
	c.sources = nil
}

// resetEviction invalidates queued work and requests cancellation of the active walk.
// The worker retains ownership until the callback has returned.
func (c *rssPressureController) resetEviction() {
	c.pending = false
	if c.cancelEviction != nil {
		c.cancelEviction()
	}
}

// Called under sampleMu, in RSS capture order.
func (s *RSSPressureSource) recordRSSCapture(at time.Time) {
	c := s.controller
	c.mu.Lock()
	defer c.mu.Unlock()
	if _, ok := c.sources[s]; ok {
		c.lastRSSCapture = at
	}
}

// recordCgroupCoverage widens a live lease's required scope before potentially
// blocking hierarchy reads. It does not publish a pressure measurement.
func (s *RSSPressureSource) recordCgroupCoverage(at time.Time, root string) {
	c := s.controller
	c.mu.Lock()
	defer c.mu.Unlock()
	now := time.Now()
	if _, live := c.sources[s]; live && now.Before(c.expires) && now.Before(at.Add(rssPressureTTL)) {
		c.setLeaseRoot(coveringCgroupRoot(c.leaseRoot, root))
	}
}

func (c *rssPressureController) setLeaseRoot(root string) {
	if root != c.leaseRoot {
		// Retain only the path, not the backing mountinfo snapshot.
		c.leaseRoot = strings.Clone(root)
	}
}

func (s *RSSPressureSource) observe(at time.Time, rss, reserved, live int64, ceiling rssPressureLimit) {
	validLimit := ceiling.bytes != 0 && ceiling.bytes < math.MaxInt64
	if !validLimit && ceiling.knownRoot == "" {
		return
	}
	c := s.controller
	c.mu.Lock()
	now := time.Now()
	expires := at.Add(rssPressureTTL)
	if _, ok := c.sources[s]; !ok || !now.Before(expires) {
		c.mu.Unlock()
		return
	}
	previous := c.state
	requiredRoot := ceiling.knownRoot
	if now.Before(c.expires) {
		requiredRoot = coveringCgroupRoot(c.leaseRoot, requiredRoot)
		// The lease may have started while the hierarchy probe was in flight.
		c.setLeaseRoot(requiredRoot)
	} else {
		previous = rssPressureNone
	}
	if !validLimit || at.Before(c.lastSample) {
		c.mu.Unlock()
		return
	}
	limit := int64(ceiling.bytes)
	latestCapture := !at.Before(c.lastRSSCapture)
	partial := ceiling.incomplete || ceiling.readRoot != "" && !cgroupPathContains(ceiling.readRoot, requiredRoot)
	next := nextRSSPressureState(previous, float64(rss)/float64(limit))
	// An upper bound can establish pressure, but cannot prove recovery.
	if partial && next == rssPressureNone ||
		next < previous && (partial || !latestCapture) {
		c.mu.Unlock()
		return
	}
	c.lastSample = at
	c.state = next
	target := rssPressureCacheTarget(c.state)
	if previous != c.state {
		c.resetEviction()
		if target == 0 || previous == rssPressureHard {
			c.clearTarget()
		}
	}
	if target == 0 {
		c.expires = time.Time{}
		c.leaseRoot = ""
		c.mu.Unlock()
		if previous != rssPressureNone {
			logRSSPressure(rss, limit, max(0, reserved+live), 0, false, false, previous, rssPressureNone)
		}
		return
	}

	c.expires = expires
	c.setLeaseRoot(requiredRoot)
	c.setTarget(target, c.expires)
	evict := previous < c.state || previous == c.state && at.Sub(c.lastEvict) > rssScavengeInterval
	if evict {
		c.lastEvict = at
		c.pending = true
		if !c.running {
			c.running = true
			go c.runEvictions()
		}
	}

	// Visibility is measured by the reporting throttler in the same refresh.
	visible := max(0, reserved+live)
	free := !c.freeRunning && float64(visible) < float64(rss)*rssScavengeVisibleRate &&
		at.Sub(c.lastFree) > rssScavengeInterval
	if free {
		c.lastFree = at
		c.freeRunning = true
	}
	state := c.state
	c.mu.Unlock()

	if free || evict || previous != state {
		logRSSPressure(rss, limit, visible, target, free, evict, previous, state)
	}
	if free {
		c.free()
		c.mu.Lock()
		c.freeRunning = false
		c.mu.Unlock()
	}
}

func (c *rssPressureController) runEvictions() {
	for {
		c.mu.Lock()
		now := time.Now()
		if !c.pending || !now.Before(c.expires) {
			c.pending = false
			c.running = false
			c.mu.Unlock()
			return
		}
		c.pending = false
		target := rssPressureCacheTarget(c.state)
		deadline := now.Add(rssCacheEvictTimeout)
		if c.expires.Before(deadline) {
			deadline = c.expires
		}
		ctx, cancel := context.WithDeadlineCause(context.Background(), deadline, moerr.CauseRSSCacheEvict)
		c.cancelEviction = cancel
		c.mu.Unlock()

		c.evict(ctx, target)
		c.mu.Lock()
		free := ctx.Err() == nil && time.Now().Before(deadline)
		cancel()
		c.cancelEviction = nil
		c.mu.Unlock()
		if free {
			c.free()
		}
	}
}

func logRSSPressure(rss, limit, visible, target int64, free, evict bool, previous, current rssPressureState) {
	metric.FSCachePressureTriggerCounter.Inc()
	logutil.Info(MemoryThrottlerLogHeader+"-RSSScavenge",
		zap.String("rss", common.HumanReadableBytes(int(rss))),
		zap.String("actual-total-memory", common.HumanReadableBytes(int(limit))),
		zap.String("visible", common.HumanReadableBytes(int(visible))),
		zap.Bool("free-os-memory", free),
		zap.Bool("evict-cache", evict),
		zap.Bool("clear-cache-target", current == rssPressureNone),
		zap.Int64("cache-target-percent", target),
		zap.String("pressure-state", current.String()),
		zap.String("previous-pressure-state", previous.String()))
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
