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
	"runtime/debug"
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

type rssPressureController struct {
	// Order RSS measurements without blocking reporter detach or cache work.
	sampleMu       sync.Mutex
	mu             sync.Mutex
	sources        map[*RSSPressureSource]struct{}
	lastSample     time.Time
	lastEvict      time.Time
	lastFree       time.Time
	expires        time.Time
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
	c.lastSample = time.Time{}
	c.lastEvict = time.Time{}
	c.expires = time.Time{}
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

func (s *RSSPressureSource) observe(at time.Time, rss, limit, reserved, live int64) {
	if limit <= 0 || limit == math.MaxInt64 {
		return
	}
	c := s.controller
	c.mu.Lock()
	expires := at.Add(rssPressureTTL)
	if _, ok := c.sources[s]; !ok || at.Before(c.lastSample) || !time.Now().Before(expires) {
		c.mu.Unlock()
		return
	}
	c.lastSample = at
	previous := c.state
	c.state = nextRSSPressureState(previous, float64(rss)/float64(limit))
	target := rssPressureCacheTarget(c.state)
	if previous != c.state {
		c.resetEviction()
		if target == 0 || previous == rssPressureHard {
			c.clearTarget()
		}
	}
	if target == 0 {
		c.expires = time.Time{}
		c.mu.Unlock()
		if previous != rssPressureNone {
			logRSSPressure(rss, limit, max(0, reserved+live), 0, false, false, previous, rssPressureNone)
		}
		return
	}

	c.expires = expires
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
