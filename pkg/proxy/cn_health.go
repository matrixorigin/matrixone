// Copyright 2021 - 2023 Matrix Origin
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

package proxy

import (
	"sync"
	"time"

	"github.com/matrixorigin/matrixone/pkg/logutil"
	v2 "github.com/matrixorigin/matrixone/pkg/util/metric/v2"
	"go.uber.org/zap"
)

const (
	// defaultCNHealthBaseCooldown is the initial cooldown applied to a CN
	// when its breaker first trips. It is intentionally short so that a
	// transient blip only sidelines a CN briefly, yet long enough that
	// follow-up connections do not keep paying the full auth timeout
	// (defaultAuthTimeout) on a known-bad CN.
	defaultCNHealthBaseCooldown = time.Second * 5
	// defaultCNHealthMaxCooldown caps the exponential backoff so a
	// persistently failing CN is still probed at least once per this window,
	// allowing it to recover automatically.
	defaultCNHealthMaxCooldown = time.Second * 30
	// defaultCNHealthFailThreshold is the number of consecutive connect
	// failures required before a CN's breaker opens.
	//
	// It is deliberately >1 so that a single transient blip (one timed-out or
	// refused connection) does not immediately sideline a CN. This matters
	// most for single-CN deployments, where tripping the only CN would make
	// new connections fast-fail with ErrAllCNServersBusy during the cooldown.
	// Requiring two consecutive failures means a lone hiccup is tolerated,
	// while a genuinely unhealthy CN (which fails repeatedly) is still taken
	// out of rotation quickly. It is configurable for operators who want a
	// more (1) or less aggressive policy.
	defaultCNHealthFailThreshold = 2
	// defaultCNHealthProbeWindow bounds how long a half-open probe may hold
	// the recovery slot for a CN. It is independent of the cooldown so that a
	// large maxCooldown cannot cause a never-resolved probe (an edge case) to
	// wedge the breaker for a long time. It must comfortably outlive a single
	// connect + auth attempt.
	defaultCNHealthProbeWindow = time.Second * 30
	// defaultCNHealthStaleTTL is how long an idle breaker entry (no failure
	// activity and not in active cooldown) is kept before it is swept. It
	// bounds the breakers map under CN churn (autoscaling, rolling restarts),
	// where CNs that briefly failed and then disappeared would otherwise leave
	// stale entries behind forever.
	defaultCNHealthStaleTTL = time.Minute * 10
	// defaultCNHealthSweepInterval is the minimum interval between stale-entry
	// sweeps. The sweep is amortized onto pick() so there is no extra timer.
	defaultCNHealthSweepInterval = time.Minute * 5
)

// cnBreaker holds the circuit-breaker state for a single CN server.
type cnBreaker struct {
	// consecutiveFailures counts connect failures since the last success.
	// It drives the exponential backoff of the cooldown.
	consecutiveFailures int
	// openUntil is the time until which the CN is considered unhealthy and
	// should be skipped. A zero value means the breaker is closed (healthy).
	openUntil time.Time
	// probeInFlight indicates that a half-open probe connection has been
	// handed out for this CN and has not yet been resolved.
	probeInFlight bool
	// probeDeadline bounds how long a probe may hold the half-open slot. It
	// guarantees that a probe which is never reported back (an edge case)
	// cannot wedge the breaker into a permanently half-open state.
	probeDeadline time.Time
	// lastFailure is when the most recent failure was reported. It is used to
	// detect idle entries that can be swept.
	lastFailure time.Time
}

// cnHealthChecker is a per-CN circuit breaker used by the router to avoid
// repeatedly routing new connections to CN servers that are currently unable
// to accept them (for example because they are overloaded and the handshake
// times out).
//
// Design goals (see also the package-level routing logic):
//   - CNs in active cooldown are skipped entirely, so new connections never
//     pay the auth timeout on a known-bad CN.
//   - Once a cooldown expires, exactly one bounded half-open probe per CN is
//     allowed through even if other peers are healthy, so a recovered CN can
//     re-enter rotation promptly instead of staying sidelined until stale
//     sweeping.
//   - Unhealthy state is time-bounded and self-healing via exponential backoff
//     plus a single half-open probe, so a recovered CN is brought back quickly
//     without a thundering herd.
//   - When every candidate CN is unhealthy, the router fast-fails with a busy
//     error instead of hanging on a timeout or returning a misleading
//     "no available CN server" error.
//
// All methods are safe for concurrent use, and are nil-receiver safe so the
// breaker can be disabled by leaving it nil.
type cnHealthChecker struct {
	mu       sync.Mutex
	breakers map[string]*cnBreaker

	// now returns the current time. It is a field so tests can use a fake
	// clock.
	//
	// In production this is time.Now, whose readings carry a monotonic clock
	// component. All breaker time comparisons (Before/Sub) are between two
	// now()-derived instants, so they use the monotonic clock and are immune
	// to wall-clock jumps (e.g. NTP stepping backwards): a cooldown can never
	// be extended or shortened by a clock adjustment.
	now func() time.Time

	failThreshold int
	baseCooldown  time.Duration
	maxCooldown   time.Duration
	// probeWindow bounds the lifetime of a half-open probe slot. It is
	// intentionally decoupled from maxCooldown.
	probeWindow time.Duration

	// staleTTL and sweepInterval bound the breakers map under CN churn. The
	// sweep is amortized onto pick(); lastSweep tracks when it last ran.
	staleTTL      time.Duration
	sweepInterval time.Duration
	lastSweep     time.Time
	// lastAllBusyLog rate-limits the "all CN busy" warning.
	lastAllBusyLog time.Time
}

// cnHealthAllBusyLogInterval rate-limits the "all candidate CN servers are
// unhealthy" warning so a full outage cannot storm the log.
const cnHealthAllBusyLogInterval = time.Second * 30

type cnHealthOption func(*cnHealthChecker)

func withCNHealthClock(now func() time.Time) cnHealthOption {
	return func(h *cnHealthChecker) {
		h.now = now
	}
}

func withCNHealthCooldown(base, max time.Duration) cnHealthOption {
	return func(h *cnHealthChecker) {
		if base > 0 {
			h.baseCooldown = base
		}
		if max > 0 {
			h.maxCooldown = max
		}
	}
}

// withCNHealthFailThreshold sets the number of consecutive failures required
// to trip a CN's breaker. Values < 1 are ignored.
func withCNHealthFailThreshold(threshold int) cnHealthOption {
	return func(h *cnHealthChecker) {
		if threshold >= 1 {
			h.failThreshold = threshold
		}
	}
}

// withCNHealthProbeWindow overrides the probe-slot lifetime (mainly for tests).
func withCNHealthProbeWindow(d time.Duration) cnHealthOption {
	return func(h *cnHealthChecker) {
		if d > 0 {
			h.probeWindow = d
		}
	}
}

// withCNHealthSweep overrides the stale-entry TTL and sweep interval (mainly
// for tests).
func withCNHealthSweep(staleTTL, interval time.Duration) cnHealthOption {
	return func(h *cnHealthChecker) {
		if staleTTL > 0 {
			h.staleTTL = staleTTL
		}
		if interval > 0 {
			h.sweepInterval = interval
		}
	}
}

// newCNHealthChecker creates a CN health checker with sane defaults.
func newCNHealthChecker(opts ...cnHealthOption) *cnHealthChecker {
	h := &cnHealthChecker{
		breakers:      make(map[string]*cnBreaker),
		now:           time.Now,
		failThreshold: defaultCNHealthFailThreshold,
		baseCooldown:  defaultCNHealthBaseCooldown,
		maxCooldown:   defaultCNHealthMaxCooldown,
		probeWindow:   defaultCNHealthProbeWindow,
		staleTTL:      defaultCNHealthStaleTTL,
		sweepInterval: defaultCNHealthSweepInterval,
	}
	for _, opt := range opts {
		opt(h)
	}
	if h.maxCooldown < h.baseCooldown {
		h.maxCooldown = h.baseCooldown
	}
	// A probe must be allowed to outlive a full connect + auth attempt,
	// otherwise it could be reclaimed while still legitimately in flight.
	if h.probeWindow < h.baseCooldown {
		h.probeWindow = h.baseCooldown
	}
	h.lastSweep = h.now()
	return h
}

// cooldownFor computes the exponential-backoff cooldown for the given number
// of consecutive failures: baseCooldown at the trip threshold, doubling for
// each further failure, clamped to maxCooldown. The capped loop (rather than a
// shift) avoids int64 overflow for large failure counts.
func (h *cnHealthChecker) cooldownFor(failures int) time.Duration {
	d := h.baseCooldown
	for i := h.failThreshold; i < failures; i++ {
		if d >= h.maxCooldown {
			return h.maxCooldown
		}
		// Saturate before doubling so a very large configured cooldown cannot
		// overflow time.Duration on the multiply itself.
		if d > h.maxCooldown/2 {
			return h.maxCooldown
		}
		d *= 2
	}
	if d > h.maxCooldown {
		return h.maxCooldown
	}
	return d
}

// reportSuccess marks a CN as healthy, closing its breaker and clearing any
// failure history. addr is used only for logging. It is nil-receiver safe.
func (h *cnHealthChecker) reportSuccess(uuid, addr string) {
	if h == nil {
		return
	}
	h.mu.Lock()
	defer h.mu.Unlock()
	if b, ok := h.breakers[uuid]; ok {
		wasOpen := !b.openUntil.IsZero()
		// Healthy is represented by the absence of a breaker entry.
		delete(h.breakers, uuid)
		if wasOpen {
			logutil.Info("proxy CN health recovered",
				zap.String("cn", uuid),
				zap.String("address", addr))
		}
	}
}

// reportFailure records a connect failure for a CN and, once the failure
// threshold is reached, opens its breaker for an exponentially backed-off
// cooldown. addr is used only for logging. It is nil-receiver safe.
func (h *cnHealthChecker) reportFailure(uuid, addr string) {
	if h == nil {
		return
	}
	h.mu.Lock()
	defer h.mu.Unlock()
	now := h.now()
	b, ok := h.breakers[uuid]
	if !ok {
		b = &cnBreaker{}
		h.breakers[uuid] = b
	}
	b.consecutiveFailures++
	b.lastFailure = now
	wasProbing := b.probeInFlight
	// A failed probe (or any failure) releases the half-open slot.
	b.probeInFlight = false
	b.probeDeadline = time.Time{}
	if b.consecutiveFailures >= h.failThreshold {
		wasOpen := !b.openUntil.IsZero()
		cooldown := h.cooldownFor(b.consecutiveFailures)
		b.openUntil = now.Add(cooldown)
		switch {
		case !wasOpen:
			// closed -> open: the CN just became unhealthy.
			v2.ProxyConnectCNHealthTripCounter.Inc()
			logutil.Warn("proxy CN health tripped",
				zap.String("cn", uuid),
				zap.String("address", addr),
				zap.Int("consecutive_failures", b.consecutiveFailures),
				zap.Duration("cooldown", cooldown),
				zap.Time("open_until", b.openUntil))
		default:
			// Already open: this is a continued failure, which for an open
			// CN means a half-open probe just failed. Without a distinct
			// signal here a CN stuck in a trip -> probe-fail -> re-trip loop
			// would only ever log its first trip, hiding a persistent outage.
			// The probe rate is bounded (one per CN per cooldown), so this
			// cannot storm.
			v2.ProxyConnectCNHealthRetripCounter.Inc()
			logutil.Warn("proxy CN health probe failed, re-tripping",
				zap.String("cn", uuid),
				zap.String("address", addr),
				zap.Bool("after_probe", wasProbing),
				zap.Int("consecutive_failures", b.consecutiveFailures),
				zap.Duration("cooldown", cooldown),
				zap.Time("open_until", b.openUntil))
		}
	}
}

// sweepStale removes idle breaker entries to bound the map under CN churn.
// The caller must hold h.mu. An entry is idle when it is not in an active
// cooldown (healthy, or its cooldown has already expired), has no half-open
// probe in flight, and has reported no failure within staleTTL. Such entries
// belong to CNs that briefly failed and then went quiet (most commonly because
// they were removed from the cluster). Deleting them is safe: an absent entry
// is treated as healthy, and a CN that is still genuinely unhealthy keeps
// failing/being probed, which refreshes lastFailure and excludes it from the
// sweep.
func (h *cnHealthChecker) sweepStale(now time.Time) {
	if now.Sub(h.lastSweep) < h.sweepInterval {
		return
	}
	h.lastSweep = now
	for uuid, b := range h.breakers {
		inActiveCooldown := !b.openUntil.IsZero() && now.Before(b.openUntil)
		// Never reclaim an entry whose probe is still legitimately in flight,
		// otherwise the probe's later report would land on a fresh entry and
		// reset its failure history.
		probing := b.probeInFlight && now.Before(b.probeDeadline)
		if !inActiveCooldown && !probing && now.Sub(b.lastFailure) > h.staleTTL {
			delete(h.breakers, uuid)
		}
	}
}

// pick applies the breaker state to the candidate CN servers and returns the
// subset the caller should balance across. It is nil-receiver safe.
//
// The returned allBusy is true only when every candidate is unhealthy and
// still in active cooldown (no half-open probe available); the caller should
// then fast-fail with a busy error rather than block on a doomed connection.
//
// Invariant: when allBusy is false and cns is non-empty, the returned slice is
// non-empty. The breaker never causes a hard "no CN" failure on its own.
//
// Invariant: a half-open probe is always returned as a single-element slice,
// and probeInFlight is only ever set in that branch. The caller (Route) must
// therefore short-circuit single-element results and return them directly,
// so a probe is never passed on to a secondary balancer that might drop it
// and leak the probe slot.
func (h *cnHealthChecker) pick(cns []*CNServer) (candidates []*CNServer, allBusy bool) {
	if h == nil || len(cns) == 0 {
		return cns, false
	}
	h.mu.Lock()
	defer h.mu.Unlock()
	now := h.now()

	// Amortized housekeeping: drop idle entries so the map stays bounded
	// under CN churn.
	h.sweepStale(now)

	// Collect both fully-healthy (closed) CNs and any CNs whose cooldown has
	// expired and are eligible for a half-open recovery probe.
	var closed []*CNServer
	var probe *CNServer
	var probeBreaker *cnBreaker
	for _, cn := range cns {
		b := h.breakers[cn.uuid]
		if b == nil {
			closed = append(closed, cn)
			continue
		}
		if b.openUntil.IsZero() {
			closed = append(closed, cn)
			continue
		}
		if now.Before(b.openUntil) {
			// Still in active cooldown.
			continue
		}
		if b.probeInFlight && now.Before(b.probeDeadline) {
			// A probe is already in flight for this CN.
			continue
		}
		if probe == nil || b.openUntil.Before(probeBreaker.openUntil) {
			probe, probeBreaker = cn, b
		}
	}
	if probe != nil {
		// Hand out a single half-open probe even if other peers are healthy:
		// otherwise a cooled-down CN would never re-enter rotation while one
		// healthy peer stayed available, and could remain sidelined until stale
		// sweeping. This caps recovery traffic to one connection per CN per
		// cooldown window, avoiding a thundering herd onto a CN that may still
		// be unhealthy.
		probeBreaker.probeInFlight = true
		probeBreaker.probeDeadline = now.Add(h.probeWindow)
		v2.ProxyConnectCNHealthProbeCounter.Inc()
		// Bounded to one probe per CN per cooldown window, so this is safe to
		// log; it makes recovery attempts visible. Debug level keeps it quiet
		// for normal operation.
		logutil.Debug("proxy CN health probing recovery",
			zap.String("cn", probe.uuid),
			zap.String("address", probe.addr))
		return []*CNServer{probe}, false
	}
	if len(closed) > 0 {
		return closed, false
	}

	// Every candidate is unhealthy and still cooling down (or already being
	// probed). Tell the caller to fast-fail with a busy error.
	v2.ProxyConnectCNAllBusyCounter.Inc()
	// Surface a log when the whole candidate set is unavailable, but rate-limit
	// it: during a full outage pick() is hit on every connection, so an
	// ungated log would storm. The counter still increments every time.
	if now.Sub(h.lastAllBusyLog) >= cnHealthAllBusyLogInterval {
		h.lastAllBusyLog = now
		logutil.Warn("proxy: all candidate CN servers are temporarily unhealthy",
			zap.Int("candidate_count", len(cns)))
	}
	return nil, true
}

// unhealthyCount returns the number of CN servers whose breaker is currently
// tripped. It is intended for logging/metrics and is nil-receiver safe.
func (h *cnHealthChecker) unhealthyCount() int {
	if h == nil {
		return 0
	}
	h.mu.Lock()
	defer h.mu.Unlock()
	var n int
	for _, b := range h.breakers {
		if !b.openUntil.IsZero() {
			n++
		}
	}
	return n
}

// canReuseCachedCN reports whether a cached backend connection to this CN may
// be reused for a fresh client login. The policy is deliberately stricter than
// Route(): a cached connection may be reused only when the breaker is closed
// (or absent). If a CN is in active cooldown or waiting for a half-open probe,
// cached reuse would bypass the intended health gate and must be rejected.
func (h *cnHealthChecker) canReuseCachedCN(uuid string) bool {
	if h == nil || uuid == "" {
		return true
	}
	h.mu.Lock()
	defer h.mu.Unlock()
	b := h.breakers[uuid]
	return b == nil || b.openUntil.IsZero()
}
