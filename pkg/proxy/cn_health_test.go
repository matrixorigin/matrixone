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
	"sync/atomic"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

// fakeClock is a controllable clock for deterministic breaker tests.
type fakeClock struct {
	mu  sync.Mutex
	now time.Time
}

func newFakeClock() *fakeClock {
	return &fakeClock{now: time.Unix(0, 0)}
}

func (c *fakeClock) Now() time.Time {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.now
}

func (c *fakeClock) advance(d time.Duration) {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.now = c.now.Add(d)
}

func uuids(cns []*CNServer) map[string]struct{} {
	m := make(map[string]struct{}, len(cns))
	for _, cn := range cns {
		m[cn.uuid] = struct{}{}
	}
	return m
}

// fail/ok are concise helpers to report a failure/success for a CN uuid.
func (h *cnHealthChecker) fail(uuid string) { h.reportFailure(uuid, uuid+"-addr") }
func (h *cnHealthChecker) ok(uuid string)   { h.reportSuccess(uuid, uuid+"-addr") }

// newTestBreaker builds a breaker that trips on the first failure, so the
// state-machine tests can focus on the cooldown/probe mechanics.
func newTestBreaker(clock *fakeClock) *cnHealthChecker {
	return newCNHealthChecker(
		withCNHealthClock(clock.Now),
		withCNHealthFailThreshold(1),
		withCNHealthCooldown(time.Second*5, time.Second*30),
	)
}

func TestCNHealthChecker_NilSafe(t *testing.T) {
	var h *cnHealthChecker
	// None of these should panic on a nil receiver.
	h.fail("cn1")
	h.ok("cn1")
	require.Equal(t, 0, h.unhealthyCount())
	cns := []*CNServer{{uuid: "cn1"}}
	got, allBusy := h.pick(cns)
	require.False(t, allBusy)
	require.Equal(t, cns, got)
}

func TestCNHealthChecker_AllHealthyByDefault(t *testing.T) {
	h := newCNHealthChecker()
	cns := []*CNServer{{uuid: "cn1"}, {uuid: "cn2"}, {uuid: "cn3"}}
	got, allBusy := h.pick(cns)
	require.False(t, allBusy)
	require.Equal(t, uuids(cns), uuids(got))
	require.Equal(t, 0, h.unhealthyCount())
}

// TestCNHealthChecker_DefaultThresholdToleratesSingleBlip verifies the default
// (failThreshold > 1): a single transient failure must NOT trip a CN, which is
// important for single-CN deployments.
func TestCNHealthChecker_DefaultThresholdToleratesSingleBlip(t *testing.T) {
	clock := newFakeClock()
	h := newCNHealthChecker(withCNHealthClock(clock.Now))
	require.GreaterOrEqual(t, h.failThreshold, 2)

	cns := []*CNServer{{uuid: "cn1"}}

	// One failure: still healthy (tolerated blip).
	h.fail("cn1")
	require.Equal(t, 0, h.unhealthyCount())
	got, allBusy := h.pick(cns)
	require.False(t, allBusy)
	require.Len(t, got, 1)

	// A success clears the single failure.
	h.ok("cn1")
	h.fail("cn1")
	require.Equal(t, 0, h.unhealthyCount(), "success must reset the failure counter")

	// Two consecutive failures trip it.
	h.fail("cn1")
	require.Equal(t, 1, h.unhealthyCount())
	_, allBusy = h.pick(cns)
	require.True(t, allBusy)
}

func TestCNHealthChecker_SkipsUnhealthyWhenHealthyExists(t *testing.T) {
	clock := newFakeClock()
	h := newTestBreaker(clock)

	cns := []*CNServer{{uuid: "cn1"}, {uuid: "cn2"}, {uuid: "cn3"}}

	// Trip cn1.
	h.fail("cn1")
	require.Equal(t, 1, h.unhealthyCount())

	got, allBusy := h.pick(cns)
	require.False(t, allBusy)
	// cn1 must be skipped, cn2 and cn3 returned.
	require.Equal(t, map[string]struct{}{"cn2": {}, "cn3": {}}, uuids(got))
}

// TestCNHealthChecker_ProbeRecoveredCNWithHealthyPeer verifies the mixed
// state: even if one healthy peer remains, a cooled-down CN still receives a
// bounded half-open probe so it can re-enter rotation promptly.
func TestCNHealthChecker_ProbeRecoveredCNWithHealthyPeer(t *testing.T) {
	clock := newFakeClock()
	h := newTestBreaker(clock)

	// cn2 stays healthy; cn1 trips and later cools down.
	cns := []*CNServer{
		{uuid: "cn1", addr: "cn1-addr"},
		{uuid: "cn2", addr: "cn2-addr"},
	}
	h.fail("cn1")

	// While cooling down, only the healthy peer is returned.
	got, allBusy := h.pick(cns)
	require.False(t, allBusy)
	require.Equal(t, map[string]struct{}{"cn2": {}}, uuids(got))

	// Once cooldown expires, cn1 must get a half-open probe even though cn2 is
	// still healthy.
	clock.advance(time.Second * 5)
	got, allBusy = h.pick(cns)
	require.False(t, allBusy)
	require.Len(t, got, 1)
	require.Equal(t, "cn1", got[0].uuid)
}

func TestCNHealthChecker_AllBusyFastFail(t *testing.T) {
	clock := newFakeClock()
	h := newTestBreaker(clock)

	cns := []*CNServer{{uuid: "cn1"}, {uuid: "cn2"}}
	h.fail("cn1")
	h.fail("cn2")

	// Both are in active cooldown -> allBusy, no candidates.
	got, allBusy := h.pick(cns)
	require.True(t, allBusy)
	require.Nil(t, got)
}

func TestCNHealthChecker_HalfOpenProbeAfterCooldown(t *testing.T) {
	clock := newFakeClock()
	h := newTestBreaker(clock)

	// Single CN to verify the one-probe-at-a-time guarantee per CN.
	cns := []*CNServer{{uuid: "cn1"}}
	h.fail("cn1")

	// Still cooling down.
	_, allBusy := h.pick(cns)
	require.True(t, allBusy)

	// After the base cooldown, exactly one half-open probe is handed out.
	clock.advance(time.Second * 5)
	got, allBusy := h.pick(cns)
	require.False(t, allBusy)
	require.Len(t, got, 1)
	require.Equal(t, "cn1", got[0].uuid)

	// A second concurrent route does NOT get another probe for the same CN
	// while one is in flight -> allBusy.
	got2, allBusy2 := h.pick(cns)
	require.True(t, allBusy2)
	require.Nil(t, got2)

	// Probe succeeds -> CN becomes healthy and is preferred again.
	h.ok("cn1")
	got3, allBusy3 := h.pick(cns)
	require.False(t, allBusy3)
	require.Equal(t, map[string]struct{}{"cn1": {}}, uuids(got3))
}

func TestCNHealthChecker_ProbesEachCNIndependently(t *testing.T) {
	clock := newFakeClock()
	h := newTestBreaker(clock)
	cns := []*CNServer{{uuid: "cn1"}, {uuid: "cn2"}}
	h.fail("cn1")
	h.fail("cn2")

	clock.advance(time.Second * 5)

	// Each CN gets its own half-open probe.
	probed := make(map[string]struct{})
	got, allBusy := h.pick(cns)
	require.False(t, allBusy)
	require.Len(t, got, 1)
	probed[got[0].uuid] = struct{}{}

	got, allBusy = h.pick(cns)
	require.False(t, allBusy)
	require.Len(t, got, 1)
	probed[got[0].uuid] = struct{}{}

	require.Equal(t, map[string]struct{}{"cn1": {}, "cn2": {}}, probed)

	// Both probes now in flight -> allBusy.
	_, allBusy = h.pick(cns)
	require.True(t, allBusy)
}

// TestCNHealthChecker_ProbeSlotReclaimedAfterWindow verifies a never-resolved
// probe does not wedge the breaker: once probeWindow elapses, a new probe is
// granted even without an explicit report.
func TestCNHealthChecker_ProbeSlotReclaimedAfterWindow(t *testing.T) {
	clock := newFakeClock()
	h := newCNHealthChecker(
		withCNHealthClock(clock.Now),
		withCNHealthFailThreshold(1),
		withCNHealthCooldown(time.Second*5, time.Second*30),
		withCNHealthProbeWindow(time.Second*15),
	)
	cns := []*CNServer{{uuid: "cn1"}}
	h.fail("cn1")

	clock.advance(time.Second * 5)
	got, allBusy := h.pick(cns)
	require.False(t, allBusy)
	require.Len(t, got, 1)

	// Probe never reported; before the window expires, no new probe.
	clock.advance(time.Second * 14)
	_, allBusy = h.pick(cns)
	require.True(t, allBusy)

	// After the probe window, the slot is reclaimed and a new probe granted.
	clock.advance(time.Second * 2)
	got, allBusy = h.pick(cns)
	require.False(t, allBusy)
	require.Len(t, got, 1)
}

func TestCNHealthChecker_ProbeFailureBacksOff(t *testing.T) {
	clock := newFakeClock()
	h := newCNHealthChecker(
		withCNHealthClock(clock.Now),
		withCNHealthFailThreshold(1),
		withCNHealthCooldown(time.Second*5, time.Second*40),
	)
	cns := []*CNServer{{uuid: "cn1"}}

	// 1st failure -> cooldown 5s.
	h.fail("cn1")
	_, allBusy := h.pick(cns)
	require.True(t, allBusy)
	clock.advance(time.Second*5 - time.Nanosecond)
	_, allBusy = h.pick(cns)
	require.True(t, allBusy)
	clock.advance(time.Nanosecond)
	got, allBusy := h.pick(cns)
	require.False(t, allBusy)
	require.Len(t, got, 1)

	// Probe fails -> 2nd failure -> cooldown doubles to 10s.
	h.fail("cn1")
	clock.advance(time.Second*5 + time.Second*4)
	_, allBusy = h.pick(cns)
	require.True(t, allBusy, "should still be cooling at 9s (<10s)")
	clock.advance(time.Second * 1)
	_, allBusy = h.pick(cns)
	require.False(t, allBusy, "probe should be available at 10s")
}

func TestCNHealthChecker_CooldownBackoffCapped(t *testing.T) {
	// threshold 1: failures==1 -> base; each extra failure doubles.
	h := newCNHealthChecker(
		withCNHealthFailThreshold(1),
		withCNHealthCooldown(time.Second*5, time.Second*30),
	)
	require.Equal(t, time.Second*5, h.cooldownFor(1))
	require.Equal(t, time.Second*10, h.cooldownFor(2))
	require.Equal(t, time.Second*20, h.cooldownFor(3))
	require.Equal(t, time.Second*30, h.cooldownFor(4), "capped at max")
	require.Equal(t, time.Second*30, h.cooldownFor(100), "still capped at max")
}

func TestCNHealthChecker_CooldownBackoffThreshold2(t *testing.T) {
	// Default-style threshold 2: cooldown starts at base when the breaker
	// trips (failures==2), then doubles for each further failure.
	h := newCNHealthChecker(
		withCNHealthFailThreshold(2),
		withCNHealthCooldown(time.Second*5, time.Second*30),
	)
	require.Equal(t, time.Second*5, h.cooldownFor(2))
	require.Equal(t, time.Second*10, h.cooldownFor(3))
	require.Equal(t, time.Second*20, h.cooldownFor(4))
	require.Equal(t, time.Second*30, h.cooldownFor(5), "capped at max")
}

func TestCNHealthChecker_CooldownBackoffAvoidsOverflow(t *testing.T) {
	// A huge base/max pair must saturate to max without overflowing on d *= 2.
	max := time.Duration(1<<62 + 123)
	base := time.Duration(1 << 62)
	h := newCNHealthChecker(
		withCNHealthFailThreshold(1),
		withCNHealthCooldown(base, max),
	)
	require.Equal(t, base, h.cooldownFor(1))
	require.Equal(t, max, h.cooldownFor(2))
}

func TestCNHealthChecker_SuccessResetsBackoff(t *testing.T) {
	clock := newFakeClock()
	h := newTestBreaker(clock)

	h.fail("cn1")
	h.fail("cn1") // consecutive: cooldown would be 10s
	h.ok("cn1")   // reset
	require.Equal(t, 0, h.unhealthyCount())

	// After reset, a fresh failure uses the base cooldown again.
	h.fail("cn1")
	clock.advance(time.Second * 5)
	got, allBusy := h.pick([]*CNServer{{uuid: "cn1"}})
	require.False(t, allBusy)
	require.Len(t, got, 1)
}

// TestCNHealthChecker_UnknownUUID covers the boundary paths: reportFailure on
// an unknown CN creates a fresh entry; reportSuccess on an unknown CN is a
// harmless no-op.
func TestCNHealthChecker_UnknownUUID(t *testing.T) {
	clock := newFakeClock()
	h := newCNHealthChecker(
		withCNHealthClock(clock.Now),
		withCNHealthFailThreshold(1),
	)

	// reportSuccess on a never-seen CN: no-op, no entry created.
	h.ok("never-seen")
	require.Empty(t, h.breakers)
	require.Equal(t, 0, h.unhealthyCount())

	// reportFailure on a never-seen CN: creates and (threshold 1) trips it.
	h.fail("fresh")
	require.Equal(t, 1, h.unhealthyCount())
	_, allBusy := h.pick([]*CNServer{{uuid: "fresh"}})
	require.True(t, allBusy)
}

// TestCNHealthChecker_SweepKeepsInFlightProbe verifies that the stale-entry
// sweep never reclaims an entry that has a half-open probe in flight, so the
// probe's later result lands on the same entry instead of a fresh one.
func TestCNHealthChecker_SweepKeepsInFlightProbe(t *testing.T) {
	clock := newFakeClock()
	h := newCNHealthChecker(
		withCNHealthClock(clock.Now),
		withCNHealthFailThreshold(1),
		withCNHealthCooldown(time.Second*5, time.Second*30),
		withCNHealthProbeWindow(time.Minute*30),
		withCNHealthSweep(time.Minute*10, time.Minute*5),
	)
	cns := []*CNServer{{uuid: "cn1"}}

	// Trip cn1, let the cooldown expire, then hand out a probe.
	h.fail("cn1")
	clock.advance(time.Second * 5)
	got, allBusy := h.pick(cns)
	require.False(t, allBusy)
	require.Len(t, got, 1)
	require.True(t, h.breakers["cn1"].probeInFlight)

	// Advance far enough to satisfy the stale TTL & sweep interval, but stay
	// within the (long) probe window so the probe is still in flight.
	clock.advance(time.Minute * 11)
	// Trigger a sweep via an unrelated CN.
	h.pick([]*CNServer{{uuid: "trigger"}})

	// The probing entry must survive the sweep.
	b, ok := h.breakers["cn1"]
	require.True(t, ok, "in-flight probe entry must not be swept")
	require.True(t, b.probeInFlight)

	// The probe succeeding still resolves the same entry.
	h.ok("cn1")
	require.Equal(t, 0, h.unhealthyCount())
}

func TestCNHealthChecker_ConcurrentAccess(t *testing.T) {
	clock := newFakeClock()
	h := newTestBreaker(clock)
	cns := []*CNServer{{uuid: "cn1"}, {uuid: "cn2"}, {uuid: "cn3"}}

	var wg sync.WaitGroup
	var picks int64
	for i := 0; i < 50; i++ {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			for j := 0; j < 100; j++ {
				h.fail("cn1")
				h.ok("cn2")
				if _, allBusy := h.pick(cns); !allBusy {
					atomic.AddInt64(&picks, 1)
				}
			}
		}(i)
	}
	wg.Wait()
	// cn2 and cn3 are healthy throughout, so every pick must succeed.
	require.Equal(t, int64(5000), atomic.LoadInt64(&picks))
}

// TestCNHealthChecker_SweepsStaleEntries verifies that idle breaker entries
// (sub-threshold healthy, or tripped-then-expired) for CNs that went quiet are
// eventually swept, while an entry still seeing failures is retained.
func TestCNHealthChecker_SweepsStaleEntries(t *testing.T) {
	clock := newFakeClock()
	h := newCNHealthChecker(
		withCNHealthClock(clock.Now),
		withCNHealthFailThreshold(2),
		withCNHealthCooldown(time.Second*5, time.Second*30),
		withCNHealthSweep(time.Minute*10, time.Minute*5),
	)

	// gone1: a single sub-threshold failure -> healthy entry lingers.
	h.fail("gone1")
	// gone2: tripped, its cooldown will expire while it stays quiet.
	h.fail("gone2")
	h.fail("gone2")
	// active: keeps failing, so it must survive the sweep.
	h.fail("active")
	require.Len(t, h.breakers, 3)

	// Move well past cooldown + staleTTL + sweepInterval.
	clock.advance(time.Minute * 11)
	// "active" failed recently relative to the new now -> not stale.
	h.fail("active")

	// Trigger a sweep via pick (unrelated CN satisfies the len>0 guard).
	h.pick([]*CNServer{{uuid: "trigger"}})

	_, hasGone1 := h.breakers["gone1"]
	_, hasGone2 := h.breakers["gone2"]
	_, hasActive := h.breakers["active"]
	require.False(t, hasGone1, "stale healthy entry should be swept")
	require.False(t, hasGone2, "stale expired entry should be swept")
	require.True(t, hasActive, "recently-failing entry must be retained")
}
