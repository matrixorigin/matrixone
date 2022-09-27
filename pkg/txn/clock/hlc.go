// Copyright 2022 Matrix Origin
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
// http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package clock

import (
	"context"
	"log"
	"sync"
	"time"

	"github.com/matrixorigin/matrixone/pkg/common/stopper"
	"github.com/matrixorigin/matrixone/pkg/pb/timestamp"
)

func toMicrosecond(nanoseconds int64) int64 {
	return nanoseconds / 1000
}

func physicalClock() int64 {
	return time.Now().UTC().UnixNano()
}

// SkipClockUncertainityPeriodOnRestart will cause the current goroutine to
// sleep to skip clock uncertainty period. This function must be called during
// the restart of the node.
//
// The system assumes that each node keeps synchronizing with accurate NTP
// servers to have the clock offset limited under HLCClock.maxOffset.
func SkipClockUncertainityPeriodOnRestart(ctx context.Context, clock Clock) {
	now, sleepUntil := clock.Now()
	for now.Less(sleepUntil) {
		time.Sleep(time.Millisecond)
		select {
		case <-ctx.Done():
			return
		default:
		}

		now, _ = clock.Now()
	}
}

// HLCClock is an implementation of the Hybrid Logical Clock as described in
// the paper titled -
//
// Logical Physical Clocks and Consistent Snapshots in Globally Distributed
// Databases
type HLCClock struct {
	maxOffset     time.Duration
	physicalClock func() int64

	mu struct {
		sync.Mutex
		// maxLearnedPhysicalTime is the max learned physical time as defined in
		// section 3.3 of the HLC paper.
		maxLearnedPhysicalTime int64
		// ts records the last HLC timestamp returned by the Now() method.
		ts timestamp.Timestamp
	}
}

var _ Clock = (*HLCClock)(nil)

// NewHLCClock returns a new HLCClock instance. The maxOffset parameter specifies
// the max allowed clock offset. The clock readings returned by clock must be
// within the maxOffset bound across the entire cluster.
func NewHLCClock(clock func() int64, maxOffset time.Duration) *HLCClock {
	if clock == nil {
		panic("physical clock source not specified")
	}

	return &HLCClock{
		physicalClock: clock,
		maxOffset:     maxOffset,
	}
}

// NewUnixNanoHLCClock returns a new HLCClock instance backed by the local wall
// clock in Unix epoch nanoseconds. Nodes' clock must be periodically
// synchronized, e.g. via NTP, to ensure that wall time readings are within the
// maxOffset offset between any two nodes.
func NewUnixNanoHLCClock(ctx context.Context, maxOffset time.Duration) *HLCClock {
	clock := &HLCClock{
		physicalClock: physicalClock,
		maxOffset:     maxOffset,
	}

	go func() {
		clock.offsetMonitor(ctx)
	}()

	return clock
}

// NewUnixNanoHLCClockWithStopper is similar to NewUnixNanoHLCClock, but perform
// clock check use stopper
func NewUnixNanoHLCClockWithStopper(stopper *stopper.Stopper, maxOffset time.Duration) *HLCClock {
	clock := &HLCClock{
		physicalClock: physicalClock,
		maxOffset:     maxOffset,
	}
	if maxOffset > 0 {
		if err := stopper.RunTask(clock.offsetMonitor); err != nil {
			panic(err)
		}
	}
	return clock
}

// HasNetworkLatency returns a boolean value indicating whether there is network
// latency involved when retrieving timestamps. There is no such network latency
// in HLCClock.
func (c *HLCClock) HasNetworkLatency() bool {
	return false
}

// MaxOffset returns the max offset of the physical clocks in the cluster.
func (c *HLCClock) MaxOffset() time.Duration {
	return c.maxOffset
}

// Now returns the current HLC timestamp and the upper bound timestamp of the
// current time in hlc.
func (c *HLCClock) Now() (timestamp.Timestamp, timestamp.Timestamp) {
	now := c.now()
	return now, timestamp.Timestamp{PhysicalTime: now.PhysicalTime + int64(c.maxOffset)}
}

// Update is called whenever messages are received from other nodes. HLC
// timestamp carried by those messages are used to update the local HLC
// clock to capture casual relationships.
func (c *HLCClock) Update(m timestamp.Timestamp) {
	c.update(m)
}

func (c *HLCClock) maxClockForwardOffset() time.Duration {
	return c.maxOffset / 2
}

func (c *HLCClock) clockOffsetMonitoringInterval() time.Duration {
	return c.maxClockForwardOffset() / 3
}

func (c *HLCClock) offsetMonitor(ctx context.Context) {
	c.getPhysicalClock()
	ticker := time.NewTicker(c.clockOffsetMonitoringInterval())
	defer ticker.Stop()
	for {
		select {
		case <-ticker.C:
			c.getPhysicalClock()
		case <-ctx.Done():
			return
		}
	}
}

func (c *HLCClock) getPhysicalClock() int64 {
	newPts := c.physicalClock()
	oldPts := c.keepPhysicalClock(newPts)
	if c.maxOffset > 0 {
		c.handleClockJump(oldPts, newPts)
	}

	return newPts
}

// keepPhysicalClock updates the c.mu.maxLearnedPhysicalTime field when
// necessary and returns the previous maxLearnedPhysicalTime value.
func (c *HLCClock) keepPhysicalClock(pts int64) int64 {
	c.mu.Lock()
	defer c.mu.Unlock()

	old := c.mu.maxLearnedPhysicalTime
	if pts > c.mu.maxLearnedPhysicalTime {
		c.mu.maxLearnedPhysicalTime = pts
	}

	return old
}

// handleClockJump is called everytime when a physical time is read from the
// local wall clock. it logs backward jump whenever it happens, jumps are
// compared with maxClockForwardOffset() and cause fatal fail when the
// maxClockForwardOffset constrain is violated.
//
// handleClockJump assumes that there are periodic background probes to the
// physical wall clock to monitor and learn its values. the probe interval
// is required to be smaller than the maxClockForwardOffset().
func (c *HLCClock) handleClockJump(oldPts int64, newPts int64) {
	if oldPts == 0 {
		return
	}

	jump := int64(0)
	if oldPts > newPts {
		jump = oldPts - newPts
		log.Printf("clock backward jump observed, %d microseconds", toMicrosecond(jump))
	} else {
		jump = newPts - oldPts
	}

	if jump > int64(c.maxClockForwardOffset()+c.clockOffsetMonitoringInterval()) {
		log.Fatalf("big clock jump observed, %d microseconds", toMicrosecond(jump))
	}
}

// now returns the current HLC timestamp. it implements the Send or Local event
// part of Figure 5 of the HLC paper.
func (c *HLCClock) now() timestamp.Timestamp {
	newPts := c.getPhysicalClock()
	c.mu.Lock()
	defer c.mu.Unlock()
	if c.mu.ts.PhysicalTime >= newPts {
		c.mu.ts.LogicalTime++
	} else {
		c.mu.ts = timestamp.Timestamp{PhysicalTime: newPts}
	}

	return c.mu.ts
}

// update updates the HLCClock based on the received Timestamp, it implements
// the Receive Event of message m part of Figure 5 of the HLC paper.
func (c *HLCClock) update(m timestamp.Timestamp) {
	newPts := c.getPhysicalClock()

	c.mu.Lock()
	defer c.mu.Unlock()
	if newPts > c.mu.ts.PhysicalTime && newPts > m.PhysicalTime {
		// local wall time is the max
		// keep the physical time, reset the logical time
		c.mu.ts = timestamp.Timestamp{PhysicalTime: newPts}
	} else if m.PhysicalTime == c.mu.ts.PhysicalTime {
		if m.LogicalTime > c.mu.ts.LogicalTime {
			// received physical time is equal to the local physical time, and it
			// also has a larger logical time keep m's logical time.
			c.mu.ts.LogicalTime = m.LogicalTime
		}
	} else if m.PhysicalTime > c.mu.ts.PhysicalTime {
		c.mu.ts.PhysicalTime = m.PhysicalTime
		c.mu.ts.LogicalTime = m.LogicalTime
	}
}
