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

package process

import (
	"sync/atomic"
	"time"
)

// ExecutionMemoryGrowthParticipant is an advisory, work-conserving share of
// the memory which remains admissible for one statement on one CN. It does not
// reserve memory: physical allocations remain the sole owners in the execution
// resource ledger.
//
// Spill-capable operators register only while they can still choose between
// retaining the next input and spilling it. Capacity available after all other
// admitted memory is divided among those live decision makers. Exact recovery
// floors are therefore subtracted before a share is computed, without
// estimating bytes per worker or deriving policy from host CPU count.
type ExecutionMemoryGrowthParticipant struct {
	generation *ExecutionResourceGeneration
	reported   uint64
	released   atomic.Bool
}

// RegisterMemoryGrowthParticipant joins the current generation's adaptive
// growth set. The returned participant must be released when the operator can
// no longer retain new input.
func (g *ExecutionResourceGeneration) RegisterMemoryGrowthParticipant() (
	*ExecutionMemoryGrowthParticipant,
	error,
) {
	if g == nil || g.budget == nil {
		return nil, ErrExecutionResourceInvalid
	}
	b := g.budget
	b.mu.Lock()
	defer b.mu.Unlock()
	if b.closed || g.closed {
		return nil, ErrExecutionResourceClosed
	}
	g.memoryGrowthParticipants++
	b.memoryGrowthParticipants++
	return &ExecutionMemoryGrowthParticipant{generation: g}, nil
}

func (b *ExecutionResourceBudget) sampleMemoryHeadroom() (uint64, bool) {
	if b == nil || b.memoryHeadroomProvider == nil {
		return 0, false
	}
	b.headroomMu.Lock()
	defer b.headroomMu.Unlock()
	now := b.memoryHeadroomNow
	if now == nil {
		now = func() time.Time { return time.Now() }
	}
	current := now()
	if b.memoryHeadroomCached && b.memoryHeadroomTTL > 0 &&
		current.Sub(b.memoryHeadroomAt) < b.memoryHeadroomTTL {
		b.mu.Lock()
		bytes, measured := b.memoryHeadroomBytes, b.memoryHeadroomMeasured
		b.mu.Unlock()
		return bytes, measured
	}
	b.mu.Lock()
	// Keep the physical sample and the aggregate-ledger checkpoint in one
	// admission critical section. Otherwise an allocation between those two
	// observations would be absent from both the sampled usage and the cached
	// growth delta, allowing the same bytes twice.
	bytes, measured := b.memoryHeadroomProvider()
	b.memoryHeadroomBytes = bytes
	b.memoryHeadroomMeasured = measured
	b.memoryHeadroomAccountedUsed = b.aggregateUsed
	b.memoryHeadroomRecoveryUnused = b.recoveryCapacityUnused
	b.memoryHeadroomAt = current
	b.memoryHeadroomCached = true
	b.mu.Unlock()
	return bytes, measured
}

// memoryHeadroomRefreshNeededLocked keeps the allocation hot path on the
// budget mutex it already owns. Only one expired sample takes headroomMu and
// performs filesystem reads; cached admissions add no second global lock.
func (b *ExecutionResourceBudget) memoryHeadroomRefreshNeededLocked() bool {
	if b == nil || b.memoryHeadroomProvider == nil {
		return false
	}
	if !b.memoryHeadroomCached || b.memoryHeadroomTTL <= 0 {
		return true
	}
	now := b.memoryHeadroomNow
	if now == nil {
		now = time.Now
	}
	return now().Sub(b.memoryHeadroomAt) >= b.memoryHeadroomTTL
}

// physicalGrowthHeadroomLocked returns the bytes which may still increase the
// process working set while preserving the runtime safety margin. It adjusts a
// cached OS sample by all net accounted growth admitted since that sample, so
// the sample remains safe under concurrent allocation. b.mu must be held.
func (b *ExecutionResourceBudget) physicalGrowthHeadroomLocked() (uint64, bool) {
	if b == nil || !b.memoryHeadroomMeasured {
		return 0, false
	}
	remaining := b.memoryHeadroomBytes
	if b.memoryHeadroomRecoveryUnused >= remaining {
		return 0, true
	}
	remaining -= b.memoryHeadroomRecoveryUnused
	if b.aggregateUsed > b.memoryHeadroomAccountedUsed {
		growth := b.aggregateUsed - b.memoryHeadroomAccountedUsed
		if growth >= remaining {
			return 0, true
		}
		remaining -= growth
	}
	if remaining <= b.memoryHeadroomSafety {
		return 0, true
	}
	return remaining - b.memoryHeadroomSafety, true
}

// RetainedLimit reports current retained bytes and returns this participant's
// equal share of the capacity available to the complete live participant set.
// Memory already admitted for those participants stays in the divisible pool;
// every other admitted byte, including exact recovery floors and completed
// pipeline state, is deducted first.
//
// The limit is advisory: concurrent allocations can change it immediately,
// and the normal allocation ledger remains the authoritative hard boundary.
func (p *ExecutionMemoryGrowthParticipant) RetainedLimit(
	current uint64,
) (uint64, error) {
	if p == nil || p.generation == nil || p.released.Load() {
		return 0, ErrExecutionResourceInvalid
	}
	g := p.generation
	b := g.budget
	if b == nil {
		return 0, ErrExecutionResourceInvalid
	}
	_, physicalMeasured := b.sampleMemoryHeadroom()
	b.mu.Lock()
	defer b.mu.Unlock()
	// Release publishes the terminal state before waiting for b.mu. Recheck it
	// inside the same critical section as the participant counters so a
	// concurrent terminal cleanup cannot remove this participant and then let
	// this call add its report back without restoring the count.
	if p.released.Load() {
		return 0, ErrExecutionResourceInvalid
	}
	if b.closed || g.closed {
		return 0, ErrExecutionResourceClosed
	}
	if g.memoryGrowthParticipants == 0 {
		return 0, ErrExecutionResourceInvalid
	}
	if p.reported > g.memoryGrowthReportedBytes ||
		p.reported > b.memoryGrowthReportedBytes {
		return 0, ErrExecutionResourceInvalid
	}
	g.memoryGrowthReportedBytes -= p.reported
	b.memoryGrowthReportedBytes -= p.reported
	if current > ^uint64(0)-g.memoryGrowthReportedBytes ||
		current > ^uint64(0)-b.memoryGrowthReportedBytes {
		g.memoryGrowthReportedBytes += p.reported
		b.memoryGrowthReportedBytes += p.reported
		return 0, ErrExecutionResourceInvalid
	}
	g.memoryGrowthReportedBytes += current
	b.memoryGrowthReportedBytes += current
	p.reported = current

	queryParticipantBytes := min(g.used, g.memoryGrowthReportedBytes)
	queryFixed := g.used - queryParticipantBytes
	queryPool := uint64(0)
	if queryFixed < g.cap {
		queryPool = g.cap - queryFixed
	}

	cnParticipantBytes := min(b.aggregateUsed, b.memoryGrowthReportedBytes)
	cnFixed := b.aggregateUsed - cnParticipantBytes
	cnPool := uint64(0)
	if cnFixed < b.aggregateCap {
		cnPool = b.aggregateCap - cnFixed
	}
	queryLimit := queryPool / g.memoryGrowthParticipants
	if b.memoryGrowthParticipants == 0 {
		return 0, ErrExecutionResourceInvalid
	}
	cnLimit := cnPool / b.memoryGrowthParticipants
	limit := min(queryLimit, cnLimit)
	if physicalMeasured {
		growthHeadroom, _ := b.physicalGrowthHeadroomLocked()
		physicalShare := growthHeadroom / b.memoryGrowthParticipants
		physicalLimit := current
		if physicalShare > ^uint64(0)-physicalLimit {
			physicalLimit = ^uint64(0)
		} else {
			physicalLimit += physicalShare
		}
		limit = min(limit, physicalLimit)
	}
	return limit, nil
}

// Release leaves the adaptive growth set exactly once. It remains valid after
// generation closure so terminal cleanup cannot strand a participant count.
func (p *ExecutionMemoryGrowthParticipant) Release() bool {
	if p == nil || p.generation == nil ||
		!p.released.CompareAndSwap(false, true) {
		return false
	}
	g := p.generation
	b := g.budget
	if b == nil {
		panic("execution memory growth participant has no budget")
	}
	b.mu.Lock()
	defer b.mu.Unlock()
	if g.memoryGrowthParticipants == 0 || b.memoryGrowthParticipants == 0 {
		panic("execution memory growth participant release underflow")
	}
	if p.reported > g.memoryGrowthReportedBytes ||
		p.reported > b.memoryGrowthReportedBytes {
		panic("execution memory growth participant report underflow")
	}
	g.memoryGrowthReportedBytes -= p.reported
	b.memoryGrowthReportedBytes -= p.reported
	p.reported = 0
	g.memoryGrowthParticipants--
	b.memoryGrowthParticipants--
	return true
}
