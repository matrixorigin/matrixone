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

package databranchutils

import (
	"context"
	"strings"
	"testing"
	"time"

	"github.com/matrixorigin/matrixone/pkg/common/mpool"
	"github.com/stretchr/testify/require"
)

// ---------------------------------------------------------------------------
// Review-F3 DETACHED-DRAIN cells (D186(B.4)): the bounded statement-return
// path never joins a lane terminal tail stalled in the lockservice's
// service-lifetime unlock retry (lockservice/service.go:769 ignores request
// cancellation); the detached drain still destroys the pool on the
// exactly-once zero-outstanding accounting. globalPools registration is the
// destruction oracle, as in cell (q).
// ---------------------------------------------------------------------------

// Timeout through the statement-shaped wrapper while the lane's "unlock"
// remains blocked: the wrapper returns at T_round; the pool is NOT
// destroyed while the lane holds its dispatch-thread reservation; after the
// lane exits the detached drain destroys it exactly once.
func TestDataBranchStageAllocatorDetachDrainReturnsWhileLaneBlocked(t *testing.T) {
	tag := "dbr-stage-alloc-detach"
	registrations := func() int {
		return strings.Count(mpool.ReportMemUsage(tag), `"`+tag+`"`)
	}
	require.Equal(t, 0, registrations())

	pool := NewDataBranchFencingPool(DataBranchFencingDriverConfig{
		LanePoolCapacity: 2,
		RoundTimeout:     200 * time.Millisecond,
		DrainStepTimeout: 100 * time.Millisecond,
		BarrierTimeout:   2 * time.Second,
	})
	driver := NewDataBranchFencingDriver(pool)

	gate := make(chan struct{})
	// statement mirrors runDataBranchFencingRetireForAddress's shape: root
	// reservation + deferred DetachDrain around the critical section.
	statement := func() error {
		alloc := NewDataBranchStageAllocator(mpool.MustNew(tag))
		alloc.Enter()
		defer alloc.DetachDrain()
		ctx := AttachDataBranchStageAllocator(context.Background(), alloc)
		_, err := driver.runStepOwned(ctx, DataBranchFencingStepRoundF2,
			func(lctx context.Context) (any, error) {
				<-gate // a stalled unlock retry: ignores the delivered cancel
				return "lane-result", nil
			}, nil, nil)
		return err
	}

	start := time.Now()
	err := statement()
	require.Error(t, err, "round deadline must expire while the lane is blocked")
	fErr, ok := err.(*DataBranchFencingError)
	require.True(t, ok, "the expiry surfaces as the typed fencing error, got %T", err)
	require.Equal(t, DataBranchFencingRoundExpired, fErr.Class)
	require.Equal(t, DataBranchFencingStepRoundF2, fErr.Step)
	require.Less(t, time.Since(start), 5*time.Second,
		"statement return is bounded by T_round, not by the stalled lane")

	// The lane still holds the reservation that runStepOwned made in the
	// dispatching thread, so the detached drain has not destroyed the pool.
	require.Equal(t, 1, registrations(),
		"pool stays registered while the blocked lane's reservation is outstanding")

	close(gate) // the service-lifetime cancel finally releases the unlock
	waitForCondition(t, 5*time.Second, "detached drain destroyed the pool after lane exit",
		func() bool { return registrations() == 0 })
	require.Equal(t, 0, registrations(), "destruction is exactly once")
}

// Spawn REFUSAL: runStepOwned's dispatch-thread reservation is released in
// the same thread; the statement returns with the typed refusal while the
// saturating (allocator-less) lane stays blocked, and the detached drain
// still completes because refusal never leaked a reservation.
func TestDataBranchStageAllocatorDetachDrainOnSpawnRefusal(t *testing.T) {
	tag := "dbr-stage-alloc-refuse"
	registrations := func() int {
		return strings.Count(mpool.ReportMemUsage(tag), `"`+tag+`"`)
	}
	require.Equal(t, 0, registrations())

	pool := NewDataBranchFencingPool(DataBranchFencingDriverConfig{
		LanePoolCapacity: 1,
		RoundTimeout:     2 * time.Second,
		DrainStepTimeout: 100 * time.Millisecond,
		BarrierTimeout:   2 * time.Second,
	})
	driver := NewDataBranchFencingDriver(pool)

	// Park the single lane-slot with an allocator-less blocked lane.
	hold := make(chan struct{})
	parked, reason := pool.spawnOwned(DataBranchFencingStepRoundF1,
		func(lctx context.Context) (any, error) {
			<-hold
			return nil, nil
		}, context.Background(), nil, nil, nil)
	require.NotNil(t, parked, "parked spawn: %s", reason)
	defer close(hold)

	statement := func() error {
		alloc := NewDataBranchStageAllocator(mpool.MustNew(tag))
		alloc.Enter()
		defer alloc.DetachDrain()
		ctx := AttachDataBranchStageAllocator(context.Background(), alloc)
		_, err := driver.runStepOwned(ctx, DataBranchFencingStepRoundF2,
			func(lctx context.Context) (any, error) {
				return nil, nil
			}, nil, nil)
		return err
	}

	err := statement()
	require.Error(t, err)
	fErr, ok := err.(*DataBranchFencingError)
	require.True(t, ok, "the refusal surfaces as the typed fencing error, got %T", err)
	require.Equal(t, DataBranchFencingRoundSaturated, fErr.Class)

	// Refusal released the dispatch reservation in place, so once the root
	// release lands the drain destroys the pool with the parked lane still
	// blocked (it holds NO allocator reservation).
	waitForCondition(t, 5*time.Second, "refusal-arm drain destroyed the pool",
		func() bool { return registrations() == 0 })
}
