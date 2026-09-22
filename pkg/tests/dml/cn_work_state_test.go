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

package dml

import (
	"context"
	"errors"
	"fmt"
	"io"
	"net"
	"runtime"
	"sort"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/lni/dragonboat/v4"

	"github.com/matrixorigin/matrixone/pkg/clusterservice"
	"github.com/matrixorigin/matrixone/pkg/common/moerr"
	"github.com/matrixorigin/matrixone/pkg/logutil"
	"github.com/matrixorigin/matrixone/pkg/pb/metadata"
	"github.com/stretchr/testify/require"
)

type cnWorkStateInventory interface {
	clusterservice.CNWorkStateUpdaterWithContext
	GetCNService(selector clusterservice.Selector, apply func(metadata.CNService) bool)
	GetCNServiceWithoutWorkingState(selector clusterservice.Selector, apply func(metadata.CNService) bool)
}

// A saved CI failure showed an approximately 9s HAKeeper commit under race
// load. Keep state transitions bounded but give this integration test the
// same 30s budget as its existing CN-readiness phase.
const (
	cnWorkStateOperationTimeout = 30 * time.Second
	cnWorkStatePollInterval     = 100 * time.Millisecond
)

type cnReadinessSnapshot struct {
	lastRefreshErr       error
	admissionReady       []string
	normallyDiscoverable []string
	peerAddr             string
}

func waitForCNReadiness(
	ctx context.Context,
	interval time.Duration,
	inventory cnWorkStateInventory,
	refresher clusterservice.AuthoritativeRefresher,
	localID string,
	peerID string,
) (cnReadinessSnapshot, error) {
	var snapshot cnReadinessSnapshot
	ticker := time.NewTicker(interval)
	defer ticker.Stop()

	for {
		if err := ctx.Err(); err != nil {
			return snapshot, fmt.Errorf("waiting for both base-cluster CNs: %w", err)
		}
		snapshot.lastRefreshErr = refresher.Refresh(ctx)
		if err := ctx.Err(); err != nil {
			if snapshot.lastRefreshErr == nil {
				snapshot.lastRefreshErr = err
			}
			return snapshot, fmt.Errorf("waiting for both base-cluster CNs: %w", err)
		}
		if snapshot.lastRefreshErr != nil && !isTransientCNControlPlaneError(snapshot.lastRefreshErr) {
			return snapshot, fmt.Errorf("refreshing base-cluster CN inventory: %w", snapshot.lastRefreshErr)
		}
		if snapshot.lastRefreshErr == nil {
			all := collectCNServices(inventory, true)
			working := collectCNServices(inventory, false)
			snapshot.admissionReady = formatCNWorkStates(all)
			snapshot.normallyDiscoverable = sortedCNIDs(working)
			snapshot.peerAddr = working[peerID].PipelineServiceAddress
			_, localFound := working[localID]
			_, peerFound := working[peerID]
			if len(working) == 2 && localFound && peerFound && snapshot.peerAddr != "" {
				return snapshot, nil
			}
		}

		select {
		case <-ctx.Done():
			return snapshot, fmt.Errorf("waiting for both base-cluster CNs: %w", ctx.Err())
		case <-ticker.C:
		}
	}
}

// waitForCNInventory polls only authoritative reads. A successful snapshot
// that contradicts an acknowledged update is a correctness failure; a failed
// read is retried only for transient control-plane errors, within the caller's
// single phase deadline.
func waitForCNInventory(
	ctx context.Context,
	interval time.Duration,
	refresher clusterservice.AuthoritativeRefresher,
	verify func() error,
	retryMismatchedInventory bool,
) error {
	ticker := time.NewTicker(interval)
	defer ticker.Stop()

	var lastRefreshErr error
	var lastVerifyErr error
	for {
		if err := ctx.Err(); err != nil {
			return cnInventoryWaitError(lastRefreshErr, lastVerifyErr, err)
		}

		lastRefreshErr = refresher.Refresh(ctx)
		if err := ctx.Err(); err != nil {
			if lastRefreshErr == nil {
				lastRefreshErr = err
			}
			return cnInventoryWaitError(lastRefreshErr, lastVerifyErr, err)
		}
		if lastRefreshErr != nil {
			if !isTransientCNControlPlaneError(lastRefreshErr) {
				return fmt.Errorf("refreshing authoritative CN inventory: %w", lastRefreshErr)
			}
		} else {
			lastVerifyErr = verify()
			if lastVerifyErr == nil {
				return nil
			}
			if !retryMismatchedInventory {
				return fmt.Errorf("authoritative CN inventory contradicts the expected topology: %w", lastVerifyErr)
			}
		}

		select {
		case <-ctx.Done():
			return cnInventoryWaitError(lastRefreshErr, lastVerifyErr, ctx.Err())
		case <-ticker.C:
		}
	}
}

func cnInventoryWaitError(lastRefreshErr, lastVerifyErr, waitErr error) error {
	var errs []error
	if lastRefreshErr != nil {
		errs = append(errs, fmt.Errorf("last authoritative refresh: %w", lastRefreshErr))
	}
	if lastVerifyErr != nil {
		errs = append(errs, fmt.Errorf("last topology check: %w", lastVerifyErr))
	}
	errs = append(errs, fmt.Errorf("waiting for authoritative CN inventory: %w", waitErr))
	return errors.Join(errs...)
}

func isTransientCNControlPlaneError(err error) bool {
	// LogService's RPC decoder can return raw Dragonboat sentinels instead of
	// their wire-level moerr codes. Match Dragonboat's temporary classification
	// and LogService's ErrShardNotFound exception, then cover transport errors.
	var networkErr net.Error
	return errors.Is(err, context.Canceled) ||
		errors.Is(err, context.DeadlineExceeded) ||
		errors.Is(err, io.EOF) ||
		errors.Is(err, io.ErrUnexpectedEOF) ||
		dragonboat.IsTempError(err) ||
		errors.Is(err, dragonboat.ErrShardNotFound) ||
		errors.As(err, &networkErr) ||
		logutil.IsExpectedConnectionCloseError(err) ||
		moerr.IsMoErrCode(err, moerr.ErrBackendClosed) ||
		moerr.IsMoErrCode(err, moerr.ErrBackendCannotConnect) ||
		moerr.IsMoErrCode(err, moerr.ErrNoAvailableBackend) ||
		moerr.IsMoErrCode(err, moerr.ErrConnectionReset) ||
		moerr.IsMoErrCode(err, moerr.ErrNoHAKeeper) ||
		moerr.IsMoErrCode(err, moerr.ErrDragonboatTimeout) ||
		moerr.IsMoErrCode(err, moerr.ErrDragonboatShardNotReady) ||
		moerr.IsMoErrCode(err, moerr.ErrDragonboatSystemClosed) ||
		moerr.IsMoErrCode(err, moerr.ErrDragonboatShardNotFound) ||
		moerr.IsMoErrCode(err, moerr.ErrRPCTimeout) ||
		moerr.IsMoErrCode(err, moerr.ErrUnexpectedEOF)
}

func formatCNWorkStates(services map[string]metadata.CNService) []string {
	states := make([]string, 0, len(services))
	for id, service := range services {
		states = append(states, fmt.Sprintf("%s=%s", id, service.WorkState))
	}
	sort.Strings(states)
	return states
}

func sortedCNIDs(services map[string]metadata.CNService) []string {
	ids := make([]string, 0, len(services))
	for id := range services {
		ids = append(ids, id)
	}
	sort.Strings(ids)
	return ids
}

// withCNDraining registers restoration before the state-changing RPC. A failed
// Draining RPC has an ambiguous outcome, so the caller must invalidate the
// shared fixture instead of sending a potentially reordered compensating write.
// After a successful transition, restoration is accepted only when an
// authoritative refresh proves the expected CN topology is usable again.
func withCNDraining(
	ctx context.Context,
	inventory cnWorkStateInventory,
	refresher clusterservice.AuthoritativeRefresher,
	targetID string,
	expectedIDs []string,
	invalidate func(error),
	body func(),
) (runErr error) {
	restore := false
	defer func() {
		if !restore {
			return
		}

		cleanupCtx, cancel := context.WithTimeout(context.Background(), cnWorkStateOperationTimeout)
		defer cancel()
		updateErr := inventory.DebugUpdateCNWorkStateWithContext(cleanupCtx, targetID, int(metadata.WorkState_Working))
		verifyErr := waitForCNInventory(
			cleanupCtx,
			cnWorkStatePollInterval,
			refresher,
			func() error { return verifyWorkingCNTopology(inventory, targetID, expectedIDs) },
			isTransientCNControlPlaneError(updateErr),
		)
		if verifyErr != nil {
			cleanupErr := errors.Join(
				wrapCNStateError("set CN back to Working", updateErr),
				wrapCNStateError("verify restored CN topology", verifyErr),
			)
			cleanupErr = fmt.Errorf("CN %q work-state restoration could not be verified: %w", targetID, cleanupErr)
			invalidate(cleanupErr)
			runErr = errors.Join(runErr, cleanupErr)
		}
	}()

	transitionCtx, cancelTransition := context.WithTimeout(ctx, cnWorkStateOperationTimeout)
	defer cancelTransition()
	if err := inventory.DebugUpdateCNWorkStateWithContext(transitionCtx, targetID, int(metadata.WorkState_Draining)); err != nil {
		transitionErr := fmt.Errorf("setting CN %q to Draining returned an ambiguous result: %w", targetID, err)
		invalidate(transitionErr)
		return transitionErr
	}
	restore = true
	if err := waitForCNInventory(
		transitionCtx,
		cnWorkStatePollInterval,
		refresher,
		func() error { return verifyDrainingCNTopology(inventory, targetID, expectedIDs) },
		false,
	); err != nil {
		return fmt.Errorf("verify CN topology after draining %q: %w", targetID, err)
	}
	body()
	return nil
}

func wrapCNStateError(operation string, err error) error {
	if err == nil {
		return nil
	}
	return fmt.Errorf("%s: %w", operation, err)
}

func verifyWorkingCNTopology(
	inventory cnWorkStateInventory,
	targetID string,
	expectedIDs []string,
) error {
	expected := make(map[string]struct{}, len(expectedIDs))
	for _, id := range expectedIDs {
		expected[id] = struct{}{}
	}

	all := collectCNServices(inventory, true)
	if err := verifyCNIDs("admission-ready", all, expected); err != nil {
		return err
	}
	target, ok := all[targetID]
	if !ok {
		return fmt.Errorf("target CN %q is absent from admission-ready inventory", targetID)
	}
	if target.WorkState != metadata.WorkState_Working {
		return fmt.Errorf("target CN %q has work state %s, want Working", targetID, target.WorkState)
	}

	working := collectCNServices(inventory, false)
	return verifyCNIDs("normally discoverable", working, expected)
}

func verifyDrainingCNTopology(
	inventory cnWorkStateInventory,
	targetID string,
	expectedIDs []string,
) error {
	expected := make(map[string]struct{}, len(expectedIDs))
	for _, id := range expectedIDs {
		expected[id] = struct{}{}
	}
	if _, ok := expected[targetID]; !ok {
		return fmt.Errorf("target CN %q is not in expected topology %v", targetID, expectedIDs)
	}
	workingExpected := make(map[string]struct{}, len(expected)-1)
	for id := range expected {
		if id != targetID {
			workingExpected[id] = struct{}{}
		}
	}

	all := collectCNServices(inventory, true)
	if err := verifyCNIDs("admission-ready", all, expected); err != nil {
		return err
	}
	if all[targetID].WorkState != metadata.WorkState_Draining {
		return fmt.Errorf("target CN %q has work state %s, want Draining", targetID, all[targetID].WorkState)
	}
	working := collectCNServices(inventory, false)
	return verifyCNIDs("normally discoverable after draining", working, workingExpected)
}

func collectCNServices(inventory cnWorkStateInventory, includeNonWorking bool) map[string]metadata.CNService {
	services := make(map[string]metadata.CNService)
	collect := func(service metadata.CNService) bool {
		services[service.ServiceID] = service
		return true
	}
	if includeNonWorking {
		inventory.GetCNServiceWithoutWorkingState(clusterservice.NewSelector(), collect)
	} else {
		inventory.GetCNService(clusterservice.NewSelector(), collect)
	}
	return services
}

func verifyCNIDs(name string, actual map[string]metadata.CNService, expected map[string]struct{}) error {
	if len(actual) == len(expected) {
		match := true
		for id := range expected {
			if _, ok := actual[id]; !ok {
				match = false
				break
			}
		}
		if match {
			return nil
		}
	}
	actualIDs := make([]string, 0, len(actual))
	for id := range actual {
		actualIDs = append(actualIDs, id)
	}
	sort.Strings(actualIDs)
	expectedIDs := make([]string, 0, len(expected))
	for id := range expected {
		expectedIDs = append(expectedIDs, id)
	}
	sort.Strings(expectedIDs)
	return fmt.Errorf("%s CN IDs are %v, want exactly %v", name, actualIDs, expectedIDs)
}

type fakeCNWorkStateInventory struct {
	services  map[string]metadata.CNService
	updates   []int
	deadlines []time.Time
	onUpdate  func(context.Context, string, int) error
}

func newFakeCNWorkStateInventory(ids ...string) *fakeCNWorkStateInventory {
	inventory := &fakeCNWorkStateInventory{services: make(map[string]metadata.CNService, len(ids))}
	for _, id := range ids {
		inventory.services[id] = metadata.CNService{ServiceID: id, WorkState: metadata.WorkState_Working}
	}
	return inventory
}

func (f *fakeCNWorkStateInventory) DebugUpdateCNWorkStateWithContext(ctx context.Context, id string, state int) error {
	f.updates = append(f.updates, state)
	if deadline, ok := ctx.Deadline(); ok {
		f.deadlines = append(f.deadlines, deadline)
	}
	if f.onUpdate != nil {
		return f.onUpdate(ctx, id, state)
	}
	service, ok := f.services[id]
	if !ok {
		return fmt.Errorf("unknown CN %q", id)
	}
	service.WorkState = metadata.WorkState(state)
	f.services[id] = service
	return nil
}

func (f *fakeCNWorkStateInventory) GetCNService(_ clusterservice.Selector, apply func(metadata.CNService) bool) {
	for _, id := range f.sortedIDs() {
		service := f.services[id]
		if service.WorkState != metadata.WorkState_Working && service.WorkState != metadata.WorkState_Unknown {
			continue
		}
		if !apply(service) {
			return
		}
	}
}

func (f *fakeCNWorkStateInventory) GetCNServiceWithoutWorkingState(_ clusterservice.Selector, apply func(metadata.CNService) bool) {
	for _, id := range f.sortedIDs() {
		if !apply(f.services[id]) {
			return
		}
	}
}

func (f *fakeCNWorkStateInventory) sortedIDs() []string {
	ids := make([]string, 0, len(f.services))
	for id := range f.services {
		ids = append(ids, id)
	}
	sort.Strings(ids)
	return ids
}

type fakeCNInventoryRefresher struct {
	calls     int
	err       error
	deadlines []time.Time
	fn        func(context.Context, int) error
}

func (f *fakeCNInventoryRefresher) Refresh(ctx context.Context) error {
	f.calls++
	if deadline, ok := ctx.Deadline(); ok {
		f.deadlines = append(f.deadlines, deadline)
	}
	if f.fn != nil {
		return f.fn(ctx, f.calls)
	}
	return f.err
}

type contextBlockingCNRefresher struct {
	calls    atomic.Int32
	entered  sync.Once
	finished sync.Once
	started  chan struct{}
	returned chan struct{}
}

func (f *contextBlockingCNRefresher) Refresh(ctx context.Context) error {
	f.calls.Add(1)
	f.entered.Do(func() { close(f.started) })
	<-ctx.Done()
	f.finished.Do(func() { close(f.returned) })
	return ctx.Err()
}

func TestWaitForCNReadinessWaitsForRefreshCancellation(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	inventory := newFakeCNWorkStateInventory("cn0", "cn1")
	refresher := &contextBlockingCNRefresher{started: make(chan struct{}), returned: make(chan struct{})}
	done := make(chan struct{})
	var snapshot cnReadinessSnapshot
	var waitErr error
	go func() {
		defer close(done)
		snapshot, waitErr = waitForCNReadiness(ctx, time.Millisecond, inventory, refresher, "cn0", "cn1")
	}()

	select {
	case <-refresher.started:
		cancel()
	case <-time.After(5 * time.Second):
		cancel()
		<-done
		t.Fatal("readiness polling did not enter Refresh")
	}
	select {
	case <-done:
	case <-time.After(5 * time.Second):
		t.Fatal("readiness polling did not stop after Refresh cancellation")
	}

	require.ErrorIs(t, waitErr, context.Canceled)
	require.ErrorIs(t, snapshot.lastRefreshErr, context.Canceled)
	require.EqualValues(t, 1, refresher.calls.Load(), "polling must not start overlapping refresh calls")
	select {
	case <-refresher.returned:
	default:
		t.Fatal("readiness diagnostics returned before the in-flight refresh stopped")
	}
}

func TestWaitForCNInventoryRetriesTransientRefreshError(t *testing.T) {
	refresher := &fakeCNInventoryRefresher{
		fn: func(_ context.Context, call int) error {
			if call == 1 {
				return fmt.Errorf("HAKeeper Refresh: %w", dragonboat.ErrTimeout)
			}
			return nil
		},
	}
	verified := false
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()

	err := waitForCNInventory(ctx, time.Millisecond, refresher, func() error {
		verified = true
		return nil
	}, false)

	require.NoError(t, err)
	require.True(t, verified)
	require.Equal(t, 2, refresher.calls)
}

func TestIsTransientCNControlPlaneErrorIncludesHAKeeperRPCDecode(t *testing.T) {
	tests := []struct {
		name string
		err  error
		want bool
	}{
		{name: "decoded raft timeout", err: dragonboat.ErrTimeout, want: true},
		{name: "wrapped decoded raft timeout", err: fmt.Errorf("refresh: %w", dragonboat.ErrTimeout), want: true},
		{name: "local raft timeout code", err: moerr.NewDragonboatTimeout(context.Background(), "HAKeeper read timed out"), want: true},
		{name: "decoded shard not ready", err: dragonboat.ErrShardNotReady, want: true},
		{name: "decoded shard not found", err: dragonboat.ErrShardNotFound, want: true},
		{name: "rejected proposal is not retried", err: dragonboat.ErrRejected, want: false},
		{name: "unrelated error is not retried", err: errors.New("invalid cluster response"), want: false},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			require.Equal(t, tt.want, isTransientCNControlPlaneError(tt.err))
		})
	}
}

func TestWaitForCNInventoryDeadlineReportsLastTopology(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	refresher := &fakeCNInventoryRefresher{}
	lastTopologyErr := errors.New("target CN is still Draining")
	verifyCalls := 0

	err := waitForCNInventory(ctx, time.Hour, refresher, func() error {
		verifyCalls++
		cancel()
		return lastTopologyErr
	}, true)

	require.ErrorIs(t, err, context.Canceled)
	require.ErrorIs(t, err, lastTopologyErr)
	require.Equal(t, 1, refresher.calls)
	require.Equal(t, 1, verifyCalls)
}

func TestWithCNDrainingRejectsAmbiguousTransition(t *testing.T) {
	transitionErr := errors.New("timed out after request admission")
	inventory := newFakeCNWorkStateInventory("cn0", "cn1")
	inventory.onUpdate = func(_ context.Context, id string, state int) error {
		service := inventory.services[id]
		service.WorkState = metadata.WorkState(state)
		inventory.services[id] = service
		if state == int(metadata.WorkState_Draining) {
			return transitionErr
		}
		return nil
	}
	refresher := &fakeCNInventoryRefresher{}
	var invalidationErr error
	bodyCalled := false

	err := withCNDraining(context.Background(), inventory, refresher, "cn0", []string{"cn0", "cn1"},
		func(err error) { invalidationErr = err },
		func() { bodyCalled = true },
	)

	require.ErrorIs(t, err, transitionErr)
	require.ErrorIs(t, invalidationErr, transitionErr)
	require.False(t, bodyCalled)
	require.Equal(t, []int{int(metadata.WorkState_Draining)}, inventory.updates,
		"an ambiguous transition must not be followed by a potentially reordered Working request")
	require.Equal(t, metadata.WorkState_Draining, inventory.services["cn0"].WorkState)
	require.Zero(t, refresher.calls)
}

func TestWithCNDrainingRejectsUnpublishedTransition(t *testing.T) {
	inventory := newFakeCNWorkStateInventory("cn0", "cn1")
	inventory.onUpdate = func(_ context.Context, id string, state int) error {
		if state == int(metadata.WorkState_Working) {
			service := inventory.services[id]
			service.WorkState = metadata.WorkState(state)
			inventory.services[id] = service
		}
		return nil
	}
	refresher := &fakeCNInventoryRefresher{}
	var invalidationErr error
	bodyCalled := false

	err := withCNDraining(context.Background(), inventory, refresher, "cn0", []string{"cn0", "cn1"},
		func(err error) { invalidationErr = err },
		func() { bodyCalled = true },
	)

	require.ErrorContains(t, err, "want Draining")
	require.NoError(t, invalidationErr, "a confirmed cleanup should keep the shared fixture reusable")
	require.False(t, bodyCalled, "the remote-query oracle must not run without its Draining precondition")
	require.Equal(t, metadata.WorkState_Working, inventory.services["cn0"].WorkState)
	require.Equal(t, []int{int(metadata.WorkState_Draining), int(metadata.WorkState_Working)}, inventory.updates)
	require.Equal(t, 2, refresher.calls)
	require.Len(t, inventory.deadlines, 2)
	require.Len(t, refresher.deadlines, 2)
	require.True(t, inventory.deadlines[0].Equal(refresher.deadlines[0]),
		"Draining update and its authoritative verification must share one deadline")
	require.True(t, inventory.deadlines[1].Equal(refresher.deadlines[1]),
		"Working restoration and its authoritative verification must share one deadline")
}

func TestWithCNDrainingRestoresAfterGoexit(t *testing.T) {
	inventory := newFakeCNWorkStateInventory("cn0", "cn1")
	refresher := &fakeCNInventoryRefresher{}
	invalidations := make(chan error, 1)
	done := make(chan struct{})
	go func() {
		defer close(done)
		withCNDraining(context.Background(), inventory, refresher, "cn0", []string{"cn0", "cn1"},
			func(err error) { invalidations <- err },
			func() { runtime.Goexit() },
		)
	}()

	select {
	case <-done:
	case <-time.After(time.Second):
		t.Fatal("CN restoration did not finish while unwinding Goexit")
	}
	require.Equal(t, metadata.WorkState_Working, inventory.services["cn0"].WorkState)
	require.Equal(t, []int{int(metadata.WorkState_Draining), int(metadata.WorkState_Working)}, inventory.updates)
	require.Equal(t, 2, refresher.calls)
	select {
	case err := <-invalidations:
		t.Fatalf("unexpected fixture invalidation: %v", err)
	default:
	}
}

func TestWithCNDrainingRestoresAfterCanceledBody(t *testing.T) {
	for _, panicBody := range []bool{false, true} {
		t.Run(fmt.Sprintf("panic=%t", panicBody), func(t *testing.T) {
			ctx, cancel := context.WithCancel(context.Background())
			defer cancel()
			inventory := newFakeCNWorkStateInventory("cn0", "cn1")
			var cleanupContextErr error
			inventory.onUpdate = func(updateCtx context.Context, id string, state int) error {
				if state == int(metadata.WorkState_Working) {
					cleanupContextErr = updateCtx.Err()
				}
				service := inventory.services[id]
				service.WorkState = metadata.WorkState(state)
				inventory.services[id] = service
				return nil
			}
			refresher := &fakeCNInventoryRefresher{}
			var invalidationErr error
			var runErr error
			var recovered any
			panicValue := errors.New("body panic")
			func() {
				defer func() { recovered = recover() }()
				runErr = withCNDraining(ctx, inventory, refresher, "cn0", []string{"cn0", "cn1"},
					func(err error) { invalidationErr = err },
					func() {
						cancel()
						if panicBody {
							panic(panicValue)
						}
					})
			}()

			if panicBody {
				require.Same(t, panicValue, recovered, "cleanup must preserve the original panic")
			} else {
				require.Nil(t, recovered)
			}
			require.NoError(t, runErr)
			require.NoError(t, invalidationErr)
			require.NoError(t, cleanupContextErr, "restoration must not inherit the canceled body context")
			require.Equal(t, []int{int(metadata.WorkState_Draining), int(metadata.WorkState_Working)}, inventory.updates)
			require.Equal(t, 2, refresher.calls)
			require.NoError(t, verifyWorkingCNTopology(inventory, "cn0", []string{"cn0", "cn1"}))
		})
	}
}

func TestWithCNDrainingReconcilesAmbiguousRestoreResponse(t *testing.T) {
	restoreErr := context.DeadlineExceeded
	inventory := newFakeCNWorkStateInventory("cn0", "cn1")
	inventory.onUpdate = func(_ context.Context, id string, state int) error {
		if state == int(metadata.WorkState_Working) {
			return restoreErr
		}
		service := inventory.services[id]
		service.WorkState = metadata.WorkState(state)
		inventory.services[id] = service
		return nil
	}
	refresher := &fakeCNInventoryRefresher{
		fn: func(_ context.Context, call int) error {
			if call == 3 {
				service := inventory.services["cn0"]
				service.WorkState = metadata.WorkState_Working
				inventory.services["cn0"] = service
			}
			return nil
		},
	}
	var invalidationErr error
	bodyCalled := false

	err := withCNDraining(context.Background(), inventory, refresher, "cn0", []string{"cn0", "cn1"},
		func(err error) { invalidationErr = err },
		func() { bodyCalled = true },
	)

	require.NoError(t, err)
	require.True(t, bodyCalled)
	require.NoError(t, invalidationErr)
	require.Equal(t, metadata.WorkState_Working, inventory.services["cn0"].WorkState)
	require.Equal(t, []int{int(metadata.WorkState_Draining), int(metadata.WorkState_Working)}, inventory.updates)
	require.Equal(t, 3, refresher.calls,
		"after an ambiguous restore response, read-only polling must wait for the authoritative Working view")
}

func TestWithCNDrainingInvalidatesUnverifiedRestore(t *testing.T) {
	t.Run("CN remains draining", func(t *testing.T) {
		restoreErr := errors.New("restore rejected")
		inventory := newFakeCNWorkStateInventory("cn0", "cn1")
		inventory.onUpdate = func(_ context.Context, id string, state int) error {
			if state == int(metadata.WorkState_Working) {
				return restoreErr
			}
			service := inventory.services[id]
			service.WorkState = metadata.WorkState(state)
			inventory.services[id] = service
			return nil
		}
		refresher := &fakeCNInventoryRefresher{}
		var invalidationErr error

		err := withCNDraining(context.Background(), inventory, refresher, "cn0", []string{"cn0", "cn1"},
			func(err error) { invalidationErr = err }, func() {})

		require.ErrorIs(t, err, restoreErr)
		require.ErrorIs(t, invalidationErr, restoreErr)
		require.Equal(t, metadata.WorkState_Draining, inventory.services["cn0"].WorkState)
	})

	t.Run("authoritative refresh fails", func(t *testing.T) {
		refreshErr := errors.New("refresh unavailable")
		inventory := newFakeCNWorkStateInventory("cn0", "cn1")
		refresher := &fakeCNInventoryRefresher{}
		var invalidationErr error

		err := withCNDraining(context.Background(), inventory, refresher, "cn0", []string{"cn0", "cn1"},
			func(err error) { invalidationErr = err },
			func() { refresher.err = refreshErr },
		)

		require.ErrorIs(t, err, refreshErr)
		require.ErrorIs(t, invalidationErr, refreshErr)
		require.Equal(t, metadata.WorkState_Working, inventory.services["cn0"].WorkState,
			"a successful update without an authoritative refresh is still unverified")
	})
}
