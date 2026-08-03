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

package lifecycle

import (
	"context"
	"errors"
	"fmt"
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"github.com/matrixorigin/matrixone/pkg/pb/task"
)

type sliceBindingPager struct {
	pages [][]Binding
	calls int
}

type orderedBindingPager struct {
	bindings []Binding
	starts   []BindingCursor
}

type emptyProgressBindingPager struct {
	calls  int
	starts []BindingCursor
}

func (p *emptyProgressBindingPager) NextActiveBindings(
	_ context.Context,
	cursor BindingCursor,
	_ int,
) ([]Binding, BindingCursor, bool, error) {
	p.calls++
	p.starts = append(p.starts, cursor)
	return nil, BindingCursor{AccountID: cursor.AccountID + 64}, false, nil
}

func (p *orderedBindingPager) NextActiveBindings(
	_ context.Context,
	cursor BindingCursor,
	limit int,
) ([]Binding, BindingCursor, bool, error) {
	p.starts = append(p.starts, cursor)
	start := 0
	for start < len(p.bindings) {
		binding := p.bindings[start]
		if binding.AccountID > cursor.AccountID ||
			(binding.AccountID == cursor.AccountID && binding.ID > cursor.BindingID) {
			break
		}
		start++
	}
	end := min(start+limit, len(p.bindings))
	page := append([]Binding(nil), p.bindings[start:end]...)
	next := cursor
	if len(page) > 0 {
		last := page[len(page)-1]
		next = BindingCursor{AccountID: last.AccountID, BindingID: last.ID}
	}
	return page, next, end == len(p.bindings), nil
}

func (p *sliceBindingPager) NextActiveBindings(
	_ context.Context,
	_ BindingCursor,
	_ int,
) ([]Binding, BindingCursor, bool, error) {
	p.calls++
	bindings := p.pages[0]
	p.pages = p.pages[1:]
	return bindings, BindingCursor{}, len(p.pages) == 0, nil
}

func TestLifecycleCoordinatorTaskMetadata(t *testing.T) {
	metadata := CoordinatorTaskMetadata()
	require.Equal(t, CoordinatorTaskID, metadata.ID)
	require.Equal(t, task.TaskCode_LifecycleCoordinator, metadata.Executor)
	require.Equal(t, uint32(1), metadata.Options.Concurrency)
	require.NotEmpty(t, CoordinatorTaskCronExpr)
}

func TestCoordinatorFeatureOffDoesNotReadCatalog(t *testing.T) {
	pager := &sliceBindingPager{pages: [][]Binding{{}}}
	coordinator := NewCoordinator(CoordinatorConfig{
		Enabled: false,
	}, pager, func(context.Context, Binding) error {
		t.Fatal("feature-off coordinator must not dispatch")
		return nil
	})
	require.NoError(t, coordinator.Run(context.Background()))
	require.Zero(t, pager.calls)
}

func TestCoordinatorOnlyRunsActiveBindingsWithHardConcurrency(t *testing.T) {
	bindings := []Binding{
		{ID: "a1", AccountID: 1, DatabaseID: 1, PhysicalTableID: 1, State: BindingStateActive},
		{ID: "a2", AccountID: 1, DatabaseID: 1, PhysicalTableID: 2, State: BindingStateActive},
		{ID: "a3", AccountID: 1, DatabaseID: 2, PhysicalTableID: 3, State: BindingStatePaused},
		{ID: "b1", AccountID: 2, DatabaseID: 3, PhysicalTableID: 4, State: BindingStateActive},
	}
	pager := &sliceBindingPager{pages: [][]Binding{bindings}}

	var (
		mu              sync.Mutex
		activeCluster   int
		maxCluster      int
		activeByAccount = make(map[uint32]int)
		maxByAccount    = make(map[uint32]int)
		seen            = make(map[string]int)
	)
	child := func(ctx context.Context, binding Binding) error {
		mu.Lock()
		activeCluster++
		activeByAccount[binding.AccountID]++
		if activeCluster > maxCluster {
			maxCluster = activeCluster
		}
		if activeByAccount[binding.AccountID] > maxByAccount[binding.AccountID] {
			maxByAccount[binding.AccountID] = activeByAccount[binding.AccountID]
		}
		seen[binding.ID]++
		mu.Unlock()

		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-time.After(10 * time.Millisecond):
		}

		mu.Lock()
		activeCluster--
		activeByAccount[binding.AccountID]--
		mu.Unlock()
		return nil
	}

	coordinator := NewCoordinator(CoordinatorConfig{
		Enabled:             true,
		PageSize:            16,
		MaxPagesPerRun:      16,
		MaxBindingsPerRun:   16,
		MaxClusterChildren:  2,
		MaxAccountChildren:  1,
		MaxDatabaseChildren: 1,
		MaxTableChildren:    1,
	}, pager, child)
	require.NoError(t, coordinator.Run(context.Background()))
	require.Equal(t, map[string]int{"a1": 1, "a2": 1, "b1": 1}, seen)
	require.LessOrEqual(t, maxCluster, 2)
	require.LessOrEqual(t, maxByAccount[1], 1)
}

func TestCoordinatorCancellationStopsChildren(t *testing.T) {
	pager := &sliceBindingPager{pages: [][]Binding{{
		{ID: "a1", AccountID: 1, DatabaseID: 1, PhysicalTableID: 1, State: BindingStateActive},
		{ID: "a2", AccountID: 1, DatabaseID: 1, PhysicalTableID: 2, State: BindingStateActive},
	}}}
	ctx, cancel := context.WithCancel(context.Background())
	started := make(chan struct{}, 1)
	coordinator := NewCoordinator(CoordinatorConfig{
		Enabled:             true,
		PageSize:            2,
		MaxPagesPerRun:      2,
		MaxBindingsPerRun:   2,
		MaxClusterChildren:  1,
		MaxAccountChildren:  1,
		MaxDatabaseChildren: 1,
		MaxTableChildren:    1,
	}, pager, func(ctx context.Context, _ Binding) error {
		started <- struct{}{}
		<-ctx.Done()
		return ctx.Err()
	})

	done := make(chan error, 1)
	go func() { done <- coordinator.Run(ctx) }()
	<-started
	cancel()
	require.ErrorIs(t, <-done, context.Canceled)
}

func TestCoordinatorBindingFailureDoesNotCancelIndependentBindings(t *testing.T) {
	pager := &sliceBindingPager{pages: [][]Binding{{
		{ID: "a1", AccountID: 1, DatabaseID: 1, PhysicalTableID: 1, State: BindingStateActive},
		{ID: "b1", AccountID: 2, DatabaseID: 2, PhysicalTableID: 2, State: BindingStateActive},
	}}}
	failed := errors.New("binding blocked")
	var (
		mu   sync.Mutex
		seen = make(map[string]bool)
	)
	coordinator := NewCoordinator(CoordinatorConfig{
		Enabled:             true,
		PageSize:            2,
		MaxPagesPerRun:      2,
		MaxBindingsPerRun:   2,
		MaxClusterChildren:  1,
		MaxAccountChildren:  1,
		MaxDatabaseChildren: 1,
		MaxTableChildren:    1,
	}, pager, func(_ context.Context, binding Binding) error {
		mu.Lock()
		seen[binding.ID] = true
		mu.Unlock()
		if binding.ID == "a1" {
			return failed
		}
		return nil
	})

	require.ErrorIs(t, coordinator.Run(context.Background()), failed)
	require.Equal(t, map[string]bool{"a1": true, "b1": true}, seen)
}

func TestCoordinatorContinuesBindingCursorAcrossRunsAndWrapsAtEnd(t *testing.T) {
	const bindingCount = 1001
	bindings := make([]Binding, 0, bindingCount)
	for index := range bindingCount {
		bindings = append(bindings, Binding{
			ID:              fmt.Sprintf("%032x", index+1),
			AccountID:       1,
			DatabaseID:      1,
			PhysicalTableID: uint64(index + 1),
			State:           BindingStateActive,
		})
	}
	pager := &orderedBindingPager{bindings: bindings}
	seen := make(map[string]int, bindingCount)
	var seenMu sync.Mutex
	coordinator := NewCoordinator(CoordinatorConfig{
		Enabled:             true,
		PageSize:            64,
		MaxPagesPerRun:      16,
		MaxBindingsPerRun:   1000,
		MaxClusterChildren:  8,
		MaxAccountChildren:  4,
		MaxDatabaseChildren: 2,
		MaxTableChildren:    1,
	}, pager, func(_ context.Context, binding Binding) error {
		seenMu.Lock()
		seen[binding.ID]++
		seenMu.Unlock()
		return nil
	})

	require.NoError(t, coordinator.Run(context.Background()))
	require.Len(t, seen, 1000)
	require.Zero(t, seen[bindings[1000].ID])

	require.NoError(t, coordinator.Run(context.Background()))
	require.Equal(t, 1, seen[bindings[1000].ID])
	require.Len(t, seen, bindingCount)
	require.Equal(t, BindingCursor{}, coordinator.cursor)
	require.NotEmpty(t, pager.starts)
	require.NotEqual(t, BindingCursor{}, pager.starts[len(pager.starts)-1])
}

func TestCoordinatorBoundsEmptyTenantPagesPerRun(t *testing.T) {
	pager := &emptyProgressBindingPager{}
	coordinator := NewCoordinator(CoordinatorConfig{
		Enabled:             true,
		PageSize:            64,
		MaxPagesPerRun:      3,
		MaxBindingsPerRun:   1000,
		MaxClusterChildren:  8,
		MaxAccountChildren:  4,
		MaxDatabaseChildren: 2,
		MaxTableChildren:    1,
	}, pager, func(context.Context, Binding) error {
		t.Fatal("empty tenant pages must not dispatch a child")
		return nil
	})

	require.NoError(t, coordinator.Run(context.Background()))
	require.Equal(t, 3, pager.calls)
	require.Equal(t, BindingCursor{AccountID: 192}, coordinator.cursor)

	require.NoError(t, coordinator.Run(context.Background()))
	require.Equal(t, 6, pager.calls)
	require.Equal(t, BindingCursor{AccountID: 384}, coordinator.cursor)
}

func TestCoordinatorConcurrentRunWaitIsCancellable(t *testing.T) {
	started := make(chan struct{})
	release := make(chan struct{})
	coordinator := NewCoordinator(CoordinatorConfig{
		Enabled:             true,
		PageSize:            1,
		MaxPagesPerRun:      1,
		MaxBindingsPerRun:   1,
		MaxClusterChildren:  1,
		MaxAccountChildren:  1,
		MaxDatabaseChildren: 1,
		MaxTableChildren:    1,
	}, &sliceBindingPager{pages: [][]Binding{{{
		ID:              "00000000000000000000000000000001",
		AccountID:       1,
		DatabaseID:      1,
		PhysicalTableID: 1,
		State:           BindingStateActive,
	}}}}, func(context.Context, Binding) error {
		close(started)
		<-release
		return nil
	})

	firstDone := make(chan error, 1)
	go func() { firstDone <- coordinator.Run(context.Background()) }()
	<-started

	secondCtx, cancel := context.WithCancel(context.Background())
	cancel()
	require.ErrorIs(t, coordinator.Run(secondCtx), context.Canceled)

	close(release)
	require.NoError(t, <-firstDone)
}
