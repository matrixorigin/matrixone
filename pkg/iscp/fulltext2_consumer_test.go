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

package iscp

import (
	"context"
	"testing"
	"time"

	"github.com/matrixorigin/matrixone/pkg/util/fault"
	"github.com/stretchr/testify/require"
)

func TestFulltext2AfterTailCommitFenceFault(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	t.Cleanup(cancel)

	// The production path must stay inert for empty flushes and when fault
	// injection is disabled.
	done := make(chan struct{})
	go func() {
		waitAfterFulltext2TailCommitBeforeFence(ctx, false)
		waitAfterFulltext2TailCommitBeforeFence(ctx, true)
		close(done)
	}()
	select {
	case <-done:
	case <-time.After(time.Second):
		t.Fatal("disabled fulltext2 after-commit fault blocked")
	}

	require.True(t, fault.Enable())
	t.Cleanup(func() { fault.Disable() })
	require.NoError(t, fault.AddFaultPoint(ctx, fulltext2AfterTailCommitBeforeFenceFault, ":::", "wait", 0, "", false))
	t.Cleanup(func() {
		_, _ = fault.RemoveFaultPoint(context.Background(), fulltext2AfterTailCommitBeforeFenceFault)
	})
	const waitersPoint = "fulltext2-after-tail-commit-waiters-test"
	require.NoError(t, fault.AddFaultPoint(ctx, waitersPoint, ":::", "getwaiters", 0, fulltext2AfterTailCommitBeforeFenceFault, false))
	t.Cleanup(func() { _, _ = fault.RemoveFaultPoint(context.Background(), waitersPoint) })

	// An empty flush must not enter the after-commit barrier even when enabled.
	waitAfterFulltext2TailCommitBeforeFence(ctx, false)
	count, _, ok := fault.TriggerFault(waitersPoint)
	require.True(t, ok)
	require.Zero(t, count)

	waitDone := make(chan struct{})
	go func() {
		waitAfterFulltext2TailCommitBeforeFence(ctx, true)
		close(waitDone)
	}()
	require.Eventually(t, func() bool {
		count, _, exists := fault.TriggerFault(waitersPoint)
		return exists && count == 1
	}, time.Second, time.Millisecond)

	removed, err := fault.RemoveFaultPoint(ctx, fulltext2AfterTailCommitBeforeFenceFault)
	require.NoError(t, err)
	require.True(t, removed)
	select {
	case <-waitDone:
	case <-time.After(time.Second):
		t.Fatal("removing fulltext2 after-commit fault did not release waiter")
	}

	// Reinstall the point and prove the framework's notifyall path releases the
	// same production barrier without removing it.
	require.NoError(t, fault.AddFaultPoint(ctx, fulltext2AfterTailCommitBeforeFenceFault, ":::", "wait", 0, "", false))
	const notifyPoint = "fulltext2-after-tail-commit-notify-test"
	require.NoError(t, fault.AddFaultPoint(ctx, notifyPoint, ":::", "notifyall", 0, fulltext2AfterTailCommitBeforeFenceFault, false))
	t.Cleanup(func() { _, _ = fault.RemoveFaultPoint(context.Background(), notifyPoint) })
	waitDone = make(chan struct{})
	go func() {
		waitAfterFulltext2TailCommitBeforeFence(ctx, true)
		close(waitDone)
	}()
	require.Eventually(t, func() bool {
		count, _, exists := fault.TriggerFault(waitersPoint)
		return exists && count == 1
	}, time.Second, time.Millisecond)
	_, _, ok = fault.TriggerFault(notifyPoint)
	require.True(t, ok)
	select {
	case <-waitDone:
	case <-time.After(time.Second):
		t.Fatal("notifyall did not release fulltext2 after-commit waiter")
	}
}

func TestFulltext2AfterTailCommitFenceFaultCancelReleasesWaiter(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	require.True(t, fault.Enable())
	t.Cleanup(func() { fault.Disable() })
	require.NoError(t, fault.AddFaultPoint(ctx, fulltext2AfterTailCommitBeforeFenceFault, ":::", "wait", 0, "", false))
	t.Cleanup(func() {
		_, _ = fault.RemoveFaultPoint(context.Background(), fulltext2AfterTailCommitBeforeFenceFault)
	})
	const waitersPoint = "fulltext2-after-tail-commit-cancel-waiters-test"
	require.NoError(t, fault.AddFaultPoint(ctx, waitersPoint, ":::", "getwaiters", 0, fulltext2AfterTailCommitBeforeFenceFault, false))
	t.Cleanup(func() { _, _ = fault.RemoveFaultPoint(context.Background(), waitersPoint) })

	done := make(chan struct{})
	go func() {
		waitAfterFulltext2TailCommitBeforeFence(ctx, true)
		close(done)
	}()
	require.Eventually(t, func() bool {
		count, _, exists := fault.TriggerFault(waitersPoint)
		return exists && count == 1
	}, time.Second, time.Millisecond)
	cancel()
	select {
	case <-done:
	case <-time.After(time.Second):
		t.Fatal("cancel did not release fulltext2 after-commit waiter")
	}
	count, _, ok := fault.TriggerFault(waitersPoint)
	require.True(t, ok)
	require.Zero(t, count)
}
