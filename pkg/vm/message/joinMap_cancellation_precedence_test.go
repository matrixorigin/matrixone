// Copyright 2026 Matrix Origin
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0

package message

import (
	"context"
	"errors"
	"runtime"
	"sync"
	"testing"
	"time"

	"github.com/matrixorigin/matrixone/pkg/common/moerr"
	"github.com/stretchr/testify/require"
)

func waitForJoinMapWaiter(t *testing.T, mb *MessageBoard, want int) {
	t.Helper()
	deadline := time.Now().Add(2 * time.Second)
	for {
		mb.rwMutex.RLock()
		waiters := len(mb.waiters)
		mb.rwMutex.RUnlock()
		if waiters == want {
			return
		}
		require.Less(t, time.Now(), deadline)
		runtime.Gosched()
	}
}

func TestReceiveJoinMapResultCancellationPrecedence(t *testing.T) {
	t.Run("context first", func(t *testing.T) {
		mb := NewMessageBoard()
		ctx, cancel := context.WithCancelCause(context.Background())
		resultCh := make(chan error, 1)
		go func() {
			_, err := ReceiveJoinMapResult(51, false, 0, mb, ctx)
			resultCh <- err
		}()
		waitForJoinMapWaiter(t, mb, 1)

		primaryErr := moerr.NewErrFKNoReferencedRow2(context.Background())
		cancel(primaryErr)
		select {
		case err := <-resultCh:
			require.ErrorIs(t, err, primaryErr)
		case <-time.After(2 * time.Second):
			t.Fatal("ReceiveJoinMapResult did not return after cancellation")
		}
	})

	t.Run("queued message first", func(t *testing.T) {
		mb := NewMessageBoard()
		ctx, cancel := context.WithCancelCause(context.Background())
		primaryErr := moerr.NewErrFKNoReferencedRow2(context.Background())
		cancel(primaryErr)
		SendJoinMapResult(NewJoinMapBuildErrorResult(context.Canceled), 52, false, 0, mb)

		_, err := ReceiveJoinMap(52, false, 0, mb, ctx)
		require.ErrorIs(t, err, primaryErr)
	})
}

func TestReceiveJoinMapResultCancellationTreePrecedence(t *testing.T) {
	primaryErr := moerr.NewErrFKNoReferencedRow2(context.Background())

	t.Run("multiple cancellation children", func(t *testing.T) {
		mb := NewMessageBoard()
		ctx, cancel := context.WithCancelCause(context.Background())
		cancel(errors.Join(primaryErr, context.Canceled))
		SendJoinMapResult(NewJoinMapBuildErrorResult(
			errors.Join(context.Canceled, context.Canceled),
		), 53, false, 0, mb)

		_, err := ReceiveJoinMap(53, false, 0, mb, ctx)
		require.ErrorIs(t, err, primaryErr)
	})

	t.Run("query interrupted", func(t *testing.T) {
		mb := NewMessageBoard()
		ctx, cancel := context.WithCancelCause(context.Background())
		cancel(primaryErr)
		SendJoinMapResult(NewJoinMapBuildErrorResult(
			moerr.NewQueryInterrupted(context.Background()),
		), 54, false, 0, mb)

		_, err := ReceiveJoinMap(54, false, 0, mb, ctx)
		require.ErrorIs(t, err, primaryErr)
	})

	t.Run("mixed message keeps substantive leaf", func(t *testing.T) {
		mb := NewMessageBoard()
		SendJoinMapResult(NewJoinMapBuildErrorResult(
			errors.Join(context.Canceled, primaryErr),
		), 55, false, 0, mb)

		_, err := ReceiveJoinMap(55, false, 0, mb, context.Background())
		require.ErrorIs(t, err, primaryErr)
	})
}

func TestReceiveJoinMapResultDeadlinePrecedence(t *testing.T) {
	t.Run("query deadline", func(t *testing.T) {
		mb := NewMessageBoard()
		diagnostic := moerr.NewErrFKNoReferencedRow2(context.Background())
		ctx, cancel := context.WithTimeoutCause(context.Background(), 10*time.Millisecond, diagnostic)
		defer cancel()

		_, err := ReceiveJoinMap(56, false, 0, mb, ctx)
		require.ErrorIs(t, err, context.DeadlineExceeded)
		require.NotErrorIs(t, err, diagnostic)
	})

	t.Run("independent producer deadline", func(t *testing.T) {
		mb := NewMessageBoard()
		ctx, cancel := context.WithCancelCause(context.Background())
		primaryErr := moerr.NewErrFKNoReferencedRow2(context.Background())
		cancel(primaryErr)
		SendJoinMapResult(NewJoinMapBuildErrorResult(context.DeadlineExceeded), 57, false, 0, mb)

		_, err := ReceiveJoinMap(57, false, 0, mb, ctx)
		require.ErrorIs(t, err, context.DeadlineExceeded)
		require.NotErrorIs(t, err, primaryErr)
	})
}

func TestReceiveJoinMapResultConcurrentCancellationDoesNotLeak(t *testing.T) {
	mb := NewMessageBoard()
	ctx, cancel := context.WithCancelCause(context.Background())
	const consumers = 2
	var wg sync.WaitGroup
	results := make([]error, consumers)
	wg.Add(consumers)
	for i := range results {
		go func(i int) {
			defer wg.Done()
			_, results[i] = ReceiveJoinMapResult(58, false, 0, mb, ctx)
		}(i)
	}
	waitForJoinMapWaiter(t, mb, consumers)
	primaryErr := moerr.NewErrFKNoReferencedRow2(context.Background())
	cancel(primaryErr)
	wg.Wait()
	for _, err := range results {
		require.ErrorIs(t, err, primaryErr)
	}
}
