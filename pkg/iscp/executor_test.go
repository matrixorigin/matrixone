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
	"errors"
	"fmt"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

func TestRetryReturnsCanceledContextBeforeFirstAttempt(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	cancel()

	called := false
	err := retry(ctx, func() error {
		called = true
		return nil
	}, 3, time.Millisecond, time.Second)

	require.False(t, called)
	require.ErrorIs(t, err, context.Canceled)
}

func TestRetryReturnsCanceledContextWhenRetryTimesZero(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	cancel()

	called := false
	err := retry(ctx, func() error {
		called = true
		return nil
	}, 0, time.Millisecond, time.Second)

	require.False(t, called)
	require.ErrorIs(t, err, context.Canceled)
}

func TestRetryReturnsCanceledContextDuringAttempt(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	called := false
	err := retry(ctx, func() error {
		called = true
		cancel()
		return errors.New("retryable")
	}, 3, time.Hour, time.Hour)

	require.True(t, called)
	require.ErrorIs(t, err, context.Canceled)
}

func TestRetryBackoffInterruptedByContextCancellation(t *testing.T) {
	for _, cancelAfterCalls := range []int{1, 2} {
		t.Run(fmt.Sprintf("backoff-%d", cancelAfterCalls), func(t *testing.T) {
			ctx, cancel := context.WithCancel(context.Background())
			defer cancel()

			calls := 0
			start := time.Now()
			err := retry(ctx, func() error {
				calls++
				if calls == cancelAfterCalls {
					go func() {
						time.Sleep(10 * time.Millisecond)
						cancel()
					}()
				}
				return errors.New("retryable")
			}, 3, 20*time.Millisecond, time.Hour)

			require.Equal(t, cancelAfterCalls, calls)
			require.ErrorIs(t, err, context.Canceled)
			require.Less(t, time.Since(start), time.Second)
		})
	}
}

func TestRegisterJobReturnsCanceledContextBeforeFirstAttempt(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	cancel()

	ok, err := RegisterJob(ctx, "", nil, nil, nil, false)

	require.False(t, ok)
	require.ErrorIs(t, err, context.Canceled)
}

func TestUnregisterJobReturnsCanceledContextBeforeFirstAttempt(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	cancel()

	ok, err := UnregisterJob(ctx, "", nil, nil)

	require.False(t, ok)
	require.ErrorIs(t, err, context.Canceled)
}
