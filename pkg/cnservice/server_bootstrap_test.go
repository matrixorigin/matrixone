// Copyright 2021-2024 Matrix Origin
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

package cnservice

import (
	"context"
	"fmt"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/matrixorigin/matrixone/pkg/common/moerr"
)

func TestHandleBootstrapErr(t *testing.T) {
	t.Run("context.Canceled returns error", func(t *testing.T) {
		ctx := context.Background()
		err := handleBootstrapErr(ctx, context.Canceled)
		require.Error(t, err)
		assert.True(t, err == context.Canceled)
	})

	t.Run("wrapped context.Canceled returns error", func(t *testing.T) {
		ctx := context.Background()
		wrappedErr := fmt.Errorf("bootstrap failed: %w", context.Canceled)
		err := handleBootstrapErr(ctx, wrappedErr)
		require.Error(t, err)
		assert.ErrorIs(t, err, context.Canceled)
	})

	t.Run("context.DeadlineExceeded panics", func(t *testing.T) {
		ctx := context.Background()
		assert.Panics(t, func() {
			handleBootstrapErr(ctx, context.DeadlineExceeded)
		})
	})

	t.Run("bootstrap timeout with cause panics", func(t *testing.T) {
		// Simulate the real bootstrap context: WithTimeoutCause sets a
		// custom cause, but the 5-minute timeout is a legitimate failure
		// that must still panic.
		ctx, cancel := context.WithTimeoutCause(
			context.Background(), 0, moerr.CauseBootstrap,
		)
		defer cancel()
		// Wait for the timeout to fire.
		<-ctx.Done()

		assert.Panics(t, func() {
			handleBootstrapErr(ctx, ctx.Err())
		})
	})

	t.Run("other error panics", func(t *testing.T) {
		ctx := context.Background()
		assert.Panics(t, func() {
			handleBootstrapErr(ctx, fmt.Errorf("SQL execution failed"))
		})
	})

	t.Run("moerr wrapped error panics", func(t *testing.T) {
		ctx := context.Background()
		assert.Panics(t, func() {
			handleBootstrapErr(ctx, moerr.NewInternalErrorNoCtx("bootstrap init failed"))
		})
	})
}

func TestBootstrapWithRetry(t *testing.T) {
	for _, tc := range []struct {
		name string
		err  error
	}{
		{"connection reset", moerr.NewConnectionResetNoCtx()},
		{"backend closed", moerr.NewBackendClosedNoCtx()},
		{"backend cannot connect", moerr.NewBackendCannotConnectNoCtx("hakeeper")},
	} {
		t.Run(tc.name, func(t *testing.T) {
			attempts := 0
			err := bootstrapWithRetry(context.Background(), time.Millisecond, func(context.Context) error {
				attempts++
				if attempts == 1 {
					return tc.err
				}
				return nil
			})
			require.NoError(t, err)
			require.Equal(t, 2, attempts)
		})
	}

	t.Run("does not retry bootstrap error", func(t *testing.T) {
		attempts := 0
		expected := moerr.NewInternalErrorNoCtx("bootstrap init failed")
		err := bootstrapWithRetry(context.Background(), time.Millisecond, func(context.Context) error {
			attempts++
			return expected
		})
		require.ErrorIs(t, err, expected)
		require.Equal(t, 1, attempts)
	})
}
