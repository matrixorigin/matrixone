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
