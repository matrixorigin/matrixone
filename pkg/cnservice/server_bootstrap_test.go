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
	"errors"
	"fmt"
	"net"
	"os"
	"syscall"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/matrixorigin/matrixone/pkg/common/moerr"
	"github.com/matrixorigin/matrixone/pkg/logservice"
)

type bootstrapLockClient struct {
	logservice.CNHAKeeperClient
	key       string
	batch     uint64
	requestID string
}

func (c *bootstrapLockClient) AllocateIDByKeyWithRequestID(
	_ context.Context,
	key string,
	batch uint64,
	requestID string,
) (uint64, error) {
	c.key = key
	c.batch = batch
	c.requestID = requestID
	return 1, nil
}

func TestBootstrapLockerUsesCNUUIDAsRequestID(t *testing.T) {
	client := &bootstrapLockClient{}
	l := locker{hakeeperClient: client, requestID: "cn-1"}
	ok, err := l.Get(context.Background(), "bootstrap")
	require.NoError(t, err)
	require.True(t, ok)
	require.Equal(t, "bootstrap", client.key)
	require.Equal(t, uint64(1), client.batch)
	require.Equal(t, "cn-1", client.requestID)
}

func TestBootstrapLockerRejectsClientWithoutIdempotentAllocation(t *testing.T) {
	l := locker{hakeeperClient: &testHAKClient{}, requestID: "cn-1"}
	_, err := l.Get(context.Background(), "bootstrap")
	require.Error(t, err)
	require.Contains(t, err.Error(), "idempotent bootstrap lock allocation")
}

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

	t.Run("context.DeadlineExceeded returns error", func(t *testing.T) {
		ctx := context.Background()
		err := handleBootstrapErr(ctx, context.DeadlineExceeded)
		require.Error(t, err)
		assert.ErrorIs(t, err, context.DeadlineExceeded)
	})

	t.Run("bootstrap timeout preserves cause", func(t *testing.T) {
		ctx, cancel := context.WithTimeoutCause(
			context.Background(), 0, moerr.CauseBootstrap,
		)
		defer cancel()
		<-ctx.Done()

		err := handleBootstrapErr(ctx, ctx.Err())
		require.Error(t, err)
		assert.ErrorIs(t, err, context.DeadlineExceeded)
		assert.Contains(t, err.Error(), "bootstrap")
	})

	t.Run("other error returns error", func(t *testing.T) {
		ctx := context.Background()
		bootstrapErr := errors.New("SQL execution failed")
		err := handleBootstrapErr(ctx, bootstrapErr)
		require.Error(t, err)
		assert.ErrorIs(t, err, bootstrapErr)
	})

	t.Run("moerr wrapped error returns error", func(t *testing.T) {
		ctx := context.Background()
		bootstrapErr := moerr.NewInternalErrorNoCtx("bootstrap init failed")
		err := handleBootstrapErr(ctx, bootstrapErr)
		require.Error(t, err)
		assert.ErrorIs(t, err, bootstrapErr)
	})

	t.Run("connection reset returns error", func(t *testing.T) {
		ctx := context.Background()
		bootstrapErr := &net.OpError{
			Op:  "read",
			Net: "tcp4",
			Err: os.NewSyscallError("read", syscall.ECONNRESET),
		}
		err := handleBootstrapErr(ctx, bootstrapErr)
		require.Error(t, err)
		assert.ErrorIs(t, err, bootstrapErr)
		assert.ErrorIs(t, err, syscall.ECONNRESET)
	})
}
