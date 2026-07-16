// Copyright 2021 - 2023 Matrix Origin
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

package proxy

import (
	"context"
	"errors"
	"time"

	"github.com/matrixorigin/matrixone/pkg/common/moerr"
	"github.com/matrixorigin/matrixone/pkg/util"
	db_holder "github.com/matrixorigin/matrixone/pkg/util/export/etl/db"
)

const (
	BootstrapInterval       = time.Millisecond * 200
	BootstrapTimeout        = time.Minute * 5
	bootstrapRequestTimeout = time.Second * 3
)

// bootstrap retries until the task-table credentials are available. It returns
// nil on success, the parent cancellation on cancellation, or a deadline error
// together with the last HAKeeper error when the bootstrap window expires.
func (h *handler) bootstrap(ctx context.Context) error {
	return h.bootstrapWithTimeout(ctx, BootstrapInterval, BootstrapTimeout)
}

func (h *handler) bootstrapWithTimeout(
	ctx context.Context,
	interval time.Duration,
	timeout time.Duration,
) error {
	ctx, cancel := context.WithTimeoutCause(ctx, timeout, moerr.CauseProxyBootstrap)
	defer cancel()

	ticker := time.NewTicker(interval)
	defer ticker.Stop()
	getClient := func() util.HAKeeperClient {
		return h.haKeeperClient
	}
	var lastErr error
	for {
		select {
		case <-ticker.C:
			requestCtx, requestCancel := context.WithTimeoutCause(
				ctx,
				bootstrapRequestTimeout,
				moerr.CauseProxyBootstrap,
			)
			state, err := h.haKeeperClient.GetClusterState(requestCtx)
			if err != nil {
				lastErr = moerr.AttachCause(requestCtx, err)
			} else {
				lastErr = nil
			}
			requestCancel()

			if ctx.Err() != nil {
				return bootstrapContextError(ctx, lastErr)
			}
			if err != nil {
				continue
			}
			if state.TaskTableUser.GetUsername() != "" && state.TaskTableUser.GetPassword() != "" {
				db_holder.SetSQLWriterDBUser(db_holder.MOLoggerUser, state.TaskTableUser.GetPassword())
				db_holder.SetSQLWriterDBAddressFunc(util.AddressFunc(h.config.UUID, getClient))
				h.sqlWorker.SetSQLUser(SQLUsername, state.TaskTableUser.GetPassword())
				h.sqlWorker.SetAddressFn(util.AddressFunc(h.config.UUID, getClient))
				return nil
			}
		case <-ctx.Done():
			return bootstrapContextError(ctx, lastErr)
		}
	}
}

func bootstrapContextError(ctx context.Context, lastErr error) error {
	err := ctx.Err()
	if cause := context.Cause(ctx); cause != nil && !errors.Is(err, cause) {
		err = errors.Join(err, cause)
	}
	if errors.Is(err, context.DeadlineExceeded) && lastErr != nil {
		err = errors.Join(err, lastErr)
	}
	return err
}
