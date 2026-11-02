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

package test

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"github.com/matrixorigin/matrixone/pkg/defines"
	pbtxn "github.com/matrixorigin/matrixone/pkg/pb/txn"
	"github.com/matrixorigin/matrixone/pkg/txn/client"
)

type snapshotCleanupTxn struct {
	client.TxnOperator
	status   pbtxn.TxnStatus
	rollback func(context.Context) error
}

func (txn *snapshotCleanupTxn) Status() pbtxn.TxnStatus { return txn.status }
func (txn *snapshotCleanupTxn) Rollback(ctx context.Context) error {
	return txn.rollback(ctx)
}

func TestSnapshotAdvanceCleanup(t *testing.T) {
	for _, state := range []string{"live", "canceled", "expired"} {
		t.Run(state, func(t *testing.T) {
			ctx := context.WithValue(context.Background(), defines.TenantIDKey{}, uint32(42))
			switch state {
			case "canceled":
				var cancel context.CancelFunc
				ctx, cancel = context.WithCancel(ctx)
				cancel()
				require.ErrorIs(t, ctx.Err(), context.Canceled)
			case "expired":
				var cancel context.CancelFunc
				ctx, cancel = context.WithDeadline(ctx, time.Time{})
				defer cancel()
				require.ErrorIs(t, ctx.Err(), context.DeadlineExceeded)
			}

			var cleanupCtx context.Context
			calls := 0
			failure := errors.New("rollback failure")
			txn := &snapshotCleanupTxn{status: pbtxn.TxnStatus_Active}
			txn.rollback = func(ctx context.Context) error {
				calls++
				cleanupCtx = ctx
				require.NoError(t, ctx.Err())
				require.Equal(t, uint32(42), ctx.Value(defines.TenantIDKey{}))
				deadline, ok := ctx.Deadline()
				require.True(t, ok)
				require.LessOrEqual(t, time.Until(deadline), time.Minute)
				return failure
			}
			require.ErrorIs(t, rollbackSnapshotAdvanceTxn(ctx, txn), failure)
			require.Equal(t, 1, calls, "cleanup must not retry a failed rollback")
			require.ErrorIs(t, cleanupCtx.Err(), context.Canceled, "release the cleanup timer")
		})
	}

	for _, status := range []pbtxn.TxnStatus{pbtxn.TxnStatus_Committed, pbtxn.TxnStatus_Aborted} {
		t.Run(status.String(), func(t *testing.T) {
			txn := &snapshotCleanupTxn{status: status, rollback: func(context.Context) error {
				t.Fatal("terminal transaction must not roll back again")
				return nil
			}}
			require.NoError(t, rollbackSnapshotAdvanceTxn(context.Background(), txn))
		})
	}
	require.NoError(t, rollbackSnapshotAdvanceTxn(context.Background(), nil))
}
