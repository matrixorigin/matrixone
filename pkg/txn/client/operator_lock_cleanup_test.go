// Copyright 2026 Matrix Origin
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
// http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package client

import (
	"context"
	"testing"

	"github.com/matrixorigin/matrixone/pkg/pb/timestamp"
	"github.com/matrixorigin/matrixone/pkg/pb/txn"
	"github.com/stretchr/testify/require"
)

type lockCleanupTimestampWaiter struct {
	TimestampWaiter
	calls     int
	requested timestamp.Timestamp
}

func (w *lockCleanupTimestampWaiter) GetTimestamp(_ context.Context, ts timestamp.Timestamp) (timestamp.Timestamp, error) {
	w.calls++
	w.requested = ts
	return ts, nil
}

func TestUnlockWaitsOnlyForActualCommit(t *testing.T) {
	for _, committed := range []bool{false, true} {
		t.Run(map[bool]string{false: "no commit", true: "committed"}[committed], func(t *testing.T) {
			runOperatorTests(t, func(ctx context.Context, tc *txnOperator, _ *testTxnSender) {
				locks := &trackingUnlockLockService{}
				waiter := &lockCleanupTimestampWaiter{}
				tc.AddWorkspace(&trackingWorkspace{readonly: true})
				tc.lockService = locks
				tc.timestampWaiter = waiter
				tc.mu.txn.Mode = txn.TxnMode_Pessimistic
				tc.mu.txn.Isolation = txn.TxnIsolation_RC
				if committed {
					tc.mu.txn.CommitTS = timestamp.Timestamp{PhysicalTime: 7}
				}
				require.NoError(t, tc.unlock(ctx))
				require.Equal(t, 1, locks.unlockCount)
				if committed {
					require.Equal(t, 1, waiter.calls)
					require.Equal(t, tc.mu.txn.CommitTS, waiter.requested)
				} else {
					require.Zero(t, waiter.calls)
				}
			})
		})
	}
}
