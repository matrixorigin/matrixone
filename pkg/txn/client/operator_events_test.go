// Copyright 2023 Matrix Origin
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

package client

import (
	"context"
	"testing"
	"time"

	"github.com/matrixorigin/matrixone/pkg/pb/txn"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestClosedEvent(t *testing.T) {
	runClosedEventTests(t,
		func(tc *txnOperator) func(context.Context) error {
			return tc.Commit
		},
		txn.TxnStatus_Committed)

	runClosedEventTests(t,
		func(tc *txnOperator) func(context.Context) error {
			return tc.Rollback
		},
		txn.TxnStatus_Aborted)
}

func TestCommitClosedEventAfterRunningSQLCleanup(t *testing.T) {
	runOperatorTests(
		t,
		func(ctx context.Context, tc *txnOperator, _ *testTxnSender) {
			_, sqlCancel := context.WithCancel(context.Background())
			token := tc.EnterRunSqlWithTokenAndSQL(sqlCancel, "select 1")
			defer sqlCancel()

			commitCtx, cancelCommit := context.WithCancel(ctx)
			cancelCommit()

			closedC := make(chan TxnEvent, 1)
			tc.AppendEventCallback(ClosedEvent,
				TxnEventCallback{
					Func: func(ctx context.Context, tc TxnOperator, event TxnEvent, value any) error {
						closedC <- event
						return nil
					},
				})

			require.Error(t, tc.Commit(commitCtx))
			select {
			case event := <-closedC:
				t.Fatalf("transaction closed before running SQL exited: %+v", event)
			default:
			}
			tc.mu.RLock()
			assert.False(t, tc.mu.closed)
			tc.mu.RUnlock()
			tc.ExitRunSqlWithToken(token)
			select {
			case event := <-closedC:
				assert.Equal(t, txn.TxnStatus_Aborted, event.Txn.Status)
				assert.Error(t, event.Err)
			case <-time.After(time.Second):
				t.Fatal("transaction did not close after running SQL exited")
			}
			tc.mu.RLock()
			assert.True(t, tc.mu.closed)
			tc.mu.RUnlock()
		})
}

func TestRollbackClosedEventAfterRunningSQLCleanup(t *testing.T) {
	runOperatorTests(
		t,
		func(ctx context.Context, tc *txnOperator, _ *testTxnSender) {
			_, sqlCancel := context.WithCancel(context.Background())
			token := tc.EnterRunSqlWithTokenAndSQL(sqlCancel, "select 1")
			defer sqlCancel()

			rollbackCtx, cancelRollback := context.WithCancel(ctx)
			cancelRollback()

			closedC := make(chan TxnEvent, 1)
			tc.AppendEventCallback(ClosedEvent,
				TxnEventCallback{
					Func: func(ctx context.Context, tc TxnOperator, event TxnEvent, value any) error {
						closedC <- event
						return nil
					},
				})

			require.Error(t, tc.Rollback(rollbackCtx))
			select {
			case event := <-closedC:
				t.Fatalf("transaction closed before running SQL exited: %+v", event)
			default:
			}
			tc.mu.RLock()
			assert.False(t, tc.mu.closed)
			tc.mu.RUnlock()
			tc.ExitRunSqlWithToken(token)
			select {
			case event := <-closedC:
				assert.Equal(t, txn.TxnStatus_Aborted, event.Txn.Status)
				assert.Error(t, event.Err)
			case <-time.After(time.Second):
				t.Fatal("transaction did not close after running SQL exited")
			}
			tc.mu.RLock()
			assert.True(t, tc.mu.closed)
			tc.mu.RUnlock()
		})
}

func runClosedEventTests(
	t *testing.T,
	getAction func(tc *txnOperator) func(context.Context) error,
	status txn.TxnStatus) {
	runOperatorTests(
		t,
		func(ctx context.Context, tc *txnOperator, _ *testTxnSender) {
			cnt := 0
			tc.AppendEventCallback(ClosedEvent,
				TxnEventCallback{
					Func: func(ctx context.Context, tc TxnOperator, event TxnEvent, value any) error {
						cnt++
						assert.Equal(t, status, event.Txn.Status)
						return nil
					},
				})
			require.NoError(t, getAction(tc)(ctx))
			assert.Equal(t, 1, cnt)
		})
}
