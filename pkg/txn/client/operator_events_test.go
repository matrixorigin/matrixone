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
	"errors"
	"testing"
	"time"

	"github.com/matrixorigin/matrixone/pkg/pb/timestamp"
	"github.com/matrixorigin/matrixone/pkg/pb/txn"
	"github.com/matrixorigin/matrixone/pkg/txn/rpc"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestEventCallbacksDefaultOrderAndFirstError(t *testing.T) {
	errDefault := errors.New("default callback failed")
	errCustom := errors.New("custom callback failed")

	tests := []struct {
		name     string
		default2 error
		custom1  error
		want     []string
		wantErr  error
	}{
		{
			name: "all callbacks",
			want: []string{"default-1", "default-2", "custom-1", "custom-2"},
		},
		{
			name:     "default error stops custom callbacks",
			default2: errDefault,
			want:     []string{"default-1", "default-2"},
			wantErr:  errDefault,
		},
		{
			name:    "custom error stops later custom callbacks",
			custom1: errCustom,
			want:    []string{"default-1", "default-2", "custom-1"},
			wantErr: errCustom,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			var called []string
			callback := func(name string, callbackErr error) TxnEventCallback {
				return TxnEventCallback{Func: func(
					context.Context,
					TxnOperator,
					TxnEvent,
					any,
				) error {
					called = append(called, name)
					return callbackErr
				}}
			}

			defaults := defaultTxnEventCallbacks{closed: [2]TxnEventCallback{
				callback("default-1", nil),
				callback("default-2", tt.default2),
			}}
			shared := txnEventCallbacks{defaults: &defaults}
			op := &txnOperator{}
			op.setDefaultEventCallbacks(&shared)
			op.AppendEventCallback(
				ClosedEvent,
				callback("custom-1", tt.custom1),
				callback("custom-2", nil),
			)

			err := op.triggerEvent(context.Background(), TxnEvent{Event: ClosedEvent})
			if tt.wantErr == nil {
				require.NoError(t, err)
			} else {
				require.ErrorIs(t, err, tt.wantErr)
			}
			require.Equal(t, tt.want, called)
		})
	}
}

func TestDefaultEventCallbacksCopyOnWrite(t *testing.T) {
	RunTxnTests(func(c TxnClient, _ rpc.TxnSender) {
		ctx := context.Background()
		client := c.(*txnClient)
		op1, err := c.New(ctx, timestamp.Timestamp{})
		require.NoError(t, err)
		op2, err := c.New(ctx, timestamp.Timestamp{})
		require.NoError(t, err)
		txn1 := op1.(*txnOperator)
		txn2 := op2.(*txnOperator)

		require.Same(t, &client.sharedEventCallbacks, txn1.mu.callbacks)
		require.Same(t, &client.sharedEventCallbacks, txn2.mu.callbacks)
		require.Nil(t, client.sharedEventCallbacks.callbacks)

		txn1.AppendEventCallback(ClosedEvent, NewTxnEventCallback(
			func(context.Context, TxnOperator, TxnEvent, any) error { return nil },
		))

		require.NotSame(t, &client.sharedEventCallbacks, txn1.mu.callbacks)
		require.Same(t, &client.defaultEventCallbacks, txn1.mu.callbacks.defaults)
		require.Same(t, &client.sharedEventCallbacks, txn2.mu.callbacks)
		require.Nil(t, client.sharedEventCallbacks.callbacks)

		require.NoError(t, txn1.Rollback(ctx))
		require.NoError(t, txn2.Rollback(ctx))
	})
}

func TestClientClosedEventDefaultsRunBeforeCustomOnce(t *testing.T) {
	tests := []struct {
		name    string
		options []TxnOption
		close   func(context.Context, TxnOperator) error
		status  txn.TxnStatus
	}{
		{
			name:    "commit",
			options: []TxnOption{WithTxnReadyOnly()},
			close:   func(ctx context.Context, op TxnOperator) error { return op.Commit(ctx) },
			status:  txn.TxnStatus_Committed,
		},
		{
			name:   "rollback",
			close:  func(ctx context.Context, op TxnOperator) error { return op.Rollback(ctx) },
			status: txn.TxnStatus_Aborted,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			RunTxnTests(func(c TxnClient, _ rpc.TxnSender) {
				ctx := context.Background()
				client := c.(*txnClient)
				op, err := c.New(ctx, timestamp.Timestamp{}, tt.options...)
				require.NoError(t, err)
				txnOp := op.(*txnOperator)
				commitTS := timestamp.Timestamp{PhysicalTime: 10}
				txnOp.mu.Lock()
				txnOp.mu.txn.CommitTS = commitTS
				txnOp.mu.Unlock()

				called := 0
				op.AppendEventCallback(ClosedEvent, NewTxnEventCallback(
					func(_ context.Context, _ TxnOperator, event TxnEvent, _ any) error {
						called++
						require.Equal(t, tt.status, event.Txn.Status)
						require.Equal(t, commitTS, client.GetLatestCommitTS())
						_, active := client.getActiveTxn(string(txnOp.reset.txnID))
						require.False(t, active)
						return nil
					},
				))

				require.NoError(t, tt.close(ctx, op))
				require.NoError(t, tt.close(ctx, op))
				require.Equal(t, 1, called)
			})
		})
	}
}

func TestRestartTxnRestoresDefaultsAndDropsCustomCallbacks(t *testing.T) {
	RunTxnTests(func(c TxnClient, _ rpc.TxnSender) {
		ctx := context.Background()
		client := c.(*txnClient)
		op, err := c.New(ctx, timestamp.Timestamp{})
		require.NoError(t, err)
		txnOp := op.(*txnOperator)
		firstTxnID := string(txnOp.reset.txnID)

		firstCalls := 0
		op.AppendEventCallback(ClosedEvent, NewTxnEventCallback(
			func(context.Context, TxnOperator, TxnEvent, any) error {
				firstCalls++
				return nil
			},
		))
		require.NoError(t, op.Rollback(ctx))
		require.Equal(t, 1, firstCalls)

		restarted, err := client.RestartTxn(ctx, op, timestamp.Timestamp{})
		require.NoError(t, err)
		require.Same(t, op, restarted)
		require.NotEqual(t, firstTxnID, string(txnOp.reset.txnID))
		require.Same(t, &client.sharedEventCallbacks, txnOp.mu.callbacks)

		secondCalls := 0
		restarted.AppendEventCallback(ClosedEvent, NewTxnEventCallback(
			func(context.Context, TxnOperator, TxnEvent, any) error {
				secondCalls++
				return nil
			},
		))
		require.NoError(t, restarted.Rollback(ctx))
		require.Equal(t, 1, firstCalls)
		require.Equal(t, 1, secondCalls)
		require.Equal(t, int64(0), client.atomic.activeTxnCount.Load())
	})
}

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
			token := mustEnterRunSQL(t, tc, sqlCancel, "select 1")
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
			token := mustEnterRunSQL(t, tc, sqlCancel, "select 1")
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
