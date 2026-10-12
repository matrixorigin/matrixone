// Copyright 2021 - 2022 Matrix Origin
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

package service

import (
	"context"
	"testing"
	"time"

	"github.com/matrixorigin/matrixone/pkg/common/moerr"
	"github.com/matrixorigin/matrixone/pkg/pb/txn"
	"github.com/matrixorigin/matrixone/pkg/txn/storage/mem"
	"github.com/matrixorigin/matrixone/pkg/txn/util"
	"github.com/stretchr/testify/require"
)

func TestSingleTNWriteCommit(t *testing.T) {
	sender := NewTestSender()
	txnService := NewTestTxnService(t, 1, sender, NewTestClock(0))
	require.NoError(t, txnService.Start())
	t.Cleanup(func() {
		require.NoError(t, txnService.Close(false))
		require.NoError(t, sender.Close())
	})
	sender.AddTxnService(txnService)

	meta := NewTestTxn(1, 1, 1)
	result, err := sender.Send(context.Background(), []txn.TxnRequest{
		NewTestWriteRequest(1, meta, 1),
		NewTestCommitRequest(meta),
	})
	require.NoError(t, err)
	require.Len(t, result.Responses, 2)
	require.Nil(t, result.Responses[0].TxnError)
	require.Nil(t, result.Responses[1].TxnError)
	require.Equal(t, txn.TxnStatus_Committed, result.Responses[1].Txn.Status)
}

func TestCommitRejectsMultipleTNShards(t *testing.T) {
	sender := NewTestSender()
	txnService := NewTestTxnService(t, 1, sender, NewTestClock(0))
	require.NoError(t, txnService.Start())
	t.Cleanup(func() {
		require.NoError(t, txnService.Close(false))
		require.NoError(t, sender.Close())
	})
	sender.AddTxnService(txnService)

	meta := NewTestTxn(1, 1, 1, 2)
	_, err := sender.Send(context.Background(), []txn.TxnRequest{
		NewTestWriteRequest(1, meta, 1),
	})
	require.NoError(t, err)

	result, err := sender.Send(context.Background(), []txn.TxnRequest{
		NewTestCommitRequest(meta),
	})
	require.NoError(t, err)
	require.Len(t, result.Responses, 1)
	require.NotNil(t, result.Responses[0].TxnError)
	require.True(t, moerr.IsMoErrCode(
		result.Responses[0].TxnError.UnwrapError(),
		moerr.ErrNotSupported,
	))

	storage := txnService.(*service).storage.(*mem.KVTxnStorage)
	require.Nil(t, storage.GetUncommittedTxn(meta.ID))
}

func TestRollbackRejectsMultipleTNShards(t *testing.T) {
	sender := NewTestSender()
	txnService := NewTestTxnService(t, 1, sender, NewTestClock(0))
	require.NoError(t, txnService.Start())
	t.Cleanup(func() {
		require.NoError(t, txnService.Close(false))
		require.NoError(t, sender.Close())
	})
	sender.AddTxnService(txnService)

	meta := NewTestTxn(1, 1, 1, 2)
	_, err := sender.Send(context.Background(), []txn.TxnRequest{
		NewTestWriteRequest(1, meta, 1),
	})
	require.NoError(t, err)

	result, err := sender.Send(context.Background(), []txn.TxnRequest{
		NewTestRollbackRequest(meta),
	})
	require.NoError(t, err)
	require.NotNil(t, result.Responses[0].TxnError)
	require.True(t, moerr.IsMoErrCode(
		result.Responses[0].TxnError.UnwrapError(),
		moerr.ErrNotSupported,
	))
	storage := txnService.(*service).storage.(*mem.KVTxnStorage)
	require.Nil(t, storage.GetUncommittedTxn(meta.ID))
}

func TestSingleTNRollback(t *testing.T) {
	sender := NewTestSender()
	txnService := NewTestTxnService(t, 1, sender, NewTestClock(0))
	require.NoError(t, txnService.Start())
	t.Cleanup(func() {
		require.NoError(t, txnService.Close(false))
		require.NoError(t, sender.Close())
	})
	sender.AddTxnService(txnService)

	meta := NewTestTxn(1, 1, 1)
	result, err := sender.Send(context.Background(), []txn.TxnRequest{
		NewTestWriteRequest(1, meta, 1),
		NewTestRollbackRequest(meta),
	})
	require.NoError(t, err)
	require.Len(t, result.Responses, 2)
	require.Nil(t, result.Responses[0].TxnError)
	require.Nil(t, result.Responses[1].TxnError)
	require.Equal(t, txn.TxnStatus_Aborted, result.Responses[1].Txn.Status)

	storage := txnService.(*service).storage.(*mem.KVTxnStorage)
	require.Nil(t, storage.GetUncommittedTxn(meta.ID))

	// A delayed Write may recreate the same logical transaction after Rollback.
	// It must be ready for use, and its eventual terminal request must still
	// notify current waiters and clean both the service map and storage.
	result, err = sender.Send(t.Context(), []txn.TxnRequest{
		NewTestWriteRequest(1, meta, 1),
	})
	require.NoError(t, err)
	require.Nil(t, result.Responses[0].TxnError)
	s := txnService.(*service)
	c := s.getTxnContext(meta.ID)
	require.NotNil(t, c)
	current, _ := c.getTxnSnapshot()
	require.Equal(t, meta.ID, current.ID)
	w := acquireWaiter()
	t.Cleanup(w.close)
	require.True(t, c.addWaiter(meta.ID, w, txn.TxnStatus_Aborted))
	result, err = sender.Send(t.Context(), []txn.TxnRequest{NewTestRollbackRequest(meta)})
	require.NoError(t, err)
	require.Nil(t, result.Responses[0].TxnError)
	ctx, cancel := context.WithTimeout(t.Context(), 10*time.Second)
	defer cancel()
	status, err := w.wait(ctx)
	require.NoError(t, err)
	require.Equal(t, txn.TxnStatus_Aborted, status)
	require.Nil(t, s.getTxnContext(meta.ID))
	require.Nil(t, storage.GetUncommittedTxn(meta.ID))
}

func TestRollbackRejectsStaleContext(t *testing.T) {
	for _, reused := range []bool{false, true} {
		name := "reset"
		if reused {
			name = "reused"
		}
		t.Run(name, func(t *testing.T) {
			oldMeta := NewTestTxn(1, 1, 1)
			current := NewTestTxn(2, 1, 1)
			s := &service{logger: util.GetLogger(""), shard: oldMeta.TNShards[0]}
			c := &txnContext{logger: s.logger}
			c.mu.Lock()
			c.initLocked(oldMeta, acquireNotifier())
			c.resetLocked()
			c.mu.Unlock()
			t.Cleanup(func() {
				s.transactions.Delete(string(oldMeta.ID))
				s.transactions.Delete(string(current.ID))
				c.mu.Lock()
				defer c.mu.Unlock()
				if c.nt != nil {
					s.releaseTxnContextLocked(c)
				}
			})
			w := acquireWaiter()
			t.Cleanup(w.close)
			if reused {
				c.mu.Lock()
				c.initLocked(current, acquireNotifier())
				c.mu.Unlock()
				s.transactions.Store(string(current.ID), c)
				require.True(t, c.addWaiter(current.ID, w, txn.TxnStatus_Committed))
			}
			nt := c.nt
			// Inject the post-lookup stale-pointer boundary: an old request
			// retained c before retirement, and now acquires it after reset/reuse.
			// The alias avoids a scheduler-dependent pause inside Rollback.
			s.transactions.Store(string(oldMeta.ID), c)
			request := NewTestRollbackRequest(oldMeta)
			response := &txn.TxnResponse{}
			require.NotPanics(t, func() {
				require.NoError(t, s.Rollback(t.Context(), &request, response))
			})
			require.NotNil(t, response.TxnError)
			require.True(t, moerr.IsMoErrCode(response.TxnError.UnwrapError(), moerr.ErrTxnNotFound))
			require.Same(t, nt, c.nt)
			observed, _ := c.getTxnSnapshot()
			if reused {
				require.Equal(t, current.ID, observed.ID)
				require.Same(t, c, s.getTxnContext(current.ID))
			} else {
				require.Empty(t, observed.ID)
			}
			select {
			case <-w.c:
				t.Fatal("stale rollback notified another generation's waiter")
			default:
			}
		})
	}
}

func TestMultiTNCleanupWithoutTxnContext(t *testing.T) {
	sender := NewTestSender()
	txnService := NewTestTxnService(t, 1, sender, NewTestClock(0))
	require.NoError(t, txnService.Start())
	t.Cleanup(func() {
		require.NoError(t, txnService.Close(false))
		require.NoError(t, sender.Close())
	})
	sender.AddTxnService(txnService)

	result, err := sender.Send(context.Background(), []txn.TxnRequest{
		NewTestCommitRequest(NewTestTxn(1, 1, 1, 2)),
	})
	require.NoError(t, err)
	require.Len(t, result.Responses, 1)
	require.NotNil(t, result.Responses[0].TxnError)
	require.True(t, moerr.IsMoErrCode(
		result.Responses[0].TxnError.UnwrapError(),
		moerr.ErrNotSupported,
	))
}

func TestCommitRequestExpired(t *testing.T) {
	now := time.Unix(0, 100)
	require.True(t, commitRequestExpired(
		&txn.TxnRequest{CommitRequest: &txn.TxnCommitRequest{DeadlineUnixNano: 99}},
		now,
		0,
	))
	require.False(t, commitRequestExpired(
		&txn.TxnRequest{CommitRequest: &txn.TxnCommitRequest{DeadlineUnixNano: 101}},
		now,
		0,
	))
	require.False(t, commitRequestExpired(&txn.TxnRequest{}, now, 0))
}
