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
