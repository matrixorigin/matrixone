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
	"bytes"
	"context"

	"github.com/matrixorigin/matrixone/pkg/pb/txn"
)

func (s *service) Prepare(ctx context.Context, request *txn.TxnRequest, response *txn.TxnResponse) error {
	response.PrepareResponse = &txn.TxnPrepareResponse{}
	s.validDNShard(request)

	txnID := request.Txn.ID
	txnCtx := s.getTxnContext(txnID)
	if txnCtx == nil {
		response.TxnError = newTxnNotFoundError()
		return nil
	}

	txnCtx.mu.Lock()
	defer txnCtx.mu.Unlock()

	newTxn := txnCtx.getTxnLocked()
	if !bytes.Equal(newTxn.ID, txnID) {
		response.TxnError = newTxnNotFoundError()
		return nil
	}

	response.Txn = &newTxn
	switch newTxn.Status {
	case txn.TxnStatus_Active:
		break
	case txn.TxnStatus_Prepared:
		return nil
	default:
		response.TxnError = newTxnNotActiveError()
		return nil
	}

	if err := s.storage.Prepare(request.Txn); err != nil {
		response.TxnError = newTAEPrepareError(err)
		return nil
	}

	newTxn.Status = txn.TxnStatus_Prepared
	txnCtx.changeStatusLocked(txn.TxnStatus_Prepared)
	return nil
}

func (s *service) GetStatus(ctx context.Context, request *txn.TxnRequest, response *txn.TxnResponse) error {
	response.GetStatusResponse = &txn.TxnGetStatusResponse{}
	s.validDNShard(request)

	txnID := request.Txn.ID
	txnCtx := s.getTxnContext(txnID)
	if txnCtx == nil {
		return nil
	}

	txnCtx.mu.RLock()
	defer txnCtx.mu.RUnlock()

	txn := txnCtx.getTxnLocked()
	if !bytes.Equal(txn.ID, txnID) {
		return nil
	}

	response.Txn = &txn
	return nil
}

func (s *service) CommitDNShard(ctx context.Context, request *txn.TxnRequest, response *txn.TxnResponse) error {
	return nil
}

func (s *service) RollbackDNShard(ctx context.Context, request *txn.TxnRequest, response *txn.TxnResponse) error {
	return nil
}
