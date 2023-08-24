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

	"github.com/matrixorigin/matrixone/pkg/common/moerr"
	"github.com/matrixorigin/matrixone/pkg/pb/txn"
	"github.com/matrixorigin/matrixone/pkg/txn/util"
	"go.uber.org/zap"
)

func (s *service) Prepare(ctx context.Context, request *txn.TxnRequest, response *txn.TxnResponse) error {
	s.waitRecoveryCompleted()

	util.LogTxnHandleRequest(request)
	defer util.LogTxnHandleResult(response)

	response.PrepareResponse = &txn.TxnPrepareResponse{}

	txnID := request.Txn.ID
	txnCtx := s.getTxnContext(txnID)
	if txnCtx == nil {
		response.TxnError = txn.WrapError(moerr.NewTxnNotFoundNoCtx(), 0)
		return nil
	}

	txnCtx.mu.Lock()
	defer txnCtx.mu.Unlock()

	newTxn := txnCtx.getTxnLocked()
	if !bytes.Equal(newTxn.ID, txnID) {
		response.TxnError = txn.WrapError(moerr.NewTxnNotFoundNoCtx(), 0)
		return nil
	}
	response.Txn = &newTxn

	switch newTxn.Status {
	case txn.TxnStatus_Active:
		break
	case txn.TxnStatus_Prepared:
		return nil
	default:
		response.TxnError = txn.WrapError(moerr.NewTxnNotActiveNoCtx(""), 0)
		return nil
	}

	newTxn.DNShards = request.Txn.DNShards
	ts, err := s.storage.Prepare(ctx, newTxn)
	if err != nil {
		response.TxnError = txn.WrapError(err, moerr.ErrTAEPrepare)
		if err := s.storage.Rollback(ctx, newTxn); err != nil {
			s.logger.Error("rollback failed",
				util.TxnIDFieldWithID(newTxn.ID),
				zap.Error(err))
		}
		return nil
	}
	newTxn.PreparedTS = ts
	txnCtx.updateTxnLocked(newTxn)

	newTxn.Status = txn.TxnStatus_Prepared
	txnCtx.changeStatusLocked(txn.TxnStatus_Prepared)
	return nil
}

func (s *service) GetStatus(ctx context.Context, request *txn.TxnRequest, response *txn.TxnResponse) error {
	s.waitRecoveryCompleted()

	util.LogTxnHandleRequest(request)
	defer util.LogTxnHandleResult(response)

	response.GetStatusResponse = &txn.TxnGetStatusResponse{}

	txnID := request.Txn.ID
	txnCtx := s.getTxnContext(txnID)
	if txnCtx == nil {
		return nil
	}

	txnCtx.mu.RLock()
	defer txnCtx.mu.RUnlock()

	newTxn := txnCtx.getTxnLocked()
	if !bytes.Equal(newTxn.ID, txnID) {
		return nil
	}
	response.Txn = &newTxn
	return nil
}

func (s *service) CommitDNShard(ctx context.Context, request *txn.TxnRequest, response *txn.TxnResponse) error {
	s.waitRecoveryCompleted()

	util.LogTxnHandleRequest(request)
	defer util.LogTxnHandleResult(response)

	response.CommitDNShardResponse = &txn.TxnCommitDNShardResponse{}

	txnID := request.Txn.ID
	txnCtx := s.getTxnContext(txnID)
	if txnCtx == nil {
		// txn must be committed, donot need to return newTxnNotFoundError
		return nil
	}

	txnCtx.mu.Lock()
	defer txnCtx.mu.Unlock()

	newTxn := txnCtx.getTxnLocked()
	if !bytes.Equal(newTxn.ID, txnID) {
		// txn must be committed, donot need to return newTxnNotFoundError
		return nil
	}

	defer func() {
		if response.Txn.Status == txn.TxnStatus_Committed {
			s.removeTxn(txnID)
			s.releaseTxnContext(txnCtx)
		}
	}()

	response.Txn = &newTxn
	switch newTxn.Status {
	case txn.TxnStatus_Prepared:
		break
	case txn.TxnStatus_Committed:
		return nil
	default:
		s.logger.Fatal("commit on invalid status",
			zap.String("status", newTxn.Status.String()),
			util.TxnIDFieldWithID(newTxn.ID))
	}

	newTxn.CommitTS = request.Txn.CommitTS
	if _, err := s.storage.Commit(ctx, newTxn); err != nil {
		response.TxnError = txn.WrapError(err, moerr.ErrTAECommit)
		return nil
	}
	txnCtx.updateTxnLocked(newTxn)

	newTxn.Status = txn.TxnStatus_Committed
	txnCtx.changeStatusLocked(txn.TxnStatus_Committed)
	return nil
}

func (s *service) RollbackDNShard(ctx context.Context, request *txn.TxnRequest, response *txn.TxnResponse) error {
	s.waitRecoveryCompleted()

	util.LogTxnHandleRequest(request)
	defer util.LogTxnHandleResult(response)

	response.RollbackDNShardResponse = &txn.TxnRollbackDNShardResponse{}

	txnID := request.Txn.ID
	txnCtx := s.getTxnContext(txnID)
	if txnCtx == nil {
		// txn must be aborted, no need to return newTxnNotFoundError
		return nil
	}

	txnCtx.mu.Lock()
	defer txnCtx.mu.Unlock()

	newTxn := txnCtx.getTxnLocked()
	if !bytes.Equal(newTxn.ID, txnID) {
		// txn must be aborted, donot need to return newTxnNotFoundError
		return nil
	}

	defer func() {
		s.removeTxn(txnID)
		s.releaseTxnContext(txnCtx)
	}()

	response.Txn = &newTxn
	switch newTxn.Status {
	case txn.TxnStatus_Prepared:
		break
	case txn.TxnStatus_Active:
		break
	case txn.TxnStatus_Aborted:
		return nil
	default:
		s.logger.Fatal("rollback on invalid status",
			zap.String("status", newTxn.Status.String()),
			util.TxnIDFieldWithID(newTxn.ID))
	}

	if err := s.storage.Rollback(ctx, newTxn); err != nil {
		response.TxnError = txn.WrapError(err, moerr.ErrTAERollback)
	}

	newTxn.Status = txn.TxnStatus_Aborted
	txnCtx.changeStatusLocked(txn.TxnStatus_Aborted)
	return nil
}
