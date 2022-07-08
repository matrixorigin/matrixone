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
	"github.com/matrixorigin/matrixone/pkg/txn/util"
	"go.uber.org/zap"
)

func (s *service) Read(ctx context.Context, request *txn.TxnRequest, response *txn.TxnResponse) error {
	response.CNOpResponse = &txn.CNOpResponse{}
	s.checkCNRequest(request)
	s.validDNShard(request)

	// We do not write transaction information to sync.Map during read operations because commit and abort
	// for read-only transactions are not sent to the DN node, so there is no way to clean up the transaction
	// information in sync.Map.

	result, err := s.storage.Read(request.Txn, int(request.CNRequest.OpCode), request.CNRequest.Payload)
	if err != nil {
		s.logger.Error("execute read failed",
			util.TxnIDFieldWithID(request.Txn.ID),
			zap.Error(err))
		response.TxnError = newTAEReadError(err)
		return nil
	}
	defer result.Release()

	if len(result.WaitTxns()) > 0 {
		waiters := make([]*waiter, 0, len(result.WaitTxns()))
		for _, txnID := range result.WaitTxns() {
			txnCtx := s.getTxnContext(txnID)
			// The transaction can not found, it means the concurrent transaction to be waited for has already
			// been committed or aborted.
			if txnCtx == nil {
				continue
			}

			w := acquireWaiter()
			// txn has been committed or aborted between call s.getTxnContext and txnCtx.addWaiter
			if !txnCtx.addWaiter(txnID, w, txn.TxnStatus_Committed) {
				w.close()
				continue
			}

			waiters = append(waiters, w)
		}

		for _, w := range waiters {
			if err != nil {
				w.close()
				continue
			}

			// If no error occurs, then it must have waited until the final state of the transaction, not caring
			// whether the final state is committed or aborted.
			_, err = w.wait(ctx)
			w.close()
		}

		if err != nil {
			s.logger.Error("wait txns failed",
				util.TxnIDFieldWithID(request.Txn.ID),
				zap.Error(err))
			response.TxnError = newWaitTxnError(err)
			return nil
		}
	}

	data, err := result.Read()
	if err != nil {
		s.logger.Error("read failed",
			zap.Error(err))
		response.TxnError = newTAEReadError(err)
		return nil
	}

	response.CNOpResponse.Payload = data
	return nil
}

func (s *service) Write(ctx context.Context, request *txn.TxnRequest, response *txn.TxnResponse) error {
	response.CNOpResponse = &txn.CNOpResponse{}
	s.checkCNRequest(request)
	s.validDNShard(request)

	txnID := request.Txn.ID
	txnCtx, _ := s.maybeAddTxn(request.Txn)

	// only commit and rollback can held write Lock
	if !txnCtx.mu.TryRLock() {
		response.TxnError = newTxnNotFoundError()
		return nil
	}
	defer txnCtx.mu.RUnlock()

	newTxn := txnCtx.getTxnLocked()
	if !bytes.Equal(newTxn.ID, txnID) {
		response.TxnError = newTxnNotFoundError()
		return nil
	}

	response.Txn = &newTxn
	if newTxn.Status != txn.TxnStatus_Active {
		response.TxnError = newTxnNotActiveError()
		return nil
	}

	data, err := s.storage.Write(request.Txn, int(request.CNRequest.OpCode), request.CNRequest.Payload)
	if err != nil {
		response.TxnError = newTAEWriteError(err)
		return nil
	}

	response.CNOpResponse.Payload = data
	return nil
}

func (s *service) Commit(ctx context.Context, request *txn.TxnRequest, response *txn.TxnResponse) error {
	response.CommitResponse = &txn.TxnCommitResponse{}
	s.validDNShard(request)
	if len(request.Txn.DNShards) == 0 {
		s.logger.Fatal("commit with empty dn shards")
	}

	txnID := request.Txn.ID
	txnCtx := s.getTxnContext(txnID)
	if txnCtx == nil {
		response.TxnError = newTxnNotFoundError()
		return nil
	}
	defer func() {
		s.removeTxn(txnID)
		s.releaseTxnContext(txnCtx)
	}()

	// block all other concurrent read and write operations.
	txnCtx.mu.Lock()
	defer txnCtx.mu.Unlock()

	newTxn := txnCtx.getTxnLocked()
	if !bytes.Equal(newTxn.ID, txnID) {
		response.TxnError = newTxnNotFoundError()
		return nil
	}

	response.Txn = &newTxn
	if newTxn.Status != txn.TxnStatus_Active {
		response.TxnError = newTxnNotActiveError()
		return nil
	}

	newTxn.DNShards = request.Txn.DNShards
	changeStatus := func(status txn.TxnStatus) {
		newTxn.Status = status
		txnCtx.changeStatusLocked(status)
	}

	// fast path: write in only one DNShard.
	if len(newTxn.DNShards) == 1 {
		changeStatus(txn.TxnStatus_Committing)

		commitTS, _ := s.clocker.Now()
		txnCtx.updateCommitTimestampLocked(commitTS)

		if err := s.storage.Commit(txnCtx.mu.txn); err != nil {
			s.logger.Error("commit failed",
				util.TxnIDFieldWithID(txnID),
				zap.Error(err))
			response.TxnError = newTAECommitError(err)
			changeStatus(txn.TxnStatus_Aborted)
		} else {
			changeStatus(txn.TxnStatus_Committed)
		}

		txnCtx.resetLocked()
		return nil
	}

	// slow path. 2pc transaction.
	// 1. send prepare request to all DNShards.
	// 2. start async commit task if all prepare succeed.
	// 3. response to client txn committed.

	// TODO: At present, due to TAE, the prepare phase cannot be fully parallelized. Later, we need to change
	// it to fully parallelized and add an RPC request to delete the memory metadata left by TAE after Commit,
	// i.e. TAE does not delete the metadata immediately after Commit.
	if err := s.storage.Prepare(newTxn); err != nil {
		changeStatus(txn.TxnStatus_Aborted)
		response.TxnError = newTAEPrepareError(err)
		return nil
	}
	for _, dn := range newTxn.DNShards[1:] {
		txnCtx.mu.requests = append(txnCtx.mu.requests, txn.TxnRequest{
			Txn:            newTxn,
			Method:         txn.TxnMethod_Prepare,
			TimeoutAt:      s.mustGetTimeoutAtFromContext(ctx),
			PrepareRequest: &txn.TxnPrepareRequest{DNShard: dn},
		})
	}
	responses, err := s.sender.Send(ctx, txnCtx.mu.requests)
	if err != nil {
		s.startAsyncRollbackTask(newTxn)
		changeStatus(txn.TxnStatus_Aborted)
		response.TxnError = newRPCError(err)
		return nil
	}
	hasError := false
	for idx, resp := range responses {
		if resp.TxnError != nil {
			hasError = true
			s.logger.Error("prepare dn failed",
				util.TxnIDFieldWithID(txnID),
				zap.String("target-dn-shard", newTxn.DNShards[1+idx].DebugString()),
				zap.String("error", resp.TxnError.DebugString()))
		}
	}
	if hasError {
		s.startAsyncRollbackTask(newTxn)
		changeStatus(txn.TxnStatus_Aborted)
		response.TxnError = newRPCError(err)
		return nil
	}

	// all dnshards prepared
	if err := s.startAsyncCommitTask(newTxn); err != nil {
		s.startAsyncRollbackTask(newTxn)
		changeStatus(txn.TxnStatus_Aborted)
		response.TxnError = newTAECommitError(err)
		return nil
	}

	changeStatus(txn.TxnStatus_Committed)
	return nil
}

func (s *service) Rollback(ctx context.Context, request *txn.TxnRequest, response *txn.TxnResponse) error {
	response.RollbackResponse = &txn.TxnRollbackResponse{}
	s.validDNShard(request)
	if len(request.Txn.DNShards) == 0 {
		s.logger.Fatal("rollback with empty dn shards")
	}

	txnID := request.Txn.ID
	txnCtx := s.getTxnContext(txnID)
	if txnCtx == nil {
		response.TxnError = newTxnNotFoundError()
		return nil
	}
	defer func() {
		s.removeTxn(txnID)
		s.releaseTxnContext(txnCtx)
	}()

	// block all other concurrent read and write operations.
	txnCtx.mu.Lock()
	defer txnCtx.mu.Unlock()

	newTxn := txnCtx.getTxnLocked()
	if !bytes.Equal(newTxn.ID, txnID) {
		response.TxnError = newTxnNotFoundError()
		return nil
	}

	response.Txn = &newTxn
	newTxn.DNShards = request.Txn.DNShards

	s.startAsyncRollbackTask(newTxn)
	newTxn.Status = txn.TxnStatus_Aborted
	txnCtx.changeStatusLocked(txn.TxnStatus_Aborted)
	return nil
}

func (s *service) checkCNRequest(request *txn.TxnRequest) {
	if request.CNRequest == nil {
		s.logger.Fatal("missing CNRequest")
	}
}

func (s *service) startAsyncRollbackTask(txnMeta txn.TxnMeta) {
	if err := s.storage.Rollback(txnMeta); err != nil {
		s.logger.Error("rollback coordinator dn failed",
			util.TxnIDFieldWithID(txnMeta.ID),
			zap.Error(err))
	}
	if len(txnMeta.DNShards) == 1 {
		return
	}

	// rollback other dnshards
	s.stopper.RunTask(func(ctx context.Context) {
		requests := make([]txn.TxnRequest, 0, len(txnMeta.DNShards)-1)
		for _, dn := range txnMeta.DNShards[1:] {
			requests = append(requests, txn.TxnRequest{
				Txn:            txnMeta,
				Method:         txn.TxnMethod_Prepare,
				TimeoutAt:      s.mustGetTimeoutAtFromContext(ctx),
				PrepareRequest: &txn.TxnPrepareRequest{DNShard: dn},
			})
		}

		for {
			select {
			case <-ctx.Done():
				return
			default:
				responses, err := s.sender.Send(ctx, requests)
				if err != nil {
					s.logger.Error("async rollback DNShard failed",
						util.TxnIDFieldWithID(txnMeta.ID),
						zap.Error(err))
					break
				}
				hasError := false
				for idx, resp := range responses {
					if resp.TxnError != nil {
						hasError = true
						s.logger.Error("async rollback DNShard failed",
							util.TxnIDFieldWithID(txnMeta.ID),
							zap.String("target-dn-shard", txnMeta.DNShards[idx+1].DebugString()),
							zap.Error(err))
					}
				}
				if !hasError {
					return
				}
			}
		}
	})
}

func (s *service) startAsyncCommitTask(txn txn.TxnMeta) error {
	if err := s.storage.Commit(txn); err != nil {
		s.logger.Error("commit coordinator dn failed",
			util.TxnIDFieldWithID(txn.ID),
			zap.Error(err))
		return err
	}
	return nil
}
