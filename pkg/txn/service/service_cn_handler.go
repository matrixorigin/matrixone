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
	"math"
	"time"

	"github.com/matrixorigin/matrixone/pkg/common/moerr"
	"github.com/matrixorigin/matrixone/pkg/pb/timestamp"
	"github.com/matrixorigin/matrixone/pkg/pb/txn"
	"github.com/matrixorigin/matrixone/pkg/txn/util"
	"go.uber.org/zap"
)

var (
	rollbackIngoreErrorCodes = map[uint16]struct{}{
		moerr.ErrTxnNotFound: {},
	}

	prepareIngoreErrorCodes = map[uint16]struct{}{
		moerr.ErrTxnNotFound: {},
	}
)

func (s *service) Read(ctx context.Context, request *txn.TxnRequest, response *txn.TxnResponse) error {
	s.waitRecoveryCompleted()

	util.LogTxnHandleRequest(s.logger, request)
	defer util.LogTxnHandleResult(s.logger, response)

	response.CNOpResponse = &txn.CNOpResponse{}
	s.checkCNRequest(request)
	if !s.validDNShard(request.GetTargetDN()) {
		response.TxnError = txn.WrapError(moerr.NewDNShardNotFound("", request.GetTargetDN().ShardID), 0)
		return nil
	}

	s.waitClockTo(request.Txn.SnapshotTS)

	// We do not write transaction information to sync.Map during read operations because commit and abort
	// for read-only transactions are not sent to the DN node, so there is no way to clean up the transaction
	// information in sync.Map.
	result, err := s.storage.Read(ctx, request.Txn, request.CNRequest.OpCode, request.CNRequest.Payload)
	if err != nil {
		util.LogTxnReadFailed(s.logger, request.Txn, err)
		response.TxnError = txn.WrapError(err, moerr.ErrTAERead)
		return nil
	}
	defer result.Release()

	if len(result.WaitTxns()) > 0 {
		util.LogTxnReadBlockedByUncommittedTxns(s.logger, request.Txn, result.WaitTxns())
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
			util.LogTxnWaitUncommittedTxnsFailed(s.logger, request.Txn, result.WaitTxns(), err)
			response.TxnError = txn.WrapError(err, moerr.ErrWaitTxn)
			return nil
		}
	}

	data, err := result.Read()
	if err != nil {
		util.LogTxnReadFailed(s.logger, request.Txn, err)
		response.TxnError = txn.WrapError(err, moerr.ErrTAERead)
		return nil
	}

	response.CNOpResponse.Payload = data
	txnMeta := request.Txn
	response.Txn = &txnMeta
	return nil
}

func (s *service) Write(ctx context.Context, request *txn.TxnRequest, response *txn.TxnResponse) error {
	s.waitRecoveryCompleted()

	util.LogTxnHandleRequest(s.logger, request)
	defer util.LogTxnHandleResult(s.logger, response)

	response.CNOpResponse = &txn.CNOpResponse{}
	s.checkCNRequest(request)
	if !s.validDNShard(request.GetTargetDN()) {
		response.TxnError = txn.WrapError(moerr.NewDNShardNotFound("", request.GetTargetDN().ShardID), 0)
		return nil
	}

	txnID := request.Txn.ID
	txnCtx, _ := s.maybeAddTxn(request.Txn)

	// only commit and rollback can held write Lock
	if !txnCtx.mu.TryRLock() {
		util.LogTxnNotFoundOn(s.logger, request.Txn, s.shard)
		response.TxnError = txn.WrapError(moerr.NewDNShardNotFound("", request.GetTargetDN().ShardID), 0)
		return nil
	}
	defer txnCtx.mu.RUnlock()

	newTxn := txnCtx.getTxnLocked()
	if !bytes.Equal(newTxn.ID, txnID) {
		util.LogTxnNotFoundOn(s.logger, request.Txn, s.shard)
		response.TxnError = txn.WrapError(moerr.NewTxnNotFound(), 0)
		return nil
	}

	response.Txn = &newTxn
	if newTxn.Status != txn.TxnStatus_Active {
		util.LogTxnWriteOnInvalidStatus(s.logger, newTxn)
		response.TxnError = txn.WrapError(moerr.NewTxnNotActive(""), 0)
		return nil
	}

	data, err := s.storage.Write(ctx, request.Txn, request.CNRequest.OpCode, request.CNRequest.Payload)
	if err != nil {
		util.LogTxnWriteFailed(s.logger, newTxn, err)
		response.TxnError = txn.WrapError(err, moerr.ErrTAEWrite)
		return nil
	}

	response.CNOpResponse.Payload = data
	return nil
}

func (s *service) Commit(ctx context.Context, request *txn.TxnRequest, response *txn.TxnResponse) error {
	s.waitRecoveryCompleted()

	util.LogTxnHandleRequest(s.logger, request)
	defer util.LogTxnHandleResult(s.logger, response)

	response.CommitResponse = &txn.TxnCommitResponse{}
	if !s.validDNShard(request.GetTargetDN()) {
		response.TxnError = txn.WrapError(moerr.NewDNShardNotFound("", request.GetTargetDN().ShardID), 0)
		return nil
	}

	if len(request.Txn.DNShards) == 0 {
		s.logger.Fatal("commit with empty dn shards")
	}

	txnID := request.Txn.ID
	txnCtx := s.getTxnContext(txnID)
	if txnCtx == nil {
		util.LogTxnNotFoundOn(s.logger, request.Txn, s.shard)
		response.TxnError = txn.WrapError(moerr.NewDNShardNotFound("", request.GetTargetDN().ShardID), 0)
		return nil
	}

	// block all other concurrent read and write operations.
	txnCtx.mu.Lock()
	defer txnCtx.mu.Unlock()

	newTxn := txnCtx.getTxnLocked()
	if !bytes.Equal(newTxn.ID, txnID) {
		util.LogTxnNotFoundOn(s.logger, request.Txn, s.shard)
		response.TxnError = txn.WrapError(moerr.NewDNShardNotFound("", request.GetTargetDN().ShardID), 0)
		return nil
	}

	cleanTxnContext := true
	defer func() {
		// remove txnCtx, commit can only execute once.
		s.removeTxn(txnID)
		if cleanTxnContext {
			s.releaseTxnContext(txnCtx)
		}
	}()

	response.Txn = &newTxn
	if newTxn.Status != txn.TxnStatus_Active {
		util.LogTxnCommitOnInvalidStatus(s.logger, newTxn)
		response.TxnError = txn.WrapError(moerr.NewTxnNotActive(""), 0)
		return nil
	}

	newTxn.DNShards = request.Txn.DNShards
	changeStatus := func(status txn.TxnStatus) {
		newTxn.Status = status
		txnCtx.changeStatusLocked(status)
	}

	// fast path: write in only one DNShard.
	if len(newTxn.DNShards) == 1 {
		newTxn.CommitTS, _ = s.clock.Now()
		txnCtx.updateTxnLocked(newTxn)

		util.LogTxnStart1PCCommit(s.logger, newTxn)
		if err := s.storage.Commit(ctx, newTxn); err != nil {
			util.LogTxnStart1PCCommitFailed(s.logger, newTxn, err)
			response.TxnError = txn.WrapError(err, moerr.ErrTAECommit)
			changeStatus(txn.TxnStatus_Aborted)
		} else {
			changeStatus(txn.TxnStatus_Committed)
			util.LogTxn1PCCommitCompleted(s.logger, newTxn)
		}
		return nil
	}

	util.LogTxnStart2PCCommit(s.logger, newTxn)

	// slow path. 2pc transaction.
	// 1. send prepare request to all DNShards.
	// 2. start async commit task if all prepare succeed.
	// 3. response to client txn committed.
	for _, dn := range newTxn.DNShards {
		txnCtx.mu.requests = append(txnCtx.mu.requests, txn.TxnRequest{
			Txn:            newTxn,
			Method:         txn.TxnMethod_Prepare,
			PrepareRequest: &txn.TxnPrepareRequest{DNShard: dn},
		})
	}

	// unlock and lock here, because the prepare request will be sent to the current TxnService, it
	// will need to get the Lock when processing the Prepare.
	txnCtx.mu.Unlock()
	// FIXME: txnCtx.mu.requests without lock, is it safe?
	util.LogTxnSendRequests(s.logger, txnCtx.mu.requests)
	result, err := s.sender.Send(ctx, txnCtx.mu.requests)
	txnCtx.mu.Lock()
	if err != nil {
		util.LogTxnParallelPrepareFailed(s.logger, newTxn, err)

		changeStatus(txn.TxnStatus_Aborted)
		response.TxnError = txn.WrapError(moerr.NewRpcError(err.Error()), 0)
		s.startAsyncRollbackTask(newTxn)
		return nil
	}

	defer result.Release()

	// get latest txn metadata
	newTxn = txnCtx.getTxnLocked()
	newTxn.CommitTS = newTxn.PreparedTS

	hasError := false
	var txnErr *txn.TxnError
	for idx, resp := range result.Responses {
		if resp.TxnError != nil {
			txnErr = resp.TxnError
			hasError = true
			util.LogTxnPrepareFailedOn(s.logger, newTxn, newTxn.DNShards[idx], txnErr)
			continue
		}

		if resp.Txn.PreparedTS.IsEmpty() {
			s.logger.Fatal("missing prepared timestamp",
				zap.String("target-dn-shard", newTxn.DNShards[idx].DebugString()),
				util.TxnIDFieldWithID(newTxn.ID))
		}

		util.LogTxnPrepareCompletedOn(s.logger, newTxn, newTxn.DNShards[idx], resp.Txn.PreparedTS)
		if newTxn.CommitTS.Less(resp.Txn.PreparedTS) {
			newTxn.CommitTS = resp.Txn.PreparedTS
		}
	}
	if hasError {
		changeStatus(txn.TxnStatus_Aborted)
		response.TxnError = txnErr
		s.startAsyncRollbackTask(newTxn)
		return nil
	}

	util.LogTxnParallelPrepareCompleted(s.logger, newTxn)

	// All DNShards prepared means the transaction is committed
	cleanTxnContext = false
	txnCtx.updateTxnLocked(newTxn)
	return s.startAsyncCommitTask(txnCtx)
}

func (s *service) Rollback(ctx context.Context, request *txn.TxnRequest, response *txn.TxnResponse) error {
	s.waitRecoveryCompleted()

	util.LogTxnHandleRequest(s.logger, request)
	defer util.LogTxnHandleResult(s.logger, response)

	response.RollbackResponse = &txn.TxnRollbackResponse{}
	if !s.validDNShard(request.GetTargetDN()) {
		response.TxnError = txn.WrapError(moerr.NewDNShardNotFound("", request.GetTargetDN().ShardID), 0)
		return nil
	}

	if len(request.Txn.DNShards) == 0 {
		s.logger.Fatal("rollback with empty dn shards")
	}

	txnID := request.Txn.ID
	txnCtx := s.getTxnContext(txnID)
	if txnCtx == nil {
		util.LogTxnNotFoundOn(s.logger, request.Txn, s.shard)
		response.TxnError = txn.WrapError(moerr.NewTxnNotFound(), 0)
		return nil
	}

	txnCtx.mu.Lock()
	defer txnCtx.mu.Unlock()

	newTxn := txnCtx.getTxnLocked()
	if !bytes.Equal(newTxn.ID, txnID) {
		util.LogTxnNotFoundOn(s.logger, request.Txn, s.shard)
		response.TxnError = txn.WrapError(moerr.NewTxnNotFound(), 0)
		return nil
	}

	response.Txn = &newTxn
	newTxn.DNShards = request.Txn.DNShards
	s.startAsyncRollbackTask(newTxn)

	response.Txn.Status = txn.TxnStatus_Aborted
	return nil
}

func (s *service) startAsyncRollbackTask(txnMeta txn.TxnMeta) {
	err := s.stopper.RunTask(func(ctx context.Context) {
		util.LogTxnStartAsyncRollback(s.logger, txnMeta)

		requests := make([]txn.TxnRequest, 0, len(txnMeta.DNShards))
		for _, dn := range txnMeta.DNShards {
			requests = append(requests, txn.TxnRequest{
				Txn:                    txnMeta,
				Method:                 txn.TxnMethod_RollbackDNShard,
				RollbackDNShardRequest: &txn.TxnRollbackDNShardRequest{DNShard: dn},
			})
		}

		s.parallelSendWithRetry(ctx, txnMeta, requests, rollbackIngoreErrorCodes)
		util.LogTxnRollbackCompleted(s.logger, txnMeta)
	})
	if err != nil {
		s.logger.Error("start rollback task failed",
			zap.Error(err),
			util.TxnIDFieldWithID(txnMeta.ID))
	}
}

func (s *service) Debug(ctx context.Context, request *txn.TxnRequest, response *txn.TxnResponse) error {
	data, err := s.storage.Debug(ctx, request.Txn, request.CNRequest.OpCode, request.CNRequest.Payload)
	if err != nil {
		response.TxnError = txn.WrapError(err, moerr.ErrTAEDebug)
		return nil
	}
	response.CNOpResponse = &txn.CNOpResponse{
		Payload: data,
	}
	return nil
}

func (s *service) startAsyncCommitTask(txnCtx *txnContext) error {
	return s.stopper.RunTask(func(ctx context.Context) {
		txnCtx.mu.Lock()
		defer txnCtx.mu.Unlock()

		txnMeta := txnCtx.getTxnLocked()
		util.LogTxnStartAsyncCommit(s.logger, txnMeta)

		if txnMeta.Status != txn.TxnStatus_Committing {
			for {
				err := s.storage.Committing(ctx, txnMeta)
				if err == nil {
					txnCtx.changeStatusLocked(txn.TxnStatus_Committing)
					break
				}
				util.LogTxnCommittingFailed(s.logger, txnMeta, err)
				// TODO: make config
				time.Sleep(time.Second)
			}
		}

		util.LogTxnCommittingCompleted(s.logger, txnMeta)

		requests := make([]txn.TxnRequest, 0, len(txnMeta.DNShards)-1)
		for _, dn := range txnMeta.DNShards[1:] {
			requests = append(requests, txn.TxnRequest{
				Txn:                  txnMeta,
				Method:               txn.TxnMethod_CommitDNShard,
				CommitDNShardRequest: &txn.TxnCommitDNShardRequest{DNShard: dn},
			})
		}

		// no timeout, keep retry until TxnService.Close
		ctx, cancel := context.WithTimeout(ctx, time.Duration(math.MaxInt64))
		defer cancel()

		if result := s.parallelSendWithRetry(ctx, txnMeta, requests, rollbackIngoreErrorCodes); result != nil {
			result.Release()
			if ce := s.logger.Check(zap.DebugLevel, "other dnshards committed"); ce != nil {
				ce.Write(util.TxnIDFieldWithID(txnMeta.ID))
			}

			if err := s.storage.Commit(ctx, txnMeta); err != nil {
				s.logger.Fatal("commit failed after prepared",
					util.TxnIDFieldWithID(txnMeta.ID),
					zap.Error(err))
			}

			if ce := s.logger.Check(zap.DebugLevel, "coordinator dnshard committed, txn committed"); ce != nil {
				ce.Write(util.TxnIDFieldWithID(txnMeta.ID))
			}

			txnCtx.changeStatusLocked(txn.TxnStatus_Committed)
			s.releaseTxnContext(txnCtx)
		}
	})
}

func (s *service) checkCNRequest(request *txn.TxnRequest) {
	if request.CNRequest == nil {
		s.logger.Fatal("missing CNRequest")
	}
}

func (s *service) waitClockTo(ts timestamp.Timestamp) {
	for {
		now, _ := s.clock.Now()
		if now.GreaterEq(ts) {
			return
		}
		time.Sleep(time.Duration(ts.PhysicalTime + 1 - now.PhysicalTime))
	}
}
