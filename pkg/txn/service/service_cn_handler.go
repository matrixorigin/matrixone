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

	"go.uber.org/zap"

	"github.com/matrixorigin/matrixone/pkg/common/moerr"
	"github.com/matrixorigin/matrixone/pkg/common/runtime"
	"github.com/matrixorigin/matrixone/pkg/lockservice"
	"github.com/matrixorigin/matrixone/pkg/pb/timestamp"
	"github.com/matrixorigin/matrixone/pkg/pb/txn"
	"github.com/matrixorigin/matrixone/pkg/txn/util"
	v2 "github.com/matrixorigin/matrixone/pkg/util/metric/v2"
)

var (
	errMultiTNTransaction = "transactions spanning more than one TN shard are unsupported"
)

func (s *service) Read(ctx context.Context, request *txn.TxnRequest, response *txn.TxnResponse) error {
	util.LogTxnHandleRequest(s.logger, request)
	defer util.LogTxnHandleResult(s.logger, response)

	response.CNOpResponse = &txn.CNOpResponse{}
	s.checkCNRequest(request)
	if !s.validTNShard(request.GetTargetTN()) {
		response.TxnError = txn.WrapError(moerr.NewTNShardNotFound(ctx, "", request.GetTargetTN().ShardID), 0)
		return nil
	}

	if err := s.waitClockTo(ctx, request.Txn.SnapshotTS); err != nil {
		return err
	}

	// We do not write transaction information to sync.Map during read operations because commit and abort
	// for read-only transactions are not sent to the TN node, so there is no way to clean up the transaction
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
			// The transaction can not be found, it means the concurrent transaction to be waited for has already
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
	util.LogTxnHandleRequest(s.logger, request)
	defer util.LogTxnHandleResult(s.logger, response)

	response.CNOpResponse = &txn.CNOpResponse{}
	s.checkCNRequest(request)
	if !s.validTNShard(request.GetTargetTN()) {
		response.TxnError = txn.WrapError(moerr.NewTNShardNotFound(ctx, "", request.GetTargetTN().ShardID), 0)
		return nil
	}

	txnID := request.Txn.ID
	txnCtx, _ := s.maybeAddTxn(request.Txn)

	// only commit and rollback can held write Lock
	if !txnCtx.mu.TryRLock() {
		util.LogTxnNotFoundOn(s.logger, request.Txn, s.shard)
		response.TxnError = txn.WrapError(moerr.NewTNShardNotFound(ctx, "", request.GetTargetTN().ShardID), 0)
		return nil
	}
	defer txnCtx.mu.RUnlock()

	newTxn := txnCtx.getTxnLocked()
	if !bytes.Equal(newTxn.ID, txnID) {
		util.LogTxnNotFoundOn(s.logger, request.Txn, s.shard)
		response.TxnError = txn.WrapError(moerr.NewTxnNotFound(ctx), 0)
		return nil
	}

	response.Txn = &newTxn
	if newTxn.Status != txn.TxnStatus_Active {
		util.LogTxnWriteOnInvalidStatus(s.logger, newTxn)
		response.TxnError = txn.WrapError(moerr.NewTxnNotActive(ctx, ""), 0)
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
	v2.TxnTNReceiveCommitCounter.Inc()
	start := time.Now()
	defer func() {
		v2.TxnTNCommitDurationHistogram.Observe(time.Since(start).Seconds())
	}()

	st := time.Now()
	defer func() {
		cost := time.Since(st)
		if cost > time.Second {
			s.logger.Warn("commit txn too slow",
				zap.Duration("cost", cost))
		}
	}()

	util.LogTxnHandleRequest(s.logger, request)
	defer util.LogTxnHandleResult(s.logger, response)

	response.CommitResponse = &txn.TxnCommitResponse{}
	if !s.validTNShard(request.GetTargetTN()) {
		response.TxnError = txn.WrapError(moerr.NewTNShardNotFound(ctx, "", request.GetTargetTN().ShardID), 0)
		return nil
	}

	if len(request.Txn.TNShards) == 0 {
		s.logger.Fatal("commit with empty tn shards")
	}
	if len(request.Txn.TNShards) > 1 {
		s.cleanupUnsupportedTxn(ctx, request.Txn)
		response.TxnError = txn.WrapError(
			moerr.NewNotSupported(ctx, errMultiTNTransaction),
			0,
		)
		return nil
	}
	var commitMeta lockservice.CommitRequestMeta
	if request.CommitRequest != nil {
		commitMeta.DeadlineUnixNano = request.CommitRequest.DeadlineUnixNano
		commitMeta.Sequence = request.CommitRequest.CommitSequence
	}
	// MORPC transports the timeout as a duration. A request delayed in a
	// server queue would otherwise receive a fresh relative timeout when it is
	// decoded. The absolute deadline is checked before lockservice admission so
	// an expired Commit cannot be admitted after its unknown-result fence has
	// been collected. The sender and TN have different wall clocks, so account
	// for the configured HLC offset before declaring a request expired.
	if commitRequestExpired(request, time.Now(), s.commitDeadlineClockOffset()) {
		response.TxnError = txn.WrapError(
			moerr.NewTxnNotActive(ctx, "commit request expired"),
			0,
		)
		return nil
	}

	if len(request.Txn.LockTables) > 0 {
		invalidBinds, err := s.allocator.Valid(
			request.Txn.LockService,
			request.Txn.ID,
			request.Txn.LockTables,
			commitMeta,
		)
		if err != nil {
			response.TxnError = txn.WrapError(err, 0)
			return nil
		}
		if len(invalidBinds) > 0 {
			response.CommitResponse.InvalidLockTables = invalidBinds
			response.TxnError = txn.WrapError(moerr.NewLockTableBindChanged(ctx), 0)
			return nil
		}
		defer s.allocator.FinishCommit(request.Txn.LockService, request.Txn.ID)
	}

	txnID := request.Txn.ID
	txnCtx, _ := s.maybeAddTxn(request.Txn)

	if txnCtx == nil {
		util.LogTxnNotFoundOn(s.logger, request.Txn, s.shard)
		response.TxnError = txn.WrapError(moerr.NewTNShardNotFound(ctx, "", request.GetTargetTN().ShardID), 0)
		return nil
	}

	// block all other concurrent read and write operations.
	txnCtx.mu.Lock()
	defer txnCtx.mu.Unlock()

	newTxn := txnCtx.getTxnLocked()
	if !bytes.Equal(newTxn.ID, txnID) {
		util.LogTxnNotFoundOn(s.logger, request.Txn, s.shard)
		response.TxnError = txn.WrapError(moerr.NewTNShardNotFound(ctx, "", request.GetTargetTN().ShardID), 0)
		return nil
	}

	defer func() {
		// remove txnCtx, commit can only execute once.
		s.removeTxn(txnID)
		s.releaseTxnContext(txnCtx)
	}()

	response.Txn = &newTxn
	if newTxn.Status != txn.TxnStatus_Active {
		util.LogTxnCommitOnInvalidStatus(s.logger, newTxn)
		response.TxnError = txn.WrapError(moerr.NewTxnNotActive(ctx, ""), 0)
		return nil
	}

	newTxn.TNShards = request.Txn.TNShards
	changeStatus := func(status txn.TxnStatus) {
		newTxn.Status = status
		txnCtx.changeStatusLocked(status)
	}

	util.LogTxnStart1PCCommit(s.logger, newTxn)
	commitTS, err := s.storage.Commit(ctx, newTxn, response, request.CommitRequest)
	v2.TxnTNCommitHandledCounter.Inc()
	if err != nil {
		util.LogTxnStart1PCCommitFailed(s.logger, newTxn, err)
		response.TxnError = txn.WrapError(err, moerr.ErrTAECommit)
		changeStatus(txn.TxnStatus_Aborted)
	} else {
		newTxn.CommitTS = commitTS
		txnCtx.updateTxnLocked(newTxn)
		changeStatus(txn.TxnStatus_Committed)
		util.LogTxn1PCCommitCompleted(s.logger, newTxn)
	}
	return nil
}

func (s *service) commitDeadlineClockOffset() time.Duration {
	rt := runtime.ServiceRuntime(s.sid)
	if rt == nil || rt.Clock() == nil {
		return 0
	}
	return rt.Clock().MaxOffset()
}

func commitRequestExpired(
	request *txn.TxnRequest,
	now time.Time,
	clockOffset time.Duration,
) bool {
	return request.CommitRequest != nil &&
		request.CommitRequest.DeadlineUnixNano > 0 &&
		!now.Before(time.Unix(0, request.CommitRequest.DeadlineUnixNano).Add(clockOffset))
}

func (s *service) Rollback(ctx context.Context, request *txn.TxnRequest, response *txn.TxnResponse) error {
	util.LogTxnHandleRequest(s.logger, request)
	defer util.LogTxnHandleResult(s.logger, response)

	response.RollbackResponse = &txn.TxnRollbackResponse{}
	if !s.validTNShard(request.GetTargetTN()) {
		response.TxnError = txn.WrapError(moerr.NewTNShardNotFound(ctx, "", request.GetTargetTN().ShardID), 0)
		return nil
	}

	if len(request.Txn.TNShards) == 0 {
		s.logger.Fatal("rollback with empty tn shards")
	}
	if len(request.Txn.TNShards) > 1 {
		s.cleanupUnsupportedTxn(ctx, request.Txn)
		response.TxnError = txn.WrapError(
			moerr.NewNotSupported(ctx, errMultiTNTransaction),
			0,
		)
		return nil
	}

	txnID := request.Txn.ID
	txnCtx := s.getTxnContext(txnID)
	if txnCtx == nil {
		util.LogTxnNotFoundOn(s.logger, request.Txn, s.shard)
		response.TxnError = txn.WrapError(moerr.NewTxnNotFound(ctx), 0)
		return nil
	}

	txnCtx.mu.Lock()
	defer txnCtx.mu.Unlock()
	defer func() {
		s.removeTxn(txnID)
		s.releaseTxnContext(txnCtx)
	}()

	newTxn := txnCtx.getTxnLocked()
	if !bytes.Equal(newTxn.ID, txnID) {
		util.LogTxnNotFoundOn(s.logger, request.Txn, s.shard)
		response.TxnError = txn.WrapError(moerr.NewTxnNotFound(ctx), 0)
		return nil
	}

	response.Txn = &newTxn
	newTxn.TNShards = request.Txn.TNShards
	if err := s.storage.Rollback(ctx, newTxn); err != nil {
		response.TxnError = txn.WrapError(err, moerr.ErrTAERollback)
	}
	newTxn.Status = txn.TxnStatus_Aborted
	txnCtx.changeStatusLocked(txn.TxnStatus_Aborted)
	response.Txn = &newTxn
	return nil
}

func (s *service) cleanupUnsupportedTxn(ctx context.Context, txnMeta txn.TxnMeta) {
	txnCtx := s.getTxnContext(txnMeta.ID)
	if txnCtx == nil {
		return
	}

	txnCtx.mu.Lock()
	defer txnCtx.mu.Unlock()
	current := txnCtx.getTxnLocked()
	if !bytes.Equal(current.ID, txnMeta.ID) {
		return
	}
	if err := s.storage.Rollback(ctx, current); err != nil {
		s.logger.Error("rollback unsupported multi-TN transaction failed",
			zap.Error(err),
			util.TxnIDFieldWithID(txnMeta.ID))
	}
	s.removeTxn(txnMeta.ID)
	s.releaseTxnContext(txnCtx)
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

func (s *service) checkCNRequest(request *txn.TxnRequest) {
	if request.CNRequest == nil {
		s.logger.Fatal("missing CNRequest")
	}
}

func (s *service) waitClockTo(ctx context.Context, ts timestamp.Timestamp) error {
	for {
		now, _ := runtime.ServiceRuntime(s.sid).Clock().Now()
		if now.GreaterEq(ts) {
			return nil
		}
		wait := time.Duration(ts.PhysicalTime - now.PhysicalTime)
		if wait < time.Duration(math.MaxInt64) {
			wait++
		}
		timer := time.NewTimer(wait)
		select {
		case <-ctx.Done():
			timer.Stop()
			return context.Cause(ctx)
		case <-timer.C:
		}
	}
}
