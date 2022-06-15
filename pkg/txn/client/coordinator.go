// Copyright 2022 Matrix Origin
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
	"fmt"
	"sync"

	"github.com/matrixorigin/matrixone/pkg/logutil"
	"github.com/matrixorigin/matrixone/pkg/pb/metadata"
	"github.com/matrixorigin/matrixone/pkg/pb/txn"
	"github.com/matrixorigin/matrixone/pkg/txn/util"
	"go.uber.org/zap"
)

// WithReadyOnlyTxn setup readyonly flag
func WithReadyOnlyTxn() TxnOption {
	return func(tc *txnCoordinator) {
		tc.option.readyOnly = true
	}
}

// WithDisable1PCOpt disable 1pc optimisation on distributed transaction. By default, mo enables 1pc
// optimization for distributed transactions. For write operations, if all partitions' prepares are
// executed successfully, then the transaction is considered committed and returned directly to the
// client. Partitions' prepared data are committed asynchronously.
func WithDisable1PCOpt() TxnOption {
	return func(tc *txnCoordinator) {
		tc.option.disable1PCOpt = true
	}
}

// WithTxnLogger setup txn logger
func WithTxnLogger(logger *zap.Logger) TxnOption {
	return func(tc *txnCoordinator) {
		tc.logger = logger
	}
}

// WithCacheWriteTxn Set cache write requests, after each Write call, the request will not be sent
// to the DN node immediately, but stored in the Coordinator's memory, and the Coordinator will
// choose the right time to send the cached requests. The following scenarios trigger the sending
// of requests to DN:
// 1. Before read, because the Coordinator is not aware of the format and content of the written data,
//    it is necessary to send the cached write requests to the corresponding DN node each time Read is
//    called, used to implement "read your write".
// 2. Before commit, obviously, the cached write requests needs to be sent to the corresponding DN node
//    before commit.
func WithCacheWriteTxn() TxnOption {
	return func(tc *txnCoordinator) {
		tc.option.enableCacheWrite = true
		tc.mu.cachedWrites = make(map[uint64][]txn.TxnRequest)
	}
}

type txnCoordinator struct {
	logger *zap.Logger
	sender TxnSender

	option struct {
		readyOnly        bool
		enableCacheWrite bool
		disable1PCOpt    bool
	}

	mu struct {
		sync.RWMutex
		closed       bool
		txn          txn.TxnMeta
		cachedWrites map[uint64][]txn.TxnRequest
		partitions   []metadata.DNShard
	}
}

func newTxnCoordinator(sender TxnSender, txnMeta txn.TxnMeta, options ...TxnOption) *txnCoordinator {
	tc := &txnCoordinator{sender: sender}
	tc.mu.txn = txnMeta

	for _, opt := range options {
		opt(tc)
	}
	tc.adjust()

	tc.logger.Debug("txn created",
		zap.String("txn", txnMeta.DebugString()),
		zap.Bool("read-only", tc.option.readyOnly),
		zap.Bool("enable-cache-write", tc.option.enableCacheWrite),
		zap.Bool("disable-1pc", tc.option.disable1PCOpt))
	return tc
}

func (tc *txnCoordinator) adjust() {
	tc.logger = logutil.Adjust(tc.logger)
	if tc.sender == nil {
		tc.logger.Fatal("missing txn sender")
	}
	if len(tc.mu.txn.ID) == 0 {
		tc.logger.Fatal("missing txn id")
	}
	if tc.mu.txn.SnapshotTS.IsEmpty() {
		tc.logger.Fatal("missing txn snapshot timestamp")
	}
	if tc.option.readyOnly && tc.option.enableCacheWrite {
		tc.logger.Fatal("readyOnly and delayWrites cannot both be set")
	}

	if tc.logger.Core().Enabled(zap.DebugLevel) {
		tc.logger = tc.logger.With(util.TxnIDField(tc.mu.txn))
	}
}

func (tc *txnCoordinator) Read(ctx context.Context, requests []txn.TxnRequest) ([]txn.TxnResponse, error) {
	for idx := range requests {
		requests[idx].Method = txn.TxnMethod_Read
	}

	if ce := tc.logger.Check(zap.DebugLevel, "handle read requests"); ce != nil {
		fields := make([]zap.Field, 0, len(requests))
		for idx, req := range requests {
			fields = append(fields, zap.String(fmt.Sprintf("request-%d", idx), req.DebugString()))
		}
		ce.Write(fields...)
	}

	if err := tc.validate(ctx, false); err != nil {
		return nil, err
	}

	requests = tc.maybeInsertCachedWrites(ctx, requests, false)
	return tc.trimResponses(tc.handleError(tc.doSend(ctx, requests, false)))
}

func (tc *txnCoordinator) Write(ctx context.Context, requests []txn.TxnRequest) ([]txn.TxnResponse, error) {
	return tc.doWrite(ctx, requests, false)
}

func (tc *txnCoordinator) WriteAndCommit(ctx context.Context, requests []txn.TxnRequest) ([]txn.TxnResponse, error) {
	return tc.doWrite(ctx, requests, true)
}

func (tc *txnCoordinator) Commit(ctx context.Context) error {
	tc.logger.Debug("handle commit")
	defer tc.logger.Debug("txn closed by commit")

	if tc.option.readyOnly {
		return nil
	}

	_, err := tc.doWrite(ctx, nil, true)
	if err != nil {
		tc.logger.Error("commit txn failed",
			zap.String("txn", tc.mu.txn.DebugString()),
			zap.Error(err))
	}
	return err
}

func (tc *txnCoordinator) Rollback(ctx context.Context) error {
	tc.logger.Debug("handle rollback")
	defer tc.logger.Debug("txn closed by rollback")

	tc.mu.Lock()
	defer func() {
		tc.mu.closed = true
		tc.mu.Unlock()
	}()

	// no write request handled
	if len(tc.mu.partitions) == 0 {
		tc.logger.Debug("rollback on 0 partitions")
		return nil
	}

	_, err := tc.handleError(tc.doSend(ctx, []txn.TxnRequest{{
		Method: txn.TxnMethod_Rollback,
		RollbackRequest: &txn.TxnRollbackRequest{
			Partitions: tc.mu.partitions,
		},
	}}, true))

	if err != nil {
		tc.logger.Error("rollback txn failed",
			zap.String("txn", tc.mu.txn.DebugString()),
			zap.Error(err))
	}
	return err
}

func (tc *txnCoordinator) doWrite(ctx context.Context, requests []txn.TxnRequest, commit bool) ([]txn.TxnResponse, error) {
	for idx := range requests {
		requests[idx].Method = txn.TxnMethod_Write
	}

	if ce := tc.logger.Check(zap.DebugLevel, "handle write requests"); ce != nil {
		fields := make([]zap.Field, 0, len(requests)+1)
		for idx, req := range requests {
			fields = append(fields, zap.String(fmt.Sprintf("request-%d", idx), req.DebugString()))
		}
		fields = append(fields, zap.Bool("commit", commit))
		ce.Write(fields...)
	}

	if tc.option.readyOnly {
		tc.logger.Fatal("can not write on ready only transaction")
	}

	if commit {
		tc.mu.Lock()
		defer func() {
			tc.mu.closed = true
			tc.mu.Unlock()
		}()
	}

	if err := tc.validate(ctx, commit); err != nil {
		return nil, err
	}

	tc.updateWritePartitions(requests, commit)

	// delayWrites enabled, no responses
	if !commit && tc.maybeCacheWrites(requests, commit) {
		tc.logger.Debug("add write requests to cache",
			zap.Int("requests", len(requests)))
		return nil, nil
	}

	if commit {
		if len(tc.mu.partitions) == 0 { // commit no write handled txn
			tc.logger.Debug("commit on 0 partitions")
			return nil, nil
		}

		requests = tc.maybeInsertCachedWrites(ctx, requests, true)
		requests = append(requests, txn.TxnRequest{
			Method: txn.TxnMethod_Commit,
			Flag:   txn.SkipResponseFlag,
			CommitRequest: &txn.TxnCommitRequest{
				Partitions:    tc.mu.partitions,
				Disable1PCOpt: tc.option.disable1PCOpt,
			}})
		if ce := tc.logger.Check(zap.DebugLevel, "commit on partitions"); ce != nil {
			fields := make([]zap.Field, 0, len(tc.mu.partitions))
			for idx, p := range tc.mu.partitions {
				fields = append(fields, zap.String(fmt.Sprintf("partition-%d", idx), p.DebugString()))
			}
			ce.Write(fields...)
		}
	}
	return tc.trimResponses(tc.handleError(tc.doSend(ctx, requests, commit)))
}

func (tc *txnCoordinator) updateWritePartitions(requests []txn.TxnRequest, locked bool) {
	if len(requests) == 0 {
		return
	}

	if !locked {
		tc.mu.Lock()
		defer tc.mu.Unlock()
	}

	for _, req := range requests {
		tc.addPartitionLocked(req.CNRequest.Target)
	}
}

func (tc *txnCoordinator) addPartitionLocked(dn metadata.DNShard) {
	for idx := range tc.mu.partitions {
		if tc.mu.partitions[idx].ShardID == dn.ShardID {
			return
		}
	}
	tc.mu.partitions = append(tc.mu.partitions, dn)
	if ce := tc.logger.Check(zap.DebugLevel, "partition added"); ce != nil {
		ce.Write(zap.String("dn", dn.DebugString()))
	}
}

func (tc *txnCoordinator) validate(ctx context.Context, locked bool) error {
	if _, ok := ctx.Deadline(); !ok {
		tc.logger.Fatal("context deadline set")
	}

	return tc.checkStatus(locked)
}

func (tc *txnCoordinator) checkStatus(locked bool) error {
	if !locked {
		tc.mu.RLock()
		defer tc.mu.RUnlock()
	}

	if tc.mu.closed {
		return errTxnClosed
	}

	return nil
}

func (tc *txnCoordinator) maybeCacheWrites(requests []txn.TxnRequest, locked bool) bool {
	if tc.option.enableCacheWrite {
		tc.mu.Lock()
		defer tc.mu.Unlock()
		for idx := range requests {
			requests[idx].Flag |= txn.SkipResponseFlag
			dn := requests[idx].CNRequest.Target.ShardID
			tc.mu.cachedWrites[dn] = append(tc.mu.cachedWrites[dn], requests[idx])
		}
		return true
	}

	return false
}

func (tc *txnCoordinator) maybeInsertCachedWrites(ctx context.Context, requests []txn.TxnRequest, locked bool) []txn.TxnRequest {
	if len(requests) == 0 || !tc.option.enableCacheWrite {
		return requests
	}

	if !locked {
		tc.mu.Lock()
		defer tc.mu.Unlock()
	}

	if len(tc.mu.cachedWrites) == 0 {
		return requests
	}

	newRequests := requests
	hasCachedWrites := false
	insertCount := 0
	for idx := range requests {
		dn := requests[idx].CNRequest.Target.ShardID
		if writes, ok := tc.getCachedWritesLocked(dn); ok {
			if !hasCachedWrites {
				// copy all requests into newRequests if cached writes encountered
				newRequests = append([]txn.TxnRequest(nil), requests[:idx]...)
			}
			newRequests = append(newRequests, writes...)
			tc.clearCachedWritesLocked(dn)
			hasCachedWrites = true
			insertCount += len(writes)
		}
		if hasCachedWrites {
			newRequests = append(newRequests, requests[idx])
		}
	}

	if insertCount > 0 {
		tc.logger.Debug("insert cached write requests",
			zap.Int("count", insertCount))
	}
	return newRequests
}

func (tc *txnCoordinator) getCachedWritesLocked(dn uint64) ([]txn.TxnRequest, bool) {
	writes, ok := tc.mu.cachedWrites[dn]
	if !ok || len(writes) == 0 {
		return nil, false
	}
	return writes, true
}

func (tc *txnCoordinator) clearCachedWritesLocked(dn uint64) {
	delete(tc.mu.cachedWrites, dn)
	tc.logger.Debug("cached write requests removed",
		zap.Uint64("dn", dn))
}

func (tc *txnCoordinator) getTxnMeta(locked bool) txn.TxnMeta {
	if !locked {
		tc.mu.RLock()
		defer tc.mu.RUnlock()
	}
	return tc.mu.txn
}

func (tc *txnCoordinator) doSend(ctx context.Context, requests []txn.TxnRequest, locked bool) ([]txn.TxnResponse, error) {
	txnMeta := tc.getTxnMeta(locked)
	for idx := range requests {
		requests[idx].Txn = txnMeta
	}

	if ce := tc.logger.Check(zap.DebugLevel, "send requests"); ce != nil {
		fields := make([]zap.Field, 0, len(requests)+1)
		fields = append(fields, zap.Int("count", len(requests)))
		for idx, req := range requests {
			fields = append(fields, zap.String(fmt.Sprintf("request-%d", idx), req.DebugString()))
		}
		ce.Write(fields...)
	}

	responses, err := tc.sender.Send(ctx, requests)
	if err != nil {
		tc.logger.Error("send requests failed",
			zap.Int("count", len(requests)),
			zap.String("txn", txnMeta.DebugString()),
			zap.Error(err))
		return nil, err
	}

	if ce := tc.logger.Check(zap.DebugLevel, "receive responses"); ce != nil {
		fields := make([]zap.Field, 0, len(responses)+1)
		fields = append(fields, zap.Int("count", len(responses)))
		for idx, resp := range responses {
			fields = append(fields, zap.String(fmt.Sprintf("response-%d", idx), resp.DebugString()))
		}
		ce.Write(fields...)
	}
	return responses, nil
}

func (tc *txnCoordinator) handleError(responses []txn.TxnResponse, err error) ([]txn.TxnResponse, error) {
	if err != nil {
		return nil, err
	}

	for _, resp := range responses {
		switch resp.Txn.GetStatus() {
		case txn.TxnStatus_Aborted, txn.TxnStatus_Aborting:
			// read after txn aborted
			return nil, errTxnAborted
		case txn.TxnStatus_Committed, txn.TxnStatus_Committing, txn.TxnStatus_Prepared:
			switch resp.Method {
			case txn.TxnMethod_Read, txn.TxnMethod_Write:
				return nil, errTxnClosed
			case txn.TxnMethod_Rollback:
				panic("BUG")
			case txn.TxnMethod_Commit:
				// it's ok
			}
		}

		// TODO: handle explicit txn error to error,  resp.TxnError
	}
	return responses, nil
}

func (tc *txnCoordinator) trimResponses(responses []txn.TxnResponse, err error) ([]txn.TxnResponse, error) {
	if err != nil {
		return nil, err
	}

	values := responses[:0]
	for _, resp := range responses {
		if !resp.HasFlag(txn.SkipResponseFlag) {
			values = append(values, resp)
		}
	}
	return values, nil
}
