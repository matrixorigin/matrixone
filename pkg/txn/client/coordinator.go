package client

import (
	"context"
	"sync"

	"github.com/matrixorigin/matrixone/pkg/logutil"
	"github.com/matrixorigin/matrixone/pkg/pb/metadata"
	"github.com/matrixorigin/matrixone/pkg/pb/txn"
	"github.com/matrixorigin/matrixone/pkg/txn/util"
	"go.uber.org/zap"
	"go.uber.org/zap/zapcore"
)

// WithReadyOnly setup readyonly flag
func WithReadyOnly() TxnOption {
	return func(tc *txnCoordinator) {
		tc.option.readyOnly = true
	}
}

// WithTxnLogger setup txn logger
func WithTxnLogger(logger *zap.Logger) TxnOption {
	return func(tc *txnCoordinator) {
		tc.logger = logger
	}
}

// WithCacheWrite Set cache write requests, after each Write call, the request will not be sent
// to the DN node immediately, but stored in the Coordinator's memory, and the Coordinator will
// choose the right time to send the cached requests. The following scenarios trigger the sending
// of requests to DN:
// 1. Before read, because the Coordinator is not aware of the format and content of the written data,
//    it is necessary to send the cached write requests to the corresponding DN node each time Read is
//    called, used to implement "read your write".
// 2. Before commit, obviously, the cached write requests needs to be sent to the corresponding DN node
//    before commit.
func WithCacheWrite() TxnOption {
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
	}

	mu struct {
		sync.RWMutex
		closed       bool
		txn          txn.TxnMeta
		cachedWrites map[uint64][]txn.TxnRequest
		partitions   []metadata.DN
	}
}

func newTxnCoordinator(sender TxnSender, txnMeta txn.TxnMeta, options ...TxnOption) *txnCoordinator {
	tc := &txnCoordinator{sender: sender}
	tc.mu.txn = txnMeta

	for _, opt := range options {
		opt(tc)
	}
	tc.adjust()
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

	if tc.logger.Core().Enabled(zapcore.DebugLevel) {
		tc.logger = tc.logger.With(util.TxnIDField(tc.mu.txn))
	}
}

func (tc *txnCoordinator) Read(ctx context.Context, requests []txn.TxnRequest) ([]txn.TxnResponse, error) {
	if err := tc.validate(ctx, false); err != nil {
		return nil, err
	}

	for idx := range requests {
		requests[idx].Op = txn.TxnOp_Read
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
	if tc.option.readyOnly {
		return nil
	}

	_, err := tc.doWrite(ctx, nil, true)
	return err
}

func (tc *txnCoordinator) Rollback(ctx context.Context) error {
	tc.mu.Lock()
	defer func() {
		tc.mu.closed = true
		tc.mu.Unlock()
	}()

	// no write request handled
	if len(tc.mu.partitions) == 0 {
		return nil
	}

	_, err := tc.handleError(tc.doSend(ctx, []txn.TxnRequest{{
		Op: txn.TxnOp_Rollback,
		RollbackRequest: &txn.TxnRollbackRequest{
			Partitions: tc.mu.partitions,
		},
	}}, true))
	return err
}

func (tc *txnCoordinator) doWrite(ctx context.Context, requests []txn.TxnRequest, commit bool) ([]txn.TxnResponse, error) {
	if tc.option.readyOnly {
		tc.logger.Fatal("can not write on ready only transaction")
	}

	for idx := range requests {
		requests[idx].Op = txn.TxnOp_Write
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
		return nil, nil
	}

	if commit {
		if len(tc.mu.partitions) == 0 { // commit no write handled txn
			return nil, nil
		}

		requests = tc.maybeInsertCachedWrites(ctx, requests, true)
		requests = append(requests, txn.TxnRequest{
			Op:   txn.TxnOp_Commit,
			Flag: txn.SkipResponseFlag,
			CommitRequest: &txn.TxnCommitRequest{
				Partitions: tc.mu.partitions,
			}})
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

func (tc *txnCoordinator) addPartitionLocked(dn metadata.DN) {
	for idx := range tc.mu.partitions {
		if tc.mu.partitions[idx].ID == dn.ID {
			return
		}
	}
	tc.mu.partitions = append(tc.mu.partitions, dn)
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
			dn := requests[idx].CNRequest.Target.ID
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
	for idx := range requests {
		dn := requests[idx].CNRequest.Target.ID
		if writes, ok := tc.getCachedWritesLocked(dn); ok {
			if !hasCachedWrites {
				// copy all requests into newRequests if cached writes encountered
				newRequests = append([]txn.TxnRequest(nil), requests[:idx]...)
			}
			newRequests = append(newRequests, writes...)
			tc.clearCachedWritesLocked(dn)
			hasCachedWrites = true
		}
		if hasCachedWrites {
			newRequests = append(newRequests, requests[idx])
		}
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
	return tc.sender.Send(ctx, requests)
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
			switch resp.Op {
			case txn.TxnOp_Read, txn.TxnOp_Write:
				return nil, errTxnClosed
			case txn.TxnOp_Rollback:
				panic("BUG")
			case txn.TxnOp_Commit:
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
