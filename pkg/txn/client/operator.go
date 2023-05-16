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
	"bytes"
	"context"
	"sync"

	"github.com/matrixorigin/matrixone/pkg/common/moerr"
	"github.com/matrixorigin/matrixone/pkg/lockservice"
	"github.com/matrixorigin/matrixone/pkg/pb/lock"
	"github.com/matrixorigin/matrixone/pkg/pb/metadata"
	"github.com/matrixorigin/matrixone/pkg/pb/timestamp"
	"github.com/matrixorigin/matrixone/pkg/pb/txn"
	"github.com/matrixorigin/matrixone/pkg/txn/rpc"
	"github.com/matrixorigin/matrixone/pkg/txn/util"
	"go.uber.org/zap"
)

var (
	_ EventableTxnOperator = (*txnOperator)(nil)
	_ DebugableTxnOperator = (*txnOperator)(nil)
)

var (
	readTxnErrors = map[uint16]struct{}{
		moerr.ErrTAERead:      {},
		moerr.ErrRpcError:     {},
		moerr.ErrWaitTxn:      {},
		moerr.ErrTxnNotFound:  {},
		moerr.ErrTxnNotActive: {},
	}
	writeTxnErrors = map[uint16]struct{}{
		moerr.ErrTAEWrite:     {},
		moerr.ErrRpcError:     {},
		moerr.ErrTxnNotFound:  {},
		moerr.ErrTxnNotActive: {},
	}
	commitTxnErrors = map[uint16]struct{}{
		moerr.ErrTAECommit:    {},
		moerr.ErrTAERollback:  {},
		moerr.ErrTAEPrepare:   {},
		moerr.ErrRpcError:     {},
		moerr.ErrTxnNotFound:  {},
		moerr.ErrTxnNotActive: {},
	}
	rollbackTxnErrors = map[uint16]struct{}{
		moerr.ErrTAERollback:  {},
		moerr.ErrRpcError:     {},
		moerr.ErrTxnNotFound:  {},
		moerr.ErrTxnNotActive: {},
	}
)

// WithTxnReadyOnly setup readyonly flag
func WithTxnReadyOnly() TxnOption {
	return func(tc *txnOperator) {
		tc.option.readyOnly = true
	}
}

// WithTxnDisable1PCOpt disable 1pc optimisation on distributed transaction. By default, mo enables 1pc
// optimization for distributed transactions. For write operations, if all partitions' prepares are
// executed successfully, then the transaction is considered committed and returned directly to the
// client. Partitions' prepared data are committed asynchronously.
func WithTxnDisable1PCOpt() TxnOption {
	return func(tc *txnOperator) {
		tc.option.disable1PCOpt = true
	}
}

// WithTxnCNCoordinator set cn txn coodinator
func WithTxnCNCoordinator() TxnOption {
	return func(tc *txnOperator) {
		tc.option.coordinator = true
	}
}

// WithTxnLockService set txn lock service
func WithTxnLockService(lockService lockservice.LockService) TxnOption {
	return func(tc *txnOperator) {
		tc.option.lockService = lockService
	}
}

// WithTxnCacheWrite Set cache write requests, after each Write call, the request will not be sent
// to the DN node immediately, but stored in the Coordinator's memory, and the Coordinator will
// choose the right time to send the cached requests. The following scenarios trigger the sending
// of requests to DN:
//  1. Before read, because the Coordinator is not aware of the format and content of the written data,
//     it is necessary to send the cached write requests to the corresponding DN node each time Read is
//     called, used to implement "read your write".
//  2. Before commit, obviously, the cached write requests needs to be sent to the corresponding DN node
//     before commit.
func WithTxnCacheWrite() TxnOption {
	return func(tc *txnOperator) {
		tc.option.enableCacheWrite = true
		tc.mu.cachedWrites = make(map[uint64][]txn.TxnRequest)
	}
}

// WithSnapshotTS use a spec snapshot timestamp to build TxnOperator.
func WithSnapshotTS(ts timestamp.Timestamp) TxnOption {
	return func(tc *txnOperator) {
		tc.mu.txn.SnapshotTS = ts
	}
}

// WithTxnMode set txn mode
func WithTxnMode(value txn.TxnMode) TxnOption {
	return func(tc *txnOperator) {
		tc.mu.txn.Mode = value
	}
}

// WithTxnIsolation set txn isolation
func WithTxnIsolation(value txn.TxnIsolation) TxnOption {
	return func(tc *txnOperator) {
		tc.mu.txn.Isolation = value
	}
}

type txnOperator struct {
	sender rpc.TxnSender
	txnID  []byte

	option struct {
		readyOnly        bool
		enableCacheWrite bool
		disable1PCOpt    bool
		coordinator      bool
		lockService      lockservice.LockService
	}

	mu struct {
		sync.RWMutex
		closed       bool
		txn          txn.TxnMeta
		cachedWrites map[uint64][]txn.TxnRequest
		lockTables   []lock.LockTable
		callbacks    map[EventType][]func(txn.TxnMeta)
	}
	workspace Workspace
}

func newTxnOperator(
	sender rpc.TxnSender,
	txnMeta txn.TxnMeta,
	options ...TxnOption) *txnOperator {
	tc := &txnOperator{sender: sender}
	tc.mu.txn = txnMeta
	tc.txnID = txnMeta.ID
	for _, opt := range options {
		opt(tc)
	}
	tc.adjust()
	util.LogTxnCreated(tc.mu.txn)
	return tc
}

func newTxnOperatorWithSnapshot(
	sender rpc.TxnSender,
	snapshot []byte) (*txnOperator, error) {
	v := &txn.CNTxnSnapshot{}
	if err := v.Unmarshal(snapshot); err != nil {
		return nil, err
	}

	tc := &txnOperator{sender: sender}
	tc.mu.txn = v.Txn
	tc.txnID = v.Txn.ID
	tc.mu.lockTables = v.LockTables
	tc.option.disable1PCOpt = v.Disable1PCOpt
	tc.option.enableCacheWrite = v.EnableCacheWrite
	tc.option.readyOnly = v.ReadyOnly

	tc.adjust()
	util.LogTxnCreated(tc.mu.txn)
	return tc, nil
}

func (tc *txnOperator) AddWorkspace(workspace Workspace) {
	tc.workspace = workspace
}

func (tc *txnOperator) GetWorkspace() Workspace {
	return tc.workspace
}

func (tc *txnOperator) adjust() {
	if tc.sender == nil {
		util.GetLogger().Fatal("missing txn sender")
	}
	if len(tc.mu.txn.ID) == 0 {
		util.GetLogger().Fatal("missing txn id")
	}
	if tc.mu.txn.SnapshotTS.IsEmpty() {
		util.GetLogger().Fatal("missing txn snapshot timestamp")
	}
	if tc.option.readyOnly && tc.option.enableCacheWrite {
		util.GetLogger().Fatal("readyOnly and delayWrites cannot both be set")
	}
}

func (tc *txnOperator) Txn() txn.TxnMeta {
	return tc.getTxnMeta(false)
}

func (tc *txnOperator) Snapshot() ([]byte, error) {
	tc.mu.Lock()
	defer tc.mu.Unlock()

	if err := tc.checkStatus(true); err != nil {
		return nil, err
	}

	snapshot := &txn.CNTxnSnapshot{
		Txn:              tc.mu.txn,
		ReadyOnly:        tc.option.readyOnly,
		EnableCacheWrite: tc.option.enableCacheWrite,
		Disable1PCOpt:    tc.option.disable1PCOpt,
		LockTables:       tc.mu.lockTables,
	}
	return snapshot.Marshal()
}

func (tc *txnOperator) UpdateSnapshot(ts timestamp.Timestamp) error {
	tc.mu.Lock()
	defer tc.mu.Unlock()
	if err := tc.checkStatus(true); err != nil {
		return err
	}
	if tc.mu.txn.SnapshotTS.Less(ts) {
		tc.mu.txn.SnapshotTS = ts
	}
	return nil
}

func (tc *txnOperator) ApplySnapshot(data []byte) error {
	if !tc.option.coordinator {
		util.GetLogger().Fatal("apply snapshot on non-coordinator txn operator")
	}

	tc.mu.Lock()
	defer tc.mu.Unlock()

	if err := tc.checkStatus(true); err != nil {
		return err
	}

	snapshot := &txn.CNTxnSnapshot{}
	if err := snapshot.Unmarshal(data); err != nil {
		return err
	}

	if !bytes.Equal(snapshot.Txn.ID, tc.mu.txn.ID) {
		util.GetLogger().Fatal("apply snapshot with invalid txn id")
	}

	// apply locked tables in other cn
	for _, v := range snapshot.LockTables {
		if err := tc.doAddLockTableLocked(v); err != nil {
			return err
		}
	}

	for _, dn := range snapshot.Txn.DNShards {
		has := false
		for _, v := range tc.mu.txn.DNShards {
			if v.ShardID == dn.ShardID {
				has = true
				break
			}
		}

		if !has {
			tc.mu.txn.DNShards = append(tc.mu.txn.DNShards, dn)
		}
	}
	if tc.mu.txn.SnapshotTS.Less(snapshot.Txn.SnapshotTS) {
		tc.mu.txn.SnapshotTS = snapshot.Txn.SnapshotTS
	}
	util.LogTxnUpdated(tc.mu.txn)
	return nil
}

func (tc *txnOperator) Read(ctx context.Context, requests []txn.TxnRequest) (*rpc.SendResult, error) {
	util.LogTxnRead(tc.getTxnMeta(false))

	for idx := range requests {
		requests[idx].Method = txn.TxnMethod_Read
	}

	if err := tc.validate(ctx, false); err != nil {
		return nil, err
	}

	requests = tc.maybeInsertCachedWrites(ctx, requests, false)
	return tc.trimResponses(tc.handleError(tc.doSend(ctx, requests, false)))
}

func (tc *txnOperator) Write(ctx context.Context, requests []txn.TxnRequest) (*rpc.SendResult, error) {
	util.LogTxnWrite(tc.getTxnMeta(false))

	return tc.doWrite(ctx, requests, false)
}

func (tc *txnOperator) WriteAndCommit(ctx context.Context, requests []txn.TxnRequest) (*rpc.SendResult, error) {
	return tc.doWrite(ctx, requests, true)
}

func (tc *txnOperator) Commit(ctx context.Context) error {
	util.LogTxnCommit(tc.getTxnMeta(false))

	if tc.option.readyOnly {
		tc.mu.Lock()
		defer tc.mu.Unlock()
		tc.closeLocked()
		return nil
	}

	result, err := tc.doWrite(ctx, nil, true)
	if err != nil {
		return err
	}
	if result != nil {
		result.Release()
	}
	return nil
}

func (tc *txnOperator) Rollback(ctx context.Context) error {
	util.LogTxnRollback(tc.getTxnMeta(false))

	tc.mu.Lock()
	defer func() {
		tc.mu.txn.Status = txn.TxnStatus_Aborted
		tc.closeLocked()
		tc.mu.Unlock()
	}()

	if len(tc.mu.txn.DNShards) == 0 {
		return nil
	}

	if tc.needUnlockLocked() {
		defer tc.unlock(ctx)
	}

	result, err := tc.handleError(tc.doSend(ctx, []txn.TxnRequest{{
		Method:          txn.TxnMethod_Rollback,
		RollbackRequest: &txn.TxnRollbackRequest{},
	}}, true))
	if err != nil {
		if moerr.IsMoErrCode(err, moerr.ErrTxnClosed) {
			return nil
		}
		return err
	}
	if result != nil {
		result.Release()
	}
	return nil
}

func (tc *txnOperator) AddLockTable(value lock.LockTable) error {
	tc.mu.Lock()
	defer tc.mu.Unlock()
	if tc.mu.txn.Mode != txn.TxnMode_Pessimistic {
		panic("lock in optimistic mode")
	}

	if err := tc.checkStatus(true); err != nil {
		return err
	}

	return tc.doAddLockTableLocked(value)
}

func (tc *txnOperator) doAddLockTableLocked(value lock.LockTable) error {
	for _, l := range tc.mu.lockTables {
		if l.Table == value.Table {
			if l.Changed(value) {
				return moerr.NewDeadLockDetectedNoCtx()
			}
			return nil
		}
	}
	tc.mu.lockTables = append(tc.mu.lockTables, value)
	return nil
}

func (tc *txnOperator) Debug(ctx context.Context, requests []txn.TxnRequest) (*rpc.SendResult, error) {
	for idx := range requests {
		requests[idx].Method = txn.TxnMethod_DEBUG
	}

	if err := tc.validate(ctx, false); err != nil {
		return nil, err
	}

	requests = tc.maybeInsertCachedWrites(ctx, requests, false)
	return tc.trimResponses(tc.handleError(tc.doSend(ctx, requests, false)))
}

func (tc *txnOperator) doWrite(ctx context.Context, requests []txn.TxnRequest, commit bool) (*rpc.SendResult, error) {
	for idx := range requests {
		requests[idx].Method = txn.TxnMethod_Write
	}

	if tc.option.readyOnly {
		util.GetLogger().Fatal("can not write on ready only transaction")
	}

	if commit {
		tc.mu.Lock()
		defer func() {
			tc.closeLocked()
			tc.mu.Unlock()
		}()
		if tc.needUnlockLocked() {
			tc.mu.txn.LockTables = tc.mu.lockTables
			defer tc.unlock(ctx)
		}
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
		if len(tc.mu.txn.DNShards) == 0 { // commit no write handled txn
			tc.mu.txn.Status = txn.TxnStatus_Committed
			return nil, nil
		}
		requests = tc.maybeInsertCachedWrites(ctx, requests, true)
		requests = append(requests, txn.TxnRequest{
			Method: txn.TxnMethod_Commit,
			Flag:   txn.SkipResponseFlag,
			CommitRequest: &txn.TxnCommitRequest{
				Disable1PCOpt: tc.option.disable1PCOpt,
			}})
	}
	return tc.trimResponses(tc.handleError(tc.doSend(ctx, requests, commit)))
}

func (tc *txnOperator) updateWritePartitions(requests []txn.TxnRequest, locked bool) {
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

func (tc *txnOperator) addPartitionLocked(dn metadata.DNShard) {
	for idx := range tc.mu.txn.DNShards {
		if tc.mu.txn.DNShards[idx].ShardID == dn.ShardID {
			return
		}
	}
	tc.mu.txn.DNShards = append(tc.mu.txn.DNShards, dn)
	util.LogTxnUpdated(tc.mu.txn)
}

func (tc *txnOperator) validate(ctx context.Context, locked bool) error {
	if _, ok := ctx.Deadline(); !ok {
		util.GetLogger().Fatal("context deadline set")
	}

	return tc.checkStatus(locked)
}

func (tc *txnOperator) checkStatus(locked bool) error {
	if !locked {
		tc.mu.RLock()
		defer tc.mu.RUnlock()
	}

	if tc.mu.closed {
		return moerr.NewTxnClosedNoCtx(tc.txnID)
	}
	return nil
}

func (tc *txnOperator) maybeCacheWrites(requests []txn.TxnRequest, locked bool) bool {
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

func (tc *txnOperator) maybeInsertCachedWrites(ctx context.Context, requests []txn.TxnRequest, locked bool) []txn.TxnRequest {
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
	return newRequests
}

func (tc *txnOperator) getCachedWritesLocked(dn uint64) ([]txn.TxnRequest, bool) {
	writes, ok := tc.mu.cachedWrites[dn]
	if !ok || len(writes) == 0 {
		return nil, false
	}
	return writes, true
}

func (tc *txnOperator) clearCachedWritesLocked(dn uint64) {
	delete(tc.mu.cachedWrites, dn)
}

func (tc *txnOperator) getTxnMeta(locked bool) txn.TxnMeta {
	if !locked {
		tc.mu.RLock()
		defer tc.mu.RUnlock()
	}
	return tc.mu.txn
}

func (tc *txnOperator) doSend(ctx context.Context, requests []txn.TxnRequest, locked bool) (*rpc.SendResult, error) {
	txnMeta := tc.getTxnMeta(locked)
	for idx := range requests {
		requests[idx].Txn = txnMeta
	}

	util.LogTxnSendRequests(requests)
	result, err := tc.sender.Send(ctx, requests)
	if err != nil {
		util.LogTxnSendRequestsFailed(requests, err)
		return nil, err
	}
	util.LogTxnReceivedResponses(result.Responses)

	if len(result.Responses) == 0 {
		return result, nil
	}

	// update commit timestamp
	resp := result.Responses[len(result.Responses)-1]
	if resp.Txn == nil {
		return result, nil
	}
	if !locked {
		tc.mu.Lock()
		defer tc.mu.Unlock()
	}
	tc.mu.txn.CommitTS = resp.Txn.CommitTS
	tc.mu.txn.Status = resp.Txn.Status
	return result, nil
}

func (tc *txnOperator) handleError(result *rpc.SendResult, err error) (*rpc.SendResult, error) {
	if err != nil {
		return nil, err
	}

	for _, resp := range result.Responses {
		if err := tc.handleErrorResponse(resp); err != nil {
			result.Release()
			return nil, err
		}
	}
	return result, nil
}

func (tc *txnOperator) handleErrorResponse(resp txn.TxnResponse) error {
	switch resp.Method {
	case txn.TxnMethod_Read:
		if err := tc.checkResponseTxnStatusForReadWrite(resp); err != nil {
			return err
		}
		return tc.checkTxnError(resp.TxnError, readTxnErrors)
	case txn.TxnMethod_Write:
		if err := tc.checkResponseTxnStatusForReadWrite(resp); err != nil {
			return err
		}
		return tc.checkTxnError(resp.TxnError, writeTxnErrors)
	case txn.TxnMethod_Commit:
		if err := tc.checkResponseTxnStatusForCommit(resp); err != nil {
			return err
		}
		return tc.checkTxnError(resp.TxnError, commitTxnErrors)
	case txn.TxnMethod_Rollback:
		if err := tc.checkResponseTxnStatusForRollback(resp); err != nil {
			return err
		}
		return tc.checkTxnError(resp.TxnError, rollbackTxnErrors)
	case txn.TxnMethod_DEBUG:
		if resp.TxnError != nil {
			return resp.TxnError.UnwrapError()
		}
		return nil
	default:
		util.GetLogger().Fatal("invalid response",
			zap.String("response", resp.DebugString()))
	}
	return nil
}

func (tc *txnOperator) checkResponseTxnStatusForReadWrite(resp txn.TxnResponse) error {
	if resp.TxnError != nil {
		return nil
	}

	txnMeta := resp.Txn
	if txnMeta == nil {
		return moerr.NewTxnClosedNoCtx(tc.txnID)
	}

	switch txnMeta.Status {
	case txn.TxnStatus_Active:
		return nil
	case txn.TxnStatus_Aborted, txn.TxnStatus_Aborting,
		txn.TxnStatus_Committed, txn.TxnStatus_Committing:
		return moerr.NewTxnClosedNoCtx(tc.txnID)
	default:
		util.GetLogger().Fatal("invalid response status for read or write",
			util.TxnField(*txnMeta))
	}
	return nil
}

func (tc *txnOperator) checkTxnError(txnError *txn.TxnError, possibleErrorMap map[uint16]struct{}) error {
	if txnError == nil {
		return nil
	}

	// use txn internal error code to check error
	txnCode := uint16(txnError.TxnErrCode)
	if txnCode == moerr.ErrDNShardNotFound {
		// do we still have the uuid and shard id?
		return moerr.NewDNShardNotFoundNoCtx("", 0xDEADBEAF)
	}

	if _, ok := possibleErrorMap[txnCode]; ok {
		return txnError.UnwrapError()
	}

	panic(moerr.NewInternalErrorNoCtx("invalid txn error, code %d, msg %s", txnCode, txnError.DebugString()))
}

func (tc *txnOperator) checkResponseTxnStatusForCommit(resp txn.TxnResponse) error {
	if resp.TxnError != nil {
		return nil
	}

	txnMeta := resp.Txn
	if txnMeta == nil {
		return moerr.NewTxnClosedNoCtx(tc.txnID)
	}

	switch txnMeta.Status {
	case txn.TxnStatus_Committed, txn.TxnStatus_Aborted:
		return nil
	default:
		panic(moerr.NewInternalErrorNoCtx("invalid respose status for commit, %v", txnMeta.Status))
	}
}

func (tc *txnOperator) checkResponseTxnStatusForRollback(resp txn.TxnResponse) error {
	if resp.TxnError != nil {
		return nil
	}

	txnMeta := resp.Txn
	if txnMeta == nil {
		return moerr.NewTxnClosedNoCtx(tc.txnID)
	}

	switch txnMeta.Status {
	case txn.TxnStatus_Aborted:
		return nil
	default:
		panic(moerr.NewInternalErrorNoCtx("invalud response status for rollback %v", txnMeta.Status))
	}
}

func (tc *txnOperator) trimResponses(result *rpc.SendResult, err error) (*rpc.SendResult, error) {
	if err != nil {
		return nil, err
	}

	values := result.Responses[:0]
	for _, resp := range result.Responses {
		if !resp.HasFlag(txn.SkipResponseFlag) {
			values = append(values, resp)
		}
	}
	result.Responses = values
	return result, nil
}

func (tc *txnOperator) unlock(ctx context.Context) {
	if err := tc.option.lockService.Unlock(
		ctx,
		tc.mu.txn.ID,
		tc.mu.txn.CommitTS); err != nil {
		util.GetLogger().Error("failed to unlock txn",
			util.TxnField(tc.mu.txn),
			zap.Error(err))
	}
}

func (tc *txnOperator) needUnlockLocked() bool {
	if tc.mu.txn.Mode ==
		txn.TxnMode_Optimistic {
		return false
	}
	return tc.option.lockService != nil
}

func (tc *txnOperator) closeLocked() {
	tc.mu.closed = true
	tc.triggerEventLocked(ClosedEvent)
}
