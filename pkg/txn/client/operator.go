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
	"encoding/hex"
	"errors"
	"fmt"
	"sync"
	"sync/atomic"
	"time"

	"go.uber.org/zap"

	"github.com/matrixorigin/matrixone/pkg/common/moerr"
	"github.com/matrixorigin/matrixone/pkg/common/runtime"
	"github.com/matrixorigin/matrixone/pkg/lockservice"
	"github.com/matrixorigin/matrixone/pkg/pb/lock"
	"github.com/matrixorigin/matrixone/pkg/pb/metadata"
	"github.com/matrixorigin/matrixone/pkg/pb/timestamp"
	"github.com/matrixorigin/matrixone/pkg/pb/txn"
	"github.com/matrixorigin/matrixone/pkg/txn/clock"
	"github.com/matrixorigin/matrixone/pkg/txn/rpc"
	"github.com/matrixorigin/matrixone/pkg/txn/util"
	v2 "github.com/matrixorigin/matrixone/pkg/util/metric/v2"
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
		moerr.ErrTAECommit:            {},
		moerr.ErrTAERollback:          {},
		moerr.ErrTAEPrepare:           {},
		moerr.ErrRpcError:             {},
		moerr.ErrTxnNotFound:          {},
		moerr.ErrTxnNotActive:         {},
		moerr.ErrLockTableBindChanged: {},
		moerr.ErrCannotCommitOrphan:   {},
	}
	rollbackTxnErrors = map[uint16]struct{}{
		moerr.ErrTAERollback:  {},
		moerr.ErrRpcError:     {},
		moerr.ErrTxnNotFound:  {},
		moerr.ErrTxnNotActive: {},
	}
)

// WithUserTxn setup user transaction flag. Only user transactions need to be controlled for the maximum
// number of active transactions.
func WithUserTxn() TxnOption {
	return func(tc *txnOperator) {
		tc.options = tc.options.WithUserTxn()
	}
}

// WithTxnReadyOnly setup readonly flag
func WithTxnReadyOnly() TxnOption {
	return func(tc *txnOperator) {
		tc.options = tc.options.WithReadOnly()
	}
}

// WithTxnDisable1PCOpt disable 1pc opt on distributed transaction. By default, mo enables 1pc
// optimization for distributed transactions. For write operations, if all partitions' prepares are
// executed successfully, then the transaction is considered committed and returned directly to the
// client. Partitions' prepared data are committed asynchronously.
func WithTxnDisable1PCOpt() TxnOption {
	return func(tc *txnOperator) {
		tc.options = tc.options.WithDisable1PC()
	}
}

// WithTxnCNCoordinator set cn txn coordinator
func WithTxnCNCoordinator() TxnOption {
	return func(tc *txnOperator) {
		tc.coordinator = true
	}
}

// WithTxnLockService set txn lock service
func WithTxnLockService(lockService lockservice.LockService) TxnOption {
	return func(tc *txnOperator) {
		tc.lockService = lockService
	}
}

// WithTxnCreateBy set txn create by.
func WithTxnCreateBy(
	accountID uint32,
	userName string,
	sessionID string,
	connectionID uint32) TxnOption {
	return func(tc *txnOperator) {
		tc.options.CN = runtime.ProcessLevelRuntime().ServiceUUID()
		tc.options.SessionID = sessionID
		tc.options.ConnectionID = connectionID
		tc.options.AccountID = accountID
		tc.options.UserName = userName
	}
}

// WithTxnCacheWrite Set cache write requests, after each Write call, the request will not be sent
// to the TN node immediately, but stored in the Coordinator's memory, and the Coordinator will
// choose the right time to send the cached requests. The following scenarios trigger the sending
// of requests to DN:
//  1. Before read, because the Coordinator is not aware of the format and content of the written data,
//     it is necessary to send the cached write requests to the corresponding TN node each time Read is
//     called, used to implement "read your write".
//  2. Before commit, obviously, the cached write requests needs to be sent to the corresponding TN node
//     before commit.
func WithTxnCacheWrite() TxnOption {
	return func(tc *txnOperator) {
		tc.options = tc.options.WithEnableCacheWrite()
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

// WithTxnSkipLock skip txn lock on specified tables
func WithTxnSkipLock(
	tables []uint64,
	modes []lock.LockMode) TxnOption {
	return func(tc *txnOperator) {
		tc.options.SkipLockTables = append(tc.options.SkipLockTables, tables...)
		tc.options.SkipLockTableModes = append(tc.options.SkipLockTableModes, modes...)
	}
}

// WithTxnEnableCheckDup enable check duplicate before commit to TN
func WithTxnEnableCheckDup() TxnOption {
	return func(tc *txnOperator) {
		tc.options = tc.options.WithEnableCheckDup()
	}
}

func WithDisableTrace(value bool) TxnOption {
	return func(tc *txnOperator) {
		if value {
			tc.options = tc.options.WithDisableTrace()
		}
	}
}

func WithSessionInfo(info string) TxnOption {
	return func(tc *txnOperator) {
		tc.options.SessionInfo = info
	}
}

type txnOperator struct {
	sender               rpc.TxnSender
	waiter               *waiter
	txnID                []byte
	coordinator          bool
	options              txn.TxnOptions
	cannotCleanWorkspace bool
	workspace            Workspace
	timestampWaiter      TimestampWaiter
	clock                clock.Clock
	createAt             time.Time
	commitAt             time.Time
	commitSeq            uint64
	lockService          lockservice.LockService
	sequence             atomic.Uint64
	createTs             timestamp.Timestamp
	//read-only txn operators for supporting snapshot read feature.
	children []*txnOperator
	parent   atomic.Pointer[txnOperator]

	mu struct {
		sync.RWMutex
		waitActive   bool
		closed       bool
		txn          txn.TxnMeta
		cachedWrites map[uint64][]txn.TxnRequest
		lockTables   []lock.LockTable
		callbacks    map[EventType][]func(TxnEvent)
		retry        bool
		lockSeq      uint64
		waitLocks    map[uint64]Lock
	}

	commitCounter   counter
	rollbackCounter counter
	runSqlCounter   counter

	waitActiveCost time.Duration
}

func newTxnOperator(
	clock clock.Clock,
	sender rpc.TxnSender,
	txnMeta txn.TxnMeta,
	options ...TxnOption) *txnOperator {
	tc := &txnOperator{sender: sender}
	tc.mu.txn = txnMeta
	tc.txnID = txnMeta.ID
	tc.clock = clock
	tc.createAt = time.Now()
	tc.createTs, _ = clock.Now()
	for _, opt := range options {
		opt(tc)
	}
	tc.adjust()
	util.LogTxnCreated(tc.mu.txn)

	if tc.options.UserTxn() {
		v2.TxnUserCounter.Inc()
	} else {
		v2.TxnInternalCounter.Inc()
	}
	return tc
}

func (tc *txnOperator) IsSnapOp() bool {
	return tc.parent.Load() != nil
}

func (tc *txnOperator) CloneSnapshotOp(snapshot timestamp.Timestamp) TxnOperator {
	op := &txnOperator{}
	op.mu.txn = txn.TxnMeta{
		SnapshotTS: snapshot,
		ID:         tc.mu.txn.ID,
		TNShards:   tc.mu.txn.TNShards,
	}
	op.txnID = op.mu.txn.ID

	op.workspace = tc.workspace.CloneSnapshotWS()
	op.workspace.BindTxnOp(op)

	tc.children = append(tc.children, op)
	op.parent.Store(tc)
	return op
}

func newTxnOperatorWithSnapshot(
	sender rpc.TxnSender,
	snapshot []byte) (*txnOperator, error) {
	v := &txn.CNTxnSnapshot{}
	if err := v.Unmarshal(snapshot); err != nil {
		return nil, err
	}

	tc := &txnOperator{sender: sender}
	tc.txnID = v.Txn.ID
	tc.options = v.Options
	tc.mu.txn = v.Txn
	tc.mu.txn.Mirror = true
	tc.mu.lockTables = v.LockTables

	tc.adjust()
	util.LogTxnCreated(tc.mu.txn)
	return tc, nil
}

func (tc *txnOperator) setWaitActive(v bool) {
	tc.mu.Lock()
	defer tc.mu.Unlock()
	tc.mu.waitActive = v
}

func (tc *txnOperator) waitActive(ctx context.Context) error {
	if tc.waiter == nil {
		return nil
	}

	tc.setWaitActive(true)
	defer func() {
		tc.waiter.close()
		tc.setWaitActive(false)
	}()

	cost, err := tc.doCostAction(
		time.Time{},
		WaitActiveEvent,
		func() error {
			return tc.waiter.wait(ctx)
		},
		false)
	tc.waitActiveCost = cost
	v2.TxnWaitActiveDurationHistogram.Observe(cost.Seconds())
	return err
}

func (tc *txnOperator) GetWaitActiveCost() time.Duration {
	return tc.waitActiveCost
}

func (tc *txnOperator) notifyActive() {
	if tc.waiter == nil {
		panic("BUG: notify active on non-waiter txn operator")
	}
	defer tc.waiter.close()
	tc.waiter.notify()
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
	if tc.options.ReadOnly() && tc.options.CacheWriteEnabled() {
		util.GetLogger().Fatal("readyOnly and delayWrites cannot both be set")
	}
}

func (tc *txnOperator) Txn() txn.TxnMeta {
	return tc.getTxnMeta(false)
}

func (tc *txnOperator) TxnRef() *txn.TxnMeta {
	tc.mu.RLock()
	defer tc.mu.RUnlock()
	return &tc.mu.txn
}

func (tc *txnOperator) SnapshotTS() timestamp.Timestamp {
	tc.mu.RLock()
	defer tc.mu.RUnlock()
	return tc.mu.txn.SnapshotTS
}

func (tc *txnOperator) CreateTS() timestamp.Timestamp {
	return tc.createTs
}

func (tc *txnOperator) Status() txn.TxnStatus {
	tc.mu.RLock()
	defer tc.mu.RUnlock()
	return tc.mu.txn.Status
}

func (tc *txnOperator) Snapshot() ([]byte, error) {
	tc.mu.Lock()
	defer tc.mu.Unlock()

	if err := tc.checkStatus(true); err != nil {
		return nil, err
	}
	snapshot := &txn.CNTxnSnapshot{
		Txn:        tc.mu.txn,
		LockTables: tc.mu.lockTables,
		Options:    tc.options,
	}
	return snapshot.Marshal()
}

func (tc *txnOperator) UpdateSnapshot(
	ctx context.Context,
	ts timestamp.Timestamp) error {
	tc.mu.Lock()
	defer tc.mu.Unlock()
	if err := tc.checkStatus(true); err != nil {
		return err
	}

	// ony push model support RC isolation
	if tc.timestampWaiter == nil {
		return nil
	}

	_, err := tc.doCostAction(
		time.Time{},
		UpdateSnapshotEvent,
		func() error {
			var err error
			tc.mu.txn.SnapshotTS, err = tc.timestampWaiter.GetTimestamp(
				ctx,
				ts)
			return err
		},
		true)
	return err
}

func (tc *txnOperator) ApplySnapshot(data []byte) error {
	if !tc.coordinator {
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

	for _, tn := range snapshot.Txn.TNShards {
		has := false
		for _, v := range tc.mu.txn.TNShards {
			if v.ShardID == tn.ShardID {
				has = true
				break
			}
		}

		if !has {
			tc.mu.txn.TNShards = append(tc.mu.txn.TNShards, tn)
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

	requests = tc.maybeInsertCachedWrites(requests, false)
	return tc.trimResponses(tc.handleError(tc.doSend(ctx, requests, false)))
}

func (tc *txnOperator) Write(ctx context.Context, requests []txn.TxnRequest) (*rpc.SendResult, error) {
	util.LogTxnWrite(tc.getTxnMeta(false))
	return tc.doWrite(ctx, requests, false)
}

func (tc *txnOperator) WriteAndCommit(ctx context.Context, requests []txn.TxnRequest) (*rpc.SendResult, error) {
	util.LogTxnWrite(tc.getTxnMeta(false))
	util.LogTxnCommit(tc.getTxnMeta(false))
	return tc.doWrite(ctx, requests, true)
}

func (tc *txnOperator) Commit(ctx context.Context) (err error) {
	tc.commitCounter.addEnter()
	defer tc.commitCounter.addExit()
	txn := tc.getTxnMeta(false)
	util.LogTxnCommit(txn)

	tc.commitSeq = tc.NextSequence()
	tc.commitAt = time.Now()

	tc.triggerEvent(newEvent(CommitEvent, txn, tc.commitSeq, nil))
	defer func() {
		cost := time.Since(tc.commitAt)
		v2.TxnCNCommitDurationHistogram.Observe(cost.Seconds())
		tc.triggerEvent(newCostEvent(CommitEvent, tc.getTxnMeta(false), tc.commitSeq, err, cost))
	}()

	if tc.options.ReadOnly() {
		tc.mu.Lock()
		defer tc.mu.Unlock()
		tc.closeLocked()
		return
	}

	result, e := tc.doWrite(ctx, nil, true)
	if e != nil {
		err = e
		return
	}

	if result != nil {
		result.Release()
	}
	return
}

func (tc *txnOperator) Rollback(ctx context.Context) (err error) {
	tc.rollbackCounter.addEnter()
	defer tc.rollbackCounter.addExit()
	v2.TxnRollbackCounter.Inc()
	txnMeta := tc.getTxnMeta(false)
	util.LogTxnRollback(txnMeta)

	if tc.workspace != nil && !tc.cannotCleanWorkspace {
		if err = tc.workspace.Rollback(ctx); err != nil {
			util.GetLogger().Error("rollback workspace failed",
				util.TxnIDField(txnMeta), zap.Error(err))
		}
	}

	tc.mu.Lock()
	defer tc.mu.Unlock()

	if tc.mu.closed {
		return nil
	}

	seq := tc.NextSequence()
	start := time.Now()
	tc.triggerEventLocked(newEvent(RollbackEvent, txnMeta, seq, nil))
	defer func() {
		cost := time.Since(start)
		tc.triggerEventLocked(newCostEvent(RollbackEvent, txnMeta, seq, err, cost))
	}()

	defer func() {
		tc.mu.txn.Status = txn.TxnStatus_Aborted
		tc.closeLocked()
	}()

	if tc.needUnlockLocked() {
		defer tc.unlock(ctx)
	}

	if len(tc.mu.txn.TNShards) == 0 {
		return nil
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

	// mirror txn can not check status, and the txn's status is on the creation cn of the txn.
	if !tc.mu.txn.Mirror {
		if err := tc.checkStatus(true); err != nil {
			return err
		}
	}

	return tc.doAddLockTableLocked(value)
}

func (tc *txnOperator) ResetRetry(retry bool) {
	tc.mu.Lock()
	defer tc.mu.Unlock()
	tc.mu.retry = retry
}

func (tc *txnOperator) IsRetry() bool {
	tc.mu.RLock()
	defer tc.mu.RUnlock()
	return tc.mu.retry
}

func (tc *txnOperator) doAddLockTableLocked(value lock.LockTable) error {
	for _, l := range tc.mu.lockTables {
		if l.Group == value.Group &&
			l.Table == value.Table {
			if l.Changed(value) {
				return moerr.NewLockTableBindChangedNoCtx()
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

	requests = tc.maybeInsertCachedWrites(requests, false)
	return tc.trimResponses(tc.handleError(tc.doSend(ctx, requests, false)))
}

func (tc *txnOperator) doWrite(ctx context.Context, requests []txn.TxnRequest, commit bool) (*rpc.SendResult, error) {
	for idx := range requests {
		requests[idx].Method = txn.TxnMethod_Write
	}

	if tc.options.ReadOnly() {
		util.GetLogger().Fatal("can not write on ready only transaction")
	}
	var payload []txn.TxnRequest
	if commit {
		if tc.workspace != nil {
			reqs, err := tc.workspace.Commit(ctx)
			if err != nil {
				return nil, errors.Join(err, tc.Rollback(ctx))
			}
			payload = reqs
		}
		tc.mu.Lock()
		defer func() {
			tc.closeLocked()
			tc.mu.Unlock()
		}()
		if tc.mu.closed {
			return nil, moerr.NewTxnClosedNoCtx(tc.txnID)
		}

		if tc.needUnlockLocked() {
			tc.mu.txn.LockTables = tc.mu.lockTables
			defer tc.unlock(ctx)
		}
	}

	if err := tc.validate(ctx, commit); err != nil {
		return nil, err
	}

	var txnReqs []*txn.TxnRequest
	if payload != nil {
		v2.TxnCNCommitCounter.Inc()
		for i := range payload {
			payload[i].Txn = tc.getTxnMeta(true)
			txnReqs = append(txnReqs, &payload[i])
		}
		tc.updateWritePartitions(payload, commit)
	}

	tc.updateWritePartitions(requests, commit)

	// delayWrites enabled, no responses
	if !commit && tc.maybeCacheWrites(requests, commit) {
		return nil, nil
	}

	if commit {
		if len(tc.mu.txn.TNShards) == 0 { // commit no write handled txn
			tc.mu.txn.Status = txn.TxnStatus_Committed
			return nil, nil
		}

		requests = tc.maybeInsertCachedWrites(requests, true)
		requests = append(requests, txn.TxnRequest{
			Method: txn.TxnMethod_Commit,
			Flag:   txn.SkipResponseFlag,
			CommitRequest: &txn.TxnCommitRequest{
				Payload:       txnReqs,
				Disable1PCOpt: tc.options.Is1PCDisabled(),
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

func (tc *txnOperator) addPartitionLocked(tn metadata.TNShard) {
	for idx := range tc.mu.txn.TNShards {
		if tc.mu.txn.TNShards[idx].ShardID == tn.ShardID {
			return
		}
	}
	tc.mu.txn.TNShards = append(tc.mu.txn.TNShards, tn)
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
	if tc.options.CacheWriteEnabled() {
		if !locked {
			tc.mu.Lock()
			defer tc.mu.Unlock()
		}

		for idx := range requests {
			requests[idx].Flag |= txn.SkipResponseFlag
			tn := requests[idx].CNRequest.Target.ShardID
			tc.mu.cachedWrites[tn] = append(tc.mu.cachedWrites[tn], requests[idx])
		}
		return true
	}
	return false
}

func (tc *txnOperator) maybeInsertCachedWrites(
	requests []txn.TxnRequest,
	locked bool,
) []txn.TxnRequest {
	if len(requests) == 0 ||
		!tc.options.CacheWriteEnabled() {
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
		tn := requests[idx].CNRequest.Target.ShardID
		if writes, ok := tc.getCachedWritesLocked(tn); ok {
			if !hasCachedWrites {
				// copy all requests into newRequests if cached writes encountered
				newRequests = append([]txn.TxnRequest(nil), requests[:idx]...)
			}
			newRequests = append(newRequests, writes...)
			tc.clearCachedWritesLocked(tn)
			hasCachedWrites = true
			insertCount += len(writes)
		}
		if hasCachedWrites {
			newRequests = append(newRequests, requests[idx])
		}
	}
	return newRequests
}

func (tc *txnOperator) getCachedWritesLocked(tn uint64) ([]txn.TxnRequest, bool) {
	writes, ok := tc.mu.cachedWrites[tn]
	if !ok || len(writes) == 0 {
		return nil, false
	}
	return writes, true
}

func (tc *txnOperator) clearCachedWritesLocked(tn uint64) {
	delete(tc.mu.cachedWrites, tn)
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
		tc.triggerEventLocked(
			newCostEvent(
				CommitResponseEvent,
				tc.mu.txn,
				tc.commitSeq,
				nil,
				time.Since(tc.commitAt)))

		if err := tc.checkResponseTxnStatusForCommit(resp); err != nil {
			return err
		}

		err := tc.checkTxnError(resp.TxnError, commitTxnErrors)
		if err == nil || !tc.mu.txn.IsPessimistic() {
			return err
		}

		// commit failed, refresh invalid lock tables
		if moerr.IsMoErrCode(err, moerr.ErrLockTableBindChanged) {
			tc.lockService.ForceRefreshLockTableBinds(
				resp.CommitResponse.InvalidLockTables,
				func(bind lock.LockTable) bool {
					for _, hold := range tc.mu.lockTables {
						if hold.Table == bind.Table && !hold.Changed(bind) {
							return true
						}
					}
					return false
				})
		}

		if moerr.IsMoErrCode(err, moerr.ErrTxnWWConflict) ||
			moerr.IsMoErrCode(err, moerr.ErrDuplicateEntry) {
			v, ok := runtime.ProcessLevelRuntime().GetGlobalVariables(runtime.EnableCheckInvalidRCErrors)
			if ok && v.(bool) {
				util.GetLogger().Fatal("failed",
					zap.Error(err),
					zap.String("txn", hex.EncodeToString(tc.txnID)))
			}
		}
		return err
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
		return moerr.NewNotSupportedNoCtx("unknown txn response method: %s", resp.DebugString())
	}
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
	if txnCode == moerr.ErrTNShardNotFound {
		// do we still have the uuid and shard id?
		return moerr.NewTNShardNotFoundNoCtx("", 0xDEADBEAF)
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
		panic(moerr.NewInternalErrorNoCtx("invalid response status for commit, %v", txnMeta.Status))
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
		panic(moerr.NewInternalErrorNoCtx("invalid response status for rollback %v", txnMeta.Status))
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
	if !tc.commitAt.IsZero() {
		v2.TxnCNCommitResponseDurationHistogram.Observe(float64(time.Since(tc.commitAt).Seconds()))
	}

	// rc mode need to see the committed value, so wait logtail applied
	if tc.mu.txn.IsRCIsolation() &&
		tc.timestampWaiter != nil {
		cost, err := tc.doCostAction(
			time.Time{},
			CommitWaitApplyEvent,
			func() error {
				_, err := tc.timestampWaiter.GetTimestamp(ctx, tc.mu.txn.CommitTS)
				return err
			},
			true)
		v2.TxnCNCommitWaitLogtailDurationHistogram.Observe(cost.Seconds())

		if err != nil {
			util.GetLogger().Error("txn wait committed log applied failed in rc mode",
				util.TxnField(tc.mu.txn),
				zap.Error(err))
		}
	}

	_, err := tc.doCostAction(
		time.Time{},
		UnlockEvent,
		func() error {
			return tc.lockService.Unlock(
				ctx,
				tc.mu.txn.ID,
				tc.mu.txn.CommitTS)
		},
		true)
	if err != nil {
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
	return tc.lockService != nil
}

func (tc *txnOperator) closeLocked() {
	if !tc.mu.closed {
		tc.mu.closed = true
		tc.triggerEventLocked(
			TxnEvent{
				Event: ClosedEvent,
				Txn:   tc.mu.txn,
			})
	}
}

func (tc *txnOperator) AddWaitLock(tableID uint64, rows [][]byte, opt lock.LockOptions) uint64 {
	tc.mu.Lock()
	defer tc.mu.Unlock()
	if tc.mu.waitLocks == nil {
		tc.mu.waitLocks = make(map[uint64]Lock)
	}

	seq := tc.mu.lockSeq
	tc.mu.lockSeq++

	tc.mu.waitLocks[seq] = Lock{
		TableID: tableID,
		Rows:    rows,
		Options: opt,
	}
	return seq
}

func (tc *txnOperator) RemoveWaitLock(key uint64) {
	tc.mu.Lock()
	defer tc.mu.Unlock()

	delete(tc.mu.waitLocks, key)
}

func (tc *txnOperator) GetOverview() TxnOverview {
	tc.mu.RLock()
	defer tc.mu.RUnlock()

	return TxnOverview{
		CreateAt:  tc.createAt,
		Meta:      tc.mu.txn,
		UserTxn:   tc.options.UserTxn(),
		WaitLocks: tc.getWaitLocksLocked(),
	}
}

func (tc *txnOperator) getWaitLocksLocked() []Lock {
	if tc.mu.waitLocks == nil {
		return nil
	}

	values := make([]Lock, 0, len(tc.mu.waitLocks))
	for _, l := range tc.mu.waitLocks {
		values = append(values, l)
	}
	return values
}

func (tc *txnOperator) LockSkipped(
	tableID uint64,
	mode lock.LockMode) bool {
	if len(tc.options.SkipLockTables) == 0 {
		return false
	}
	for i, id := range tc.options.SkipLockTables {
		if id == tableID &&
			mode == tc.options.SkipLockTableModes[i] {
			return true
		}
	}
	return false
}

func (tc *txnOperator) TxnOptions() txn.TxnOptions {
	return tc.options
}

func (tc *txnOperator) NextSequence() uint64 {
	return tc.sequence.Add(1)
}

func (tc *txnOperator) doCostAction(
	startAt time.Time,
	event EventType,
	action func() error,
	locked bool) (time.Duration, error) {
	if !locked {
		tc.mu.RLock()
		defer tc.mu.RUnlock()
	}

	seq := tc.NextSequence()
	if startAt == (time.Time{}) {
		startAt = time.Now()
	}

	tc.triggerEventLocked(
		newEvent(
			event,
			tc.mu.txn,
			seq,
			nil))

	err := action()
	cost := time.Since(startAt)
	tc.triggerEventLocked(
		newCostEvent(
			event,
			tc.mu.txn,
			seq,
			err,
			time.Since(startAt)))
	return cost, err
}

func (tc *txnOperator) EnterRunSql() {
	tc.runSqlCounter.addEnter()
}

func (tc *txnOperator) ExitRunSql() {
	tc.runSqlCounter.addExit()
}

func (tc *txnOperator) inRunSql() bool {
	return tc.runSqlCounter.more()
}

func (tc *txnOperator) inCommit() bool {
	return tc.commitCounter.more()
}

func (tc *txnOperator) inRollback() bool {
	return tc.rollbackCounter.more()
}

func (tc *txnOperator) counter() string {
	return fmt.Sprintf("commit: %s rollback: %s runSql: %s",
		tc.commitCounter.String(),
		tc.rollbackCounter.String(),
		tc.runSqlCounter.String())
}
