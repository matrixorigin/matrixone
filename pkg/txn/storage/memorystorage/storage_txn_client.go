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

package memorystorage

import (
	"context"
	"fmt"
	"time"

	"github.com/google/uuid"

	"github.com/matrixorigin/matrixone/pkg/common/moerr"
	"github.com/matrixorigin/matrixone/pkg/pb/lock"
	"github.com/matrixorigin/matrixone/pkg/pb/timestamp"
	"github.com/matrixorigin/matrixone/pkg/pb/txn"
	"github.com/matrixorigin/matrixone/pkg/txn/client"
	"github.com/matrixorigin/matrixone/pkg/txn/clock"
	"github.com/matrixorigin/matrixone/pkg/txn/rpc"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/memoryengine"
)

type StorageTxnClient struct {
	clock    clock.Clock
	storages map[string]*Storage
}

func NewStorageTxnClient(
	clock clock.Clock,
	storages map[string]*Storage,
) *StorageTxnClient {
	return &StorageTxnClient{
		clock:    clock,
		storages: storages,
	}
}

var _ client.TxnClient = new(StorageTxnClient)

func (s *StorageTxnClient) New(
	ctx context.Context,
	ts timestamp.Timestamp,
	options ...client.TxnOption) (client.TxnOperator, error) {
	now, _ := s.clock.Now()
	uid, _ := uuid.NewV7()
	meta := txn.TxnMeta{
		ID:         []byte(uid.String()),
		SnapshotTS: now,
	}
	return &StorageTxnOperator{
		storages: s.storages,
		meta:     meta,
	}, nil
}

func (s *StorageTxnClient) GetState() client.TxnState {
	panic("unimplemented")
}

func (s *StorageTxnClient) IterTxns(func(client.TxnOverview) bool) {
	panic("unimplemented")
}

func (*StorageTxnClient) NewWithSnapshot(snapshot []byte) (client.TxnOperator, error) {
	panic("unimplemented")
}

func (*StorageTxnClient) AbortAllRunningTxn() {
	panic("unimplemented")
}

func (*StorageTxnClient) Close() error {
	return nil
}

func (*StorageTxnClient) MinTimestamp() timestamp.Timestamp {
	return timestamp.Timestamp{}
}

func (*StorageTxnClient) WaitLogTailAppliedAt(
	ctx context.Context,
	ts timestamp.Timestamp) (timestamp.Timestamp, error) {
	return timestamp.Timestamp{}, nil
}

func (*StorageTxnClient) GetLatestCommitTS() timestamp.Timestamp { panic("unimplemented") }
func (*StorageTxnClient) SyncLatestCommitTS(timestamp.Timestamp) { panic("unimplemented") }
func (*StorageTxnClient) GetSyncLatestCommitTSTimes() uint64     { panic("unimplemented") }
func (*StorageTxnClient) Pause()                                 { panic("unimplemented") }
func (*StorageTxnClient) Resume()                                { panic("unimplemented") }
func (*StorageTxnClient) RefreshExpressionEnabled() bool         { panic("unimplemented") }
func (*StorageTxnClient) CNBasedConsistencyEnabled() bool        { panic("unimplemented") }

type StorageTxnOperator struct {
	storages map[string]*Storage
	meta     txn.TxnMeta
}

func (s *StorageTxnOperator) IsSnapOp() bool {
	panic("unimplemented")
}

func (s *StorageTxnOperator) CloneSnapshotOp(snapshot timestamp.Timestamp) client.TxnOperator {
	panic("unimplemented")
}

func (s *StorageTxnOperator) EnterRunSql() {
	//TODO implement me
	panic("implement me")
}

func (s *StorageTxnOperator) ExitRunSql() {
	//TODO implement me
	panic("implement me")
}

var _ client.TxnOperator = new(StorageTxnOperator)

func (s *StorageTxnOperator) AddWorkspace(_ client.Workspace) {
	panic("unimplemented")
}

func (s *StorageTxnOperator) GetWorkspace() client.Workspace {
	panic("unimplemented")
}

func (s *StorageTxnOperator) ApplySnapshot(data []byte) error {
	panic("unimplemented")
}

func (s *StorageTxnOperator) ResetRetry(retry bool) {
	panic("unimplemented")
}

func (s *StorageTxnOperator) IsRetry() bool {
	panic("unimplemented")
}

func (s *StorageTxnOperator) SetOpenLog(retry bool) {
	panic("unimplemented")
}

func (s *StorageTxnOperator) IsOpenLog() bool {
	panic("unimplemented")
}

func (s *StorageTxnOperator) AppendEventCallback(event client.EventType, callbacks ...func(client.TxnEvent)) {
	panic("unimplemented")
}

func (s *StorageTxnOperator) Debug(ctx context.Context, ops []txn.TxnRequest) (*rpc.SendResult, error) {
	panic("unimplemented")
}

func (s *StorageTxnOperator) Commit(ctx context.Context) error {
	for _, storage := range s.storages {
		if _, err := storage.Commit(ctx, s.meta); err != nil {
			return err
		}
	}
	return nil
}

func (s *StorageTxnOperator) Read(ctx context.Context, ops []txn.TxnRequest) (*rpc.SendResult, error) {

	// set op txn meta
	for i := range ops {
		ops[i].Txn = s.meta
	}

	result := &rpc.SendResult{}
	for _, op := range ops {
		txnResponse := txn.TxnResponse{
			RequestID:    op.RequestID,
			Txn:          &op.Txn,
			Method:       op.Method,
			Flag:         op.Flag,
			CNOpResponse: new(txn.CNOpResponse),
		}
		storage, ok := s.storages[op.CNRequest.Target.Address]
		if !ok {
			panic(fmt.Sprintf("storage not found at %s", op.CNRequest.Target.Address))
		}
		res, err := storage.Read(
			ctx,
			op.Txn,
			op.CNRequest.OpCode,
			op.CNRequest.Payload,
		)
		if err != nil {
			txnResponse.TxnError = txn.WrapError(err, moerr.ErrTAERead)
		} else {
			payload, err := res.Read()
			if err != nil {
				panic(err)
			}
			txnResponse.CNOpResponse.Payload = payload
			res.Release()
		}
		result.Responses = append(result.Responses, txnResponse)
	}
	return result, nil
}

func (s *StorageTxnOperator) Rollback(ctx context.Context) error {
	for _, storage := range s.storages {
		if err := storage.Rollback(ctx, s.meta); err != nil {
			return err
		}
	}
	return nil
}

func (*StorageTxnOperator) Snapshot() ([]byte, error) {
	panic("unimplemented")
}

func (s *StorageTxnOperator) Txn() txn.TxnMeta {
	return s.meta
}

func (s *StorageTxnOperator) SnapshotTS() timestamp.Timestamp {
	panic("unimplemented")
}

func (s *StorageTxnOperator) CreateTS() timestamp.Timestamp {
	panic("unimplemented")
}

func (s *StorageTxnOperator) Status() txn.TxnStatus {
	panic("unimplemented")
}

func (s *StorageTxnOperator) PKDedupCount() int {
	panic("unimplemented")
}

func (s *StorageTxnOperator) TxnRef() *txn.TxnMeta {
	return &s.meta
}

func (s *StorageTxnOperator) Write(ctx context.Context, ops []txn.TxnRequest) (*rpc.SendResult, error) {

	// set op txn meta
	for i := range ops {
		ops[i].Txn = s.meta
	}

	result := &rpc.SendResult{}
	for _, op := range ops {
		txnResponse := txn.TxnResponse{
			RequestID:    op.RequestID,
			Txn:          &op.Txn,
			Method:       op.Method,
			Flag:         op.Flag,
			CNOpResponse: new(txn.CNOpResponse),
		}
		storage, ok := s.storages[op.CNRequest.Target.Address]
		if !ok {
			panic(fmt.Sprintf("storage not found at %s", op.CNRequest.Target.Address))
		}
		payload, err := storage.Write(
			ctx,
			op.Txn,
			op.CNRequest.OpCode,
			op.CNRequest.Payload,
		)
		if err != nil {
			txnResponse.TxnError = txn.WrapError(err, moerr.ErrTAEWrite)
		} else {
			txnResponse.CNOpResponse.Payload = payload
		}
		result.Responses = append(result.Responses, txnResponse)
	}
	return result, nil
}

func (s *StorageTxnOperator) WriteAndCommit(ctx context.Context, ops []txn.TxnRequest) (*rpc.SendResult, error) {

	// set op txn meta
	for i := range ops {
		ops[i].Txn = s.meta
	}

	result, err := s.Write(ctx, ops)
	if err != nil {
		return nil, err
	}
	if err := s.Commit(ctx); err != nil {
		return nil, err
	}
	return result, nil
}

var _ memoryengine.OperationHandlerProvider = new(StorageTxnOperator)

func (s *StorageTxnOperator) GetOperationHandler(shard memoryengine.Shard) (memoryengine.OperationHandler, txn.TxnMeta) {
	storage, ok := s.storages[shard.Address]
	if !ok {
		panic(fmt.Sprintf("storage not found at %s", shard.Address))
	}
	return storage.handler, s.meta
}

func (s *StorageTxnOperator) AddLockTable(lock.LockTable) error {
	panic("should not call")
}

func (s *StorageTxnOperator) UpdateSnapshot(ctx context.Context, ts timestamp.Timestamp) error {
	panic("should not call")
}

func (s *StorageTxnOperator) AddWaitLock(tableID uint64, rows [][]byte, opt lock.LockOptions) uint64 {
	panic("should not call")
}

func (s *StorageTxnOperator) RemoveWaitLock(key uint64) {
	panic("should not call")
}

func (s *StorageTxnOperator) LockTableCount() int32 {
	panic("should not call")
}

func (s *StorageTxnOperator) GetOverview() client.TxnOverview {
	panic("should not call")
}

func (s *StorageTxnOperator) LockSkipped(tableID uint64, mode lock.LockMode) bool {
	panic("should not call")
}

func (s *StorageTxnOperator) TxnOptions() txn.TxnOptions {
	panic("should not call")
}

func (s *StorageTxnOperator) NextSequence() uint64 {
	panic("should not call")
}

func (s *StorageTxnOperator) GetWaitActiveCost() time.Duration {
	return time.Duration(0)
}
