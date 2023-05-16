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
	"testing"
	"time"

	"github.com/fagongzi/util/protoc"
	"github.com/matrixorigin/matrixone/pkg/clusterservice"
	"github.com/matrixorigin/matrixone/pkg/common/moerr"
	"github.com/matrixorigin/matrixone/pkg/common/runtime"
	"github.com/matrixorigin/matrixone/pkg/lockservice"
	"github.com/matrixorigin/matrixone/pkg/pb/lock"
	"github.com/matrixorigin/matrixone/pkg/pb/metadata"
	"github.com/matrixorigin/matrixone/pkg/pb/timestamp"
	"github.com/matrixorigin/matrixone/pkg/pb/txn"
	"github.com/matrixorigin/matrixone/pkg/txn/rpc"
	"github.com/stretchr/testify/assert"
)

func TestRead(t *testing.T) {
	runOperatorTests(t, func(ctx context.Context, tc *txnOperator, _ *testTxnSender) {
		result, err := tc.Read(ctx, []txn.TxnRequest{newDNRequest(1, 1), newDNRequest(2, 2)})
		assert.NoError(t, err)
		assert.Equal(t, 2, len(result.Responses))
		assert.Equal(t, []byte("r-1"), result.Responses[0].CNOpResponse.Payload)
		assert.Equal(t, []byte("r-2"), result.Responses[1].CNOpResponse.Payload)
	})
}

func TestWrite(t *testing.T) {
	runOperatorTests(t, func(ctx context.Context, tc *txnOperator, _ *testTxnSender) {
		assert.Empty(t, tc.mu.txn.DNShards)
		result, err := tc.Write(ctx, []txn.TxnRequest{newDNRequest(1, 1), newDNRequest(2, 2)})
		assert.NoError(t, err)
		assert.Equal(t, 2, len(result.Responses))
		assert.Equal(t, []byte("w-1"), result.Responses[0].CNOpResponse.Payload)
		assert.Equal(t, []byte("w-2"), result.Responses[1].CNOpResponse.Payload)

		assert.Equal(t, uint64(1), tc.mu.txn.DNShards[0].ShardID)
		assert.Equal(t, 2, len(tc.mu.txn.DNShards))
	})
}

func TestWriteWithCacheWriteEnabled(t *testing.T) {
	runOperatorTests(t, func(ctx context.Context, tc *txnOperator, ts *testTxnSender) {
		assert.Empty(t, tc.mu.txn.DNShards)
		responses, err := tc.Write(ctx, []txn.TxnRequest{newDNRequest(1, 1), newDNRequest(2, 2)})
		assert.NoError(t, err)
		assert.Empty(t, responses)
		assert.Equal(t, uint64(1), tc.mu.txn.DNShards[0].ShardID)
		assert.Equal(t, 2, len(tc.mu.txn.DNShards))
		assert.Empty(t, ts.getLastRequests())
	}, WithTxnCacheWrite())
}

func TestRollback(t *testing.T) {
	runOperatorTests(t, func(ctx context.Context, tc *txnOperator, ts *testTxnSender) {
		tc.mu.txn.DNShards = append(tc.mu.txn.DNShards, metadata.DNShard{DNShardRecord: metadata.DNShardRecord{ShardID: 1}})
		err := tc.Rollback(ctx)
		assert.NoError(t, err)

		requests := ts.getLastRequests()
		assert.Equal(t, 1, len(requests))
		assert.Equal(t, txn.TxnMethod_Rollback, requests[0].Method)
	})
}

func TestRollbackWithClosedTxn(t *testing.T) {
	runOperatorTests(t, func(ctx context.Context, tc *txnOperator, ts *testTxnSender) {
		ts.setManual(func(sr *rpc.SendResult, err error) (*rpc.SendResult, error) {
			return nil, moerr.NewTxnClosed(ctx, tc.txnID)
		})

		tc.mu.txn.DNShards = append(tc.mu.txn.DNShards, metadata.DNShard{DNShardRecord: metadata.DNShardRecord{ShardID: 1}})
		err := tc.Rollback(ctx)
		assert.NoError(t, err)
	})
}

func TestRollbackWithNoWrite(t *testing.T) {
	runOperatorTests(t, func(ctx context.Context, tc *txnOperator, ts *testTxnSender) {
		err := tc.Rollback(ctx)
		assert.NoError(t, err)
		assert.Empty(t, ts.getLastRequests())
	})
}

func TestRollbackReadOnly(t *testing.T) {
	runOperatorTests(t, func(ctx context.Context, tc *txnOperator, ts *testTxnSender) {
		err := tc.Rollback(ctx)
		assert.NoError(t, err)
		assert.Empty(t, ts.getLastRequests())
	}, WithTxnReadyOnly())
}

func TestCommit(t *testing.T) {
	runOperatorTests(t, func(ctx context.Context, tc *txnOperator, ts *testTxnSender) {
		tc.mu.txn.DNShards = append(tc.mu.txn.DNShards, metadata.DNShard{DNShardRecord: metadata.DNShardRecord{ShardID: 1}})
		err := tc.Commit(ctx)
		assert.NoError(t, err)
		assert.Equal(t, tc.mu.txn.SnapshotTS.Next(), tc.mu.txn.CommitTS)

		requests := ts.getLastRequests()
		assert.Equal(t, 1, len(requests))
		assert.Equal(t, txn.TxnMethod_Commit, requests[0].Method)
	})
}

func TestCommitWithNoWrite(t *testing.T) {
	runOperatorTests(t, func(ctx context.Context, tc *txnOperator, ts *testTxnSender) {
		err := tc.Commit(ctx)
		assert.NoError(t, err)
		assert.Empty(t, ts.getLastRequests())
	})
}

func TestCommitReadOnly(t *testing.T) {
	runOperatorTests(t, func(ctx context.Context, tc *txnOperator, ts *testTxnSender) {
		err := tc.Commit(ctx)
		assert.NoError(t, err)
		assert.Empty(t, ts.getLastRequests())
	}, WithTxnReadyOnly())
}

func TestCommitWithLockTables(t *testing.T) {
	runOperatorTests(t, func(ctx context.Context, tc *txnOperator, ts *testTxnSender) {
		r := runtime.DefaultRuntime()
		runtime.SetupProcessLevelRuntime(r)

		c := clusterservice.NewMOCluster(nil, time.Hour, clusterservice.WithDisableRefresh())
		defer c.Close()
		r.SetGlobalVariables(runtime.ClusterService, c)

		s := lockservice.NewLockService(lockservice.Config{ServiceID: "s1"})
		defer func() {
			assert.NoError(t, s.Close())
		}()

		tc.mu.txn.Mode = txn.TxnMode_Pessimistic
		tc.option.lockService = s
		tc.AddLockTable(lock.LockTable{Table: 1})
		tc.mu.txn.DNShards = append(tc.mu.txn.DNShards, metadata.DNShard{DNShardRecord: metadata.DNShardRecord{ShardID: 1}})
		err := tc.Commit(ctx)
		assert.NoError(t, err)

		requests := ts.getLastRequests()
		assert.Equal(t, 1, len(requests))
		assert.Equal(t, txn.TxnMethod_Commit, requests[0].Method)
		assert.Equal(t, 1, len(requests[0].Txn.LockTables))
	})
}

func TestContextWithoutDeadlineWillPanic(t *testing.T) {
	runOperatorTests(t, func(_ context.Context, tc *txnOperator, _ *testTxnSender) {
		defer func() {
			if err := recover(); err != nil {
				return
			}
			assert.Fail(t, "must panic")
		}()

		_, err := tc.Write(context.Background(), nil)
		assert.NoError(t, err)
	})
}

func TestMissingSenderWillPanic(t *testing.T) {
	defer func() {
		if err := recover(); err != nil {
			return
		}
		assert.Fail(t, "must panic")
	}()
	runtime.SetupProcessLevelRuntime(runtime.DefaultRuntime())
	newTxnOperator(nil, txn.TxnMeta{})
}

func TestMissingTxnIDWillPanic(t *testing.T) {
	defer func() {
		if err := recover(); err != nil {
			return
		}
		assert.Fail(t, "must panic")
	}()
	runtime.SetupProcessLevelRuntime(runtime.DefaultRuntime())
	newTxnOperator(newTestTxnSender(), txn.TxnMeta{})
}

func TestEmptyTxnSnapshotTSWillPanic(t *testing.T) {
	defer func() {
		if err := recover(); err != nil {
			return
		}
		assert.Fail(t, "must panic")
	}()
	runtime.SetupProcessLevelRuntime(runtime.DefaultRuntime())
	newTxnOperator(newTestTxnSender(), txn.TxnMeta{ID: []byte{1}})
}

func TestReadOnlyAndCacheWriteBothSetWillPanic(t *testing.T) {
	defer func() {
		if err := recover(); err != nil {
			return
		}
		assert.Fail(t, "must panic")
	}()
	runtime.SetupProcessLevelRuntime(runtime.DefaultRuntime())
	newTxnOperator(
		newTestTxnSender(),
		txn.TxnMeta{ID: []byte{1}, SnapshotTS: timestamp.Timestamp{PhysicalTime: 1}},
		WithTxnReadyOnly(),
		WithTxnCacheWrite())
}

func TestWriteOnReadyOnlyTxnWillPanic(t *testing.T) {
	runOperatorTests(t, func(ctx context.Context, tc *txnOperator, _ *testTxnSender) {
		defer func() {
			if err := recover(); err != nil {
				return
			}
			assert.Fail(t, "must panic")
		}()

		_, err := tc.Write(ctx, nil)
		assert.NoError(t, err)
	}, WithTxnReadyOnly())
}

func TestWriteOnClosedTxnWillPanic(t *testing.T) {
	runOperatorTests(t, func(ctx context.Context, tc *txnOperator, _ *testTxnSender) {
		tc.mu.closed = true
		_, err := tc.Write(ctx, nil)
		assert.True(t, moerr.IsMoErrCode(err, moerr.ErrTxnClosed))
	})
}

func TestReadOnClosedTxnWillPanic(t *testing.T) {
	runOperatorTests(t, func(ctx context.Context, tc *txnOperator, _ *testTxnSender) {
		tc.mu.closed = true
		_, err := tc.Read(ctx, nil)
		assert.True(t, moerr.IsMoErrCode(err, moerr.ErrTxnClosed))
	})
}

func TestCacheWrites(t *testing.T) {
	runOperatorTests(t, func(ctx context.Context, tc *txnOperator, _ *testTxnSender) {
		responses, err := tc.Write(ctx, []txn.TxnRequest{txn.NewTxnRequest(&txn.CNOpRequest{OpCode: 1})})
		assert.NoError(t, err)
		assert.Empty(t, responses)
		assert.Equal(t, 1, len(tc.mu.cachedWrites))
		assert.Equal(t, 1, len(tc.mu.cachedWrites[0]))
	}, WithTxnCacheWrite())
}

func TestCacheWritesWillInsertBeforeRead(t *testing.T) {
	runOperatorTests(t, func(ctx context.Context, tc *txnOperator, ts *testTxnSender) {
		result, err := tc.Write(ctx, []txn.TxnRequest{newDNRequest(1, 1), newDNRequest(2, 2), newDNRequest(3, 3)})
		assert.NoError(t, err)
		assert.Empty(t, result)
		assert.Equal(t, 3, len(tc.mu.cachedWrites))
		assert.Equal(t, 1, len(tc.mu.cachedWrites[1]))
		assert.Equal(t, 1, len(tc.mu.cachedWrites[2]))
		assert.Equal(t, 1, len(tc.mu.cachedWrites[3]))

		result, err = tc.Read(ctx, []txn.TxnRequest{newDNRequest(11, 1), newDNRequest(22, 2), newDNRequest(33, 3), newDNRequest(4, 4)})
		assert.NoError(t, err)
		assert.Equal(t, 4, len(result.Responses))
		assert.Equal(t, []byte("r-11"), result.Responses[0].CNOpResponse.Payload)
		assert.Equal(t, []byte("r-22"), result.Responses[1].CNOpResponse.Payload)
		assert.Equal(t, []byte("r-33"), result.Responses[2].CNOpResponse.Payload)
		assert.Equal(t, []byte("r-4"), result.Responses[3].CNOpResponse.Payload)

		requests := ts.getLastRequests()
		assert.Equal(t, 7, len(requests))
		assert.Equal(t, uint32(1), requests[0].CNRequest.OpCode)
		assert.Equal(t, uint32(11), requests[1].CNRequest.OpCode)
		assert.Equal(t, uint32(2), requests[2].CNRequest.OpCode)
		assert.Equal(t, uint32(22), requests[3].CNRequest.OpCode)
		assert.Equal(t, uint32(3), requests[4].CNRequest.OpCode)
		assert.Equal(t, uint32(33), requests[5].CNRequest.OpCode)
		assert.Equal(t, uint32(4), requests[6].CNRequest.OpCode)

		assert.Equal(t, 0, len(tc.mu.cachedWrites))
	}, WithTxnCacheWrite())
}

func TestReadOnAbortedTxn(t *testing.T) {
	runOperatorTests(t, func(ctx context.Context, tc *txnOperator, ts *testTxnSender) {
		ts.setManual(func(result *rpc.SendResult, err error) (*rpc.SendResult, error) {
			for idx := range result.Responses {
				result.Responses[idx].Txn = &txn.TxnMeta{Status: txn.TxnStatus_Aborted}
			}
			return result, err
		})
		responses, err := tc.Read(ctx, []txn.TxnRequest{txn.NewTxnRequest(&txn.CNOpRequest{OpCode: 1})})
		assert.True(t, moerr.IsMoErrCode(err, moerr.ErrTxnClosed))
		assert.Empty(t, responses)
	})
}

func TestWriteOnAbortedTxn(t *testing.T) {
	runOperatorTests(t, func(ctx context.Context, tc *txnOperator, ts *testTxnSender) {
		ts.setManual(func(result *rpc.SendResult, err error) (*rpc.SendResult, error) {
			for idx := range result.Responses {
				result.Responses[idx].Txn = &txn.TxnMeta{Status: txn.TxnStatus_Aborted}
			}
			return result, err
		})
		result, err := tc.Write(ctx, []txn.TxnRequest{txn.NewTxnRequest(&txn.CNOpRequest{OpCode: 1})})
		assert.True(t, moerr.IsMoErrCode(err, moerr.ErrTxnClosed))
		assert.Empty(t, result)
	})
}

func TestWriteOnCommittedTxn(t *testing.T) {
	runOperatorTests(t, func(ctx context.Context, tc *txnOperator, ts *testTxnSender) {
		ts.setManual(func(result *rpc.SendResult, err error) (*rpc.SendResult, error) {
			for idx := range result.Responses {
				result.Responses[idx].Txn = &txn.TxnMeta{Status: txn.TxnStatus_Committed}
			}
			return result, err
		})
		result, err := tc.Write(ctx, []txn.TxnRequest{txn.NewTxnRequest(&txn.CNOpRequest{OpCode: 1})})
		assert.True(t, moerr.IsMoErrCode(err, moerr.ErrTxnClosed))
		assert.Empty(t, result)
	})
}

func TestWriteOnCommittingTxn(t *testing.T) {
	runOperatorTests(t, func(ctx context.Context, tc *txnOperator, ts *testTxnSender) {
		ts.setManual(func(result *rpc.SendResult, err error) (*rpc.SendResult, error) {
			for idx := range result.Responses {
				result.Responses[idx].Txn = &txn.TxnMeta{Status: txn.TxnStatus_Committing}
			}
			return result, err
		})
		result, err := tc.Write(ctx, []txn.TxnRequest{txn.NewTxnRequest(&txn.CNOpRequest{OpCode: 1})})
		assert.True(t, moerr.IsMoErrCode(err, moerr.ErrTxnClosed))
		assert.Empty(t, result)
	})
}

func TestSnapshotTxnOperator(t *testing.T) {
	runOperatorTests(t, func(_ context.Context, tc *txnOperator, _ *testTxnSender) {
		assert.NoError(t, tc.AddLockTable(lock.LockTable{Table: 1}))

		v, err := tc.Snapshot()
		assert.NoError(t, err)

		tc2, err := newTxnOperatorWithSnapshot(tc.sender, v)
		assert.NoError(t, err)

		assert.Equal(t, tc.mu.txn, tc2.mu.txn)
		assert.False(t, tc2.option.coordinator)
		tc2.option.coordinator = true
		assert.Equal(t, tc.option, tc2.option)
		assert.Equal(t, 1, len(tc2.mu.lockTables))
	}, WithTxnReadyOnly(), WithTxnDisable1PCOpt())
}

func TestApplySnapshotTxnOperator(t *testing.T) {
	runOperatorTests(t, func(_ context.Context, tc *txnOperator, _ *testTxnSender) {
		snapshot := &txn.CNTxnSnapshot{}
		snapshot.Txn.ID = tc.mu.txn.ID
		assert.NoError(t, tc.ApplySnapshot(protoc.MustMarshal(snapshot)))
		assert.Equal(t, 0, len(tc.mu.txn.DNShards))

		snapshot.Txn.DNShards = append(snapshot.Txn.DNShards, metadata.DNShard{DNShardRecord: metadata.DNShardRecord{ShardID: 1}})
		assert.NoError(t, tc.ApplySnapshot(protoc.MustMarshal(snapshot)))
		assert.Equal(t, 1, len(tc.mu.txn.DNShards))

		snapshot.Txn.DNShards = append(snapshot.Txn.DNShards, metadata.DNShard{DNShardRecord: metadata.DNShardRecord{ShardID: 2}})
		assert.NoError(t, tc.ApplySnapshot(protoc.MustMarshal(snapshot)))
		assert.Equal(t, 2, len(tc.mu.txn.DNShards))

		snapshot.LockTables = append(snapshot.LockTables, lock.LockTable{Table: 1})
		assert.NoError(t, tc.ApplySnapshot(protoc.MustMarshal(snapshot)))
		assert.Equal(t, 1, len(tc.mu.lockTables))
	})
}

func TestDebugTxnOperator(t *testing.T) {
	runOperatorTests(t, func(ctx context.Context, tc *txnOperator, _ *testTxnSender) {
		responses, err := tc.Debug(ctx,
			[]txn.TxnRequest{txn.NewTxnRequest(&txn.CNOpRequest{OpCode: 1, Payload: []byte("OK")})})
		assert.NoError(t, err)
		assert.Equal(t, len(responses.Responses), 1)
		assert.Equal(t, responses.Responses[0].CNOpResponse.Payload, []byte("OK"))
	})
}

func TestAddLockTable(t *testing.T) {
	runOperatorTests(t, func(ctx context.Context, tc *txnOperator, _ *testTxnSender) {
		assert.NoError(t, tc.AddLockTable(lock.LockTable{Table: 1}))
		assert.Equal(t, 1, len(tc.mu.lockTables))

		// same lock table
		assert.NoError(t, tc.AddLockTable(lock.LockTable{Table: 1}))
		assert.Equal(t, 1, len(tc.mu.lockTables))

		// changed lock table
		assert.Error(t, tc.AddLockTable(lock.LockTable{Table: 1, Version: 2}))
	})
}

func runOperatorTests(t *testing.T, tc func(context.Context, *txnOperator, *testTxnSender), options ...TxnOption) {
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()

	RunTxnTests(func(
		c TxnClient,
		ts rpc.TxnSender) {
		txn, err := c.New(ctx, newTestTimestamp(0), options...)
		assert.Nil(t, err)
		tc(ctx, txn.(*txnOperator), ts.(*testTxnSender))
	})
}

func newDNRequest(op uint32, dn uint64) txn.TxnRequest {
	return txn.NewTxnRequest(&txn.CNOpRequest{
		OpCode: op,
		Target: metadata.DNShard{
			DNShardRecord: metadata.DNShardRecord{ShardID: dn},
		},
	})
}
