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

	"github.com/matrixorigin/matrixone/pkg/logutil"
	"github.com/matrixorigin/matrixone/pkg/pb/metadata"
	"github.com/matrixorigin/matrixone/pkg/pb/timestamp"
	"github.com/matrixorigin/matrixone/pkg/pb/txn"
	"github.com/stretchr/testify/assert"
)

func TestRead(t *testing.T) {
	runCoordinatorTests(func(ctx context.Context, tc *txnCoordinator, ts *testTxnSender) {
		respones, err := tc.Read(ctx, []txn.TxnRequest{newDNRequest(1, 1), newDNRequest(2, 2)})
		assert.NoError(t, err)
		assert.Equal(t, 2, len(respones))
		assert.Equal(t, []byte("r-1"), respones[0].CNOpResponse.Payload)
		assert.Equal(t, []byte("r-2"), respones[1].CNOpResponse.Payload)
	})
}

func TestWrite(t *testing.T) {
	runCoordinatorTests(func(ctx context.Context, tc *txnCoordinator, ts *testTxnSender) {
		assert.Empty(t, tc.mu.partitions)
		respones, err := tc.Write(ctx, []txn.TxnRequest{newDNRequest(1, 1), newDNRequest(2, 2)})
		assert.NoError(t, err)
		assert.Equal(t, 2, len(respones))
		assert.Equal(t, []byte("w-1"), respones[0].CNOpResponse.Payload)
		assert.Equal(t, []byte("w-2"), respones[1].CNOpResponse.Payload)

		assert.Equal(t, uint64(1), tc.mu.partitions[0].ID)
		assert.Equal(t, 2, len(tc.mu.partitions))
	})
}

func TestWriteWithCacheWriteEnabled(t *testing.T) {
	runCoordinatorTests(func(ctx context.Context, tc *txnCoordinator, ts *testTxnSender) {
		assert.Empty(t, tc.mu.partitions)
		respones, err := tc.Write(ctx, []txn.TxnRequest{newDNRequest(1, 1), newDNRequest(2, 2)})
		assert.NoError(t, err)
		assert.Empty(t, respones)
		assert.Equal(t, uint64(1), tc.mu.partitions[0].ID)
		assert.Equal(t, 2, len(tc.mu.partitions))
		assert.Empty(t, ts.getLastRequests())
	}, WithCacheWrite())
}

func TestRollback(t *testing.T) {
	runCoordinatorTests(func(ctx context.Context, tc *txnCoordinator, ts *testTxnSender) {
		tc.mu.partitions = append(tc.mu.partitions, metadata.DN{ID: 1})
		err := tc.Rollback(ctx)
		assert.NoError(t, err)

		requests := ts.getLastRequests()
		assert.Equal(t, 1, len(requests))
		assert.Equal(t, txn.TxnOp_Rollback, requests[0].Op)
		assert.Equal(t, 1, len(requests[0].RollbackRequest.Partitions))
		assert.Equal(t, metadata.DN{ID: 1}, requests[0].RollbackRequest.Partitions[0])
	})
}

func TestRollbackWithNoWrite(t *testing.T) {
	runCoordinatorTests(func(ctx context.Context, tc *txnCoordinator, ts *testTxnSender) {
		err := tc.Rollback(ctx)
		assert.NoError(t, err)
		assert.Empty(t, ts.getLastRequests())
	})
}

func TestRollbackReadOnly(t *testing.T) {
	runCoordinatorTests(func(ctx context.Context, tc *txnCoordinator, ts *testTxnSender) {
		err := tc.Rollback(ctx)
		assert.NoError(t, err)
		assert.Empty(t, ts.getLastRequests())
	}, WithReadyOnly())
}

func TestCommit(t *testing.T) {
	runCoordinatorTests(func(ctx context.Context, tc *txnCoordinator, ts *testTxnSender) {
		tc.mu.partitions = append(tc.mu.partitions, metadata.DN{ID: 1})
		err := tc.Commit(ctx)
		assert.NoError(t, err)

		requests := ts.getLastRequests()
		assert.Equal(t, 1, len(requests))
		assert.Equal(t, txn.TxnOp_Commit, requests[0].Op)
		assert.Equal(t, 1, len(requests[0].CommitRequest.Partitions))
		assert.Equal(t, metadata.DN{ID: 1}, requests[0].CommitRequest.Partitions[0])
	})
}

func TestCommitWithNoWrite(t *testing.T) {
	runCoordinatorTests(func(ctx context.Context, tc *txnCoordinator, ts *testTxnSender) {
		err := tc.Commit(ctx)
		assert.NoError(t, err)
		assert.Empty(t, ts.getLastRequests())
	})
}

func TestCommitReadOnly(t *testing.T) {
	runCoordinatorTests(func(ctx context.Context, tc *txnCoordinator, ts *testTxnSender) {
		err := tc.Commit(ctx)
		assert.NoError(t, err)
		assert.Empty(t, ts.getLastRequests())
	}, WithReadyOnly())
}

func TestContextWithoutDeadlineWillPanic(t *testing.T) {
	runCoordinatorTests(func(ctx context.Context, tc *txnCoordinator, ts *testTxnSender) {
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
	newTxnCoordinator(nil, txn.TxnMeta{}, WithTxnLogger(logutil.GetPanicLogger()))
}

func TestMissingTxnIDWillPanic(t *testing.T) {
	defer func() {
		if err := recover(); err != nil {
			return
		}
		assert.Fail(t, "must panic")
	}()
	newTxnCoordinator(newTestTxnSender(), txn.TxnMeta{}, WithTxnLogger(logutil.GetPanicLogger()))
}

func TestEmptyTxnSnapshotTSWillPanic(t *testing.T) {
	defer func() {
		if err := recover(); err != nil {
			return
		}
		assert.Fail(t, "must panic")
	}()
	newTxnCoordinator(newTestTxnSender(), txn.TxnMeta{ID: []byte{1}}, WithTxnLogger(logutil.GetPanicLogger()))
}

func TestReadOnlyAndCacheWriteBothSetWillPanic(t *testing.T) {
	defer func() {
		if err := recover(); err != nil {
			return
		}
		assert.Fail(t, "must panic")
	}()
	newTxnCoordinator(newTestTxnSender(),
		txn.TxnMeta{ID: []byte{1}, SnapshotTS: timestamp.Timestamp{PhysicalTime: 1}},
		WithTxnLogger(logutil.GetPanicLogger()),
		WithReadyOnly(),
		WithCacheWrite())
}

func TestWriteOnReadyOnlyTxnWillPanic(t *testing.T) {
	runCoordinatorTests(func(ctx context.Context, tc *txnCoordinator, ts *testTxnSender) {
		defer func() {
			if err := recover(); err != nil {
				return
			}
			assert.Fail(t, "must panic")
		}()

		_, err := tc.Write(ctx, nil)
		assert.NoError(t, err)
	}, WithReadyOnly())
}

func TestWriteOnClosedTxnWillPanic(t *testing.T) {
	runCoordinatorTests(func(ctx context.Context, tc *txnCoordinator, ts *testTxnSender) {
		tc.mu.closed = true
		_, err := tc.Write(ctx, nil)
		assert.Error(t, err)
		assert.Equal(t, errTxnClosed, err)
	})
}

func TestReadOnClosedTxnWillPanic(t *testing.T) {
	runCoordinatorTests(func(ctx context.Context, tc *txnCoordinator, ts *testTxnSender) {
		tc.mu.closed = true
		_, err := tc.Read(ctx, nil)
		assert.Error(t, err)
		assert.Equal(t, errTxnClosed, err)
	})
}

func TestCacheWrites(t *testing.T) {
	runCoordinatorTests(func(ctx context.Context, tc *txnCoordinator, ts *testTxnSender) {
		responses, err := tc.Write(ctx, []txn.TxnRequest{txn.NewTxnRequest(&txn.CNOpRequest{OpCode: 1})})
		assert.NoError(t, err)
		assert.Empty(t, responses)
		assert.Equal(t, 1, len(tc.mu.cachedWrites))
		assert.Equal(t, 1, len(tc.mu.cachedWrites[0]))
	}, WithCacheWrite())
}

func TestCacheWritesWillInsertBeforeRead(t *testing.T) {
	runCoordinatorTests(func(ctx context.Context, tc *txnCoordinator, ts *testTxnSender) {
		responses, err := tc.Write(ctx, []txn.TxnRequest{newDNRequest(1, 1), newDNRequest(2, 2), newDNRequest(3, 3)})
		assert.NoError(t, err)
		assert.Empty(t, responses)
		assert.Equal(t, 3, len(tc.mu.cachedWrites))
		assert.Equal(t, 1, len(tc.mu.cachedWrites[1]))
		assert.Equal(t, 1, len(tc.mu.cachedWrites[2]))
		assert.Equal(t, 1, len(tc.mu.cachedWrites[3]))

		responses, err = tc.Read(ctx, []txn.TxnRequest{newDNRequest(11, 1), newDNRequest(22, 2), newDNRequest(33, 3), newDNRequest(4, 4)})
		assert.NoError(t, err)
		assert.Equal(t, 4, len(responses))
		assert.Equal(t, []byte("r-11"), responses[0].CNOpResponse.Payload)
		assert.Equal(t, []byte("r-22"), responses[1].CNOpResponse.Payload)
		assert.Equal(t, []byte("r-33"), responses[2].CNOpResponse.Payload)
		assert.Equal(t, []byte("r-4"), responses[3].CNOpResponse.Payload)

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
	}, WithCacheWrite())
}

func TestReadOnAbortedTxn(t *testing.T) {
	runCoordinatorTests(func(ctx context.Context, tc *txnCoordinator, ts *testTxnSender) {
		ts.setManual(func(responses []txn.TxnResponse, err error) ([]txn.TxnResponse, error) {
			for idx := range responses {
				responses[idx].Txn = &txn.TxnMeta{Status: txn.TxnStatus_Aborted}
			}
			return responses, err
		})
		responses, err := tc.Read(ctx, []txn.TxnRequest{txn.NewTxnRequest(&txn.CNOpRequest{OpCode: 1})})
		assert.Error(t, err)
		assert.Equal(t, errTxnAborted, err)
		assert.Empty(t, responses)
	})
}

func TestWriteOnAbortedTxn(t *testing.T) {
	runCoordinatorTests(func(ctx context.Context, tc *txnCoordinator, ts *testTxnSender) {
		ts.setManual(func(responses []txn.TxnResponse, err error) ([]txn.TxnResponse, error) {
			for idx := range responses {
				responses[idx].Txn = &txn.TxnMeta{Status: txn.TxnStatus_Aborted}
			}
			return responses, err
		})
		responses, err := tc.Write(ctx, []txn.TxnRequest{txn.NewTxnRequest(&txn.CNOpRequest{OpCode: 1})})
		assert.Error(t, err)
		assert.Equal(t, errTxnAborted, err)
		assert.Empty(t, responses)
	})
}

func TestWriteOnCommittedTxn(t *testing.T) {
	runCoordinatorTests(func(ctx context.Context, tc *txnCoordinator, ts *testTxnSender) {
		ts.setManual(func(responses []txn.TxnResponse, err error) ([]txn.TxnResponse, error) {
			for idx := range responses {
				responses[idx].Txn = &txn.TxnMeta{Status: txn.TxnStatus_Committed}
			}
			return responses, err
		})
		responses, err := tc.Write(ctx, []txn.TxnRequest{txn.NewTxnRequest(&txn.CNOpRequest{OpCode: 1})})
		assert.Error(t, err)
		assert.Equal(t, errTxnClosed, err)
		assert.Empty(t, responses)
	})
}

func TestWriteOnCommittingTxn(t *testing.T) {
	runCoordinatorTests(func(ctx context.Context, tc *txnCoordinator, ts *testTxnSender) {
		ts.setManual(func(responses []txn.TxnResponse, err error) ([]txn.TxnResponse, error) {
			for idx := range responses {
				responses[idx].Txn = &txn.TxnMeta{Status: txn.TxnStatus_Committing}
			}
			return responses, err
		})
		responses, err := tc.Write(ctx, []txn.TxnRequest{txn.NewTxnRequest(&txn.CNOpRequest{OpCode: 1})})
		assert.Error(t, err)
		assert.Equal(t, errTxnClosed, err)
		assert.Empty(t, responses)
	})
}

func runCoordinatorTests(tc func(context.Context, *txnCoordinator, *testTxnSender), options ...TxnOption) {
	ts := newTestTxnSender()
	c := NewTxnClient(ts, WithLogger(logutil.GetPanicLogger()))
	txn := c.New(options...)
	ctx, cancel := context.WithTimeout(context.Background(), time.Millisecond)
	defer cancel()

	tc(ctx, txn.(*txnCoordinator), ts)
}

func newDNRequest(op uint32, dn uint64) txn.TxnRequest {
	return txn.NewTxnRequest(&txn.CNOpRequest{
		OpCode: op,
		Target: metadata.DN{
			ID: dn,
		},
	})
}
