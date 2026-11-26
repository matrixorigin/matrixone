// Copyright 2021-2024 Matrix Origin
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

package disttae

import (
	"context"
	"errors"
	"fmt"
	"strings"
	"testing"

	"github.com/golang/mock/gomock"
	"github.com/matrixorigin/matrixone/pkg/common/moerr"
	"github.com/matrixorigin/matrixone/pkg/common/mpool"
	"github.com/matrixorigin/matrixone/pkg/container/batch"
	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/container/vector"
	mock_frontend "github.com/matrixorigin/matrixone/pkg/frontend/test"
	"github.com/matrixorigin/matrixone/pkg/frontend/test/mock_shard"
	"github.com/matrixorigin/matrixone/pkg/objectio"
	"github.com/matrixorigin/matrixone/pkg/pb/timestamp"
	pbtxn "github.com/matrixorigin/matrixone/pkg/pb/txn"
	"github.com/matrixorigin/matrixone/pkg/shardservice"
	"github.com/matrixorigin/matrixone/pkg/vm/engine"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestShardingReaderOwnsBatchAfterRPCResponseRelease(t *testing.T) {
	txn, table := newDumpInsertTxnForTest(t)
	t.Cleanup(func() { cleanTxnWritesForTest(txn) })
	op := txn.op.(*mock_frontend.MockTxnOperator)
	op.EXPECT().Snapshot().Return(pbtxn.CNTxnSnapshot{}, nil).AnyTimes()
	op.EXPECT().SnapshotTS().Return(timestamp.Timestamp{}).AnyTimes()
	op.EXPECT().IsSnapOp().Return(false).AnyTimes()
	txn.proc.Base.TxnOperator = op
	table.proc.Store(txn.proc)
	table.createdInTxn = true
	delegate := &txnTableDelegate{origin: table}
	service := mock_shard.NewMockShardService(gomock.NewController(t))
	delegate.shard.service = service
	source := batch.NewWithSize(2)
	defer source.Clean(txn.proc.Mp())
	source.Vecs[0] = vector.NewVec(types.T_int64.ToType())
	source.Vecs[1] = vector.NewVec(types.T_varchar.ToType())
	require.NoError(t, vector.AppendFixed(source.Vecs[0], int64(42), false, txn.proc.Mp()))
	require.NoError(t, vector.AppendBytes(source.Vecs[1], []byte("owned remote row"), false, txn.proc.Mp()))
	source.SetRowCount(1)
	// The stable wire carries sorted/NULL metadata, but has no grouping field.
	source.Vecs[0].SetSorted(true)
	source.Vecs[0].GetGrouping().Add(0)
	frame := func(payload []byte) []byte {
		end, size := false, uint32(len(payload))
		response := append(types.EncodeBool(&end), types.EncodeUint32(&size)...)
		return append(response, payload...)
	}
	encoded, err := source.MarshalBinary()
	require.NoError(t, err)
	response := frame(encoded)
	service.EXPECT().Read(gomock.Any(), gomock.Any(), gomock.Any()).DoAndReturn(func(_ context.Context, request shardservice.ReadRequest, _ shardservice.ReadOptions) error {
		require.Equal(t, shardservice.ReadNext, request.Method)
		borrowed := append([]byte(nil), response...)
		applyErr := request.Apply(borrowed)
		// ShardService releases the response or its local morpc.Buffer after
		// Apply returns. Reusing those bytes must not change the caller's batch.
		clear(borrowed)
		return applyErr
	}).AnyTimes()
	registry, err := mpool.NewAllocationAccountRegistry(1, 128)
	require.NoError(t, err)
	account, err := registry.Open(1 << 20)
	require.NoError(t, err)
	selection, err := vector.NewAllocationAccountSelection(account, mpool.AllocationOwnerMin, 1, 2, 3, 4)
	require.NoError(t, err)
	destination := batch.NewOffHeapWithSize(2)
	require.NoError(t, destination.SetAllocationAccount(selection))
	defer func() {
		destination.Clean(txn.proc.Mp())
		require.Zero(t, account.Seal().Used)
		_, e := registry.Finalize(account)
		require.NoError(t, e)
		require.Zero(t, registry.LiveAllocationMetadata())
	}()
	reader := &shardingLocalReader{iteratePhase: InRemote, tblDelegate: delegate}
	defer reader.Close()
	finished, err := reader.Read(t.Context(), []string{"pk", "payload"}, nil, txn.proc.Mp(), destination)
	require.NoError(t, err)
	require.False(t, finished)
	require.Equal(t, 1, destination.RowCount())
	require.Equal(t, int64(42), vector.GetFixedAtNoTypeCheck[int64](destination.Vecs[0], 0))
	require.Equal(t, "owned remote row", string(destination.Vecs[1].GetBytesAt(0)))
	require.Positive(t, account.Snapshot().Used, "owned decoded data uses the destination account")
	require.True(t, destination.Vecs[0].GetSorted())
	require.False(t, destination.Vecs[0].HasGrouping(), "wire format has no grouping field")
	valid := append([]byte(nil), response...)
	for _, tc := range []struct {
		name  string
		frame []byte
		cols  []string
	}{
		{"empty", nil, nil}, {"end flag", []byte{2}, nil}, {"size prefix", []byte{0, 1}, nil},
		{"size exceeds frame", []byte{0, 255, 255, 255, 127}, nil}, {"truncated batch", frame(encoded[:10]), nil},
		{"column mismatch", valid, []string{"pk"}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			response = tc.frame
			cols := tc.cols
			if cols == nil {
				cols = []string{"pk", "payload"}
			}
			failedReader := &shardingLocalReader{iteratePhase: InRemote, tblDelegate: delegate}
			_, err := failedReader.Read(t.Context(), cols, nil, txn.proc.Mp(), destination)
			require.True(t, moerr.IsMoErrCode(err, moerr.ErrInvalidInput), "%v", err)
			require.Zero(t, destination.RowCount())
			require.True(t, failedReader.closed)
		})
	}
	// A real destination-account denial after one copied column must release
	// partial data, retain the context and allow a new reader to reuse the batch.
	require.NoError(t, vector.SetBytesAt(source.Vecs[1], 0, []byte(strings.Repeat("x", 4096)), txn.proc.Mp()))
	large, err := source.MarshalBinary()
	require.NoError(t, err)
	response = frame(large)
	filler, err := txn.proc.Mp().AllocAccounted(int(account.Snapshot().Limit-account.Snapshot().Used-32), account, mpool.AllocationOwnerMin, 5)
	require.NoError(t, err)
	defer func() {
		if filler != nil {
			txn.proc.Mp().Free(filler)
		}
	}()
	deniedReader := &shardingLocalReader{iteratePhase: InRemote, tblDelegate: delegate}
	_, err = deniedReader.Read(t.Context(), []string{"pk", "payload"}, nil, txn.proc.Mp(), destination)
	require.Equal(t, mpool.AllocationFailureCapacity, mpool.AllocationFailureReasonOf(err))
	require.Zero(t, destination.RowCount())
	require.Equal(t, uint64(len(filler)), account.Snapshot().Used)
	txn.proc.Mp().Free(filler)
	filler = nil
	response = valid
	retryReader := &shardingLocalReader{iteratePhase: InRemote, tblDelegate: delegate}
	finished, err = retryReader.Read(t.Context(), []string{"pk", "payload"}, nil, txn.proc.Mp(), destination)
	require.NoError(t, err)
	require.False(t, finished)
	require.Equal(t, int64(42), vector.GetFixedAtNoTypeCheck[int64](destination.Vecs[0], 0))
	require.Equal(t, "owned remote row", string(destination.Vecs[1].GetBytesAt(0)))
}

func TestPrimaryShardReadErrorPreservesFirstCause(t *testing.T) {
	ioErr := moerr.NewInternalErrorNoCtx("first storage failure")
	retry := moerr.NewRetryForCNRollingRestart()
	ctx, cancel := context.WithCancelCause(t.Context())
	cancel(ioErr)
	for _, tc := range []struct {
		name      string
		err, want error
	}{
		{"IO before retry", errors.Join(ioErr, retry), ioErr},
		{"retry before IO", errors.Join(retry, ioErr), retry},
		{"nested joins", fmt.Errorf("fanout: %w", errors.Join(fmt.Errorf("storage: %w", ioErr), errors.Join(retry, context.Canceled))), ioErr},
		{"cancel cannot replace selected IO", errors.Join(retry, ioErr), retry},
		{"wrapped caller cause", fmt.Errorf("stopped: %w", ioErr), ioErr},
	} {
		t.Run(tc.name, func(t *testing.T) { require.Same(t, tc.want, primaryShardReadError(ctx, tc.err)) })
	}
}

func TestNormalizePKCheckErrorPreservesRollingRestart(t *testing.T) {
	rollingRestart := moerr.NewRetryForCNRollingRestart()
	joined := errors.Join(
		moerr.NewReplicaNotFound("other replica"),
		errors.New("replica read failed"),
		rollingRestart,
	)

	err := normalizePKCheckError(joined)
	require.Same(t, rollingRestart, err)
	require.True(t, moerr.IsMoErrCode(err, moerr.ErrRetryForCNRollingRestart))
	require.Same(t, rollingRestart,
		normalizePKCheckError(fmt.Errorf("wrapped: %w", rollingRestart)))

	other := errors.New("other read failure")
	require.Same(t, other, normalizePKCheckError(other))
	joinedOther := errors.Join(other)
	require.Same(t, joinedOther, normalizePKCheckError(joinedOther))
}

func TestTxnTableDelegate_CollectChanges(t *testing.T) {
	table := &txnTableDelegate{}
	table.combined.is = true
	table.combined.tbl = newMockCombinedTxnTable()

	handle, err := table.CollectChanges(
		context.Background(),
		types.TS{},
		types.TS{},
		false,
		&mpool.MPool{},
	)
	assert.NoError(t, err)
	assert.NotNil(t, handle)
	assert.NoError(t, handle.Close())
}

func TestTxnTableDelegate_MergeObjects(t *testing.T) {
	table := &txnTableDelegate{}
	table.combined.is = true
	table.combined.tbl = newMockCombinedTxnTable()

	assert.PanicsWithValue(t, "not implemented", func() {
		table.MergeObjects(
			context.Background(),
			[]objectio.ObjectStats{},
			1024,
		)
	})
}

func TestTxnTableDelegate_UpdateConstraint(t *testing.T) {
	table := &txnTableDelegate{}
	table.combined.is = true
	table.combined.tbl = newMockCombinedTxnTable()

	assert.PanicsWithValue(t, "not implemented", func() {
		table.UpdateConstraint(
			context.Background(),
			&engine.ConstraintDef{},
		)
	})
}

func TestTxnTableDelegate_TableRenameInTxn(t *testing.T) {
	table := &txnTableDelegate{}
	table.combined.is = true
	table.combined.tbl = newMockCombinedTxnTable()

	assert.PanicsWithValue(t, "not implemented", func() {
		table.TableRenameInTxn(
			context.Background(),
			[][]byte{},
		)
	})
}

func TestTxnTableDelegate_MaxAndMinValues(t *testing.T) {
	table := &txnTableDelegate{}
	table.combined.is = true
	table.combined.tbl = newMockCombinedTxnTable()

	assert.PanicsWithValue(t, "not implemented", func() {
		table.MaxAndMinValues(context.Background())
	})
}

func TestTxnTableDelegate_Write(t *testing.T) {
	table := &txnTableDelegate{}
	table.combined.is = true
	table.combined.tbl = newMockCombinedTxnTable()

	assert.PanicsWithValue(t, "BUG: cannot write data to partition primary table", func() {
		table.Write(context.Background(), &batch.Batch{})
	})
}

func TestTxnTableDelegate_Delete(t *testing.T) {
	table := &txnTableDelegate{}
	table.combined.is = true
	table.combined.tbl = newMockCombinedTxnTable()

	assert.PanicsWithValue(t, "BUG: cannot delete data to partition primary table", func() {
		table.Delete(context.Background(), &batch.Batch{}, "")
	})
}
