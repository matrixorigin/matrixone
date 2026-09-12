// Copyright 2026 Matrix Origin
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
// http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package incrservice

import (
	"context"
	"sync"
	"testing"
	"time"

	"github.com/golang/mock/gomock"
	"github.com/matrixorigin/matrixone/pkg/common/mpool"
	"github.com/matrixorigin/matrixone/pkg/common/runtime"
	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/pb/timestamp"
	"github.com/matrixorigin/matrixone/pkg/txn/client"
	"github.com/matrixorigin/matrixone/pkg/util/executor"
	mock_executor "github.com/matrixorigin/matrixone/pkg/util/executor/test"
	"github.com/stretchr/testify/require"
)

type cancelledAutoIDCacheAllocator struct {
	valueAllocator
	entered chan struct{}
	once    sync.Once
}

func (a *cancelledAutoIDCacheAllocator) allocate(ctx context.Context, _ uint64, _ string, _ int, _ client.TxnOperator) (uint64, uint64, timestamp.Timestamp, error) {
	a.once.Do(func() { close(a.entered) })
	<-ctx.Done()
	return 0, 0, timestamp.Timestamp{}, ctx.Err()
}

func TestAutoIDCacheCancellationAndRetirement(t *testing.T) {
	runtime.RunTest(t.Name(), func(runtime.Runtime) {
		ctx, cancel := context.WithTimeout(t.Context(), 5*time.Second)
		defer cancel()
		a := &cancelledAutoIDCacheAllocator{entered: make(chan struct{})}
		c, err := newColumnCache(ctx, t.Name(), 42, AutoColumn{ColName: "id", Step: 1, CacheSize: 1}, Config{EnableAutoIDCache: true}, true, a, nil)
		require.NoError(t, err)
		defer c.close()
		done := make(chan error, 1)
		apply := func() error {
			return c.applyAutoValues(ctx, 42, 1, nil, func(int) bool { return false }, func(int, uint64) error { panic("cancelled reservation must not publish a value") }, nil, NormalizeAutoIncrementOptions(1, 1), 1)
		}
		go func() { done <- apply() }()
		select {
		case <-a.entered:
		case <-ctx.Done():
			t.Fatal("allocator did not receive demand")
		}
		cancel()
		select {
		case err = <-done:
			require.ErrorIs(t, err, context.Canceled)
		case <-time.After(5 * time.Second):
			t.Fatal("cancelled demand did not terminate")
		}
		require.False(t, c.allocating)
		require.True(t, c.ranges.empty())
		require.False(t, c.terminal)
		c.retire()
		require.Error(t, apply(), "retirement remains terminal after cancellation")
	})
}

func TestAutoIDCacheProbeRejectsUnknownOwnedTimestamp(t *testing.T) {
	runtime.RunTest(t.Name(), func(runtime.Runtime) {
		tc, err := newTableCache(t.Context(), t.Name(), 42, 0, []AutoColumn{{ColName: "id", Step: 1, CacheSize: 1}}, Config{EnableAutoIDCache: true}, nil, nil, true)
		require.NoError(t, err)
		c := tc.(*tableCache).getColumnCache("id")
		ts, err := tc.getLastAllocateTS(t.Context(), "id")
		require.NoError(t, err)
		require.True(t, ts.IsEmpty())
		c.terminal = true
		c.terminalValue = ^uint64(0)
		_, err = tc.getLastAllocateTS(t.Context(), "id")
		require.ErrorContains(t, err, "no allocation timestamp")
		c.terminalTS = timestamp.Timestamp{PhysicalTime: 17}
		ts, err = tc.getLastAllocateTS(t.Context(), "id")
		require.NoError(t, err)
		require.Equal(t, c.terminalTS, ts)
		c.terminal = false
		c.terminalTS = timestamp.Timestamp{}
		c.ranges.add(1, 2)
		_, err = tc.getLastAllocateTS(t.Context(), "id")
		require.ErrorContains(t, err, "no allocation timestamp")
	})
}

func TestAutoIDCacheSQLMetadataErrorsCloseResult(t *testing.T) {
	for _, tc := range []struct {
		name    string
		raw     []byte
		null    bool
		wantErr bool
	}{
		{name: "legacy-empty"},
		{name: "missing-table", null: true, wantErr: true},
		{name: "corrupt", raw: []byte{0xff}, wantErr: true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			mp := mpool.MustNewZero()
			mem := executor.NewMemResult([]types.Type{types.T_varchar.ToType(), types.T_int32.ToType(), types.T_uint64.ToType(), types.T_uint64.ToType(), types.T_varchar.ToType()}, mp)
			mem.NewBatchWithRowCount(1)
			require.NoError(t, executor.AppendStringRows(mem, 0, []string{"id"}))
			require.NoError(t, executor.AppendFixedRows(mem, 1, []int32{0}))
			require.NoError(t, executor.AppendFixedRows(mem, 2, []uint64{1}))
			require.NoError(t, executor.AppendFixedRows(mem, 3, []uint64{1}))
			require.NoError(t, executor.AppendStringRows(mem, 4, []string{string(tc.raw)}))
			result := mem.GetResult()
			if tc.null {
				result.Batches[0].Vecs[4].SetNull(0)
			}
			exec := mock_executor.NewMockSQLExecutor(gomock.NewController(t))
			exec.EXPECT().Exec(gomock.Any(), gomock.Any(), gomock.Any()).Return(result, nil)
			store := &sqlStore{exec: exec}
			cols, err := store.GetColumns(t.Context(), 42, nil)
			if tc.wantErr {
				require.Error(t, err)
				require.Nil(t, cols)
			} else {
				require.NoError(t, err)
				require.Len(t, cols, 1)
				require.Zero(t, cols[0].CacheSize)
			}
			require.Zero(t, mp.CurrNB())
		})
	}
}
