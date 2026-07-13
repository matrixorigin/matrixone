// Copyright 2023 Matrix Origin
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

package incrservice

import (
	"context"
	"errors"
	"fmt"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/lni/goutils/leaktest"
	"github.com/matrixorigin/matrixone/pkg/catalog"
	"github.com/matrixorigin/matrixone/pkg/common/moerr"
	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/container/vector"
	"github.com/matrixorigin/matrixone/pkg/defines"
	"github.com/matrixorigin/matrixone/pkg/pb/timestamp"
	"github.com/matrixorigin/matrixone/pkg/txn/client"
	"github.com/matrixorigin/matrixone/pkg/txn/rpc"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

type setOffsetStore struct {
	IncrValueStore

	t           *testing.T
	forceCalled bool
	tableID     uint64
	colName     string
	offset      uint64
}

type failingGetColumnsStore struct {
	IncrValueStore

	mu  sync.Mutex
	err error
}

type blockingGetColumnsStore struct {
	IncrValueStore

	mu      sync.Mutex
	block   bool
	started chan struct{}
	release chan struct{}
}

type blockingAllocateStore struct {
	IncrValueStore

	mu      sync.Mutex
	block   bool
	started chan struct{}
	release chan struct{}
}

func (s *blockingAllocateStore) blockNext() (<-chan struct{}, func()) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.block = true
	s.started = make(chan struct{})
	s.release = make(chan struct{})
	var once sync.Once
	return s.started, func() { once.Do(func() { close(s.release) }) }
}

func (s *blockingAllocateStore) Allocate(
	ctx context.Context,
	tableID uint64,
	col string,
	count int,
	txnOp client.TxnOperator,
) (uint64, uint64, timestamp.Timestamp, error) {
	s.mu.Lock()
	block := s.block
	started := s.started
	release := s.release
	if block {
		s.block = false
	}
	s.mu.Unlock()
	if block {
		close(started)
		select {
		case <-release:
		case <-ctx.Done():
			return 0, 0, timestamp.Timestamp{}, context.Cause(ctx)
		}
	}
	return s.IncrValueStore.Allocate(ctx, tableID, col, count, txnOp)
}

type countingAllocator struct {
	asyncCalls atomic.Int64
	asyncErr   error
}

func (a *countingAllocator) allocate(context.Context, uint64, string, int, client.TxnOperator) (uint64, uint64, timestamp.Timestamp, error) {
	return 0, 0, timestamp.Timestamp{}, nil
}

func (a *countingAllocator) asyncAllocate(context.Context, uint64, string, int, client.TxnOperator, func(uint64, uint64, timestamp.Timestamp, error)) error {
	a.asyncCalls.Add(1)
	return a.asyncErr
}

func (a *countingAllocator) updateMinValue(context.Context, uint64, string, uint64, client.TxnOperator) error {
	return nil
}

func (a *countingAllocator) forceSetOffset(context.Context, uint64, string, uint64, client.TxnOperator) error {
	return nil
}

func (a *countingAllocator) close() {}

func (s *blockingGetColumnsStore) blockNext() (<-chan struct{}, chan<- struct{}) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.block = true
	s.started = make(chan struct{})
	s.release = make(chan struct{})
	return s.started, s.release
}

func (s *blockingGetColumnsStore) GetColumns(
	ctx context.Context,
	tableID uint64,
	txnOp client.TxnOperator,
) ([]AutoColumn, error) {
	s.mu.Lock()
	block := s.block
	started := s.started
	release := s.release
	if block {
		s.block = false
	}
	s.mu.Unlock()
	if block {
		close(started)
		select {
		case <-release:
		case <-ctx.Done():
			return nil, context.Cause(ctx)
		}
	}
	return s.IncrValueStore.GetColumns(ctx, tableID, txnOp)
}

func (s *failingGetColumnsStore) GetColumns(
	ctx context.Context,
	tableID uint64,
	txnOp client.TxnOperator,
) ([]AutoColumn, error) {
	s.mu.Lock()
	err := s.err
	s.mu.Unlock()
	if err != nil {
		return nil, err
	}
	return s.IncrValueStore.GetColumns(ctx, tableID, txnOp)
}

func (s *failingGetColumnsStore) failWith(err error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.err = err
}

func (s *setOffsetStore) GetColumns(
	ctx context.Context,
	tableID uint64,
	txnOp client.TxnOperator,
) ([]AutoColumn, error) {
	s.t.Fatalf("SetOffset should not read committed columns")
	return nil, nil
}

func (s *setOffsetStore) ForceSetOffset(
	ctx context.Context,
	tableID uint64,
	colName string,
	offset uint64,
	txnOp client.TxnOperator,
) error {
	s.forceCalled = true
	s.tableID = tableID
	s.colName = colName
	s.offset = offset
	return nil
}

func TestCreate(t *testing.T) {
	runServiceTests(
		t,
		1,
		func(
			ctx context.Context,
			ss []*service,
			ops []client.TxnOperator) {
			s := ss[0]
			op := ops[0]

			def := newTestTableDef(2)
			require.NoError(t, s.Create(ctx, 0, def, op))

			s.mu.Lock()
			assert.Equal(t, 1, len(s.mu.tables))
			assert.Equal(t, 1, len(s.mu.creates))
			assert.Equal(t, 0, len(s.mu.deletes))
			assert.Equal(t, 1, len(s.mu.creates[string(op.Txn().ID)]))
			assert.Equal(t, 2, len(s.mu.tables[0].columns()))
			s.mu.Unlock()
			checkStoreCachesUncommitted(t, s.store.(*memStore), op, 2)

			require.NoError(t, op.Commit(ctx))
			s.mu.Lock()
			assert.Equal(t, 1, len(s.mu.tables))
			assert.Equal(t, 0, len(s.mu.creates))
			assert.Equal(t, 0, len(s.mu.deletes))
			s.mu.Unlock()
			checkStoreCachesCommitted(t, s.store.(*memStore), 2)
		})
}

func TestCreateOnOtherService(t *testing.T) {
	runServiceTests(
		t,
		2,
		func(
			ctx context.Context,
			ss []*service,
			ops []client.TxnOperator) {
			s := ss[0]
			op := ops[0]
			def := newTestTableDef(2)
			require.NoError(t, s.Create(ctx, 0, def, op))
			require.NoError(t, op.Commit(ctx))

			s2 := ss[0]
			_, err := s2.getCommittedTableCache(ctx, 0)
			require.NoError(t, err)
			s2.mu.Lock()
			assert.Equal(t, 1, len(s2.mu.tables))
			assert.Equal(t, 0, len(s2.mu.creates))
			assert.Equal(t, 0, len(s2.mu.deletes))
			s2.mu.Unlock()
		})
}

func TestReloadIncrCache(t *testing.T) {
	runServiceTests(
		t,
		2,
		func(
			ctx context.Context,
			ss []*service,
			ops []client.TxnOperator,
		) {
			s := ss[0]
			op := ops[0]
			def := newTestTableDef(1)
			require.NoError(t, s.Create(ctx, 0, def, op))
			require.NoError(t, op.Commit(ctx))

			s2 := ss[1]
			_, err := s2.getCommittedTableCache(ctx, 0)
			require.NoError(t, err)

			v := uint64(100000000)
			s.mu.Lock()
			c := s.mu.tables[0].(*tableCache)
			s.mu.Unlock()

			c.mu.Lock()
			cc := c.mu.cols[def[0].ColName]
			c.mu.Unlock()
			err = cc.updateTo(
				ctx,
				0,
				v,
				nil,
			)
			require.NoError(t, err)

			require.NoError(t, s2.Reload(ctx, 0))
			tc, err := s2.getCommittedTableCache(
				ctx,
				0,
			)
			require.NoError(t, err)

			c = tc.(*tableCache)
			c.mu.Lock()
			cc = c.mu.cols[def[0].ColName]
			c.mu.Unlock()

			require.Equal(t, v+1, cc.ranges.current())
		})
}

func TestSetOffset(t *testing.T) {
	runServiceTests(
		t,
		1,
		func(
			ctx context.Context,
			ss []*service,
			ops []client.TxnOperator,
		) {
			s := ss[0]
			op := ops[0]
			def := newTestTableDef(1)
			require.NoError(t, s.Create(ctx, 0, def, op))
			require.NoError(t, op.Commit(ctx))

			require.NoError(t, s.SetOffset(ctx, 0, def[0].ColName, 42, nil))

			store := s.store.(*memStore)
			store.Lock()
			require.Equal(t, uint64(42), store.caches[0][0].Offset)
			store.Unlock()
		})
}

func TestSetOffsetDoesNotReadCommittedColumns(t *testing.T) {
	ctx := defines.AttachAccountId(context.Background(), catalog.System_Account)
	store := &setOffsetStore{t: t}
	allocator := newValueAllocator("", store)
	defer allocator.close()
	s := &service{store: store, allocator: allocator}

	require.NoError(t, s.SetOffset(ctx, 10, "auto_col", 99, nil))
	require.True(t, store.forceCalled)
	require.Equal(t, uint64(10), store.tableID)
	require.Equal(t, "auto_col", store.colName)
	require.Equal(t, uint64(99), store.offset)
}

func TestSetOffsetReturnsStoreError(t *testing.T) {
	runServiceTests(
		t,
		1,
		func(
			ctx context.Context,
			ss []*service,
			ops []client.TxnOperator,
		) {
			s := ss[0]
			err := s.SetOffset(ctx, 42, "auto_0", 42, ops[0])
			require.Error(t, err)
		})
}

func TestMemStoreSetOffset(t *testing.T) {
	runServiceTests(
		t,
		1,
		func(
			ctx context.Context,
			ss []*service,
			ops []client.TxnOperator,
		) {
			store := ss[0].store.(*memStore)
			op := ops[0]
			def := newTestTableDef(1)
			require.NoError(t, store.Create(ctx, 0, def, op))

			require.NoError(t, store.SetOffset(ctx, 0, def[0].ColName, 77, op))

			store.Lock()
			require.Equal(t, uint64(77), store.uncommitted[string(op.Txn().ID)][0][0].Offset)
			store.Unlock()
		})
}

func TestMemStoreSetOffsetReturnsError(t *testing.T) {
	runServiceTests(
		t,
		1,
		func(
			ctx context.Context,
			ss []*service,
			ops []client.TxnOperator,
		) {
			store := ss[0].store.(*memStore)
			def := newTestTableDef(1)
			require.NoError(t, store.Create(ctx, 0, def, nil))

			require.Error(t, store.SetOffset(ctx, 1, def[0].ColName, 77, nil))
			require.Error(t, store.SetOffset(ctx, 0, "missing_col", 77, nil))

			op := ops[0]
			require.NoError(t, store.SetOffset(ctx, 0, def[0].ColName, 88, op))
			store.Lock()
			require.Equal(t, uint64(88), store.caches[0][0].Offset)
			store.Unlock()
		})
}

func TestMemStoreSetOffsetLowerThanPreAllocated(t *testing.T) {
	runServiceTests(
		t,
		1,
		func(
			ctx context.Context,
			ss []*service,
			ops []client.TxnOperator,
		) {
			store := ss[0].store.(*memStore)
			def := newTestTableDef(1)
			// Simulate pre-allocation by advancing the offset to a high value
			def[0].Offset = 10000
			require.NoError(t, store.Create(ctx, 0, def, nil))

			// ForceSetOffset to a value LOWER than the current offset bypasses
			// the monotonic guard. Regular SetOffset would reject the decrease.
			require.NoError(t, store.ForceSetOffset(ctx, 0, def[0].ColName, 99, nil))

			store.Lock()
			require.Equal(t, uint64(99), store.caches[0][0].Offset)
			store.Unlock()
		})
}

func TestCreateWithTxnAborted(t *testing.T) {
	runServiceTests(
		t,
		1,
		func(
			ctx context.Context,
			ss []*service,
			ops []client.TxnOperator) {
			s := ss[0]
			op := ops[0]
			def := newTestTableDef(2)
			require.NoError(t, s.Create(ctx, 0, def, op))

			s.mu.Lock()
			assert.Equal(t, 1, len(s.mu.tables))
			assert.Equal(t, 1, len(s.mu.creates))
			assert.Equal(t, 0, len(s.mu.deletes))
			assert.Equal(t, 1, len(s.mu.creates[string(op.Txn().ID)]))
			assert.Equal(t, 2, len(s.mu.tables[0].columns()))
			s.mu.Unlock()
			checkStoreCachesUncommitted(t, s.store.(*memStore), op, 2)

			require.NoError(t, op.Rollback(ctx))
			s.mu.Lock()
			assert.Equal(t, 0, len(s.mu.creates))
			assert.Equal(t, 0, len(s.mu.deletes))
			s.mu.Unlock()
			checkStoreCachesCommitted(t, s.store.(*memStore), 0)
			assert.Equal(t, 0, len(s.mu.tables))
		})
}

func TestDelete(t *testing.T) {
	lazyDeleteInterval = time.Millisecond * 10
	runServiceTests(
		t,
		2,
		func(
			ctx context.Context,
			ss []*service,
			ops []client.TxnOperator) {
			s := ss[0]
			op := ops[0]

			def := newTestTableDef(2)
			require.NoError(t, s.Create(ctx, 0, def, op))
			require.NoError(t, op.Commit(ctx))
			checkStoreCachesCommitted(t, s.store.(*memStore), 2)

			s2 := ss[1]
			op2 := ops[1]
			require.NoError(t, s.Delete(ctx, 0, op2))
			require.NoError(t, op2.Commit(ctx))
			waitStoreCachesCommitted(t, s2.store.(*memStore), 0)
		})
}

func TestDeleteWithTxnAborted(t *testing.T) {
	lazyDeleteInterval = time.Millisecond * 10
	runServiceTests(
		t,
		2,
		func(
			ctx context.Context,
			ss []*service,
			ops []client.TxnOperator) {
			s := ss[0]
			op := ops[0]

			def := newTestTableDef(2)
			require.NoError(t, s.Create(ctx, 0, def, op))
			require.NoError(t, op.Commit(ctx))
			checkStoreCachesCommitted(t, s.store.(*memStore), 2)

			op2 := ops[1]
			require.NoError(t, s.Delete(ctx, 0, op2))
			require.NoError(t, op2.Rollback(ctx))
			checkStoreCachesCommitted(t, s.store.(*memStore), 2)
		})
}

func TestDeleteOnOtherService(t *testing.T) {
	lazyDeleteInterval = time.Millisecond * 10
	runServiceTests(
		t,
		2,
		func(
			ctx context.Context,
			ss []*service,
			ops []client.TxnOperator) {
			s := ss[0]
			op := ops[0]

			def := newTestTableDef(2)
			require.NoError(t, s.Create(ctx, 0, def, op))
			require.NoError(t, op.Commit(ctx))
			checkStoreCachesCommitted(t, s.store.(*memStore), 2)

			s2 := ss[1]
			op2 := ops[1]
			require.NoError(t, s2.Delete(ctx, 0, op2))
			require.NoError(t, op2.Commit(ctx))
		})
}

func TestForceSetOffset(t *testing.T) {
	runServiceTests(
		t,
		1,
		func(
			ctx context.Context,
			ss []*service,
			ops []client.TxnOperator,
		) {
			s := ss[0]
			op := ops[0]
			def := newTestTableDef(1)
			require.NoError(t, s.Create(ctx, 0, def, op))
			require.NoError(t, op.Commit(ctx))

			// Simulate pre-allocation by advancing the store offset past the
			// desired value. CountPerAllocate defaults to 10000.
			store := s.store.(*memStore)
			store.Lock()
			store.caches[0][0].Offset = 10000
			store.Unlock()

			// SetOffset should detect the pre-allocation gap and use
			// ForceSetOffset to bypass the store-level monotonic guard.
			require.NoError(t, s.SetOffset(ctx, 0, def[0].ColName, 100, nil))

			store.Lock()
			require.Equal(t, uint64(100), store.caches[0][0].Offset)
			store.Unlock()
		})
}

func TestReloadAfterSetOffsetDropsStalePreAllocatedRange(t *testing.T) {
	client.RunTxnTests(func(tc client.TxnClient, _ rpc.TxnSender) {
		defer leaktest.AfterTest(t)()
		ctx, cancel := context.WithTimeout(defines.AttachAccountId(context.Background(), catalog.System_Account), 10*time.Second)
		defer cancel()

		store := NewMemStore()
		cn1 := NewIncrService("", store, Config{CountPerAllocate: 100}).(*service)
		cn2 := NewIncrService("", store, Config{CountPerAllocate: 100}).(*service)
		defer cn1.Close()
		defer cn2.Close()

		op, err := tc.New(ctx, timestamp.Timestamp{})
		require.NoError(t, err)
		def := newTestTableDef(1)
		require.NoError(t, cn1.Create(ctx, 0, def, op))
		require.NoError(t, op.Commit(ctx))

		vecType := types.New(types.T_uint64, 0, 0)
		input := newTestVector[uint64](1, vecType, nil, nil)
		last, err := cn1.InsertValues(ctx, 0, 0, []*vector.Vector{input}, 1, 0)
		require.NoError(t, err)
		require.Equal(t, uint64(1), last)

		require.NoError(t, cn2.SetOffset(ctx, 0, def[0].ColName, 100, nil))
		require.NoError(t, cn1.Reload(ctx, 0))

		input = newTestVector[uint64](1, vecType, nil, nil)
		last, err = cn1.InsertValues(ctx, 0, 0, []*vector.Vector{input}, 1, 0)
		require.NoError(t, err)
		require.Equal(t, uint64(101), last)
	})
}

func TestInsertValuesReplacesCacheOnTableVersionChange(t *testing.T) {
	client.RunTxnTests(func(tc client.TxnClient, _ rpc.TxnSender) {
		defer leaktest.AfterTest(t)()
		ctx, cancel := context.WithTimeout(defines.AttachAccountId(context.Background(), catalog.System_Account), 10*time.Second)
		defer cancel()

		store := NewMemStore()
		s := NewIncrService("", store, Config{CountPerAllocate: 100}).(*service)
		defer s.Close()

		op, err := tc.New(ctx, timestamp.Timestamp{})
		require.NoError(t, err)
		def := newTestTableDef(1)
		require.NoError(t, s.Create(ctx, 0, def, op))
		require.NoError(t, op.Commit(ctx))

		vecType := types.New(types.T_uint64, 0, 0)
		input := newTestVector[uint64](1, vecType, nil, nil)
		last, err := s.InsertValues(ctx, 0, 7, []*vector.Vector{input}, 1, 0)
		require.NoError(t, err)
		require.Equal(t, uint64(101), last)

		require.NoError(t, store.ForceSetOffset(ctx, 0, def[0].ColName, 1000, nil))
		input = newTestVector[uint64](1, vecType, nil, nil)
		last, err = s.InsertValues(ctx, 0, 8, []*vector.Vector{input}, 1, 0)
		require.NoError(t, err)
		require.Equal(t, uint64(1001), last)

		s.mu.Lock()
		cached := s.mu.tables[0]
		s.mu.Unlock()
		require.Equal(t, uint32(8), cached.version())
	})
}

func TestInsertValuesVersionChangeReloadsBothServices(t *testing.T) {
	client.RunTxnTests(func(tc client.TxnClient, _ rpc.TxnSender) {
		defer leaktest.AfterTest(t)()
		ctx, cancel := context.WithTimeout(defines.AttachAccountId(context.Background(), catalog.System_Account), 10*time.Second)
		defer cancel()

		store := NewMemStore()
		cn1 := NewIncrService("", store, Config{CountPerAllocate: 100}).(*service)
		cn2 := NewIncrService("", store, Config{CountPerAllocate: 100}).(*service)
		defer cn1.Close()
		defer cn2.Close()

		op, err := tc.New(ctx, timestamp.Timestamp{})
		require.NoError(t, err)
		def := newTestTableDef(1)
		require.NoError(t, cn1.Create(ctx, 0, def, op))
		require.NoError(t, op.Commit(ctx))

		vecType := types.New(types.T_uint64, 0, 0)
		input1 := newTestVector[uint64](1, vecType, nil, nil)
		last1, err := cn1.InsertValues(ctx, 0, 7, []*vector.Vector{input1}, 1, 0)
		require.NoError(t, err)
		input2 := newTestVector[uint64](1, vecType, nil, nil)
		last2, err := cn2.InsertValues(ctx, 0, 7, []*vector.Vector{input2}, 1, 0)
		require.NoError(t, err)
		require.Less(t, last1, uint64(1000))
		require.Less(t, last2, uint64(1000))

		require.NoError(t, store.ForceSetOffset(ctx, 0, def[0].ColName, 1000, nil))
		input1 = newTestVector[uint64](1, vecType, nil, nil)
		last1, err = cn1.InsertValues(ctx, 0, 8, []*vector.Vector{input1}, 1, 0)
		require.NoError(t, err)
		input2 = newTestVector[uint64](1, vecType, nil, nil)
		last2, err = cn2.InsertValues(ctx, 0, 8, []*vector.Vector{input2}, 1, 0)
		require.NoError(t, err)
		require.GreaterOrEqual(t, last1, uint64(1001))
		require.GreaterOrEqual(t, last2, uint64(1001))
	})
}

func TestInsertValuesFailedVersionReplacementPreservesOldCache(t *testing.T) {
	client.RunTxnTests(func(tc client.TxnClient, _ rpc.TxnSender) {
		defer leaktest.AfterTest(t)()
		ctx, cancel := context.WithTimeout(defines.AttachAccountId(context.Background(), catalog.System_Account), 10*time.Second)
		defer cancel()

		store := &failingGetColumnsStore{IncrValueStore: NewMemStore()}
		s := NewIncrService("", store, Config{CountPerAllocate: 100}).(*service)
		defer s.Close()

		op, err := tc.New(ctx, timestamp.Timestamp{})
		require.NoError(t, err)
		def := newTestTableDef(1)
		require.NoError(t, s.Create(ctx, 0, def, op))
		require.NoError(t, op.Commit(ctx))

		vecType := types.New(types.T_uint64, 0, 0)
		input := newTestVector[uint64](1, vecType, nil, nil)
		last, err := s.InsertValues(ctx, 0, 7, []*vector.Vector{input}, 1, 0)
		require.NoError(t, err)

		loadErr := errors.New("load version 8")
		store.failWith(loadErr)
		input = newTestVector[uint64](1, vecType, nil, nil)
		_, err = s.InsertValues(ctx, 0, 8, []*vector.Vector{input}, 1, 0)
		require.ErrorIs(t, err, loadErr)

		store.failWith(nil)
		input = newTestVector[uint64](1, vecType, nil, nil)
		next, err := s.InsertValues(ctx, 0, 7, []*vector.Vector{input}, 1, 0)
		require.NoError(t, err)
		require.Equal(t, last+1, next)
	})
}

func TestTableCacheRetireWaitsForActiveUsers(t *testing.T) {
	c := &tableCache{}
	c.acquire()
	c.retire()

	c.lifecycle.Lock()
	require.False(t, c.lifecycle.closed)
	c.lifecycle.Unlock()

	c.release()
	c.lifecycle.Lock()
	require.True(t, c.lifecycle.closed)
	c.lifecycle.Unlock()
}

func TestInsertValuesRejectsOlderVersion(t *testing.T) {
	runServiceTests(t, 1, func(ctx context.Context, ss []*service, ops []client.TxnOperator) {
		s := ss[0]
		def := newTestTableDef(1)
		require.NoError(t, s.Create(ctx, 0, def, ops[0]))
		require.NoError(t, ops[0].Commit(ctx))
		vecType := types.New(types.T_uint64, 0, 0)
		input := newTestVector[uint64](1, vecType, nil, nil)
		_, err := s.InsertValues(ctx, 0, 8, []*vector.Vector{input}, 1, 0)
		require.NoError(t, err)

		input = newTestVector[uint64](1, vecType, nil, nil)
		_, err = s.InsertValues(ctx, 0, 7, []*vector.Vector{input}, 1, 0)
		require.True(t, moerr.IsMoErrCode(err, moerr.ErrTxnNeedRetryWithDefChanged))
		require.Equal(t, uint32(8), s.getTableCache(0).version())
	})
}

func TestInsertValuesRejectsOlderBuilderFinishingAfterNewerVersion(t *testing.T) {
	client.RunTxnTests(func(tc client.TxnClient, _ rpc.TxnSender) {
		ctx, cancel := context.WithTimeout(defines.AttachAccountId(context.Background(), catalog.System_Account), 10*time.Second)
		defer cancel()
		store := &blockingGetColumnsStore{IncrValueStore: NewMemStore()}
		s := NewIncrService("", store, Config{CountPerAllocate: 10}).(*service)
		defer s.Close()
		op, err := tc.New(ctx, timestamp.Timestamp{})
		require.NoError(t, err)
		require.NoError(t, s.Create(ctx, 0, newTestTableDef(1), op))
		require.NoError(t, op.Commit(ctx))

		started, release := store.blockNext()
		oldErr := make(chan error, 1)
		go func() {
			input := newTestVector[uint64](1, types.New(types.T_uint64, 0, 0), nil, nil)
			_, err := s.InsertValues(ctx, 0, 7, []*vector.Vector{input}, 1, 0)
			oldErr <- err
		}()
		<-started
		input := newTestVector[uint64](1, types.New(types.T_uint64, 0, 0), nil, nil)
		_, err = s.InsertValues(ctx, 0, 8, []*vector.Vector{input}, 1, 0)
		require.NoError(t, err)
		close(release)
		require.True(t, moerr.IsMoErrCode(<-oldErr, moerr.ErrTxnNeedRetryWithDefChanged))
		require.Equal(t, uint32(8), s.getTableCache(0).version())
	})
}

func TestInsertValuesBuilderCannotReviveCacheAfterReload(t *testing.T) {
	testBlockedBuilderInvalidation(t, false)
}

func TestInsertValuesBuilderCannotReviveCacheAfterClose(t *testing.T) {
	testBlockedBuilderInvalidation(t, true)
}

func TestSetOffsetWaitsForQueuedOldAllocation(t *testing.T) {
	client.RunTxnTests(func(tc client.TxnClient, _ rpc.TxnSender) {
		ctx, cancel := context.WithTimeout(defines.AttachAccountId(context.Background(), catalog.System_Account), 10*time.Second)
		defer cancel()
		store := &blockingAllocateStore{IncrValueStore: NewMemStore()}
		s := NewIncrService("", store, Config{CountPerAllocate: 2, LowCapacity: 1}).(*service)
		defer s.Close()
		op, err := tc.New(ctx, timestamp.Timestamp{})
		require.NoError(t, err)
		def := newTestTableDef(1)
		require.NoError(t, s.Create(ctx, 0, def, op))
		require.NoError(t, op.Commit(ctx))

		started, release := store.blockNext()
		defer release()
		input := newTestVector[uint64](1, types.New(types.T_uint64, 0, 0), nil, nil)
		last, err := s.InsertValues(ctx, 0, 0, []*vector.Vector{input}, 1, 0)
		require.NoError(t, err)
		require.Equal(t, uint64(1), last)
		<-started

		setResult := make(chan error, 1)
		go func() { setResult <- s.SetOffset(ctx, 0, def[0].ColName, 99, nil) }()
		require.Eventually(t, func() bool {
			return len(s.allocator.(*allocator).c) == 1
		}, time.Second, time.Millisecond)
		release()
		require.NoError(t, <-setResult)

		input = newTestVector[uint64](1, types.New(types.T_uint64, 0, 0), nil, nil)
		last, err = s.InsertValues(ctx, 0, 1, []*vector.Vector{input}, 1, 0)
		require.NoError(t, err)
		require.Equal(t, uint64(100), last)
	})
}

func TestCanceledSetOffsetDoesNotRunQueuedForceUpdate(t *testing.T) {
	client.RunTxnTests(func(tc client.TxnClient, _ rpc.TxnSender) {
		ctx, cancel := context.WithTimeout(defines.AttachAccountId(context.Background(), catalog.System_Account), 10*time.Second)
		defer cancel()
		mem := NewMemStore().(*memStore)
		store := &blockingAllocateStore{IncrValueStore: mem}
		s := NewIncrService("", store, Config{CountPerAllocate: 2, LowCapacity: 1}).(*service)
		defer s.Close()
		op, err := tc.New(ctx, timestamp.Timestamp{})
		require.NoError(t, err)
		def := newTestTableDef(1)
		require.NoError(t, s.Create(ctx, 0, def, op))
		require.NoError(t, op.Commit(ctx))

		started, release := store.blockNext()
		defer release()
		input := newTestVector[uint64](1, types.New(types.T_uint64, 0, 0), nil, nil)
		_, err = s.InsertValues(ctx, 0, 0, []*vector.Vector{input}, 1, 0)
		require.NoError(t, err)
		<-started

		setCtx, cancelSet := context.WithCancel(ctx)
		setResult := make(chan error, 1)
		go func() { setResult <- s.SetOffset(setCtx, 0, def[0].ColName, 99, nil) }()
		require.Eventually(t, func() bool {
			return len(s.allocator.(*allocator).c) == 1
		}, time.Second, time.Millisecond)
		cancelSet()
		require.ErrorIs(t, <-setResult, context.Canceled)
		release()

		require.NoError(t, s.allocator.updateMinValue(ctx, 0, def[0].ColName, 0, nil))
		mem.Lock()
		offset := mem.caches[0][0].Offset
		mem.Unlock()
		require.Equal(t, uint64(4), offset)
	})
}

func TestRetiredTableCacheCannotQueueAllocation(t *testing.T) {
	allocator := &countingAllocator{}
	col := &columnCache{
		col:       AutoColumn{TableID: 1, ColName: "auto", Step: 1},
		cfg:       Config{CountPerAllocate: 2, LowCapacity: 1},
		ranges:    &ranges{step: 1},
		allocator: allocator,
		committed: true,
	}
	c := &tableCache{}
	c.mu.cols = map[string]*columnCache{"auto": col}
	c.retire()
	col.preAllocate(context.Background(), 1, 2, nil)
	require.Zero(t, allocator.asyncCalls.Load())
}

func TestPreAllocateClearsStateWhenEnqueueFails(t *testing.T) {
	allocator := &countingAllocator{asyncErr: context.Canceled}
	col := &columnCache{
		col:       AutoColumn{TableID: 1, ColName: "auto", Step: 1},
		cfg:       Config{CountPerAllocate: 2, LowCapacity: 1},
		ranges:    &ranges{step: 1},
		allocator: allocator,
		committed: true,
	}
	col.preAllocate(context.Background(), 1, 2, nil)
	col.Lock()
	allocating := col.allocating
	col.Unlock()
	require.False(t, allocating)
}

func testBlockedBuilderInvalidation(t *testing.T, closeService bool) {
	client.RunTxnTests(func(tc client.TxnClient, _ rpc.TxnSender) {
		ctx, cancel := context.WithTimeout(defines.AttachAccountId(context.Background(), catalog.System_Account), 10*time.Second)
		defer cancel()
		store := &blockingGetColumnsStore{IncrValueStore: NewMemStore()}
		s := NewIncrService("", store, Config{CountPerAllocate: 10}).(*service)
		if !closeService {
			defer s.Close()
		}
		op, err := tc.New(ctx, timestamp.Timestamp{})
		require.NoError(t, err)
		require.NoError(t, s.Create(ctx, 0, newTestTableDef(1), op))
		require.NoError(t, op.Commit(ctx))

		started, release := store.blockNext()
		result := make(chan error, 1)
		go func() {
			input := newTestVector[uint64](1, types.New(types.T_uint64, 0, 0), nil, nil)
			_, err := s.InsertValues(ctx, 0, 8, []*vector.Vector{input}, 1, 0)
			result <- err
		}()
		<-started
		if closeService {
			closed := make(chan struct{})
			go func() {
				s.Close()
				close(closed)
			}()
			require.Eventually(t, func() bool {
				s.mu.Lock()
				defer s.mu.Unlock()
				return s.mu.closed
			}, time.Second, time.Millisecond)
			close(release)
			<-closed
		} else {
			require.NoError(t, s.Reload(ctx, 0))
			close(release)
		}
		require.True(t, moerr.IsMoErrCode(<-result, moerr.ErrTxnNeedRetryWithDefChanged))
		s.mu.Lock()
		cache, installed := s.mu.tables[0]
		s.mu.Unlock()
		if closeService {
			require.True(t, installed)
			require.Equal(t, uint32(0), cache.version())
		} else {
			require.False(t, installed)
		}
	})
}

func TestMemStoreForceSetOffset(t *testing.T) {
	runServiceTests(
		t,
		1,
		func(
			ctx context.Context,
			ss []*service,
			ops []client.TxnOperator,
		) {
			store := ss[0].store.(*memStore)
			op := ops[0]
			def := newTestTableDef(1)
			require.NoError(t, store.Create(ctx, 0, def, op))

			// ForceSetOffset bypasses the monotonic guard, allowing any value.
			require.NoError(t, store.ForceSetOffset(ctx, 0, def[0].ColName, 50, op))

			store.Lock()
			require.Equal(t, uint64(50), store.uncommitted[string(op.Txn().ID)][0][0].Offset)
			store.Unlock()
		})
}

func TestMemStoreForceSetOffsetLowerThanCurrent(t *testing.T) {
	runServiceTests(
		t,
		1,
		func(
			ctx context.Context,
			ss []*service,
			ops []client.TxnOperator,
		) {
			store := ss[0].store.(*memStore)
			op := ops[0]
			def := newTestTableDef(1)
			require.NoError(t, store.Create(ctx, 0, def, op))

			// Raise offset first with monotonic SetOffset.
			require.NoError(t, store.SetOffset(ctx, 0, def[0].ColName, 1000, op))

			// ForceSetOffset can lower it below the current value.
			require.NoError(t, store.ForceSetOffset(ctx, 0, def[0].ColName, 100, op))

			store.Lock()
			require.Equal(t, uint64(100), store.uncommitted[string(op.Txn().ID)][0][0].Offset)
			store.Unlock()
		})
}

func runServiceTests(
	t *testing.T,
	n int,
	fn func(ctx context.Context, ss []*service, ops []client.TxnOperator),
) {
	client.RunTxnTests(func(
		tc client.TxnClient,
		ts rpc.TxnSender) {
		defer leaktest.AfterTest(t)()
		ctx, cancel := context.WithTimeout(defines.AttachAccountId(context.Background(), catalog.System_Account), 10*time.Second)
		defer cancel()

		ss := make([]*service, 0, n)
		ops := make([]client.TxnOperator, 0, n)
		store := NewMemStore()
		for i := 0; i < n; i++ {
			ss = append(ss, NewIncrService("", store, Config{CountPerAllocate: 1}).(*service))

			op, err := tc.New(ctx, timestamp.Timestamp{})
			require.NoError(t, err)
			ops = append(ops, op)
		}
		defer func() {
			for _, s := range ss {
				s.Close()
			}
		}()

		fn(ctx, ss, ops)
	})
}

func newTestTableDef(autoCols int) []AutoColumn {
	var cols []AutoColumn
	for i := 0; i < autoCols; i++ {
		cols = append(cols, AutoColumn{
			ColName: fmt.Sprintf("auto_%d", i),
			Step:    1,
			Offset:  0,
		})
	}
	return cols
}

func waitStoreCachesCommitted(
	_ *testing.T,
	store *memStore,
	n int) {
	for {
		store.Lock()
		if len(store.caches[0]) == n {
			store.Unlock()
			return
		}
		store.Unlock()
		time.Sleep(time.Millisecond * 10)
	}
}

func checkStoreCachesCommitted(
	t *testing.T,
	store *memStore,
	n int) {
	store.Lock()
	defer store.Unlock()
	require.Equal(t, n, len(store.caches[0]))
}

func checkStoreCachesUncommitted(
	t *testing.T,
	store *memStore,
	txnOp client.TxnOperator,
	n int) {
	store.Lock()
	defer store.Unlock()
	require.Equal(t, n, len(store.uncommitted[string(txnOp.Txn().ID)][0]))
}
