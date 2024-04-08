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
	"fmt"
	"testing"
	"time"

	"github.com/lni/goutils/leaktest"
	"github.com/matrixorigin/matrixone/pkg/catalog"
	"github.com/matrixorigin/matrixone/pkg/defines"
	"github.com/matrixorigin/matrixone/pkg/pb/timestamp"
	"github.com/matrixorigin/matrixone/pkg/txn/client"
	"github.com/matrixorigin/matrixone/pkg/txn/rpc"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

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
			waitStoreCachesCommitted(t, s2.store.(*memStore), 0)
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
		for i := 0; i < n; i++ {
			store := NewMemStore()
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
