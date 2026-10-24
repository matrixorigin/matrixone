// Copyright 2026 Matrix Origin
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

package txnimpl

import (
	"context"
	"fmt"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/matrixorigin/matrixone/pkg/common/moerr"
	"github.com/matrixorigin/matrixone/pkg/objectio"
	v2 "github.com/matrixorigin/matrixone/pkg/util/metric/v2"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/catalog"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/common"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/containers"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/iface/data"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/iface/handle"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/iface/txnif"
	dto "github.com/prometheus/client_model/go"
	"github.com/stretchr/testify/require"
)

// These decorators park real ownership boundaries without a production hook.
type materializationFactory struct {
	catalog.DataFactory
	before        func()
	beforeFreeze  func(data.ObjectAppender, bool)
	beforePrepare func(data.ObjectAppender, bool)
}

func (f *materializationFactory) MakeTableFactory() catalog.TableDataFactory {
	return func(e *catalog.TableEntry) data.Table {
		return &materializationTable{Table: f.DataFactory.MakeTableFactory()(e), factory: f}
	}
}

type materializationTable struct {
	data.Table
	factory *materializationFactory
}

func (t *materializationTable) GetHandle(tombstone bool) data.TableHandle {
	return &materializationHandle{TableHandle: t.Table.GetHandle(tombstone), factory: t.factory, tombstone: tombstone}
}
func (t *materializationTable) ApplyHandle(h data.TableHandle, tombstone bool) {
	t.Table.ApplyHandle(h.(*materializationHandle).TableHandle, tombstone)
}

type materializationHandle struct {
	data.TableHandle
	factory   *materializationFactory
	tombstone bool
}

func (h *materializationHandle) GetAppender() (data.ObjectAppender, error) {
	a, err := h.TableHandle.GetAppender()
	if err != nil {
		return nil, err
	}
	return &materializationAppender{ObjectAppender: a, factory: h.factory, tombstone: h.tombstone}, nil
}
func (h *materializationHandle) SetAppender(id *common.ID) data.ObjectAppender {
	return &materializationAppender{ObjectAppender: h.TableHandle.SetAppender(id), factory: h.factory, tombstone: h.tombstone}
}

type materializationAppender struct {
	data.ObjectAppender
	factory   *materializationFactory
	tombstone bool
}

func (a *materializationAppender) LockFreeze() {
	if a.factory.beforeFreeze != nil {
		a.factory.beforeFreeze(a.ObjectAppender, a.tombstone)
	}
	a.ObjectAppender.LockFreeze()
}
func (a *materializationAppender) PrepareAppend(merge bool, rows uint32, txn txnif.AsyncTxn) (txnif.AppendNode, bool, uint32, error) {
	if a.factory.beforePrepare != nil {
		a.factory.beforePrepare(a.ObjectAppender, a.tombstone)
	}
	return a.ObjectAppender.PrepareAppend(merge, rows, txn)
}
func (a *materializationAppender) ReserveAppend(offset, rows uint32) {
	if a.factory.before != nil {
		a.factory.before()
	}
	a.ObjectAppender.ReserveAppend(offset, rows)
}

func TestFreezeMaterializationDoesNotHoldAllocatorLocks(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
	defer cancel()
	entered, release := make(chan struct{}), make(chan struct{})
	var blocked, armed atomic.Bool
	var once sync.Once
	unblock := func() { once.Do(func() { close(release) }) }
	defer unblock()
	dir := t.TempDir()
	c, mgr, driver := initTestContext(ctx, t, dir, func(factory catalog.DataFactory) catalog.DataFactory {
		return &materializationFactory{DataFactory: factory, before: func() {
			if armed.Load() && blocked.CompareAndSwap(false, true) {
				close(entered)
				<-release
			}
		}}
	})
	defer driver.Close()
	defer c.Close()
	defer mgr.Stop()
	schema := catalog.MockSchema(2, 0)
	setup, err := mgr.StartTxn(nil)
	require.NoError(t, err)
	db, err := setup.CreateDatabase("db", "", "")
	require.NoError(t, err)
	seedRel, err := db.CreateRelation(schema)
	require.NoError(t, err)
	input := catalog.MockBatch(schema, 3)
	defer input.Close()
	seed := input.CloneWindow(0, 1)
	err = seedRel.Append(ctx, seed)
	seed.Close()
	require.NoError(t, err)
	require.NoError(t, setup.Commit(ctx))
	armed.Store(true)
	txns := make([]txnif.AsyncTxn, 2)
	for i := range txns {
		txns[i], err = mgr.StartTxn(nil)
		require.NoError(t, err)
		defer txns[i].Rollback(context.Background())
		db, err := txns[i].GetDatabase("db")
		require.NoError(t, err)
		rel, err := db.GetRelationByName(schema.Name)
		require.NoError(t, err)
		bat := input.CloneWindow(i+1, 1)
		err = rel.Append(ctx, bat)
		bat.Close()
		require.NoError(t, err)
	}
	freezeCount := func() uint64 {
		var metric dto.Metric
		require.NoError(t, v2.TxnTNFreezeDurationHistogram.Write(&metric))
		return metric.GetHistogram().GetSampleCount()
	}
	beforeFreeze := freezeCount()
	done := make(chan error, 1)
	go func() { done <- txns[0].Commit(ctx) }()
	// Cleanup unblocks and joins the owner before stopping its manager/catalog.
	defer func() { unblock(); require.NoError(t, <-done) }()
	select {
	case <-entered:
	case <-ctx.Done():
		t.Fatal(ctx.Err())
	}
	secondDone := make(chan error, 1)
	go func() { secondDone <- txns[1].Commit(ctx) }()
	select {
	case err := <-secondDone:
		require.NoError(t, err)
	case <-ctx.Done():
		unblock()
		<-secondDone
		t.Fatal("later reservation blocked behind materialization")
	}
	unblock()
	// Join A before reading; retain the error for the deferred join oracle.
	err = <-done
	done <- err
	require.NoError(t, err)
	require.Equal(t, beforeFreeze+2, freezeCount())
	read, err := mgr.StartTxn(nil)
	require.NoError(t, err)
	defer read.Rollback(context.Background())
	db, err = read.GetDatabase("db")
	require.NoError(t, err)
	rel, err := db.GetRelationByName(schema.Name)
	require.NoError(t, err)
	for row := 0; row < input.Length(); row++ {
		pk := input.Vecs[0].Get(row)
		value, isNull, err := rel.GetValueByFilter(ctx, handle.NewEQFilter(pk), 0)
		require.NoError(t, err)
		require.False(t, isNull)
		require.Equal(t, pk, value)
	}
	require.NoError(t, read.Commit(ctx))
}

func allocatorRelation(t *testing.T, txn txnif.AsyncTxn, schema *catalog.Schema) handle.Relation {
	t.Helper()
	db, err := txn.GetDatabase("db")
	require.NoError(t, err)
	rel, err := db.GetRelationByName(schema.Name)
	require.NoError(t, err)
	return rel
}

func appendAllocatorRow(t *testing.T, ctx context.Context, txn txnif.AsyncTxn, schema *catalog.Schema, input *containers.Batch, row int) {
	t.Helper()
	rel := allocatorRelation(t, txn, schema)
	batch := input.CloneWindow(row, 1)
	defer batch.Close()
	require.NoError(t, rel.Append(ctx, batch))
}

func TestFreezeReservationDoesNotBlockOtherObjectKind(t *testing.T) {
	for _, pauseTombstone := range []bool{false, true} {
		t.Run(fmt.Sprintf("pause_tombstone_%v", pauseTombstone), func(t *testing.T) {
			ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
			defer cancel()
			entered, release := make(chan struct{}), make(chan struct{})
			var once sync.Once
			unblock := func() { once.Do(func() { close(release) }) }
			var armed, blocked atomic.Bool
			c, mgr, driver := initTestContext(ctx, t, t.TempDir(), func(f catalog.DataFactory) catalog.DataFactory {
				return &materializationFactory{DataFactory: f, beforePrepare: func(_ data.ObjectAppender, tombstone bool) {
					if armed.Load() && tombstone == pauseTombstone && blocked.CompareAndSwap(false, true) {
						close(entered)
						<-release
					}
				}}
			})
			defer driver.Close()
			defer c.Close()
			defer mgr.Stop()
			defer unblock()
			schema := catalog.MockSchema(2, 0)
			input := catalog.MockBatch(schema, 3)
			defer input.Close()
			setup, err := mgr.StartTxn(nil)
			require.NoError(t, err)
			defer setup.Rollback(context.Background())
			db, err := setup.CreateDatabase("db", "", "")
			require.NoError(t, err)
			rel, err := db.CreateRelation(schema)
			require.NoError(t, err)
			seed := input.CloneWindow(0, 2)
			err = rel.Append(ctx, seed)
			seed.Close()
			require.NoError(t, err)
			require.NoError(t, setup.Commit(ctx))
			insert, err := mgr.StartTxn(nil)
			require.NoError(t, err)
			defer insert.Rollback(context.Background())
			appendAllocatorRow(t, ctx, insert, schema, input, 2)
			remove, err := mgr.StartTxn(nil)
			require.NoError(t, err)
			defer remove.Rollback(context.Background())
			rel = allocatorRelation(t, remove, schema)
			id, row, err := rel.GetByFilter(ctx, handle.NewEQFilter(input.Vecs[0].Get(0)))
			require.NoError(t, err)
			require.NoError(t, rel.RangeDelete(id, row, row, handle.DT_Normal))
			first, second := insert, remove
			if pauseTombstone {
				first, second = remove, insert
			}
			armed.Store(true)
			done := make(chan error, 1)
			var wg sync.WaitGroup
			wg.Add(1)
			go func() { defer wg.Done(); done <- first.Commit(ctx) }()
			defer func() { unblock(); wg.Wait() }()
			select {
			case <-entered:
			case <-ctx.Done():
				t.Fatal(ctx.Err())
			}
			secondDone := make(chan error, 1)
			wg.Add(1)
			go func() { defer wg.Done(); secondDone <- second.Commit(ctx) }()
			select {
			case err := <-secondDone:
				require.NoError(t, err)
			case <-ctx.Done():
				t.Fatal("independent object kind blocked behind reservation")
			}
			unblock()
			require.NoError(t, <-done)
			wg.Wait()
			read, err := mgr.StartTxn(nil)
			require.NoError(t, err)
			defer read.Rollback(context.Background())
			rel = allocatorRelation(t, read, schema)
			_, _, err = rel.GetValueByFilter(ctx, handle.NewEQFilter(input.Vecs[0].Get(0)), 0)
			require.True(t, moerr.IsMoErrCode(err, moerr.ErrNotFound), "%v", err)
			for _, row := range []int{1, 2} {
				pk := input.Vecs[0].Get(row)
				value, isNull, err := rel.GetValueByFilter(ctx, handle.NewEQFilter(pk), 0)
				require.NoError(t, err)
				require.False(t, isNull)
				require.Equal(t, pk, value)
			}
			require.NoError(t, read.Commit(ctx))
		})
	}
}

func TestFreezeSelectedOwnersRollOverWithoutExtraCarriers(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
	defer cancel()
	const owners = 4
	arrived, release := make(chan struct{}), make(chan struct{})
	var once sync.Once
	unblock := func() { once.Do(func() { close(release) }) }
	var armed atomic.Bool
	var arrivals atomic.Int32
	var mu sync.Mutex
	selected := make([]objectio.ObjectId, 0, owners)
	prepared := map[objectio.ObjectId]int{}
	c, mgr, driver := initTestContext(ctx, t, t.TempDir(), func(f catalog.DataFactory) catalog.DataFactory {
		return &materializationFactory{DataFactory: f, beforeFreeze: func(a data.ObjectAppender, tombstone bool) {
			if !armed.Load() || tombstone {
				return
			}
			n := arrivals.Add(1)
			if n <= owners {
				mu.Lock()
				selected = append(selected, *a.GetMeta().(*catalog.ObjectEntry).ID())
				if len(selected) == owners {
					close(arrived)
				}
				mu.Unlock()
				<-release
			}
		}, beforePrepare: func(a data.ObjectAppender, tombstone bool) {
			if !tombstone {
				mu.Lock()
				prepared[*a.GetMeta().(*catalog.ObjectEntry).ID()]++
				mu.Unlock()
			}
		}}
	})
	defer driver.Close()
	defer c.Close()
	defer mgr.Stop()
	defer unblock()
	schema := catalog.MockSchema(2, 0)
	schema.Extra.BlockMaxRows = 2
	input := catalog.MockBatch(schema, owners+2)
	defer input.Close()
	setup, err := mgr.StartTxn(nil)
	require.NoError(t, err)
	defer setup.Rollback(context.Background())
	db, err := setup.CreateDatabase("db", "", "")
	require.NoError(t, err)
	rel, err := db.CreateRelation(schema)
	require.NoError(t, err)
	entry := rel.GetMeta().(*catalog.TableEntry)
	seed := input.CloneWindow(0, 1)
	err = rel.Append(ctx, seed)
	seed.Close()
	require.NoError(t, err)
	require.NoError(t, setup.Commit(ctx))
	txns := make([]txnif.AsyncTxn, owners)
	for i := range txns {
		txns[i], err = mgr.StartTxn(nil)
		require.NoError(t, err)
		defer txns[i].Rollback(context.Background())
		appendAllocatorRow(t, ctx, txns[i], schema, input, i+1)
	}
	armed.Store(true)
	errs := make(chan error, owners)
	var wg sync.WaitGroup
	defer func() { unblock(); wg.Wait() }()
	for _, txn := range txns {
		wg.Add(1)
		go func(txn txnif.AsyncTxn) { defer wg.Done(); errs <- txn.Commit(ctx) }(txn)
	}
	select {
	case <-arrived:
	case <-ctx.Done():
		t.Fatal("referenced owners did not reach the reservation boundary")
	}
	mu.Lock()
	ids := append([]objectio.ObjectId(nil), selected...)
	mu.Unlock()
	require.Len(t, ids, owners)
	for _, id := range ids {
		require.Equal(t, ids[0], id)
	}
	unblock()
	wg.Wait()
	for range owners {
		require.NoError(t, <-errs)
	}
	require.Equal(t, 3, entry.ObjectCnt(false))
	mu.Lock()
	var partial objectio.ObjectId
	for id, rows := range prepared {
		if rows == 1 {
			partial = id
		}
	}
	mu.Unlock()
	require.NotEqual(t, objectio.ObjectId{}, partial)
	last, err := mgr.StartTxn(nil)
	require.NoError(t, err)
	defer last.Rollback(context.Background())
	appendAllocatorRow(t, ctx, last, schema, input, owners+1)
	require.NoError(t, last.Commit(ctx))
	require.Equal(t, 3, entry.ObjectCnt(false), "the last writer must reuse remaining capacity")
	mu.Lock()
	partialRows := prepared[partial]
	mu.Unlock()
	require.Equal(t, 2, partialRows)
	read, err := mgr.StartTxn(nil)
	require.NoError(t, err)
	defer read.Rollback(context.Background())
	rel = allocatorRelation(t, read, schema)
	for row := 0; row < input.Length(); row++ {
		pk := input.Vecs[0].Get(row)
		value, isNull, err := rel.GetValueByFilter(ctx, handle.NewEQFilter(pk), 0)
		require.NoError(t, err)
		require.False(t, isNull)
		require.Equal(t, pk, value)
	}
	require.NoError(t, read.Commit(ctx))
}

func TestFreezeCancelledCreatorLeavesSharedCarrierUsable(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
	defer cancel()
	freezeCtx, cancelFreeze := context.WithCancel(ctx)
	defer cancelFreeze()
	entered, release := make(chan struct{}), make(chan struct{})
	var once sync.Once
	unblock := func() { once.Do(func() { close(release) }) }
	var blocked atomic.Bool
	var carrier objectio.ObjectId
	c, mgr, driver := initTestContext(ctx, t, t.TempDir(), func(f catalog.DataFactory) catalog.DataFactory {
		return &materializationFactory{DataFactory: f, beforeFreeze: func(a data.ObjectAppender, tombstone bool) {
			if !tombstone && blocked.CompareAndSwap(false, true) {
				carrier = *a.GetMeta().(*catalog.ObjectEntry).ID()
				close(entered)
				<-release
			}
		}}
	})
	defer driver.Close()
	defer c.Close()
	defer mgr.Stop()
	defer unblock()
	schema := catalog.MockSchema(2, 0)
	schema.Extra.BlockMaxRows = 4
	input := catalog.MockBatch(schema, 3)
	defer input.Close()
	setup, err := mgr.StartTxn(nil)
	require.NoError(t, err)
	defer setup.Rollback(context.Background())
	db, err := setup.CreateDatabase("db", "", "")
	require.NoError(t, err)
	rel, err := db.CreateRelation(schema)
	require.NoError(t, err)
	entry := rel.GetMeta().(*catalog.TableEntry)
	require.NoError(t, setup.Commit(ctx))
	first, err := mgr.StartTxn(nil)
	require.NoError(t, err)
	defer first.Rollback(context.Background())
	appendAllocatorRow(t, ctx, first, schema, input, 0)
	done := make(chan error, 1)
	var wg sync.WaitGroup
	var second txnif.AsyncTxn
	wg.Add(1)
	go func() { defer wg.Done(); done <- first.Commit(freezeCtx) }()
	defer func() {
		unblock()
		wg.Wait()
		if second != nil {
			_ = second.Rollback(context.Background())
		}
	}()
	select {
	case <-entered:
	case <-ctx.Done():
		t.Fatal(ctx.Err())
	}
	second, err = mgr.StartTxn(nil)
	require.NoError(t, err)
	appendAllocatorRow(t, ctx, second, schema, input, 1)
	secondDone := make(chan error, 1)
	wg.Add(1)
	go func() { defer wg.Done(); secondDone <- second.Commit(ctx) }()
	select {
	case err := <-secondDone:
		require.NoError(t, err)
	case <-ctx.Done():
		t.Fatal("shared carrier user blocked behind its selected creator")
	}
	cancelFreeze()
	unblock()
	require.ErrorIs(t, <-done, context.Canceled)
	wg.Wait()
	require.NoError(t, mgr.WaitEmpty(ctx))
	require.Equal(t, 1, entry.ObjectCnt(false), "cancellation must not create another carrier")
	obj, err := entry.GetObjectByID(&carrier, false)
	require.NoError(t, err)
	require.Zero(t, obj.GetObjectData().(interface{ RefCount() int64 }).RefCount())
	third, err := mgr.StartTxn(nil)
	require.NoError(t, err)
	defer third.Rollback(context.Background())
	appendAllocatorRow(t, ctx, third, schema, input, 2)
	require.NoError(t, third.Commit(ctx))
	require.Equal(t, 1, entry.ObjectCnt(false))
	require.Equal(t, uint64(2), entry.GetRows())
	read, err := mgr.StartTxn(nil)
	require.NoError(t, err)
	defer read.Rollback(context.Background())
	rel = allocatorRelation(t, read, schema)
	_, _, err = rel.GetValueByFilter(ctx, handle.NewEQFilter(input.Vecs[0].Get(0)), 0)
	require.True(t, moerr.IsMoErrCode(err, moerr.ErrNotFound), "%v", err)
	for _, row := range []int{1, 2} {
		pk := input.Vecs[0].Get(row)
		value, isNull, err := rel.GetValueByFilter(ctx, handle.NewEQFilter(pk), 0)
		require.NoError(t, err)
		require.False(t, isNull)
		require.Equal(t, pk, value)
	}
	require.NoError(t, read.Commit(ctx))
}
