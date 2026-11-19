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

package gc

import (
	"context"
	"errors"
	"sync"
	"testing"

	"github.com/matrixorigin/matrixone/pkg/common/mpool"
	"github.com/matrixorigin/matrixone/pkg/container/batch"
	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/container/vector"
	"github.com/matrixorigin/matrixone/pkg/pb/plan"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/catalog"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/containers"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/db/checkpoint"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/iface/txnif"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/logtail"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/txn/txnbase"
	"github.com/stretchr/testify/require"
)

func TestBuildBloomfilter(t *testing.T) {
	for _, tc := range []struct {
		name         string
		rows         int
		probability  float64
		readFailure  bool
		cancelBefore bool
		cancelDuring bool
	}{
		{name: "explicit sizing", rows: 64, probability: 0.000001},
		// A high explicit probability keeps the default-row contract test small.
		{name: "default rows", probability: 0.9},
		{name: "default probability", rows: 64},
		{name: "reader failure", rows: 64, probability: 0.01, readFailure: true},
		{name: "cancel before acquisition", rows: 64, probability: 0.01, cancelBefore: true},
		{name: "cancel during read", rows: 64, probability: 0.01, cancelDuring: true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			mp := mpool.MustNewZero()
			t.Cleanup(func() { require.Zero(t, mp.CurrNB()) })
			typ := types.T_uint64.ToType()
			buffer := containers.NewOneSchemaBatchBuffer(1024, []string{"id"}, []types.Type{typ}, false)
			t.Cleanup(func() { buffer.Close(mp) })
			ctx, cancel := context.WithCancelCause(context.Background())
			defer cancel(nil)
			cause := errors.New("GC reader stopped")
			if tc.cancelBefore {
				cancel(cause)
			}
			calls := 0
			values := []uint64{1, 7, 17}
			source := func(_ context.Context, _ []string, _ *plan.Expr, mp *mpool.MPool, bat *batch.Batch) (bool, error) {
				calls++
				if tc.readFailure {
					return false, cause
				}
				if calls > 1 {
					return true, nil
				}
				if err := vector.AppendFixedList(bat.Vecs[0], values, nil, mp); err != nil {
					return false, err
				}
				bat.SetRowCount(len(values))
				if tc.cancelDuring {
					cancel(cause)
				}
				return false, nil
			}
			bf, err := BuildBloomfilter(ctx, tc.rows, tc.probability, 0, source, buffer, mp)
			if bf != nil {
				t.Cleanup(bf.Free)
			}
			if tc.cancelBefore {
				require.ErrorIs(t, err, cause)
				require.Nil(t, bf)
				require.Zero(t, calls)
				require.Zero(t, buffer.Len(), "cancelled work must not acquire a batch")
				return
			}
			require.Equal(t, 1, buffer.Len(), "the acquired batch must be returned on every terminal path")
			if tc.readFailure || tc.cancelDuring {
				require.ErrorIs(t, err, cause)
				require.Equal(t, 1, calls)
				return
			}
			require.NoError(t, err)
			require.True(t, bf.Valid())
			require.Equal(t, 2, calls)
			probe := vector.NewVec(typ)
			t.Cleanup(func() { probe.Free(mp) })
			require.NoError(t, vector.AppendFixedList(probe, values, nil, mp))
			checked := 0
			bf.Test(probe, func(exists bool, _ int) { require.True(t, exists); checked++ })
			require.Equal(t, len(values), checked)
		})
	}
}

// gcTableExistenceRunner hands buildTableExistenceMap the real catalog while
// letting the rest of the checkpoint.Runner surface stay inert. The racing
// chain under test is entirely real: catalog entries, their MVCC chains, the
// published iterators and the drop writer path.
type gcTableExistenceRunner struct {
	checkpoint.Runner
	cat *catalog.Catalog
}

func (r *gcTableExistenceRunner) GetCatalog() *catalog.Catalog { return r.cat }

// TestBuildTableExistenceMapConcurrentCatalogDrop is the owner
// regression for the UT race on PR CI job 134540016566: buildTableExistenceMap
// read each table entry's MVCC chain head / payload / delete TS through
// GetDeleteAtLocked without the entry lock while DBEntry.DropTableEntryByID
// (handle DropRelationByID) inserts the delete node under the entry write
// lock. Live tables must stay present in every concurrent snapshot, committed
// drops must stop being published, and every transaction this test starts is
// driven to a real terminal commit or rollback via the mock factory's store
// registration before the txn manager stops - TxnManager.Stop only parks the
// manager, it does not finalize anyone's transactions. The bounded concurrent
// loop exercises the racing window; it is not a claim of a deterministic
// interleaving, and under -race the pre-fix unlocked read fires on the very
// chains this test mutates.
func TestBuildTableExistenceMapConcurrentCatalogDrop(t *testing.T) {
	cat := catalog.MockCatalog(nil)
	t.Cleanup(func() { require.NoError(t, cat.Close()) })
	txnMgr := txnbase.NewTxnManager(
		catalog.MockTxnStoreFactory(cat),
		catalog.MockTxnFactory(cat),
		types.NewMockHLCClock(1),
	)
	txnMgr.Start(context.Background())
	t.Cleanup(txnMgr.Stop)

	var ownedMu sync.Mutex
	owned := make(map[string]txnif.AsyncTxn)
	track := func(txn txnif.AsyncTxn) {
		ownedMu.Lock()
		owned[txn.GetID()] = txn
		ownedMu.Unlock()
	}
	// Safety net before txnMgr.Stop (LIFO cleanup order): every still-active
	// owned transaction is rolled back so no error or FailNow path can leak
	// one into the manager.
	t.Cleanup(func() {
		ownedMu.Lock()
		defer ownedMu.Unlock()
		for _, txn := range owned {
			if txn.GetTxnState(false) == txnif.TxnStateActive {
				// Nonfatal reporting: keep the original cause visible while
				// still letting every remaining owned rollback, manager.Stop
				// and catalog.Close in the LIFO chain run.
				if err := txn.Rollback(context.Background()); err != nil {
					t.Errorf("safety-net rollback of owned txn %s failed: %v", txn.GetID(), err)
				}
			}
		}
	})

	createTxn, err := txnMgr.StartTxn(nil)
	require.NoError(t, err)
	track(createTxn)
	dbEntry, err := cat.CreateDBEntry(t.Name(), "", "", createTxn)
	require.NoError(t, err)
	// Register the DB entry like mockTxn.CreateDatabase does, so Commit()
	// applies the DB create node too.
	createTxn.GetStore().AddTxnEntry(dbEntry)

	liveIDs := []uint64{91001, 91002, 91003}
	victims := make([]uint64, 0, 12)
	for id := uint64(92001); id < 92001+12; id++ {
		victims = append(victims, id)
	}
	for _, id := range append(append([]uint64{}, liveIDs...), victims...) {
		tbl, err := dbEntry.CreateTableEntryWithTableId(catalog.MockSchema(2, 0), createTxn, nil, id)
		require.NoError(t, err)
		// Register the entry in the txn store exactly like the catalog mock
		// handles do, so Commit() applies the create nodes and later drop
		// txns see committed creators instead of a stale w-w conflict.
		createTxn.GetStore().AddTxnEntry(tbl)
	}
	require.NoError(t, createTxn.Commit(context.Background()))

	runner := &gcTableExistenceRunner{cat: cat}
	build := func(t *testing.T) (map[uint64]bool, error) {
		return buildTableExistenceMap(logtail.NewSnapshotMeta(), runner)
	}

	// Warm boundary: before any drop every live and victim table is present.
	warm, err := build(t)
	require.NoError(t, err)
	for _, id := range append(append([]uint64{}, liveIDs...), victims...) {
		require.True(t, warm[id], "table %d must be visible before the drop starts", id)
	}

	var wg sync.WaitGroup
	wg.Add(2)
	go func() {
		defer wg.Done()
		for range 200 {
			m, err := build(t)
			if err != nil {
				t.Errorf("concurrent build failed: %v", err)
				return
			}
			for _, id := range liveIDs {
				if !m[id] {
					t.Errorf("live table %d lost from a concurrent snapshot", id)
					return
				}
			}
		}
	}()
	go func() {
		defer wg.Done()
		for _, id := range victims {
			dropTxn, err := txnMgr.StartTxn(nil)
			if err != nil {
				t.Errorf("start drop txn failed: %v", err)
				return
			}
			track(dropTxn)
			_, dropped, err := dbEntry.DropTableEntryByID(id, dropTxn)
			if err != nil {
				_ = dropTxn.Rollback(context.Background())
				t.Errorf("drop table %d failed: %v", id, err)
				return
			}
			// Register the dropped entry the way mockDBHandle.DropRelationByName
			// does, so the commit below really applies the delete node.
			dropTxn.GetStore().AddTxnEntry(dropped)
			if err := dropTxn.Commit(context.Background()); err != nil {
				t.Errorf("commit drop txn for table %d failed: %v", id, err)
				return
			}
		}
	}()
	wg.Wait()

	// Every transaction this test owns must be terminally committed, and the
	// manager must no longer hold any of them. This only covers txns started
	// by this test; it makes no claim about the manager's own housekeeping.
	// Snapshot under ownedMu, then release it BEFORE the fatal assertions: a
	// require.FailNow inside the critical section would Goexit without the
	// inline Unlock, and the LIFO t.Cleanup below would then wait on the same
	// held mutex forever before it could roll back txns or stop the manager.
	ownedMu.Lock()
	snapshot := make(map[string]txnif.AsyncTxn, len(owned))
	for id, txn := range owned {
		snapshot[id] = txn
	}
	ownedMu.Unlock()
	for id, txn := range snapshot {
		require.Equal(t, txnif.TxnStateCommitted, txn.GetTxnState(false),
			"owned txn %s must be terminally committed", id)
		require.Nil(t, txnMgr.GetTxn(id), "manager must not hold owned txn %s anymore", id)
	}

	// Settled boundary: only commits from here on leave the delete markers
	// applied, so no victim may remain published while every live table must
	// still be present.
	final, err := build(t)
	require.NoError(t, err)
	for _, id := range liveIDs {
		require.True(t, final[id], "live table %d missing after concurrent drops", id)
	}
	for _, id := range victims {
		require.False(t, final[id], "dropped table %d still published to GC", id)
	}
}
