// Copyright 2021 Matrix Origin
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

package updates

import (
	"context"
	"sync"
	"testing"
	"time"

	"github.com/matrixorigin/matrixone/pkg/common/mpool"
	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/objectio"

	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/catalog"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/containers"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/iface/txnif"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/index"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/testutils"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/txn/txnbase"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

type waitTrackingTxn struct {
	txnif.AsyncTxn
	waited      chan struct{}
	release     chan struct{}
	waitOnce    sync.Once
	releaseOnce sync.Once
}

func (txn *waitTrackingTxn) GetTxnState(wait bool) txnif.TxnState {
	if wait {
		txn.waitOnce.Do(func() { close(txn.waited) })
		<-txn.release
	}
	return txn.AsyncTxn.GetTxnState(false)
}

func TestMutationControllerAppend(t *testing.T) {
	defer testutils.AfterTest(t)()
	testutils.EnsureNoLeak(t)
	schema := catalog.MockSchema(1, 0)
	c := catalog.MockCatalog(nil)
	defer c.Close()
	db, _ := c.CreateDBEntry("db", "", "", nil)
	table, _ := db.CreateTableEntry(schema, nil, nil)
	noid := objectio.NewObjectid()
	stats := objectio.NewObjectStatsWithObjectID(&noid, true, false, false)
	obj, _ := table.CreateObject(nil, &objectio.CreateObjOpt{Stats: stats}, nil)
	mc := NewAppendMVCCHandle(obj)

	nodeCnt := 10000
	rowsPerNode := uint32(5)
	//ts := uint64(2)
	//ts = 4

	ts := types.NextGlobalTsForTest()
	ts = ts.Next()
	ts = ts.Next()
	//queries := make([]uint64, 0)
	//queries = append(queries, ts-1)
	queries := make([]types.TS, 0)
	queries = append(queries, ts.Prev())

	for i := 0; i < nodeCnt; i++ {
		txn := mockTxn()
		txn.CommitTS = ts
		txn.PrepareTS = ts
		node, _ := mc.AddAppendNodeLocked(txn, rowsPerNode*uint32(i), rowsPerNode*(uint32(i)+1))
		err := node.ApplyCommit(txn.ID)
		assert.Nil(t, err)
		//queries = append(queries, ts+1)
		queries = append(queries, ts.Next())
		//ts += 2
		ts = ts.Next()
		ts = ts.Next()
	}

	st := time.Now()
	for i, qts := range queries {
		selection, ok, _ := mc.GetVisibleRowLocked(context.TODO(), MockTxnWithStartTS(qts))
		if i == 0 {
			assert.False(t, ok)
		} else {
			assert.True(t, ok)
			assert.Equal(t, uint32(i)*rowsPerNode, selection.MaxRow)
		}
	}
	t.Logf("%s -- %d ops", time.Since(st), len(queries))
}

func TestSealedAppendMVCCPublishesMaxCommitAfterAllTransactionsFinish(t *testing.T) {
	schema := catalog.MockSchema(1, 0)
	c := catalog.MockCatalog(nil)
	defer c.Close()
	db, err := c.CreateDBEntry("db", "", "", nil)
	require.NoError(t, err)
	table, err := db.CreateTableEntry(schema, nil, nil)
	require.NoError(t, err)
	noid := objectio.NewObjectid()
	stats := objectio.NewObjectStatsWithObjectID(&noid, true, false, false)
	obj, err := table.CreateObject(nil, &objectio.CreateObjOpt{Stats: stats}, nil)
	require.NoError(t, err)
	h := NewAppendMVCCHandle(obj)

	txn1, txn2, txn3 := mockTxn(), mockTxn(), mockTxn()
	n1, _ := h.AddAppendNodeLocked(txn1, 0, 1)
	n2, _ := h.AddAppendNodeLocked(txn2, 1, 2)
	n3, _ := h.AddAppendNodeLocked(txn3, 2, 3)
	h.Seal()
	_, finalized := h.GetMaxCommitTS()
	require.False(t, finalized)
	require.Panics(t, func() { h.AddAppendNodeLocked(mockTxn(), 3, 4) })

	txn2.CommitTS = types.BuildTS(9, 0)
	require.NoError(t, n2.ApplyCommit(txn2.ID))
	_, finalized = h.GetMaxCommitTS()
	require.False(t, finalized)

	// The highest-timestamp transaction aborts and must not contribute to the
	// stable maximum.
	txn3.CommitTS = types.BuildTS(11, 0)
	require.NoError(t, n3.ApplyRollback())
	_, finalized = h.GetMaxCommitTS()
	require.False(t, finalized)

	txn1.CommitTS = types.BuildTS(7, 0)
	require.NoError(t, n1.ApplyCommit(txn1.ID))
	maxCommit, finalized := h.GetMaxCommitTS()
	require.True(t, finalized)
	require.Equal(t, types.BuildTS(9, 0), maxCommit)
}

func TestSealedEmptyAppendMVCCIsImmediatelyFinalized(t *testing.T) {
	h := NewAppendMVCCHandle(nil)
	h.Seal()
	maxCommit, finalized := h.GetMaxCommitTS()
	require.True(t, finalized)
	require.True(t, maxCommit.IsEmpty())
}

func TestSealedAppendMVCCHistoryIncompleteHasNoCommitBound(t *testing.T) {
	h := NewAppendMVCCHandle(nil)
	h.MarkAppendHistoryIncomplete()
	h.Seal()
	_, finalized := h.GetMaxCommitTS()
	require.False(t, finalized)
}

// AppendNode Start Prepare End Aborted
// a1 1,1,1 false
// a2 1,3,5 false
// a3 1,4,4 false
// a4 1,5,5 true
func TestGetVisibleRow(t *testing.T) {
	defer testutils.AfterTest(t)()
	schema := catalog.MockSchema(1, 0)
	c := catalog.MockCatalog(nil)
	defer c.Close()
	db, _ := c.CreateDBEntry("db", "", "", nil)
	table, _ := db.CreateTableEntry(schema, nil, nil)
	noid := objectio.NewObjectid()
	stats := objectio.NewObjectStatsWithObjectID(&noid, true, false, false)
	obj, _ := table.CreateObject(nil, &objectio.CreateObjOpt{Stats: stats}, nil)
	n := NewAppendMVCCHandle(obj)
	an1, _ := n.AddAppendNodeLocked(nil, 0, 1)
	an1.Start = types.BuildTS(1, 0)
	an1.Prepare = types.BuildTS(1, 0)
	an1.End = types.BuildTS(1, 0)
	an2, _ := n.AddAppendNodeLocked(nil, 1, 2)
	an2.Start = types.BuildTS(1, 0)
	an2.Prepare = types.BuildTS(3, 0)
	an2.End = types.BuildTS(5, 0)
	an3, _ := n.AddAppendNodeLocked(nil, 2, 3)
	an3.Start = types.BuildTS(1, 0)
	an3.Prepare = types.BuildTS(4, 0)
	an3.End = types.BuildTS(4, 0)
	an4, _ := n.AddAppendNodeLocked(nil, 3, 4)
	an4.Start = types.BuildTS(1, 0)
	an4.Prepare = types.BuildTS(5, 0)
	an4.End = types.BuildTS(5, 0)
	an4.Aborted = true

	// ts=1 maxrow=1, holes={}
	selection, visible, err := n.GetVisibleRowLocked(context.TODO(), MockTxnWithStartTS(types.BuildTS(1, 0)))
	assert.NoError(t, err)
	assert.Equal(t, uint32(1), selection.MaxRow)
	assert.True(t, visible)
	assert.True(t, selection.Holes == nil || selection.Holes.IsEmpty())

	// ts=4 maxrow=3, holes={1}
	selection, visible, err = n.GetVisibleRowLocked(context.TODO(), MockTxnWithStartTS(types.BuildTS(4, 0)))
	assert.NoError(t, err)
	assert.Equal(t, uint32(3), selection.MaxRow)
	assert.True(t, visible)
	assert.Equal(t, 1, selection.Holes.GetCardinality())
	assert.True(t, selection.Holes.Contains(1))

	// ts=5 maxrow=3, holes={}
	selection, visible, err = n.GetVisibleRowLocked(context.TODO(), MockTxnWithStartTS(types.BuildTS(5, 0)))
	assert.NoError(t, err)
	assert.Equal(t, uint32(3), selection.MaxRow)
	assert.True(t, visible)
	assert.True(t, selection.Holes == nil || selection.Holes.IsEmpty())

}

func TestPrepareRollbackKeepsAppendRangeAsHole(t *testing.T) {
	defer testutils.AfterTest(t)()
	schema := catalog.MockSchema(1, 0)
	c := catalog.MockCatalog(nil)
	defer c.Close()
	db, _ := c.CreateDBEntry("db", "", "", nil)
	table, _ := db.CreateTableEntry(schema, nil, nil)
	noid := objectio.NewObjectid()
	stats := objectio.NewObjectStatsWithObjectID(&noid, true, false, false)
	obj, _ := table.CreateObject(nil, &objectio.CreateObjOpt{Stats: stats}, nil)
	handle := NewAppendMVCCHandle(obj)

	node, _ := handle.AddAppendNodeLocked(nil, 2, 5)
	require.NoError(t, node.PrepareRollback())
	require.True(t, node.IsAborted())
	require.Same(t, node, handle.GetAppendNodeByRowLocked(3))
	require.Equal(t, uint32(5), handle.GetTotalRow())
}

func TestFillInCommitTSVecPreservesPhysicalRowGaps(t *testing.T) {
	schema := catalog.MockSchema(1, 0)
	c := catalog.MockCatalog(nil)
	defer c.Close()
	db, _ := c.CreateDBEntry("db", "", "", nil)
	table, _ := db.CreateTableEntry(schema, nil, nil)
	noid := objectio.NewObjectid()
	stats := objectio.NewObjectStatsWithObjectID(&noid, true, false, false)
	obj, _ := table.CreateObject(nil, &objectio.CreateObjOpt{Stats: stats}, nil)
	h := NewAppendMVCCHandle(obj)

	ts1, ts2 := types.BuildTS(10, 0), types.BuildTS(20, 0)
	n1, _ := h.AddAppendNodeLocked(nil, 2, 4)
	n1.End = ts1
	n2, _ := h.AddAppendNodeLocked(nil, 7, 8)
	n2.End = ts2

	mp := mpool.MustNewZero()
	commitTS := containers.MakeVector(types.T_TS.ToType(), mp)
	defer commitTS.Close()
	h.RLock()
	h.FillInCommitTSVecLocked(commitTS, 10, mp)
	h.RUnlock()

	require.Equal(t, 10, commitTS.Length())
	expected := []types.TS{
		txnif.UncommitTS, txnif.UncommitTS,
		ts1, ts1,
		txnif.UncommitTS, txnif.UncommitTS, txnif.UncommitTS,
		ts2,
		txnif.UncommitTS, txnif.UncommitTS,
	}
	for row, ts := range expected {
		require.Equal(t, ts, commitTS.Get(row), "row %d", row)
	}
}

func TestAppendMVCCPrepareAndRowOrders(t *testing.T) {
	schema := catalog.MockSchema(1, 0)
	c := catalog.MockCatalog(nil)
	defer c.Close()
	db, _ := c.CreateDBEntry("db", "", "", nil)
	table, _ := db.CreateTableEntry(schema, nil, nil)
	noid := objectio.NewObjectid()
	stats := objectio.NewObjectStatsWithObjectID(&noid, true, false, false)
	obj, _ := table.CreateObject(nil, &objectio.CreateObjOpt{Stats: stats}, nil)
	h := NewAppendMVCCHandle(obj)

	txn1, txn2, txn3 := mockTxn(), mockTxn(), mockTxn()
	n1, _ := h.AddAppendNodeLocked(txn1, 0, 10)
	n2, _ := h.AddAppendNodeLocked(txn2, 10, 20)
	n3, _ := h.AddAppendNodeLocked(txn3, 20, 30)
	assert.Equal(t, txnif.UncommitTS, n1.GetPrepare())
	assert.Equal(t, txnif.UncommitTS, n2.GetPrepare())
	assert.Equal(t, txnif.UncommitTS, n3.GetPrepare())
	_, unpreparedSnapshot := h.GetRowSelectionAfterWithSnapshot(types.TS{}, types.MaxTs())
	require.NotNil(t, unpreparedSnapshot)

	txn2.PrepareTS = types.BuildTS(2, 0)
	assert.NoError(t, n2.PrepareCommit())
	txn1.PrepareTS = types.BuildTS(3, 0)
	assert.NoError(t, n1.PrepareCommit())
	txn3.PrepareTS = types.BuildTS(4, 0)
	assert.NoError(t, n3.PrepareCommit())
	assert.False(t, h.IsPrepareSnapshotCurrent(unpreparedSnapshot))
	_, preparedSnapshot := h.GetRowSelectionAfterWithSnapshot(types.TS{}, types.MaxTs())
	assert.True(t, h.IsPrepareSnapshotCurrent(preparedSnapshot))
	assert.Same(t, n2, h.appends.MVCC[0])
	assert.Same(t, n1, h.appends.MVCC[1])
	assert.Same(t, n3, h.appends.MVCC[2])
	assert.Same(t, n1, h.rows[0])
	assert.Same(t, n2, h.rows[1])
	assert.Same(t, n3, h.rows[2])
	assert.Same(t, n1, h.GetAppendNodeByRowLocked(5))
	assert.Same(t, n2, h.GetAppendNodeByRowLocked(15))
	assert.Same(t, n3, h.GetAppendNodeByRowLocked(25))

	// Prepare order differs from physical row order. The selected ranges must
	// still be assembled in physical order, with strict/inclusive TS bounds.
	selection := h.GetRowSelectionAfter(types.BuildTS(1, 0), types.BuildTS(3, 0))
	assert.Equal(t, uint32(0), selection.MinRow)
	assert.Equal(t, uint32(20), selection.MaxRow)
	assert.Nil(t, selection.Holes)

	selection = h.GetRowSelectionAfter(types.BuildTS(2, 0), types.BuildTS(4, 0))
	assert.Equal(t, uint32(0), selection.MinRow)
	assert.Equal(t, uint32(30), selection.MaxRow)
	require.NotNil(t, selection.Holes)
	assert.Equal(t, 10, selection.Holes.GetCardinality())
	assert.True(t, selection.Holes.Contains(10))
	assert.True(t, selection.Holes.Contains(19))
	assert.False(t, selection.Holes.Contains(9))
	assert.False(t, selection.Holes.Contains(20))
}

func TestAppendMVCCPrepareTreeConcurrentReaders(t *testing.T) {
	schema := catalog.MockSchema(1, 0)
	c := catalog.MockCatalog(nil)
	defer c.Close()
	db, _ := c.CreateDBEntry("db", "", "", nil)
	table, _ := db.CreateTableEntry(schema, nil, nil)
	noid := objectio.NewObjectid()
	stats := objectio.NewObjectStatsWithObjectID(&noid, true, false, false)
	obj, _ := table.CreateObject(nil, &objectio.CreateObjOpt{Stats: stats}, nil)
	h := NewAppendMVCCHandle(obj)

	const nodeCount = 64
	nodes := make([]*AppendNode, nodeCount)
	for i := range nodes {
		txn := mockTxn()
		nodes[i], _ = h.AddAppendNodeLocked(txn, uint32(i), uint32(i+1))
		// Reverse PrepareTS order to exercise COW replacement and physical-row
		// reordering concurrently with lock-free readers.
		txn.PrepareTS = types.BuildTS(int64(nodeCount-i), 0)
	}

	readerStarted := make(chan struct{})
	done := make(chan struct{})
	var readers sync.WaitGroup
	for range 8 {
		readers.Add(1)
		go func() {
			defer readers.Done()
			select {
			case readerStarted <- struct{}{}:
			case <-done:
				return
			}
			for {
				select {
				case <-done:
					return
				default:
				}
				selection := h.GetRowSelectionAfter(types.TS{}, types.MaxTs())
				require.Equal(t, uint32(0), selection.MinRow)
				require.Equal(t, uint32(nodeCount), selection.MaxRow)
				require.True(t, selection.Holes == nil || selection.Holes.IsEmpty())
			}
		}()
	}
	for range 8 {
		<-readerStarted
	}
	for _, node := range nodes {
		require.NoError(t, node.PrepareCommit())
	}
	close(done)
	readers.Wait()
}

func TestAppendMVCCRollbackKeepsPhysicalHole(t *testing.T) {
	schema := catalog.MockSchema(1, 0)
	c := catalog.MockCatalog(nil)
	defer c.Close()
	db, _ := c.CreateDBEntry("db", "", "", nil)
	table, _ := db.CreateTableEntry(schema, nil, nil)
	noid := objectio.NewObjectid()
	stats := objectio.NewObjectStatsWithObjectID(&noid, true, false, false)
	obj, _ := table.CreateObject(nil, &objectio.CreateObjOpt{Stats: stats}, nil)
	h := NewAppendMVCCHandle(obj)
	txn := mockTxn()
	node, _ := h.AddAppendNodeLocked(txn, 0, 4)
	txn.PrepareTS = types.BuildTS(2, 0)
	assert.NoError(t, node.PrepareRollback())
	assert.True(t, node.IsAborted())
	assert.Same(t, node, h.GetAppendNodeByRowLocked(2))
	assert.Equal(t, uint32(4), h.GetTotalRow())
}

func TestCollectAppendWaitsForTxnPrepareTS(t *testing.T) {
	schema := catalog.MockSchema(1, 0)
	c := catalog.MockCatalog(nil)
	defer c.Close()
	db, _ := c.CreateDBEntry("db", "", "", nil)
	table, _ := db.CreateTableEntry(schema, nil, nil)
	noid := objectio.NewObjectid()
	stats := objectio.NewObjectStatsWithObjectID(&noid, true, false, false)
	obj, _ := table.CreateObject(nil, &objectio.CreateObjOpt{Stats: stats}, nil)
	h := NewAppendMVCCHandle(obj)

	prepareTS := types.BuildTS(10, 0)
	txn := &waitTrackingTxn{
		AsyncTxn: mockTxn(),
		waited:   make(chan struct{}),
		release:  make(chan struct{}),
	}
	releaseTxn := func() { txn.releaseOnce.Do(func() { close(txn.release) }) }
	defer releaseTxn()
	txn.AsyncTxn.(*txnbase.Txn).PrepareTS = prepareTS
	node, _ := h.AddAppendNodeLocked(txn, 0, 1)
	require.Equal(t, txnif.UncommitTS, node.Prepare)

	type collectResult struct {
		selection index.RowSelection
		commits   containers.Vector
		aborts    containers.Vector
	}
	done := make(chan collectResult, 1)
	mp := mpool.MustNewZero()
	go func() {
		h.RLock()
		selection, commits, aborts := h.CollectAppendLocked(
			prepareTS.Prev(), prepareTS, mp,
		)
		h.RUnlock()
		done <- collectResult{selection, commits, aborts}
	}()

	select {
	case <-txn.waited:
	case <-time.After(time.Second):
		t.Fatal("range scan did not wait on the transaction prepare timestamp")
	}
	h.Lock()
	node.Prepare = prepareTS
	node.End = prepareTS
	node.Txn = nil
	h.Unlock()
	releaseTxn()

	result := <-done
	defer result.commits.Close()
	defer result.aborts.Close()
	require.Equal(t, uint32(1), result.selection.MaxRow)
	require.Equal(t, 1, result.commits.Length())
	require.Equal(t, 1, result.aborts.Length())
}
