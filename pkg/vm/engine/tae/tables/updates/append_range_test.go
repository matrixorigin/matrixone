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

package updates

import (
	"fmt"
	"sync"
	"sync/atomic"
	"testing"

	"github.com/matrixorigin/matrixone/pkg/common/mpool"
	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/catalog"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/txn/txnbase"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func replayRangeNode(h *AppendMVCCHandle, row, count uint32, prepare int64, aborted bool) *AppendNode {
	node := NewEmptyAppendNode()
	node.startRow, node.maxRow = row, row+count
	node.Start, node.Prepare, node.End = types.BuildTS(1, 0), types.BuildTS(prepare, 0), types.BuildTS(prepare+10, 0)
	node.Aborted = aborted
	h.OnReplayAppendNode(node)
	return node
}

func warmAppendRangeHistory(t *testing.T, h *AppendMVCCHandle) {
	t.Helper()
	mp := mpool.MustNewZero()
	defer mpool.DeleteMPool(mp)
	for range 2 {
		h.RLock()
		selection, commits, aborts := h.CollectAppendLocked(types.TS{}, types.TS{}, mp)
		h.RUnlock()
		if commits != nil {
			defer commits.Close()
			defer aborts.Close()
		}
		require.True(t, selection.IsEmpty())
		require.Nil(t, commits)
		require.Nil(t, aborts)
	}
}

func TestCollectAppendRangePhysicalOrder(t *testing.T) {
	for _, dense := range []bool{false, true} {
		t.Run(fmt.Sprintf("dense=%t", dense), func(t *testing.T) {
			h := NewAppendMVCCHandle(nil)
			for i := range 200 {
				replayRangeNode(h, uint32(1000+i), 1, 1, false)
			}
			// Equal prepare timestamps, reversed timestamp order, physical gaps,
			// multi-row nodes and an aborted range must share one output order.
			count := 4
			if dense {
				count = 32
			}
			for i := count - 1; i >= 0; i-- {
				replayRangeNode(h, uint32(3*i), 2, int64(2+i%2), i%3 == 0)
			}
			warmAppendRangeHistory(t, h)
			mp := mpool.MustNewZero()
			t.Cleanup(func() { mpool.DeleteMPool(mp) })
			h.RLock()
			selection, commits, aborts := h.CollectAppendLocked(types.BuildTS(2, 0), types.BuildTS(3, 0), mp)
			h.RUnlock()
			require.NotNil(t, commits)
			t.Cleanup(commits.Close)
			t.Cleanup(aborts.Close)
			require.Equal(t, 2*count, commits.Length())
			require.Equal(t, commits.Length(), aborts.Length())
			for i := range count {
				for j := range 2 {
					require.True(t, selection.Contains(uint32(3*i+j)))
					require.Equal(t, types.BuildTS(int64(12+i%2), 0), commits.Get(2*i+j))
					require.Equal(t, i%3 == 0, aborts.Get(2*i+j))
				}
				require.False(t, selection.Contains(uint32(3*i+2)))
			}
		})
	}
}

func TestCollectAppendRangeBoundariesAndDeletion(t *testing.T) {
	h := NewAppendMVCCHandle(nil)
	for i := range 129 {
		replayRangeNode(h, uint32(1000+i), 1, 0, false)
	}
	replayRangeNode(h, 0, 1, 0, false) // PreparedIn excludes the empty timestamp.
	first := replayRangeNode(h, 1, 1, 2, false)
	replayRangeNode(h, 2, 1, 3, true)
	warmAppendRangeHistory(t, h)
	mp := mpool.MustNewZero()
	t.Cleanup(func() { mpool.DeleteMPool(mp) })
	for _, c := range []struct {
		from, to int64
		rows     int
	}{
		{0, 0, 0}, {1, 1, 0}, {2, 2, 1}, {3, 3, 1}, {2, 3, 2}, {4, 5, 0}, {3, 2, 0},
	} {
		t.Run(fmt.Sprintf("%d-%d", c.from, c.to), func(t *testing.T) {
			h.RLock()
			selection, commits, aborts := h.CollectAppendLocked(types.BuildTS(c.from, 0), types.BuildTS(c.to, 0), mp)
			h.RUnlock()
			if c.rows == 0 {
				require.True(t, selection.IsEmpty())
				require.Nil(t, commits)
				require.Nil(t, aborts)
				return
			}
			t.Cleanup(commits.Close)
			t.Cleanup(aborts.Close)
			require.Equal(t, c.rows, commits.Length())
			require.Equal(t, c.rows, aborts.Length())
		})
	}
	h.Lock()
	h.DeleteAppendNodeLocked(first)
	h.Unlock()
	h.RLock()
	selection, commits, aborts := h.CollectAppendLocked(types.BuildTS(2, 0), types.BuildTS(2, 0), mp)
	h.RUnlock()
	require.True(t, selection.IsEmpty())
	require.Nil(t, commits)
	require.Nil(t, aborts)
}

func TestCollectAppendRangeLifecycle(t *testing.T) {
	h := NewAppendMVCCHandle(&catalog.ObjectEntry{})
	// Keep four completions within the incremental-index admission budget.
	for i := range 252 {
		replayRangeNode(h, uint32(1000+i), 1, 1, false)
	}
	txns := make([]*txnbase.Txn, 4)
	nodes := make([]*AppendNode, len(txns))
	for i := range txns {
		txns[i] = MockTxnWithStartTS(types.BuildTS(1, 0))
		txns[i].PrepareTS, txns[i].CommitTS = types.BuildTS(int64(i+2), 0), types.BuildTS(int64(i+12), 0)
		nodes[i], _ = h.AddAppendNodeLocked(txns[i], uint32(2000+i), uint32(2001+i))
	}
	// Extending one transaction adds no second pending membership.
	extended, created := h.AddAppendNodeLocked(txns[3], 2004, 2005)
	require.False(t, created)
	require.Same(t, nodes[3], extended)
	warmAppendRangeHistory(t, h)
	// Finish middle, head and tail nodes; keep a prepared node unfinished.
	require.NoError(t, nodes[1].PrepareRollback())
	require.NoError(t, nodes[1].ApplyRollback())
	require.NoError(t, nodes[3].PrepareCommit())
	require.NoError(t, nodes[3].ApplyCommit(txns[3].ID))
	require.NoError(t, nodes[0].PrepareCommit())
	require.NoError(t, nodes[0].ApplyCommit(txns[0].ID))
	require.NoError(t, nodes[2].PrepareCommit())
	// The mock txn remains Active, so GetTxnState(true) returns immediately.
	mp := mpool.MustNewZero()
	t.Cleanup(func() { mpool.DeleteMPool(mp) })
	h.RLock()
	selection, commits, aborts := h.CollectAppendLocked(types.BuildTS(2, 0), types.BuildTS(5, 0), mp)
	h.RUnlock()
	t.Cleanup(commits.Close)
	t.Cleanup(aborts.Close)
	require.Equal(t, uint32(2005), selection.MaxRow)
	require.Equal(t, 5, commits.Length())
	require.Equal(t, true, aborts.Get(1))
	require.Equal(t, types.BuildTS(15, 0), commits.Get(4))
	h.Lock()
	h.DeleteAppendNodeLocked(nodes[2])
	h.Unlock()
	require.NoError(t, nodes[2].ApplyRollback())
	h.RLock()
	deleted, deletedCommits, deletedAborts := h.CollectAppendLocked(types.BuildTS(4, 0), types.BuildTS(4, 0), mp)
	h.RUnlock()
	require.True(t, deleted.IsEmpty())
	require.Nil(t, deletedCommits)
	require.Nil(t, deletedAborts)
	// Release drops both unfinished and completed index references.
	h.AddAppendNodeLocked(MockTxnWithStartTS(types.BuildTS(1, 0)), 2005, 2006)
	h.ReleaseAppends()
	require.Zero(t, h.EstimateMemSizeLocked())
	h.RLock()
	selection, emptyCommits, emptyAborts := h.CollectAppendLocked(types.BuildTS(2, 0), types.BuildTS(5, 0), mp)
	h.RUnlock()
	require.True(t, selection.IsEmpty())
	require.Nil(t, emptyCommits)
	require.Nil(t, emptyAborts)
}

func TestAppendRangeIndexIncludesReadySuffix(t *testing.T) {
	h := NewAppendMVCCHandle(nil)
	first := replayRangeNode(h, 0, 1, 2, false)
	h.rangeIndex.refresh(h.rows)
	second := replayRangeNode(h, 1, 1, 3, true)
	// Model a writer completing after refresh unlocks but before the reader
	// reacquires its lock. Selection must include both disjoint stores.
	head, rest, dense := h.rangeIndex.selectNodes(types.BuildTS(2, 0), types.BuildTS(3, 0), 16)
	require.False(t, dense)
	require.ElementsMatch(t, []*AppendNode{first, second}, append(rest, head))
	// Removing a queued completion must survive the next index refresh.
	h.Lock()
	h.DeleteAppendNodeLocked(second)
	h.rangeIndex.refresh(h.rows)
	h.Unlock()
	head, rest, dense = h.rangeIndex.selectNodes(types.BuildTS(2, 0), types.BuildTS(3, 0), 16)
	require.False(t, dense)
	require.Same(t, first, head)
	require.Empty(t, rest)
	h.Lock()
	h.DeleteAppendNodeLocked(first)
	h.Unlock()
	head, rest, dense = h.rangeIndex.selectNodes(types.BuildTS(2, 0), types.BuildTS(3, 0), 16)
	require.False(t, dense)
	require.Nil(t, head)
	require.Empty(t, rest)
	h.ReleaseAppends()
	require.Zero(t, h.EstimateMemSizeLocked())
}

func TestCollectAppendRangeConcurrentReaders(t *testing.T) {
	h := NewAppendMVCCHandle(&catalog.ObjectEntry{})
	for row := range 129 {
		node := replayRangeNode(h, uint32(row), 1, int64(row+2), false)
		node.End = node.Prepare
	}
	warmAppendRangeHistory(t, h)
	start := make(chan struct{})
	var cutoff atomic.Int64
	cutoff.Store(130)
	var workers sync.WaitGroup
	for range 2 {
		workers.Go(func() {
			<-start
			mp := mpool.MustNewZero()
			defer mpool.DeleteMPool(mp)
			for range 32 {
				func() {
					last := cutoff.Load()
					end := types.BuildTS(last, 0)
					h.RLock()
					selection, commits, aborts := h.CollectAppendLocked(types.BuildTS(2, 0), end, mp)
					h.RUnlock()
					if !assert.NotNil(t, commits) {
						return
					}
					defer commits.Close()
					defer aborts.Close()
					assert.Equal(t, int(last-1), commits.Length())
					assert.Zero(t, selection.MinRow)
					assert.Equal(t, int(selection.MaxRow), commits.Length())
					for row := range commits.Length() {
						assert.Equal(t, types.BuildTS(int64(row+2), 0), commits.Get(row))
						assert.Equal(t, false, aborts.Get(row))
					}
				}()
			}
		})
	}
	close(start)
	for i := range 32 {
		row := 129 + i
		txn := MockTxnWithStartTS(types.BuildTS(1, 0))
		// Freeze installs the node before the serial queue binds PrepareTS.
		h.Lock()
		node, _ := h.AddAppendNodeLocked(txn, uint32(row), uint32(row+1))
		h.Unlock()
		txn.Lock()
		err := txn.ToPreparingLocked(types.BuildTS(int64(row+2), 0))
		txn.Unlock()
		require.NoError(t, err)
		cutoff.Store(int64(row + 2))
		require.NoError(t, node.PrepareCommit())
		require.NoError(t, node.ApplyCommit(txn.ID))
		txn.Lock()
		err = txn.ToCommittedLocked()
		txn.DoneCond.Broadcast()
		txn.Unlock()
		require.NoError(t, err)
	}
	workers.Wait()
}
