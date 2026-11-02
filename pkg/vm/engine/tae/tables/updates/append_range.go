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
	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/tidwall/btree"
)

type appendPrepareEntry struct {
	prepare  types.TS
	startRow uint32
	node     *AppendNode
}

// appendRangeIndex uses the object's existing MVCC lock. It activates on the
// repeated large-history reads with small changes, so writer-only objects pay
// no per-node allocation or search maintenance. Readers index completions;
// finalization only updates membership.
type appendRangeIndex struct {
	active       bool
	completed    *btree.BTreeG[appendPrepareEntry]
	pending      map[*AppendNode]struct{}
	pendingPeak  int
	ready        []*AppendNode
	lastReadRows int
}

func (idx *appendRangeIndex) add(node *AppendNode) {
	if !idx.active {
		return
	}
	if node.GetTxn() == nil {
		idx.ready = append(idx.ready, node)
		return
	}
	if idx.pending == nil {
		idx.pending = make(map[*AppendNode]struct{})
	}
	idx.pending[node] = struct{}{}
	idx.pendingPeak = max(idx.pendingPeak, len(idx.pending))
}

func (idx *appendRangeIndex) complete(node *AppendNode) {
	if !idx.active {
		return
	}
	// A detached node may still finish rollback. Never recreate membership
	// after DeleteAppendNodeLocked or ReleaseAppends has removed it.
	if _, exists := idx.pending[node]; exists {
		delete(idx.pending, node)
		idx.ready = append(idx.ready, node)
	}
}

// refreshAppendRangeIndexLocked is called with exactly one MVCC read lock.
// Object-node pins held by callers keep payload/history lifetime stable across
// the same unlock/relock boundary already required for transaction waits.
func (n *AppendMVCCHandle) refreshAppendRangeIndexLocked() bool {
	if n.rangeIndex.active && len(n.rangeIndex.ready) == 0 {
		return true
	}
	n.RUnlock()
	n.Lock()
	idx := &n.rangeIndex
	count := len(n.rows)
	// Tree maintenance must be cheaper than scanning history. Stable/repeated
	// reads amortize construction; a large completion batch instead uses the
	// physical scan and drops derived state. The conservative 1/64 budget avoids
	// moving a scan regression onto batched write-and-collect workloads.
	budget := max(1, count/64)
	delta := count - idx.lastReadRows
	useIndex := idx.active && len(idx.ready) <= budget ||
		!idx.active && idx.lastReadRows != 0 && delta >= 0 && delta <= budget
	if useIndex {
		idx.refresh(n.rows)
	} else {
		idx.clear()
	}
	idx.lastReadRows = count
	n.Unlock()
	n.RLock()
	// Another reader may have discarded derived state before this read relock.
	return useIndex && n.rangeIndex.active
}

func (idx *appendRangeIndex) refresh(rows []*AppendNode) {
	if !idx.active {
		idx.active = true
		for _, node := range rows {
			idx.add(node)
		}
	}
	for _, node := range idx.ready {
		if node == nil || node.Prepare.IsEmpty() {
			continue
		}
		if idx.completed == nil {
			idx.completed = btree.NewBTreeGOptions(func(a, b appendPrepareEntry) bool {
				if c := a.prepare.Compare(&b.prepare); c != 0 {
					return c < 0
				}
				return a.startRow < b.startRow
			}, btree.Options{Degree: 32, NoLocks: true})
		}
		idx.completed.Set(appendPrepareEntry{prepare: node.Prepare, startRow: node.startRow, node: node})
	}
	clear(idx.ready)
	idx.ready = idx.ready[:0]
}

func (idx *appendRangeIndex) remove(node *AppendNode) {
	if !idx.active {
		return
	}
	delete(idx.pending, node)
	for i, candidate := range idx.ready {
		if candidate == node {
			idx.ready[i] = nil
		}
	}
	if idx.completed != nil && node.GetTxn() == nil {
		idx.completed.Delete(appendPrepareEntry{prepare: node.Prepare, startRow: node.startRow})
		if idx.completed.Len() == 0 {
			idx.completed = nil
		}
	}
}

// selectNodes runs after transaction waits, with the MVCC read lock reacquired.
// Return one candidate without allocating; allocate scratch only for multiple
// matches. Dense ranges use one physical-order pass instead of sorting history.
func (idx *appendRangeIndex) selectNodes(start, end types.TS, limit int) (first *AppendNode, rest []*AppendNode, dense bool) {
	count := 0
	idx.visit(start, end, func(node *AppendNode) bool {
		if first == nil {
			first = node
		}
		count++
		dense = count >= limit
		return !dense
	})
	if dense || count < 2 {
		return
	}
	// Count before allocating: dense ranges need no candidate scratch at all.
	// The second sparse traversal shares the same read lock and stable nodes.
	rest = make([]*AppendNode, 0, count)
	idx.visit(start, end, func(node *AppendNode) bool {
		if node != first {
			rest = append(rest, node)
		}
		return true
	})
	return
}

func (idx *appendRangeIndex) visit(start, end types.TS, fn func(*AppendNode) bool) {
	for node := range idx.pending {
		if in, _ := node.PreparedIn(start, end); in && !fn(node) {
			return
		}
	}
	// Writers can finish between refresh's write unlock and the read relock.
	// Include that suffix directly; the tree and ready set remain disjoint.
	for _, node := range idx.ready {
		if node != nil {
			if in, _ := node.PreparedIn(start, end); in && !fn(node) {
				return
			}
		}
	}
	if idx.completed != nil {
		idx.completed.Ascend(appendPrepareEntry{prepare: start}, func(entry appendPrepareEntry) bool {
			if entry.prepare.GT(&end) {
				return false
			}
			if !entry.prepare.IsEmpty() {
				return fn(entry.node)
			}
			return true
		})
	}
	return
}

func (idx *appendRangeIndex) clear() { *idx = appendRangeIndex{} }

func (idx *appendRangeIndex) estimateMemSize() int {
	// Pending-map storage is bounded by its peak membership until history is
	// released. Ready slots are charged by capacity even after references clear.
	size := 64*idx.pendingPeak + 8*cap(idx.ready)
	if idx.pending != nil {
		size += 128 // Map header and the initial group.
	}
	if idx.completed != nil {
		// Degree 32: 24-byte entries, at least 31 per non-root node. Include
		// spare slots, node/child overhead, and fixed tree/root allocation.
		size += 256 + 64*idx.completed.Len()
	}
	return size
}
