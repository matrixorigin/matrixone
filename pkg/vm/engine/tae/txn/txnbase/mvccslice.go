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

package txnbase

import (
	"bytes"

	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/iface/txnif"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/wal"
)

type MVCCSlice struct {
	MVCC      []txnif.MVCCNode
	comparefn func(txnif.MVCCNode, txnif.MVCCNode) int
}

func NewMVCCSlice(newnodefn func() txnif.MVCCNode,
	comparefn func(txnif.MVCCNode, txnif.MVCCNode) int) *MVCCSlice {
	return &MVCCSlice{
		MVCC:      make([]txnif.MVCCNode, 0),
		comparefn: comparefn,
	}
}
func (be *MVCCSlice) StringLocked() string {
	var w bytes.Buffer

	length := len(be.MVCC)
	for i := length - 1; i >= 0; i-- {
		version := be.MVCC[i]
		_, _ = w.WriteString(" -> ")
		_, _ = w.WriteString(version.String())
	}
	return w.String()
}

// for replay
func (be *MVCCSlice) GetTs() types.TS {
	return be.GetUpdateNodeLocked().GetEnd()
}

// func (be *MVCCSlice) GetTxn() txnif.TxnReader { return be.GetUpdateNodeLocked().GetTxn() }

func (be *MVCCSlice) InsertNode(un txnif.MVCCNode) {
	be.MVCC = append(be.MVCC, un)
}

// GetUpdateNode gets the latest UpdateNode.
// It is useful in making command, apply state(e.g. ApplyCommit),
// check confilct.
func (be *MVCCSlice) GetUpdateNodeLocked() txnif.MVCCNode {
	length := len(be.MVCC)
	if length == 0 {
		return nil
	}
	return be.MVCC[length-1]
}

// GetCommittedNode gets the latest committed UpdateNode.
// It's useful when check whether the catalog/metadata entry is deleted.
func (be *MVCCSlice) GetCommittedNode() (node txnif.MVCCNode) {
	length := len(be.MVCC)
	for i := length - 1; i >= 0; i-- {
		un := be.MVCC[i]
		if !un.IsActive() && !un.IsCommitting() {
			node = un
			break
		}
	}
	return
}
func (be *MVCCSlice) DeleteNode(node txnif.MVCCNode) {
	length := len(be.MVCC)
	for i := length - 1; i >= 0; i-- {
		un := be.MVCC[i]
		compare := be.comparefn(un, node)
		if compare == 0 {
			be.MVCC = append(be.MVCC[:i], be.MVCC[i+1:]...)
			break
		} else if compare < 0 {
			break
		}
	}
}
func (be *MVCCSlice) SearchNode(o txnif.MVCCNode) (node txnif.MVCCNode) {
	for _, n := range be.MVCC {
		if be.comparefn(n, o) == 0 {
			node = n
			break
		}
	}
	return
}

func (be *MVCCSlice) GetVisibleNode(ts types.TS) (node txnif.MVCCNode) {
	length := len(be.MVCC)
	for i := length - 1; i >= 0; i-- {
		un := be.MVCC[i]
		var visible bool
		if visible = un.IsVisible(ts); visible {
			node = un
			break
		}
	}
	return
}

func (be *MVCCSlice) GetLastNonAbortedNode() (node txnif.MVCCNode) {
	length := len(be.MVCC)
	for i := length - 1; i >= 0; i-- {
		un := be.MVCC[i]
		if !un.IsAborted() {
			node = un
			break
		}
	}
	return
}

func (be *MVCCSlice) SearchNodeByTS(ts types.TS) (node txnif.MVCCNode) {
	for _, n := range be.MVCC {
		if n.GetStart().Equal(ts) {
			node = n
			break
		}
	}
	return
}
func (be *MVCCSlice) ForEach(fn func(un txnif.MVCCNode) bool, reverse bool) {
	if reverse {
		be.forEachReverse(fn)
	} else {
		be.forEach(fn)
	}
}
func (be *MVCCSlice) Close() {
	be.forEach(func(un txnif.MVCCNode) bool {
		un.Close()
		return true
	})
}
func (be *MVCCSlice) forEach(fn func(un txnif.MVCCNode) bool) {
	for i := len(be.MVCC) - 1; i >= 0; i-- {
		n := be.MVCC[i]
		gonext := fn(n)
		if !gonext {
			break
		}
	}
}
func (be *MVCCSlice) forEachReverse(fn func(un txnif.MVCCNode) bool) {
	for _, n := range be.MVCC {
		gonext := fn(n)
		if !gonext {
			break
		}
	}
}

// GetNodeToRead gets UpdateNode according to the timestamp.
// It returns the UpdateNode in the same txn as the read txn
// or returns the latest UpdateNode with commitTS less than the timestamp.
// todo getend or getcommitts
func (be *MVCCSlice) GetNodeToReadByPrepareTS(ts types.TS) (offset int, node txnif.MVCCNode) {
	if len(be.MVCC) == 0 {
		return 0, nil
	}
	lastAppend := be.MVCC[len(be.MVCC)-1]

	// 1. Last append node is in the window and it was already committed
	if ts.Greater(lastAppend.GetPrepare()) {
		return len(be.MVCC) - 1, lastAppend
	}
	start, end := 0, len(be.MVCC)-1
	var mid int
	for start <= end {
		mid = (start + end) / 2
		if be.MVCC[mid].GetPrepare().Less(ts) {
			start = mid + 1
		} else if be.MVCC[mid].GetPrepare().Greater(ts) {
			end = mid - 1
		} else {
			break
		}
	}
	if mid == 0 && be.MVCC[mid].GetPrepare().Greater(ts) {
		// 2. The first node is found and it was committed after ts
		return 0, nil
	} else if mid != 0 && be.MVCC[mid].GetPrepare().Greater(ts) {
		// 3. A node (not first) is found and it was committed after ts. Use the prev node
		mid = mid - 1
	}
	return mid, be.MVCC[mid]
}

func (be *MVCCSlice) SearchNodeByCompareFn(fn func(a txnif.MVCCNode) int) (offset int, node txnif.MVCCNode) {
	if len(be.MVCC) == 0 {
		return 0, nil
	}
	lastAppend := be.MVCC[len(be.MVCC)-1]

	// 1. Last append node is in the window and it was already committed
	if fn(lastAppend) < 0 {
		return 0, nil
	}
	start, end := 0, len(be.MVCC)-1
	var mid int
	for start <= end {
		mid = (start + end) / 2
		if fn(be.MVCC[mid]) < 0 {
			start = mid + 1
		} else if fn(be.MVCC[mid]) > 0 {
			end = mid - 1
		} else {
			break
		}
	}
	if fn(be.MVCC[mid]) != 0 {
		return 0, nil
	}
	return mid, be.MVCC[mid]
}

func (be *MVCCSlice) IsEmpty() bool {
	return len(be.MVCC) == 0
}

func (be *MVCCSlice) IsCommitting() bool {
	node := be.GetUpdateNodeLocked()
	if node == nil {
		return false
	}
	return node.IsCommitting()
}

func (be *MVCCSlice) IsCommitted() bool {
	un := be.GetUpdateNodeLocked()
	if un == nil {
		return false
	}
	return un.IsCommitted()
}

func (be *MVCCSlice) LoopInRange(start, end types.TS, fn func(txnif.MVCCNode) bool) (indexes []*wal.Index) {
	startOffset, node := be.GetNodeToReadByPrepareTS(start)
	if node != nil && node.GetPrepare().Less(start) {
		startOffset++
	}
	endOffset, node := be.GetNodeToReadByPrepareTS(end)
	if node == nil {
		return nil
	}
	for i := endOffset; i >= startOffset; i-- {
		if !fn(be.MVCC[i]) {
			break
		}
	}
	return
}

func (be *MVCCSlice) LoopOffsetRange(start, end int, fn func(txnif.MVCCNode) bool) {
	for i := start; i <= end; i++ {
		if !fn(be.MVCC[i]) {
			break
		}
	}
}

func (be *MVCCSlice) GetNodeByOffset(offset int) txnif.MVCCNode {
	return be.MVCC[offset]
}
