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
)

type MVCCSlice[T txnif.MVCCNode[T]] struct {
	MVCC      []T
	comparefn func(T, T) int
	zero      T
}

func NewMVCCSlice[T txnif.MVCCNode[T]](newnodefn func() T,
	comparefn func(T, T) int) *MVCCSlice[T] {
	return &MVCCSlice[T]{
		MVCC:      make([]T, 0),
		comparefn: comparefn,
	}
}
func (be *MVCCSlice[T]) StringLocked() string {
	if be == nil {
		return "nil"
	}
	var w bytes.Buffer

	length := len(be.MVCC)
	for i := length - 1; i >= 0; i-- {
		version := be.MVCC[i]
		_, _ = w.WriteString(" -> \n")
		_, _ = w.WriteString(version.String())
	}
	return w.String()
}

// for replay
func (be *MVCCSlice[T]) GetTs() types.TS {
	return be.GetUpdateNodeLocked().GetEnd()
}

// func (be *MVCCSlice) GetTxn() txnif.TxnReader { return be.GetUpdateNodeLocked().GetTxn() }

func (be *MVCCSlice[T]) InsertNode(un T) {
	be.MVCC = append(be.MVCC, un)
}

// GetUpdateNode gets the latest UpdateNode.
// It is useful in making command, apply state(e.g. ApplyCommit),
// check confilct.
func (be *MVCCSlice[T]) GetUpdateNodeLocked() T {
	length := len(be.MVCC)
	if length == 0 {
		return be.zero
	}
	return be.MVCC[length-1]
}

// GetCommittedNode gets the latest committed UpdateNode.
// It's useful when check whether the catalog/metadata entry is deleted.
func (be *MVCCSlice[T]) GetCommittedNode() (node T) {
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
func (be *MVCCSlice[T]) DeleteNode(node T) {
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
func (be *MVCCSlice[T]) SearchNode(o T) (node T) {
	for _, n := range be.MVCC {
		if be.comparefn(n, o) == 0 {
			node = n
			break
		}
	}
	return
}

// func (be *MVCCSlice[T]) GetVisibleNode(ts types.TS) (node T) {
// 	length := len(be.MVCC)
// 	for i := length - 1; i >= 0; i-- {
// 		un := be.MVCC[i]
// 		var visible bool
// 		if visible = un.IsVisible(ts); visible {
// 			node = un
// 			break
// 		}
// 	}
// 	return
// }

func (be *MVCCSlice[T]) GetLastNonAbortedNode() (node T) {
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

func (be *MVCCSlice[T]) SearchNodeByTS(ts types.TS) (node T) {
	for _, n := range be.MVCC {
		startTS := n.GetStart()
		if startTS.Equal(&ts) {
			node = n
			break
		}
	}
	return
}
func (be *MVCCSlice[T]) ForEach(fn func(un T) bool, reverse bool) {
	if reverse {
		be.forEachReverse(fn)
	} else {
		be.forEach(fn)
	}
}
func (be *MVCCSlice[T]) forEach(fn func(un T) bool) {
	for i := len(be.MVCC) - 1; i >= 0; i-- {
		n := be.MVCC[i]
		gonext := fn(n)
		if !gonext {
			break
		}
	}
}
func (be *MVCCSlice[T]) forEachReverse(fn func(un T) bool) {
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
// if checkCommitted, it ignores uncommitted nodes
func (be *MVCCSlice[T]) GetNodeToReadByPrepareTS(ts types.TS) (offset int, node T) {
	if len(be.MVCC) == 0 {
		return 0, be.zero
	}
	lastAppend := be.MVCC[len(be.MVCC)-1]

	// 1. Last append node is in the window and it was already committed
	prepareTS := lastAppend.GetPrepare()
	if ts.Greater(&prepareTS) {
		return len(be.MVCC) - 1, lastAppend
	}
	start, end := 0, len(be.MVCC)-1
	var mid int
	for start <= end {
		mid = (start + end) / 2
		midPrepareTS := be.MVCC[mid].GetPrepare()
		if midPrepareTS.Less(&ts) {
			start = mid + 1
		} else if midPrepareTS.Greater(&ts) {
			end = mid - 1
		} else {
			break
		}
	}
	midPrepareTS := be.MVCC[mid].GetPrepare()
	visible := midPrepareTS.LessEq(&ts)
	if mid == 0 && !visible {
		// 2. The first node is found and it was committed after ts
		return 0, be.zero
	} else if mid != 0 && !visible {
		// 3. A node (not first) is found and it was committed after ts. Use the prev node
		mid = mid - 1
	}
	return mid, be.MVCC[mid]
}

func (be *MVCCSlice[T]) SearchNodeByCompareFn(fn func(a T) int) (offset int, node T) {
	if len(be.MVCC) == 0 {
		return 0, be.zero
	}
	lastAppend := be.MVCC[len(be.MVCC)-1]

	// 1. Last append node is in the window and it was already committed
	if fn(lastAppend) < 0 {
		return 0, be.zero
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
		return 0, be.zero
	}
	return mid, be.MVCC[mid]
}

func (be *MVCCSlice[T]) IsEmpty() bool {
	return len(be.MVCC) == 0
}

func (be *MVCCSlice[T]) IsCommitting() bool {
	node := be.GetUpdateNodeLocked()
	if node.IsNil() {
		return false
	}
	return node.IsCommitting()
}

func (be *MVCCSlice[T]) IsCommitted() bool {
	un := be.GetUpdateNodeLocked()
	if un.IsNil() {
		return false
	}
	return un.IsCommitted()
}

func (be *MVCCSlice[T]) LoopInRange(start, end types.TS, fn func(T) bool) {
	startOffset, node := be.GetNodeToReadByPrepareTS(start)
	prepareTS := node.GetPrepare()
	if node.IsNil() && prepareTS.Less(&start) {
		startOffset++
	}
	endOffset, node := be.GetNodeToReadByPrepareTS(end)
	if node.IsNil() {
		return
	}
	for i := endOffset; i >= startOffset; i-- {
		if !fn(be.MVCC[i]) {
			break
		}
	}
}

func (be *MVCCSlice[T]) LoopOffsetRange(start, end int, fn func(T) bool) {
	for i := start; i <= end; i++ {
		if !fn(be.MVCC[i]) {
			break
		}
	}
}

func (be *MVCCSlice[T]) GetNodeByOffset(offset int) T {
	return be.MVCC[offset]
}
