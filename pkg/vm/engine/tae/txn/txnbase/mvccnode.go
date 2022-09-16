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
	"encoding/binary"
	"fmt"
	"io"

	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/iface/txnif"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/wal"
)

type MVCCNode interface {
	String() string

	IsVisible(ts types.TS) (visible bool)
	CheckConflict(ts types.TS) error
	UpdateNode(o MVCCNode)

	PreparedIn(minTS, maxTS types.TS) (in, before bool)
	CommittedIn(minTS, maxTS types.TS) (in, before bool)
	NeedWaitCommitting(ts types.TS) (bool, txnif.TxnReader)
	IsSameTxn(ts types.TS) bool
	IsActive() bool
	IsCommitting() bool
	IsCommitted() bool

	GetEnd() types.TS
	GetPrepare() types.TS
	GetTxn() txnif.TxnReader
	AddLogIndex(idx *wal.Index)
	GetLogIndex() []*wal.Index

	ApplyCommit(index *wal.Index) (err error)
	ApplyRollback(index *wal.Index) (err error)
	PrepareCommit() (err error)

	WriteTo(w io.Writer) (n int64, err error)
	ReadFrom(r io.Reader) (n int64, err error)
	CloneData() MVCCNode
	CloneAll() MVCCNode
}

// TODO prepare ts
type TxnMVCCNode struct {
	Start, Prepare, End types.TS
	Txn                 txnif.TxnReader
	//Aborted bool
	//State txnif.TxnState
	LogIndex []*wal.Index
}

func NewTxnMVCCNodeWithTxn(txn txnif.TxnReader) *TxnMVCCNode {
	var ts types.TS
	if txn != nil {
		ts = txn.GetStartTS()
	}
	return &TxnMVCCNode{
		Start: ts,
		Txn:   txn,
	}
}

// Check w-w confilct
func (un *TxnMVCCNode) CheckConflict(ts types.TS) error {
	// If node is held by a active txn
	if !un.IsCommitted() {
		// No conflict if it is the same txn
		if un.IsSameTxn(ts) {
			return nil
		}
		return txnif.ErrTxnWWConflict
	}

	// For a committed node, it is w-w conflict if ts is lt the node commit ts
	// -------+-------------+-------------------->
	//        ts         CommitTs            time
	if un.End.Greater(ts) {
		return txnif.ErrTxnWWConflict
	}
	return nil
}

// Check whether is mvcc node is visible to ts
// Make sure all the relevant prepared txns should be committed|rollbacked
func (un *TxnMVCCNode) IsVisible(ts types.TS) (visible bool) {
	// Node is always visible to its born txn
	if un.IsSameTxn(ts) {
		return true
	}

	// The born txn of this node has not been commited|rollbacked
	if un.IsActive() || un.IsCommitting() {
		return false
	}

	// Node is visible if the commit ts is le ts
	if un.End.LessEq(ts) {
		return true
	}

	// Node is invisible if the commit ts is gt ts
	return false

}
func (un *TxnMVCCNode) GetPrepare() types.TS { return un.Prepare }

func (un *TxnMVCCNode) PreparedIn(minTS, maxTS types.TS) (in, before bool) {
	// -------+----------+----------------+--------------->
	//        |          |                |             Time
	//       MinTS     MaxTs       Prepare In future
	// Created by other active txn
	// false: not prepared in range
	// false: not prepared before minTs
	if un.Prepare.IsEmpty() {
		return false, false
	}

	// -------+--------------+------------+--------------->
	//        |              |            |             Time
	//    PrepareTs        MinTs         MaxTs
	// Created by other committed txn
	// false: not prepared in range
	// true: prepared before minTs
	if un.Prepare.Less(minTS) {
		return false, true
	}

	// -------+--------------+------------+--------------->
	//        |              |            |             Time
	//       MinTs       PrepareTs       MaxTs
	// Created by other committed txn
	// true: prepared in range
	// false: not prepared before minTs
	if un.Prepare.GreaterEq(minTS) && un.Prepare.LessEq(maxTS) {
		return true, false
	}

	// -------+--------------+------------+--------------->
	//        |              |            |             Time
	//       MinTs          MaxTs     PrepareTs
	// Created by other committed txn
	// false: not prepared in range
	// false: not prepared before minTs
	return false, false
}

// in indicates whether this node is committed in between [minTs, maxTs]
// before indicates whether this node is committed before minTs
// NeedWaitCommitting should be called before to make sure all prepared active
// txns in between [minTs, maxTs] be committed or rollbacked
func (un *TxnMVCCNode) CommittedIn(minTS, maxTS types.TS) (in, before bool) {
	// -------+----------+----------------+--------------->
	//        |          |                |             Time
	//       MinTS     MaxTs       Commit In future
	// Created by other active txn
	// false: not committed in range
	// false: not committed before minTs
	if un.End.IsEmpty() {
		return false, false
	}

	// -------+--------------+------------+--------------->
	//        |              |            |             Time
	//    CommittedTS     MinTs         MaxTs
	// Created by other committed txn
	// false: not committed in range
	// true: committed before minTs
	if un.End.Less(minTS) {
		return false, true
	}

	// -------+--------------+------------+--------------->
	//        |              |            |             Time
	//       MinTs     CommittedTS       MaxTs
	// Created by other committed txn
	// true: committed in range
	// false: not committed before minTs
	if un.End.GreaterEq(minTS) && un.End.LessEq(maxTS) {
		return true, false
	}

	// -------+--------------+------------+--------------->
	//        |              |            |             Time
	//       MinTs          MaxTs     CommittedTs
	// Created by other committed txn
	// false: not committed in range
	// false: not committed before minTs
	return false, false
}

// Check whether need to wait this mvcc node
func (un *TxnMVCCNode) NeedWaitCommitting(ts types.TS) (bool, txnif.TxnReader) {
	// If this node is active, not to wait
	// If this node is committed|rollbacked, not to wait
	if !un.IsCommitting() {
		return false, nil
	}

	// --------+----------------+------------------------>
	//         Ts           PrepareTs                Time
	// If ts is before the prepare ts. not to wait
	if un.Txn.GetPrepareTS().GreaterEq(ts) {
		return false, nil
	}

	// --------+----------------+------------------------>
	//     PrepareTs            Ts                   Time
	// If ts is before the prepare ts. not to wait
	return true, un.Txn
}

func (un *TxnMVCCNode) IsSameTxn(ts types.TS) bool {
	if un.Txn == nil {
		return false
	}
	return un.Txn.GetStartTS().Equal(ts)
}

func (un *TxnMVCCNode) IsActive() bool {
	if un.Txn == nil {
		return false
	}
	return un.Txn.GetTxnState(false) == txnif.TxnStateActive
}

func (un *TxnMVCCNode) IsCommitting() bool {
	if un.Txn == nil {
		return false
	}
	return un.Txn.GetTxnState(false) != txnif.TxnStateActive
}

func (un *TxnMVCCNode) IsCommitted() bool {
	return un.Txn == nil
}

func (un *TxnMVCCNode) GetStart() types.TS {
	return un.Start
}

func (un *TxnMVCCNode) GetEnd() types.TS {
	return un.End
}

func (un *TxnMVCCNode) GetTxn() txnif.TxnReader {
	return un.Txn
}

func (un *TxnMVCCNode) Compare(o *TxnMVCCNode) int {
	if un.Start.Less(o.Start) {
		return -1
	}
	if un.Start.Equal(o.Start) {
		return 0
	}
	return 1
}

func (un *TxnMVCCNode) AddLogIndex(idx *wal.Index) {
	if un.LogIndex == nil {
		un.LogIndex = make([]*wal.Index, 0)
	}
	un.LogIndex = append(un.LogIndex, idx)

}
func (un *TxnMVCCNode) GetLogIndex() []*wal.Index {
	return un.LogIndex
}
func (un *TxnMVCCNode) ApplyCommit(index *wal.Index, deleteTxn bool) (ts types.TS, err error) {
	un.End = un.Txn.GetCommitTS()
	if deleteTxn {
		un.Txn = nil
	}
	un.AddLogIndex(index)
	ts = un.End
	return
}
func (un *TxnMVCCNode) OnReplayCommit(ts types.TS) {
	un.End = ts
}
func (un *TxnMVCCNode) ApplyRollback(index *wal.Index) (err error) {
	un.End = un.Txn.GetCommitTS()
	un.Txn = nil
	un.AddLogIndex(index)
	return
}

func (un *TxnMVCCNode) WriteTo(w io.Writer) (n int64, err error) {
	if err = binary.Write(w, binary.BigEndian, un.Start); err != nil {
		return
	}
	n += 12
	if err = binary.Write(w, binary.BigEndian, un.Prepare); err != nil {
		return
	}
	n += 12
	if err = binary.Write(w, binary.BigEndian, un.End); err != nil {
		return
	}
	n += 12
	var sn int64
	length := uint32(len(un.LogIndex))
	if err = binary.Write(w, binary.BigEndian, length); err != nil {
		return
	}
	n += 4
	for _, idx := range un.LogIndex {
		sn, err = idx.WriteTo(w)
		if err != nil {
			return
		}
		n += sn
	}
	return
}

func (un *TxnMVCCNode) ReadFrom(r io.Reader) (n int64, err error) {
	if err = binary.Read(r, binary.BigEndian, &un.Start); err != nil {
		return
	}
	n += 12
	if err = binary.Read(r, binary.BigEndian, &un.Prepare); err != nil {
		return
	}
	n += 12
	if err = binary.Read(r, binary.BigEndian, &un.End); err != nil {
		return
	}
	n += 12

	length := uint32(0)
	if err = binary.Read(r, binary.BigEndian, &length); err != nil {
		return
	}
	n += 4
	if length != 0 {
		un.LogIndex = make([]*wal.Index, 0)
	}
	var sn int64
	for i := 0; i < int(length); i++ {
		idx := new(wal.Index)
		sn, err = idx.ReadFrom(r)
		if err != nil {
			return
		}
		n += sn
		un.LogIndex = append(un.LogIndex, idx)
	}
	return
}

func CompareTxnMVCCNode(e, o *TxnMVCCNode) int {
	return e.Compare(o)
}

func (un *TxnMVCCNode) UpdateNode(o *TxnMVCCNode) {
	if !un.Start.Equal(o.Start) {
		panic("logic err")
	}
	if !un.End.Equal(o.End) {
		panic("logic err")
	}
	un.AddLogIndex(o.LogIndex[0])
}

func (un *TxnMVCCNode) CloneAll() *TxnMVCCNode {
	n := &TxnMVCCNode{}
	n.Start = un.Start
	n.Prepare = un.Prepare
	n.End = un.End
	if len(un.LogIndex) != 0 {
		n.LogIndex = make([]*wal.Index, 0)
		for _, idx := range un.LogIndex {
			n.LogIndex = append(n.LogIndex, idx.Clone())
		}
	}
	return n
}

func (un *TxnMVCCNode) String() string {
	return fmt.Sprintf("[%v,%v,%v][logIndex=%v]",
		un.Start,
		un.Prepare,
		un.End,
		un.LogIndex)
}

func (un *TxnMVCCNode) PrepareCommit() (ts types.TS, err error) {
	un.Prepare = un.Txn.GetPrepareTS()
	ts = un.Prepare
	return
}
