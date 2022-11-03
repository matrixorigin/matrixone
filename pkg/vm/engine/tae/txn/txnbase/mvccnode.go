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

	"github.com/matrixorigin/matrixone/pkg/common/moerr"
	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/containers"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/iface/txnif"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/logstore/store"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/wal"
)

type TxnMVCCNode struct {
	Start, Prepare, End types.TS
	Txn                 txnif.TxnReader
	Aborted             bool
	is1PC               bool // for subTxn
	LogIndex            *wal.Index
}

var (
	SnapshotAttr_StartTS       = "start_ts"
	SnapshotAttr_PrepareTS     = "prepare_ts"
	SnapshotAttr_CommitTS      = "commit_ts"
	SnapshotAttr_LogIndex_LSN  = "log_index_lsn"
	SnapshotAttr_LogIndex_CSN  = "log_index_csn"
	SnapshotAttr_LogIndex_Size = "log_index_size"
)

func NewTxnMVCCNodeWithTxn(txn txnif.TxnReader) *TxnMVCCNode {
	var ts types.TS
	if txn != nil {
		ts = txn.GetStartTS()
	}
	return &TxnMVCCNode{
		Start:   ts,
		Prepare: txnif.UncommitTS,
		End:     txnif.UncommitTS,
		Txn:     txn,
	}
}
func NewTxnMVCCNodeWithTS(ts types.TS) *TxnMVCCNode {
	return &TxnMVCCNode{
		Start:   ts,
		Prepare: ts,
		End:     ts,
	}
}
func (un *TxnMVCCNode) IsAborted() bool {
	return un.Aborted
}
func (un *TxnMVCCNode) Is1PC() bool {
	return un.is1PC
}
func (un *TxnMVCCNode) Set1PC() {
	un.is1PC = true
}

// Check w-w confilct
func (un *TxnMVCCNode) CheckConflict(ts types.TS) error {
	// If node is held by a active txn
	if !un.IsCommitted() {
		// No conflict if it is the same txn
		if un.IsSameTxn(ts) {
			return nil
		}
		return moerr.NewTxnWWConflict()
	}

	// For a committed node, it is w-w conflict if ts is lt the node commit ts
	// -------+-------------+-------------------->
	//        ts         CommitTs            time
	if un.End.Greater(ts) {
		return moerr.NewTxnWWConflict()
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
	if un.End.LessEq(ts) && !un.Aborted {
		return true
	}

	// Node is not invisible if the commit ts is gt ts
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
	// If ts is after the prepare ts. need to wait
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

func (un *TxnMVCCNode) SetLogIndex(idx *wal.Index) {
	un.LogIndex = idx

}
func (un *TxnMVCCNode) GetLogIndex() *wal.Index {
	return un.LogIndex
}
func (un *TxnMVCCNode) ApplyCommit(index *wal.Index) (ts types.TS, err error) {
	if un.Is1PC() {
		un.End = un.Txn.GetPrepareTS()
	} else {
		un.End = un.Txn.GetCommitTS()
	}
	if index != nil {
		un.SetLogIndex(index)
	}
	un.Txn = nil
	ts = un.End
	return
}

func (un *TxnMVCCNode) ApplyRollback(index *wal.Index) (ts types.TS, err error) {
	ts = un.Txn.GetCommitTS()
	un.End = ts
	un.Txn = nil
	un.Aborted = true
	if index != nil {
		un.SetLogIndex(index)
	}
	return
}

func (un *TxnMVCCNode) WriteTo(w io.Writer) (n int64, err error) {
	if err = binary.Write(w, binary.BigEndian, un.Start); err != nil {
		return
	}
	n += types.TxnTsSize
	if err = binary.Write(w, binary.BigEndian, un.Prepare); err != nil {
		return
	}
	n += types.TxnTsSize
	if err = binary.Write(w, binary.BigEndian, un.End); err != nil {
		return
	}
	n += types.TxnTsSize
	var sn int64
	logIndex := un.LogIndex
	if logIndex == nil {
		logIndex = new(wal.Index)
	}
	sn, err = logIndex.WriteTo(w)
	if err != nil {
		return
	}
	n += sn
	is1PC := uint8(0)
	if un.is1PC {
		is1PC = 1
	}
	if err = binary.Write(w, binary.BigEndian, is1PC); err != nil {
		return
	}
	n += 1
	return
}

func (un *TxnMVCCNode) ReadFrom(r io.Reader) (n int64, err error) {
	if err = binary.Read(r, binary.BigEndian, &un.Start); err != nil {
		return
	}
	n += types.TxnTsSize
	if err = binary.Read(r, binary.BigEndian, &un.Prepare); err != nil {
		return
	}
	n += types.TxnTsSize
	if err = binary.Read(r, binary.BigEndian, &un.End); err != nil {
		return
	}
	n += types.TxnTsSize

	var sn int64
	un.LogIndex = &store.Index{}
	sn, err = un.LogIndex.ReadFrom(r)
	if err != nil {
		return
	}
	n += sn

	is1PC := uint8(0)
	if err = binary.Read(r, binary.BigEndian, &is1PC); err != nil {
		return
	}
	n += 1
	if is1PC == 1 {
		un.is1PC = true
	}
	return
}

func CompareTxnMVCCNode(e, o *TxnMVCCNode) int {
	return e.Compare(o)
}

func (un *TxnMVCCNode) Update(o *TxnMVCCNode) {
	if !un.Start.Equal(o.Start) {
		panic(fmt.Sprintf("logic err, expect %s, start at %s", un.Start.ToString(), o.Start.ToString()))
	}
	if !un.Prepare.Equal(o.Prepare) {
		panic(fmt.Sprintf("logic err expect %s, prepare at %s", un.Prepare.ToString(), o.Prepare.ToString()))
	}
	un.LogIndex = o.LogIndex
}

func (un *TxnMVCCNode) CloneAll() *TxnMVCCNode {
	n := &TxnMVCCNode{}
	n.Start = un.Start
	n.Prepare = un.Prepare
	n.End = un.End
	n.LogIndex = un.LogIndex.Clone()
	return n
}

func (un *TxnMVCCNode) String() string {
	return fmt.Sprintf("[%s,%s,%s][%v]",
		un.Start.ToString(),
		un.Prepare.ToString(),
		un.End.ToString(),
		un.LogIndex)
}

func (un *TxnMVCCNode) PrepareCommit() (ts types.TS, err error) {
	un.Prepare = un.Txn.GetPrepareTS()
	ts = un.Prepare
	return
}

func (un *TxnMVCCNode) AppendTuple(bat *containers.Batch) {
	bat.GetVectorByName(SnapshotAttr_StartTS).Append(un.Start)
	bat.GetVectorByName(SnapshotAttr_PrepareTS).Append(un.Prepare)
	bat.GetVectorByName(SnapshotAttr_CommitTS).Append(un.End)
	if un.LogIndex != nil {
		bat.GetVectorByName(SnapshotAttr_LogIndex_LSN).Append(un.LogIndex.LSN)
		bat.GetVectorByName(SnapshotAttr_LogIndex_CSN).Append(un.LogIndex.CSN)
		bat.GetVectorByName(SnapshotAttr_LogIndex_Size).Append(un.LogIndex.Size)
	} else {
		bat.GetVectorByName(SnapshotAttr_LogIndex_LSN).Append(uint64(0))
		bat.GetVectorByName(SnapshotAttr_LogIndex_CSN).Append(uint32(0))
		bat.GetVectorByName(SnapshotAttr_LogIndex_Size).Append(uint32(0))
	}
}

func (un *TxnMVCCNode) ReadTuple(bat *containers.Batch, offset int) {
	// TODO
}

func ReadTuple(bat *containers.Batch, row int) (un *TxnMVCCNode) {
	end := bat.GetVectorByName(SnapshotAttr_CommitTS).Get(row).(types.TS)
	start := bat.GetVectorByName(SnapshotAttr_StartTS).Get(row).(types.TS)
	prepare := bat.GetVectorByName(SnapshotAttr_PrepareTS).Get(row).(types.TS)
	lsn := bat.GetVectorByName(SnapshotAttr_LogIndex_LSN).Get(row).(uint64)
	var logIndex *wal.Index
	if lsn != 0 {
		logIndex = &wal.Index{
			LSN:  lsn,
			CSN:  bat.GetVectorByName(SnapshotAttr_LogIndex_CSN).Get(row).(uint32),
			Size: bat.GetVectorByName(SnapshotAttr_LogIndex_Size).Get(row).(uint32),
		}
	}
	un = &TxnMVCCNode{
		Start:    start,
		Prepare:  prepare,
		End:      end,
		LogIndex: logIndex,
	}
	return
}
