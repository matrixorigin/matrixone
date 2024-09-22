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
	"fmt"
	"io"

	"github.com/matrixorigin/matrixone/pkg/common/moerr"
	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/container/vector"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/containers"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/iface/txnif"
)

type TxnMVCCNode struct {
	Start, Prepare, End types.TS
	Txn                 txnif.TxnReader
	Aborted             bool
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
func NewTxnMVCCNodeWithStartEnd(start, end types.TS) *TxnMVCCNode {
	return &TxnMVCCNode{
		Start:   start,
		Prepare: end,
		End:     end,
	}
}
func (un *TxnMVCCNode) IsAborted() bool {
	return un.Aborted
}

// Check w-w confilct
func (un *TxnMVCCNode) CheckConflict(txn txnif.TxnReader) error {
	// If node is held by a active txn
	if !un.IsCommitted() {
		// No conflict if it is the same txn
		if un.IsSameTxn(txn) {
			return nil
		}
		return txnif.ErrTxnWWConflict
	}

	// For a committed node, it is w-w conflict if ts is lt the node commit ts
	// -------+-------------+-------------------->
	//        ts         CommitTs            time
	startTS := txn.GetStartTS()
	if un.End.GT(&startTS) {
		return txnif.ErrTxnWWConflict
	}
	return nil
}

// Check whether is mvcc node is visible to ts
// Make sure all the relevant prepared txns should be committed|rollbacked
func (un *TxnMVCCNode) IsVisible(txn txnif.TxnReader) (visible bool) {
	// Node is always visible to its born txn
	if un.IsSameTxn(txn) {
		return true
	}

	// The born txn of this node has not been commited|rollbacked
	if un.IsActive() || un.IsCommitting() {
		return false
	}

	// Node is visible if the commit ts is le ts
	startTS := txn.GetStartTS()
	if un.End.LE(&startTS) && !un.Aborted {
		return true
	}

	// Node is not invisible if the commit ts is gt ts
	return false

}
func (un *TxnMVCCNode) Reset() {
	un.Start = types.TS{}
	un.Prepare = types.TS{}
	un.End = types.TS{}
	un.Aborted = false
	un.Txn = nil
}
func (un *TxnMVCCNode) IsEmpty() bool {
	return un.Start.IsEmpty()
}

// Check whether is mvcc node is visible to ts
// Make sure all the relevant prepared txns should be committed|rollbacked
func (un *TxnMVCCNode) IsVisibleByTS(ts types.TS) (visible bool) {
	// The born txn of this node has not been commited|rollbacked
	if un.IsActive() || un.IsCommitting() {
		return false
	}

	// Node is visible if the commit ts is le ts
	if un.End.LE(&ts) && !un.Aborted {
		return true
	}

	// Node is not invisible if the commit ts is gt ts
	return false

}
func (un *TxnMVCCNode) GetPrepare() types.TS {
	if un.Txn != nil {
		return un.Txn.GetPrepareTS()
	}
	return un.Prepare
}

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
	if un.Prepare.LT(&minTS) {
		return false, true
	}

	// -------+--------------+------------+--------------->
	//        |              |            |             Time
	//       MinTs       PrepareTs       MaxTs
	// Created by other committed txn
	// true: prepared in range
	// false: not prepared before minTs
	if un.Prepare.GE(&minTS) && un.Prepare.LE(&maxTS) {
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
	if un.End.LT(&minTS) {
		return false, true
	}

	// -------+--------------+------------+--------------->
	//        |              |            |             Time
	//       MinTs     CommittedTS       MaxTs
	// Created by other committed txn
	// true: committed in range
	// false: not committed before minTs
	if un.End.GE(&minTS) && un.End.LE(&maxTS) {
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
	prepareTS := un.Txn.GetPrepareTS()
	if prepareTS.GE(&ts) {
		return false, nil
	}

	// --------+----------------+------------------------>
	//     PrepareTs            Ts                   Time
	// If ts is after the prepare ts. need to wait
	return true, un.Txn
}

func (un *TxnMVCCNode) IsSameTxn(txn txnif.TxnReader) bool {
	if un.Txn == nil {
		return false
	}
	// for appendnode test
	if txn == nil {
		return false
	}
	return un.Txn.GetID() == txn.GetID()
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
	if un.Start.LT(&o.Start) {
		return -1
	}
	if un.Start.Equal(&o.Start) {
		return 0
	}
	return 1
}

func (un *TxnMVCCNode) Compare2(o *TxnMVCCNode) int {
	if un.Prepare.LT(&o.Prepare) {
		return -1
	}
	if un.Prepare.Equal(&o.Prepare) {
		return un.Compare(o)
	}
	return 1
}

func (un *TxnMVCCNode) ApplyCommit(id string) (ts types.TS, err error) {
	if un.Txn == nil {
		err = moerr.NewTxnNotFoundNoCtx()
		return
	}
	if un.Txn.GetID() != id {
		err = moerr.NewMissingTxnNoCtx()
	}
	un.End = un.Txn.GetCommitTS()
	un.Txn = nil
	ts = un.End
	return
}

func (un *TxnMVCCNode) PrepareRollback() (err error) {
	un.Aborted = true
	return
}

func (un *TxnMVCCNode) ApplyRollback() (ts types.TS, err error) {
	ts = un.Txn.GetCommitTS()
	un.End = ts
	un.Txn = nil
	un.Aborted = true
	return
}

func (un *TxnMVCCNode) WriteTo(w io.Writer) (n int64, err error) {
	var sn1 int
	if sn1, err = w.Write(un.Start[:]); err != nil {
		return
	}
	n += int64(sn1)
	if sn1, err = w.Write(un.Prepare[:]); err != nil {
		return
	}
	n += int64(sn1)
	if sn1, err = w.Write(un.End[:]); err != nil {
		return
	}
	n += int64(sn1)
	return
}

func (un *TxnMVCCNode) ReadFrom(r io.Reader) (n int64, err error) {
	var sn int
	if sn, err = r.Read(un.Start[:]); err != nil {
		return
	}
	n += int64(sn)
	if sn, err = r.Read(un.Prepare[:]); err != nil {
		return
	}
	n += int64(sn)
	if sn, err = r.Read(un.End[:]); err != nil {
		return
	}
	n += int64(sn)
	return
}

func CompareTxnMVCCNode(e, o *TxnMVCCNode) int {
	return e.Compare(o)
}

func (un *TxnMVCCNode) Update(o *TxnMVCCNode) {
	if !un.Start.Equal(&o.Start) {
		panic(fmt.Sprintf("logic err, expect %s, start at %s", un.Start.ToString(), o.Start.ToString()))
	}
	if !un.Prepare.Equal(&o.Prepare) {
		panic(fmt.Sprintf("logic err expect %s, prepare at %s", un.Prepare.ToString(), o.Prepare.ToString()))
	}
}

func (un *TxnMVCCNode) CloneAll() *TxnMVCCNode {
	n := &TxnMVCCNode{}
	n.Start = un.Start
	n.Prepare = un.Prepare
	n.End = un.End
	n.Txn = un.Txn
	return n
}

func (un *TxnMVCCNode) String() string {
	return fmt.Sprintf("[%s,%s][Committed(%v)]",
		un.Start.ToString(),
		un.End.ToString(), un.Txn == nil)
}

func (un *TxnMVCCNode) PrepareCommit() (ts types.TS, err error) {
	if un.Txn == nil {
		err = moerr.NewTxnNotFoundNoCtx()
		return
	}
	un.Prepare = un.Txn.GetPrepareTS()
	ts = un.Prepare
	return
}

func (un *TxnMVCCNode) AppendTuple(bat *containers.Batch) {
	startTSVec := bat.GetVectorByName(SnapshotAttr_StartTS)
	vector.AppendFixed(
		startTSVec.GetDownstreamVector(),
		un.Start,
		false,
		startTSVec.GetAllocator(),
	)
	vector.AppendFixed(
		bat.GetVectorByName(SnapshotAttr_PrepareTS).GetDownstreamVector(),
		un.Prepare,
		false,
		startTSVec.GetAllocator(),
	)
	vector.AppendFixed(
		bat.GetVectorByName(SnapshotAttr_CommitTS).GetDownstreamVector(),
		un.End,
		false,
		startTSVec.GetAllocator(),
	)
}

// In push model, logtail is prepared before committing txn,
// un.End is txnif.Uncommit
func (un *TxnMVCCNode) AppendTupleWithCommitTS(bat *containers.Batch, commitTS types.TS) {
	startTSVec := bat.GetVectorByName(SnapshotAttr_StartTS)
	vector.AppendFixed(
		startTSVec.GetDownstreamVector(),
		un.Start,
		false,
		startTSVec.GetAllocator(),
	)
	vector.AppendFixed(
		bat.GetVectorByName(SnapshotAttr_PrepareTS).GetDownstreamVector(),
		un.Prepare,
		false,
		startTSVec.GetAllocator(),
	)
	vector.AppendFixed(
		bat.GetVectorByName(SnapshotAttr_CommitTS).GetDownstreamVector(),
		commitTS,
		false,
		startTSVec.GetAllocator(),
	)
}

func (un *TxnMVCCNode) ReadTuple(bat *containers.Batch, offset int) {
	// TODO
}

func ReadTuple(bat *containers.Batch, row int) (un *TxnMVCCNode) {
	end := bat.GetVectorByName(SnapshotAttr_CommitTS).Get(row).(types.TS)
	start := bat.GetVectorByName(SnapshotAttr_StartTS).Get(row).(types.TS)
	prepare := bat.GetVectorByName(SnapshotAttr_PrepareTS).Get(row).(types.TS)
	un = &TxnMVCCNode{
		Start:   start,
		Prepare: prepare,
		End:     end,
	}
	return
}
