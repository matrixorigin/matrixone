package catalog

import (
	"encoding/binary"
	"fmt"
	"io"
	"sync"

	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/iface/txnif"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/wal"
)

func CompareUint64(left, right uint64) int {
	if left > right {
		return 1
	} else if left < right {
		return -1
	}
	return 0
}

type Waitable interface {
	Wait() error
}

type waitable struct {
	fn func() error
}

func (w *waitable) Wait() error {
	return w.fn()
}

type CommitInfo struct {
	CurrOp   OpT
	Txn      txnif.TxnReader
	LogIndex *wal.Index
}

func (info *CommitInfo) WriteTo(w io.Writer) (err error) {
	if err = binary.Write(w, binary.BigEndian, info.CurrOp); err != nil {
		return
	}
	return
}

func (info *CommitInfo) ReadFrom(r io.Reader) (err error) {
	if err = binary.Read(r, binary.BigEndian, &info.CurrOp); err != nil {
		return
	}
	return
}

func (info *CommitInfo) Clone() *CommitInfo {
	return &CommitInfo{
		CurrOp:   info.CurrOp,
		LogIndex: info.LogIndex.Clone(),
	}
}

type BaseEntry struct {
	*sync.RWMutex
	CommitInfo
	PrevCommit         *CommitInfo
	ID                 uint64
	CreateAt, DeleteAt uint64
}

func (be *BaseEntry) MaxCommittedTS() uint64 {
	if be.Txn == nil {
		if be.DeleteAt != 0 {
			return be.DeleteAt
		}
		return be.CreateAt
	}
	if be.CreateAt != be.Txn.GetCommitTS() {
		return be.CreateAt
	}
	return 0
}

func (be *BaseEntry) CloneCreate() *BaseEntry {
	info := be.PrevCommit.Clone()
	cloned := &BaseEntry{
		CommitInfo: *info,
		ID:         be.ID,
		CreateAt:   be.CreateAt,
	}
	return cloned
}

func (be *BaseEntry) Clone() *BaseEntry {
	info := be.CommitInfo.Clone()
	cloned := &BaseEntry{
		CommitInfo: *info,
		ID:         be.ID,
		CreateAt:   be.CreateAt,
		DeleteAt:   be.DeleteAt,
	}
	return cloned
}

func (be *BaseEntry) WriteTo(w io.Writer) (err error) {
	if err = binary.Write(w, binary.BigEndian, be.ID); err != nil {
		return
	}
	if err = binary.Write(w, binary.BigEndian, be.CreateAt); err != nil {
		return
	}
	if err = binary.Write(w, binary.BigEndian, be.DeleteAt); err != nil {
		return
	}
	if err = be.CommitInfo.WriteTo(w); err != nil {
		return
	}
	return
}

func (be *BaseEntry) ReadFrom(r io.Reader) (err error) {
	if err = binary.Read(r, binary.BigEndian, &be.ID); err != nil {
		return
	}
	if err = binary.Read(r, binary.BigEndian, &be.CreateAt); err != nil {
		return
	}
	if err = binary.Read(r, binary.BigEndian, &be.DeleteAt); err != nil {
		return
	}
	return be.CommitInfo.ReadFrom(r)
}

func (be *BaseEntry) GetTxn() txnif.TxnReader { return be.Txn }

func (be *BaseEntry) IsTerminated(waitIfcommitting bool) bool {
	return be.Txn.IsTerminated(waitIfcommitting)
}

func (be *BaseEntry) IsCommitted() bool {
	return be.Txn == nil && be.CreateAt > 0
	// if be.Txn == nil {
	// 	return true
	// }
	// state := be.Txn.GetTxnState(true)
	// return state == txnif.TxnStateCommitted || state == txnif.TxnStateRollbacked
}

func (be *BaseEntry) GetID() uint64 { return be.ID }

func (be *BaseEntry) DoCompre(oe *BaseEntry) int {
	be.RLock()
	defer be.RUnlock()
	oe.RLock()
	defer oe.RUnlock()
	r := 0
	if be.CreateAt != 0 && oe.CreateAt != 0 {
		r = CompareUint64(be.CreateAt, oe.CreateAt)
	} else if be.CreateAt != 0 {
		r = -1
	} else if oe.CreateAt != 0 {
		r = 1
	} else {
		r = CompareUint64(be.Txn.GetStartTS(), oe.Txn.GetStartTS())
	}
	return r
}

func (be *BaseEntry) PrepareCommit() error {
	be.Lock()
	defer be.Unlock()
	if be.CreateAt == 0 {
		be.CreateAt = be.Txn.GetCommitTS()
	}
	if be.CurrOp == OpSoftDelete {
		be.DeleteAt = be.Txn.GetCommitTS()
	}
	return nil
}

func (be *BaseEntry) PrepareRollback() error {
	be.Lock()
	if be.PrevCommit != nil {
		be.CurrOp = be.PrevCommit.CurrOp
		be.LogIndex = be.PrevCommit.LogIndex
	}
	be.Txn = nil
	be.Unlock()
	return nil
}

func (be *BaseEntry) ApplyRollback() error {
	return nil
}

func (be *BaseEntry) ApplyCommit(index *wal.Index) error {
	be.Lock()
	defer be.Unlock()
	// if be.Txn == nil {
	// 	panic("logic error")
	// }
	// if be.PrevCommit != nil {
	// 	be.PrevCommit = nil
	// }
	be.Txn = nil
	be.LogIndex = index
	// logutil.Infof("Apply0Index %s", index.String())
	return nil
}

func (be *BaseEntry) HasDropped() bool {
	return be.DeleteAt != 0
}

func (be *BaseEntry) CreateBefore(ts uint64) bool {
	if be.CreateAt != 0 {
		return be.CreateAt < ts
	}
	return false
}

func (be *BaseEntry) CreateAfter(ts uint64) bool {
	if be.CreateAt != 0 {
		return be.CreateAt > ts
	}
	return false
}

func (be *BaseEntry) DeleteBefore(ts uint64) bool {
	if be.DeleteAt != 0 {
		return be.DeleteAt < ts
	}
	return false
}

func (be *BaseEntry) DeleteAfter(ts uint64) bool {
	if be.DeleteAt != 0 {
		return be.DeleteAt > ts
	}
	return false
}

func (be *BaseEntry) HasCreated() bool {
	return be.CreateAt != 0
}

func (be *BaseEntry) DropEntryLocked(txnCtx txnif.TxnReader) error {
	if be.Txn == nil {
		if be.HasDropped() {
			return ErrNotFound
		}
		if be.CreateAt > txnCtx.GetStartTS() {
			panic("unexpected")
		}
		be.PrevCommit = &CommitInfo{
			CurrOp:   be.CurrOp,
			LogIndex: be.LogIndex,
		}
		be.Txn = txnCtx
		be.CurrOp = OpSoftDelete
		return nil
	}
	if be.Txn.GetID() == txnCtx.GetID() {
		if be.CurrOp == OpSoftDelete {
			return ErrNotFound
		}
		be.CurrOp = OpSoftDelete
		return nil
	}
	return txnif.TxnWWConflictErr
}

func (be *BaseEntry) SameTxn(o *BaseEntry) bool {
	if be.Txn != nil && o.Txn != nil {
		return be.Txn.GetID() == o.Txn.GetID()
	}
	return false
}

func (be *BaseEntry) IsCreatedUncommitted() bool {
	if be.Txn != nil {
		return be.CurrOp == OpCreate
	}
	return false
}

func (be *BaseEntry) IsDroppedUncommitted() bool {
	if be.Txn != nil {
		return be.CurrOp == OpSoftDelete
	}
	return false
}

func (be *BaseEntry) IsDroppedCommitted() bool {
	return be.Txn == nil && be.CurrOp == OpSoftDelete
}

func (be *BaseEntry) InTxnOrRollbacked() bool {
	return be.CreateAt == 0 && be.DeleteAt == 0
}

func (be *BaseEntry) HasActiveTxn() bool {
	return be.Txn != nil
}

func (be *BaseEntry) GetTxnID() uint64 {
	if be.Txn != nil {
		return be.Txn.GetID()
	}
	return 0
}

func (be *BaseEntry) IsSameTxn(ctx txnif.TxnReader) bool {
	if be.Txn != nil {
		return be.Txn.GetID() == ctx.GetID()
	}
	return false
}

func (be *BaseEntry) IsCommitting() bool {
	if be.Txn != nil && be.Txn.GetCommitTS() != txnif.UncommitTS {
		return true
	}
	return false
}

func (be *BaseEntry) CreateAndDropInSameTxn() bool {
	if be.CreateAt != 0 && (be.CreateAt == be.DeleteAt) {
		return true
	}
	return false
}

func (be *BaseEntry) TxnCanRead(txn txnif.AsyncTxn, rwlocker *sync.RWMutex) bool {
	if txn == nil {
		return true
	}
	thisTxn := be.Txn
	// No active txn is on this entry
	if !be.HasActiveTxn() {
		// This entry is created after txn starts, skip this entry
		// This entry is deleted before txn starts, skip this entry
		if be.CreateAfter(txn.GetStartTS()) || be.DeleteBefore(txn.GetStartTS()) {
			return false
		}
		// Otherwise, use this entry
		return true
	}
	// If this entry was written by the same txn as txn
	if be.IsSameTxn(txn) {
		// This entry was deleted by the same txn, skip this entry
		if be.IsDroppedUncommitted() {
			return false
		}
		// This entry was created by the same txn, use this entry
		return true
	}
	// This entry is not created, skip this entry
	if !be.HasCreated() {
		return false
	}
	// This entry was created after txn start ts, skip this entry
	if be.CreateAfter(txn.GetStartTS()) {
		return false
	}

	// This entry was not dropped before or by any active tansactions, use this entry
	if !be.HasDropped() {
		return true
	}

	// This entry was dropped after txn starts, use this entry
	if be.DeleteAfter(txn.GetStartTS()) {
		return true
	}

	// This entry was deleted before txn start
	// Delete is uncommited by other txn, skip this entry
	if !be.IsCommitting() {
		return false
	}
	if be.CreateAndDropInSameTxn() {
		return false
	}
	// The txn is committing, wait till committed
	if rwlocker != nil {
		rwlocker.RUnlock()
	}
	state := thisTxn.GetTxnState(true)
	if rwlocker != nil {
		rwlocker.RLock()
	}
	if state == txnif.TxnStateRollbacked {
		return true
	}

	return false
}

func (be *BaseEntry) String() string {
	s := fmt.Sprintf("[Op=%s][ID=%d][%d,%d]%s", OpNames[be.CurrOp], be.ID, be.CreateAt, be.DeleteAt, be.LogIndex.String())
	if be.Txn != nil {
		s = fmt.Sprintf("%s%s", s, be.Txn.Repr())
	}
	return s
}

func (be *BaseEntry) PrepareWrite(txn txnif.TxnReader, rwlocker *sync.RWMutex) (err error) {
	if txn == nil {
		return
	}
	eTxn := be.Txn
	// No active txn is on this entry
	if eTxn == nil {
		return
	}
	// The same txn is on this entry
	if eTxn.GetID() == txn.GetID() {
		return
	}
	commitTS := be.Txn.GetCommitTS()
	// Another active txn is on this entry
	if commitTS == txnif.UncommitTS {
		err = txnif.TxnWWConflictErr
		return
	}
	// Another committing|rollbacking|committed|rollbacked txn commits|rollbacks after txn starts
	if commitTS > txn.GetStartTS() {
		return
	}
	if rwlocker != nil {
		rwlocker.RUnlock()
	}
	eTxn.GetTxnState(true)
	if rwlocker != nil {
		rwlocker.RLock()
	}
	return
}
