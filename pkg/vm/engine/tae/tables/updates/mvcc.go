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
	"fmt"
	"sync"
	"sync/atomic"
	"unsafe"

	"github.com/matrixorigin/matrixone/pkg/common/mpool"
	"github.com/matrixorigin/matrixone/pkg/container/nulls"
	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/logutil"
	"github.com/matrixorigin/matrixone/pkg/objectio"

	pkgcatalog "github.com/matrixorigin/matrixone/pkg/catalog"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/blockio"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/catalog"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/common"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/containers"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/iface/handle"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/iface/txnif"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/txn/txnbase"
)

var (
	AppendNodeApproxSize int
	DeleteNodeApproxSize int

	DeleteChainApproxSize int
	MVCCHandleApproxSize  int
)

func init() {
	txnNodeSize := int(unsafe.Sizeof(txnbase.TxnMVCCNode{}))
	AppendNodeApproxSize = int(unsafe.Sizeof(AppendNode{})) + txnNodeSize
	DeleteNodeApproxSize = int(unsafe.Sizeof(DeleteNode{})) + txnNodeSize

	DeleteChainApproxSize = int(unsafe.Sizeof(DeleteChain{}))
	MVCCHandleApproxSize = int(unsafe.Sizeof(MVCCHandle{}))
}

type AppendMVCCHandle struct {
	*sync.RWMutex
	meta           *catalog.ObjectEntry
	appends        *txnbase.MVCCSlice[*AppendNode]
	appendListener func(txnif.AppendNode) error
}

func NewAppendMVCCHandle(meta *catalog.ObjectEntry) *AppendMVCCHandle {
	node := &AppendMVCCHandle{
		RWMutex: meta.RWMutex,
		meta:    meta,
		appends: txnbase.NewMVCCSlice(NewEmptyAppendNode, CompareAppendNode),
	}
	return node
}

// ==========================================================
// *************** All appends related APIs *****************
// ==========================================================

// NOTE: after this call all appends related APIs should not be called
// ReleaseAppends release all append nodes.
// it is only called when the appendable block is persisted and the
// memory node is released
func (n *AppendMVCCHandle) ReleaseAppends() {
	n.Lock()
	defer n.Unlock()
	n.appends = nil
}

// only for internal usage
// given a row, it returns the append node which contains the row
func (n *AppendMVCCHandle) GetAppendNodeByRow(row uint32) (an *AppendNode) {
	_, an = n.appends.SearchNodeByCompareFn(func(node *AppendNode) int {
		if node.maxRow <= row {
			return -1
		}
		if node.startRow > row {
			return 1
		}
		return 0
	})
	return
}

// it collects all append nodes in the range [start, end]
// minRow: is the min row
// maxRow: is the max row
// commitTSVec: is the commit ts vector
// abortVec: is the abort vector
// aborts: is the aborted bitmap
func (n *AppendMVCCHandle) CollectAppendLocked(
	start, end types.TS, mp *mpool.MPool,
) (
	minRow, maxRow uint32,
	commitTSVec, abortVec containers.Vector,
	aborts *nulls.Bitmap,
) {
	startOffset, node := n.appends.GetNodeToReadByPrepareTS(start)
	if node != nil {
		prepareTS := node.GetPrepare()
		if prepareTS.Less(&start) {
			startOffset++
		}
	}
	endOffset, node := n.appends.GetNodeToReadByPrepareTS(end)
	if node == nil || startOffset > endOffset {
		return
	}
	minRow = n.appends.GetNodeByOffset(startOffset).startRow
	maxRow = node.maxRow

	aborts = &nulls.Bitmap{}
	commitTSVec = containers.MakeVector(types.T_TS.ToType(), mp)
	abortVec = containers.MakeVector(types.T_bool.ToType(), mp)
	n.appends.LoopOffsetRange(
		startOffset,
		endOffset,
		func(node *AppendNode) bool {
			txn := node.GetTxn()
			if txn != nil {
				n.RUnlock()
				txn.GetTxnState(true)
				n.RLock()
			}
			if node.IsAborted() {
				aborts.AddRange(uint64(node.startRow), uint64(node.maxRow))
			}
			for i := 0; i < int(node.maxRow-node.startRow); i++ {
				commitTSVec.Append(node.GetCommitTS(), false)
				abortVec.Append(node.IsAborted(), false)
			}
			return true
		})
	return
}

// it is used to get the visible max row for a txn
// maxrow: is the max row that the txn can see
// visible: is true if the txn can see any row
// holes: is the bitmap of the holes that the txn cannot see
// holes exists only if any append node was rollbacked
func (n *AppendMVCCHandle) GetVisibleRowLocked(
	ctx context.Context,
	txn txnif.TxnReader,
) (maxrow uint32, visible bool, holes *nulls.Bitmap, err error) {
	var holesMax uint32
	anToWait := make([]*AppendNode, 0)
	txnToWait := make([]txnif.TxnReader, 0)
	n.appends.ForEach(func(an *AppendNode) bool {
		needWait, waitTxn := an.NeedWaitCommitting(txn.GetStartTS())
		if needWait {
			anToWait = append(anToWait, an)
			txnToWait = append(txnToWait, waitTxn)
			return true
		}
		if an.IsVisible(txn) {
			visible = true
			maxrow = an.maxRow
		} else {
			if holes == nil {
				holes = nulls.NewWithSize(int(an.maxRow) + 1)
			}
			holes.AddRange(uint64(an.startRow), uint64(an.maxRow))
			if holesMax < an.maxRow {
				holesMax = an.maxRow
			}
		}
		startTS := txn.GetStartTS()
		return !an.Prepare.Greater(&startTS)
	}, true)
	if len(anToWait) != 0 {
		n.RUnlock()
		for _, txn := range txnToWait {
			txn.GetTxnState(true)
		}
		n.RLock()
	}
	for _, an := range anToWait {
		if an.IsVisible(txn) {
			visible = true
			if maxrow < an.maxRow {
				maxrow = an.maxRow
			}
		} else {
			if holes == nil {
				holes = nulls.NewWithSize(int(an.maxRow) + 1)
			}
			holes.AddRange(uint64(an.startRow), uint64(an.maxRow))
			if holesMax < an.maxRow {
				holesMax = an.maxRow
			}
		}
	}
	if !holes.IsEmpty() {
		for i := uint64(maxrow); i < uint64(holesMax); i++ {
			holes.Del(i)
		}
	}
	return
}

// it collects all append nodes that are prepared before the given ts
// foreachFn is called for each append node that is prepared before the given ts
func (n *AppendMVCCHandle) CollectUncommittedANodesPreparedBefore(
	ts types.TS,
	foreachFn func(*AppendNode),
) (anyWaitable bool) {
	if n.appends.IsEmpty() {
		return
	}
	n.appends.ForEach(func(an *AppendNode) bool {
		needWait, txn := an.NeedWaitCommitting(ts)
		if txn == nil {
			return false
		}
		if needWait {
			foreachFn(an)
			anyWaitable = true
		}
		return true
	}, false)
	return
}

func (n *AppendMVCCHandle) OnReplayAppendNode(an *AppendNode) {
	an.mvcc = n
	n.appends.InsertNode(an)
}

// AddAppendNodeLocked add a new appendnode to the list.
func (n *AppendMVCCHandle) AddAppendNodeLocked(
	txn txnif.AsyncTxn,
	startRow uint32,
	maxRow uint32,
) (an *AppendNode, created bool) {
	if n.appends.IsEmpty() || !n.appends.GetUpdateNodeLocked().IsSameTxn(txn) {
		// if the appends is empty or the last appendnode is not of the same txn,
		// create a new appendnode and append it to the list.
		an = NewAppendNode(txn, startRow, maxRow, n)
		n.appends.InsertNode(an)
		created = true
	} else {
		// if the last appendnode is of the same txn, update the maxrow of the last appendnode.
		an = n.appends.GetUpdateNodeLocked()
		created = false
		an.SetMaxRow(maxRow)
	}
	return
}

// Reschedule until all appendnode is committed.
// Pending appendnode is not visible for compaction txn.
func (n *AppendMVCCHandle) PrepareCompact() bool {
	return n.allAppendsCommitted()
}

func (n *AppendMVCCHandle) GetLatestAppendPrepareTSLocked() types.TS {
	return n.appends.GetUpdateNodeLocked().Prepare
}

// check if all appendnodes are committed.
func (n *AppendMVCCHandle) allAppendsCommitted() bool {
	n.RLock()
	defer n.RUnlock()
	return n.appends.IsCommitted()
}

// DeleteAppendNodeLocked deletes the appendnode from the append list.
// it is called when txn of the appendnode is aborted.
func (n *AppendMVCCHandle) DeleteAppendNodeLocked(node *AppendNode) {
	n.appends.DeleteNode(node)
}

func (n *AppendMVCCHandle) SetAppendListener(l func(txnif.AppendNode) error) {
	n.appendListener = l
}

func (n *AppendMVCCHandle) GetAppendListener() func(txnif.AppendNode) error {
	return n.appendListener
}

// AllAppendsCommittedBefore returns true if all appendnode is committed before ts.
func (n *AppendMVCCHandle) AllAppendsCommittedBefore(ts types.TS) bool {
	// get the latest appendnode
	anode := n.appends.GetUpdateNodeLocked()
	if anode == nil {
		return false
	}

	// if the latest appendnode is not committed, return false
	if !anode.IsCommitted() {
		return false
	}

	// check if the latest appendnode is committed before ts
	commitTS := anode.GetCommitTS()
	return commitTS.Less(&ts)
}

func (n *AppendMVCCHandle) StringLocked() string {
	return n.appends.StringLocked()
}

func (n *AppendMVCCHandle) EstimateMemSizeLocked() int {
	asize := 0
	if n.appends != nil {
		asize += len(n.appends.MVCC) * AppendNodeApproxSize
	}
	return asize
}

// GetTotalRow is only for replay
func (n *AppendMVCCHandle) GetTotalRow() uint32 {
	an := n.appends.GetUpdateNodeLocked()
	if an == nil {
		return 0
	}
	return an.maxRow
}

func (n *AppendMVCCHandle) GetID() *common.ID {
	return n.meta.AsCommonID()
}

type ObjectMVCCHandle struct {
	*sync.RWMutex
	deletes         map[uint16]*MVCCHandle
	meta            *catalog.ObjectEntry
	deletesListener func(uint64, types.TS) error
}

func NewObjectMVCCHandle(meta *catalog.ObjectEntry) *ObjectMVCCHandle {
	node := &ObjectMVCCHandle{
		RWMutex: meta.RWMutex,
		meta:    meta,
		deletes: make(map[uint16]*MVCCHandle),
	}
	node.UpgradeAllDeleteChain()
	node.SetDeletesListener(node.OnApplyDelete)
	return node
}
func (n *ObjectMVCCHandle) OnApplyDelete(
	deleted uint64,
	ts types.TS) (err error) {
	n.meta.GetTable().RemoveRows(deleted)
	return
}
func (n *ObjectMVCCHandle) GetOrCreateDeleteChainLocked(blkID uint16) *MVCCHandle {
	deletes := n.deletes[blkID]
	if deletes == nil {
		deletes = NewMVCCHandle(n, blkID)
		n.deletes[blkID] = deletes
	}
	return deletes
}
func (n *ObjectMVCCHandle) TryGetDeleteChain(blkID uint16) *MVCCHandle {
	deletes := n.deletes[blkID]
	return deletes
}
func (n *ObjectMVCCHandle) SetDeletesListener(l func(uint64, types.TS) error) {
	n.deletesListener = l
}

func (n *ObjectMVCCHandle) GetDeletesListener() func(uint64, types.TS) error {
	return n.deletesListener
}

func (n *ObjectMVCCHandle) GetChangeIntentionCntLocked() uint32 {
	changes := uint32(0)
	for _, deletes := range n.deletes {
		changes += deletes.GetChangeIntentionCnt()
	}
	return changes
}
func (n *ObjectMVCCHandle) IsDeletedLocked(
	row uint32, txn txnif.TxnReader, blkID uint16,
) (bool, error) {
	deletes := n.TryGetDeleteChain(blkID)
	if deletes == nil {
		return false, nil
	}
	return deletes.IsDeletedLocked(row, txn)
}

func (n *ObjectMVCCHandle) UpgradeAllDeleteChain() {
	for _, deletes := range n.deletes {
		deletes.upgradeDeleteChain()
	}
}
func (n *ObjectMVCCHandle) GetDeltaPersistedTS() types.TS {
	persisted := types.TS{}
	for _, deletes := range n.deletes {
		ts := deletes.getDeltaPersistedTSLocked()
		if ts.Greater(&persisted) {
			persisted = ts
		}
	}
	return persisted
}

func (n *ObjectMVCCHandle) UpgradeDeleteChain(blkID uint16) {
	deletes := n.deletes[blkID]
	if deletes == nil {
		return
	}
	deletes.upgradeDeleteChain()
}

// for test
func (n *ObjectMVCCHandle) UpgradeDeleteChainByTSLocked(ts types.TS) {
	for _, deletes := range n.deletes {
		deletes.upgradeDeleteChainByTSLocked(ts)
	}
}

func (n *ObjectMVCCHandle) EstimateMemSizeLocked() (dsize int) {
	for _, deletes := range n.deletes {
		dsize += deletes.EstimateMemSizeLocked()
	}
	return
}

func (n *ObjectMVCCHandle) GetDeltaLocAndCommitTS(blkID uint16) (loc objectio.Location, start, end types.TS) {
	n.RLock()
	defer n.RUnlock()
	deletes := n.deletes[blkID]
	if deletes == nil {
		return
	}
	return deletes.GetDeltaLocAndCommitTSLocked()
}
func (n *ObjectMVCCHandle) GetDeltaLocAndCommitTSByTxn(blkID uint16, txn txnif.TxnReader) (objectio.Location, types.TS) {
	deletes := n.deletes[blkID]
	if deletes == nil {
		return nil, types.TS{}
	}
	return deletes.GetDeltaLocAndCommitTSByTxn(txn)
}

func (n *ObjectMVCCHandle) StringLocked(level common.PPLevel, depth int, prefix string) string {
	s := ""
	for _, deletes := range n.deletes {
		s = fmt.Sprintf("%s%s", s, deletes.StringLocked(level, depth+1, prefix))
	}
	return s
}

func (n *ObjectMVCCHandle) StringBlkLocked(level common.PPLevel, depth int, prefix string, blkid int) string {
	s := ""
	if d, exist := n.deletes[uint16(blkid)]; exist {
		s = fmt.Sprintf("%s%s", s, d.StringLocked(level, depth+1, prefix))
	}
	return s
}

func (n *ObjectMVCCHandle) GetDeleteCnt() uint32 {
	cnt := uint32(0)
	for _, deletes := range n.deletes {
		cnt += deletes.GetDeleteCnt()
	}
	return cnt
}
func (n *ObjectMVCCHandle) HasDeleteIntentsPreparedIn(from, to types.TS) (found, isPersist bool) {
	for _, deletes := range n.deletes {
		found, isPersist = deletes.GetDeleteChain().HasDeleteIntentsPreparedInLocked(from, to)
		if found {
			return
		}
	}
	return
}
func (n *ObjectMVCCHandle) HasInMemoryDeleteIntentsPreparedInByBlock(blkID uint16, from, to types.TS) (found, isPersist bool) {
	mvcc := n.deletes[blkID]
	if mvcc == nil {
		return false, false
	}
	if mvcc.deletes.mask.IsEmpty() {
		return false, false
	}
	found, isPersist = mvcc.GetDeleteChain().HasDeleteIntentsPreparedInLocked(from, to)
	return
}

func (n *ObjectMVCCHandle) ReplayDeltaLoc(vMVCCNode any, blkID uint16) {
	mvccNode := vMVCCNode.(*catalog.MVCCNode[*catalog.MetadataMVCCNode])
	mvcc := n.GetOrCreateDeleteChainLocked(blkID)
	mvcc.ReplayDeltaLoc(mvccNode)
}
func (n *ObjectMVCCHandle) InMemoryDeletesExisted() bool {
	for _, deletes := range n.deletes {
		if !deletes.deletes.mask.IsEmpty() {
			return true
		}
	}
	return false
}
func (n *ObjectMVCCHandle) GetObject() any {
	return n.meta
}
func (n *ObjectMVCCHandle) GetLatestDeltaloc(blkOffset uint16) objectio.Location {
	mvcc := n.TryGetDeleteChain(blkOffset)
	if mvcc == nil {
		return nil
	}
	return mvcc.deltaloc.GetLatestNodeLocked().BaseNode.DeltaLoc
}
func (n *ObjectMVCCHandle) GetLatestMVCCNode(blkOffset uint16) *catalog.MVCCNode[*catalog.MetadataMVCCNode] {
	mvcc := n.TryGetDeleteChain(blkOffset)
	if mvcc == nil {
		return nil
	}
	return mvcc.deltaloc.GetLatestNodeLocked()
}
func (n *ObjectMVCCHandle) VisitDeletes(
	ctx context.Context,
	start, end types.TS,
	deltalocBat *containers.Batch,
	tnInsertBat *containers.Batch,
	skipInMemory bool) (delBatch *containers.Batch, deltalocStart, deltalocEnd int, err error) {
	n.RLock()
	defer n.RUnlock()
	deltalocStart = deltalocBat.Length()
	for blkOffset, mvcc := range n.deletes {
		newStart := start
		nodes := mvcc.deltaloc.ClonePreparedInRangeLocked(start, end)
		var skipData bool
		if len(nodes) != 0 {
			blkID := objectio.NewBlockidWithObjectID(&n.meta.ID, blkOffset)
			for _, node := range nodes {
				VisitDeltaloc(deltalocBat, tnInsertBat, n.meta, blkID, node, node.End, node.CreatedAt)
			}
			newest := nodes[len(nodes)-1]
			// block has newer delta data on s3, no need to collect data
			startTS := newest.GetStart()
			skipData = startTS.GreaterEq(&end)
			newStart = newest.GetStart()
		}
		if !skipData && !skipInMemory {
			deletes := n.deletes[blkOffset]
			n.RUnlock()
			delBat, err := deletes.CollectDeleteInRangeAfterDeltalocation(ctx, newStart, end, false, common.LogtailAllocator)
			n.RLock()
			if err != nil {
				if delBatch != nil {
					delBatch.Close()
				}
				delBat.Close()
				return nil, 0, 0, err
			}
			if delBat != nil && delBat.Length() > 0 {
				if delBatch == nil {
					delBatch = containers.NewBatch()
					delBatch.AddVector(
						catalog.AttrRowID,
						containers.MakeVector(types.T_Rowid.ToType(), common.LogtailAllocator),
					)
					delBatch.AddVector(
						catalog.AttrCommitTs,
						containers.MakeVector(types.T_TS.ToType(), common.LogtailAllocator),
					)
					delBatch.AddVector(
						catalog.AttrPKVal,
						containers.MakeVector(*delBat.GetVectorByName(catalog.AttrPKVal).GetType(), common.LogtailAllocator),
					)
				}
				delBatch.Extend(delBat)
				// delBatch is freed, don't use anymore
				delBat.Close()
			}
		}
	}
	deltalocEnd = deltalocBat.Length()
	return
}

func VisitDeltaloc(bat, tnBatch *containers.Batch, object *catalog.ObjectEntry, blkID *objectio.Blockid, node *catalog.MVCCNode[*catalog.MetadataMVCCNode], commitTS, createTS types.TS) {
	is_sorted := false
	if !object.IsAppendable() && object.GetSchema().HasSortKey() {
		is_sorted = true
	}
	bat.GetVectorByName(pkgcatalog.BlockMeta_ID).Append(*blkID, false)
	bat.GetVectorByName(pkgcatalog.BlockMeta_EntryState).Append(object.IsAppendable(), false)
	bat.GetVectorByName(pkgcatalog.BlockMeta_Sorted).Append(is_sorted, false)
	bat.GetVectorByName(pkgcatalog.BlockMeta_MetaLoc).Append([]byte(node.BaseNode.MetaLoc), false)
	bat.GetVectorByName(pkgcatalog.BlockMeta_DeltaLoc).Append([]byte(node.BaseNode.DeltaLoc), false)
	bat.GetVectorByName(pkgcatalog.BlockMeta_CommitTs).Append(commitTS, false)
	bat.GetVectorByName(pkgcatalog.BlockMeta_SegmentID).Append(*object.ID.Segment(), false)
	bat.GetVectorByName(pkgcatalog.BlockMeta_MemTruncPoint).Append(node.Start, false)
	bat.GetVectorByName(catalog.AttrCommitTs).Append(createTS, false)
	bat.GetVectorByName(catalog.AttrRowID).Append(objectio.HackBlockid2Rowid(blkID), false)

	// When pull and push, it doesn't collect tn batch
	if tnBatch != nil {
		tnBatch.GetVectorByName(catalog.SnapshotAttr_DBID).Append(object.GetTable().GetDB().ID, false)
		tnBatch.GetVectorByName(catalog.SnapshotAttr_TID).Append(object.GetTable().ID, false)
		node.TxnMVCCNode.AppendTuple(tnBatch)
	}
}

type DeltalocChain struct {
	mvcc *MVCCHandle
	*catalog.BaseEntryImpl[*catalog.MetadataMVCCNode]
}

func NewDeltalocChain(mvcc *MVCCHandle) *DeltalocChain {
	delChain := &DeltalocChain{
		mvcc:          mvcc,
		BaseEntryImpl: catalog.NewBaseEntry(func() *catalog.MetadataMVCCNode { return &catalog.MetadataMVCCNode{} }),
	}
	delChain.RWMutex = mvcc.RWMutex
	return delChain
}
func (d *DeltalocChain) PrepareCommit() (err error) {
	d.Lock()
	defer d.Unlock()
	node := d.GetLatestNodeLocked()
	if node.BaseNode.NeedCheckDeleteChainWhenCommit {
		if found, _ := d.mvcc.GetDeleteChain().HasDeleteIntentsPreparedInLocked(node.Start, node.Txn.GetPrepareTS()); found {
			return txnif.ErrTxnNeedRetry
		}
	}
	_, err = node.TxnMVCCNode.PrepareCommit()
	if err != nil {
		return
	}
	return
}
func (d *DeltalocChain) Is1PC() bool { return false }
func (d *DeltalocChain) MakeCommand(id uint32) (cmd txnif.TxnCmd, err error) {
	return catalog.NewDeltalocCmd(id, catalog.IOET_WALTxnCommand_Block, d.mvcc.GetID(), d.BaseEntryImpl), nil
}
func (d *DeltalocChain) PrepareRollback() error {
	d.RLock()
	node := d.GetLatestNodeLocked()
	d.RUnlock()
	// If it's deleted by deltaloc, reset persisted mask when rollback
	if node.BaseNode.NeedCheckDeleteChainWhenCommit {
		d.Lock()
		d.mvcc.GetDeleteChain().ResetPersistedMask()
		d.Unlock()
	}
	_, err := d.BaseEntryImpl.PrepareRollback()
	return err
}
func (d *DeltalocChain) Set1PC() {}

func (d *DeltalocChain) GetBlockID() *objectio.Blockid {
	return objectio.NewBlockidWithObjectID(&d.mvcc.meta.ID, d.mvcc.blkID)
}
func (d *DeltalocChain) GetMeta() *catalog.ObjectEntry { return d.mvcc.meta }

type MVCCHandle struct {
	*ObjectMVCCHandle
	changes     atomic.Uint32
	deletes     *DeleteChain
	deltaloc    *DeltalocChain
	blkID       uint16
	persistedTS types.TS
}

func NewMVCCHandle(meta *ObjectMVCCHandle, blkID uint16) *MVCCHandle {
	node := &MVCCHandle{
		ObjectMVCCHandle: meta,
		blkID:            blkID,
	}
	if meta == nil {
		return node
	}
	node.deletes = NewDeleteChain(node.RWMutex, node)
	node.deltaloc = NewDeltalocChain(node)
	return node
}

// ==========================================================
// *************** All common related APIs *****************
// ==========================================================

func (n *MVCCHandle) GetID() *common.ID {
	id := n.meta.AsCommonID()
	id.SetBlockOffset(n.blkID)
	return id
}
func (n *MVCCHandle) GetEntry() *catalog.ObjectEntry { return n.meta }

func (n *MVCCHandle) StringLocked(level common.PPLevel, depth int, prefix string) string {
	inMemoryCount := 0
	if n.deletes.DepthLocked() > 0 {
		// s = fmt.Sprintf("%s%s", s, n.deletes.StringLocked())
		inMemoryCount = n.deletes.mask.GetCardinality()
	}
	s := fmt.Sprintf("%sBLK[%d]InMem:%d\n", common.RepeatStr("\t", depth), n.blkID, inMemoryCount)
	if level > common.PPL3 {
		if imemChain := n.deletes.StringLocked(); imemChain != "" {
			s = fmt.Sprintf("%s%s", s, imemChain)
		}
	}
	if n.deltaloc.Depth() > 0 {
		s = fmt.Sprintf("%s%s", s, n.deltaloc.StringLocked())
	}
	s = s + "\n"
	return s
}

func (n *MVCCHandle) EstimateMemSizeLocked() (dsize int) {
	dsize = n.deletes.EstimateMemSizeLocked()
	return dsize + MVCCHandleApproxSize
}

// ==========================================================
// *************** All deletes related APIs *****************
// ==========================================================

func (n *MVCCHandle) getDeltaPersistedTSLocked() types.TS {
	persisted := types.TS{}
	n.deltaloc.LoopChainLocked(func(m *catalog.MVCCNode[*catalog.MetadataMVCCNode]) bool {
		if !m.BaseNode.DeltaLoc.IsEmpty() && m.IsCommitted() {
			persisted = m.GetStart()
			return false
		}
		return true
	})
	return persisted
}

func (n *MVCCHandle) upgradeDeleteChainByTSLocked(flushed types.TS) {
	if n.persistedTS.Equal(&flushed) {
		return
	}
	n.deletes = n.deletes.shrinkDeleteChainByTSLocked(flushed)

	n.persistedTS = flushed
}

func (n *MVCCHandle) upgradeDeleteChain() {
	persisted := n.getDeltaPersistedTSLocked()
	n.upgradeDeleteChainByTSLocked(persisted)
}

func (n *MVCCHandle) IncChangeIntentionCnt() {
	n.changes.Add(1)
}

func (n *MVCCHandle) DecChangeIntentionCnt() {
	n.changes.Add(^uint32(0))
}

// GetChangeIntentionCnt returns the number of operation of delete, which is updated before commiting.
// Note: Now it is ** only ** used in checkpointe runner to check whether this block has any chance to be flushed
func (n *MVCCHandle) GetChangeIntentionCnt() uint32 {
	return n.changes.Load()
}

// GetDeleteCnt returns committed deleted rows
func (n *MVCCHandle) GetDeleteCnt() uint32 {
	return n.deletes.GetDeleteCnt()
}

// it checks whether there is any delete in the range [start, end)
// ts is not used for now
func (n *MVCCHandle) CheckNotDeleted(start, end uint32, ts types.TS) error {
	return n.deletes.PrepareRangeDelete(start, end, ts)
}

func (n *MVCCHandle) CreateDeleteNode(txn txnif.AsyncTxn, deleteType handle.DeleteType) txnif.DeleteNode {
	return n.deletes.AddNodeLocked(txn, deleteType)
}

func (n *MVCCHandle) OnReplayDeleteNode(deleteNode txnif.DeleteNode) {
	n.deletes.OnReplayNode(deleteNode.(*DeleteNode))
}

func (n *MVCCHandle) GetDeleteChain() *DeleteChain {
	return n.deletes
}

func (n *MVCCHandle) IsDeletedLocked(
	row uint32, txn txnif.TxnReader,
) (bool, error) {
	return n.deletes.IsDeleted(row, txn, n.RWMutex)
}

// it collects all deletes in the range [start, end)
func (n *MVCCHandle) CollectDeleteLocked(
	start, end types.TS, pkType types.Type, mp *mpool.MPool,
) (rowIDVec, commitTSVec, pkVec, abortVec containers.Vector,
	aborts *nulls.Bitmap, deletes []uint32, minTS, persistedTS types.TS,
) {
	persistedTS = n.persistedTS
	if n.deletes.IsEmpty() {
		return
	}
	if !n.ExistDeleteInRangeLocked(start, end) {
		return
	}

	for {
		needWaitFound := false
		if rowIDVec != nil {
			rowIDVec.Close()
		}
		rowIDVec = containers.MakeVector(types.T_Rowid.ToType(), mp)
		if commitTSVec != nil {
			commitTSVec.Close()
		}
		commitTSVec = containers.MakeVector(types.T_TS.ToType(), mp)
		if pkVec != nil {
			pkVec.Close()
		}
		pkVec = containers.MakeVector(pkType, mp)
		aborts = &nulls.Bitmap{}
		id := objectio.NewBlockidWithObjectID(&n.meta.ID, n.blkID)
		n.deletes.LoopChainLocked(
			func(node *DeleteNode) bool {
				needWait, txn := node.NeedWaitCommitting(end.Next())
				if needWait {
					n.RUnlock()
					txn.GetTxnState(true)
					n.RLock()
					needWaitFound = true
					return false
				}
				if node.nt == NT_Persisted {
					return true
				}
				in, before := node.PreparedIn(start, end)
				if in {
					it := node.mask.Iterator()
					if node.IsAborted() {
						it := node.mask.Iterator()
						for it.HasNext() {
							row := it.Next()
							nulls.Add(aborts, uint64(row))
						}
					}
					for it.HasNext() {
						row := it.Next()
						rowIDVec.Append(*objectio.NewRowid(id, row), false)
						commitTSVec.Append(node.GetEnd(), false)
						// for deleteNode V1，rowid2PK is nil after restart
						if node.version < IOET_WALTxnCommand_DeleteNode_V2 {
							if deletes == nil {
								deletes = make([]uint32, 0)
							}
							deletes = append(deletes, row)
						} else {
							pkVec.Append(node.rowid2PK[row].Get(0), false)
						}
						if minTS.IsEmpty() {
							minTS = node.GetEnd()
						} else {
							end := node.GetEnd()
							if minTS.Greater(&end) {
								minTS = node.GetEnd()
							}
						}
					}
				}
				return !before
			})
		if !needWaitFound {
			break
		}
	}
	abortVec = containers.NewConstFixed[bool](types.T_bool.ToType(), false, rowIDVec.Length(), containers.Options{Allocator: mp})
	return
}

func (n *MVCCHandle) InMemoryCollectDeleteInRange(
	ctx context.Context,
	start, end types.TS,
	withAborted bool,
	mp *mpool.MPool,
) (bat *containers.Batch, minTS, persisitedTS types.TS, err error) {
	n.RLock()
	schema := n.meta.GetSchemaLocked()
	pkDef := schema.GetPrimaryKey()
	rowID, ts, pk, abort, abortedMap, deletes, minTS, persisitedTS := n.CollectDeleteLocked(start, end, pkDef.Type, mp)
	n.RUnlock()
	if rowID == nil {
		return
	}
	// for deleteNode version less than 2, pk doesn't exist in memory
	// collect pk by block.Foreach
	if len(deletes) != 0 {
		logutil.Infof("visit deletes: collect pk by load, obj is %v", n.meta.ID.String())
		pkIdx := pkDef.Idx
		data := n.meta.GetObjectData()
		data.Foreach(ctx, schema, n.blkID, pkIdx, func(v any, isNull bool, row int) error {
			pk.Append(v, false)
			return nil
		}, deletes, mp)
	}
	// batch: rowID, ts, pkVec, abort
	bat = containers.NewBatch()
	bat.AddVector(catalog.PhyAddrColumnName, rowID)
	bat.AddVector(catalog.AttrCommitTs, ts)
	bat.AddVector(catalog.AttrPKVal, pk)
	if withAborted {
		bat.AddVector(catalog.AttrAborted, abort)
	} else {
		abort.Close()
		bat.Deletes = abortedMap
		bat.Compact()
	}
	return
}

// CollectDeleteInRangeAfterDeltalocation collects deletes after
// a certain delta location and committed in [start,end]
// When subscribe a table, it collects delta location, then it collects deletes.
// To avoid collecting duplicate deletes,
// it collects after start ts of the delta location.
// If the delta location is from CN, deletes is committed after startTS.
// CollectDeleteInRange still collect duplicate deletes.
func (n *MVCCHandle) CollectDeleteInRangeAfterDeltalocation(
	ctx context.Context,
	start, end types.TS, // start is startTS of deltalocation
	withAborted bool,
	mp *mpool.MPool,
) (bat *containers.Batch, err error) {
	// persisted is persistedTS of deletes of the blk
	// it equals startTS of the last delta location
	deletes, _, persisted, err := n.InMemoryCollectDeleteInRange(
		ctx,
		start,
		end,
		withAborted,
		mp,
	)
	if err != nil {
		return nil, err
	}
	// if persisted > start,
	// there's another delta location committed.
	// It includes more deletes than former delta location.
	if persisted.Greater(&start) {
		deletes, err = n.meta.GetObjectData().PersistedCollectDeleteInRange(
			ctx,
			deletes,
			n.blkID,
			start,
			end,
			withAborted,
			mp,
		)
	}
	if deletes != nil && deletes.Length() != 0 {
		if bat == nil {
			bat = containers.NewBatch()
			bat.AddVector(catalog.AttrRowID, containers.MakeVector(types.T_Rowid.ToType(), mp))
			bat.AddVector(catalog.AttrCommitTs, containers.MakeVector(types.T_TS.ToType(), mp))
			bat.AddVector(catalog.AttrPKVal, containers.MakeVector(*deletes.GetVectorByName(catalog.AttrPKVal).GetType(), mp))
			if withAborted {
				bat.AddVector(catalog.AttrAborted, containers.MakeVector(types.T_bool.ToType(), mp))
			}
		}
		bat.Extend(deletes)
		deletes.Close()
	}
	return
}

// ExistDeleteInRange check if there is any delete in the range [start, end]
// it loops the delete chain and check if there is any delete node in the range
func (n *MVCCHandle) ExistDeleteInRangeLocked(start, end types.TS) (exist bool) {
	for {
		needWaitFound := false
		n.deletes.LoopChainLocked(
			func(node *DeleteNode) bool {
				needWait, txn := node.NeedWaitCommitting(end.Next())
				if needWait {
					n.RUnlock()
					txn.GetTxnState(true)
					n.RLock()
					needWaitFound = true
					return false
				}
				in, before := node.PreparedIn(start, end)
				if in {
					exist = true
					return false
				}
				return !before
			})
		if !needWaitFound {
			break
		}
	}

	return
}

func (n *MVCCHandle) GetDeleteNodeByRow(row uint32) (an *DeleteNode) {
	return n.deletes.GetDeleteNodeByRow(row)
}
func (n *MVCCHandle) GetDeltaLocAndCommitTS() (objectio.Location, types.TS, types.TS) {
	n.RLock()
	defer n.RUnlock()
	return n.GetDeltaLocAndCommitTSLocked()
}
func (n *MVCCHandle) GetDeltaLocAndCommitTSLocked() (objectio.Location, types.TS, types.TS) {
	node := n.deltaloc.GetLatestNodeLocked()
	if node == nil {
		return nil, types.TS{}, types.TS{}
	}
	str := node.BaseNode.DeltaLoc
	committs := node.End
	startts := node.Start
	return str, startts, committs
}
func (n *MVCCHandle) GetDeltaLocAndCommitTSByTxn(txn txnif.TxnReader) (objectio.Location, types.TS) {
	n.RLock()
	defer n.RUnlock()
	node := n.deltaloc.GetVisibleNodeLocked(txn)
	if node == nil {
		return nil, types.TS{}
	}
	str := node.BaseNode.DeltaLoc
	ts := node.End
	return str, ts
}
func (n *MVCCHandle) isEmptyLocked() bool {
	if n.deltaloc.Depth() != 0 {
		return false
	}
	if !n.deletes.IsEmpty() {
		return false
	}
	return true
}
func (n *MVCCHandle) TryDeleteByDeltalocLocked(txn txnif.AsyncTxn, deltaLoc objectio.Location, needCheckWhenCommit bool) (entry txnif.TxnEntry, ok bool, err error) {
	if !n.isEmptyLocked() {
		return
	}
	_, entry, err = n.UpdateDeltaLocLocked(txn, deltaLoc, needCheckWhenCommit)
	if err != nil {
		return
	}
	bat, release, err := blockio.LoadTombstoneColumns(
		txn.GetContext(),
		[]uint16{0},
		nil,
		n.meta.GetObjectData().GetFs().Service,
		deltaLoc,
		nil,
	)
	defer release()
	if err == nil {
		ok = true
	}
	rowids := containers.ToTNVector(bat.Vecs[0], common.MutMemAllocator)
	defer rowids.Close()
	err = containers.ForeachVector(rowids, func(rowid types.Rowid, _ bool, row int) error {
		offset := rowid.GetRowOffset()
		n.deletes.persistedMask.Add(uint64(offset))
		return nil
	}, nil)
	if err == nil {
		ok = true
	}
	return
}
func (n *MVCCHandle) UpdateDeltaLocLocked(txn txnif.TxnReader, deltaloc objectio.Location, needCheckWhenCommit bool) (isNewNode bool, entry txnif.TxnEntry, err error) {
	needWait, txnToWait := n.deltaloc.NeedWaitCommittingLocked(txn.GetStartTS())
	if needWait {
		n.Unlock()
		txnToWait.GetTxnState(true)
		n.Lock()
	}
	err = n.deltaloc.CheckConflictLocked(txn)
	if err != nil {
		return
	}
	baseNode := &catalog.MetadataMVCCNode{
		DeltaLoc:                       deltaloc,
		NeedCheckDeleteChainWhenCommit: needCheckWhenCommit,
	}
	entry = n.deltaloc

	if !n.deltaloc.IsEmptyLocked() {
		node := n.deltaloc.GetLatestNodeLocked()
		if node.IsSameTxn(txn) {
			node.BaseNode.Update(baseNode)
			return
		}

	}

	node := &catalog.MVCCNode[*catalog.MetadataMVCCNode]{
		EntryMVCCNode: &catalog.EntryMVCCNode{},
		BaseNode:      baseNode,
	}
	node.TxnMVCCNode = txnbase.NewTxnMVCCNodeWithTxn(txn)
	n.deltaloc.Insert(node)
	isNewNode = true
	return
}

func (n *MVCCHandle) ReplayDeltaLoc(mvcc *catalog.MVCCNode[*catalog.MetadataMVCCNode]) {
	n.deltaloc.Insert(mvcc)
}
