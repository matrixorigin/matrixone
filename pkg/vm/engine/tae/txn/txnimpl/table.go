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

package txnimpl

import (
	"context"
	"fmt"
	"runtime/trace"
	"time"

	"github.com/matrixorigin/matrixone/pkg/fileservice"

	"github.com/matrixorigin/matrixone/pkg/perfcounter"

	"github.com/RoaringBitmap/roaring"

	"github.com/matrixorigin/matrixone/pkg/common/moerr"
	"github.com/matrixorigin/matrixone/pkg/common/moprobe"
	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/logutil"
	"github.com/matrixorigin/matrixone/pkg/objectio"
	apipb "github.com/matrixorigin/matrixone/pkg/pb/api"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/blockio"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/catalog"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/common"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/containers"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/iface/handle"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/iface/txnif"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/index"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/model"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/tables/updates"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/wal"
)

var (
	ErrDuplicateNode = moerr.NewInternalErrorNoCtx("tae: duplicate node")
)

type txnEntries struct {
	entries []txnif.TxnEntry
	mask    *roaring.Bitmap
}

func newTxnEntries() *txnEntries {
	return &txnEntries{
		entries: make([]txnif.TxnEntry, 0),
		mask:    roaring.New(),
	}
}

func (entries *txnEntries) Len() int {
	return len(entries.entries)
}

func (entries *txnEntries) Append(entry txnif.TxnEntry) {
	entries.entries = append(entries.entries, entry)
}

func (entries *txnEntries) Delete(idx int) {
	entries.mask.Add(uint32(idx))
}

func (entries *txnEntries) IsDeleted(idx int) bool {
	return entries.mask.ContainsInt(idx)
}

func (entries *txnEntries) AnyDelete() bool {
	return !entries.mask.IsEmpty()
}

func (entries *txnEntries) Close() {
	entries.mask = nil
	entries.entries = nil
}

type deleteNode struct {
	DeleteNodes []txnif.DeleteNode
	idx         []int
}

func newDeleteNode(node txnif.DeleteNode, idx int) *deleteNode {
	nodes := []txnif.DeleteNode{node}
	return &deleteNode{
		DeleteNodes: nodes,
		idx:         []int{idx},
	}
}

type txnTable struct {
	store       *txnStore
	createEntry txnif.TxnEntry
	dropEntry   txnif.TxnEntry
	tableSpace  *tableSpace
	deleteNodes map[common.ID]*deleteNode
	entry       *catalog.TableEntry
	schema      *catalog.Schema
	logs        []wal.LogEntry

	dedupedObjectHint uint64
	dedupedBlockID    *types.Blockid

	txnEntries *txnEntries
	csnStart   uint32

	idx int
}

func newTxnTable(store *txnStore, entry *catalog.TableEntry) (*txnTable, error) {
	schema := entry.GetVisibleSchema(store.txn)
	if schema == nil {
		return nil, moerr.NewInternalErrorNoCtx("No visible schema for ts %s", store.txn.GetStartTS().ToString())
	}
	tbl := &txnTable{
		store:       store,
		entry:       entry,
		schema:      schema,
		deleteNodes: make(map[common.ID]*deleteNode),
		logs:        make([]wal.LogEntry, 0),
		txnEntries:  newTxnEntries(),
	}
	return tbl, nil
}
func (tbl *txnTable) getNormalDeleteNode(id common.ID) *updates.DeleteNode {
	nodes, ok := tbl.deleteNodes[id]
	if !ok {
		return nil
	}
	for _, node := range nodes.DeleteNodes {
		if !node.IsPersistedDeletedNode() {
			return node.(*updates.DeleteNode)
		}
	}
	return nil
}
func (tbl *txnTable) PrePreareTransfer(phase string, ts types.TS) (err error) {
	return tbl.TransferDeletes(ts, phase)
}

func (tbl *txnTable) TransferDeleteIntent(
	id *common.ID,
	row uint32) (changed bool, nid *common.ID, nrow uint32, err error) {
	pinned, err := tbl.store.rt.TransferTable.Pin(*id)
	if err != nil {
		err = nil
		return
	}
	defer pinned.Close()
	entry, err := tbl.store.warChecker.CacheGet(
		tbl.entry.GetDB().ID,
		id.TableID,
		id.ObjectID(),
		&id.BlockID)
	if err != nil {
		panic(err)
	}
	ts := types.BuildTS(time.Now().UTC().UnixNano(), 0)
	if err = readWriteConfilictCheck(&entry.BaseEntryImpl, ts); err == nil {
		return
	}
	err = nil
	nid = &common.ID{
		TableID: id.TableID,
	}
	rowID, ok := pinned.Item().Transfer(row)
	if !ok {
		err = moerr.NewTxnWWConflictNoCtx()
		return
	}
	changed = true
	nid.BlockID, nrow = rowID.Decode()
	return
}

func (tbl *txnTable) TransferDeletes(ts types.TS, phase string) (err error) {
	if tbl.store.rt.TransferTable == nil {
		return
	}
	if len(tbl.deleteNodes) == 0 {
		return
	}
	for id, nodes := range tbl.deleteNodes {
		for offset, node := range nodes.DeleteNodes {
			// search the read set to check wether the delete node relevant
			// block was deleted.
			// if not deleted, go to next
			// if deleted, try to transfer the delete node
			if err = tbl.store.warChecker.checkOne(
				&id,
				ts,
			); err == nil {
				continue
			}

			// if the error is not a r-w conflict. something wrong really happened
			if !moerr.IsMoErrCode(err, moerr.ErrTxnRWConflict) {
				return
			}

			// try to transfer the delete node
			// here are some possible returns
			// nil: transferred successfully
			// ErrTxnRWConflict: the target block was also be compacted
			// ErrTxnWWConflict: w-w error
			if _, err = tbl.TransferDeleteNode(&id, node, offset, nodes.idx[offset], phase); err != nil {
				return
			}

		}
	}
	return
}

func (tbl *txnTable) recurTransferDelete(
	memo map[types.Blockid]*common.PinnedItem[*model.TransferHashPage],
	page *model.TransferHashPage,
	id *common.ID,
	row uint32,
	pk containers.Vector,
	depth int) error {

	var page2 *common.PinnedItem[*model.TransferHashPage]

	rowID, ok := page.Transfer(row)
	if !ok {
		err := moerr.NewTxnWWConflictNoCtx()
		msg := fmt.Sprintf("table-%d blk-%d delete row-%d depth-%d",
			id.TableID,
			id.BlockID,
			row,
			depth)
		logutil.Warnf("[ts=%s]TransferDeleteNode: %v",
			tbl.store.txn.GetStartTS().ToString(),
			msg)
		return err
	}
	blockID, offset := rowID.Decode()
	newID := &common.ID{
		DbID:    id.DbID,
		TableID: id.TableID,
		BlockID: blockID,
	}
	var err error
	if page2, ok = memo[blockID]; !ok {
		page2, err = tbl.store.rt.TransferTable.Pin(*newID)
		if err == nil {
			memo[blockID] = page2
		}
	}
	if page2 != nil {
		return tbl.recurTransferDelete(
			memo,
			page2.Item(),
			newID,
			offset,
			pk,
			depth+1)
	}
	if err = tbl.RangeDelete(newID, offset, offset, pk, handle.DT_Normal); err != nil {
		return err
	}
	common.DoIfInfoEnabled(func() {
		logutil.Infof("depth-%d %s transfer delete from blk-%s row-%d to blk-%s row-%d",
			depth,
			tbl.schema.Name,
			id.BlockID.String(),
			row,
			blockID.String(),
			offset)
	})
	return nil
}

func (tbl *txnTable) TransferDeleteNode(
	id *common.ID, node txnif.DeleteNode, offset, idx int, phase string,
) (transferred bool, err error) {
	rows := node.DeletedRows()
	pk := node.DeletedPK()
	if transferred, err = tbl.TransferDeleteRows(id, rows, pk, phase); err != nil {
		return
	}

	// rollback transferred delete node. should not fail
	if err = node.PrepareRollback(); err != nil {
		panic(err)
	}
	if err = node.ApplyRollback(); err != nil {
		panic(err)
	}
	tbl.commitTransferDeleteNode(id, offset, idx)
	return
}

func (tbl *txnTable) TransferDeleteRows(
	id *common.ID,
	rows []uint32,
	pk map[uint32]containers.Vector,
	phase string) (transferred bool, err error) {
	memo := make(map[types.Blockid]*common.PinnedItem[*model.TransferHashPage])
	common.DoIfInfoEnabled(func() {
		logutil.Info("[Start]",
			common.AnyField("txn-start-ts", tbl.store.txn.GetStartTS().ToString()),
			common.OperationField("transfer-deletes"),
			common.OperandField(id.BlockString()),
			common.AnyField("phase", phase))
	})
	defer func() {
		common.DoIfInfoEnabled(func() {
			logutil.Info("[End]",
				common.AnyField("txn-start-ts", tbl.store.txn.GetStartTS().ToString()),
				common.OperationField("transfer-deletes"),
				common.OperandField(id.BlockString()),
				common.AnyField("phase", phase),
				common.ErrorField(err))
		})
		for _, m := range memo {
			m.Close()
		}
	}()

	pinned, err := tbl.store.rt.TransferTable.Pin(*id)
	// cannot find a transferred record. maybe the transferred record was TTL'ed
	// here we can convert the error back to r-w conflict
	if err != nil {
		err = moerr.NewTxnRWConflictNoCtx()
		return
	}
	memo[id.BlockID] = pinned

	// logutil.Infof("TransferDeleteNode deletenode %s", node.DeleteNode.(*updates.DeleteNode).GeneralVerboseString())
	page := pinned.Item()
	depth := 0
	for _, row := range rows {
		if err = tbl.recurTransferDelete(memo, page, id, row, pk[row], depth); err != nil {
			return
		}
	}

	return
}

func (tbl *txnTable) commitTransferDeleteNode(id *common.ID, offset, idx int) {
	tbl.store.warChecker.Delete(id)
	tbl.txnEntries.Delete(idx)
	nodes := tbl.deleteNodes[*id]
	if offset == len(nodes.DeleteNodes)-1 {
		nodes.DeleteNodes = nodes.DeleteNodes[:offset]
	} else {
		nodes.DeleteNodes = append(nodes.DeleteNodes[:offset], nodes.DeleteNodes[offset+1:]...)
	}
	if len(nodes.DeleteNodes) == 0 {
		delete(tbl.deleteNodes, *id)
	}
}

func (tbl *txnTable) WaitSynced() {
	for _, e := range tbl.logs {
		if err := e.WaitDone(); err != nil {
			panic(err)
		}
		e.Free()
	}
}

func (tbl *txnTable) CollectCmd(cmdMgr *commandManager) (err error) {
	tbl.csnStart = uint32(cmdMgr.GetCSN())
	for idx, txnEntry := range tbl.txnEntries.entries {
		if tbl.txnEntries.IsDeleted(idx) {
			continue
		}
		csn := cmdMgr.GetCSN()
		cmd, err := txnEntry.MakeCommand(csn)
		// logutil.Infof("%d-%d",csn,cmd.GetType())
		if err != nil {
			return err
		}
		if cmd == nil {
			panic(txnEntry)
		}
		cmdMgr.AddCmd(cmd)
	}
	if tbl.tableSpace != nil {
		if err = tbl.tableSpace.CollectCmd(cmdMgr); err != nil {
			return
		}
	}
	return
}

func (tbl *txnTable) GetObject(id *types.Objectid) (obj handle.Object, err error) {
	var meta *catalog.ObjectEntry
	if meta, err = tbl.entry.GetObjectByID(id); err != nil {
		return
	}
	var ok bool
	meta.RLock()
	ok, err = meta.IsVisible(tbl.store.txn, meta.RWMutex)
	meta.RUnlock()
	if err != nil {
		return
	}
	if !ok {
		err = moerr.NewNotFoundNoCtx()
		return
	}
	obj = newObject(tbl, meta)
	return
}

func (tbl *txnTable) SoftDeleteObject(id *types.Objectid) (err error) {
	txnEntry, err := tbl.entry.DropObjectEntry(id, tbl.store.txn)
	if err != nil {
		return
	}
	tbl.store.IncreateWriteCnt()
	if txnEntry != nil {
		tbl.txnEntries.Append(txnEntry)
	}
	tbl.store.txn.GetMemo().AddObject(tbl.entry.GetDB().GetID(), tbl.entry.ID, id)
	return
}

func (tbl *txnTable) CreateObject(is1PC bool) (obj handle.Object, err error) {
	perfcounter.Update(tbl.store.ctx, func(counter *perfcounter.CounterSet) {
		counter.TAE.Object.Create.Add(1)
	})
	return tbl.createObject(catalog.ES_Appendable, is1PC, nil)
}

func (tbl *txnTable) CreateNonAppendableObject(is1PC bool, opts *objectio.CreateObjOpt) (obj handle.Object, err error) {
	perfcounter.Update(tbl.store.ctx, func(counter *perfcounter.CounterSet) {
		counter.TAE.Object.CreateNonAppendable.Add(1)
	})
	return tbl.createObject(catalog.ES_NotAppendable, is1PC, opts)
}

func (tbl *txnTable) createObject(state catalog.EntryState, is1PC bool, opts *objectio.CreateObjOpt) (obj handle.Object, err error) {
	var meta *catalog.ObjectEntry
	if meta, err = tbl.entry.CreateObject(tbl.store.txn, state, opts); err != nil {
		return
	}
	obj = newObject(tbl, meta)
	tbl.store.IncreateWriteCnt()
	tbl.store.txn.GetMemo().AddObject(tbl.entry.GetDB().ID, tbl.entry.ID, &meta.ID)
	if is1PC {
		meta.Set1PC()
	}
	tbl.txnEntries.Append(meta)
	return
}

func (tbl *txnTable) SoftDeleteBlock(id *common.ID) (err error) {
	var obj *catalog.ObjectEntry
	if obj, err = tbl.entry.GetObjectByID(id.ObjectID()); err != nil {
		return
	}
	meta, err := obj.DropBlockEntry(&id.BlockID, tbl.store.txn)
	if err != nil {
		return
	}
	tbl.store.IncreateWriteCnt()
	tbl.store.txn.GetMemo().AddBlock(tbl.entry.GetDB().ID, id.TableID, &id.BlockID)
	if meta != nil {
		tbl.txnEntries.Append(meta)
	}
	return
}

func (tbl *txnTable) LogTxnEntry(entry txnif.TxnEntry, readed []*common.ID) (err error) {
	tbl.store.IncreateWriteCnt()
	tbl.txnEntries.Append(entry)
	for _, id := range readed {
		// warChecker skip non-block read
		if objectio.IsEmptyBlkid(&id.BlockID) {
			continue
		}

		// record block into read set
		tbl.store.warChecker.InsertByID(
			tbl.entry.GetDB().ID,
			id.TableID,
			id.ObjectID(),
			&id.BlockID)
	}
	return
}

func (tbl *txnTable) GetBlock(id *common.ID) (blk handle.Block, err error) {
	meta, err := tbl.store.warChecker.CacheGet(
		tbl.entry.GetDB().ID,
		id.TableID,
		id.ObjectID(),
		&id.BlockID)
	if err != nil {
		return
	}
	blk = buildBlock(tbl, meta)
	return
}

func (tbl *txnTable) CreateNonAppendableBlock(sid *types.Objectid, opts *objectio.CreateBlockOpt) (blk handle.Block, err error) {
	return tbl.createBlock(sid, catalog.ES_NotAppendable, false, opts)
}

func (tbl *txnTable) CreateBlock(sid *types.Objectid, is1PC bool) (blk handle.Block, err error) {
	return tbl.createBlock(sid, catalog.ES_Appendable, is1PC, nil)
}

func (tbl *txnTable) createBlock(
	sid *types.Objectid,
	state catalog.EntryState,
	is1PC bool,
	opts *objectio.CreateBlockOpt) (blk handle.Block, err error) {
	var obj *catalog.ObjectEntry
	if obj, err = tbl.entry.GetObjectByID(sid); err != nil {
		return
	}
	if !obj.IsAppendable() && state == catalog.ES_Appendable {
		err = moerr.NewInternalErrorNoCtx("not appendable")
		return
	}
	var factory catalog.BlockDataFactory
	if tbl.store.dataFactory != nil {
		factory = tbl.store.dataFactory.MakeBlockFactory()
	}
	meta, err := obj.CreateBlock(tbl.store.txn, state, factory, opts)
	if err != nil {
		return
	}
	if is1PC {
		meta.Set1PC()
	}
	tbl.store.IncreateWriteCnt()
	id := meta.AsCommonID()
	tbl.store.txn.GetMemo().AddBlock(tbl.entry.GetDB().ID, id.TableID, &id.BlockID)
	tbl.txnEntries.Append(meta)
	return buildBlock(tbl, meta), err
}

func (tbl *txnTable) SetCreateEntry(e txnif.TxnEntry) {
	if tbl.createEntry != nil {
		panic("logic error")
	}
	tbl.store.IncreateWriteCnt()
	tbl.store.txn.GetMemo().AddCatalogChange()
	tbl.createEntry = e
	tbl.txnEntries.Append(e)
}

func (tbl *txnTable) SetDropEntry(e txnif.TxnEntry) error {
	if tbl.dropEntry != nil {
		panic("logic error")
	}
	tbl.store.IncreateWriteCnt()
	tbl.store.txn.GetMemo().AddCatalogChange()
	tbl.dropEntry = e
	tbl.txnEntries.Append(e)
	return nil
}

func (tbl *txnTable) IsDeleted() bool {
	return tbl.dropEntry != nil
}

// GetLocalSchema returns the schema remains in the txn table, rather than the
// latest schema in TableEntry
func (tbl *txnTable) GetLocalSchema() *catalog.Schema {
	return tbl.schema
}

func (tbl *txnTable) GetMeta() *catalog.TableEntry {
	return tbl.entry
}

func (tbl *txnTable) GetID() uint64 {
	return tbl.entry.GetID()
}

func (tbl *txnTable) Close() error {
	var err error
	if tbl.tableSpace != nil {
		if err = tbl.tableSpace.Close(); err != nil {
			return err
		}
		tbl.tableSpace = nil
	}
	tbl.deleteNodes = nil
	tbl.logs = nil
	tbl.txnEntries = nil
	return nil
}

func (tbl *txnTable) AddDeleteNode(id *common.ID, node txnif.DeleteNode) error {
	nid := *id
	u := tbl.deleteNodes[nid]
	if u != nil {
		for _, n := range u.DeleteNodes {
			if n.IsPersistedDeletedNode() == node.IsPersistedDeletedNode() {
				return ErrDuplicateNode
			}
		}
		u.DeleteNodes = append(u.DeleteNodes, node)
		if !node.IsPersistedDeletedNode() {
			u.idx = append(u.idx, tbl.txnEntries.Len())
		}
	} else {
		tbl.store.IncreateWriteCnt()
		tbl.store.txn.GetMemo().AddBlock(tbl.entry.GetDB().ID, id.TableID, &id.BlockID)
		tbl.deleteNodes[nid] = newDeleteNode(node, tbl.txnEntries.Len())
	}
	tbl.txnEntries.Append(node)
	return nil
}

func (tbl *txnTable) Append(ctx context.Context, data *containers.Batch) (err error) {
	if tbl.schema.HasPK() {
		dedupType := tbl.store.txn.GetDedupType()
		if dedupType == txnif.FullDedup {
			//do PK deduplication check against txn's work space.
			if err = tbl.DedupWorkSpace(
				data.Vecs[tbl.schema.GetSingleSortKeyIdx()]); err != nil {
				return
			}
			//do PK deduplication check against txn's snapshot data.
			if err = tbl.DedupSnapByPK(
				ctx,
				data.Vecs[tbl.schema.GetSingleSortKeyIdx()], false); err != nil {
				return
			}
		} else if dedupType == txnif.FullSkipWorkSpaceDedup {
			if err = tbl.DedupSnapByPK(
				ctx,
				data.Vecs[tbl.schema.GetSingleSortKeyIdx()], false); err != nil {
				return
			}
		} else if dedupType == txnif.IncrementalDedup {
			if err = tbl.DedupSnapByPK(
				ctx,
				data.Vecs[tbl.schema.GetSingleSortKeyIdx()], true); err != nil {
				return
			}
		}
	}
	if tbl.tableSpace == nil {
		tbl.tableSpace = newTableSpace(tbl)
	}
	return tbl.tableSpace.Append(data)
}
func (tbl *txnTable) AddBlksWithMetaLoc(ctx context.Context, stats containers.Vector) (err error) {
	return stats.Foreach(func(v any, isNull bool, row int) error {
		s := objectio.ObjectStats(v.([]byte))
		return tbl.addBlksWithMetaLoc(ctx, s)
	}, nil)
}
func (tbl *txnTable) addBlksWithMetaLoc(ctx context.Context, stats objectio.ObjectStats) (err error) {
	var pkVecs []containers.Vector
	var closeFuncs []func()
	defer func() {
		for _, v := range pkVecs {
			v.Close()
		}
		for _, f := range closeFuncs {
			f()
		}
	}()
	if tbl.tableSpace != nil && tbl.tableSpace.isStatsExisted(stats) {
		return nil
	}
	metaLocs := make([]objectio.Location, 0)
	blkCount := stats.BlkCnt()
	totalRow := stats.Rows()
	blkMaxRows := tbl.schema.BlockMaxRows
	for i := uint16(0); i < uint16(blkCount); i++ {
		var blkRow uint32
		if totalRow > blkMaxRows {
			blkRow = blkMaxRows
		} else {
			blkRow = totalRow
		}
		totalRow -= blkRow
		metaloc := objectio.BuildLocation(stats.ObjectName(), stats.Extent(), blkRow, i)

		metaLocs = append(metaLocs, metaloc)
	}
	if tbl.schema.HasPK() {
		dedupType := tbl.store.txn.GetDedupType()
		if dedupType == txnif.FullDedup {
			//TODO::parallel load pk.
			for _, loc := range metaLocs {
				var vectors []containers.Vector
				var closeFunc func()
				//Extend lifetime of vectors is within the function.
				//No NeedCopy. closeFunc is required after use.
				//VectorPool is nil.
				vectors, closeFunc, err = blockio.LoadColumns2(
					ctx,
					[]uint16{uint16(tbl.schema.GetSingleSortKeyIdx())},
					nil,
					tbl.store.rt.Fs.Service,
					loc,
					fileservice.Policy(0),
					false,
					nil,
				)
				if err != nil {
					return err
				}
				closeFuncs = append(closeFuncs, closeFunc)
				pkVecs = append(pkVecs, vectors[0])
			}
			for _, v := range pkVecs {
				//do PK deduplication check against txn's work space.
				if err = tbl.DedupWorkSpace(v); err != nil {
					return
				}
				//do PK deduplication check against txn's snapshot data.
				if err = tbl.DedupSnapByPK(ctx, v, false); err != nil {
					return
				}
			}
		} else if dedupType == txnif.FullSkipWorkSpaceDedup {
			//do PK deduplication check against txn's snapshot data.
			if err = tbl.DedupSnapByMetaLocs(ctx, metaLocs, false); err != nil {
				return
			}
		} else if dedupType == txnif.IncrementalDedup {
			//do PK deduplication check against txn's snapshot data.
			if err = tbl.DedupSnapByMetaLocs(ctx, metaLocs, true); err != nil {
				return
			}
		}
	}
	if tbl.tableSpace == nil {
		tbl.tableSpace = newTableSpace(tbl)
	}
	return tbl.tableSpace.AddBlksWithMetaLoc(pkVecs, stats)
}

func (tbl *txnTable) RangeDeleteLocalRows(start, end uint32) (err error) {
	if tbl.tableSpace != nil {
		err = tbl.tableSpace.RangeDelete(start, end)
	}
	return
}

func (tbl *txnTable) LocalDeletesToString() string {
	s := fmt.Sprintf("<txnTable-%d>[LocalDeletes]:\n", tbl.GetID())
	if tbl.tableSpace != nil {
		s = fmt.Sprintf("%s%s", s, tbl.tableSpace.DeletesToString())
	}
	return s
}

func (tbl *txnTable) IsLocalDeleted(row uint32) bool {
	if tbl.tableSpace == nil {
		return false
	}
	return tbl.tableSpace.IsDeleted(row)
}

// RangeDelete delete block rows in range [start, end]
func (tbl *txnTable) RangeDelete(
	id *common.ID,
	start,
	end uint32,
	pk containers.Vector,
	dt handle.DeleteType) (err error) {
	defer func() {
		if err == nil {
			return
		}
		// if moerr.IsMoErrCode(err, moerr.ErrTxnWWConflict) {
		// 	moerr.NewTxnWriteConflictNoCtx("table-%d blk-%d delete rows from %d to %d",
		// 		id.TableID,
		// 		id.BlockID,
		// 		start,
		// 		end)
		// }
		// This err also captured by txn's write conflict check.
		if err != nil {
			logutil.Debugf("[ts=%s]: table-%d blk-%s delete rows from %d to %d %v",
				tbl.store.txn.GetStartTS().ToString(),
				id.TableID,
				id.BlockID.String(),
				start,
				end,
				err)
			if tbl.store.rt.Options.IncrementalDedup && moerr.IsMoErrCode(err, moerr.ErrTxnWWConflict) {
				logutil.Warnf("[txn%X,ts=%s]: table-%d blk-%s delete rows [%d,%d] pk %s",
					tbl.store.txn.GetID(),
					tbl.store.txn.GetStartTS().ToString(),
					id.TableID,
					id.BlockID.String(),
					start, end,
					pk.PPString(int(start-end+1)),
				)
			}
		}
	}()
	if tbl.tableSpace != nil && id.ObjectID().Eq(tbl.tableSpace.entry.ID) {
		err = tbl.RangeDeleteLocalRows(start, end)
		return
	}
	node := tbl.getNormalDeleteNode(*id)

	if node != nil {
		// TODO: refactor
		chain := node.GetChain().(*updates.DeleteChain)
		mvcc := chain.GetController()
		mvcc.Lock()
		if err = mvcc.CheckNotDeleted(start, end, tbl.store.txn.GetStartTS()); err == nil {
			node.RangeDeleteLocked(start, end, pk, common.WorkspaceAllocator)
		}
		mvcc.Unlock()
		if err != nil {
			tbl.store.warChecker.Insert(mvcc.GetEntry())
		}
		return
	}

	blk, err := tbl.store.warChecker.CacheGet(
		tbl.entry.GetDB().ID,
		id.TableID, id.ObjectID(),
		&id.BlockID)
	if err != nil {
		return
	}
	blkData := blk.GetBlockData()
	node2, err := blkData.RangeDelete(tbl.store.txn, start, end, pk, dt)
	if err == nil {
		if err = tbl.AddDeleteNode(id, node2); err != nil {
			return
		}
		tbl.store.warChecker.Insert(blk)
	}
	return
}

func (tbl *txnTable) TryDeleteByDeltaloc(id *common.ID, deltaloc objectio.Location) (ok bool, err error) {
	node := tbl.deleteNodes[*id]
	if node != nil {
		return
	}

	blk, err := tbl.store.warChecker.CacheGet(
		tbl.entry.GetDB().ID,
		id.TableID, id.ObjectID(),
		&id.BlockID)
	if err != nil {
		return
	}
	blkData := blk.GetBlockData()
	node2, ok, err := blkData.TryDeleteByDeltaloc(tbl.store.txn, deltaloc)
	if err == nil && ok {
		if err = tbl.UpdateDeltaLoc(id, deltaloc); err != nil {
			err = node2.PrepareRollback()
			return false, err
		}
		if err = tbl.AddDeleteNode(id, node2); err != nil {
			return
		}
		tbl.store.warChecker.Insert(blk)
	}
	return
}

func (tbl *txnTable) GetByFilter(ctx context.Context, filter *handle.Filter) (id *common.ID, offset uint32, err error) {
	if tbl.tableSpace != nil {
		id, offset, err = tbl.tableSpace.GetByFilter(filter)
		if err == nil {
			return
		}
		err = nil
	}
	h := newRelation(tbl)
	blockIt := h.MakeBlockIt()
	for blockIt.Valid() {
		h := blockIt.GetBlock()
		defer h.Close()
		if h.IsUncommitted() {
			blockIt.Next()
			continue
		}
		offset, err = h.GetByFilter(ctx, filter, common.WorkspaceAllocator)
		if err == nil {
			id = h.Fingerprint()
			break
		}
		blockIt.Next()
	}
	if err == nil && id == nil {
		err = moerr.NewNotFoundNoCtx()
	}
	return
}

func (tbl *txnTable) GetLocalValue(row uint32, col uint16) (v any, isNull bool, err error) {
	if tbl.tableSpace == nil {
		return
	}
	return tbl.tableSpace.GetValue(row, col)
}

func (tbl *txnTable) GetValue(ctx context.Context, id *common.ID, row uint32, col uint16) (v any, isNull bool, err error) {
	if tbl.tableSpace != nil && id.ObjectID().Eq(tbl.tableSpace.entry.ID) {
		return tbl.tableSpace.GetValue(row, col)
	}
	meta, err := tbl.store.warChecker.CacheGet(
		tbl.entry.GetDB().ID,
		id.TableID,
		id.ObjectID(),
		&id.BlockID)
	if err != nil {
		panic(err)
	}
	block := meta.GetBlockData()
	return block.GetValue(ctx, tbl.store.txn, tbl.GetLocalSchema(), int(row), int(col), common.WorkspaceAllocator)
}
func (tbl *txnTable) UpdateObjectStats(id *common.ID, stats *objectio.ObjectStats) error {
	meta, err := tbl.entry.GetObjectByID(id.ObjectID())
	if err != nil {
		return err
	}
	isNewNode, err := meta.UpdateObjectInfo(tbl.store.txn, stats)
	if err != nil {
		return err
	}
	tbl.store.txn.GetMemo().AddObject(tbl.entry.GetDB().ID, tbl.entry.ID, &meta.ID)
	if isNewNode {
		tbl.txnEntries.Append(meta)
	}
	return nil
}

func (tbl *txnTable) UpdateMetaLoc(id *common.ID, metaLoc objectio.Location) (err error) {
	meta, err := tbl.store.warChecker.CacheGet(
		tbl.entry.GetDB().ID,
		id.TableID,
		id.ObjectID(),
		&id.BlockID)
	if err != nil {
		panic(err)
	}
	isNewNode, err := meta.UpdateMetaLoc(tbl.store.txn, metaLoc)
	if err != nil {
		return
	}
	tbl.store.txn.GetMemo().AddBlock(tbl.entry.GetDB().ID, id.TableID, &id.BlockID)
	if isNewNode {
		tbl.txnEntries.Append(meta)
	}
	return
}

func (tbl *txnTable) UpdateDeltaLoc(id *common.ID, deltaloc objectio.Location) (err error) {
	meta, err := tbl.store.warChecker.CacheGet(
		tbl.entry.GetDB().ID,
		id.TableID,
		id.ObjectID(),
		&id.BlockID)
	if err != nil {
		panic(err)
	}
	isNewNode, err := meta.UpdateDeltaLoc(tbl.store.txn, deltaloc)
	if err != nil {
		return
	}
	tbl.store.txn.GetMemo().AddBlock(tbl.entry.GetDB().ID, id.TableID, &id.BlockID)
	if isNewNode {
		tbl.txnEntries.Append(meta)
	}
	return
}

func (tbl *txnTable) AlterTable(ctx context.Context, req *apipb.AlterTableReq) error {
	switch req.Kind {
	case apipb.AlterKind_UpdateConstraint,
		apipb.AlterKind_UpdateComment,
		apipb.AlterKind_AddColumn,
		apipb.AlterKind_DropColumn,
		apipb.AlterKind_RenameTable,
		apipb.AlterKind_UpdatePolicy,
		apipb.AlterKind_AddPartition,
		apipb.AlterKind_RenameColumn:
	default:
		return moerr.NewNYI(ctx, "alter table %s", req.Kind.String())
	}
	tbl.store.IncreateWriteCnt()
	tbl.store.txn.GetMemo().AddCatalogChange()
	isNewNode, newSchema, err := tbl.entry.AlterTable(ctx, tbl.store.txn, req)
	if isNewNode {
		tbl.txnEntries.Append(tbl.entry)
	}
	if err != nil {
		return err
	}
	if req.Kind == apipb.AlterKind_RenameTable {
		rename := req.GetRenameTable()
		// udpate name index in db entry
		tenantID := newSchema.AcInfo.TenantID
		err = tbl.entry.GetDB().RenameTableInTxn(rename.OldName, rename.NewName, tbl.entry.ID, tenantID, tbl.store.txn, isNewNode)
		if err != nil {
			return err
		}
	}

	tbl.schema = newSchema // update new schema to txn local schema
	//TODO(aptend): handle written data in localobj, keep the batch aligned with the new schema
	return err
}

func (tbl *txnTable) UncommittedRows() uint32 {
	if tbl.tableSpace == nil {
		return 0
	}
	return tbl.tableSpace.Rows()
}
func (tbl *txnTable) NeedRollback() bool {
	return tbl.createEntry != nil && tbl.dropEntry != nil
}

// PrePrepareDedup do deduplication check for 1PC Commit or 2PC Prepare
func (tbl *txnTable) PrePrepareDedup(ctx context.Context) (err error) {
	if tbl.tableSpace == nil || !tbl.schema.HasPK() {
		return
	}
	var zm index.ZM
	pkColPos := tbl.schema.GetSingleSortKeyIdx()
	for _, node := range tbl.tableSpace.nodes {
		if node.IsPersisted() {
			err = tbl.DoPrecommitDedupByNode(ctx, node)
			if err != nil {
				return
			}
			continue
		}
		pkVec, err := node.WindowColumn(0, node.Rows(), pkColPos)
		if err != nil {
			return err
		}
		if zm.Valid() {
			zm.ResetMinMax()
		} else {
			pkType := pkVec.GetType()
			zm = index.NewZM(pkType.Oid, pkType.Scale)
		}
		if err = index.BatchUpdateZM(zm, pkVec.GetDownstreamVector()); err != nil {
			pkVec.Close()
			return err
		}
		if err = tbl.DoPrecommitDedupByPK(pkVec, zm); err != nil {
			pkVec.Close()
			return err
		}
		pkVec.Close()
	}
	return
}

func (tbl *txnTable) updateDedupedObjectHintAndBlockID(hint uint64, id *types.Blockid) {
	if tbl.dedupedObjectHint == 0 {
		tbl.dedupedObjectHint = hint
		tbl.dedupedBlockID = id
		return
	}
	if tbl.dedupedObjectHint > hint {
		tbl.dedupedObjectHint = hint
		tbl.dedupedObjectHint = hint
		return
	}
	if tbl.dedupedObjectHint == hint && tbl.dedupedBlockID.Compare(*id) > 0 {
		tbl.dedupedBlockID = id
	}
}

func (tbl *txnTable) quickSkipThisBlock(
	ctx context.Context,
	keysZM index.ZM,
	meta *catalog.BlockEntry,
) (ok bool, err error) {
	zm, err := meta.GetPKZoneMap(ctx, tbl.store.rt.Fs.Service)
	if err != nil {
		return
	}
	ok = !zm.FastIntersect(keysZM)
	return
}

func (tbl *txnTable) tryGetCurrentObjectBF(
	ctx context.Context,
	currLocation objectio.Location,
	prevBF objectio.BloomFilter,
	prevObjName *objectio.ObjectNameShort,
) (currBf objectio.BloomFilter, err error) {
	if len(currLocation) == 0 {
		return
	}
	if objectio.IsSameObjectLocVsShort(currLocation, prevObjName) {
		currBf = prevBF
		return
	}
	currBf, err = objectio.FastLoadBF(
		ctx,
		currLocation,
		false,
		tbl.store.rt.Fs.Service,
	)
	return
}

// DedupSnapByPK 1. checks whether these primary keys exist in the list of block
// which are visible and not dropped at txn's snapshot timestamp.
// 2. It is called when appending data into this table.
func (tbl *txnTable) DedupSnapByPK(ctx context.Context, keys containers.Vector, dedupAfterSnapshotTS bool) (err error) {
	r := trace.StartRegion(ctx, "DedupSnapByPK")
	defer r.End()
	h := newRelation(tbl)
	it := newRelationBlockItOnSnap(h)
	maxObjectHint := uint64(0)
	pkType := keys.GetType()
	keysZM := index.NewZM(pkType.Oid, pkType.Scale)
	if err = index.BatchUpdateZM(keysZM, keys.GetDownstreamVector()); err != nil {
		return
	}
	var (
		name objectio.ObjectNameShort
		bf   objectio.BloomFilter
	)
	maxBlockID := &types.Blockid{}
	for it.Valid() {
		blkH := it.GetBlock()
		blk := blkH.GetMeta().(*catalog.BlockEntry)
		blkH.Close()
		ObjectHint := blk.GetObject().SortHint
		if ObjectHint > maxObjectHint {
			maxObjectHint = ObjectHint
			maxBlockID = &blk.ID
		}
		if blk.ID.Compare(*maxBlockID) > 0 {
			maxBlockID = &blk.ID
		}
		blkData := blk.GetBlockData()
		if blkData == nil {
			it.Next()
			continue
		}
		if dedupAfterSnapshotTS && blkData.CoarseCheckAllRowsCommittedBefore(tbl.store.txn.GetSnapshotTS()) {
			it.Next()
			continue
		}
		var rowmask *roaring.Bitmap
		if len(tbl.deleteNodes) > 0 {
			fp := blk.AsCommonID()
			deleteNode := tbl.getNormalDeleteNode(*fp)
			if deleteNode != nil {
				rowmask = deleteNode.GetRowMaskRefLocked()
			}
		}
		location := blk.FastGetMetaLoc()
		if len(location) > 0 {
			var skip bool
			if skip, err = tbl.quickSkipThisBlock(ctx, keysZM, blk); err != nil {
				return
			} else if skip {
				it.Next()
				continue
			}
		}
		if bf, err = tbl.tryGetCurrentObjectBF(
			ctx,
			location,
			bf,
			&name,
		); err != nil {
			return
		}
		name = *objectio.ToObjectNameShort(&blk.ID)

		if err = blkData.BatchDedup(
			ctx,
			tbl.store.txn,
			keys,
			keysZM,
			rowmask,
			false,
			bf,
			common.WorkspaceAllocator,
		); err != nil {
			// logutil.Infof("%s, %s, %v", blk.String(), rowmask, err)
			return
		}
		it.Next()
	}
	tbl.updateDedupedObjectHintAndBlockID(maxObjectHint, maxBlockID)
	return
}

// DedupSnapByMetaLocs 1. checks whether the Primary Key of all the input blocks exist in the list of block
// which are visible and not dropped at txn's snapshot timestamp.
// 2. It is called when appending blocks into this table.
func (tbl *txnTable) DedupSnapByMetaLocs(ctx context.Context, metaLocs []objectio.Location, dedupAfterSnapshotTS bool) (err error) {
	loaded := make(map[int]containers.Vector)
	maxObjectHint := uint64(0)
	maxBlockID := &types.Blockid{}
	for i, loc := range metaLocs {
		h := newRelation(tbl)
		it := newRelationBlockItOnSnap(h)
		for it.Valid() {
			blk := it.GetBlock().GetMeta().(*catalog.BlockEntry)
			ObjectHint := blk.GetObject().SortHint
			if ObjectHint > maxObjectHint {
				maxObjectHint = ObjectHint
				maxBlockID = &blk.ID
			}
			if blk.ID.Compare(*maxBlockID) > 0 {
				maxBlockID = &blk.ID
			}
			blkData := blk.GetBlockData()
			if blkData == nil {
				it.Next()
				continue
			}

			// if it is in the incremental deduplication scenario
			// coarse check whether all rows in this block are committed before the snapshot timestamp
			// if true, skip this block's deduplication
			if dedupAfterSnapshotTS &&
				blkData.CoarseCheckAllRowsCommittedBefore(tbl.store.txn.GetSnapshotTS()) {
				it.Next()
				continue
			}

			var rowmask *roaring.Bitmap
			if len(tbl.deleteNodes) > 0 {
				fp := blk.AsCommonID()
				deleteNode := tbl.getNormalDeleteNode(*fp)
				if deleteNode != nil {
					rowmask = deleteNode.GetRowMaskRefLocked()
				}
			}
			//TODO::laod zm index first, then load pk column if necessary.
			_, ok := loaded[i]
			if !ok {
				//Extend lifetime of vectors is within the function.
				//No NeedCopy. closeFunc is required after use.
				//VectorPool is nil.
				vectors, closeFunc, err := blockio.LoadColumns2(
					ctx,
					[]uint16{uint16(tbl.schema.GetSingleSortKeyIdx())},
					nil,
					tbl.store.rt.Fs.Service,
					loc,
					fileservice.Policy(0),
					false,
					nil,
				)
				if err != nil {
					return err
				}
				defer closeFunc()
				loaded[i] = vectors[0]
			}
			if err = blkData.BatchDedup(
				ctx,
				tbl.store.txn,
				loaded[i],
				nil,
				rowmask,
				false,
				objectio.BloomFilter{},
				common.WorkspaceAllocator,
			); err != nil {
				// logutil.Infof("%s, %s, %v", blk.String(), rowmask, err)
				loaded[i].Close()
				return
			}
			it.Next()
		}
		if v, ok := loaded[i]; ok {
			v.Close()
		}
		tbl.updateDedupedObjectHintAndBlockID(maxObjectHint, maxBlockID)
	}
	return
}

// DoPrecommitDedupByPK 1. it do deduplication by traversing all the Objects/blocks, and
// skipping over some blocks/Objects which being active or drop-committed or aborted;
//  2. it is called when txn dequeues from preparing queue.
//  3. we should make this function run quickly as soon as possible.
//     TODO::it would be used to do deduplication with the logtail.
func (tbl *txnTable) DoPrecommitDedupByPK(pks containers.Vector, pksZM index.ZM) (err error) {
	moprobe.WithRegion(context.Background(), moprobe.TxnTableDoPrecommitDedupByPK, func() {
		objIt := tbl.entry.MakeObjectIt(false)
		for objIt.Valid() {
			obj := objIt.Get().GetPayload()
			if obj.SortHint < tbl.dedupedObjectHint {
				break
			}
			{
				obj.RLock()
				//FIXME:: Why need to wait committing here? waiting had happened at Dedup.
				//needwait, txnToWait := obj.NeedWaitCommitting(tbl.store.txn.GetStartTS())
				//if needwait {
				//	obj.RUnlock()
				//	txnToWait.GetTxnState(true)
				//	obj.RLock()
				//}
				shouldSkip := obj.HasDropCommittedLocked() || obj.IsCreatingOrAborted()
				obj.RUnlock()
				if shouldSkip {
					objIt.Next()
					continue
				}
			}
			var shouldSkip bool
			err = nil
			blkIt := obj.MakeBlockIt(false)
			for blkIt.Valid() {
				blk := blkIt.Get().GetPayload()
				if obj.SortHint == tbl.dedupedObjectHint {
					if blk.ID.Compare(*tbl.dedupedBlockID) < 0 {
						break
					}
				}
				{
					blk.RLock()
					shouldSkip = blk.HasDropCommittedLocked() || blk.IsCreatingOrAborted()
					blk.RUnlock()
					if shouldSkip {
						blkIt.Next()
						continue
					}
				}
				blkData := blk.GetBlockData()
				var rowmask *roaring.Bitmap
				if len(tbl.deleteNodes) > 0 {
					if tbl.store.warChecker.HasConflict(blk.ID) {
						continue
					}
					fp := blk.AsCommonID()
					deleteNode := tbl.getNormalDeleteNode(*fp)
					if deleteNode != nil {
						rowmask = deleteNode.GetRowMaskRefLocked()
					}
				}
				if err = blkData.BatchDedup(
					context.Background(),
					tbl.store.txn,
					pks,
					pksZM,
					rowmask,
					true,
					objectio.BloomFilter{},
					common.WorkspaceAllocator,
				); err != nil {
					return
				}
				blkIt.Next()
			}
			objIt.Next()
		}
	})
	return
}

func (tbl *txnTable) DoPrecommitDedupByNode(ctx context.Context, node InsertNode) (err error) {
	objIt := tbl.entry.MakeObjectIt(false)
	var pks containers.Vector
	//loaded := false
	for objIt.Valid() {
		obj := objIt.Get().GetPayload()
		if obj.SortHint < tbl.dedupedObjectHint {
			break
		}
		{
			obj.RLock()
			//FIXME:: Why need to wait committing here? waiting had happened at Dedup.
			//needwait, txnToWait := obj.NeedWaitCommitting(tbl.store.txn.GetStartTS())
			//if needwait {
			//	obj.RUnlock()
			//	txnToWait.GetTxnState(true)
			//	obj.RLock()
			//}
			shouldSkip := obj.HasDropCommittedLocked() || obj.IsCreatingOrAborted()
			obj.RUnlock()
			if shouldSkip {
				objIt.Next()
				continue
			}
		}

		//TODO::load ZM/BF index first, then load PK column if necessary.
		if pks == nil {
			colV, err := node.GetColumnDataById(ctx, tbl.schema.GetSingleSortKeyIdx(), common.WorkspaceAllocator)
			if err != nil {
				return err
			}
			colV.ApplyDeletes()
			pks = colV.Orphan()
			defer pks.Close()
		}
		var shouldSkip bool
		err = nil
		blkIt := obj.MakeBlockIt(false)
		for blkIt.Valid() {
			blk := blkIt.Get().GetPayload()
			if obj.SortHint == tbl.dedupedObjectHint {
				if blk.ID.Compare(*tbl.dedupedBlockID) < 0 {
					break
				}
			}
			{
				blk.RLock()
				shouldSkip = blk.HasDropCommittedLocked() || blk.IsCreatingOrAborted()
				blk.RUnlock()
				if shouldSkip {
					blkIt.Next()
					continue
				}
			}
			blkData := blk.GetBlockData()
			var rowmask *roaring.Bitmap
			if len(tbl.deleteNodes) > 0 {
				if tbl.store.warChecker.HasConflict(blk.ID) {
					continue
				}
				fp := blk.AsCommonID()
				deleteNode := tbl.getNormalDeleteNode(*fp)
				if deleteNode != nil {
					rowmask = deleteNode.GetRowMaskRefLocked()
				}
			}
			if err = blkData.BatchDedup(
				context.Background(),
				tbl.store.txn,
				pks,
				nil,
				rowmask,
				true,
				objectio.BloomFilter{},
				common.WorkspaceAllocator,
			); err != nil {
				return err
			}
			blkIt.Next()
		}
		objIt.Next()
	}
	return
}

func (tbl *txnTable) DedupWorkSpace(key containers.Vector) (err error) {
	index := NewSimpleTableIndex()
	//Check whether primary key is duplicated.
	if err = index.BatchInsert(
		tbl.schema.GetSingleSortKey().Name,
		key,
		0,
		key.Length(),
		0,
		true); err != nil {
		return
	}

	if tbl.tableSpace != nil {
		//Check whether primary key is duplicated in txn's workspace.
		if err = tbl.tableSpace.BatchDedup(key); err != nil {
			return
		}
	}
	return
}

func (tbl *txnTable) DoBatchDedup(key containers.Vector) (err error) {
	index := NewSimpleTableIndex()
	//Check whether primary key is duplicated.
	if err = index.BatchInsert(
		tbl.schema.GetSingleSortKey().Name,
		key,
		0,
		key.Length(),
		0,
		true); err != nil {
		return
	}

	if tbl.tableSpace != nil {
		//Check whether primary key is duplicated in txn's workspace.
		if err = tbl.tableSpace.BatchDedup(key); err != nil {
			return
		}
	}
	//Check whether primary key is duplicated in txn's snapshot data.
	err = tbl.DedupSnapByPK(context.Background(), key, false)
	return
}

func (tbl *txnTable) BatchDedupLocal(bat *containers.Batch) (err error) {
	if tbl.tableSpace == nil || !tbl.schema.HasPK() {
		return
	}
	err = tbl.tableSpace.BatchDedup(bat.Vecs[tbl.schema.GetSingleSortKeyIdx()])
	return
}

func (tbl *txnTable) PrepareRollback() (err error) {
	for idx, txnEntry := range tbl.txnEntries.entries {
		if tbl.txnEntries.IsDeleted(idx) {
			continue
		}
		if err = txnEntry.PrepareRollback(); err != nil {
			break
		}
	}
	return
}

func (tbl *txnTable) ApplyAppend() (err error) {
	if tbl.tableSpace != nil {
		err = tbl.tableSpace.ApplyAppend()
	}
	return
}

func (tbl *txnTable) PrePrepare() (err error) {
	if tbl.tableSpace != nil {
		err = tbl.tableSpace.PrepareApply()
	}
	return
}

func (tbl *txnTable) PrepareCommit() (err error) {
	for idx, node := range tbl.txnEntries.entries {
		if tbl.txnEntries.IsDeleted(idx) {
			continue
		}
		if err = node.PrepareCommit(); err != nil {
			break
		}
	}
	return
}

func (tbl *txnTable) PreApplyCommit() (err error) {
	return tbl.ApplyAppend()
}

func (tbl *txnTable) ApplyCommit() (err error) {
	csn := tbl.csnStart
	for idx, node := range tbl.txnEntries.entries {
		if tbl.txnEntries.IsDeleted(idx) {
			continue
		}
		if node.Is1PC() {
			continue
		}
		if err = node.ApplyCommit(); err != nil {
			break
		}
		csn++
	}
	return
}

func (tbl *txnTable) Apply1PCCommit() (err error) {
	for idx, node := range tbl.txnEntries.entries {
		if tbl.txnEntries.IsDeleted(idx) {
			continue
		}
		if !node.Is1PC() {
			continue
		}
		if err = node.ApplyCommit(); err != nil {
			break
		}
		tbl.csnStart++
	}
	return
}
func (tbl *txnTable) ApplyRollback() (err error) {
	csn := tbl.csnStart
	for idx, node := range tbl.txnEntries.entries {
		if tbl.txnEntries.IsDeleted(idx) {
			continue
		}
		if node.Is1PC() {
			continue
		}
		if err = node.ApplyRollback(); err != nil {
			break
		}
		csn++
	}
	return
}

func (tbl *txnTable) CleanUp() {
	if tbl.tableSpace != nil {
		tbl.tableSpace.CloseAppends()
	}
}
