package db

import (
	"bytes"
	"fmt"
	"io/ioutil"
	"os"
	"path"
	"sync"

	"github.com/RoaringBitmap/roaring"
	gbat "github.com/matrixorigin/matrixone/pkg/container/batch"
	"github.com/matrixorigin/matrixone/pkg/logutil"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/catalog"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/common"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/compute"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/container/batch"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/iface/txnif"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/tables"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/tables/updates"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/txn/txnbase"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/txn/txnimpl"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/wal"
)

const DefaultReplayCacheSize = 2 * common.M

type Replayer struct {
	DataFactory  *tables.DataFactory
	db           *DB
	maxTs        uint64
	cache        *bytes.Buffer
	staleIndexes []*wal.Index
	once         sync.Once
}

func newReplayer(dataFactory *tables.DataFactory, db *DB) *Replayer {
	return &Replayer{
		DataFactory:  dataFactory,
		db:           db,
		cache:        bytes.NewBuffer(make([]byte, DefaultReplayCacheSize)),
		staleIndexes: make([]*wal.Index, 0),
	}
}

func (replayer *Replayer) PreReplayWal() {
	processor := new(catalog.LoopProcessor)
	processor.BlockFn = func(entry *catalog.BlockEntry) (err error) {
		entry.InitData(replayer.DataFactory)
		blkData := entry.GetBlockData()
		replayer.OnTimeStamp(blkData.GetMaxCheckpointTS())
		return
	}
	processor.SegmentFn = func(entry *catalog.SegmentEntry) (err error) {
		if entry.GetTable().IsVirtual() {
			return catalog.ErrStopCurrRecur
		}
		dropCommit := entry.TreeMaxDropCommitEntry()
		if dropCommit != nil && dropCommit.LogIndex.LSN <= replayer.db.Wal.GetCheckpointed() {
			return catalog.ErrStopCurrRecur
		}
		entry.InitData(replayer.DataFactory)
		entry.ReplayFile(replayer.cache)
		return
	}
	if err := replayer.db.Catalog.RecurLoop(processor); err != nil {
		if err != catalog.ErrStopCurrRecur {
			panic(err)
		}
	}
}

func (replayer *Replayer) scanFiles() map[uint64]string {
	files := make(map[uint64]string)
	infos, err := ioutil.ReadDir(replayer.db.Dir)
	if err != nil {
		panic(err)
	}
	for _, info := range infos {
		if info.IsDir() {
			continue
		}
		name := info.Name()
		id, err := replayer.db.FileFactory.DecodeName(name)
		if err != nil {
			continue
		}
		files[id] = path.Join(replayer.db.Dir, name)
	}
	return files
}

func (replayer *Replayer) Replay() {
	if err := replayer.db.Wal.Replay(replayer.OnReplayEntry); err != nil {
		panic(err)
	}
	if _, err := replayer.db.Wal.Checkpoint(replayer.staleIndexes); err != nil {
		panic(err)
	}
	replayer.PostReplayWal()
}

func (replayer *Replayer) PostReplayWal() {
	activeSegs := make(map[uint64]*catalog.SegmentEntry)
	processor := new(catalog.LoopProcessor)
	processor.DatabaseFn = func(entry *catalog.DBEntry) (err error) {
		if entry.IsActive() {
			return
		}
		if entry.GetLogIndex().LSN > replayer.db.Wal.GetCheckpointed() {
			return
		}
		if err = entry.GetCatalog().RemoveEntry(entry); err != nil {
			panic(err)
		}
		err = catalog.ErrStopCurrRecur
		return
	}
	processor.TableFn = func(entry *catalog.TableEntry) (err error) {
		if entry.IsActive() {
			return
		}
		if entry.GetLogIndex().LSN > replayer.db.Wal.GetCheckpointed() {
			return
		}
		if err = entry.GetDB().RemoveEntry(entry); err != nil {
			panic(err)
		}
		err = catalog.ErrStopCurrRecur
		return
	}
	processor.SegmentFn = func(entry *catalog.SegmentEntry) (err error) {
		if entry.IsActive() {
			if !entry.GetTable().IsVirtual() {
				activeSegs[entry.ID] = entry
			}
			return
		}
		if entry.GetLogIndex().LSN > replayer.db.Wal.GetCheckpointed() {
			if !entry.GetTable().IsVirtual() {
				activeSegs[entry.ID] = entry
			}
			return
		}
		if err = entry.GetTable().RemoveEntry(entry); err != nil {
			panic(err)
		}
		err = catalog.ErrStopCurrRecur
		return
	}
	processor.BlockFn = func(entry *catalog.BlockEntry) (err error) {
		if entry.IsActive() {
			return
		}
		if entry.GetLogIndex().LSN > replayer.db.Wal.GetCheckpointed() {
			return
		}
		if err = gcBlockClosure(entry, GCType_Block)(); err != nil {
			panic(err)
		}
		return
	}
	_ = replayer.db.Catalog.RecurLoop(processor)

	files := replayer.scanFiles()
	for id := range activeSegs {
		_, ok := files[id]
		if !ok {
			panic(fmt.Errorf("Cannot find segment file for: %d", id))
		}
		delete(files, id)
	}
	for _, file := range files {
		logutil.Info("[Replay]", common.OperationField("clean-segment"),
			common.OperandField(file))
		if err := os.Remove(file); err != nil {
			panic(err)
		}
	}

	logutil.Info(replayer.db.Catalog.SimplePPString(common.PPL1))
}

func (replayer *Replayer) OnStaleIndex(idx *wal.Index) {
	replayer.staleIndexes = append(replayer.staleIndexes, idx)
}

func (replayer *Replayer) OnReplayEntry(group uint32, commitId uint64, payload []byte, typ uint16, info any) {
	replayer.once.Do(replayer.PreReplayWal)
	if group != wal.GroupC {
		return
	}
	idxCtx := wal.NewIndex(commitId, 0, 0)
	r := bytes.NewBuffer(payload)
	txnCmd, _, err := txnbase.BuildCommandFrom(r)
	if err != nil {
		panic(err)
	}
	replayer.OnReplayCmd(txnCmd, idxCtx)
	if err != nil {
		panic(err)
	}
}

func (replayer *Replayer) GetMaxTS() uint64 {
	return replayer.maxTs
}

func (replayer *Replayer) OnTimeStamp(ts uint64) {
	if ts > replayer.maxTs {
		replayer.maxTs = ts
	}
}

func (replayer *Replayer) OnReplayCmd(txncmd txnif.TxnCmd, idxCtx *wal.Index) {
	if idxCtx != nil && idxCtx.Size > 0 {
		logutil.Info("", common.OperationField("replay-cmd"),
			common.OperandField(txncmd.Desc()),
			common.AnyField("index", idxCtx.String()))
	}
	var err error
	switch cmd := txncmd.(type) {
	case *txnbase.ComposedCmd:
		idxCtx.Size = cmd.CmdSize
		internalCnt := uint32(0)
		for i, command := range cmd.Cmds {
			_, ok := command.(*txnimpl.AppendCmd)
			if ok {
				internalCnt++
				replayer.OnReplayCmd(command, nil)
			} else {
				idx := idxCtx.Clone()
				idx.CSN = uint32(i) - internalCnt
				replayer.OnReplayCmd(command, idx)
			}
		}
	case *catalog.EntryCommand:
		replayer.db.Catalog.ReplayCmd(txncmd, replayer.DataFactory, idxCtx, replayer, replayer.cache)
	case *txnimpl.AppendCmd:
		replayer.db.onReplayAppendCmd(cmd, replayer)
	case *updates.UpdateCmd:
		err = replayer.db.onReplayUpdateCmd(cmd, idxCtx, replayer)
	}
	if err != nil {
		panic(err)
	}
}

func (db *DB) onReplayAppendCmd(cmd *txnimpl.AppendCmd, observer wal.ReplayObserver) {
	var data batch.IBatch
	var deletes *roaring.Bitmap
	for _, subTxnCmd := range cmd.Cmds {
		switch subCmd := subTxnCmd.(type) {
		case *txnbase.BatchCmd:
			data = subCmd.Bat
		case *txnbase.DeleteBitmapCmd:
			deletes = subCmd.Bitmap
		case *txnbase.PointerCmd:
			batEntry, err := db.Wal.LoadEntry(subCmd.Group, subCmd.Lsn)
			if err != nil {
				panic(err)
			}
			r := bytes.NewBuffer(batEntry.GetPayload())
			txnCmd, _, err := txnbase.BuildCommandFrom(r)
			if err != nil {
				panic(err)
			}
			data = txnCmd.(*txnbase.BatchCmd).Bat
		}
	}

	for _, info := range cmd.Infos {
		database, err := db.Catalog.GetDatabaseByID(info.GetDBID())
		if err != nil {
			panic(err)
		}
		id := info.GetDest()
		blk, err := database.GetBlockEntryByID(id)
		if err != nil {
			panic(err)
		}
		if !blk.IsActive() {
			continue
		}
		if observer != nil {
			observer.OnTimeStamp(blk.GetBlockData().GetMaxCheckpointTS())
		}
		if cmd.Ts <= blk.GetBlockData().GetMaxCheckpointTS() {
			continue
		}
		start := info.GetSrcOff()
		end := start + info.GetSrcLen() - 1
		bat, err := db.window(blk.GetSchema(), data, deletes, start, end)
		if err != nil {
			panic(err)
		}
		len := info.GetDestLen()
		// off := info.GetDestOff()
		datablk := blk.GetBlockData()
		appender, err := datablk.MakeAppender()
		if err != nil {
			panic(err)
		}
		_, _, err = appender.OnReplayInsertNode(bat, 0, len, nil)
		if err != nil {
			panic(err)
		}
	}
}

func (db *DB) window(schema *catalog.Schema, data batch.IBatch, deletes *roaring.Bitmap, start, end uint32) (*gbat.Batch, error) {
	ret := gbat.New(true, []string{})
	for _, attrId := range data.GetAttrs() {
		def := schema.ColDefs[attrId]
		if def.IsHidden() {
			continue
		}
		src, err := data.GetVectorByAttr(attrId)
		if err != nil {
			return nil, err
		}
		srcVec, err := src.Window(start, end+1).CopyToVector()
		if err != nil {
			return nil, err
		}
		deletes := common.BM32Window(deletes, int(start), int(end))
		srcVec = compute.ApplyDeleteToVector(srcVec, deletes)
		ret.Vecs = append(ret.Vecs, srcVec)
		ret.Attrs = append(ret.Attrs, def.Name)
	}
	return ret, nil
}

func (db *DB) onReplayUpdateCmd(cmd *updates.UpdateCmd, idxCtx *wal.Index, observer wal.ReplayObserver) (err error) {
	switch cmd.GetType() {
	case txnbase.CmdAppend:
		db.onReplayAppend(cmd, idxCtx, observer)
	case txnbase.CmdUpdate:
		db.onReplayUpdate(cmd, idxCtx, observer)
	case txnbase.CmdDelete:
		db.onReplayDelete(cmd, idxCtx, observer)
	}
	return
}

func (db *DB) onReplayDelete(cmd *updates.UpdateCmd, idxCtx *wal.Index, observer wal.ReplayObserver) {
	database, err := db.Catalog.GetDatabaseByID(cmd.GetDBID())
	if err != nil {
		panic(err)
	}
	deleteNode := cmd.GetDeleteNode()
	deleteNode.SetLogIndex(idxCtx)
	id := deleteNode.GetID()
	blk, err := database.GetBlockEntryByID(id)
	if err != nil {
		panic(err)
	}
	if !blk.IsActive() {
		observer.OnStaleIndex(idxCtx)
		return
	}
	if deleteNode.GetCommitTSLocked() <= blk.GetBlockData().GetMaxCheckpointTS() {
		observer.OnStaleIndex(idxCtx)
		return
	}
	datablk := blk.GetBlockData()
	err = datablk.OnReplayDelete(deleteNode)
	if err != nil {
		panic(err)
	}
	if observer != nil {
		observer.OnTimeStamp(deleteNode.GetCommitTSLocked())
	}
}

func (db *DB) onReplayAppend(cmd *updates.UpdateCmd, idxCtx *wal.Index, observer wal.ReplayObserver) {
	database, err := db.Catalog.GetDatabaseByID(cmd.GetDBID())
	if err != nil {
		panic(err)
	}
	appendNode := cmd.GetAppendNode()
	appendNode.SetLogIndex(idxCtx)
	id := appendNode.GetID()
	blk, err := database.GetBlockEntryByID(id)
	if err != nil {
		panic(err)
	}
	if !blk.IsActive() {
		observer.OnStaleIndex(idxCtx)
		return
	}
	if appendNode.GetCommitTS() <= blk.GetBlockData().GetMaxCheckpointTS() {
		observer.OnStaleIndex(idxCtx)
		return
	}
	datablk := blk.GetBlockData()

	appender, err := datablk.MakeAppender()
	if err != nil {
		panic(err)
	}
	appender.OnReplayAppendNode(cmd.GetAppendNode())
	if observer != nil {
		observer.OnTimeStamp(appendNode.GetCommitTS())
	}
}

func (db *DB) onReplayUpdate(cmd *updates.UpdateCmd, idxCtx *wal.Index, observer wal.ReplayObserver) {
	database, err := db.Catalog.GetDatabaseByID(cmd.GetDBID())
	if err != nil {
		panic(err)
	}
	updateNode := cmd.GetUpdateNode()
	updateNode.SetLogIndex(idxCtx)
	id := updateNode.GetID()
	blk, err := database.GetBlockEntryByID(id)
	if err != nil {
		panic(err)
	}
	if blk.CurrOp == catalog.OpSoftDelete {
		observer.OnStaleIndex(idxCtx)
		return
	}
	if updateNode.GetCommitTSLocked() <= blk.GetBlockData().GetMaxCheckpointTS() {
		observer.OnStaleIndex(idxCtx)
		return
	}
	blkdata := blk.GetBlockData()
	err = blkdata.OnReplayUpdate(id.Idx, updateNode)
	if err != nil {
		panic(err)
	}
	if observer != nil {
		observer.OnTimeStamp(updateNode.GetCommitTSLocked())
	}
}
