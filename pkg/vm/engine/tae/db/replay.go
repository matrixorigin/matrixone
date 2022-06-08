package db

import (
	"bytes"

	"github.com/RoaringBitmap/roaring"
	gbat "github.com/matrixorigin/matrixone/pkg/container/batch"
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
}

func newReplayer(dataFactory *tables.DataFactory, db *DB) *Replayer {
	return &Replayer{
		DataFactory:  dataFactory,
		db:           db,
		cache:        bytes.NewBuffer(make([]byte, DefaultReplayCacheSize)),
		staleIndexes: make([]*wal.Index, 0),
	}
}

func (replayer *Replayer) Replay() {
	err := replayer.db.Wal.Replay(replayer.OnReplayEntry)
	if err != nil {
		panic(err)
	}
	_, err = replayer.db.Wal.Checkpoint(replayer.staleIndexes)
	if err != nil {
		panic(err)
	}
}

func (replayer *Replayer) OnStaleIndex(idx *wal.Index) {
	replayer.staleIndexes = append(replayer.staleIndexes, idx)
}

func (replayer *Replayer) OnReplayEntry(group uint32, commitId uint64, payload []byte, typ uint16, info any) {
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
		replayer.db.onReplayAppendCmd(cmd)
	case *updates.UpdateCmd:
		err = replayer.db.onReplayUpdateCmd(cmd, idxCtx, replayer)
	}
	if err != nil {
		panic(err)
	}
}

func (db *DB) onReplayAppendCmd(cmd *txnimpl.AppendCmd) {
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
		tb, err := database.GetTableEntryByID(id.TableID)
		if err != nil {
			panic(err)
		}
		// attrs := make([]string, len(tb.GetSchema().ColDefs))
		// for i := range attrs {
		// 	attrs[i] = tb.GetSchema().ColDefs[i].Name
		// }
		seg, err := tb.GetSegmentByID(id.SegmentID)
		if err != nil {
			panic(err)
		}
		blk, err := seg.GetBlockEntryByID(id.BlockID)
		if err != nil {
			panic(err)
		}
		if blk.CurrOp == catalog.OpSoftDelete {
			continue
		}
		fileTs, err := blk.GetFileTs()
		if err != nil {
			panic(err)
		}
		if fileTs > cmd.Ts {
			continue
		}
		start := info.GetSrcOff()
		end := start + info.GetSrcLen() - 1
		bat, err := db.window(tb.GetSchema(), data, deletes, start, end)
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
	tb, err := database.GetTableEntryByID(id.TableID)
	if err != nil {
		panic(err)
	}
	seg, err := tb.GetSegmentByID(id.SegmentID)
	if err != nil {
		panic(err)
	}
	blk, err := seg.GetBlockEntryByID(id.BlockID)
	if err != nil {
		panic(err)
	}
	if blk.CurrOp == catalog.OpSoftDelete {
		observer.OnStaleIndex(idxCtx)
		return
	}
	fileTs, err := blk.GetFileTs()
	if err != nil {
		panic(err)
	}
	if fileTs > deleteNode.GetCommitTSLocked() {
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
	tb, err := database.GetTableEntryByID(id.TableID)
	if err != nil {
		panic(err)
	}
	seg, err := tb.GetSegmentByID(id.SegmentID)
	if err != nil {
		panic(err)
	}
	blk, err := seg.GetBlockEntryByID(id.BlockID)
	if err != nil {
		panic(err)
	}
	if blk.CurrOp == catalog.OpSoftDelete {
		observer.OnStaleIndex(idxCtx)
		return
	}
	fileTs, err := blk.GetFileTs()
	if err != nil {
		panic(err)
	}
	if fileTs > appendNode.GetCommitTS() {
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
	tb, err := database.GetTableEntryByID(id.TableID)
	if err != nil {
		panic(err)
	}
	seg, err := tb.GetSegmentByID(id.SegmentID)
	if err != nil {
		panic(err)
	}
	blk, err := seg.GetBlockEntryByID(id.BlockID)
	if err != nil {
		panic(err)
	}
	if blk.CurrOp == catalog.OpSoftDelete {
		observer.OnStaleIndex(idxCtx)
		return
	}
	fileTs, err := blk.GetFileTs()
	if err != nil {
		panic(err)
	}
	if fileTs > updateNode.GetCommitTSLocked() {
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
