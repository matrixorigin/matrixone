package db

import (
	"bytes"

	"github.com/RoaringBitmap/roaring"
	gbat "github.com/matrixorigin/matrixone/pkg/container/batch"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/catalog"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/common"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/container/batch"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/container/compute"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/iface/txnif"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/tables"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/tables/updates"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/txn/txnbase"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/txn/txnimpl"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/wal"
)

type Replayer struct {
	DataFactory *tables.DataFactory
	db          *DB
	maxTs       uint64
}

func newReplayer(dataFactory *tables.DataFactory, db *DB) *Replayer {
	return &Replayer{
		DataFactory: dataFactory,
		db:          db,
	}
}

func (replayer *Replayer) Replay() {
	err := replayer.db.Wal.Replay(replayer.OnReplayEntry)
	if err != nil {
		panic(err)
	}
}

func (replayer *Replayer) OnReplayEntry(group uint32, commitId uint64, payload []byte, typ uint16, info interface{}) {
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
	return
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
		replayer.db.Catalog.ReplayCmd(txncmd, replayer.DataFactory, idxCtx, replayer)
	case *txnimpl.AppendCmd:
		replayer.db.onReplayAppendCmd(cmd)
	case *updates.UpdateCmd:
		err = replayer.db.onReplayUpdateCmd(cmd)
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
		attrs := make([]string, len(tb.GetSchema().ColDefs))
		for i := range attrs {
			attrs[i] = tb.GetSchema().ColDefs[i].Name
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
			continue
		}
		start := info.GetSrcOff()
		end := start + info.GetSrcLen() - 1
		bat, err := db.window(attrs, data, deletes, start, end)
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
	return
}

func (db *DB) window(attrs []string, data batch.IBatch, deletes *roaring.Bitmap, start, end uint32) (*gbat.Batch, error) {
	ret := gbat.New(true, attrs)
	for i, attr := range data.GetAttrs() {
		src, err := data.GetVectorByAttr(attr)
		if err != nil {
			return nil, err
		}
		srcVec, err := src.Window(start, end+1).CopyToVector()
		if err != nil {
			return nil, err
		}
		deletes := common.BitMapWindow(deletes, int(start), int(end))
		srcVec = compute.ApplyDeleteToVector(srcVec, deletes)
		ret.Vecs[i] = srcVec
	}
	return ret, nil
}

func (db *DB) onReplayUpdateCmd(cmd *updates.UpdateCmd) (err error) {
	switch cmd.GetType() {
	case txnbase.CmdAppend:
		db.onReplayAppend(cmd)
	case txnbase.CmdUpdate:
		db.onReplayUpdate(cmd)
	case txnbase.CmdDelete:
		db.onReplayDelete(cmd)
	}
	return
}

func (db *DB) onReplayDelete(cmd *updates.UpdateCmd) {
	database, err := db.Catalog.GetDatabaseByID(cmd.GetDBID())
	if err != nil {
		panic(err)
	}
	deleteNode := cmd.GetDeleteNode()
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
		return
	}
	datablk := blk.GetBlockData()
	iterator := deleteNode.GetDeleteMaskLocked().Iterator()
	for iterator.HasNext() {
		row := iterator.Next()
		err = datablk.OnReplayDelete(row, row)
		if err != nil {
			panic(err)
		}
	}
}

func (db *DB) onReplayAppend(cmd *updates.UpdateCmd) {
	database, err := db.Catalog.GetDatabaseByID(cmd.GetDBID())
	if err != nil {
		panic(err)
	}
	appendNode := cmd.GetAppendNode()
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
		return
	}
	datablk := blk.GetBlockData()

	appender, err := datablk.MakeAppender()
	if err != nil {
		panic(err)
	}
	appender.OnReplayAppendNode(cmd.GetAppendNode().GetMaxRow())
}

func (db *DB) onReplayUpdate(cmd *updates.UpdateCmd) {
	database, err := db.Catalog.GetDatabaseByID(cmd.GetDBID())
	if err != nil {
		panic(err)
	}
	updateNode := cmd.GetUpdateNode()
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
		return
	}
	blkdata := blk.GetBlockData()
	iterator := updateNode.GetMask().Iterator()
	vals := updateNode.GetValues()
	for iterator.HasNext() {
		row := iterator.Next()
		err = blkdata.OnReplayUpdate(row, id.Idx, vals[row])
		if err != nil {
			panic(err)
		}
	}
}
