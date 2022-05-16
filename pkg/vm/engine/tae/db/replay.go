package db

import (
	"bytes"

	"github.com/RoaringBitmap/roaring"
	gbat "github.com/matrixorigin/matrixone/pkg/container/batch"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/catalog"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/common"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/container/batch"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/container/compute"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/tables/updates"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/txn/txnbase"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/txn/txnimpl"
)

func (db *DB) ReplayDDL() {
	db.Wal.Replay(db.replayHandle)
}

func (db *DB) replayHandle(group uint32, commitId uint64, payload []byte, typ uint16, info interface{}) (err error) {
	r := bytes.NewBuffer(payload)
	txnCmd, _, err := txnbase.BuildCommandFrom(r)
	if err != nil {
		return err
	}
	switch cmd := txnCmd.(type) {
	case *catalog.EntryCommand:
		err = db.Catalog.ReplayCmd(txnCmd)
	case *txnimpl.AppendCmd:
		err = db.onReplayAppendCmd(cmd)
	case *updates.UpdateCmd:
		err = db.onReplayUpdateCmd(cmd)
	}
	return
}

func (db *DB) onReplayAppendCmd(cmd *txnimpl.AppendCmd) (err error) {
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
				return err
			}
			r := bytes.NewBuffer(batEntry.GetPayload())
			txnCmd, _, err := txnbase.BuildCommandFrom(r)
			if err != nil {
				return err
			}
			data = txnCmd.(*txnbase.BatchCmd).Bat
		}
	}

	for _, info := range cmd.Infos {
		database, err := db.Catalog.GetDatabaseByID(info.GetDBID())
		if err != nil {
			return err
		}
		id := info.GetDest()
		tb, err := database.GetTableEntryByID(id.TableID)
		if err != nil {
			return err
		}
		attrs := make([]string, len(tb.GetSchema().ColDefs))
		for i := range attrs {
			attrs[i] = tb.GetSchema().ColDefs[i].Name
		}
		seg, err := tb.GetSegmentByID(id.SegmentID)
		if err != nil {
			return err
		}
		blk, err := seg.GetBlockEntryByID(id.BlockID)
		if err != nil {
			return err
		}
		start := info.GetSrcOff()
		end := start + info.GetSrcLen() - 1
		bat, err := db.window(attrs, data, deletes, start, end)
		if err != nil {
			return err
		}
		len := info.GetDestLen()
		// off := info.GetDestOff()
		datablk := blk.GetBlockData()
		appender, err := datablk.MakeAppender()
		if err != nil {
			return err
		}
		_, _, err = appender.OnReplayInsertNode(bat, 0, len, nil)
		if err != nil {
			return err
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
		srcVec, _ := src.Window(start, end+1).CopyToVector()
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

func (db *DB) onReplayDelete(cmd *updates.UpdateCmd) (err error){
	database, err := db.Catalog.GetDatabaseByID(cmd.GetDBID())
	if err != nil {
		return err
	}
	deleteNode := cmd.GetDeleteNode()
	id := deleteNode.GetID()
	tb, err := database.GetTableEntryByID(id.TableID)
	if err != nil {
		return err
	}
	seg, err := tb.GetSegmentByID(id.SegmentID)
	if err != nil {
		return err
	}
	blk, err := seg.GetBlockEntryByID(id.BlockID)
	if err != nil {
		return err
	}
	datablk := blk.GetBlockData()
	iterator:=deleteNode.GetDeleteMaskLocked().Iterator()
	for iterator.HasNext(){
		row:=iterator.Next()
		datablk.OnReplayDelete(row,row)
	}
	return
}

func (db *DB) onReplayAppend(cmd *updates.UpdateCmd) (err error) {
	database, err := db.Catalog.GetDatabaseByID(cmd.GetDBID())
	if err != nil {
		return err
	}
	appendNode := cmd.GetAppendNode()
	id := appendNode.GetID()
	tb, err := database.GetTableEntryByID(id.TableID)
	if err != nil {
		return err
	}
	seg, err := tb.GetSegmentByID(id.SegmentID)
	if err != nil {
		return err
	}
	blk, err := seg.GetBlockEntryByID(id.BlockID)
	if err != nil {
		return err
	}
	datablk := blk.GetBlockData()
	appender, err := datablk.MakeAppender()
	if err != nil {
		return err
	}
	appender.OnReplayAppendNode(cmd.GetAppendNode().GetMaxRow())
	return
}
func (db *DB) onReplayUpdate(cmd *updates.UpdateCmd) (err error) {
	database, err := db.Catalog.GetDatabaseByID(cmd.GetDBID())
	if err != nil {
		return err
	}
	updateNode := cmd.GetUpdateNode()
	id := updateNode.GetID()
	tb, err := database.GetTableEntryByID(id.TableID)
	if err != nil {
		return err
	}
	seg, err := tb.GetSegmentByID(id.SegmentID)
	if err != nil {
		return err
	}
	blk, err := seg.GetBlockEntryByID(id.BlockID)
	if err != nil {
		return err
	}
	blkdata := blk.GetBlockData()
	iterator := updateNode.GetMask().Iterator()
	vals := updateNode.GetValues()
	for iterator.HasNext() {
		row := iterator.Next()
		err = blkdata.OnReplayUpdate(row, id.Idx, vals[row])
		if err != nil {
			return
		}
	}
	return
}
