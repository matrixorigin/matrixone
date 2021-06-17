package memtable

import (
	"context"
	"matrixone/pkg/container/types"
	"matrixone/pkg/vm/engine/aoe/storage"
	buf "matrixone/pkg/vm/engine/aoe/storage/buffer"
	"matrixone/pkg/vm/engine/aoe/storage/common"
	dio "matrixone/pkg/vm/engine/aoe/storage/dataio"
	"matrixone/pkg/vm/engine/aoe/storage/layout/table"
	"matrixone/pkg/vm/engine/aoe/storage/layout/table/col"
	imem "matrixone/pkg/vm/engine/aoe/storage/memtable/base"
	md "matrixone/pkg/vm/engine/aoe/storage/metadata"
	"matrixone/pkg/vm/engine/aoe/storage/mock/type/chunk"
	"matrixone/pkg/vm/engine/aoe/storage/mock/type/vector"
	cops "matrixone/pkg/vm/engine/aoe/storage/ops/coldata"
	mops "matrixone/pkg/vm/engine/aoe/storage/ops/meta"
	"sync"

	log "github.com/sirupsen/logrus"
)

type MemTable struct {
	Opts *engine.Options
	sync.RWMutex
	TableData table.ITableData
	Meta      *md.Block
	Data      *chunk.Chunk
	Full      bool
	Columns   []col.IColumnBlock
	Cursors   []col.IScanCursor
	ID        common.ID
	Types     []types.Type
}

var (
	_ imem.IMemTable = (*MemTable)(nil)
)

func NewMemTable(tableData table.ITableData, colTypes []types.Type, columnBlocks []col.IColumnBlock,
	cursors []col.IScanCursor, opts *engine.Options, meta *md.Block) imem.IMemTable {
	mt := &MemTable{
		Meta:      meta,
		Full:      false,
		Opts:      opts,
		Columns:   columnBlocks,
		Cursors:   cursors,
		ID:        columnBlocks[0].GetID(),
		Types:     colTypes,
		TableData: tableData,
	}
	var vectors []vector.Vector
	for idx, blk := range columnBlocks {
		vec := vector.NewStdVector(colTypes[idx], blk.GetPartRoot().GetDataNode().(*buf.RawMemoryNode).Data)
		vectors = append(vectors, vec)
	}

	mt.Data = &chunk.Chunk{
		Vectors: vectors,
	}

	return mt
}

func (mt *MemTable) GetID() common.ID {
	return mt.ID
}

func (mt *MemTable) InitScanCursors(cursors []interface{}) error {
	for idx, colBlock := range mt.Columns {
		cursor := cursors[idx].(*col.ScanCursor)
		err := colBlock.InitScanCursor(cursor)
		if err != nil {
			return err
		}
	}
	return nil
}

func (mt *MemTable) Append(c *chunk.Chunk, offset uint64, index *md.LogIndex) (n uint64, err error) {
	mt.Lock()
	defer mt.Unlock()
	n, err = mt.Data.Append(c, offset)
	if err != nil {
		return n, err
	}
	index.Count = n
	mt.Meta.SetIndex(*index)
	mt.Meta.AddCount(n)
	log.Infof("offset=%d, writecnt=%d, cap=%d, index=%s, blkcnt=%d", offset, n, c.GetCount(), index.String(), mt.Meta.GetCount())
	if mt.Data.GetCount() == mt.Meta.MaxRowCount {
		mt.Full = true
		mt.Meta.DataState = md.FULL
	}
	return n, err
}

// A flush worker call this Flush API. When a MemTable is ready to flush. It immutable.
// Steps:
// 1. Serialize mt.Data to block_file (dir/$table_id_$segment_id_$block_id.blk)
// 2. Create a UpdateBlockOp and excute it
// 3. Start a checkpoint job
// If crashed before Step 1, all data from last checkpoint will be restored from WAL
// If crashed before Step 2, the untracked block file will be cleanup at startup.
// If crashed before Step 3, same as above.
func (mt *MemTable) Flush() error {
	mt.Opts.EventListener.FlushBlockBeginCB(mt)
	wCtx := context.TODO()
	wCtx = context.WithValue(wCtx, "memtable", mt)
	writer := dio.WRITER_FACTORY.MakeWriter(MEMTABLE_WRITER, wCtx)
	err := writer.Flush()
	if err != nil {
		mt.Opts.EventListener.BackgroundErrorCB(err)
		return err
	}
	ctx := mops.OpCtx{Block: mt.Meta, Opts: mt.Opts}
	op := mops.NewUpdateOp(&ctx)
	op.Push()
	err = op.WaitDone()
	if err != nil {
		mt.Opts.EventListener.BackgroundErrorCB(err)
		return err
	}
	newMeta := op.NewMeta
	// go func() {
	{
		ctx := mops.OpCtx{Opts: mt.Opts}
		getssop := mops.NewGetSSOp(&ctx)
		err := getssop.Push()
		if err != nil {
			mt.Opts.EventListener.BackgroundErrorCB(err)
			return err
		}
		err = getssop.WaitDone()
		if err != nil {
			mt.Opts.EventListener.BackgroundErrorCB(err)
			return err
		}
		op := mops.NewCheckpointOp(&ctx, getssop.SS)
		err = op.Push()
		if err != nil {
			mt.Opts.EventListener.BackgroundErrorCB(err)
			return err
		}
		err = op.WaitDone()
		if err != nil {
			mt.Opts.EventListener.BackgroundErrorCB(err)
			return err
		}
	}
	// }()
	// go func() {
	{
		colCtx := cops.OpCtx{Opts: mt.Opts, BlkMeta: newMeta}
		upgradeBlkOp := cops.NewUpgradeBlkOp(&colCtx, mt.TableData)
		err := upgradeBlkOp.Push()
		if err != nil {
			mt.Opts.EventListener.BackgroundErrorCB(err)
			return err
		}
		err = upgradeBlkOp.WaitDone()
		if err != nil {
			mt.Opts.EventListener.BackgroundErrorCB(err)
			return err
		}
	}
	// }()
	mt.Opts.EventListener.FlushBlockEndCB(mt)
	return nil
}

func (mt *MemTable) GetMeta() *md.Block {
	return mt.Meta
}

func (mt *MemTable) Close() error {
	for _, colBlk := range mt.Columns {
		colBlk.UnRef()
	}
	return nil
}

func (mt *MemTable) IsFull() bool {
	return mt.Full
}
