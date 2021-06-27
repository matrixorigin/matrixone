package memtable

import (
	"context"
	"matrixone/pkg/vm/engine/aoe/storage"
	buf "matrixone/pkg/vm/engine/aoe/storage/buffer"
	"matrixone/pkg/vm/engine/aoe/storage/common"
	dio "matrixone/pkg/vm/engine/aoe/storage/dataio"
	"matrixone/pkg/vm/engine/aoe/storage/layout/table/v2/iface"
	imem "matrixone/pkg/vm/engine/aoe/storage/memtable/base"
	md "matrixone/pkg/vm/engine/aoe/storage/metadata"
	"matrixone/pkg/vm/engine/aoe/storage/mock/type/chunk"
	"matrixone/pkg/vm/engine/aoe/storage/mock/type/vector"
	cops "matrixone/pkg/vm/engine/aoe/storage/ops/coldatav2"
	mops "matrixone/pkg/vm/engine/aoe/storage/ops/meta/v2"
	"sync"

	log "github.com/sirupsen/logrus"
)

type MemTable struct {
	Opts *engine.Options
	sync.RWMutex
	TableData iface.ITableData
	Data      *chunk.Chunk
	Full      bool
	Handle    iface.IBlockHandle
	Meta      *md.Block
	Block     iface.IBlock
}

var (
	_ imem.IMemTable = (*MemTable)(nil)
)

func NewMemTable(opts *engine.Options, tableData iface.ITableData, data iface.IBlock) imem.IMemTable {
	mt := &MemTable{
		Opts:      opts,
		TableData: tableData,
		Handle:    data.GetBlockHandle(),
		Block:     data,
		Meta:      data.GetMeta(),
	}

	var vectors []vector.Vector
	for idx := 0; idx < mt.Handle.Cols(); idx++ {
		vec := vector.NewStdVector(mt.Handle.ColType(idx), mt.Handle.GetPageNode(idx, 0).DataNode.(*buf.RawMemoryNode).Data)
		vectors = append(vectors, vec)
	}

	mt.Data = &chunk.Chunk{
		Vectors: vectors,
	}

	return mt
}

func (mt *MemTable) GetID() common.ID {
	return mt.Meta.AsCommonID().AsBlockID()
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

func (mt *MemTable) Unpin() {
	if mt.Handle != nil {
		mt.Handle.Close()
		mt.Handle = nil
	}
}

func (mt *MemTable) Close() error {
	if mt.Handle != nil {
		mt.Handle.Close()
		mt.Handle = nil
	}
	mt.Block.Unref()
	return nil
}

func (mt *MemTable) IsFull() bool {
	return mt.Full
}
