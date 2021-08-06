package memtable

import (
	"fmt"
	ro "matrixone/pkg/container/batch"
	engine "matrixone/pkg/vm/engine/aoe/storage"
	"matrixone/pkg/vm/engine/aoe/storage/common"
	"matrixone/pkg/vm/engine/aoe/storage/container/batch"
	"matrixone/pkg/vm/engine/aoe/storage/layout/table/v2/iface"
	imem "matrixone/pkg/vm/engine/aoe/storage/memtable/base"
	md "matrixone/pkg/vm/engine/aoe/storage/metadata/v1"
	cops "matrixone/pkg/vm/engine/aoe/storage/ops/coldata/v2"
	dops "matrixone/pkg/vm/engine/aoe/storage/ops/data"
	mops "matrixone/pkg/vm/engine/aoe/storage/ops/meta/v2"
	"sync"
	// log "github.com/sirupsen/logrus"
)

type MemTable struct {
	sync.RWMutex
	common.RefHelper
	Opts      *engine.Options
	TableData iface.ITableData
	Full      bool
	Batch     batch.IBatch
	Meta      *md.Block
	TableMeta *md.Table
	Block     iface.IBlock
}

var (
	_ imem.IMemTable = (*MemTable)(nil)
)

func NewMemTable(opts *engine.Options, tableData iface.ITableData, data iface.IBlock) imem.IMemTable {
	mt := &MemTable{
		Opts:      opts,
		TableData: tableData,
		Batch:     data.GetFullBatch(),
		Block:     data,
		Meta:      data.GetMeta(),
		TableMeta: tableData.GetMeta(),
	}

	for idx, colIdx := range mt.Batch.GetAttrs() {
		vec := mt.Batch.GetVectorByAttr(colIdx)
		vec.PlacementNew(mt.Meta.Segment.Table.Schema.ColDefs[idx].Type)
	}

	mt.OnZeroCB = mt.close
	mt.Ref()
	return mt
}

func (mt *MemTable) GetID() common.ID {
	return mt.Meta.AsCommonID().AsBlockID()
}

func (mt *MemTable) String() string {
	mt.RLock()
	defer mt.RUnlock()
	id := mt.GetID()
	bat := mt.Batch
	length := -1
	if bat != nil {
		length = bat.Length()
	}
	s := fmt.Sprintf("<MT[%s]>(Refs=%d)(Count=%d)", id.BlockString(), mt.RefCount(), length)
	return s
}

func (mt *MemTable) Append(bat *ro.Batch, offset uint64, index *md.LogIndex) (n uint64, err error) {
	mt.Lock()
	defer mt.Unlock()
	var na int
	for idx, attr := range mt.Batch.GetAttrs() {
		if na, err = mt.Batch.GetVectorByAttr(attr).AppendVector(bat.Vecs[idx], int(offset)); err != nil {
			return n, err
		}
	}
	n = uint64(na)
	index.Count = n
	mt.Meta.SetIndex(*index)
	// log.Infof("1. offset=%d, n=%d, cap=%d, index=%s, blkcnt=%d", offset, n, bat.Vecs[0].Length(), index.String(), mt.Meta.GetCount())
	mt.Meta.AddCount(n)
	mt.TableData.AddRows(n)
	// log.Infof("2. offset=%d, n=%d, cap=%d, index=%s, blkcnt=%d", offset, n, bat.Vecs[0].Length(), index.String(), mt.Meta.GetCount())
	if uint64(mt.Batch.Length()) == mt.Meta.MaxRowCount {
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
	writer := &MemtableWriter{
		Opts:     mt.Opts,
		Dirname:  mt.Meta.Segment.Table.Conf.Dir,
		Memtable: mt,
	}
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
		getssop := mops.NewGetTblSSOp(&ctx, mt.TableMeta)
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
		op := mops.NewFlushTblOp(&ctx, getssop.Result)
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
		if upgradeBlkOp.SegmentClosed {
			flushOp := dops.NewFlushSegOp(&dops.OpCtx{Opts: mt.Opts}, mt.TableData.StrongRefSegment(newMeta.Segment.ID))
			err = flushOp.Push()
			if err != nil {
				mt.Opts.EventListener.BackgroundErrorCB(err)
				return err
			}
			mt.TableData.Ref()
			go func(td iface.ITableData) {
				defer td.Unref()
				err = flushOp.WaitDone()
				if err != nil {
					mt.Opts.EventListener.BackgroundErrorCB(err)
				} else {
					ctx := cops.OpCtx{Opts: flushOp.Ctx.Opts}
					op := cops.NewUpgradeSegOp(&ctx, newMeta.Segment.ID, td)
					if err = op.Push(); err != nil {
						mt.Opts.EventListener.BackgroundErrorCB(err)
					}
					if err = op.WaitDone(); err != nil {
						mt.Opts.EventListener.BackgroundErrorCB(err)
					}
					op.Segment.Unref()
				}
			}(mt.TableData)
		}
		upgradeBlkOp.Block.Unref()
	}
	// }()
	mt.Opts.EventListener.FlushBlockEndCB(mt)
	return nil
}

func (mt *MemTable) GetMeta() *md.Block {
	return mt.Meta
}

func (mt *MemTable) Unpin() {
	if mt.Batch != nil {
		mt.Batch.Close()
		mt.Batch = nil
	}
}

func (mt *MemTable) close() {
	if mt.Batch != nil {
		mt.Batch.Close()
		mt.Batch = nil
	}
	if mt.Block != nil {
		mt.Block.Unref()
		mt.Block = nil
	}
	if mt.TableData != nil {
		mt.TableData.Unref()
		mt.TableData = nil
	}
}

func (mt *MemTable) IsFull() bool {
	return mt.Full
}
