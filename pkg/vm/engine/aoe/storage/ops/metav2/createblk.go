package meta

import (
	log "github.com/sirupsen/logrus"
	"matrixone/pkg/vm/engine/aoe/storage/layout/table2/iface"
	md "matrixone/pkg/vm/engine/aoe/storage/metadata"
	mmop "matrixone/pkg/vm/engine/aoe/storage/ops/memdatav2"
)

func NewCreateBlkOp(ctx *OpCtx, tid uint64, tableData iface.ITableData) *CreateBlkOp {
	op := &CreateBlkOp{
		TableData: tableData,
		TableID:   tid,
	}
	op.Op = *NewOp(op, ctx, ctx.Opts.Meta.Updater)
	return op
}

type CreateBlkOp struct {
	Op
	NewSegment bool
	TableID    uint64
	TableData  iface.ITableData
	Block      iface.IBlock
}

func (op *CreateBlkOp) HasNewSegment() bool {
	return op.NewSegment
}

func (op *CreateBlkOp) GetBlock() *md.Block {
	if op.Err != nil {
		return nil
	}
	return op.Result.(*md.Block)
}

func (op *CreateBlkOp) Execute() error {
	table, err := op.Ctx.Opts.Meta.Info.ReferenceTable(op.TableID)
	if err != nil {
		return err
	}

	seg := table.GetActiveSegment()
	if seg == nil {
		seg = table.NextActiveSegment()
	}
	if seg == nil {
		seg, err = table.CreateSegment()
		if err != nil {
			return err
		}
		err = table.RegisterSegment(seg)
		if err != nil {
			return err
		}
		op.NewSegment = true
	}

	var cloned *md.Block
	blk := seg.GetActiveBlk()
	if blk == nil {
		blk = seg.NextActiveBlk()
	} else {
		seg.NextActiveBlk()
	}
	if blk == nil {
		blk, err = seg.CreateBlock()
		if err != nil {
			return err
		}
		err = seg.RegisterBlock(blk)
		if err != nil {
			return err
		}
		ctx := md.CopyCtx{}
		cloned, err = seg.CloneBlock(blk.ID, ctx)
		if err != nil {
			return err
		}
	} else {
		cloned = blk.Copy()
		cloned.Detach()
	}
	op.Result = cloned
	if op.TableData != nil {
		// op.registerTableData(cloned)
		ctx := new(mmop.OpCtx)
		ctx.Opts = op.Ctx.Opts
		segBlkOp := mmop.NewCreateSegBlkOp(ctx, op.NewSegment, cloned, op.TableData)
		segBlkOp.Push()
		err = segBlkOp.WaitDone()
		if err != nil {
			return err
		}
		op.Block = segBlkOp.Block
	}
	return err
}

func (op *CreateBlkOp) registerTableData(blk *md.Block) {
	if op.NewSegment {
		seg, err := op.TableData.RegisterSegment(blk.Segment)
		if err != nil {
			log.Error(err)
			panic("should not happend")
		}
		seg.Unref()
	}
	blkData, err := op.TableData.RegisterBlock(blk)
	if err != nil {
		log.Error(err)
		panic("should not happend")
	}
	blkData.Unref()
}
