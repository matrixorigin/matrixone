package meta

import (
	log "github.com/sirupsen/logrus"
	"matrixone/pkg/vm/engine/aoe/storage/layout/table"
	"matrixone/pkg/vm/engine/aoe/storage/layout/table/col"
	md "matrixone/pkg/vm/engine/aoe/storage/metadata"
	mmop "matrixone/pkg/vm/engine/aoe/storage/ops/memdata"
)

func NewCreateBlkOp(ctx *OpCtx, tid uint64, tableData table.ITableData) *CreateBlkOp {
	op := &CreateBlkOp{
		TableData: tableData,
		ColBlocks: make([]col.IColumnBlock, 0),
		TableID:   tid,
	}
	op.Op = *NewOp(op, ctx, ctx.Opts.Meta.Updater)
	return op
}

type CreateBlkOp struct {
	Op
	NewSegment bool
	TableData  table.ITableData
	ColBlocks  []col.IColumnBlock
	TableID    uint64
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
		op.ColBlocks = segBlkOp.ColBlocks
	}
	return err
}

func (op *CreateBlkOp) registerTableData(blk *md.Block) {
	for _, column := range op.TableData.GetCollumns() {
		if op.NewSegment {
			seg, err := column.RegisterSegment(blk.Segment)
			if err != nil {
				log.Error(err)
				panic("should not happend")
			}
			seg.UnRef()
		}
		colBlk, err := column.RegisterBlock(blk)
		if err != nil {
			panic("should not happend")
		}
		op.ColBlocks = append(op.ColBlocks, colBlk)
	}
}
