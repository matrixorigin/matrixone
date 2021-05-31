package meta

import (
	"errors"
	"fmt"
	log "github.com/sirupsen/logrus"
	md "matrixone/pkg/vm/engine/aoe/storage/metadata"
)

func NewUpdateOp(ctx *OpCtx) *UpdateOp {
	op := &UpdateOp{}
	op.Op = *NewOp(op, ctx, ctx.Opts.Meta.Updater)
	return op
}

type UpdateOp struct {
	Op
}

func (op *UpdateOp) updateBlock(blk *md.Block) error {
	if blk.BoundSate != md.Detatched {
		log.Errorf("")
		return errors.New(fmt.Sprintf("Block %d BoundSate should be %d", blk.ID, md.Detatched))
	}

	table, err := op.Ctx.Opts.Meta.Info.ReferenceTable(blk.TableID)
	if err != nil {
		return err
	}

	seg, err := table.ReferenceSegment(blk.SegmentID)
	if err != nil {
		return err
	}
	rblk, err := seg.ReferenceBlock(blk.ID)
	if err != nil {
		return err
	}
	err = rblk.Update(blk)
	if err != nil {
		return err
	}

	if rblk.IsFull() {
		seg.TryClose()
	}

	return nil
}

func (op *UpdateOp) Execute() error {
	if op.Ctx.Block != nil {
		return op.updateBlock(op.Ctx.Block)
	}
	return nil
}
