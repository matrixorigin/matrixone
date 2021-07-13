package meta

import (
	"errors"
	"fmt"
	log "github.com/sirupsen/logrus"
	md "matrixone/pkg/vm/engine/aoe/storage/metadata/v1"
)

func NewUpdateOp(ctx *OpCtx) *UpdateOp {
	op := &UpdateOp{}
	op.Op = *NewOp(op, ctx, ctx.Opts.Meta.Updater)
	return op
}

type UpdateOp struct {
	Op
	NewMeta *md.Block
}

func (op *UpdateOp) updateBlock(blk *md.Block) error {
	if blk.BoundSate != md.Detatched {
		log.Errorf("")
		return errors.New(fmt.Sprintf("Block %d BoundSate should be %d, but %d", blk.ID, md.Detatched, blk.BoundSate))
	}

	table, err := op.Ctx.Opts.Meta.Info.ReferenceTable(blk.Segment.TableID)
	if err != nil {
		return err
	}

	seg, err := table.ReferenceSegment(blk.Segment.ID)
	if err != nil {
		return err
	}
	rblk, err := seg.ReferenceBlock(blk.ID)
	if err != nil {
		return err
	}
	tmpBlk := blk.Copy()
	tmpBlk.Attach()
	err = rblk.Update(tmpBlk)
	if err != nil {
		return err
	}

	if rblk.IsFull() {
		seg.TryClose()
	}

	op.NewMeta = rblk

	return nil
}

func (op *UpdateOp) Execute() error {
	if op.Ctx.Block != nil {
		return op.updateBlock(op.Ctx.Block)
	}
	return nil
}
