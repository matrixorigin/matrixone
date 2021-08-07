package meta

import (
	"errors"
	"fmt"
	md "matrixone/pkg/vm/engine/aoe/storage/metadata/v1"
	"matrixone/pkg/vm/engine/aoe/storage/sched"

	log "github.com/sirupsen/logrus"
)

type commitBlkEvent struct {
	baseEvent
	NewMeta   *md.Block
	LocalMeta *md.Block
}

func NewCommitBlkEvent(ctx *Context, localMeta *md.Block, doneCB func()) *commitBlkEvent {
	e := &commitBlkEvent{LocalMeta: localMeta}
	e.baseEvent = baseEvent{
		Ctx:       ctx,
		BaseEvent: *sched.NewBaseEvent(e, sched.MetaUpdateEvent, doneCB),
	}
	return e
}

func (e *commitBlkEvent) updateBlock(blk *md.Block) error {
	if blk.BoundSate != md.Detatched {
		log.Errorf("")
		return errors.New(fmt.Sprintf("Block %d BoundSate should be %d, but %d", blk.ID, md.Detatched, blk.BoundSate))
	}

	table, err := e.Ctx.Opts.Meta.Info.ReferenceTable(blk.Segment.Table.ID)
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

	e.NewMeta = rblk

	return nil
}

func (e *commitBlkEvent) Execute() error {
	if e.LocalMeta != nil {
		return e.updateBlock(e.LocalMeta)
	}
	return nil
}
