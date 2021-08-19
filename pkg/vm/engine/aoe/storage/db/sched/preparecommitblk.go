package sched

import (
	"matrixone/pkg/vm/engine/aoe/storage/memtable/v2/base"
	"matrixone/pkg/vm/engine/aoe/storage/sched"
)

type prepareCommitBlockEvent struct {
	BaseEvent
	mut base.IMemTable
}

func NewPrepareCommitBlockEvent(ctx *Context, mut base.IMemTable) *prepareCommitBlockEvent {
	e := &prepareCommitBlockEvent{
		mut: mut,
	}
	e.BaseEvent = BaseEvent{
		Ctx:       ctx,
		BaseEvent: *sched.NewBaseEvent(e, sched.PrepareCommitBlockTask, ctx.DoneCB, ctx.Waitable),
	}
	return e
}

func (e *prepareCommitBlockEvent) Execute() error {
	// TODO
	return nil
}
