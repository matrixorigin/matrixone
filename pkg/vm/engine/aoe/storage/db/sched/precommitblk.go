package db

import (
	"matrixone/pkg/vm/engine/aoe/storage/common"
	"matrixone/pkg/vm/engine/aoe/storage/sched"
	// log "github.com/sirupsen/logrus"
)

type precommitBlockEvent struct {
	baseEvent
	Id common.ID
}

func NewPrecommitBlockEvent(ctx *Context, id common.ID) *precommitBlockEvent {
	e := &precommitBlockEvent{
		Id: id,
	}
	e.baseEvent = baseEvent{
		Ctx:       ctx,
		BaseEvent: *sched.NewBaseEvent(e, sched.PrecommitBlkMetaTask, ctx.DoneCB, ctx.Waitable),
	}
	return e
}

func (e *precommitBlockEvent) Execute() error {
	return nil
}
