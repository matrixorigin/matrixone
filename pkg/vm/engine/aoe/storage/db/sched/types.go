package sched

import (
	iops "github.com/matrixorigin/matrixone/pkg/vm/engine/aoe/storage/ops/base"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/aoe/storage/sched"
)

type BaseEvent struct {
	sched.BaseEvent
	Ctx *Context
}

func NewBaseEvent(impl iops.IOpInternal, t sched.EventType, ctx *Context) *BaseEvent {
	return &BaseEvent{
		Ctx:       ctx,
		BaseEvent: *sched.NewBaseEvent(impl, t, ctx.DoneCB, ctx.Waitable),
	}
}
