package meta

import (
	md "matrixone/pkg/vm/engine/aoe/storage/metadata/v1"
	"matrixone/pkg/vm/engine/aoe/storage/sched"
	// log "github.com/sirupsen/logrus"
)

type flushInfoEvent struct {
	baseEvent
	Info *md.MetaInfo
}

func NewFlushInfoEvent(ctx *Context, info *md.MetaInfo) *flushInfoEvent {
	e := new(flushInfoEvent)
	e.Info = info
	e.baseEvent = baseEvent{
		Ctx:       ctx,
		BaseEvent: *sched.NewBaseEvent(e, sched.StatelessEvent, nil),
	}
	return e
}

func (e *flushInfoEvent) Execute() (err error) {
	ck := e.Ctx.Opts.Meta.CKFactory.Create()
	err = ck.PreCommit(e.Info)
	if err != nil {
		return err
	}
	err = ck.Commit(e.Info)
	if err != nil {
		return err
	}
	e.Ctx.Opts.Meta.Info.UpdateCheckpointTime(e.Info.CkpTime)

	return err
}
