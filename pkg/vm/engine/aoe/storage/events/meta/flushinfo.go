package meta

import (
	dbsched "matrixone/pkg/vm/engine/aoe/storage/db/sched"
	md "matrixone/pkg/vm/engine/aoe/storage/metadata/v1"
	"matrixone/pkg/vm/engine/aoe/storage/sched"
	// log "github.com/sirupsen/logrus"
)

type flushInfoEvent struct {
	dbsched.BaseEvent
	Info *md.MetaInfo
}

func NewFlushInfoEvent(ctx *dbsched.Context, info *md.MetaInfo) *flushInfoEvent {
	e := new(flushInfoEvent)
	e.Info = info
	e.BaseEvent = dbsched.BaseEvent{
		Ctx:       ctx,
		BaseEvent: *sched.NewBaseEvent(e, sched.StatelessEvent, ctx.DoneCB, ctx.Waitable),
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
