package meta

import (
	dbsched "matrixone/pkg/vm/engine/aoe/storage/db/sched"
	md "matrixone/pkg/vm/engine/aoe/storage/metadata/v1"
	"matrixone/pkg/vm/engine/aoe/storage/sched"
	// log "github.com/sirupsen/logrus"
)

type flushTableEvent struct {
	dbsched.BaseEvent
	Table *md.Table
}

func NewFlushTableEvent(ctx *dbsched.Context, tbl *md.Table) *flushTableEvent {
	e := new(flushTableEvent)
	e.Table = tbl
	e.BaseEvent = dbsched.BaseEvent{
		Ctx:       ctx,
		BaseEvent: *sched.NewBaseEvent(e, sched.StatelessEvent, ctx.DoneCB, ctx.Waitable),
	}
	return e
}

func (e *flushTableEvent) Execute() (err error) {
	ck := e.Ctx.Opts.Meta.CKFactory.Create()
	err = ck.PreCommit(e.Table)
	if err != nil {
		return err
	}
	err = ck.Commit(e.Table)
	if err != nil {
		return err
	}
	_, err = e.Ctx.Opts.Meta.Info.ReferenceTable(e.Table.ID)
	if err != nil {
		panic(err)
	}

	return err
}
