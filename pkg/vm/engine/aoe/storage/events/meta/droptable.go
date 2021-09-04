package meta

import (
	"matrixone/pkg/vm/engine/aoe/storage/db/gcreqs"
	dbsched "matrixone/pkg/vm/engine/aoe/storage/db/sched"
	"matrixone/pkg/vm/engine/aoe/storage/dbi"
	"matrixone/pkg/vm/engine/aoe/storage/layout/table/v1"
	mtif "matrixone/pkg/vm/engine/aoe/storage/memtable/base"
	md "matrixone/pkg/vm/engine/aoe/storage/metadata/v1"
	"matrixone/pkg/vm/engine/aoe/storage/sched"
	// log "github.com/sirupsen/logrus"
)

type dropTableEvent struct {
	dbsched.BaseEvent
	reqCtx dbi.DropTableCtx
	Id     uint64
	MTMgr  mtif.IManager
	Tables *table.Tables
}

func NewDropTableEvent(ctx *dbsched.Context, reqCtx dbi.DropTableCtx, mtMgr mtif.IManager, tables *table.Tables) *dropTableEvent {
	e := &dropTableEvent{
		reqCtx: reqCtx,
		Tables: tables,
		MTMgr:  mtMgr,
	}
	e.BaseEvent = dbsched.BaseEvent{
		Ctx:       ctx,
		BaseEvent: *sched.NewBaseEvent(e, sched.MetaDropTableTask, ctx.DoneCB, ctx.Waitable),
	}
	return e
}

func (e *dropTableEvent) Execute() error {
	id, err := e.Ctx.Opts.Meta.Info.SoftDeleteTable(e.reqCtx.TableName, e.reqCtx.OpIndex)
	if err != nil {
		return err
	}
	e.Id = id
	ctx := md.CopyCtx{Ts: md.NowMicro() + 1, Attached: true}
	info := e.Ctx.Opts.Meta.Info.Copy(ctx)
	eCtx := &dbsched.Context{Opts: e.Ctx.Opts}
	flushEvent := NewFlushInfoEvent(eCtx, info)
	e.Ctx.Opts.Scheduler.Schedule(flushEvent)
	gcReq := gcreqs.NewDropTblRequest(e.Ctx.Opts, id, e.Tables, e.MTMgr, e.reqCtx.OnFinishCB)
	e.Ctx.Opts.GC.Acceptor.Accept(gcReq)
	return err
}
