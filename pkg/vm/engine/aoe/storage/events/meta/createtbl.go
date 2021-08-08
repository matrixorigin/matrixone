package meta

import (
	"matrixone/pkg/vm/engine/aoe"
	"matrixone/pkg/vm/engine/aoe/storage/dbi"
	md "matrixone/pkg/vm/engine/aoe/storage/metadata/v1"
	"matrixone/pkg/vm/engine/aoe/storage/ops"
	"matrixone/pkg/vm/engine/aoe/storage/sched"
	// log "github.com/sirupsen/logrus"
)

type createTableEvent struct {
	baseEvent
	reqCtx    dbi.TableOpCtx
	tableInfo *aoe.TableInfo
}

func NewCreateTableEvent(ctx *Context, reqCtx dbi.TableOpCtx, tableInfo *aoe.TableInfo, doneCB ops.OpDoneCB) *createTableEvent {
	e := &createTableEvent{
		reqCtx:    reqCtx,
		tableInfo: tableInfo,
	}
	e.baseEvent = baseEvent{
		Ctx:       ctx,
		BaseEvent: *sched.NewBaseEvent(e, sched.MetaUpdateEvent, doneCB),
	}
	return e
}

func (e *createTableEvent) GetTable() *md.Table {
	tbl := e.Result.(*md.Table)
	return tbl
}

func (e *createTableEvent) Execute() error {
	tbl, err := e.Ctx.Opts.Meta.Info.CreateTableFromTableInfo(e.tableInfo, e.reqCtx)
	if err != nil {
		return err
	}
	var table *md.Table
	{
		e.Result = tbl
		ctx := md.CopyCtx{Ts: md.NowMicro() + 1, Attached: true}
		info := e.Ctx.Opts.Meta.Info.Copy(ctx)
		table, _ = info.ReferenceTable(tbl.ID)
		eCtx := &Context{Opts: e.Ctx.Opts}
		flushEvent := NewFlushInfoEvent(eCtx, info)
		e.Ctx.Opts.Scheduler.Schedule(flushEvent)
	}
	{
		eCtx := &Context{Opts: e.Ctx.Opts}
		flushEvent := NewFlushTableEvent(eCtx, table)
		e.Ctx.Opts.Scheduler.Schedule(flushEvent)
	}
	return err
}
