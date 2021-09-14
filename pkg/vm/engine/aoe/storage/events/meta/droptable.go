// Copyright 2021 Matrix Origin
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//      http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package meta

import (
	"matrixone/pkg/vm/engine/aoe/storage/db/gcreqs"
	dbsched "matrixone/pkg/vm/engine/aoe/storage/db/sched"
	"matrixone/pkg/vm/engine/aoe/storage/dbi"
	"matrixone/pkg/vm/engine/aoe/storage/layout/table/v1"
	mtif "matrixone/pkg/vm/engine/aoe/storage/memtable/v1/base"
	md "matrixone/pkg/vm/engine/aoe/storage/metadata/v1"
	"matrixone/pkg/vm/engine/aoe/storage/sched"
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
