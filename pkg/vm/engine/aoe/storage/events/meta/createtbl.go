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
	"matrixone/pkg/vm/engine/aoe"
	dbsched "matrixone/pkg/vm/engine/aoe/storage/db/sched"
	"matrixone/pkg/vm/engine/aoe/storage/dbi"
	md "matrixone/pkg/vm/engine/aoe/storage/metadata/v1"
	"matrixone/pkg/vm/engine/aoe/storage/sched"
	// log "github.com/sirupsen/logrus"
)

type createTableEvent struct {
	dbsched.BaseEvent

	// reqCtx is Op context, record the raft log index and table name
	reqCtx dbi.TableOpCtx

	// tableInfo is Table's metadata info
	tableInfo *aoe.TableInfo
}

// NewCreateTableEvent creates a logical table event,
// records table's metadata to a .tckp file,
// records table's info to a .ckp file
func NewCreateTableEvent(ctx *dbsched.Context, reqCtx dbi.TableOpCtx, tableInfo *aoe.TableInfo) *createTableEvent {
	e := &createTableEvent{
		reqCtx:    reqCtx,
		tableInfo: tableInfo,
	}
	e.BaseEvent = dbsched.BaseEvent{
		Ctx:       ctx,
		BaseEvent: *sched.NewBaseEvent(e, sched.MetaCreateTableTask, ctx.DoneCB, ctx.Waitable),
	}
	return e
}

// Return the table just created
func (e *createTableEvent) GetTable() *md.Table {
	tbl := e.Result.(*md.Table)
	return tbl
}

// 1. Create and register a table in MetaInfo
// 2. Modify MetaInfo's metadata file(.ckp)
// 3. Create table's metadata file(.tckp)
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
		eCtx := &dbsched.Context{Opts: e.Ctx.Opts, Waitable: true}
		flushEvent := NewFlushInfoEvent(eCtx, info)
		e.Ctx.Opts.Scheduler.Schedule(flushEvent)
		if err = flushEvent.WaitDone(); err != nil {
			// TODO: Drop table
			return err
		}
	}
	{
		eCtx := &dbsched.Context{Opts: e.Ctx.Opts, Waitable: true}
		flushEvent := NewFlushTableEvent(eCtx, table)
		e.Ctx.Opts.Scheduler.Schedule(flushEvent)
		if err = flushEvent.WaitDone(); err != nil {
			// TODO: Drop table
			return err
		}
	}
	return err
}
