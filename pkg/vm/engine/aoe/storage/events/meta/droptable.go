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
	"github.com/matrixorigin/matrixone/pkg/logutil"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/aoe/storage/adaptor"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/aoe/storage/db/gcreqs"
	dbsched "github.com/matrixorigin/matrixone/pkg/vm/engine/aoe/storage/db/sched"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/aoe/storage/dbi"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/aoe/storage/layout/table/v1"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/aoe/storage/layout/table/v1/muthandle/base"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/aoe/storage/sched"
)

type dropTableEvent struct {
	dbsched.BaseEvent

	// reqCtx is Op context, record the raft log index and table name
	reqCtx dbi.DropTableCtx

	// Table's id, aoe is generated when the table is created
	Id    uint64
	MTMgr base.IManager

	// The created table will be inserted into the Tables
	Tables *table.Tables
}

func NewDropTableEvent(ctx *dbsched.Context, reqCtx dbi.DropTableCtx, mtMgr base.IManager, tables *table.Tables) *dropTableEvent {
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

// 1. Modify MetaInfo and mark the table as deleted
// 2. Modify the metadata file
// 3. Modify the metadata info in the memeory and release resources
func (e *dropTableEvent) Execute() error {
	index := adaptor.GetLogIndexFromDropTableCtx(&e.reqCtx)
	logutil.Infof("DropTable %s", index.String())
	if err := e.Ctx.Opts.Wal.SyncLog(index); err != nil {
		return err
	}
	defer e.Ctx.Opts.Wal.Checkpoint(index)
	tbl, err := e.Ctx.Opts.Meta.Catalog.SimpleGetTableByName(e.reqCtx.DBName,
		e.reqCtx.TableName)
	if err != nil {
		return err
	}
	e.Id = tbl.Id
	err = tbl.SimpleSoftDelete(index)
	gcReq := gcreqs.NewDropTblRequest(e.Ctx.Opts, tbl, e.Tables, e.MTMgr, e.reqCtx.OnFinishCB)
	e.Ctx.Opts.GC.Acceptor.Accept(gcReq)
	return nil
}
