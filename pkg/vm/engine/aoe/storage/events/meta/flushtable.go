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
