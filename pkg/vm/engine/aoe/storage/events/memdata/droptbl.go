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

package memdata

import (
	"matrixone/pkg/vm/engine/aoe/storage/layout/table/v1/iface"
	"matrixone/pkg/vm/engine/aoe/storage/sched"
)

type dropTableEvent struct {
	BaseEvent
	TableId uint64
	Data    iface.ITableData
}

func NewDropTableEvent(ctx *Context, tableId uint64) *dropTableEvent {
	e := &dropTableEvent{TableId: tableId}
	e.BaseEvent = BaseEvent{
		BaseEvent: *sched.NewBaseEvent(e, sched.MemdataUpdateEvent, ctx.DoneCB, ctx.Waitable),
		Ctx:       ctx,
	}
	return e
}

func (e *dropTableEvent) Execute() error {
	tbl, err := e.Ctx.Tables.DropTable(e.TableId)
	e.Data = tbl
	return err
}
