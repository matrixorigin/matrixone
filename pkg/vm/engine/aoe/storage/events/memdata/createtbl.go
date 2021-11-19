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
	imem "github.com/matrixorigin/matrixone/pkg/vm/engine/aoe/storage/memtable/v1/base"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/aoe/storage/sched"
)

type createTableEvent struct {
	BaseEvent

	// Collection manages the memTable data of a table(creates Block and
	// creates memTable) and provides Append() interface externally
	Collection imem.MutableTable
}

func NewCreateTableEvent(ctx *Context) *createTableEvent {
	e := &createTableEvent{}
	e.BaseEvent = BaseEvent{
		BaseEvent: *sched.NewBaseEvent(e, sched.MemdataUpdateEvent, ctx.DoneCB, ctx.Waitable),
		Ctx:       ctx,
	}
	return e
}

// 1. Create a Collection
// 2. Create and register a TableData
// 3. Register Collection to the memTable manager
func (e *createTableEvent) Execute() error {
	collection := e.Ctx.MTMgr.StrongRefTable(e.Ctx.TableMeta.Id)
	if collection != nil {
		e.Collection = collection
		return nil
	}
	meta := e.Ctx.TableMeta

	// FIXME: table is dropped by another thread
	tableData, err := e.Ctx.Tables.StrongRefTable(meta.Id)
	if err != nil {
		tableData, err = e.Ctx.Tables.RegisterTable(meta)
		if err != nil {
			return err
		}
		tableData.Ref()
	}
	collection, err = e.Ctx.MTMgr.RegisterTable(tableData)
	if err != nil {
		tableData.Unref()
		return err
	}

	e.Collection = collection

	return nil
}
