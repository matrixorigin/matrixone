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

package sched

import (
	logutil2 "matrixone/pkg/logutil"
	imem "matrixone/pkg/vm/engine/aoe/storage/memtable/base"
	md "matrixone/pkg/vm/engine/aoe/storage/metadata/v1"
	"matrixone/pkg/vm/engine/aoe/storage/sched"
)

type flushMemtableEvent struct {
	BaseEvent
	Meta       *md.Block
	Collection imem.ICollection
}

func NewFlushMemtableEvent(ctx *Context, collection imem.ICollection) *flushMemtableEvent {
	e := &flushMemtableEvent{
		Collection: collection,
	}
	e.BaseEvent = BaseEvent{
		Ctx:       ctx,
		BaseEvent: *sched.NewBaseEvent(e, sched.FlushMemtableTask, ctx.DoneCB, ctx.Waitable),
	}
	return e
}

func (e *flushMemtableEvent) Execute() error {
	defer e.Collection.Unref()
	mem := e.Collection.FetchImmuTable()
	if mem == nil {
		return nil
	}
	defer mem.Unref()
	e.Meta = mem.GetMeta()
	err := mem.Flush()
	if err != nil {
		logutil2.Errorf("Flush memtable %d failed %s", e.Meta.ID, err)
		return err
	}
	return err
}
