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
	"matrixone/pkg/vm/engine/aoe/storage/metadata/v1"
	"matrixone/pkg/vm/engine/aoe/storage/sched"
)

type createSegBlkEvent struct {
	BaseEvent

	// TableData is Table's metadata in memory
	TableData iface.ITableData

	// BlkMeta is the metadata of the Block, which is
	// created and registered during NewCreateBlkEvent
	BlkMeta *metadata.Block

	// Block is an instance registered to segment
	Block iface.IBlock
}

func NewCreateSegBlkEvent(ctx *Context, meta *metadata.Block, tableData iface.ITableData) *createSegBlkEvent {
	e := &createSegBlkEvent{TableData: tableData, BlkMeta: meta}
	e.BaseEvent = BaseEvent{
		Ctx:       ctx,
		BaseEvent: *sched.NewBaseEvent(e, sched.MemdataUpdateEvent, ctx.DoneCB, ctx.Waitable),
	}
	return e
}

// 1. Create and register a segment in TableData
// 2. Create and register a Block in TableData
func (e *createSegBlkEvent) Execute() error {
	var err error
	seg := e.TableData.StrongRefSegment(e.BlkMeta.Segment.Id)
	if seg == nil {
		seg, err = e.TableData.RegisterSegment(e.BlkMeta.Segment)
		if err != nil {
			panic("should not happend")
		}
	}
	defer seg.Unref()
	blk, err := e.TableData.RegisterBlock(e.BlkMeta)
	if err != nil {
		panic(err)
	}
	e.Block = blk

	return nil
}
