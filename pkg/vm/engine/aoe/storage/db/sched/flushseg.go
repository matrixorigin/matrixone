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
	"github.com/matrixorigin/matrixone/pkg/vm/engine/aoe/storage/layout/dataio"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/aoe/storage/layout/table/v1"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/aoe/storage/layout/table/v1/iface"
)

type flushSegEvent struct {
	BaseEvent
	// Segment to be flushed
	Segment iface.ISegment
}

func NewFlushSegEvent(ctx *Context, seg iface.ISegment) *flushSegEvent {
	e := &flushSegEvent{Segment: seg}
	e.BaseEvent = *NewBaseEvent(e, FlushSegTask, ctx)
	return e
}

func (e *flushSegEvent) Execute() error {
	ids := e.Segment.BlockIds()
	meta := e.Segment.GetMeta()
	blks := make([]iface.IBlock, 0)
	for _, id := range ids {
		blk := e.Segment.WeakRefBlock(id)
		blks = append(blks, blk)
	}
	iter := table.NewBacktrackingBlockIterator(blks, 0)
	w := dataio.NewSegmentWriter(iter, meta, meta.Table.Database.Catalog.Cfg.Dir)
	return w.Execute()
}
