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
	"github.com/matrixorigin/matrixone/pkg/container/batch"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/aoe/storage/common"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/aoe/storage/layout/dataio"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/aoe/storage/layout/table/v1/iface"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/aoe/storage/sched"
)

type flushSegEvent struct {
	BaseEvent
	// Segment to be flushed
	Segment iface.ISegment
}

func NewFlushSegEvent(ctx *Context, seg iface.ISegment) *flushSegEvent {
	e := &flushSegEvent{Segment: seg}
	e.BaseEvent = BaseEvent{
		Ctx:       ctx,
		BaseEvent: *sched.NewBaseEvent(e, sched.FlushSegTask, ctx.DoneCB, ctx.Waitable),
	}
	return e
}

func (e *flushSegEvent) Execute() error {
	ids := e.Segment.BlockIds()
	meta := e.Segment.GetMeta()
	batches := make([]*batch.Batch, len(ids))
	colCnt := len(meta.Table.Schema.ColDefs)
	nodes := make([]*common.MemNode, 0)
	for i := 0; i < len(ids); i++ {
		blk := e.Segment.WeakRefBlock(ids[i])
		bat := &batch.Batch{}
		for colIdx := 0; colIdx < colCnt; colIdx++ {
			vec, err := blk.GetVectorWrapper(colIdx)
			if err != nil {
				panic(err)
			}
			bat.Vecs = append(bat.Vecs, &vec.Vector)
			if vec.Vector.Length() == 0 {
				panic("")
			}
			nodes = append(nodes, vec.MNode)
		}
		batches[i] = bat
	}

	release := func() {
		for _, node := range nodes {
			common.GPool.Free(node)
		}
	}
	defer release()

	w := dataio.NewSegmentWriter(batches, meta, meta.Table.Catalog.Cfg.Dir)
	return w.Execute()
}
