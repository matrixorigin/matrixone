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

package sched

import (
	"matrixone/pkg/container/vector"
	"matrixone/pkg/logutil"
	"matrixone/pkg/vm/engine/aoe/storage/layout/dataio"
	"matrixone/pkg/vm/engine/aoe/storage/layout/table/v1/iface"
	"matrixone/pkg/vm/engine/aoe/storage/metadata/v1"
	mb "matrixone/pkg/vm/engine/aoe/storage/mutation/base"
	"matrixone/pkg/vm/engine/aoe/storage/sched"
	// "matrixone/pkg/logutil"
)

// flushMemblockEvent supports flushing not-full block.
type flushMemblockEvent struct {
	BaseEvent
	// Block data node to be flushed
	Block iface.IMutBlock
	// Metadata of this block
	Meta  *metadata.Block
}

func NewFlushMemBlockEvent(ctx *Context, blk iface.IMutBlock) *flushMemblockEvent {
	e := &flushMemblockEvent{
		Block: blk,
		Meta:  blk.GetMeta(),
	}
	e.BaseEvent = BaseEvent{
		Ctx:       ctx,
		BaseEvent: *sched.NewBaseEvent(e, sched.FlushBlkTask, ctx.DoneCB, ctx.Waitable),
	}
	return e
}

func (e *flushMemblockEvent) Execute() error {
	defer e.Block.Unref()
	return e.Block.WithPinedContext(func(mut mb.IMutableBlock) error {
		meta := mut.GetMeta()
		data := mut.GetData()
		var vecs []*vector.Vector
		for attri, _ := range data.GetAttrs() {
			v, err := data.GetVectorByAttr(attri)
			if err != nil {
				return err
			}
			view := v.GetLatestView()
			ro, err := view.CopyToVector()
			if err != nil {
				return err
			}
			vecs = append(vecs, ro)
		}

		bw := dataio.NewBlockWriter(vecs, meta, meta.Segment.Table.Conf.Dir)
		bw.SetPreExecutor(func() {
			logutil.Infof(" %s | Memtable | Flushing", bw.GetFileName())
		})
		bw.SetPostExecutor(func() {
			logutil.Infof(" %s | Memtable | Flushed", bw.GetFileName())
		})
		return bw.Execute()
	})
}
