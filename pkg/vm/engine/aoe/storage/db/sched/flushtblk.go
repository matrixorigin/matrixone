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
	"matrixone/pkg/vm/engine/aoe/storage/container/batch"
	"matrixone/pkg/vm/engine/aoe/storage/layout/dataio"
	"matrixone/pkg/vm/engine/aoe/storage/metadata/v1"
	"matrixone/pkg/vm/engine/aoe/storage/mutation/buffer/base"
	"matrixone/pkg/vm/engine/aoe/storage/sched"
	// "matrixone/pkg/logutil"
)

type flushTransientBlockEvent struct {
	BaseEvent
	Node base.INode
	Data batch.IBatch
	Meta *metadata.Block
	File *dataio.TransientBlockFile
}

func NewFlushTransientBlockEvent(ctx *Context, n base.INode, data batch.IBatch, meta *metadata.Block, file *dataio.TransientBlockFile) *flushTransientBlockEvent {
	e := &flushTransientBlockEvent{
		Node: n,
		File: file,
		Data: data,
		Meta: meta,
	}
	e.BaseEvent = BaseEvent{
		Ctx:       ctx,
		BaseEvent: *sched.NewBaseEvent(e, sched.FlushTBlkTask, ctx.DoneCB, ctx.Waitable),
	}
	return e
}

func (e *flushTransientBlockEvent) Execute() error {
	return e.File.Sync(e.Data, e.Meta)
}
