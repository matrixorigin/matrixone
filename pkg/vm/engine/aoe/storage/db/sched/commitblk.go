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
	"errors"
	"fmt"
	logutil2 "matrixone/pkg/logutil"
	md "matrixone/pkg/vm/engine/aoe/storage/metadata/v1"
	"matrixone/pkg/vm/engine/aoe/storage/sched"
)

type commitBlkEvent struct {
	BaseEvent
	NewMeta   *md.Block
	LocalMeta *md.Block
}

func NewCommitBlkEvent(ctx *Context, localMeta *md.Block) *commitBlkEvent {
	e := &commitBlkEvent{LocalMeta: localMeta}
	e.BaseEvent = BaseEvent{
		Ctx:       ctx,
		BaseEvent: *sched.NewBaseEvent(e, sched.CommitBlkTask, ctx.DoneCB, ctx.Waitable),
	}
	return e
}

func (e *commitBlkEvent) updateBlock(blk *md.Block) error {
	if blk.BoundSate != md.Detatched {
		logutil2.Error("")
		return errors.New(fmt.Sprintf("Block %d BoundSate should be %d, but %d", blk.ID, md.Detatched, blk.BoundSate))
	}

	table, err := e.Ctx.Opts.Meta.Info.ReferenceTable(blk.Segment.Table.ID)
	if err != nil {
		return err
	}

	seg, err := table.ReferenceSegment(blk.Segment.ID)
	if err != nil {
		return err
	}
	rblk, err := seg.ReferenceBlock(blk.ID)
	if err != nil {
		return err
	}
	tmpBlk := blk.Copy()
	tmpBlk.Attach()
	err = rblk.Update(tmpBlk)
	if err != nil {
		return err
	}

	if rblk.IsFull() {
		seg.TryClose()
	}

	e.NewMeta = rblk

	return nil
}

func (e *commitBlkEvent) Execute() error {
	if e.LocalMeta != nil {
		return e.updateBlock(e.LocalMeta)
	}
	return nil
}
