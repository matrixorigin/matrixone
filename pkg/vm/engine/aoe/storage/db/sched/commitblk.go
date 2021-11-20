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
	"github.com/matrixorigin/matrixone/pkg/vm/engine/aoe/storage/metadata/v1"
)

type commitBlkEvent struct {
	BaseEvent
	Meta *metadata.Block
}

func NewCommitBlkEvent(ctx *Context, meta *metadata.Block) *commitBlkEvent {
	e := &commitBlkEvent{Meta: meta}
	e.BaseEvent = *NewBaseEvent(e, CommitBlkTask, ctx)
	return e
}

func (e *commitBlkEvent) Execute() error {
	if e.Meta != nil {
		if err := e.Meta.SimpleUpgrade(nil); err != nil {
			return err
		}
		wal := e.Meta.Segment.Table.Database.Catalog.IndexWal
		if wal != nil {
			snip := e.Meta.ConsumeSnippet(true)
			// logutil.Infof("commit snip: %s", snip.String())
			wal.Checkpoint(snip)
		}
	}
	return nil
}
