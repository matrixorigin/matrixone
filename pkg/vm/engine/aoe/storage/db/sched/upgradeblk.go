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
	"github.com/matrixorigin/matrixone/pkg/vm/engine/aoe/storage/layout/table/v1/iface"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/aoe/storage/metadata/v1"
)

type upgradeBlkEvent struct {
	BaseEvent
	// Table data of the upgraded block
	TableData iface.ITableData
	// Metadata of the upgraded block
	Meta *metadata.Block
	// Data of the upgraded block
	Data iface.IBlock
	// Is the segment that block belongs to already closed or not
	SegmentClosed bool
}

func NewUpgradeBlkEvent(ctx *Context, meta *metadata.Block, td iface.ITableData) *upgradeBlkEvent {
	e := &upgradeBlkEvent{
		TableData: td,
		Meta:      meta,
	}
	e.BaseEvent = *NewBaseEvent(e, UpgradeBlkTask, ctx)
	return e
}

func (e *upgradeBlkEvent) Execute() error {
	var err error
	e.Data, err = e.TableData.UpgradeBlock(e.Meta)
	if err != nil {
		return err
	}
	if e.Data.WeakRefSegment().CanUpgrade() {
		e.SegmentClosed = true
	}
	return nil
}
