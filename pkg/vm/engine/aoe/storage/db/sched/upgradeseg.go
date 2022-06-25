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
)

type upgradeSegEvent struct {
	BaseEvent
	// Table data of the upgraded segment
	TableData iface.ITableData
	// Data of the upgraded segment
	Segment iface.ISegment
	// Data of the old segment
	OldSegment iface.ISegment
}

func NewUpgradeSegEvent(ctx *Context, old iface.ISegment, td iface.ITableData) *upgradeSegEvent {
	e := &upgradeSegEvent{
		OldSegment: old,
		TableData:  td,
	}
	e.BaseEvent = *NewBaseEvent(e, UpgradeSegTask, ctx)
	return e
}

func (e *upgradeSegEvent) Execute() error {
	var err error
	sid := e.OldSegment.GetMeta().Id
	e.Segment, err = e.TableData.UpgradeSegment(sid)
	if err == nil {
		if e.Ctx.Controller.IsOn(UpgradeSegMetaMask) {
			newSize := e.Segment.GetSegmentFile().Stat().Size()
			if err = e.Segment.GetMeta().SimpleUpgrade(newSize, nil); err != nil {
				panic(err)
			}
		}
	}
	return err
}
