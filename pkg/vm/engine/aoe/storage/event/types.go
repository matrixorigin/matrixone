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

package event

import (
	logutil2 "matrixone/pkg/logutil"
	imem "matrixone/pkg/vm/engine/aoe/storage/memtable/base"
	md "matrixone/pkg/vm/engine/aoe/storage/metadata/v1"
)

type EventListener struct {
	BackgroundErrorCB func(error)
	MemTableFullCB    func(imem.IMemTable)
	FlushBlockBeginCB func(imem.IMemTable)
	FlushBlockEndCB   func(imem.IMemTable)
	CheckpointStartCB func(*md.MetaInfo)
	CheckpointEndCB   func(*md.MetaInfo)
}

func (l *EventListener) FillDefaults() {
	if l.BackgroundErrorCB == nil {
		l.BackgroundErrorCB = func(err error) {
			logutil2.Errorf("BackgroundError %v", err)
		}
	}

	if l.MemTableFullCB == nil {
		l.MemTableFullCB = func(table imem.IMemTable) {}
	}

	if l.FlushBlockBeginCB == nil {
		l.FlushBlockBeginCB = func(table imem.IMemTable) {}
	}

	if l.FlushBlockEndCB == nil {
		l.FlushBlockEndCB = func(table imem.IMemTable) {}
	}

	if l.CheckpointStartCB == nil {
		l.CheckpointStartCB = func(info *md.MetaInfo) {}
	}

	if l.CheckpointEndCB == nil {
		l.CheckpointEndCB = func(info *md.MetaInfo) {}
	}
}
