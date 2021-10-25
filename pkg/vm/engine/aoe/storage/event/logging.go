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
	"github.com/matrixorigin/matrixone/pkg/logutil"
	imem "github.com/matrixorigin/matrixone/pkg/vm/engine/aoe/storage/memtable/v1/base"
)

func NewLoggingEventListener() EventListener {
	return EventListener{
		BackgroundErrorCB: func(err error) {
			logutil.Errorf("BackgroundError %s", err)
		},

		MemTableFullCB: func(table imem.IMemTable) {
			logutil.Debugf("MemTable %d is full", table.GetMeta().Id)
		},

		FlushBlockBeginCB: func(table imem.IMemTable) {
			logutil.Debugf("MemTable %d begins to flush", table.GetMeta().Id)
		},

		FlushBlockEndCB: func(table imem.IMemTable) {
			logutil.Debugf("MemTable %d end flush", table.GetMeta().Id)
		},
	}
}
