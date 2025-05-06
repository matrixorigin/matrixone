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

package gc

import (
	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/objectio"
	"sync"
)

type ObjectEntry struct {
	stats    *objectio.ObjectStats
	createTS types.TS
	dropTS   types.TS
	db       uint64
	table    uint64
}

var objectEntryPool = sync.Pool{
	New: func() interface{} {
		return &ObjectEntry{}
	},
}

func NewObjectEntry() *ObjectEntry {
	entry, ok := objectEntryPool.Get().(*ObjectEntry)
	if !ok {
		// Defensive programming: create a new instance when the pool is polluted
		return &ObjectEntry{}
	}
	return entry
}

func (e *ObjectEntry) Release() {
	e.stats = nil
	e.createTS = types.TS{}
	e.dropTS = types.TS{}
	e.db = 0
	e.table = 0
	objectEntryPool.Put(e)
}
