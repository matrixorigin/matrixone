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

package model

import (
	"github.com/matrixorigin/matrixone/pkg/common/moerr"
	v2 "github.com/matrixorigin/matrixone/pkg/util/metric/v2"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/common"
	"sync"
	"time"
)

type PageT[T common.IRef] interface {
	common.IRef
	Pin() *common.PinnedItem[T]
	TTL() uint8 // 0 skip, 1 clear memory, 2 clear disk
	ID() *common.ID
	Length() int
	Clear(clearDisk bool)
}

type TransferTable[T PageT[T]] struct {
	sync.RWMutex
	pages        map[common.ID]*common.PinnedItem[T]
	deletedPages []*common.PinnedItem[T]
}

func NewTransferTable[T PageT[T]]() *TransferTable[T] {
	table := &TransferTable[T]{
		pages: make(map[common.ID]*common.PinnedItem[T]),
	}
	return table
}

func (table *TransferTable[T]) Pin(id common.ID) (pinned *common.PinnedItem[T], err error) {
	table.RLock()
	defer table.RUnlock()
	var found bool
	if pinned, found = table.pages[id]; !found {
		err = moerr.GetOkExpectedEOB()
	} else {
		pinned = pinned.Item().Pin()
	}
	return
}

func (table *TransferTable[T]) Len() int {
	table.RLock()
	defer table.RUnlock()
	return len(table.pages)
}

func (table *TransferTable[T]) prepareTTL() (mem, disk []*common.PinnedItem[T]) {
	table.RLock()
	for _, page := range table.pages {
		opt := page.Item().TTL()
		if opt == clearMemory {
			mem = append(mem, page)
		} else if opt == clearDisk {
			disk = append(disk, page)
		}
	}
	disk = append(disk, table.deletedPages...)
	table.RUnlock()
	table.Lock()
	defer table.Unlock()
	table.deletedPages = make([]*common.PinnedItem[T], 0)
	return
}

func (table *TransferTable[T]) executeTTL(mem, disk []*common.PinnedItem[T]) {
	for _, page := range mem {
		page.Val.Clear(false)
	}

	table.Lock()
	for _, pinned := range disk {
		delete(table.pages, *pinned.Item().ID())
	}
	table.Unlock()
	for _, pinned := range disk {
		pinned.Val.Clear(true)
		pinned.Close()
	}
}

func (table *TransferTable[T]) RunTTL() {
	now := time.Now()
	mem, disk := table.prepareTTL()
	table.executeTTL(mem, disk)
	v2.TransferTableRunTTLDurationHistogram.Observe(time.Since(now).Seconds())
}

func (table *TransferTable[T]) AddPage(page T) (dup bool) {
	pinned := page.Pin()
	defer func() {
		if dup {
			pinned.Close()
		}
	}()
	table.Lock()
	defer table.Unlock()
	id := *page.ID()
	if _, found := table.pages[id]; found {
		dup = true
		return
	}
	table.pages[id] = pinned

	v2.TaskMergeTransferPageSizeGauge.Add(float64(page.Length()))
	return
}

func (table *TransferTable[T]) DeletePage(id *common.ID) (deleted bool) {
	table.Lock()
	defer table.Unlock()
	if _, deleted = table.pages[*id]; !deleted {
		return
	}
	table.deletedPages = append(table.deletedPages, table.pages[*id])
	delete(table.pages, *id)

	return
}

func (table *TransferTable[T]) Close() {
	table.Lock()
	defer table.Unlock()
	for _, item := range table.pages {
		item.Close()
	}
	table.pages = make(map[common.ID]*common.PinnedItem[T])
}
