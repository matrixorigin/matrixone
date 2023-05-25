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
	"sync"
	"time"

	"github.com/matrixorigin/matrixone/pkg/common/moerr"
	"github.com/matrixorigin/matrixone/pkg/logutil"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/common"
)

type PageT[T common.IRef] interface {
	common.IRef
	Pin() *common.PinnedItem[T]
	TTL(time.Time, time.Duration) bool
	ID() *common.ID
}

type TransferTable[T PageT[T]] struct {
	sync.RWMutex
	ttl   time.Duration
	pages map[common.ID]*common.PinnedItem[T]
}

func NewTransferTable[T PageT[T]](ttl time.Duration) *TransferTable[T] {
	return &TransferTable[T]{
		ttl:   ttl,
		pages: make(map[common.ID]*common.PinnedItem[T]),
	}
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
func (table *TransferTable[T]) prepareTTL(now time.Time) (items []*common.PinnedItem[T]) {
	table.RLock()
	defer table.RUnlock()
	logutil.Debugf("processing transfer.go gavin 10 %d", len(table.pages))
	for _, page := range table.pages {
		if page.Item().TTL(now, table.ttl) {
			items = append(items, page)
		}
	}
	logutil.Debugf("processing transfer.go gavin 11")
	return
}

func (table *TransferTable[T]) executeTTL(items []*common.PinnedItem[T]) {
	if len(items) == 0 {
		return
	}
	logutil.Debugf("processing transfer.go gavin 4")

	table.Lock()
	logutil.Debugf("processing transfer.go gavin 5")

	for _, pinned := range items {
		delete(table.pages, *pinned.Item().ID())
	}
	logutil.Debugf("processing transfer.go gavin 6")

	table.Unlock()
	logutil.Debugf("processing transfer.go gavin 7")

	for _, pinned := range items {
		pinned.Close()
	}
	logutil.Debugf("processing transfer.go gavin 8")
}

func (table *TransferTable[T]) RunTTL(now time.Time) {
	logutil.Debugf("processing transfer.go gavin")
	items := table.prepareTTL(now)
	logutil.Debugf("processing transfer.go gavin 2 %d", len(items))
	table.executeTTL(items)
	logutil.Debugf("processing transfer.go gavin 3")

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
	return
}

func (table *TransferTable[T]) DeletePage(id *common.ID) (deleted bool) {
	table.Lock()
	defer table.Unlock()
	if _, deleted = table.pages[*id]; !deleted {
		return
	}
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
