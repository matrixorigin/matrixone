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
	v2 "github.com/matrixorigin/matrixone/pkg/util/metric/v2"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/common"
)

type PageT[T common.IRef] interface {
	common.IRef
	Pin() *common.PinnedItem[T]
	TTL(time.Time, time.Duration) bool
	ID() *common.ID
	Length() int
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
	for _, page := range table.pages {
		if page.Item().TTL(now, table.ttl) {
			items = append(items, page)
		}
	}
	return
}

func (table *TransferTable[T]) executeTTL(items []*common.PinnedItem[T]) {
	if len(items) == 0 {
		return
	}

	cnt := 0

	table.Lock()
	for _, pinned := range items {
		cnt += pinned.Val.Length()
		delete(table.pages, *pinned.Item().ID())
	}
	table.Unlock()
	for _, pinned := range items {
		pinned.Close()
	}

	v2.TaskMergeTransferPageLengthGauge.Sub(float64(cnt))
}

func (table *TransferTable[T]) RunTTL(now time.Time) {
	items := table.prepareTTL(now)
	table.executeTTL(items)
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

	v2.TaskMergeTransferPageLengthGauge.Add(float64(page.Length()))
	return
}

func (table *TransferTable[T]) DeletePage(id *common.ID) {
	table.Lock()
	defer table.Unlock()
	if _, ok := table.pages[*id]; !ok {
		return
	}
	cnt := table.pages[*id].Val.Length()
	delete(table.pages, *id)

	v2.TaskMergeTransferPageLengthGauge.Sub(float64(cnt))
}

func (table *TransferTable[T]) Close() {
	table.Lock()
	defer table.Unlock()
	for _, item := range table.pages {
		item.Close()
	}
	table.pages = make(map[common.ID]*common.PinnedItem[T])
}
