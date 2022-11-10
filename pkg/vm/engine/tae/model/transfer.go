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
	"fmt"
	"sync"
	"time"

	"github.com/matrixorigin/matrixone/pkg/common/moerr"
	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/logutil"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/common"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/containers"
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

type TransferPage struct {
	common.RefHelper
	bornTS  time.Time
	id      *common.ID
	mapping containers.Vector
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
	table.Lock()
	for _, pinned := range items {
		delete(table.pages, *pinned.Item().ID())
	}
	table.Unlock()
	for _, pinned := range items {
		pinned.Close()
	}
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

func NewRowIDVector() containers.Vector {
	return containers.MakeVector(
		types.T_Rowid.ToType(),
		false,
		&containers.Options{
			Allocator: common.CacheAllocator,
		})
}

func NewTransferPage(
	bornTS time.Time,
	id *common.ID,
	mapping containers.Vector) *TransferPage {
	if mapping == nil {
		mapping = NewRowIDVector()
	}
	page := &TransferPage{
		bornTS:  bornTS,
		id:      id,
		mapping: mapping,
	}
	page.OnZeroCB = page.Close
	return page
}

func (page *TransferPage) Close() {
	logutil.Infof("Closing %s", page.String())
	if page.mapping != nil {
		page.mapping.Close()
		page.mapping = nil
	} else {
		panic(moerr.NewInternalError("page was closed more than once"))
	}
}

func (page *TransferPage) GetBornTS() time.Time { return page.bornTS }
func (page *TransferPage) ID() *common.ID       { return page.id }

func (page *TransferPage) TTL(now time.Time, ttl time.Duration) bool {
	return now.After(page.bornTS.Add(ttl))
}

func (page *TransferPage) String() string {
	return fmt.Sprintf("page[%s][%s][Len=%d]",
		page.id.BlockString(),
		page.bornTS.String(),
		page.mapping.Length())
}

func (page *TransferPage) Pin() *common.PinnedItem[*TransferPage] {
	page.Ref()
	return &common.PinnedItem[*TransferPage]{
		Val: page,
	}
}

func (page *TransferPage) TransferOne(srcOff uint32) types.Rowid {
	return page.mapping.Get(int(srcOff)).(types.Rowid)
}

func (page *TransferPage) TransferMany(srcOffs ...uint32) (dest containers.Vector) {
	dest = NewRowIDVector()
	slice := page.mapping.Slice().([]types.Rowid)
	for _, off := range srcOffs {
		dest.Append(slice[off])
	}
	return
}
