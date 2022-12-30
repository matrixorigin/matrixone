// Copyright 2021 Matrix Origin
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//	http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package service

import (
	"sync"
	"sync/atomic"

	"github.com/matrixorigin/matrixone/pkg/pb/api"
	"github.com/matrixorigin/matrixone/pkg/pb/timestamp"
)

// Waterliner maintains waterline for all subscribed tables.
type Waterliner struct {
	sync.RWMutex
	tables map[TableID]*tableInfo
}

func NewWaterliner() *Waterliner {
	return &Waterliner{
		tables: make(map[TableID]*tableInfo),
	}
}

// Register registers subscribed table.
//
// We wouldn't advance waterline here, just register table.
func (w *Waterliner) Register(
	id TableID, table api.TableID, waterline timestamp.Timestamp,
) {
	w.Lock()
	defer w.Unlock()

	if _, ok := w.tables[id]; !ok {
		w.tables[id] = newTableInfo(id, table, waterline)
	}
	w.tables[id].Ref()
}

// Unregister decreases reference count for tables.
func (w *Waterliner) Unregister(ids ...TableID) {
	w.Lock()
	defer w.Unlock()

	for _, id := range ids {
		info, ok := w.tables[id]
		if !ok {
			continue
		}
		if info.Deref() == 0 {
			delete(w.tables, id)
		}
	}
}

// ListTable takes snapshot for all subscribed tables.
func (w *Waterliner) ListTable() []tableInfo {
	w.RLock()
	defer w.RUnlock()

	tables := make([]tableInfo, 0, len(w.tables))
	for _, info := range w.tables {
		tables = append(tables, *info)
	}
	return tables
}

// Waterline returns waterline for subscribed table.
//
// If table not subscribed before, we would take current timestamp as waterline.
// if table subscribed, just take the last waterline.
func (w *Waterliner) Waterline(id TableID) (timestamp.Timestamp, bool) {
	w.RLock()
	defer w.RUnlock()

	if info, ok := w.tables[id]; ok {
		return info.waterline, true
	}
	return timestamp.Timestamp{}, false
}

// Advance updates waterline.
//
// For simplicity, just update all subscribed tables directly.
func (w *Waterliner) AdvanceWaterline(waterline timestamp.Timestamp) {
	w.Lock()
	defer w.Unlock()

	for _, info := range w.tables {
		info.waterline = waterline
	}
}

// tableInfo describes subscribed table.
type tableInfo struct {
	rc        int32
	id        TableID
	table     api.TableID
	waterline timestamp.Timestamp
}

func newTableInfo(
	id TableID, table api.TableID, waterline timestamp.Timestamp,
) *tableInfo {
	return &tableInfo{
		rc:        0,
		id:        id,
		table:     table,
		waterline: waterline,
	}
}

// Ref increases reference count.
func (t *tableInfo) Ref() int32 {
	return atomic.AddInt32(&t.rc, 1)
}

// Ref decreases reference count.
func (t *tableInfo) Deref() int32 {
	return atomic.AddInt32(&t.rc, -1)
}
