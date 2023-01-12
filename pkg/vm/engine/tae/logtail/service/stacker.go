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
)

// TableStacker registers table repeatedly.
type TableStacker struct {
	sync.RWMutex
	tables map[TableID]*tableInfo
}

func NewTableStacker() *TableStacker {
	return &TableStacker{
		tables: make(map[TableID]*tableInfo),
	}
}

// ListTable takes a snapshot for all registered tables.
func (s *TableStacker) ListTable() []tableInfo {
	s.RLock()
	defer s.RUnlock()

	list := make([]tableInfo, 0, len(s.tables))
	for _, info := range s.tables {
		list = append(list, *info)
	}
	return list
}

// Register registers subscribed table.
func (s *TableStacker) Register(id TableID, table api.TableID) {
	s.Lock()
	defer s.Unlock()

	if _, ok := s.tables[id]; !ok {
		s.tables[id] = newTableInfo(id, table)
	}
	s.tables[id].Ref()
}

// Unregister decreases reference count for tables.
func (s *TableStacker) Unregister(ids ...TableID) {
	s.Lock()
	defer s.Unlock()

	for _, id := range ids {
		info, ok := s.tables[id]
		if !ok {
			continue
		}
		if info.Deref() == 0 {
			delete(s.tables, id)
		}
	}
}

// tableInfo maintains a table's reference counter.
type tableInfo struct {
	rc    int32
	id    TableID
	table api.TableID
}

func newTableInfo(id TableID, table api.TableID) *tableInfo {
	return &tableInfo{
		rc:    0,
		id:    id,
		table: table,
	}
}

// Ref increases reference count.
func (t *tableInfo) Ref() int32 {
	return atomic.AddInt32(&t.rc, 1)
}

// Deref decreases reference count.
func (t *tableInfo) Deref() int32 {
	return atomic.AddInt32(&t.rc, -1)
}
