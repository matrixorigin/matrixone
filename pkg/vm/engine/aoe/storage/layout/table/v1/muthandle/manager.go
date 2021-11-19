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

package muthandle

import (
	"errors"
	"fmt"
	"sync"

	"github.com/matrixorigin/matrixone/pkg/vm/engine/aoe/storage"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/aoe/storage/layout/table/v1/iface"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/aoe/storage/layout/table/v1/muthandle/base"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/aoe/storage/metadata/v1"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/aoe/storage/shard"
)

var (
	ErrTableNotFound = metadata.TableNotFoundErr
)

// manager is the collection manager, it's global,
// created when open db
type manager struct {
	sync.RWMutex

	// opts is the options of aoe
	opts *storage.Options

	// tables are containers of managed collection
	tables map[uint64]base.MutableTable

	aware shard.NodeAware
}

var (
	_ base.IManager = (*manager)(nil)
)

func NewManager(opts *storage.Options, aware shard.NodeAware) *manager {
	m := &manager{
		opts:   opts,
		aware:  aware,
		tables: make(map[uint64]base.MutableTable),
	}
	return m
}

func (m *manager) TableIds() map[uint64]uint64 {
	ids := make(map[uint64]uint64)
	m.RLock()
	for k, _ := range m.tables {
		ids[k] = k
	}
	m.RUnlock()
	return ids
}

func (m *manager) WeakRefTable(id uint64) base.MutableTable {
	m.RLock()
	c, ok := m.tables[id]
	m.RUnlock()
	if !ok {
		return nil
	}
	return c
}

func (m *manager) StrongRefTable(id uint64) base.MutableTable {
	m.RLock()
	c, ok := m.tables[id]
	if ok {
		c.Ref()
	}
	m.RUnlock()
	if !ok {
		return nil
	}
	return c
}

func (m *manager) String() string {
	m.RLock()
	defer m.RUnlock()
	s := fmt.Sprintf("<MTManager>(TableCnt=%d)", len(m.tables))
	for _, c := range m.tables {
		s = fmt.Sprintf("%s\n\t%s", s, c.String())
	}
	return s
}

func (m *manager) RegisterTable(td interface{}) (c base.MutableTable, err error) {
	m.Lock()
	tableData := td.(iface.ITableData)
	_, ok := m.tables[tableData.GetID()]
	if ok {
		m.Unlock()
		return nil, errors.New("logic error")
	}
	c = newMutableTable(m, tableData)
	m.tables[tableData.GetID()] = c
	m.Unlock()
	c.Ref()
	if m.aware != nil {
		meta := c.GetMeta()
		m.aware.ShardNodeCreated(meta.GetCommit().LogIndex.ShardId, meta.Id)
	}
	return c, err
}

func (m *manager) UnregisterTable(id uint64) (c base.MutableTable, err error) {
	m.Lock()
	c, ok := m.tables[id]
	if ok {
		delete(m.tables, id)
	} else {
		m.Unlock()
		return nil, ErrTableNotFound
	}
	m.Unlock()
	if m.aware != nil {
		meta := c.GetMeta()
		m.aware.ShardNodeDeleted(meta.GetCommit().LogIndex.ShardId, meta.Id)
	}
	return c, err
}
