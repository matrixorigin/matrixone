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

package memtable

import (
	"errors"
	"fmt"
	"sync"

	"github.com/matrixorigin/matrixone/pkg/vm/engine/aoe/storage"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/aoe/storage/layout/table/v1/iface"
	imem "github.com/matrixorigin/matrixone/pkg/vm/engine/aoe/storage/memtable/v1/base"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/aoe/storage/shard"
)

// manager is the collection manager, it's global,
// created when open db
type manager struct {
	sync.RWMutex

	// opts is the options of aoe
	opts *storage.Options

	// collections are containers of managed collection
	collections map[uint64]imem.ICollection

	aware shard.NodeAware
}

var (
	_ imem.IManager = (*manager)(nil)
)

func NewManager(opts *storage.Options, aware shard.NodeAware) *manager {
	m := &manager{
		opts:        opts,
		aware:       aware,
		collections: make(map[uint64]imem.ICollection),
	}
	return m
}

func (m *manager) CollectionIDs() map[uint64]uint64 {
	ids := make(map[uint64]uint64)
	m.RLock()
	for k, _ := range m.collections {
		ids[k] = k
	}
	m.RUnlock()
	return ids
}

func (m *manager) WeakRefCollection(id uint64) imem.ICollection {
	m.RLock()
	c, ok := m.collections[id]
	m.RUnlock()
	if !ok {
		return nil
	}
	return c
}

func (m *manager) StrongRefCollection(id uint64) imem.ICollection {
	m.RLock()
	c, ok := m.collections[id]
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
	s := fmt.Sprintf("<MTManager>(TableCnt=%d)", len(m.collections))
	for _, c := range m.collections {
		s = fmt.Sprintf("%s\n\t%s", s, c.String())
	}
	return s
}

func (m *manager) RegisterCollection(td interface{}) (c imem.ICollection, err error) {
	m.Lock()
	tableData := td.(iface.ITableData)
	_, ok := m.collections[tableData.GetID()]
	if ok {
		m.Unlock()
		return nil, errors.New("logic error")
	}
	c = newCollection(m, tableData)
	m.collections[tableData.GetID()] = c
	m.Unlock()
	c.Ref()
	if m.aware != nil {
		meta := c.GetMeta()
		m.aware.ShardNodeCreated(meta.GetCommit().LogIndex.ShardId, meta.Id)
	}
	return c, err
}

func (m *manager) UnregisterCollection(id uint64) (c imem.ICollection, err error) {
	m.Lock()
	c, ok := m.collections[id]
	if ok {
		delete(m.collections, id)
	} else {
		m.Unlock()
		return nil, errors.New("logic error")
	}
	m.Unlock()
	if m.aware != nil {
		meta := c.GetMeta()
		m.aware.ShardNodeDeleted(meta.GetCommit().LogIndex.ShardId, meta.Id)
	}
	return c, err
}
