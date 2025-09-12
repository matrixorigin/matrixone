// Copyright 2025 Matrix Origin
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

package group

import (
	"fmt"
	"sync/atomic"

	"github.com/matrixorigin/matrixone/pkg/common/mpool"
)

type MemorySpillManager struct {
	data     map[SpillID][]byte
	nextID   int64
	totalMem int64
}

func NewMemorySpillManager() *MemorySpillManager {
	return &MemorySpillManager{
		data: make(map[SpillID][]byte),
	}
}

func (m *MemorySpillManager) Spill(data SpillableData) (SpillID, error) {
	serialized, err := data.Serialize()
	if err != nil {
		return "", err
	}

	id := SpillID(fmt.Sprintf("spill_%d", atomic.AddInt64(&m.nextID, 1)))
	m.data[id] = serialized
	atomic.AddInt64(&m.totalMem, int64(len(serialized)))
	return id, nil
}

func (m *MemorySpillManager) Retrieve(id SpillID, mp *mpool.MPool) (SpillableData, error) {
	serialized, exists := m.data[id]
	if !exists {
		return nil, fmt.Errorf("spill data not found: %s", id)
	}

	data := &SpillableAggState{}
	if err := data.Deserialize(serialized, mp); err != nil {
		return nil, err
	}
	return data, nil
}

func (m *MemorySpillManager) Delete(id SpillID) error {
	if serialized, exists := m.data[id]; exists {
		atomic.AddInt64(&m.totalMem, -int64(len(serialized)))
		delete(m.data, id)
	}
	return nil
}

func (m *MemorySpillManager) Free() {
	for id := range m.data {
		m.Delete(id)
	}
	m.data = nil
}
