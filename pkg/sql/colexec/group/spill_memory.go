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
	"sync"
	"sync/atomic"

	"github.com/matrixorigin/matrixone/pkg/common/mpool"
	"github.com/matrixorigin/matrixone/pkg/logutil"
	"go.uber.org/zap"
)

type MemorySpillManager struct {
	data     map[SpillID][]byte
	nextID   int64
	totalMem int64
	mu       sync.Mutex
}

func NewMemorySpillManager() *MemorySpillManager {
	return &MemorySpillManager{
		data: make(map[SpillID][]byte),
	}
}

func (m *MemorySpillManager) Spill(data SpillableData) (SpillID, error) {
	serialized, err := data.Serialize()
	if err != nil {
		logutil.Error("[SPILL] MemorySpillManager failed to serialize data", zap.Error(err))
		return "", err
	}

	id := SpillID(fmt.Sprintf("spill_%d", atomic.AddInt64(&m.nextID, 1)))
	m.mu.Lock()
	defer m.mu.Unlock()
	m.data[id] = serialized
	newTotalMem := atomic.AddInt64(&m.totalMem, int64(len(serialized)))

	logutil.Info("[SPILL] MemorySpillManager spilled data",
		zap.String("spill_id", string(id)),
		zap.Int("data_size", len(serialized)),
		zap.Int64("total_memory", newTotalMem))

	return id, nil
}

func (m *MemorySpillManager) Retrieve(id SpillID, mp *mpool.MPool) (SpillableData, error) {
	m.mu.Lock()
	defer m.mu.Unlock()

	serialized, exists := m.data[id]
	if !exists {
		logutil.Error("[SPILL] MemorySpillManager failed to find spilled data",
			zap.String("spill_id", string(id)))
		return nil, fmt.Errorf("spill data not found: %s", id)
	}

	logutil.Info("[SPILL] MemorySpillManager retrieving spilled data",
		zap.String("spill_id", string(id)),
		zap.Int("data_size", len(serialized)))

	data := &SpillableAggState{}
	if err := data.Deserialize(serialized, mp); err != nil {
		logutil.Error("[SPILL] MemorySpillManager failed to deserialize data",
			zap.String("spill_id", string(id)), zap.Error(err))
		data.Free(mp)
		return nil, err
	}

	logutil.Info("[SPILL] MemorySpillManager successfully retrieved and deserialized data",
		zap.String("spill_id", string(id)),
		zap.Int64("estimated_size", data.EstimateSize()))

	return data, nil
}

func (m *MemorySpillManager) Delete(id SpillID) error {
	m.mu.Lock()
	defer m.mu.Unlock()

	if serialized, exists := m.data[id]; exists {
		newTotalMem := atomic.AddInt64(&m.totalMem, -int64(len(serialized)))
		delete(m.data, id)

		logutil.Info("[SPILL] MemorySpillManager deleted spilled data",
			zap.String("spill_id", string(id)),
			zap.Int("data_size", len(serialized)),
			zap.Int64("total_memory", newTotalMem))
	} else {
		logutil.Warn("[SPILL] MemorySpillManager attempted to delete non-existent spilled data",
			zap.String("spill_id", string(id)))
	}
	return nil
}

func (m *MemorySpillManager) Free() {
	m.mu.Lock()
	defer m.mu.Unlock()

	count := len(m.data)
	totalSize := atomic.LoadInt64(&m.totalMem)

	logutil.Info("[SPILL] MemorySpillManager freeing all spilled data",
		zap.Int("spilled_count", count),
		zap.Int64("total_size", totalSize))

	for id := range m.data {
		m.Delete(id)
	}
	m.data = nil

	logutil.Info("[SPILL] MemorySpillManager completed cleanup")
}

func (m *MemorySpillManager) TotalMem() int64 {
	m.mu.Lock()
	defer m.mu.Unlock()

	return atomic.LoadInt64(&m.totalMem)
}
