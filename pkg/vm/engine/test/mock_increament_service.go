// Copyright 2024 Matrix Origin
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

package test

import (
	"context"
	"sync"

	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/container/vector"
	"github.com/matrixorigin/matrixone/pkg/incrservice"
	"github.com/matrixorigin/matrixone/pkg/pb/timestamp"
	"github.com/matrixorigin/matrixone/pkg/txn/client"
)

var _ incrservice.AutoIncrementService = (*MockAutoIncrementService)(nil)

// MockAutoIncrementService is a mock implementation of AutoIncrementService for testing
type MockAutoIncrementService struct {
	mu       sync.Mutex
	uuid     string
	counters map[uint64]map[string]uint64 // tableID -> colName -> currentValue
	columns  map[uint64][]incrservice.AutoColumn
}

// NewMockAutoIncrementService creates a new MockAutoIncrementService
func NewMockAutoIncrementService(uuid string) *MockAutoIncrementService {
	return &MockAutoIncrementService{
		uuid:     uuid,
		counters: make(map[uint64]map[string]uint64),
		columns:  make(map[uint64][]incrservice.AutoColumn),
	}
}

// UUID returns the uuid of this increment service
func (m *MockAutoIncrementService) UUID() string {
	return m.uuid
}

// Create creates cache for auto-increment columns
func (m *MockAutoIncrementService) Create(
	ctx context.Context,
	tableID uint64,
	caches []incrservice.AutoColumn,
	txn client.TxnOperator,
) error {
	m.mu.Lock()
	defer m.mu.Unlock()

	m.columns[tableID] = caches
	m.counters[tableID] = make(map[string]uint64)
	for _, col := range caches {
		// Initialize counter with offset (default start value)
		m.counters[tableID][col.ColName] = col.Offset
	}
	return nil
}

// Reset resets auto-increment cache
func (m *MockAutoIncrementService) Reset(
	ctx context.Context,
	oldTableID, newTableID uint64,
	keep bool,
	txn client.TxnOperator,
) error {
	m.mu.Lock()
	defer m.mu.Unlock()

	if keep {
		// Copy counters from old table to new table
		if oldCounters, ok := m.counters[oldTableID]; ok {
			m.counters[newTableID] = make(map[string]uint64)
			for k, v := range oldCounters {
				m.counters[newTableID][k] = v
			}
		}
		if oldCols, ok := m.columns[oldTableID]; ok {
			m.columns[newTableID] = oldCols
		}
	}
	delete(m.counters, oldTableID)
	delete(m.columns, oldTableID)
	return nil
}

// Delete deletes auto-increment cache for a table
func (m *MockAutoIncrementService) Delete(
	ctx context.Context,
	tableID uint64,
	txn client.TxnOperator,
) error {
	m.mu.Lock()
	defer m.mu.Unlock()

	delete(m.counters, tableID)
	delete(m.columns, tableID)
	return nil
}

// InsertValues inserts auto-increment values into vectors
func (m *MockAutoIncrementService) InsertValues(
	ctx context.Context,
	tableID uint64,
	vecs []*vector.Vector,
	rows int,
	estimate int64,
) (uint64, error) {
	m.mu.Lock()
	defer m.mu.Unlock()

	cols, ok := m.columns[tableID]
	if !ok {
		return 0, nil
	}

	var maxValue uint64
	for _, col := range cols {
		if col.ColIndex >= len(vecs) {
			continue
		}
		vec := vecs[col.ColIndex]
		if vec == nil {
			continue
		}

		counter := m.counters[tableID][col.ColName]
		step := col.Step
		if step == 0 {
			step = 1
		}

		// Fill auto-increment values
		for i := 0; i < rows; i++ {
			counter += step
			// Set value based on vector type
			switch vec.GetType().Oid {
			case types.T_int8:
				vector.SetFixedAtNoTypeCheck(vec, i, int8(counter))
			case types.T_int16:
				vector.SetFixedAtNoTypeCheck(vec, i, int16(counter))
			case types.T_int32:
				vector.SetFixedAtNoTypeCheck(vec, i, int32(counter))
			case types.T_int64:
				vector.SetFixedAtNoTypeCheck(vec, i, int64(counter))
			case types.T_uint8:
				vector.SetFixedAtNoTypeCheck(vec, i, uint8(counter))
			case types.T_uint16:
				vector.SetFixedAtNoTypeCheck(vec, i, uint16(counter))
			case types.T_uint32:
				vector.SetFixedAtNoTypeCheck(vec, i, uint32(counter))
			case types.T_uint64:
				vector.SetFixedAtNoTypeCheck(vec, i, counter)
			}
		}

		m.counters[tableID][col.ColName] = counter
		if counter > maxValue {
			maxValue = counter
		}
	}

	return maxValue, nil
}

// CurrentValue returns current value of an auto-increment column
func (m *MockAutoIncrementService) CurrentValue(
	ctx context.Context,
	tableID uint64,
	col string,
) (uint64, error) {
	m.mu.Lock()
	defer m.mu.Unlock()

	if counters, ok := m.counters[tableID]; ok {
		if val, ok := counters[col]; ok {
			return val, nil
		}
	}
	return 0, nil
}

// Reload reloads auto-increment cache
func (m *MockAutoIncrementService) Reload(ctx context.Context, tableID uint64) error {
	// No-op for mock
	return nil
}

// Close closes the service
func (m *MockAutoIncrementService) Close() {
	m.mu.Lock()
	defer m.mu.Unlock()

	m.counters = make(map[uint64]map[string]uint64)
	m.columns = make(map[uint64][]incrservice.AutoColumn)
}

// GetLastAllocateTS returns the last allocate timestamp
func (m *MockAutoIncrementService) GetLastAllocateTS(
	ctx context.Context,
	tableID uint64,
	colName string,
) (timestamp.Timestamp, error) {
	return timestamp.Timestamp{}, nil
}
