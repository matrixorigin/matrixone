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

package cdc

import (
	"context"
	"testing"

	"github.com/matrixorigin/matrixone/pkg/common/moerr"
	"github.com/matrixorigin/matrixone/pkg/common/mpool"
	"github.com/matrixorigin/matrixone/pkg/container/batch"
	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/vm/engine"
	"github.com/stretchr/testify/assert"
)

// mockChangesHandle for testing
type mockChangesHandle struct {
	nextFunc  func(ctx context.Context, mp *mpool.MPool) (*batch.Batch, *batch.Batch, engine.ChangesHandle_Hint, error)
	closeFunc func() error
	closed    bool
}

func (m *mockChangesHandle) Next(ctx context.Context, mp *mpool.MPool) (*batch.Batch, *batch.Batch, engine.ChangesHandle_Hint, error) {
	if m.nextFunc != nil {
		return m.nextFunc(ctx, mp)
	}
	return nil, nil, engine.ChangesHandle_Snapshot, nil
}

func (m *mockChangesHandle) Close() error {
	m.closed = true
	if m.closeFunc != nil {
		return m.closeFunc()
	}
	return nil
}

func TestChangeType_String(t *testing.T) {
	tests := []struct {
		name     string
		ct       ChangeType
		expected string
	}{
		{"Snapshot", ChangeTypeSnapshot, "Snapshot"},
		{"TailWip", ChangeTypeTailWip, "TailWip"},
		{"TailDone", ChangeTypeTailDone, "TailDone"},
		{"NoMoreData", ChangeTypeNoMoreData, "NoMoreData"},
		{"Unknown", ChangeType(999), "Unknown"},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			assert.Equal(t, tt.expected, tt.ct.String())
		})
	}
}

func TestChangeData_HasData(t *testing.T) {
	tests := []struct {
		name        string
		insertBatch *batch.Batch
		deleteBatch *batch.Batch
		expected    bool
	}{
		{"BothNil", nil, nil, false},
		{"InsertOnly", &batch.Batch{}, nil, true},
		{"DeleteOnly", nil, &batch.Batch{}, true},
		{"BothPresent", &batch.Batch{}, &batch.Batch{}, true},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			cd := &ChangeData{
				InsertBatch: tt.insertBatch,
				DeleteBatch: tt.deleteBatch,
			}
			assert.Equal(t, tt.expected, cd.HasData())
		})
	}
}

func TestChangeData_Clean(t *testing.T) {
	mp, err := mpool.NewMPool("test", 0, mpool.NoFixed)
	assert.NoError(t, err)
	defer mpool.DeleteMPool(mp)

	cd := &ChangeData{
		InsertBatch: &batch.Batch{},
		DeleteBatch: &batch.Batch{},
	}

	cd.Clean(mp)

	assert.Nil(t, cd.InsertBatch)
	assert.Nil(t, cd.DeleteBatch)
}

func TestNewChangeCollector(t *testing.T) {
	mp, err := mpool.NewMPool("test", 0, mpool.NoFixed)
	assert.NoError(t, err)
	defer mpool.DeleteMPool(mp)

	handle := &mockChangesHandle{}
	fromTs := types.TS{}
	toTs := (&fromTs).Next()

	cc := NewChangeCollector(handle, mp, fromTs, toTs, 1, "task1", "db1", "table1")

	assert.NotNil(t, cc)
	assert.Equal(t, handle, cc.changesHandle)
	assert.Equal(t, mp, cc.mp)
	assert.Equal(t, fromTs, cc.fromTs)
	assert.Equal(t, toTs, cc.toTs)
	assert.Equal(t, uint64(1), cc.accountId)
	assert.Equal(t, "task1", cc.taskId)
	assert.Equal(t, "db1", cc.dbName)
	assert.Equal(t, "table1", cc.tableName)
	assert.False(t, cc.closed)
}

func TestChangeCollector_Next_Snapshot(t *testing.T) {
	ctx := context.Background()
	mp, err := mpool.NewMPool("test", 0, mpool.NoFixed)
	assert.NoError(t, err)
	defer mpool.DeleteMPool(mp)

	insertBatch := &batch.Batch{}
	handle := &mockChangesHandle{
		nextFunc: func(ctx context.Context, mp *mpool.MPool) (*batch.Batch, *batch.Batch, engine.ChangesHandle_Hint, error) {
			return insertBatch, nil, engine.ChangesHandle_Snapshot, nil
		},
	}

	fromTs := types.TS{}
	toTs := (&fromTs).Next()
	cc := NewChangeCollector(handle, mp, fromTs, toTs, 1, "task1", "db1", "table1")

	data, err := cc.Next(ctx)

	assert.NoError(t, err)
	assert.NotNil(t, data)
	assert.Equal(t, ChangeTypeSnapshot, data.Type)
	assert.Equal(t, insertBatch, data.InsertBatch)
	assert.Nil(t, data.DeleteBatch)
	assert.True(t, data.HasData())
}

func TestChangeCollector_Next_TailWip(t *testing.T) {
	ctx := context.Background()
	mp, err := mpool.NewMPool("test", 0, mpool.NoFixed)
	assert.NoError(t, err)
	defer mpool.DeleteMPool(mp)

	insertBatch := &batch.Batch{}
	deleteBatch := &batch.Batch{}
	handle := &mockChangesHandle{
		nextFunc: func(ctx context.Context, mp *mpool.MPool) (*batch.Batch, *batch.Batch, engine.ChangesHandle_Hint, error) {
			return insertBatch, deleteBatch, engine.ChangesHandle_Tail_wip, nil
		},
	}

	fromTs := types.TS{}
	toTs := (&fromTs).Next()
	cc := NewChangeCollector(handle, mp, fromTs, toTs, 1, "task1", "db1", "table1")

	data, err := cc.Next(ctx)

	assert.NoError(t, err)
	assert.NotNil(t, data)
	assert.Equal(t, ChangeTypeTailWip, data.Type)
	assert.Equal(t, insertBatch, data.InsertBatch)
	assert.Equal(t, deleteBatch, data.DeleteBatch)
	assert.True(t, data.HasData())
}

func TestChangeCollector_Next_TailDone(t *testing.T) {
	ctx := context.Background()
	mp, err := mpool.NewMPool("test", 0, mpool.NoFixed)
	assert.NoError(t, err)
	defer mpool.DeleteMPool(mp)

	deleteBatch := &batch.Batch{}
	handle := &mockChangesHandle{
		nextFunc: func(ctx context.Context, mp *mpool.MPool) (*batch.Batch, *batch.Batch, engine.ChangesHandle_Hint, error) {
			return nil, deleteBatch, engine.ChangesHandle_Tail_done, nil
		},
	}

	fromTs := types.TS{}
	toTs := (&fromTs).Next()
	cc := NewChangeCollector(handle, mp, fromTs, toTs, 1, "task1", "db1", "table1")

	data, err := cc.Next(ctx)

	assert.NoError(t, err)
	assert.NotNil(t, data)
	assert.Equal(t, ChangeTypeTailDone, data.Type)
	assert.Nil(t, data.InsertBatch)
	assert.Equal(t, deleteBatch, data.DeleteBatch)
	assert.True(t, data.HasData())
}

func TestChangeCollector_Next_NoMoreData(t *testing.T) {
	ctx := context.Background()
	mp, err := mpool.NewMPool("test", 0, mpool.NoFixed)
	assert.NoError(t, err)
	defer mpool.DeleteMPool(mp)

	handle := &mockChangesHandle{
		nextFunc: func(ctx context.Context, mp *mpool.MPool) (*batch.Batch, *batch.Batch, engine.ChangesHandle_Hint, error) {
			return nil, nil, engine.ChangesHandle_Tail_done, nil
		},
	}

	fromTs := types.TS{}
	toTs := (&fromTs).Next()
	cc := NewChangeCollector(handle, mp, fromTs, toTs, 1, "task1", "db1", "table1")

	data, err := cc.Next(ctx)

	assert.NoError(t, err)
	assert.NotNil(t, data)
	assert.Equal(t, ChangeTypeNoMoreData, data.Type)
	assert.Nil(t, data.InsertBatch)
	assert.Nil(t, data.DeleteBatch)
	assert.False(t, data.HasData())
}

func TestChangeCollector_Next_WithError(t *testing.T) {
	ctx := context.Background()
	mp, err := mpool.NewMPool("test", 0, mpool.NoFixed)
	assert.NoError(t, err)
	defer mpool.DeleteMPool(mp)

	expectedErr := moerr.NewInternalError(ctx, "test error")
	handle := &mockChangesHandle{
		nextFunc: func(ctx context.Context, mp *mpool.MPool) (*batch.Batch, *batch.Batch, engine.ChangesHandle_Hint, error) {
			return nil, nil, engine.ChangesHandle_Snapshot, expectedErr
		},
	}

	fromTs := types.TS{}
	toTs := (&fromTs).Next()
	cc := NewChangeCollector(handle, mp, fromTs, toTs, 1, "task1", "db1", "table1")

	data, err := cc.Next(ctx)

	assert.Error(t, err)
	assert.Equal(t, expectedErr, err)
	assert.Nil(t, data)
}

func TestChangeCollector_Next_AfterClose(t *testing.T) {
	ctx := context.Background()
	mp, err := mpool.NewMPool("test", 0, mpool.NoFixed)
	assert.NoError(t, err)
	defer mpool.DeleteMPool(mp)

	handle := &mockChangesHandle{}
	fromTs := types.TS{}
	toTs := (&fromTs).Next()
	cc := NewChangeCollector(handle, mp, fromTs, toTs, 1, "task1", "db1", "table1")

	// Close first
	err = cc.Close()
	assert.NoError(t, err)

	// Try to call Next after close
	data, err := cc.Next(ctx)

	assert.NoError(t, err)
	assert.NotNil(t, data)
	assert.Equal(t, ChangeTypeNoMoreData, data.Type)
}

func TestChangeCollector_Close(t *testing.T) {
	mp, err := mpool.NewMPool("test", 0, mpool.NoFixed)
	assert.NoError(t, err)
	defer mpool.DeleteMPool(mp)

	handle := &mockChangesHandle{}
	fromTs := types.TS{}
	toTs := (&fromTs).Next()
	cc := NewChangeCollector(handle, mp, fromTs, toTs, 1, "task1", "db1", "table1")

	err = cc.Close()

	assert.NoError(t, err)
	assert.True(t, cc.closed)
	assert.True(t, handle.closed)
}

func TestChangeCollector_Close_Multiple(t *testing.T) {
	mp, err := mpool.NewMPool("test", 0, mpool.NoFixed)
	assert.NoError(t, err)
	defer mpool.DeleteMPool(mp)

	handle := &mockChangesHandle{}
	fromTs := types.TS{}
	toTs := (&fromTs).Next()
	cc := NewChangeCollector(handle, mp, fromTs, toTs, 1, "task1", "db1", "table1")

	// Close multiple times should be safe
	err = cc.Close()
	assert.NoError(t, err)

	err = cc.Close()
	assert.NoError(t, err)

	assert.True(t, cc.closed)
}

func TestChangeCollector_Close_WithError(t *testing.T) {
	ctx := context.Background()
	mp, err := mpool.NewMPool("test", 0, mpool.NoFixed)
	assert.NoError(t, err)
	defer mpool.DeleteMPool(mp)

	expectedErr := moerr.NewInternalError(ctx, "close error")
	handle := &mockChangesHandle{
		closeFunc: func() error {
			return expectedErr
		},
	}
	fromTs := types.TS{}
	toTs := (&fromTs).Next()
	cc := NewChangeCollector(handle, mp, fromTs, toTs, 1, "task1", "db1", "table1")

	err = cc.Close()

	assert.Error(t, err)
	assert.Equal(t, expectedErr, err)
	assert.True(t, cc.closed)
}

func TestChangeCollector_IsClosed(t *testing.T) {
	mp, err := mpool.NewMPool("test", 0, mpool.NoFixed)
	assert.NoError(t, err)
	defer mpool.DeleteMPool(mp)

	handle := &mockChangesHandle{}
	fromTs := types.TS{}
	toTs := (&fromTs).Next()
	cc := NewChangeCollector(handle, mp, fromTs, toTs, 1, "task1", "db1", "table1")

	assert.False(t, cc.IsClosed())

	cc.Close()

	assert.True(t, cc.IsClosed())
}

func TestChangeCollector_GetFromTs(t *testing.T) {
	mp, err := mpool.NewMPool("test", 0, mpool.NoFixed)
	assert.NoError(t, err)
	defer mpool.DeleteMPool(mp)

	handle := &mockChangesHandle{}
	fromTs := types.TS{}
	toTs := (&fromTs).Next()
	cc := NewChangeCollector(handle, mp, fromTs, toTs, 1, "task1", "db1", "table1")

	assert.Equal(t, fromTs, cc.GetFromTs())
}

func TestChangeCollector_GetToTs(t *testing.T) {
	mp, err := mpool.NewMPool("test", 0, mpool.NoFixed)
	assert.NoError(t, err)
	defer mpool.DeleteMPool(mp)

	handle := &mockChangesHandle{}
	fromTs := types.TS{}
	toTs := (&fromTs).Next()
	cc := NewChangeCollector(handle, mp, fromTs, toTs, 1, "task1", "db1", "table1")

	assert.Equal(t, toTs, cc.GetToTs())
}
