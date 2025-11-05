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
	"sync"
	"testing"
	"time"

	"github.com/matrixorigin/matrixone/pkg/common/mpool"
	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/fileservice"
	"github.com/matrixorigin/matrixone/pkg/pb/plan"
	"github.com/stretchr/testify/assert"
)

func TestNewTableChangeStream(t *testing.T) {
	mp, err := mpool.NewMPool("test", 0, mpool.NoFixed)
	assert.NoError(t, err)
	defer mpool.DeleteMPool(mp)

	sinker := &mockDataProcessorSinker{mockSinker: &mockSinker{}}
	updater := newMockWatermarkUpdater()
	packerPool := fileservice.NewPool(
		128,
		func() *types.Packer { return types.NewPacker() },
		func(packer *types.Packer) { packer.Reset() },
		func(packer *types.Packer) {},
	)

	tableDef := &plan.TableDef{
		Cols: []*plan.ColDef{
			{Name: "id"},
			{Name: "name"},
			{Name: "ts"},
		},
		Pkey: &plan.PrimaryKeyDef{
			Names: []string{"id"},
		},
		Name2ColIndex: map[string]int32{
			"id":   0,
			"name": 1,
			"ts":   2,
		},
	}

	tableInfo := &DbTableInfo{
		SourceDbName:  "db1",
		SourceTblName: "table1",
		SourceTblId:   1,
	}

	runningReaders := &sync.Map{}
	startTs := types.TS{}
	endTs := (&startTs).Next()

	stream := NewTableChangeStream(
		nil, // cnTxnClient
		nil, // cnEngine
		mp,
		packerPool,
		1, // accountId
		"task1",
		tableInfo,
		sinker,
		updater,
		tableDef,
		false, // initSnapshotSplitTxn
		runningReaders,
		startTs,
		endTs,
		false, // noFull
		200*time.Millisecond,
	)

	assert.NotNil(t, stream)
	assert.Equal(t, mp, stream.mp)
	assert.Equal(t, sinker, stream.sinker)
	assert.Equal(t, updater, stream.watermarkUpdater)
	assert.Equal(t, uint64(1), stream.accountId)
	assert.Equal(t, "task1", stream.taskId)
	assert.Equal(t, tableInfo, stream.tableInfo)
	assert.Equal(t, tableDef, stream.tableDef)
	assert.NotNil(t, stream.txnManager)
	assert.NotNil(t, stream.dataProcessor)
	assert.Equal(t, 200*time.Millisecond, stream.frequency)
	assert.Equal(t, 2, stream.insTsColIdx)           // len(Cols)-1
	assert.Equal(t, 0, stream.insCompositedPkColIdx) // single PK
	assert.Equal(t, 1, stream.delTsColIdx)
	assert.Equal(t, 0, stream.delCompositedPkColIdx)
}

func TestNewTableChangeStream_CompositePK(t *testing.T) {
	mp, err := mpool.NewMPool("test", 0, mpool.NoFixed)
	assert.NoError(t, err)
	defer mpool.DeleteMPool(mp)

	sinker := &mockDataProcessorSinker{mockSinker: &mockSinker{}}
	updater := newMockWatermarkUpdater()
	packerPool := fileservice.NewPool(
		128,
		func() *types.Packer { return types.NewPacker() },
		func(packer *types.Packer) { packer.Reset() },
		func(packer *types.Packer) {},
	)

	tableDef := &plan.TableDef{
		Cols: []*plan.ColDef{
			{Name: "id"},
			{Name: "name"},
			{Name: "cpk"}, // Composite PK column
			{Name: "ts"},
		},
		Pkey: &plan.PrimaryKeyDef{
			Names: []string{"id", "name"}, // Composite PK
		},
		Name2ColIndex: map[string]int32{
			"id":   0,
			"name": 1,
			"cpk":  2,
			"ts":   3,
		},
	}

	tableInfo := &DbTableInfo{
		SourceDbName:  "db1",
		SourceTblName: "table1",
		SourceTblId:   1,
	}

	stream := NewTableChangeStream(
		nil, nil, mp, packerPool,
		1, "task1", tableInfo, sinker, updater, tableDef,
		false, &sync.Map{}, types.TS{}, types.TS{}, false, 0,
	)

	assert.NotNil(t, stream)
	assert.Equal(t, 3, stream.insTsColIdx)           // len(Cols)-1
	assert.Equal(t, 2, stream.insCompositedPkColIdx) // Composite PK col
}

func TestTableChangeStream_Info(t *testing.T) {
	mp, err := mpool.NewMPool("test", 0, mpool.NoFixed)
	assert.NoError(t, err)
	defer mpool.DeleteMPool(mp)

	tableInfo := &DbTableInfo{
		SourceDbName:  "db1",
		SourceTblName: "table1",
	}

	stream := createTestStream(mp, tableInfo)

	assert.Equal(t, tableInfo, stream.Info())
}

func TestTableChangeStream_GetWg(t *testing.T) {
	mp, err := mpool.NewMPool("test", 0, mpool.NoFixed)
	assert.NoError(t, err)
	defer mpool.DeleteMPool(mp)

	stream := createTestStream(mp, &DbTableInfo{})

	wg := stream.GetWg()
	assert.NotNil(t, wg)
	assert.Equal(t, &stream.wg, wg)
}

func TestTableChangeStream_ForceNextInterval(t *testing.T) {
	mp, err := mpool.NewMPool("test", 0, mpool.NoFixed)
	assert.NoError(t, err)
	defer mpool.DeleteMPool(mp)

	stream := createTestStream(mp, &DbTableInfo{})

	assert.False(t, stream.force)

	stream.forceNextInterval(100 * time.Millisecond)

	assert.True(t, stream.force)
}

// Helper function to create a test stream with minimal setup
func createTestStream(mp *mpool.MPool, tableInfo *DbTableInfo) *tableChangeStream {
	sinker := &mockDataProcessorSinker{mockSinker: &mockSinker{}}
	updater := newMockWatermarkUpdater()
	packerPool := fileservice.NewPool(
		128,
		func() *types.Packer { return types.NewPacker() },
		func(packer *types.Packer) { packer.Reset() },
		func(packer *types.Packer) {},
	)

	tableDef := &plan.TableDef{
		Cols: []*plan.ColDef{
			{Name: "id"},
			{Name: "ts"},
		},
		Pkey: &plan.PrimaryKeyDef{
			Names: []string{"id"},
		},
		Name2ColIndex: map[string]int32{
			"id": 0,
			"ts": 1,
		},
	}

	return NewTableChangeStream(
		nil, nil, mp, packerPool,
		1, "task1", tableInfo, sinker, updater, tableDef,
		false, &sync.Map{}, types.TS{}, types.TS{}, false, 0,
	)
}
