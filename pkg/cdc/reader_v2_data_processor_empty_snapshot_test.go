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

	"github.com/matrixorigin/matrixone/pkg/common/mpool"
	"github.com/matrixorigin/matrixone/pkg/container/batch"
	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/fileservice"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// Bug #4: Empty snapshot (rows=0) should be skipped by DataProcessor
// Without this fix, empty batch would be sent to Sinker and fail validation
func TestDataProcessor_ProcessChange_EmptySnapshot(t *testing.T) {
	ctx := context.Background()
	mp, err := mpool.NewMPool("test", 0, mpool.NoFixed)
	require.NoError(t, err)
	defer mpool.DeleteMPool(mp)

	sinker := &mockDataProcessorSinker{mockSinker: &mockSinker{}}
	updater := newMockWatermarkUpdater()
	txnMgr := NewTransactionManager(sinker, updater, 1, "task1", "db1", "table1")
	packerPool := fileservice.NewPool(
		128,
		func() *types.Packer { return types.NewPacker() },
		func(packer *types.Packer) { packer.Reset() },
		func(packer *types.Packer) {},
	)

	dp := NewDataProcessor(
		sinker, txnMgr, mp, packerPool,
		1, 0, 1, 0, false,
		1, "task1", "db1", "table1",
	)

	fromTs := types.TS{}
	toTs := (&fromTs).Next()
	dp.SetTransactionRange(fromTs, toTs)

	data := &ChangeData{
		Type:        ChangeTypeSnapshot,
		InsertBatch: &batch.Batch{},
	}

	err = dp.ProcessChange(ctx, data)

	assert.NoError(t, err, "Empty snapshot should not cause error")
	assert.False(t, sinker.beginCalled, "Empty snapshot should skip transaction")
	assert.Equal(t, 0, len(sinker.sinkCalls), "Empty snapshot should not call Sink")
}
