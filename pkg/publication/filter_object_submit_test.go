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

package publication

import (
	"context"
	"testing"

	"github.com/matrixorigin/matrixone/pkg/common/moerr"
	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/objectio"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// ---- submitObjectsAsInsert tests ----

func TestSubmitObjectsAsInsert_EmptyInput(t *testing.T) {
	err := submitObjectsAsInsert(context.Background(), "task-1", nil, nil, nil, nil, nil)
	assert.NoError(t, err)
}

func TestSubmitObjectsAsInsert_EmptyBothSlices(t *testing.T) {
	err := submitObjectsAsInsert(context.Background(), "task-1", nil, nil,
		[]*ObjectWithTableInfo{}, []*ObjectWithTableInfo{}, nil)
	assert.NoError(t, err)
}

func TestSubmitObjectsAsInsert_NilEngine_Tombstone(t *testing.T) {
	stats := []*ObjectWithTableInfo{{DBName: "db1", TableName: "t1"}}
	err := submitObjectsAsInsert(context.Background(), "task-1", nil, nil, stats, nil, nil)
	require.Error(t, err)
	assert.Contains(t, err.Error(), "engine is nil")
}

func TestSubmitObjectsAsInsert_NilEngine_Data(t *testing.T) {
	stats := []*ObjectWithTableInfo{{DBName: "db1", TableName: "t1"}}
	err := submitObjectsAsInsert(context.Background(), "task-1", nil, nil, nil, stats, nil)
	require.Error(t, err)
	assert.Contains(t, err.Error(), "engine is nil")
}

// ---- submitObjectsAsDelete tests ----

func TestSubmitObjectsAsDelete_EmptyInput(t *testing.T) {
	err := submitObjectsAsDelete(context.Background(), "task-1", nil, nil, nil, nil)
	assert.NoError(t, err)
}

func TestSubmitObjectsAsDelete_EmptySlice(t *testing.T) {
	err := submitObjectsAsDelete(context.Background(), "task-1", nil, nil, []*ObjectWithTableInfo{}, nil)
	assert.NoError(t, err)
}

// ---- GetObjectListMap tests ----

func TestGetObjectListMap_ExecError(t *testing.T) {
	mock := &mockSQLExec{fn: func(q string) (*Result, context.CancelFunc, error) {
		return nil, nil, moerr.NewInternalErrorNoCtx("connection failed")
	}}
	iterCtx := &IterationContext{
		UpstreamExecutor:        mock,
		CurrentSnapshotName:     "snap1",
		SubscriptionAccountName: "acc1",
		SubscriptionName:        "sub1",
	}
	_, err := GetObjectListMap(context.Background(), iterCtx, nil)
	require.Error(t, err)
	assert.Contains(t, err.Error(), "failed to get object list")
}

func TestGetObjectListMap_EmptySnapshot(t *testing.T) {
	iterCtx := &IterationContext{
		UpstreamExecutor:    &mockSQLExec{fn: func(q string) (*Result, context.CancelFunc, error) { return nil, nil, nil }},
		CurrentSnapshotName: "",
	}
	_, err := GetObjectListMap(context.Background(), iterCtx, nil)
	require.Error(t, err)
}

// ---- ApplyObjects tests ----

func TestApplyObjects_EmptyObjectMap(t *testing.T) {
	objectMap := make(map[objectio.ObjectId]*ObjectWithTableInfo)
	err := ApplyObjects(
		context.Background(), "task-1", 0, nil, objectMap,
		nil, nil, types.TS{}, nil, nil, nil, nil,
		nil, nil, nil, "", "", nil, nil, nil,
	)
	assert.NoError(t, err)
}

func TestApplyObjects_NilObjectMap(t *testing.T) {
	err := ApplyObjects(
		context.Background(), "task-1", 0, nil, nil,
		nil, nil, types.TS{}, nil, nil, nil, nil,
		nil, nil, nil, "", "", nil, nil, nil,
	)
	assert.NoError(t, err)
}

func TestApplyObjects_TTLExpired(t *testing.T) {
	objectMap := map[objectio.ObjectId]*ObjectWithTableInfo{
		{}: {DBName: "db1", TableName: "t1"},
	}
	ttlChecker := func() bool { return false }
	err := ApplyObjects(
		context.Background(), "task-1", 0, nil, objectMap,
		nil, nil, types.TS{}, nil, nil, nil, nil,
		nil, nil, nil, "", "", nil, nil, ttlChecker,
	)
	assert.ErrorIs(t, err, ErrSyncProtectionTTLExpired)
}

func TestApplyObjects_TTLValid_NonAppendableDelete_NilEngine(t *testing.T) {
	var objID objectio.ObjectId
	objID[0] = 1

	var stats objectio.ObjectStats
	// Non-appendable object with Delete=true, not tombstone
	objectMap := map[objectio.ObjectId]*ObjectWithTableInfo{
		objID: {
			Stats:       stats,
			IsTombstone: false,
			Delete:      true,
			DBName:      "db1",
			TableName:   "t1",
		},
	}
	ttlChecker := func() bool { return true }
	err := ApplyObjects(
		context.Background(), "task-1", 0, nil, objectMap,
		nil, nil, types.TS{}, nil, nil, nil, nil,
		nil, nil, nil, "", "", nil, nil, ttlChecker,
	)
	// Should fail at submitObjectsAsDelete with nil engine
	require.Error(t, err)
	assert.Contains(t, err.Error(), "engine is nil")
}

func TestApplyObjects_IndexTableMapping(t *testing.T) {
	var objID objectio.ObjectId
	objID[0] = 2

	var stats objectio.ObjectStats
	objectMap := map[objectio.ObjectId]*ObjectWithTableInfo{
		objID: {
			Stats:       stats,
			IsTombstone: false,
			Delete:      true,
			DBName:      "db1",
			TableName:   "__mo_index_t1",
		},
	}
	indexMappings := map[string]string{
		"__mo_index_t1": "__mo_index_downstream_t1",
	}
	ttlChecker := func() bool { return true }
	err := ApplyObjects(
		context.Background(), "task-1", 0, indexMappings, objectMap,
		nil, nil, types.TS{}, nil, nil, nil, nil,
		nil, nil, nil, "", "", nil, nil, ttlChecker,
	)
	// Should fail at submit but with the renamed table
	require.Error(t, err)
}

func TestApplyObjects_TombstoneDelete_NilEngine(t *testing.T) {
	var objID objectio.ObjectId
	objID[0] = 3

	var stats objectio.ObjectStats
	objectMap := map[objectio.ObjectId]*ObjectWithTableInfo{
		objID: {
			Stats:       stats,
			IsTombstone: true,
			Delete:      true,
			DBName:      "db1",
			TableName:   "t1",
		},
	}
	err := ApplyObjects(
		context.Background(), "task-1", 0, nil, objectMap,
		nil, nil, types.TS{}, nil, nil, nil, nil,
		nil, nil, nil, "", "", nil, nil, nil,
	)
	require.Error(t, err)
	assert.Contains(t, err.Error(), "engine is nil")
}

func TestAppendDownstreamTombstoneStatsKeepsEverySpill(t *testing.T) {
	firstID := objectio.NewObjectid()
	secondID := objectio.NewObjectid()
	first := *objectio.NewObjectStatsWithObjectID(
		&firstID, false, true, true)
	second := *objectio.NewObjectStatsWithObjectID(
		&secondID, false, true, true)
	info := &ObjectWithTableInfo{DBName: "db", TableName: "table"}

	got := appendDownstreamTombstoneStats(
		nil,
		[]objectio.ObjectStats{first, {}, second},
		info,
	)
	require.Len(t, got, 2)
	require.Equal(t, first, got[0].Stats)
	require.Equal(t, second, got[1].Stats)
	for _, item := range got {
		require.Equal(t, "db", item.DBName)
		require.Equal(t, "table", item.TableName)
		require.True(t, item.IsTombstone)
		require.False(t, item.Delete)
	}
}

type presetFilterObjectWorker struct {
	result *FilterObjectJobResult
}

func (w *presetFilterObjectWorker) SubmitFilterObject(job Job) error {
	job.(*FilterObjectJob).complete(w.result)
	return nil
}

func (*presetFilterObjectWorker) Stop() {}

func TestApplyObjectsSubmitsNonAppendableTombstoneSpills(t *testing.T) {
	firstID := objectio.NewObjectid()
	secondID := objectio.NewObjectid()
	first := *objectio.NewObjectStatsWithObjectID(
		&firstID, false, true, true)
	second := *objectio.NewObjectStatsWithObjectID(
		&secondID, false, true, true)
	var upstreamID objectio.ObjectId
	upstreamID[0] = 1
	objects := map[objectio.ObjectId]*ObjectWithTableInfo{
		upstreamID: {
			IsTombstone: true,
			DBName:      "db",
			TableName:   "table",
		},
	}
	worker := &presetFilterObjectWorker{result: &FilterObjectJobResult{
		DownstreamStatsList: []objectio.ObjectStats{first, second},
	}}

	err := ApplyObjects(
		context.Background(), "task", 0, nil, objects,
		nil, nil, types.TS{}, nil, nil, nil, nil,
		worker, nil, nil, "account", "publication", nil, nil, nil,
	)
	require.ErrorContains(t, err, "engine is nil",
		"a non-empty spill list must reach tombstone submission")
}
