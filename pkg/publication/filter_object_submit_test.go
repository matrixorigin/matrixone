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
	"encoding/json"
	"errors"
	"testing"

	"github.com/golang/mock/gomock"
	"github.com/matrixorigin/matrixone/pkg/common/moerr"
	"github.com/matrixorigin/matrixone/pkg/common/mpool"
	"github.com/matrixorigin/matrixone/pkg/container/batch"
	"github.com/matrixorigin/matrixone/pkg/container/types"
	mock_frontend "github.com/matrixorigin/matrixone/pkg/frontend/test"
	"github.com/matrixorigin/matrixone/pkg/objectio"
	"github.com/matrixorigin/matrixone/pkg/vm/engine"
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

func TestResolveCCPRObjectCleanupOwnersScopesCostToUniqueTombstones(t *testing.T) {
	ctrl := gomock.NewController(t)
	rel := mock_frontend.NewMockRelation(ctrl)
	rel.EXPECT().GetDBID(gomock.Any()).Return(uint64(11))
	rel.EXPECT().GetTableID(gomock.Any()).Return(uint64(22))
	db := mock_frontend.NewMockDatabase(ctrl)
	db.EXPECT().Relation(gomock.Any(), "table", nil).Return(rel, nil)
	cnEngine := mock_frontend.NewMockEngine(ctrl)
	cnEngine.EXPECT().Database(gomock.Any(), "db", nil).Return(db, nil)

	tombstoneID := objectio.NewObjectid()
	dataID := objectio.NewObjectid()
	owners, err := resolveCCPRObjectCleanupOwners(
		context.Background(),
		7,
		map[objectio.ObjectId]*ObjectWithTableInfo{
			tombstoneID: {
				Stats: *objectio.NewObjectStatsWithObjectID(
					&tombstoneID, false, true, false),
				DBName:      "db",
				TableName:   "table",
				IsTombstone: true,
			},
			dataID: {
				Stats: *objectio.NewObjectStatsWithObjectID(
					&dataID, false, false, false),
				DBName:    "db",
				TableName: "table",
			},
		},
		nil,
		cnEngine,
		CCPRSyncProtection{
			JobID:     "job",
			TNShardID: 44,
			ValidTS:   func() int64 { return 33 },
		},
	)
	require.NoError(t, err)
	require.Equal(t, uint64(11), owners[TableKey{
		DBName: "db", TableName: "table"}].DBID)
	require.Equal(t, uint64(22), owners[TableKey{
		DBName: "db", TableName: "table"}].TableID)
	require.Equal(t, uint64(44), owners[TableKey{
		DBName: "db", TableName: "table"}].TNShardID)

	owners, err = resolveCCPRObjectCleanupOwners(
		context.Background(),
		7,
		map[objectio.ObjectId]*ObjectWithTableInfo{
			dataID: {
				Stats: *objectio.NewObjectStatsWithObjectID(
					&dataID, false, false, false),
				DBName:    "db",
				TableName: "table",
			},
		},
		nil,
		nil,
		CCPRSyncProtection{},
	)
	require.NoError(t, err)
	require.Empty(t, owners,
		"stable-name data copies must not pay durable-owner lookup cost")
}

func TestResolveCCPRObjectCleanupOwnersRejectsIncompleteOwner(t *testing.T) {
	tombstoneID := objectio.NewObjectid()
	objectMap := map[objectio.ObjectId]*ObjectWithTableInfo{
		tombstoneID: {
			Stats: *objectio.NewObjectStatsWithObjectID(
				&tombstoneID, false, true, false),
			DBName:      "db",
			TableName:   "table",
			IsTombstone: true,
		},
	}

	validProtection := CCPRSyncProtection{
		JobID:     "job",
		TNShardID: 44,
		ValidTS:   func() int64 { return 33 },
	}
	tests := []struct {
		name       string
		engine     engine.Engine
		protection CCPRSyncProtection
	}{
		{name: "nil engine", protection: validProtection},
		{name: "empty job id", engine: mock_frontend.NewMockEngine(gomock.NewController(t)), protection: CCPRSyncProtection{
			TNShardID: 44,
			ValidTS:   validProtection.ValidTS,
		}},
		{name: "zero tn shard id", engine: mock_frontend.NewMockEngine(gomock.NewController(t)), protection: CCPRSyncProtection{
			JobID:   "job",
			ValidTS: validProtection.ValidTS,
		}},
		{name: "nil valid ts", engine: mock_frontend.NewMockEngine(gomock.NewController(t)), protection: CCPRSyncProtection{
			JobID:     "job",
			TNShardID: 44,
		}},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			owners, err := resolveCCPRObjectCleanupOwners(
				context.Background(), 7, objectMap, nil,
				test.engine, test.protection,
			)
			require.Nil(t, owners)
			require.ErrorContains(t, err,
				"CCPR durable cleanup owner is not configured")
		})
	}
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
		nil, nil, nil, "", "", nil, NewAObjectMap(), nil,
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

type recordingSoftDeleteRelation struct {
	engine.Relation
	objectIDs  []objectio.ObjectId
	tombstones []bool
	err        error
}

func (r *recordingSoftDeleteRelation) SoftDeleteObject(
	_ context.Context,
	objectID *objectio.ObjectId,
	isTombstone bool,
) error {
	r.objectIDs = append(r.objectIDs, *objectID)
	r.tombstones = append(r.tombstones, isTombstone)
	return r.err
}

func roundTripAObjectMap(t *testing.T, mapping *AObjectMap) *AObjectMap {
	t.Helper()
	encoded, err := json.Marshal(IterationContextJSON{
		AObjectMap: serializeAObjectMap(mapping),
	})
	require.NoError(t, err)
	var decoded IterationContextJSON
	require.NoError(t, json.Unmarshal(encoded, &decoded))
	restored, err := restoreAObjectMap(context.Background(), decoded.AObjectMap)
	require.NoError(t, err)
	return restored
}

func TestApplyObjectsSubmitsNonAppendableTombstoneSpills(t *testing.T) {
	firstID := objectio.NewObjectid()
	secondID := objectio.NewObjectid()
	first := *objectio.NewObjectStatsWithObjectID(
		&firstID, false, true, true)
	second := *objectio.NewObjectStatsWithObjectID(
		&secondID, false, true, true)
	var upstreamID objectio.ObjectId
	upstreamID[0] = 1
	upstreamStats := *objectio.NewObjectStatsWithObjectID(
		&upstreamID, false, true, false)
	objects := map[objectio.ObjectId]*ObjectWithTableInfo{
		upstreamID: {
			Stats:       upstreamStats,
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
		worker, nil, nil, "account", "publication", nil, NewAObjectMap(), nil,
	)
	require.ErrorContains(t, err, "engine is nil",
		"a non-empty spill list must reach tombstone submission")
}

func TestApplyObjectsTracksNonAppendableTombstoneSpillsAcrossIterations(t *testing.T) {
	ctrl := gomock.NewController(t)
	mp, err := mpool.NewMPool("ccpr-nonappendable-tombstone-map", 0, mpool.NoFixed)
	require.NoError(t, err)

	upstreamID := objectio.NewObjectid()
	upstreamStats := *objectio.NewObjectStatsWithObjectID(
		&upstreamID, false, true, false)
	firstID := objectio.NewObjectid()
	secondID := objectio.NewObjectid()
	first := *objectio.NewObjectStatsWithObjectID(
		&firstID, false, true, true)
	second := *objectio.NewObjectStatsWithObjectID(
		&secondID, false, true, true)

	insertRelation := mock_frontend.NewMockRelation(ctrl)
	insertRelation.EXPECT().GetTableDef(gomock.Any()).Return(nil)
	var insertedIDs []objectio.ObjectId
	insertRelation.EXPECT().Delete(gomock.Any(), gomock.Any(), "").DoAndReturn(
		func(_ context.Context, bat *batch.Batch, _ string) error {
			for i := 0; i < bat.RowCount(); i++ {
				stats := objectio.ObjectStats(bat.Vecs[0].GetBytesAt(i))
				insertedIDs = append(insertedIDs, *stats.ObjectName().ObjectId())
			}
			return nil
		},
	)
	insertDB := mock_frontend.NewMockDatabase(ctrl)
	insertDB.EXPECT().Relation(gomock.Any(), "table", nil).Return(insertRelation, nil)
	insertEngine := mock_frontend.NewMockEngine(ctrl)
	insertEngine.EXPECT().Database(gomock.Any(), "db", nil).Return(insertDB, nil)

	mapping := NewAObjectMap()
	worker := &presetFilterObjectWorker{result: &FilterObjectJobResult{
		DownstreamStatsList: []objectio.ObjectStats{first, {}, second},
	}}
	createObjects := map[objectio.ObjectId]*ObjectWithTableInfo{
		upstreamID: {
			Stats:       upstreamStats,
			IsTombstone: true,
			DBName:      "db",
			TableName:   "table",
		},
	}
	require.NoError(t, ApplyObjects(
		context.Background(), "task", 0, nil, createObjects,
		nil, nil, types.TS{}, nil, insertEngine, mp, nil,
		worker, nil, nil, "account", "publication", nil, mapping, nil,
	))
	require.Equal(t, []objectio.ObjectId{firstID, secondID}, insertedIDs)

	restored := roundTripAObjectMap(t, mapping)
	stored, ok := restored.Get(upstreamID.String())
	require.True(t, ok)
	require.NotNil(t, stored.DownstreamObjectIDs)
	require.Equal(t, []objectio.ObjectId{firstID, secondID}, *stored.DownstreamObjectIDs)

	deleteRelationMock := mock_frontend.NewMockRelation(ctrl)
	deleteRelationMock.EXPECT().GetTableDef(gomock.Any()).Return(nil)
	deleteRelation := &recordingSoftDeleteRelation{Relation: deleteRelationMock}
	deleteDB := mock_frontend.NewMockDatabase(ctrl)
	deleteDB.EXPECT().Relation(gomock.Any(), "table", nil).Return(deleteRelation, nil)
	deleteEngine := mock_frontend.NewMockEngine(ctrl)
	deleteEngine.EXPECT().Database(gomock.Any(), "db", nil).Return(deleteDB, nil)
	deleteObjects := map[objectio.ObjectId]*ObjectWithTableInfo{
		upstreamID: {
			Stats:       upstreamStats,
			IsTombstone: true,
			Delete:      true,
			DBName:      "db",
			TableName:   "table",
		},
	}
	require.NoError(t, ApplyObjects(
		context.Background(), "task", 0, nil, deleteObjects,
		nil, nil, types.TS{}, nil, deleteEngine, mp, nil,
		nil, nil, nil, "account", "publication", nil, restored, nil,
	))
	require.Equal(t, []objectio.ObjectId{firstID, secondID}, deleteRelation.objectIDs)
	require.Equal(t, []bool{true, true}, deleteRelation.tombstones)
	_, ok = restored.Get(upstreamID.String())
	require.False(t, ok)
	require.Zero(t, mp.CurrNB())
}

func TestApplyObjectsTracksEmptyNonAppendableTombstoneRewrite(t *testing.T) {
	upstreamID := objectio.NewObjectid()
	upstreamStats := *objectio.NewObjectStatsWithObjectID(
		&upstreamID, false, true, false)
	mapping := NewAObjectMap()
	worker := &presetFilterObjectWorker{result: &FilterObjectJobResult{}}

	require.NoError(t, ApplyObjects(
		context.Background(), "task", 0, nil,
		map[objectio.ObjectId]*ObjectWithTableInfo{
			upstreamID: {
				Stats:       upstreamStats,
				IsTombstone: true,
				DBName:      "db",
				TableName:   "table",
			},
		},
		nil, nil, types.TS{}, nil, nil, nil, nil,
		worker, nil, nil, "account", "publication", nil, mapping, nil,
	))
	stored, ok := mapping.Get(upstreamID.String())
	require.True(t, ok)
	require.NotNil(t, stored.DownstreamObjectIDs)
	require.Empty(t, *stored.DownstreamObjectIDs)

	mapping = roundTripAObjectMap(t, mapping)
	stored, ok = mapping.Get(upstreamID.String())
	require.True(t, ok)
	require.NotNil(t, stored.DownstreamObjectIDs)
	require.Empty(t, *stored.DownstreamObjectIDs)

	require.NoError(t, ApplyObjects(
		context.Background(), "task", 0, nil,
		map[objectio.ObjectId]*ObjectWithTableInfo{
			upstreamID: {
				Stats:       upstreamStats,
				IsTombstone: true,
				Delete:      true,
				DBName:      "db",
				TableName:   "table",
			},
		},
		nil, nil, types.TS{}, nil, nil, nil, nil,
		nil, nil, nil, "account", "publication", nil, mapping, nil,
	))
	_, ok = mapping.Get(upstreamID.String())
	require.False(t, ok)
}

func TestApplyObjectsRequiresNonAppendableTombstoneMappingOwner(t *testing.T) {
	upstreamID := objectio.NewObjectid()
	upstreamStats := *objectio.NewObjectStatsWithObjectID(
		&upstreamID, false, true, false)
	downstreamID := objectio.NewObjectid()
	downstreamStats := *objectio.NewObjectStatsWithObjectID(
		&downstreamID, false, true, true)
	worker := &presetFilterObjectWorker{result: &FilterObjectJobResult{
		DownstreamStatsList: []objectio.ObjectStats{downstreamStats},
	}}

	err := ApplyObjects(
		context.Background(), "task", 0, nil,
		map[objectio.ObjectId]*ObjectWithTableInfo{
			upstreamID: {
				Stats:       upstreamStats,
				IsTombstone: true,
				DBName:      "db",
				TableName:   "table",
			},
		},
		nil, nil, types.TS{}, nil, nil, nil, nil,
		worker, nil, nil, "account", "publication", nil, nil, nil,
	)
	require.ErrorContains(t, err, "mapping owner is required")

	err = ApplyObjects(
		context.Background(), "task", 0, nil,
		map[objectio.ObjectId]*ObjectWithTableInfo{
			upstreamID: {
				Stats:       upstreamStats,
				IsTombstone: true,
				Delete:      true,
				DBName:      "db",
				TableName:   "table",
			},
		},
		nil, nil, types.TS{}, nil, nil, nil, nil,
		nil, nil, nil, "account", "publication", nil, nil, nil,
	)
	require.ErrorContains(t, err, "mapping owner is required")
}

func TestApplyObjectsRetainsNonAppendableTombstoneMappingOnDeleteFailure(t *testing.T) {
	ctrl := gomock.NewController(t)
	upstreamID := objectio.NewObjectid()
	upstreamStats := *objectio.NewObjectStatsWithObjectID(
		&upstreamID, false, true, false)
	downstreamID := objectio.NewObjectid()
	downstreamIDs := []objectio.ObjectId{downstreamID}
	mapping := NewAObjectMap()
	mapping.Set(upstreamID.String(), &AObjectMapping{
		DownstreamObjectIDs: &downstreamIDs,
		IsTombstone:         true,
		DBName:              "db",
		TableName:           "table",
	})

	deleteRelationMock := mock_frontend.NewMockRelation(ctrl)
	deleteRelationMock.EXPECT().GetTableDef(gomock.Any()).Return(nil)
	deleteRelation := &recordingSoftDeleteRelation{
		Relation: deleteRelationMock,
		err:      errors.New("delete failed"),
	}
	deleteDB := mock_frontend.NewMockDatabase(ctrl)
	deleteDB.EXPECT().Relation(gomock.Any(), "table", nil).Return(deleteRelation, nil)
	deleteEngine := mock_frontend.NewMockEngine(ctrl)
	deleteEngine.EXPECT().Database(gomock.Any(), "db", nil).Return(deleteDB, nil)

	err := ApplyObjects(
		context.Background(), "task", 0, nil,
		map[objectio.ObjectId]*ObjectWithTableInfo{
			upstreamID: {
				Stats:       upstreamStats,
				IsTombstone: true,
				Delete:      true,
				DBName:      "db",
				TableName:   "table",
			},
		},
		nil, nil, types.TS{}, nil, deleteEngine, nil, nil,
		nil, nil, nil, "account", "publication", nil, mapping, nil,
	)
	require.ErrorContains(t, err, "delete failed")
	stored, ok := mapping.Get(upstreamID.String())
	require.True(t, ok)
	require.NotNil(t, stored.DownstreamObjectIDs)
	require.Equal(t, []objectio.ObjectId{downstreamID}, *stored.DownstreamObjectIDs)
}

func TestApplyObjectsRejectsWrongNonAppendableTombstoneMappingKind(t *testing.T) {
	upstreamID := objectio.NewObjectid()
	upstreamStats := *objectio.NewObjectStatsWithObjectID(
		&upstreamID, false, true, false)
	appendableDownstreamID := objectio.NewObjectid()
	mapping := NewAObjectMap()
	mapping.Set(upstreamID.String(), &AObjectMapping{
		DownstreamStats: *objectio.NewObjectStatsWithObjectID(
			&appendableDownstreamID, true, true, true),
		IsTombstone: true,
		DBName:      "db",
		TableName:   "table",
	})

	err := ApplyObjects(
		context.Background(), "task", 0, nil,
		map[objectio.ObjectId]*ObjectWithTableInfo{
			upstreamID: {
				Stats:       upstreamStats,
				IsTombstone: true,
				Delete:      true,
				DBName:      "db",
				TableName:   "table",
			},
		},
		nil, nil, types.TS{}, nil, nil, nil, nil,
		nil, nil, nil, "account", "publication", nil, mapping, nil,
	)
	require.ErrorContains(t, err, "is not owned by a non-appendable tombstone")
	_, ok := mapping.Get(upstreamID.String())
	require.True(t, ok)
}
