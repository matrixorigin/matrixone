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

	"github.com/matrixorigin/matrixone/pkg/common/mpool"
	"github.com/matrixorigin/matrixone/pkg/container/batch"
	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/container/vector"
	"github.com/matrixorigin/matrixone/pkg/objectio"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// ---- tombstoneFSinkerWithName ----

func TestTombstoneFSinkerWithName_SyncNilWriter(t *testing.T) {
	s := &tombstoneFSinkerWithName{}
	stats, err := s.Sync(context.Background())
	assert.NoError(t, err)
	assert.Nil(t, stats)
}

func TestTombstoneFSinkerWithName_ResetNilWriter(t *testing.T) {
	s := &tombstoneFSinkerWithName{}
	s.Reset() // should not panic
}

func TestTombstoneFSinkerWithName_Close(t *testing.T) {
	s := &tombstoneFSinkerWithName{}
	err := s.Close()
	assert.NoError(t, err)
}

// ---- newTombstoneFSinkerFactoryWithName ----

func TestNewTombstoneFSinkerFactoryWithName(t *testing.T) {
	mp, err := mpool.NewMPool("test", 0, mpool.NoFixed)
	require.NoError(t, err)
	defer mp.Free(nil)

	segid := objectio.NewSegmentid()
	objName := objectio.BuildObjectName(segid, 0)
	factory := newTombstoneFSinkerFactoryWithName(objName, objectio.HiddenColumnSelection_None)
	assert.NotNil(t, factory)

	sinker := factory(mp, nil)
	assert.NotNil(t, sinker)

	ts, ok := sinker.(*tombstoneFSinkerWithName)
	assert.True(t, ok)
	assert.Equal(t, objName, ts.objectName)
}

// ---- FilterObject TTL checker ----

func TestFilterObject_TTLExpired(t *testing.T) {
	ttlChecker := func() bool { return false }
	_, err := FilterObject(
		context.Background(),
		make([]byte, objectio.ObjectStatsLen),
		types.TS{}, nil, false, nil, nil, nil, nil, "", "", nil, nil, nil, ttlChecker,
	)
	assert.ErrorIs(t, err, ErrSyncProtectionTTLExpired)
}

func TestFilterObject_InvalidStatsLength(t *testing.T) {
	_, err := FilterObject(
		context.Background(),
		[]byte("short"),
		types.TS{}, nil, false, nil, nil, nil, nil, "", "", nil, nil, nil, nil,
	)
	assert.Error(t, err)
	assert.Contains(t, err.Error(), "invalid object stats length")
}

// ---- FilterObjectJob TTL expired ----

func TestFilterObjectJob_TTLExpired(t *testing.T) {
	job := NewFilterObjectJob(
		context.Background(),
		nil, types.TS{}, nil, false, nil, nil, nil, nil, "", "", nil, nil, nil,
		func() bool { return false },
	)
	job.Execute()
	result := job.WaitDone().(*FilterObjectJobResult)
	assert.ErrorIs(t, result.Err, ErrSyncProtectionTTLExpired)
}

// ---- rewriteTombstoneRowidsBatch with mapping but no RowOffsetMap ----

func TestRewriteTombstoneRowidsBatch_MappingWithoutRowOffsetMap(t *testing.T) {
	mp, err := mpool.NewMPool("test", 0, mpool.NoFixed)
	require.NoError(t, err)
	defer mp.Free(nil)

	upstreamObjID := types.NewObjectid()
	rid := types.NewRowIDWithObjectIDBlkNumAndRowID(upstreamObjID, 0, 42)

	rowidVec := vector.NewVec(types.T_Rowid.ToType())
	require.NoError(t, vector.AppendFixed(rowidVec, rid, false, mp))

	bat := &batch.Batch{Vecs: []*vector.Vector{rowidVec}}
	bat.SetRowCount(1)

	downstreamObjID := types.NewObjectid()
	var downstreamStats objectio.ObjectStats
	objectio.SetObjectStatsObjectName(&downstreamStats, objectio.BuildObjectNameWithObjectID(&downstreamObjID))

	amap := NewAObjectMap()
	amap.Set(upstreamObjID.String(), &AObjectMapping{
		DownstreamStats: downstreamStats,
		RowOffsetMap:    nil,
	})

	err = rewriteTombstoneRowidsBatch(context.Background(), bat, amap, mp)
	assert.NoError(t, err)

	rowids := vector.MustFixedColWithTypeCheck[types.Rowid](rowidVec)
	rewrittenObjID := rowids[0].BorrowObjectID()
	assert.Equal(t, downstreamObjID.Segment(), rewrittenObjID.Segment())
	assert.Equal(t, uint32(42), rowids[0].GetRowOffset())

	rowidVec.Free(mp)
}
