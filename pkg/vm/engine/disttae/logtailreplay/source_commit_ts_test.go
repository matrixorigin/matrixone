// Copyright 2026 Matrix Origin
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package logtailreplay

import (
	"context"
	"testing"

	"github.com/matrixorigin/matrixone/pkg/common/mpool"
	"github.com/matrixorigin/matrixone/pkg/container/batch"
	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/container/vector"
	"github.com/matrixorigin/matrixone/pkg/defines"
	"github.com/matrixorigin/matrixone/pkg/fileservice"
	"github.com/matrixorigin/matrixone/pkg/objectio"
	"github.com/matrixorigin/matrixone/pkg/objectio/ioutil"
	"github.com/matrixorigin/matrixone/pkg/testutil"
	"github.com/stretchr/testify/require"
)

// writeObjectWithCommitTS writes an in-memory object carrying a commit_ts column
// with the given per-row values and returns its entry plus the file service that
// holds it, so the commit-ts scan paths can be exercised against real data.
func writeObjectWithCommitTS(t *testing.T, mp *mpool.MPool, tsValues []types.TS) (objectio.ObjectEntry, fileservice.FileService) {
	t.Helper()
	fs, err := fileservice.NewMemoryFS(defines.SharedFileServiceName, fileservice.DisabledCacheConfig, nil)
	require.NoError(t, err)
	writer := ioutil.ConstructWriter(
		0, []uint16{0, objectio.SEQNUM_ROWID, objectio.SEQNUM_COMMITTS}, -1, false, false, fs,
	)
	bat := batch.NewWithSize(3)
	bat.Vecs[0] = vector.NewVec(types.T_int32.ToType())
	bat.Vecs[1] = vector.NewVec(types.T_Rowid.ToType())
	bat.Vecs[2] = vector.NewVec(types.T_TS.ToType())
	var blk types.Blockid
	for i, ts := range tsValues {
		require.NoError(t, vector.AppendFixed(bat.Vecs[0], int32(i), false, mp))
		require.NoError(t, vector.AppendFixed(bat.Vecs[1], types.NewRowid(&blk, uint32(i+1)), false, mp))
		require.NoError(t, vector.AppendFixed(bat.Vecs[2], ts, false, mp))
	}
	bat.SetRowCount(len(tsValues))
	_, err = writer.WriteBatch(bat)
	require.NoError(t, err)
	blocks, _, err := writer.Sync(context.Background())
	require.NoError(t, err)
	stats := writer.Stats()
	require.NoError(t, objectio.SetObjectStatsBlkCnt(&stats, uint32(len(blocks))))
	return objectio.ObjectEntry{ObjectStats: stats, CreateTime: types.BuildTS(50, 0)}, fs
}

func TestMaxCommitTSFromZonemapReadsMax(t *testing.T) {
	mp := mpool.MustNewZero()
	defer mpool.DeleteMPool(mp)
	obj, fs := writeObjectWithCommitTS(t, mp, []types.TS{types.BuildTS(10, 0), types.BuildTS(20, 0), types.BuildTS(15, 0)})
	got, err := maxCommitTSFromZonemap(context.Background(), fs, obj)
	require.NoError(t, err)
	require.Equal(t, types.BuildTS(20, 0), got)
}

func TestMaxCommitTSInAppendableObjectReadsMax(t *testing.T) {
	mp := mpool.MustNewZero()
	defer mpool.DeleteMPool(mp)
	obj, fs := writeObjectWithCommitTS(t, mp, []types.TS{types.BuildTS(10, 0), types.BuildTS(25, 0)})
	got, err := maxCommitTSInAppendableObject(context.Background(), types.BuildTS(100, 0), fs, obj, mp)
	require.NoError(t, err)
	require.Equal(t, types.BuildTS(25, 0), got)
}

// SourceCommitTSAt reads a TN object's per-row commit_ts zonemap (20 here), not
// its CreateTime (30 here). commit_ts is the original user commit that flush and
// merge copy verbatim; CreateTime is the flush/merge time and advances on every
// merge. Reading commit_ts avoids two hazards: under-reporting rows that survive
// only in a TN object after an appendable flush (would fall back to the retention
// boundary and let a not-yet-indexed row pass), and over-reporting on merge (the
// bound stays at the last user DML, so an idle-but-merged table stays coverable
// instead of failing the watermark check forever).
func TestSourceCommitTSAtTNObjectPreservesCommitTS(t *testing.T) {
	mp := mpool.MustNewZero()
	defer mpool.DeleteMPool(mp)
	obj, fs := writeObjectWithCommitTS(t, mp, []types.TS{types.BuildTS(10, 0), types.BuildTS(20, 0)})
	objectio.SetObjectStatsAppendable(&obj.ObjectStats, false)
	require.False(t, obj.GetAppendable())
	require.False(t, obj.GetCNCreated())

	state := NewPartitionState("", false, 42, false)
	state.UpdateDuration(types.BuildTS(5, 0), types.MaxTs())
	obj.CreateTime = types.BuildTS(30, 0)
	state.dataObjectsNameIndex.Set(obj)

	info, err := state.SourceCommitTSAt(context.Background(), types.BuildTS(40, 0), fs, mp)
	require.NoError(t, err)
	require.Equal(t, types.BuildTS(20, 0), info.TNObject)
	require.Equal(t, types.BuildTS(20, 0), info.Max())
}

// An appendable object visible at the snapshot may hold blocks flushed after it.
// Rows committed after the snapshot must be excluded so the coverage bound is not
// inflated above the snapshot (which would needlessly decline a covered probe).
func TestMaxCommitTSInAppendableObjectRespectsSnapshot(t *testing.T) {
	mp := mpool.MustNewZero()
	defer mpool.DeleteMPool(mp)
	obj, fs := writeObjectWithCommitTS(t, mp, []types.TS{types.BuildTS(10, 0), types.BuildTS(30, 0)})
	got, err := maxCommitTSInAppendableObject(context.Background(), types.BuildTS(20, 0), fs, obj, mp)
	require.NoError(t, err)
	require.Equal(t, types.BuildTS(10, 0), got)
}

func TestSourceCommitTSMaxPicksTNObject(t *testing.T) {
	info := SourceCommitTS{
		StateStart: types.BuildTS(5, 0),
		InMemory:   types.BuildTS(10, 0),
		Appendable: types.BuildTS(20, 0),
		CNCreated:  types.BuildTS(30, 0),
		TNObject:   types.BuildTS(40, 0),
	}
	require.Equal(t, types.BuildTS(40, 0), info.Max())
}

func TestSourceCommitTSMaxEmptyIsZero(t *testing.T) {
	m := (SourceCommitTS{}).Max()
	require.True(t, m.IsEmpty())
}

// In-memory rows contribute their commit ts; rows beyond the snapshot are excluded.
func TestSourceCommitTSAtInMemory(t *testing.T) {
	state := NewPartitionState("", false, 42, false)
	state.UpdateDuration(types.BuildTS(1, 0), types.MaxTs())
	objID := objectio.NewObjectid()
	rid := types.NewRowIDWithObjectIDBlkNumAndRowID(objID, 0, 0)
	state.rows.Set(&RowEntry{BlockID: rid.CloneBlockID(), RowID: rid, Time: types.BuildTS(50, 0)})
	rid2 := types.NewRowIDWithObjectIDBlkNumAndRowID(objID, 0, 1)
	state.rows.Set(&RowEntry{BlockID: rid2.CloneBlockID(), RowID: rid2, Time: types.BuildTS(300, 0)})

	info, err := state.SourceCommitTSAt(context.Background(), types.BuildTS(200, 1), nil, nil)
	require.NoError(t, err)
	require.Equal(t, types.BuildTS(50, 0), info.InMemory)
	require.Equal(t, types.BuildTS(50, 0), info.Max())
}

// A CN-created non-appendable object carries its data commit on CreateTime.
func TestSourceCommitTSAtCNCreated(t *testing.T) {
	state := NewPartitionState("", false, 42, false)
	state.UpdateDuration(types.BuildTS(1, 0), types.MaxTs())
	id := objectio.NewObjectid()
	stats := objectio.NewObjectStatsWithObjectID(&id, false, false, true)
	require.NoError(t, objectio.SetObjectStatsRowCnt(stats, 10))
	state.dataObjectsNameIndex.Set(objectio.ObjectEntry{
		ObjectStats: *stats,
		CreateTime:  types.BuildTS(60, 0),
		DeleteTime:  types.TS{},
	})

	info, err := state.SourceCommitTSAt(context.Background(), types.BuildTS(200, 1), nil, nil)
	require.NoError(t, err)
	require.Equal(t, types.BuildTS(60, 0), info.CNCreated)
	require.Equal(t, types.BuildTS(60, 0), info.Max())
}

// A retention boundary floors the bound even when no object/row survives locally.
func TestSourceCommitTSAtStateStartFloor(t *testing.T) {
	state := NewPartitionState("", false, 42, false)
	state.UpdateDuration(types.BuildTS(100, 1), types.MaxTs())
	info, err := state.SourceCommitTSAt(context.Background(), types.BuildTS(200, 1), nil, nil)
	require.NoError(t, err)
	require.Equal(t, types.BuildTS(100, 1), info.StateStart)
	require.Equal(t, types.BuildTS(100, 1), info.Max())
}

// A TN non-appendable object requires reading its commit_ts zonemap. When the
// object meta cannot be loaded the scan must fail closed rather than skip the
// object and lower the bound (which would let a not-yet-indexed row pass).
func TestSourceCommitTSAtTNObjectFailsClosed(t *testing.T) {
	ctx := context.Background()
	mp := mpool.MustNewZero()
	defer mpool.DeleteMPool(mp)
	fs := testutil.NewSharedFS()
	state := NewPartitionState("", false, 42, false)
	state.UpdateDuration(types.BuildTS(1, 0), types.MaxTs())
	id := objectio.NewObjectid()
	stats := objectio.NewObjectStatsWithObjectID(&id, false, false, false)
	require.NoError(t, objectio.SetObjectStatsRowCnt(stats, 10))
	state.dataObjectsNameIndex.Set(objectio.ObjectEntry{
		ObjectStats: *stats,
		CreateTime:  types.BuildTS(60, 0),
		DeleteTime:  types.TS{},
	})

	_, err := state.SourceCommitTSAt(ctx, types.BuildTS(200, 1), fs, mp)
	require.Error(t, err)
}

func TestMaxCommitTSFromZonemapRequiresFS(t *testing.T) {
	_, err := maxCommitTSFromZonemap(context.Background(), nil, objectio.ObjectEntry{})
	require.Error(t, err)
}

func TestMaxCommitTSInAppendableObjectRequiresFS(t *testing.T) {
	_, err := maxCommitTSInAppendableObject(context.Background(), types.TS{}, nil, objectio.ObjectEntry{}, nil)
	require.Error(t, err)
}
