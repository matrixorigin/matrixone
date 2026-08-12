// Copyright 2021 Matrix Origin
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

package logtail

import (
	"context"
	"testing"

	"github.com/matrixorigin/matrixone/pkg/container/batch"
	"github.com/matrixorigin/matrixone/pkg/container/nulls"
	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/container/vector"
	"github.com/matrixorigin/matrixone/pkg/objectio"
	"github.com/matrixorigin/matrixone/pkg/objectio/ioutil"
	"github.com/matrixorigin/matrixone/pkg/testutil"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/ckputil"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/catalog"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/common"
	"github.com/stretchr/testify/require"
)

func TestCanonicalizeBackupTombstone(t *testing.T) {
	for _, test := range []struct {
		name       string
		layout     backupSpecialColumnLayout
		pkType     types.T
		newBatch   func() *batch.Batch
		resultRows int
		cutoff     types.TS
	}{
		{
			name:       "commit timestamp only",
			layout:     newBackupTombstoneLayout(2, invalidBackupSpecialColumnPosition, invalidBackupSpecialColumnPosition),
			pkType:     types.T_int64,
			resultRows: 2,
			cutoff:     types.BuildTS(15, 0),
			newBatch: func() *batch.Batch {
				return newBackupTombstoneTestBatch(t, types.T_int64, []types.T{types.T_TS})
			},
		},
		{
			name:       "commit timestamp and abort with timestamp pk",
			layout:     newBackupTombstoneLayout(2, invalidBackupSpecialColumnPosition, 3),
			pkType:     types.T_TS,
			resultRows: 1,
			cutoff:     types.BuildTS(15, 0),
			newBatch: func() *batch.Batch {
				return newBackupTombstoneTestBatch(t, types.T_TS, []types.T{types.T_TS, types.T_bool})
			},
		},
		{
			name:       "physical address before commit timestamp",
			layout:     newBackupTombstoneLayout(3, 2, 4),
			pkType:     types.T_int64,
			resultRows: 1,
			cutoff:     types.BuildTS(15, 0),
			newBatch: func() *batch.Batch {
				return newBackupTombstoneTestBatch(t, types.T_int64, []types.T{types.T_Rowid, types.T_TS, types.T_bool})
			},
		},
		{
			name:       "physical address after commit timestamp",
			layout:     newBackupTombstoneLayout(2, 3, 4),
			pkType:     types.T_int64,
			resultRows: 1,
			cutoff:     types.BuildTS(15, 0),
			newBatch: func() *batch.Batch {
				return newBackupTombstoneTestBatch(t, types.T_int64, []types.T{types.T_TS, types.T_Rowid, types.T_bool})
			},
		},
		{
			name:       "all rows filtered",
			layout:     newBackupTombstoneLayout(2, invalidBackupSpecialColumnPosition, 3),
			pkType:     types.T_int64,
			resultRows: 0,
			cutoff:     types.BuildTS(5, 0),
			newBatch: func() *batch.Batch {
				return newBackupTombstoneTestBatch(t, types.T_int64, []types.T{types.T_TS, types.T_bool})
			},
		},
	} {
		t.Run(test.name, func(t *testing.T) {
			input := test.newBatch()
			defer input.Clean(common.DebugAllocator)
			visibleRows, err := visibleAppendableRows(
				context.Background(), input, test.layout, &test.cutoff)
			require.NoError(t, err)
			input.Shrink(visibleRows, false)
			result, err := canonicalizeBackupTombstone(context.Background(), input, test.layout)
			require.NoError(t, err)
			defer result.Clean(common.DebugAllocator)

			require.Len(t, result.Vecs, len(objectio.TombstoneSeqnums_DN_Created))
			require.Equal(t, test.resultRows, result.RowCount())
			require.Equal(t, types.T_Rowid, result.Vecs[objectio.TombstoneAttr_Rowid_Idx].GetType().Oid)
			require.Equal(t, test.pkType, result.Vecs[objectio.TombstoneAttr_PK_Idx].GetType().Oid)
			require.Equal(t, types.T_TS, result.Vecs[objectio.TombstoneAttr_NA_CommitTs_Idx].GetType().Oid)
			if test.resultRows > 0 {
				require.Equal(t, types.BuildTS(10, 0), vector.GetFixedAtWithTypeCheck[types.TS](
					result.Vecs[objectio.TombstoneAttr_NA_CommitTs_Idx], 0))
			}
		})
	}
}

func TestVisibleAppendableRowsBroadcastsConstantCommitTS(t *testing.T) {
	input := newBackupTombstoneTestBatch(t, types.T_int64, []types.T{types.T_TS})
	input.Vecs[2].Free(common.DebugAllocator)
	commitTS := types.BuildTS(10, 0)
	var err error
	input.Vecs[2], err = vector.NewConstFixed(
		types.T_TS.ToType(), commitTS, input.RowCount(), common.DebugAllocator)
	require.NoError(t, err)
	defer input.Clean(common.DebugAllocator)

	layout := newBackupTombstoneLayout(
		2,
		invalidBackupSpecialColumnPosition,
		invalidBackupSpecialColumnPosition,
	)
	visibleRows, err := visibleAppendableRows(
		context.Background(), input, layout, ptrTo(types.BuildTS(15, 0)),
	)
	require.NoError(t, err)
	require.Equal(t, []int64{0, 1, 2}, visibleRows)

	input.Shrink(visibleRows, false)
	result, err := canonicalizeBackupTombstone(context.Background(), input, layout)
	require.NoError(t, err)
	defer result.Clean(common.DebugAllocator)
	require.Equal(t, 3, result.RowCount())
	for row := range result.RowCount() {
		require.Equal(t, commitTS, vector.GetFixedAtNoTypeCheck[types.TS](
			result.Vecs[objectio.TombstoneAttr_NA_CommitTs_Idx], row))
	}
}

func TestBackupTombstoneValidationRejectsMalformedColumns(t *testing.T) {
	tests := []struct {
		name   string
		layout backupSpecialColumnLayout
		mutate func(*batch.Batch)
	}{
		{
			name:   "missing commit timestamp metadata",
			layout: newBackupTombstoneLayout(invalidBackupSpecialColumnPosition, invalidBackupSpecialColumnPosition, invalidBackupSpecialColumnPosition),
		},
		{
			name:   "short commit timestamp vector",
			layout: newBackupTombstoneLayout(2, invalidBackupSpecialColumnPosition, 3),
			mutate: func(bat *batch.Batch) {
				bat.Vecs[2].SetLength(2)
			},
		},
		{
			name:   "wrong commit timestamp type",
			layout: newBackupTombstoneLayout(3, invalidBackupSpecialColumnPosition, 2),
		},
		{
			name:   "short abort vector",
			layout: newBackupTombstoneLayout(2, invalidBackupSpecialColumnPosition, 3),
			mutate: func(bat *batch.Batch) {
				bat.Vecs[3].SetLength(2)
			},
		},
		{
			name:   "null commit timestamp",
			layout: newBackupTombstoneLayout(2, invalidBackupSpecialColumnPosition, 3),
			mutate: func(bat *batch.Batch) {
				nulls.Add(bat.Vecs[2].GetNulls(), 1)
			},
		},
		{
			name:   "null abort flag",
			layout: newBackupTombstoneLayout(2, invalidBackupSpecialColumnPosition, 3),
			mutate: func(bat *batch.Batch) {
				nulls.Add(bat.Vecs[3].GetNulls(), 1)
			},
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			input := newBackupTombstoneTestBatch(t, types.T_int64, []types.T{types.T_TS, types.T_bool})
			defer input.Clean(common.DebugAllocator)
			if test.mutate != nil {
				test.mutate(input)
			}

			_, err := visibleAppendableRows(context.Background(), input, test.layout, nil)
			require.Error(t, err)
		})
	}
}

func TestBackupTombstoneWriterPreservesHiddenCommitTS(t *testing.T) {
	ctx := context.Background()
	fs := testutil.NewSharedFS()

	input := newBackupTombstoneTestBatch(t, types.T_int64, []types.T{types.T_Rowid, types.T_TS, types.T_bool})
	defer input.Clean(common.DebugAllocator)
	layout := newBackupTombstoneLayout(3, 2, 4)
	visibleRows, err := visibleAppendableRows(ctx, input, layout, ptrTo(types.BuildTS(15, 0)))
	require.NoError(t, err)
	require.Equal(t, []int64{0}, visibleRows)
	input.Shrink(visibleRows, false)

	output, err := canonicalizeBackupTombstone(ctx, input, layout)
	require.NoError(t, err)
	defer output.Clean(common.DebugAllocator)

	objectID := objectio.NewObjectid()
	name := objectio.BuildObjectNameWithObjectID(&objectID)
	writer, err := ioutil.NewBlockWriterNew(
		fs,
		name,
		0,
		objectio.TombstoneSeqnums_DN_Created,
		true,
	)
	require.NoError(t, err)
	_, err = writer.WriteBatch(output)
	require.NoError(t, err)
	blocks, extent, err := writer.Sync(ctx)
	require.NoError(t, err)
	require.Len(t, blocks, 1)

	location := objectio.BuildLocation(name, extent, blocks[0].GetRows(), blocks[0].GetID())
	meta, err := objectio.FastLoadObjectMeta(ctx, &location, false, fs)
	require.NoError(t, err)
	blockMeta := meta.MustDataMeta().GetBlockMeta(uint32(location.ID()))
	require.Equal(t, uint16(1), blockMeta.GetMaxSeqnum())
	require.Equal(t, uint16(3), blockMeta.GetMetaColumnCount())
	writtenLayout := resolveBackupSpecialColumnLayout(blockMeta)
	require.Equal(t, uint16(2), writtenLayout.CommitTS)
	require.Equal(t, uint16(invalidBackupSpecialColumnPosition), writtenLayout.Abort)
	stats := writer.GetObjectStats()
	require.False(t, stats.GetAppendable())
	require.False(t, stats.GetCNCreated())

	rowID := vector.GetFixedAtWithTypeCheck[types.Rowid](output.Vecs[0], 0)
	deleted, err := ioutil.IsRowDeletedByLocation(
		ctx,
		ptrTo(types.BuildTS(15, 0)),
		&rowID,
		location,
		fs,
		false,
	)
	require.NoError(t, err)
	require.True(t, deleted)
}

func TestRewriteCheckpointCanonicalizesBackupTombstone(t *testing.T) {
	ctx := context.Background()
	srcFS := testutil.NewSharedFS()
	dstFS := testutil.NewSharedFS()

	sourceBatch := newBackupTombstoneTestBatch(
		t,
		types.T_int64,
		[]types.T{types.T_TS, types.T_Rowid},
	)
	defer sourceBatch.Clean(common.DebugAllocator)
	objectID := objectio.NewObjectid()
	sourceName := objectio.BuildObjectNameWithObjectID(&objectID)
	sourceWriter, err := ioutil.NewBlockWriterNew(
		srcFS,
		sourceName,
		0,
		objectio.TombstoneSeqnums_DN_Created_PhyAddr,
		true,
	)
	require.NoError(t, err)
	sourceWriter.SetAppendable()
	_, err = sourceWriter.WriteBatch(sourceBatch)
	require.NoError(t, err)
	_, _, err = sourceWriter.Sync(ctx)
	require.NoError(t, err)
	sourceStats := sourceWriter.GetObjectStats(objectio.WithAppendable())

	cata := catalog.MockCatalog(nil)
	defer cata.Close()
	db, err := cata.CreateDBEntry("backup_test", "", "", nil)
	require.NoError(t, err)
	table, err := db.CreateTableEntry(catalog.MockSchema(2, 0), nil, nil)
	require.NoError(t, err)
	createTS := types.BuildTS(5, 0)
	backupTS := types.BuildTS(15, 0)
	dropTS := types.BuildTS(30, 0)
	sourceEntry, err := table.CreateCommittedObject(
		createTS,
		&objectio.CreateObjOpt{Stats: &sourceStats, IsTombstone: true},
		nil,
	)
	require.NoError(t, err)
	catalog.MockDroppedObjectEntry2List(sourceEntry, dropTS)

	collector := NewBaseCollector_V2(backupTS, dropTS, 0, srcFS)
	defer collector.Close()
	require.NoError(t, collector.Collect(cata))
	checkpointData := collector.OrphanData()
	defer checkpointData.Close()
	checkpointLocation, _, err := checkpointData.Sync(ctx, srcFS)
	require.NoError(t, err)

	lastReader, err := GetCheckpointReader(
		ctx,
		"backup-test",
		srcFS,
		checkpointLocation,
		CheckpointCurrentVersion,
	)
	require.NoError(t, err)
	rewrittenLocation, _, _, err := ReWriteCheckpointAndBlockFromKey(
		ctx,
		"backup-test",
		srcFS,
		dstFS,
		checkpointLocation,
		lastReader,
		CheckpointCurrentVersion,
		backupTS,
	)
	require.NoError(t, err)

	rewrittenReader, err := GetCheckpointReader(
		ctx,
		"backup-test",
		dstFS,
		rewrittenLocation,
		CheckpointCurrentVersion,
	)
	require.NoError(t, err)
	var rewrittenStats objectio.ObjectStats
	err = rewrittenReader.ForEachRow(
		ctx,
		func(
			_ uint32,
			_, _ uint64,
			objectType int8,
			stats objectio.ObjectStats,
			_ types.TS,
			deleteTS types.TS,
			_ types.Rowid,
		) error {
			if objectType == ckputil.ObjectType_Tombstone &&
				deleteTS.IsEmpty() &&
				stats.ObjectName().String() != sourceName.String() {
				rewrittenStats = stats
			}
			return nil
		},
	)
	require.NoError(t, err)
	require.False(t, rewrittenStats.IsZero())
	require.False(t, rewrittenStats.GetAppendable())
	require.False(t, rewrittenStats.GetCNCreated())
	require.True(t, rewrittenStats.GetSorted())
	require.Equal(t, uint32(2), rewrittenStats.Rows())

	rewrittenObjectLocation := rewrittenStats.ObjectLocation()
	rewrittenMeta, err := objectio.FastLoadObjectMeta(ctx, &rewrittenObjectLocation, false, dstFS)
	require.NoError(t, err)
	rewrittenBlockMeta := rewrittenMeta.MustDataMeta().GetBlockMeta(uint32(rewrittenObjectLocation.ID()))
	require.Equal(t, uint16(1), rewrittenBlockMeta.GetMaxSeqnum())
	require.Equal(t, uint16(3), rewrittenBlockMeta.GetMetaColumnCount())
	rewrittenLayout := resolveBackupSpecialColumnLayout(rewrittenBlockMeta)
	require.Equal(t, uint16(2), rewrittenLayout.CommitTS)
	require.Equal(t, uint16(invalidBackupSpecialColumnPosition), rewrittenLayout.PhysicalAddr)
	require.Equal(t, uint16(invalidBackupSpecialColumnPosition), rewrittenLayout.Abort)

	var targetBlock types.Blockid
	for row, expected := range []bool{true, true, false} {
		rowID := types.NewRowid(&targetBlock, uint32(row))
		deleted, err := ioutil.IsRowDeletedByLocation(
			ctx,
			&backupTS,
			&rowID,
			rewrittenObjectLocation,
			dstFS,
			false,
		)
		require.NoError(t, err)
		require.Equal(t, expected, deleted)
	}
}

func newBackupTombstoneTestBatch(t *testing.T, pkType types.T, trailingTypes []types.T) *batch.Batch {
	t.Helper()

	bat := batch.NewWithSize(len(objectio.TombstoneSeqnums_CN_Created) + len(trailingTypes))
	bat.Vecs[0] = vector.NewVec(types.T_Rowid.ToType())
	bat.Vecs[1] = vector.NewVec(pkType.ToType())
	for pos, typ := range trailingTypes {
		bat.Vecs[len(objectio.TombstoneSeqnums_CN_Created)+pos] = vector.NewVec(typ.ToType())
	}

	var deletedBlock, physicalBlock types.Blockid
	for row := range 3 {
		require.NoError(t, vector.AppendFixed(
			bat.Vecs[0], types.NewRowid(&deletedBlock, uint32(row)), false, common.DebugAllocator))
		switch pkType {
		case types.T_int64:
			require.NoError(t, vector.AppendFixed(
				bat.Vecs[1], int64(row+1), false, common.DebugAllocator))
		case types.T_TS:
			require.NoError(t, vector.AppendFixed(
				bat.Vecs[1], types.BuildTS(int64(row+1), 0), false, common.DebugAllocator))
		default:
			t.Fatalf("unsupported test primary-key type %s", pkType.String())
		}
		for trailingPos, typ := range trailingTypes {
			vec := bat.Vecs[len(objectio.TombstoneSeqnums_CN_Created)+trailingPos]
			switch typ {
			case types.T_Rowid:
				require.NoError(t, vector.AppendFixed(
					vec, types.NewRowid(&physicalBlock, uint32(row)), false, common.DebugAllocator))
			case types.T_TS:
				commitTS := types.BuildTS(10, 0)
				if row == 2 {
					commitTS = types.BuildTS(20, 0)
				}
				require.NoError(t, vector.AppendFixed(vec, commitTS, false, common.DebugAllocator))
			case types.T_bool:
				require.NoError(t, vector.AppendFixed(vec, row == 1, false, common.DebugAllocator))
			default:
				t.Fatalf("unsupported test column type %s", typ.String())
			}
		}
	}
	bat.SetRowCount(3)
	return bat
}

func newBackupTombstoneLayout(commit, physical, abort uint16) backupSpecialColumnLayout {
	return backupSpecialColumnLayout{
		PhysicalAddr: physical,
		CommitTS:     commit,
		Abort:        abort,
	}
}

func ptrTo[T any](value T) *T {
	return &value
}
