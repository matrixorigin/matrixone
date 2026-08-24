// Copyright 2026 Matrix Origin
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

package tables

import (
	"context"
	"testing"

	"github.com/matrixorigin/matrixone/pkg/common/mpool"
	"github.com/matrixorigin/matrixone/pkg/container/batch"
	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/container/vector"
	"github.com/matrixorigin/matrixone/pkg/objectio"
	"github.com/matrixorigin/matrixone/pkg/objectio/ioutil"
	"github.com/matrixorigin/matrixone/pkg/testutil"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/catalog"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/common"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/containers"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/db/dbutils"
	"github.com/stretchr/testify/require"
)

func TestLoadPersistedColumnDataBroadcastsConstantCommitTS(t *testing.T) {
	ctx := context.Background()
	fs := testutil.NewSharedFS()
	mp := mpool.MustNewZero()
	defer mpool.DeleteMPool(mp)

	smallPool := containers.NewVectorPool("constant-commit-small", 4, containers.WithMPool(mp))
	transientPool := containers.NewVectorPool("constant-commit-transient", 4, containers.WithMPool(mp))
	defer smallPool.Destory()
	defer transientPool.Destory()
	rt := dbutils.NewRuntime(
		dbutils.WithRuntimeObjectFS(fs),
		dbutils.WithRuntimeSmallPool(smallPool),
		dbutils.WithRuntimeTransientPool(transientPool),
	)

	const rowCount = 3
	schema := catalog.MockSchema(1, 0)
	input := batch.NewWithSize(3)
	input.Vecs[0] = vector.NewVec(types.T_int32.ToType())
	var err error
	input.Vecs[1], err = vector.NewConstFixed(
		types.T_TS.ToType(), types.BuildTS(5, 0), rowCount, mp,
	)
	require.NoError(t, err)
	input.Vecs[2] = vector.NewVec(types.T_bool.ToType())
	for row, aborted := range []bool{false, true, false} {
		require.NoError(t, vector.AppendFixed(input.Vecs[0], int32(row), false, mp))
		require.NoError(t, vector.AppendFixed(input.Vecs[2], aborted, false, mp))
	}
	input.SetRowCount(rowCount)
	defer input.Clean(mp)

	name := objectio.BuildObjectName(objectio.NewSegmentid(), 0)
	writer, err := objectio.NewObjectWriter(
		name,
		fs,
		0,
		[]uint16{schema.ColDefs[0].SeqNum, objectio.SEQNUM_COMMITTS, objectio.SEQNUM_ABORT},
		nil,
	)
	require.NoError(t, err)
	writer.SetAppendable()
	_, err = writer.Write(input)
	require.NoError(t, err)
	_, err = writer.WriteEnd(ctx)
	require.NoError(t, err)
	stats := writer.GetObjectStats(objectio.WithAppendable())
	require.NoError(t, objectio.SetObjectStatsRowCnt(&stats, rowCount))
	location := stats.BlockLocation(0, objectio.BlockMaxRows)
	snapshot := types.BuildTS(10, 0)

	vecs, deletes, release, err := LoadPersistedColumnData(
		ctx,
		schema,
		rt,
		new(common.ID),
		[]int{0},
		location,
		mp,
		&snapshot,
		true,
		false,
	)
	require.NoError(t, err)
	if release != nil {
		defer release()
	}
	defer func() {
		for _, vec := range vecs {
			if vec != nil {
				vec.Close()
			}
		}
	}()

	require.Len(t, vecs, 1)
	require.Equal(t, rowCount, vecs[0].Length())
	require.NotNil(t, deletes)
	require.Equal(t, 1, deletes.Count())
	require.True(t, deletes.Contains(1))
}

func TestLoadPersistedColumnDataUsesDecodedRowsForShortIntermediateBlock(t *testing.T) {
	ctx := context.Background()
	fs := testutil.NewSharedFS()
	mp := mpool.MustNewZero()
	defer mpool.DeleteMPool(mp)

	smallPool := containers.NewVectorPool("short-block-small", 4, containers.WithMPool(mp))
	transientPool := containers.NewVectorPool("short-block-transient", 4, containers.WithMPool(mp))
	defer smallPool.Destory()
	defer transientPool.Destory()
	rt := dbutils.NewRuntime(
		dbutils.WithRuntimeObjectFS(fs),
		dbutils.WithRuntimeSmallPool(smallPool),
		dbutils.WithRuntimeTransientPool(transientPool),
	)

	schema := catalog.MockSchema(1, 0)
	writer := ioutil.ConstructWriter(
		0,
		[]uint16{schema.ColDefs[0].SeqNum},
		-1,
		false,
		false,
		fs,
	)
	writeBlock := func(values []int32) {
		t.Helper()
		input := batch.NewWithSize(1)
		input.Vecs[0] = vector.NewVec(types.T_int32.ToType())
		for _, value := range values {
			require.NoError(t, vector.AppendFixed(input.Vecs[0], value, false, mp))
		}
		input.SetRowCount(len(values))
		_, err := writer.WriteBatch(input)
		require.NoError(t, err)
		input.Clean(mp)
	}
	writeBlock([]int32{1, 2})
	writeBlock([]int32{3, 4, 5})
	_, _, err := writer.Sync(ctx)
	require.NoError(t, err)
	stats := writer.GetObjectStats()
	require.Equal(t, uint32(2), stats.BlkCnt())
	location := stats.BlockLocation(0, objectio.BlockMaxRows)
	id := &common.ID{BlockID: stats.ConstructBlockId(0)}
	physicalIdx := len(schema.ColDefs) - 1

	vecs, deletes, release, err := LoadPersistedColumnData(
		ctx,
		schema,
		rt,
		id,
		[]int{0, physicalIdx},
		location,
		mp,
		nil,
		true,
		false,
	)
	require.NoError(t, err)
	if release != nil {
		defer release()
	}
	defer closePersistedVectors(vecs)
	require.Nil(t, deletes)
	require.Len(t, vecs, 2)
	require.Equal(t, 2, vecs[0].Length())
	require.Equal(t, 2, vecs[1].Length())
	rowIDs := vector.MustFixedColWithTypeCheck[types.Rowid](vecs[1].GetDownstreamVector())
	require.Equal(t, uint32(0), rowIDs[0].GetRowOffset())
	require.Equal(t, uint32(1), rowIDs[1].GetRowOffset())
}

func TestCNCreatedTombstoneClassificationAllowsPhysicalRowID(t *testing.T) {
	ctx := context.Background()
	fs := testutil.NewSharedFS()
	mp := mpool.MustNewZero()
	defer mpool.DeleteMPool(mp)

	writer := ioutil.ConstructTombstoneWriter(
		objectio.HiddenColumnSelection_PhysicalAddr, fs,
	)
	input := batch.NewWithSize(3)
	input.Vecs[0] = vector.NewVec(types.T_Rowid.ToType())
	input.Vecs[1] = vector.NewVec(types.T_int32.ToType())
	input.Vecs[2] = vector.NewVec(types.T_Rowid.ToType())
	deletedBlock := objectio.NewBlockid(objectio.NewSegmentid(), 1, 0)
	storageBlock := objectio.NewBlockid(objectio.NewSegmentid(), 2, 0)
	require.NoError(t, vector.AppendFixed(
		input.Vecs[0], types.NewRowid(deletedBlock, 3), false, mp,
	))
	require.NoError(t, vector.AppendFixed(input.Vecs[1], int32(7), false, mp))
	require.NoError(t, vector.AppendFixed(
		input.Vecs[2], types.NewRowid(storageBlock, 0), false, mp,
	))
	input.SetRowCount(1)
	defer input.Clean(mp)
	_, err := writer.WriteBatch(input)
	require.NoError(t, err)
	_, _, err = writer.Sync(ctx)
	require.NoError(t, err)
	stats := writer.GetObjectStats(objectio.WithCNCreated())
	location := stats.ObjectLocation()
	meta, err := objectio.FastLoadObjectMeta(ctx, &location, false, fs)
	require.NoError(t, err)
	block := meta.MustDataMeta().GetBlockMeta(0)
	require.True(t, isCNCreatedTombstoneBlock(block))
}
