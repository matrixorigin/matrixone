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

package ioutil

import (
	"context"
	"testing"

	"github.com/matrixorigin/matrixone/pkg/common/mpool"
	"github.com/matrixorigin/matrixone/pkg/container/batch"
	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/container/vector"
	"github.com/matrixorigin/matrixone/pkg/objectio"
	"github.com/matrixorigin/matrixone/pkg/testutil"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/index"
	"github.com/stretchr/testify/require"
)

func TestObjectColumnMetasBuilderUpdateZmWithoutInspect(t *testing.T) {
	mp := mpool.MustNewZero()
	vec := vector.NewVec(types.T_TS.ToType())
	for i := 0; i < 4; i++ {
		require.NoError(t, vector.AppendFixed(vec, types.BuildTS(int64(i+1), 0), false, mp))
	}

	builder := NewObjectColumnMetasBuilder(1)
	zm := index.NewZM(vec.GetType().Oid, vec.GetType().Scale)
	require.NoError(t, index.BatchUpdateZM(zm, vec))
	index.SetZMSum(zm, vec)
	builder.UpdateZm(0, zm)

	_, metas := builder.Build()
	require.True(t, metas[0].ZoneMap().IsInited())
	require.Zero(t, metas[0].Ndv())
	require.Zero(t, metas[0].NullCnt())
}

func TestHiddenColumnsZoneMapPersistedAfterWriteAndRead(t *testing.T) {
	ctx := context.Background()
	fs := testutil.NewSharedFS()
	mp := mpool.MustNewZero()

	writer := ConstructWriter(0, []uint16{0, objectio.SEQNUM_ROWID, objectio.SEQNUM_COMMITTS}, -1, false, false, fs)
	bat := batchForHiddenColumnsTest(t, mp)
	blocks, _, err := func() ([]objectio.BlockObject, objectio.Extent, error) {
		_, err := writer.WriteBatch(bat)
		require.NoError(t, err)
		return writer.Sync(ctx)
	}()
	require.NoError(t, err)
	require.Len(t, blocks, 1)

	loc := objectio.BuildLocation(writer.GetName(), blocks[0].GetExtent(), uint32(bat.RowCount()), blocks[0].GetID())
	reader, err := NewObjectReader(fs, loc)
	require.NoError(t, err)
	meta, err := reader.LoadObjectMeta(ctx, mp)
	require.NoError(t, err)

	expectedMinRowid := types.NewRowid(new(types.Blockid), 1)
	expectedMaxRowid := types.NewRowid(new(types.Blockid), 9)
	expectedMinTS := types.BuildTS(10, 0)
	expectedMaxTS := types.BuildTS(30, 0)

	rowidBlkZM := meta.GetColumnMeta(0, 1).ZoneMap()
	require.True(t, rowidBlkZM.IsInited())
	require.Equal(t, expectedMinRowid, types.DecodeFixed[types.Rowid](rowidBlkZM.GetMinBuf()))
	require.Equal(t, expectedMaxRowid, types.DecodeFixed[types.Rowid](rowidBlkZM.GetMaxBuf()))

	tsBlkZM := meta.GetColumnMeta(0, 2).ZoneMap()
	require.True(t, tsBlkZM.IsInited())
	require.Equal(t, expectedMinTS, types.DecodeFixed[types.TS](tsBlkZM.GetMinBuf()))
	require.Equal(t, expectedMaxTS, types.DecodeFixed[types.TS](tsBlkZM.GetMaxBuf()))
}

func batchForHiddenColumnsTest(t *testing.T, mp *mpool.MPool) *batch.Batch {
	t.Helper()
	bat := batch.NewWithSize(3)
	bat.Vecs[0] = vector.NewVec(types.T_int32.ToType())
	bat.Vecs[1] = vector.NewVec(types.T_Rowid.ToType())
	bat.Vecs[2] = vector.NewVec(types.T_TS.ToType())

	values := []int32{11, 22, 33}
	rowOffsets := []uint32{7, 1, 9}
	commitTS := []types.TS{
		types.BuildTS(20, 0),
		types.BuildTS(10, 0),
		types.BuildTS(30, 0),
	}

	var blk types.Blockid
	for i := range values {
		require.NoError(t, vector.AppendFixed(bat.Vecs[0], values[i], false, mp))
		require.NoError(t, vector.AppendFixed(bat.Vecs[1], types.NewRowid(&blk, rowOffsets[i]), false, mp))
		require.NoError(t, vector.AppendFixed(bat.Vecs[2], commitTS[i], false, mp))
	}
	bat.SetRowCount(len(values))
	return bat
}
