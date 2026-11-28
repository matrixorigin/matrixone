// Copyright 2022 Matrix Origin
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

package disttae

import (
	"context"
	"testing"

	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/container/vector"
	"github.com/matrixorigin/matrixone/pkg/defines"
	"github.com/matrixorigin/matrixone/pkg/fileservice"
	"github.com/matrixorigin/matrixone/pkg/objectio"
	"github.com/matrixorigin/matrixone/pkg/sql/colexec"
	"github.com/matrixorigin/matrixone/pkg/testutil"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/readutil"
	"github.com/stretchr/testify/require"
)

func TestWorkspaceRewriteTombstoneOffsets(t *testing.T) {
	ctx := t.Context()
	proc := testutil.NewProc(t)
	mp := proc.Mp()
	fs, err := fileservice.Get[fileservice.FileService](proc.GetFileService(), defines.SharedFileServiceName)
	require.NoError(t, err)
	pkType := types.T_int32.ToType()
	var tombstones workspaceFlushedTombstones
	name := objectio.BuildObjectName(objectio.NewSegmentid(), 0)
	blockA := types.NewBlockidWithObjectID(name.ObjectId(), 0)
	blockB := types.NewBlockidWithObjectID(name.ObjectId(), 1)

	writeTombstones := func(tableID uint64, block types.Blockid, offset uint32) objectio.ObjectStats {
		writer := colexec.NewCNS3TombstoneWriter(mp, fs, pkType, -1)
		defer writer.Close()
		bat := readutil.NewCNTombstoneBatch(&pkType, objectio.HiddenColumnSelection_None, mp)
		defer bat.Clean(mp)
		row := types.NewRowid(&block, offset)
		require.NoError(t, vector.AppendFixed(bat.Vecs[0], row, false, mp))
		require.NoError(t, vector.AppendFixed(bat.Vecs[1], int32(offset), false, mp))
		bat.SetRowCount(1)
		require.NoError(t, writer.Write(ctx, bat))
		stats, err := writer.Sync(ctx)
		require.NoError(t, err)
		require.Len(t, stats, 1)
		vec := vector.NewVec(types.T_blob.ToType(), mp)
		defer vec.Free(mp)
		require.NoError(t, vector.AppendBytes(vec, stats[0][:], false, mp))
		tombstones.adjust(tableID, vec, 1)
		return stats[0]
	}

	inherited := writeTombstones(1, blockA, 2)
	writeTombstones(2, blockA, 7)
	unrelated := types.RandomRowid()
	unrelatedStats := writeTombstones(1, *unrelated.BorrowBlockID(), 0)
	require.NoError(t, fs.Delete(ctx, unrelatedStats.ObjectName().String()))
	first := tombstones.objectTombstones(1, name.ObjectId())
	second := tombstones.objectTombstones(2, name.ObjectId())
	require.Equal(t, []objectio.ObjectStats{inherited}, first)
	require.Len(t, second, 1)
	require.Empty(t, tombstones.objectTombstones(3, name.ObjectId()))

	var snapshot types.TS
	pending := []int64{2, 1, 1}
	got, err := mergeTombstoneOffsets(ctx, fs, &snapshot, &blockA, first, pending, nil)
	require.NoError(t, err)
	require.Equal(t, []int64{1, 2}, got)
	require.Equal(t, []int64{2, 1, 1}, pending)
	got, err = mergeTombstoneOffsets(ctx, fs, &snapshot, &blockA, second, nil, nil)
	require.NoError(t, err)
	require.Equal(t, []int64{7}, got)
	got, err = mergeTombstoneOffsets(ctx, fs, &snapshot, &blockB, first, []int64{4}, nil)
	require.NoError(t, err)
	require.Equal(t, []int64{4}, got)

	canceled, cancel := context.WithCancel(ctx)
	cancel()
	_, err = mergeTombstoneOffsets(canceled, fs, &snapshot, &blockA, first, pending, nil)
	require.ErrorIs(t, err, context.Canceled)
	require.NoError(t, fs.Delete(ctx, inherited.ObjectName().String()))
	_, err = mergeTombstoneOffsets(ctx, fs, &snapshot, &blockA, first, pending, nil)
	require.Error(t, err)
	require.Equal(t, []int64{2, 1, 1}, pending)
}
