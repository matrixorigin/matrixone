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

package test

import (
	"context"
	"testing"
	"time"

	"github.com/matrixorigin/matrixone/pkg/common/mpool"
	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/container/vector"
	"github.com/matrixorigin/matrixone/pkg/defines"
	"github.com/matrixorigin/matrixone/pkg/fileservice"
	"github.com/matrixorigin/matrixone/pkg/objectio"
	"github.com/matrixorigin/matrixone/pkg/pb/timestamp"
	testutil3 "github.com/matrixorigin/matrixone/pkg/testutil"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/disttae"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/engine_util"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/blockio"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/containers"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/mergesort"
	"github.com/stretchr/testify/require"
)

func Test_Sinker(t *testing.T) {
	mp := mpool.MustNewZeroNoFixed()
	proc := testutil3.NewProcessWithMPool("", mp)
	pkType := types.T_int32.ToType()
	fs, err := fileservice.Get[fileservice.FileService](proc.GetFileService(), defines.SharedFileServiceName)
	require.NoError(t, err)

	sinker1 := engine_util.NewTombstoneSinker(
		false,
		pkType,
		mp,
		fs,
		// engine_util.WithDedupAll(),
		engine_util.WithMemorySizeThreshold(mpool.KB*400),
	)

	blkCnt := 5
	blkRows := 8192
	pkVec := containers.MockVector2(
		pkType,
		blkCnt*blkRows,
		0,
	)
	rowIDVec, err := objectio.MockOneObj_MulBlks_Rowids(
		blkCnt, blkRows, true, mp,
	)
	require.NoError(t, err)
	bat1 := engine_util.NewCNTombstoneBatch(&pkType)
	bat1.SetVector(0, rowIDVec)
	bat1.SetVector(1, pkVec.GetDownstreamVector())
	bat1.SetRowCount(rowIDVec.Length())

	ctx, cancel := context.WithTimeout(context.Background(), time.Second*100)
	defer cancel()

	for i := 0; i < bat1.RowCount(); i += 10 {
		var end int
		if i+10 > bat1.RowCount() {
			end = bat1.RowCount()
		} else {
			end = 10 + i
		}
		data, err := bat1.Window(i, end)
		require.NoError(t, err)
		err = sinker1.Write(ctx, data)
		require.NoError(t, err)
	}

	err = sinker1.Sync(ctx)
	require.NoError(t, err)

	objs, batches := sinker1.GetResult()
	require.Equal(t, 0, len(batches))
	require.Equal(t, 3, len(objs))
	rows := 0
	for _, stats := range objs {
		rows += int(stats.Rows())
	}

	require.Equal(t, bat1.RowCount(), rows)

	r := disttae.SimpleMultiObjectsReader(
		ctx, fs, objs, timestamp.Timestamp{},
		disttae.WithColumns(
			objectio.TombstoneSeqnums_CN_Created,
			objectio.GetTombstoneTypes(pkType, false),
		),
		disttae.WithTombstone(),
	)
	blockio.Start("")
	defer blockio.Stop("")
	bat2 := engine_util.NewCNTombstoneBatch(&pkType)
	buffer := engine_util.NewCNTombstoneBatch(&pkType)
	for {
		done, err := r.Read(ctx, buffer.Attrs, nil, mp, buffer)
		require.NoError(t, err)
		if done {
			break
		}
		rowIds := vector.MustFixedColWithTypeCheck[types.Rowid](buffer.Vecs[0])
		var (
			curr, last *types.Rowid
		)
		for i := range rowIds {
			if i == 0 {
				last = &rowIds[i]
				continue
			}
			curr = &rowIds[i]
			require.True(t, curr.GE(last))
		}
		err = bat2.Union(buffer, 0, buffer.RowCount(), mp)
		require.NoError(t, err)
	}
	buffer.Clean(mp)
	require.Equal(t, bat1.RowCount(), bat2.RowCount())
	err = mergesort.SortColumnsByIndex(
		bat1.Vecs,
		objectio.TombstonePrimaryKeyIdx,
		mp,
	)
	require.NoError(t, err)
	err = mergesort.SortColumnsByIndex(
		bat2.Vecs,
		objectio.TombstonePrimaryKeyIdx,
		mp,
	)
	rowids1 := vector.MustFixedColWithTypeCheck[types.Rowid](bat1.Vecs[0])
	rowids2 := vector.MustFixedColWithTypeCheck[types.Rowid](bat2.Vecs[0])
	pks1 := vector.MustFixedColWithTypeCheck[int32](bat1.Vecs[1])
	pks2 := vector.MustFixedColWithTypeCheck[int32](bat2.Vecs[1])
	for pos, end := 0, bat1.RowCount(); pos < end; pos++ {
		require.Equalf(t, rowids1[pos], rowids2[pos], "pos:%d, rowids1:%s, rowids2:%s", pos, rowids1[pos], rowids2[pos])
		require.Equalf(t, pks1[pos], pks2[pos], "pos:%d, pks1:%d, pks2:%d", pos, pks1[pos], pks2[pos])
	}

	pkVec.Close()
	rowIDVec.Free(mp)
	bat2.Clean(mp)

	require.True(t, mp.CurrNB() > 0)
	sinker1.Close()
	require.True(t, mp.CurrNB() == 0)
}
