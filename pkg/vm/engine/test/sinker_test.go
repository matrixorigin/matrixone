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
	"github.com/matrixorigin/matrixone/pkg/defines"
	"github.com/matrixorigin/matrixone/pkg/fileservice"
	"github.com/matrixorigin/matrixone/pkg/objectio"
	testutil3 "github.com/matrixorigin/matrixone/pkg/testutil"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/engine_util"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/containers"
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

	pkVec.Close()
	rowIDVec.Free(mp)

	require.True(t, mp.CurrNB() > 0)
	sinker1.Close()
	require.True(t, mp.CurrNB() == 0)
}
