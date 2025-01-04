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

package ckputil

import (
	"context"
	"testing"

	"github.com/matrixorigin/matrixone/pkg/common/mpool"
	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/defines"
	"github.com/matrixorigin/matrixone/pkg/fileservice"
	"github.com/matrixorigin/matrixone/pkg/objectio/ioutil"
	"github.com/matrixorigin/matrixone/pkg/testutil"
	"github.com/stretchr/testify/require"
)

func Test_Reader1(t *testing.T) {
	proc := testutil.NewProc()
	fs, err := fileservice.Get[fileservice.FileService](
		proc.GetFileService(), defines.SharedFileServiceName,
	)
	require.NoError(t, err)
	mp := proc.Mp()

	tables := []uint64{4, 3, 5, 2, 1}
	sinker := NewDataSinker(
		mp,
		fs,
		ioutil.WithMemorySizeThreshold(mpool.KB*200),
	)

	dataBatch := NewObjectListBatch()
	defer dataBatch.Clean(mp)

	packer := types.NewPacker()
	defer packer.Close()
	ctx := context.Background()

	rows := []int{10, 50, 9000, 100}

	allRows := 0
	for i := 0; i < 4; i++ {
		allRows += rows[i]
		getDBID := func(int) uint64 {
			return 1
		}
		getTBLID := func(_ uint64, _ int) uint64 {
			return tables[i]
		}

		mockDataBatch(
			t, dataBatch, rows[i], packer, 0, getDBID, getTBLID, mp,
		)
		require.NoError(t, sinker.Write(ctx, dataBatch))
	}
	require.NoError(t, sinker.Sync(ctx))
	files, inMems := sinker.GetResult()
	require.Equal(t, 0, len(inMems))
	totalRows := 0
	for _, file := range files {
		t.Log(file.String())
		totalRows += int(file.Rows())
	}
	require.Equal(t, 2, len(files))
	require.Equal(t, allRows, totalRows)

}
