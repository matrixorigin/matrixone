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
	"github.com/matrixorigin/matrixone/pkg/container/batch"
	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/container/vector"
	"github.com/matrixorigin/matrixone/pkg/defines"
	"github.com/matrixorigin/matrixone/pkg/fileservice"
	"github.com/matrixorigin/matrixone/pkg/objectio/ioutil"
	"github.com/matrixorigin/matrixone/pkg/testutil"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/readutil"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/common"
	"github.com/stretchr/testify/require"
)

func Test_Reader1(t *testing.T) {
	proc := testutil.NewProc(t)
	fs, err := fileservice.Get[fileservice.FileService](
		proc.GetFileService(), defines.SharedFileServiceName,
	)
	require.NoError(t, err)
	mp := proc.Mp()

	tables := []uint64{4, 3, 2, 5, 1}
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

	dataRows := []int{10, 50, 9000, 100, 10}
	tombstoneRows := []int{0, 1, 2, 9000, 10}
	// rows := []int{10, 20}

	allRows := 0
	for i := 0; i < len(dataRows); i++ {
		allRows += dataRows[i]
		allRows += tombstoneRows[i]
		getDBID := func(int) uint64 {
			return 1
		}
		getTBLID := func(_ uint64, _ int) uint64 {
			return tables[i]
		}

		mockDataBatch(
			t, dataBatch, dataRows[i], tombstoneRows[i], packer, 0, getDBID, getTBLID, mp,
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
	require.Equal(t, 5, len(files))
	require.Equal(t, allRows, totalRows)

	tableidScanBat := batch.NewWithSchema(
		true,
		DataScan_TableIDAtrrs,
		DataScan_TableIDTypes,
	)
	defer tableidScanBat.Clean(mp)

	for _, file := range files {
		reader := NewDataReader(
			ctx,
			fs,
			file,
			readutil.WithColumns(
				DataScan_TableIDSeqnums,
				DataScan_TableIDTypes,
			),
		)
		row := 0
		for {
			tableidScanBat.CleanOnlyData()
			isEnd, err := reader.Read(
				ctx, tableidScanBat.Attrs, nil, mp, tableidScanBat,
			)
			require.NoError(t, err)
			if isEnd {
				break
			}
			row += tableidScanBat.RowCount()
		}
		require.Equal(t, int(file.Rows()), row)
	}

	ranges := MakeTableRangeBatch()
	defer ranges.Clean(mp)
	err = CollectTableRanges(ctx, files, ranges, mp, fs)
	require.NoError(t, err)
	t.Log(common.MoBatchToString(ranges, 100))
	require.Equal(t, 11, ranges.RowCount())
	tableIds := vector.MustFixedColNoTypeCheck[uint64](ranges.Vecs[0])
	require.Equal(t, []uint64{1, 1, 2, 2, 2, 3, 3, 4, 5, 5, 5}, tableIds)
	objectTypes := vector.MustFixedColNoTypeCheck[int8](ranges.Vecs[1])
	require.Equal(t, []int8{1, 2, 1, 1, 2, 1, 2, 1, 1, 2, 2}, objectTypes)

	startRows := vector.MustFixedColNoTypeCheck[types.Rowid](ranges.Vecs[2])
	endRows := vector.MustFixedColNoTypeCheck[types.Rowid](ranges.Vecs[3])
	tableDataRows := make(map[uint64]int)
	tableTombstoneRows := make(map[uint64]int)
	for i := range tableIds {
		if objectTypes[i] == ObjectType_Data {
			rows := endRows[i].GetRowOffset() - startRows[i].GetRowOffset() + 1
			tableDataRows[tableIds[i]] += int(rows)
		}
		if objectTypes[i] == ObjectType_Tombstone {
			rows := endRows[i].GetRowOffset() - startRows[i].GetRowOffset() + 1
			tableTombstoneRows[tableIds[i]] += int(rows)
		}
	}
	expectDataRows := map[uint64]int{
		1: 10,
		2: 9000,
		3: 50,
		4: 10,
		5: 100,
	}
	expectTombstoneRows := map[uint64]int{
		1: 10,
		2: 2,
		3: 1,
		5: 9000,
	}
	require.Equal(t, expectDataRows, tableDataRows)
	require.Equal(t, expectTombstoneRows, tableTombstoneRows)

	// tableRanges := ExportToTableRanges(
	// 	ranges,
	// 	uint64(4),
	// )
	// t.Log(TableRangesString(tableRanges))
	// require.Equal(t, 1, len(tableRanges))
	// require.Equal(t, 10, TableRangesRows(tableRanges))

	tableRanges := ExportToTableRangesByFilter(
		ranges,
		uint64(2),
		ObjectType_Data,
	)
	t.Log(TableRangesString(tableRanges))
	require.Equal(t, 2, len(tableRanges))

	iter := NewObjectIter(
		ctx,
		tableRanges,
		mp,
		fs,
	)
	cnt := 0
	for ok, err := iter.Next(); ok && err == nil; ok, err = iter.Next() {
		entry := iter.Entry()
		require.Truef(t, entry.DeleteTime.GT(&entry.CreateTime), entry.String())
		// the size of the object is hard code to 1000 in mockDataBatch
		require.Falsef(t, entry.ObjectStats.Size() == uint32(1000), entry.String())
		// t.Log(entry.String())
		cnt++
	}
	iter.Close()
	require.Equal(t, 9000, cnt)

	tableRanges = ExportToTableRangesByFilter(
		ranges,
		uint64(5),
		ObjectType_Tombstone,
	)
	t.Log(TableRangesString(tableRanges))
	require.Equal(t, 2, len(tableRanges))

	iter = NewObjectIter(
		ctx,
		tableRanges,
		mp,
		fs,
	)
	cnt = 0
	for ok, err := iter.Next(); ok && err == nil; ok, err = iter.Next() {
		entry := iter.Entry()
		require.Truef(t, entry.DeleteTime.GT(&entry.CreateTime), entry.String())
		// the size of the object is hard code to 1000 in mockDataBatch
		require.Falsef(t, entry.ObjectStats.Size() == uint32(1000), entry.String())
		// t.Log(entry.String())
		cnt++
	}
	iter.Close()
	require.Equal(t, 9000, cnt)
}
