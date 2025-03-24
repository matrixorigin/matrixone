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
	"bytes"
	"context"
	"sort"
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

func Test_ClusterKey1(t *testing.T) {
	packer := types.NewPacker()
	defer packer.Close()

	tableId := uint64(20)
	obj := types.NewObjectid()

	EncodeCluser(packer, tableId, ObjectType_Data, &obj)

	buf := packer.Bytes()
	packer.Reset()

	tuple, _, schemas, err := types.DecodeTuple(buf)
	require.NoError(t, err)
	require.Equalf(t, 3, len(schemas), "schemas: %v", schemas)
	require.Equalf(t, types.T_uint64, schemas[0], "schemas: %v", schemas)
	require.Equalf(t, types.T_int8, schemas[1], "schemas: %v", schemas)
	require.Equalf(t, types.T_Objectid, schemas[2], "schemas: %v", schemas)

	t.Log(tuple.SQLStrings(nil))
	require.Equal(t, tableId, tuple[0].(uint64))
	require.Equal(t, ObjectType_Data, tuple[1].(int8))
	oid := tuple[2].(types.Objectid)
	require.True(t, obj.EQ(&oid))
}

func Test_ClusterKey2(t *testing.T) {
	packer := types.NewPacker()
	defer packer.Close()
	cnt := 5000
	clusters := make([][]byte, 0, cnt)
	objTemplate := types.NewObjectid()
	for i := cnt; i >= 1; i-- {
		obj := objTemplate.Copy(uint16(i))
		EncodeCluser(packer, 1, ObjectType_Data, &obj)
		clusters = append(clusters, packer.Bytes())
		packer.Reset()
	}
	sort.Slice(clusters, func(i, j int) bool {
		return bytes.Compare(clusters[i], clusters[j]) < 0
	})

	last := uint16(0)
	for _, cluster := range clusters {
		tuple, _, _, err := types.DecodeTuple(cluster)
		require.NoError(t, err)
		require.Equalf(t, 3, len(tuple), "%v", tuple)
		require.Equal(t, uint64(1), tuple[0].(uint64))
		require.Equal(t, ObjectType_Data, tuple[1].(int8))
		obj := tuple[2].(types.Objectid)
		curr := obj.Offset()
		require.Truef(t, curr > last, "%v,%v", curr, last)
		last = curr
	}
}

func mockDataBatch(
	t *testing.T,
	data *batch.Batch,
	dataRows, tombstoneRows int,
	packer *types.Packer,
	accountId uint32,
	getDBID func(int) uint64,
	getTBLID func(uint64, int) uint64,
	mp *mpool.MPool,
) {
	data.CleanOnlyData()
	for i, vec := range data.Vecs {
		if i == TableObjectsAttr_Accout_Idx {
			require.NoError(t, vector.AppendMultiFixed(vec, accountId, false, dataRows+tombstoneRows, mp))
		} else if i == TableObjectsAttr_DB_Idx {
			tableVec := data.Vecs[TableObjectsAttr_Table_Idx]
			objectTypeVec := data.Vecs[TableObjectsAttr_ObjectType_Idx]
			idVec := data.Vecs[TableObjectsAttr_ID_Idx]
			clusterVec := data.Vecs[TableObjectsAttr_Cluster_Idx]
			for j := 0; j < dataRows+tombstoneRows; j++ {
				dbid := getDBID(j)
				tableid := getTBLID(dbid, j)

				var obj objectio.ObjectStats
				objname := objectio.MockObjectName()
				objectio.SetObjectStatsObjectName(&obj, objname)
				// Here we hard code the object size to 1000 for testing
				objectio.SetObjectStatsSize(&obj, uint32(1000))
				obj2 := obj.Clone()
				objectio.SetObjectStatsSize(obj2, 0)
				packer.Reset()
				EncodeCluser(packer, tableid, ObjectType_Data, objname.ObjectId())
				// if tableid == uint64(4) {
				// 	t.Logf("debug %s", obj.String())
				// }

				if j < dataRows {
					require.NoError(t, vector.AppendFixed(objectTypeVec, ObjectType_Data, false, mp))
				} else {
					require.NoError(t, vector.AppendFixed(objectTypeVec, ObjectType_Tombstone, false, mp))
				}

				require.NoError(t, vector.AppendFixed(vec, dbid, false, mp))
				require.NoError(t, vector.AppendFixed(tableVec, tableid, false, mp))
				require.NoError(t, vector.AppendBytes(idVec, obj2[:], false, mp))
				require.NoError(t, vector.AppendBytes(clusterVec, packer.Bytes(), false, mp))
			}
		} else if i == TableObjectsAttr_CreateTS_Idx {
			for j := 0; j < dataRows+tombstoneRows; j++ {
				require.NoError(t, vector.AppendFixed(vec, types.NextGlobalTsForTest(), false, mp))
			}
		} else if i == TableObjectsAttr_DeleteTS_Idx {
			for j := 0; j < dataRows+tombstoneRows; j++ {
				require.NoError(t, vector.AppendFixed(vec, types.NextGlobalTsForTest(), false, mp))
			}
		}
	}
	data.SetRowCount(dataRows + tombstoneRows)
}

func Test_Sinker1(t *testing.T) {
	proc := testutil.NewProc()
	fs, err := fileservice.Get[fileservice.FileService](
		proc.GetFileService(), defines.SharedFileServiceName,
	)
	require.NoError(t, err)
	mp := proc.Mp()

	bat := NewObjectListBatch()
	accountId := uint32(1)
	mapping := map[uint64][]uint64{
		1: {41, 31, 21, 11, 1},
		2: {42, 32, 22, 12, 2},
		3: {43, 33, 23, 13, 3},
	}
	dbs := []uint64{1, 2, 3}

	sinker := NewDataSinker(
		mp,
		fs,
		ioutil.WithMemorySizeThreshold(mpool.KB),
	)
	defer sinker.Close()

	packer := types.NewPacker()
	defer packer.Close()
	// dbid := dbs[j%len(dbs)]
	// tables := mapping[dbid]
	// tableid := tables[j%len(tables)]
	getDBID := func(i int) uint64 {
		return dbs[i%len(dbs)]
	}
	getTBLID := func(dbid uint64, i int) uint64 {
		tables := mapping[dbid]
		return tables[i%len(tables)]
	}

	ctx := context.Background()

	rows := 0
	for i := 0; i < 5; i++ {
		mockDataBatch(
			t, bat, 100, 0, packer, accountId, getDBID, getTBLID, mp,
		)
		require.NoError(t, sinker.Write(ctx, bat))
		rows += 100
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
	require.Equal(t, rows, totalRows)
}
