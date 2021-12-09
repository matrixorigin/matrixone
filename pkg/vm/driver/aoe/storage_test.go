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

package aoe

import (
	"bytes"
	"os"
	"testing"

	"github.com/fagongzi/util/protoc"
	"github.com/matrixorigin/matrixcube/pb/meta"
	"github.com/matrixorigin/matrixone/pkg/container/batch"
	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/sql/protocol"
	"github.com/matrixorigin/matrixone/pkg/vm/driver/pb"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/aoe"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/aoe/common/helper"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/aoe/storage/aoedb/v1"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/aoe/storage/container/vector"
	"github.com/stretchr/testify/require"
)

const (
	ShardMetadataCount = 5
	Rows               = uint64(40000)
)

var (
	testMetadatas      []meta.ShardMetadata
	testSplitMetadatas []meta.ShardMetadata
	testPath           = "./test"
	dbPath1            = "./test/1"
	dbPath2            = "./test/2"
	createTableCmd     []byte
	createTableKey     []byte
	testTableName      = "testTable"
	testTableInfo      []byte
	appendCmd          []byte
)

func init() {
	for i := 0; i < ShardMetadataCount; i++ {
		testMetadata := meta.ShardMetadata{
			ShardID:  uint64(i),
			LogIndex: 1,
			Metadata: meta.ShardLocalState{}}
		testMetadatas = append(testMetadatas, testMetadata)
	}

	for i := 0; i < 2; i++ {
		testSplitMetadata := meta.ShardMetadata{
			ShardID:  uint64(i + ShardMetadataCount),
			LogIndex: 1,
			Metadata: meta.ShardLocalState{}}
		testSplitMetadatas = append(testMetadatas, testSplitMetadata)
	}

	columns := aoe.ColumnInfo{
		Name: "col",
		Type: types.Type{Oid: types.T(types.T_varchar), Size: 24},
	}
	testTableInfo, _ = helper.EncodeTable(aoe.TableInfo{
		Columns: []aoe.ColumnInfo{columns},
	})

	createTableCmd = protoc.MustMarshal(&pb.CreateTabletRequest{
		Name:      testTableName,
		TableInfo: testTableInfo,
	})

	attrs := []string{"col"}
	bat := batch.New(true, attrs)
	var err error
	vec := vector.MockVector(types.Type{Oid: types.T(types.T_varchar), Size: 24}, Rows)
	bat.Vecs[0], err = vec.CopyToVector()
	if err != nil {
		panic(err)
	}
	vec.Close()
	var buf bytes.Buffer
	protocol.EncodeBatch(bat, &buf)
	testBatch := buf.Bytes()
	appendCmd = protoc.MustMarshal(&pb.AppendRequest{
		TabletName: testTableName,
		Data:       testBatch,
	})
}

func TestStorage(t *testing.T) {
	os.RemoveAll(testPath)
	db1, err := NewStorage(dbPath1)
	require.Nil(t, err)
	db2, err := NewStorage(dbPath2)
	require.Nil(t, err)
	err = db1.SaveShardMetadata(testMetadatas)
	require.Nil(t, err)
	err = db2.SaveShardMetadata(testMetadatas)
	require.Nil(t, err)

	initShardMetadata, err := db1.GetInitialStates()
	require.Equal(t, ShardMetadataCount, len(initShardMetadata))
	require.Nil(t, err)

	tbls := db1.tableNames()
	require.Equal(t, 5, len(tbls))

	tbls = db2.tableNames()
	require.Equal(t, 5, len(tbls))

	db1.createTable(uint64(2), 0, 1, uint64(0), createTableCmd, createTableKey)
	db1.Append(uint64(3), 0, 1, uint64(0), appendCmd, createTableKey)
	db1.Sync([]uint64{uint64(0)})

	tbls = db1.tableNames()
	require.Equal(t, 6, len(tbls))

	//test snapshot
	err = db1.CreateSnapshot(uint64(0), testPath)
	require.Nil(t, err)
	err = db2.ApplySnapshot(uint64(0), testPath)
	require.Nil(t, err)

	// tbls = db2.tableNames()
	// switch index {
	// case 0:
	// 	require.Equal(t, 4, len(tbls))
	// case 1:
	// 	require.Equal(t, 5, len(tbls))
	// case 2:
	// 	require.Equal(t, 6, len(tbls))
	// default:
	// 	require.Equal(t, 6, len(tbls))
	// }

	//test split
	// currentApproximateSize, currentApproximateKeys, splitKeys, ctx, err := db1.SplitCheck(testMetadatas[0].Metadata.Shard, uint64(1))
	// require.Nil(t, err)
	// fmt.Printf("%v,%v,%v,%v", currentApproximateSize, currentApproximateKeys, splitKeys, ctx)
	// testMetadatas[0].LogIndex = 4
	// err = db1.Split(testMetadatas[0], testSplitMetadatas, ctx)
	// require.Nil(t, err)
}

//for test
func (s *Storage) ReadAll(sid uint64) ([]*batch.Batch, error) {
	batchs:=make([]*batch.Batch,0)
	tbls := s.DB.TableNames(aoedb.IdToNameFactory.Encode(sid))
	for _, tbl := range tbls {
		relation,_:=s.Relation(aoedb.IdToNameFactory.Encode(sid), tbl)
		attrs := make([]string, 0)
		for _, ColDef := range relation.Meta.Schema.ColDefs {
			attrs = append(attrs, ColDef.Name)
		}
		relation.Data.GetBlockFactory()
		for _,segment := range relation.Meta.SegmentSet{
			seg := relation.Segment(segment.Id, nil)
			blks := seg.Blocks()
			for _,blk:=range blks{
				block := seg.Block(blk, nil)
				cds := make([]*bytes.Buffer, len(attrs))
				dds := make([]*bytes.Buffer, len(attrs))
				for i := range cds {
					cds[i] = bytes.NewBuffer(make([]byte, 0))
					dds[i] = bytes.NewBuffer(make([]byte, 0))
				}
				refs := make([]uint64, len(attrs))
				bat, _ := block.Read(refs, attrs, cds, dds)
				batchs=append(batchs, bat)
			}
		}

	}
	return batchs, nil
}
