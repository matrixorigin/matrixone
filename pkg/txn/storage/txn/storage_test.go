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

package txnstorage

import (
	"bytes"
	"context"
	"encoding/gob"
	"testing"

	"github.com/matrixorigin/matrixone/pkg/container/batch"
	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/container/vector"
	"github.com/matrixorigin/matrixone/pkg/pb/timestamp"
	"github.com/matrixorigin/matrixone/pkg/pb/txn"
	"github.com/matrixorigin/matrixone/pkg/testutil"
	"github.com/matrixorigin/matrixone/pkg/vm/engine"
	txnengine "github.com/matrixorigin/matrixone/pkg/vm/engine/txn"
	"github.com/stretchr/testify/assert"
)

func testDatabase(
	t *testing.T,
	newStorage func() (*Storage, error),
) {
	heap := testutil.NewMheap()

	// new
	s, err := newStorage()
	assert.Nil(t, err)
	defer s.Close(context.TODO())

	// txn
	txnMeta := txn.TxnMeta{
		ID:     []byte("1"),
		Status: txn.TxnStatus_Active,
		SnapshotTS: timestamp.Timestamp{
			PhysicalTime: 1,
			LogicalTime:  1,
		},
	}

	// open database
	{
		resp := testRead[txnengine.OpenDatabaseResp](
			t, s, txnMeta,
			txnengine.OpOpenDatabase,
			txnengine.OpenDatabaseReq{
				Name: "foo",
			},
		)
		assert.Equal(t, "foo", resp.ErrNotFound.Name)
	}

	// create database
	{
		resp := testWrite[txnengine.CreateDatabaseResp](
			t, s, txnMeta,
			txnengine.OpCreateDatabase,
			txnengine.CreateDatabaseReq{
				Name: "foo",
			},
		)
		assert.Equal(t, txnengine.ErrExisted(false), resp.ErrExisted)
		assert.NotEmpty(t, resp.ID)
	}

	// get databases
	{
		resp := testRead[txnengine.GetDatabasesResp](
			t, s, txnMeta,
			txnengine.OpGetDatabases,
			txnengine.GetDatabasesReq{},
		)
		assert.Equal(t, 1, len(resp.Names))
		assert.Equal(t, "foo", resp.Names[0])
	}

	// open database
	var dbID string
	{
		resp := testRead[txnengine.OpenDatabaseResp](
			t, s, txnMeta,
			txnengine.OpOpenDatabase,
			txnengine.OpenDatabaseReq{
				Name: "foo",
			},
		)
		assert.Equal(t, "", resp.ErrNotFound.Name)
		assert.NotNil(t, resp.ID)
		dbID = resp.ID

		// delete database
		defer func() {
			{
				resp := testWrite[txnengine.DeleteDatabaseResp](
					t, s, txnMeta,
					txnengine.OpDeleteDatabase,
					txnengine.DeleteDatabaseReq{
						Name: "foo",
					},
				)
				assert.Equal(t, "", resp.ErrNotFound.Name)
				assert.NotEmpty(t, resp.ID)
			}
			{
				resp := testRead[txnengine.GetDatabasesResp](
					t, s, txnMeta,
					txnengine.OpGetDatabases,
					txnengine.GetDatabasesReq{},
				)
				assert.Equal(t, 0, len(resp.Names))
			}
		}()
	}

	// open relation
	{
		resp := testRead[txnengine.OpenRelationResp](
			t, s, txnMeta,
			txnengine.OpOpenRelation,
			txnengine.OpenRelationReq{
				DatabaseID: dbID,
				Name:       "table",
			},
		)
		assert.Equal(t, "table", resp.ErrNotFound.Name)
	}

	// create relation
	{
		resp := testWrite[txnengine.CreateRelationResp](
			t, s, txnMeta,
			txnengine.OpCreateRelation,
			txnengine.CreateRelationReq{
				DatabaseID: dbID,
				Name:       "table",
				Type:       txnengine.RelationTable,
				Defs: []engine.TableDef{
					&engine.AttributeDef{
						Attr: engine.Attribute{
							Name:    "a",
							Type:    types.T_int64.ToType(),
							Primary: true,
						},
					},
					&engine.AttributeDef{
						Attr: engine.Attribute{
							Name:    "b",
							Type:    types.T_int64.ToType(),
							Primary: false,
						},
					},
				},
			},
		)
		assert.Equal(t, txnengine.ErrExisted(false), resp.ErrExisted)
		assert.NotEmpty(t, resp.ID)
	}

	// get relations
	{
		resp := testRead[txnengine.GetRelationsResp](
			t, s, txnMeta,
			txnengine.OpGetRelations,
			txnengine.GetRelationsReq{
				DatabaseID: dbID,
			},
		)
		assert.Equal(t, 1, len(resp.Names))
		assert.Equal(t, "table", resp.Names[0])
	}

	// open relation
	var relID string
	{
		resp := testRead[txnengine.OpenRelationResp](
			t, s, txnMeta,
			txnengine.OpOpenRelation,
			txnengine.OpenRelationReq{
				DatabaseID: dbID,
				Name:       "table",
			},
		)
		assert.Equal(t, "", resp.ErrNotFound.Name)
		assert.NotNil(t, resp.ID)
		relID = resp.ID
		assert.Equal(t, txnengine.RelationTable, resp.Type)
	}
	_ = relID

	// get relation defs
	{
		resp := testRead[txnengine.GetTableDefsResp](
			t, s, txnMeta,
			txnengine.OpGetTableDefs,
			txnengine.GetTableDefsReq{
				TableID: relID,
			},
		)
		assert.Empty(t, resp.ErrTableNotFound.ID)
		assert.Empty(t, resp.ErrTableNotFound.Name)
		assert.Equal(t, 3, len(resp.Defs))
	}

	// write
	{
		colA := testutil.NewVector(
			5,
			types.T_int64.ToType(),
			heap,
			false,
			[]int64{
				1, 2, 3, 4, 5,
			},
		)
		colB := testutil.NewVector(
			5,
			types.T_int64.ToType(),
			heap,
			false,
			[]int64{
				6, 7, 8, 9, 10,
			},
		)
		bat := batch.New(false, []string{"a", "b"})
		bat.Vecs[0] = colA
		bat.Vecs[1] = colB
		bat.InitZsOne(5)
		resp := testWrite[txnengine.WriteResp](
			t, s, txnMeta,
			txnengine.OpWrite,
			txnengine.WriteReq{
				TableID: relID,
				Batch:   bat,
			},
		)
		assert.Empty(t, resp.ErrReadOnly)
		assert.Empty(t, resp.ErrTableNotFound)
	}

	// read
	var iterID string
	{
		resp := testRead[txnengine.NewTableIterResp](
			t, s, txnMeta,
			txnengine.OpNewTableIter,
			txnengine.NewTableIterReq{
				TableID: relID,
			},
		)
		assert.NotEmpty(t, resp.IterID)
		assert.Empty(t, resp.ErrTableNotFound)
		iterID = resp.IterID
	}
	{
		resp := testRead[txnengine.ReadResp](
			t, s, txnMeta,
			txnengine.OpRead,
			txnengine.ReadReq{
				IterID:   iterID,
				ColNames: []string{"a", "b"},
			},
		)
		assert.Empty(t, resp.ErrIterNotFound)
		assert.Empty(t, resp.ErrColumnNotFound)
		assert.NotNil(t, resp.Batch)
		assert.Equal(t, 5, resp.Batch.Length())
	}

	// delete by primary key
	{
		colA := testutil.NewVector(
			1,
			types.T_int64.ToType(),
			heap,
			false,
			[]int64{
				1,
			},
		)
		resp := testWrite[txnengine.DeleteResp](
			t, s, txnMeta,
			txnengine.OpDelete,
			txnengine.DeleteReq{
				TableID:    relID,
				ColumnName: "a",
				Vector:     colA,
			},
		)
		assert.Empty(t, resp.ErrReadOnly)
		assert.Empty(t, resp.ErrTableNotFound)
	}

	// read after delete
	{
		resp := testRead[txnengine.NewTableIterResp](
			t, s, txnMeta,
			txnengine.OpNewTableIter,
			txnengine.NewTableIterReq{
				TableID: relID,
			},
		)
		assert.NotEmpty(t, resp.IterID)
		assert.Empty(t, resp.ErrTableNotFound)
		iterID = resp.IterID
	}
	{
		resp := testRead[txnengine.ReadResp](
			t, s, txnMeta,
			txnengine.OpRead,
			txnengine.ReadReq{
				IterID:   iterID,
				ColNames: []string{"a", "b"},
			},
		)
		assert.Empty(t, resp.ErrIterNotFound)
		assert.Empty(t, resp.ErrColumnNotFound)
		assert.NotNil(t, resp.Batch)
		assert.Equal(t, 4, resp.Batch.Length())
	}

	// delete by non-primary key
	{
		colB := testutil.NewVector(
			1,
			types.T_int64.ToType(),
			heap,
			false,
			[]int64{
				8,
			},
		)
		resp := testWrite[txnengine.DeleteResp](
			t, s, txnMeta,
			txnengine.OpDelete,
			txnengine.DeleteReq{
				TableID:    relID,
				ColumnName: "b",
				Vector:     colB,
			},
		)
		assert.Empty(t, resp.ErrReadOnly)
		assert.Empty(t, resp.ErrTableNotFound)
	}

	// read after delete
	{
		resp := testRead[txnengine.NewTableIterResp](
			t, s, txnMeta,
			txnengine.OpNewTableIter,
			txnengine.NewTableIterReq{
				TableID: relID,
			},
		)
		assert.NotEmpty(t, resp.IterID)
		assert.Empty(t, resp.ErrTableNotFound)
		iterID = resp.IterID
	}
	{
		resp := testRead[txnengine.ReadResp](
			t, s, txnMeta,
			txnengine.OpRead,
			txnengine.ReadReq{
				IterID:   iterID,
				ColNames: []string{"a", "b"},
			},
		)
		assert.Empty(t, resp.ErrIterNotFound)
		assert.Empty(t, resp.ErrColumnNotFound)
		assert.NotNil(t, resp.Batch)
		assert.Equal(t, 3, resp.Batch.Length())
	}

	// write after delete
	{
		colA := testutil.NewVector(
			1,
			types.T_int64.ToType(),
			heap,
			false,
			[]int64{
				1,
			},
		)
		colB := testutil.NewVector(
			1,
			types.T_int64.ToType(),
			heap,
			false,
			[]int64{
				6,
			},
		)
		bat := batch.New(false, []string{"a", "b"})
		bat.Vecs[0] = colA
		bat.Vecs[1] = colB
		bat.InitZsOne(1)
		resp := testWrite[txnengine.WriteResp](
			t, s, txnMeta,
			txnengine.OpWrite,
			txnengine.WriteReq{
				TableID: relID,
				Batch:   bat,
			},
		)
		assert.Empty(t, resp.ErrReadOnly)
		assert.Empty(t, resp.ErrTableNotFound)
	}

	// delete relation
	{
		resp := testWrite[txnengine.DeleteRelationResp](
			t, s, txnMeta,
			txnengine.OpDeleteRelation,
			txnengine.DeleteRelationReq{
				DatabaseID: dbID,
				Name:       "table",
			},
		)
		assert.Equal(t, "", resp.ErrNotFound.Name)
		assert.NotEmpty(t, resp.ID)
	}
	{
		resp := testRead[txnengine.GetRelationsResp](
			t, s, txnMeta,
			txnengine.OpGetRelations,
			txnengine.GetRelationsReq{
				DatabaseID: dbID,
			},
		)
		assert.Equal(t, 0, len(resp.Names))
	}

	// new relation without primary key
	{
		resp := testWrite[txnengine.CreateRelationResp](
			t, s, txnMeta,
			txnengine.OpCreateRelation,
			txnengine.CreateRelationReq{
				DatabaseID: dbID,
				Name:       "table",
				Type:       txnengine.RelationTable,
				Defs: []engine.TableDef{
					&engine.AttributeDef{
						Attr: engine.Attribute{
							Name: "a",
							Type: types.T_int64.ToType(),
						},
					},
					&engine.AttributeDef{
						Attr: engine.Attribute{
							Name: "b",
							Type: types.T_int64.ToType(),
						},
					},
				},
			},
		)
		assert.Empty(t, resp.ErrReadOnly)
		assert.Empty(t, resp.ErrDatabaseNotFound)
		assert.Empty(t, resp.ErrExisted)
		assert.NotEmpty(t, resp.ID)
		relID = resp.ID
	}

	// write
	{
		colA := testutil.NewVector(
			5,
			types.T_int64.ToType(),
			heap,
			false,
			[]int64{
				1, 2, 3, 4, 5,
			},
		)
		colB := testutil.NewVector(
			5,
			types.T_int64.ToType(),
			heap,
			false,
			[]int64{
				6, 7, 8, 9, 10,
			},
		)
		bat := batch.New(false, []string{"a", "b"})
		bat.Vecs[0] = colA
		bat.Vecs[1] = colB
		bat.InitZsOne(5)
		resp := testWrite[txnengine.WriteResp](
			t, s, txnMeta,
			txnengine.OpWrite,
			txnengine.WriteReq{
				TableID: relID,
				Batch:   bat,
			},
		)
		assert.Empty(t, resp.ErrReadOnly)
		assert.Empty(t, resp.ErrTableNotFound)
	}

	// delete by primary key
	{
		colA := testutil.NewVector(
			1,
			types.T_int64.ToType(),
			heap,
			false,
			[]int64{
				1,
			},
		)
		resp := testWrite[txnengine.DeleteResp](
			t, s, txnMeta,
			txnengine.OpDelete,
			txnengine.DeleteReq{
				TableID:    relID,
				ColumnName: "a",
				Vector:     colA,
			},
		)
		assert.Empty(t, resp.ErrReadOnly)
		assert.Empty(t, resp.ErrTableNotFound)
	}

	// read after delete
	{
		resp := testRead[txnengine.NewTableIterResp](
			t, s, txnMeta,
			txnengine.OpNewTableIter,
			txnengine.NewTableIterReq{
				TableID: relID,
			},
		)
		assert.NotEmpty(t, resp.IterID)
		assert.Empty(t, resp.ErrTableNotFound)
		iterID = resp.IterID
	}
	{
		resp := testRead[txnengine.ReadResp](
			t, s, txnMeta,
			txnengine.OpRead,
			txnengine.ReadReq{
				IterID:   iterID,
				ColNames: []string{"a", "b"},
			},
		)
		assert.Empty(t, resp.ErrIterNotFound)
		assert.Empty(t, resp.ErrColumnNotFound)
		assert.NotNil(t, resp.Batch)
		assert.Equal(t, 4, resp.Batch.Length())
	}

	// delete by non-primary key
	{
		colB := testutil.NewVector(
			1,
			types.T_int64.ToType(),
			heap,
			false,
			[]int64{
				8,
			},
		)
		resp := testWrite[txnengine.DeleteResp](
			t, s, txnMeta,
			txnengine.OpDelete,
			txnengine.DeleteReq{
				TableID:    relID,
				ColumnName: "b",
				Vector:     colB,
			},
		)
		assert.Empty(t, resp.ErrReadOnly)
		assert.Empty(t, resp.ErrTableNotFound)
	}

	// read after delete
	{
		resp := testRead[txnengine.NewTableIterResp](
			t, s, txnMeta,
			txnengine.OpNewTableIter,
			txnengine.NewTableIterReq{
				TableID: relID,
			},
		)
		assert.NotEmpty(t, resp.IterID)
		assert.Empty(t, resp.ErrTableNotFound)
		iterID = resp.IterID
	}
	var rowIDs *vector.Vector
	{
		resp := testRead[txnengine.ReadResp](
			t, s, txnMeta,
			txnengine.OpRead,
			txnengine.ReadReq{
				IterID:   iterID,
				ColNames: []string{"a", "b", rowIDColumnName},
			},
		)
		assert.Empty(t, resp.ErrIterNotFound)
		assert.Empty(t, resp.ErrColumnNotFound)
		assert.NotNil(t, resp.Batch)
		assert.Equal(t, 3, resp.Batch.Length())
		rowIDs = resp.Batch.Vecs[2]
	}

	// delete by row id
	{
		resp := testWrite[txnengine.DeleteResp](
			t, s, txnMeta,
			txnengine.OpDelete,
			txnengine.DeleteReq{
				TableID:    relID,
				ColumnName: rowIDColumnName,
				Vector:     rowIDs,
			},
		)
		assert.Empty(t, resp.ErrReadOnly)
		assert.Empty(t, resp.ErrTableNotFound)
	}

	// read after delete
	{
		resp := testRead[txnengine.NewTableIterResp](
			t, s, txnMeta,
			txnengine.OpNewTableIter,
			txnengine.NewTableIterReq{
				TableID: relID,
			},
		)
		assert.NotEmpty(t, resp.IterID)
		assert.Empty(t, resp.ErrTableNotFound)
		iterID = resp.IterID
	}
	{
		resp := testRead[txnengine.ReadResp](
			t, s, txnMeta,
			txnengine.OpRead,
			txnengine.ReadReq{
				IterID:   iterID,
				ColNames: []string{"a", "b", rowIDColumnName},
			},
		)
		assert.Empty(t, resp.ErrIterNotFound)
		assert.Empty(t, resp.ErrColumnNotFound)
		assert.Nil(t, resp.Batch)
	}

	t.Run("log tail", func(t *testing.T) {
		testLogTail(t, newStorage)
	})

}

func testRead[
	Resp any,
	Req any,
](
	t *testing.T,
	s *Storage,
	txnMeta txn.TxnMeta,
	op uint32,
	req Req,
) (
	resp Resp,
) {

	buf := new(bytes.Buffer)
	err := gob.NewEncoder(buf).Encode(req)
	assert.Nil(t, err)

	res, err := s.Read(context.TODO(), txnMeta, op, buf.Bytes())
	assert.Nil(t, err)
	data, err := res.Read()
	assert.Nil(t, err)

	err = gob.NewDecoder(bytes.NewReader(data)).Decode(&resp)
	assert.Nil(t, err)

	return
}

func testWrite[
	Resp any,
	Req any,
](
	t *testing.T,
	s *Storage,
	txnMeta txn.TxnMeta,
	op uint32,
	req Req,
) (
	resp Resp,
) {

	buf := new(bytes.Buffer)
	err := gob.NewEncoder(buf).Encode(req)
	assert.Nil(t, err)

	data, err := s.Write(context.TODO(), txnMeta, op, buf.Bytes())
	assert.Nil(t, err)

	err = gob.NewDecoder(bytes.NewReader(data)).Decode(&resp)
	assert.Nil(t, err)

	return
}
