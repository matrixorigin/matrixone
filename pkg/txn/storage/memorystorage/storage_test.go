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

package memorystorage

import (
	"context"
	"encoding"
	"testing"
	"time"

	"github.com/matrixorigin/matrixone/pkg/pb/plan"

	"github.com/matrixorigin/matrixone/pkg/common/moerr"
	"github.com/matrixorigin/matrixone/pkg/common/mpool"
	"github.com/matrixorigin/matrixone/pkg/container/batch"
	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/container/vector"
	"github.com/matrixorigin/matrixone/pkg/pb/timestamp"
	"github.com/matrixorigin/matrixone/pkg/pb/txn"
	"github.com/matrixorigin/matrixone/pkg/testutil"
	"github.com/matrixorigin/matrixone/pkg/vm/engine"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/memoryengine"
	"github.com/stretchr/testify/assert"
)

func testDatabase(
	t *testing.T,
	newStorage func() (*Storage, error),
) {
	mp := mpool.MustNewZero()
	ctx, cancel := context.WithTimeout(context.Background(), time.Hour)
	defer cancel()

	// new
	s, err := newStorage()
	assert.Nil(t, err)
	defer s.Close(ctx)

	// txn
	txnMeta := txn.TxnMeta{
		ID:     []byte("1"),
		Status: txn.TxnStatus_Active,
		SnapshotTS: timestamp.Timestamp{
			PhysicalTime: 1,
			LogicalTime:  1,
		},
	}
	defer func() {
		_, err := s.Commit(ctx, txnMeta)
		assert.Nil(t, err)
	}()

	// open database
	{
		resp := &memoryengine.OpenDatabaseResp{}
		err := testRead(
			ctx, t, s, txnMeta,
			memoryengine.OpOpenDatabase,
			&memoryengine.OpenDatabaseReq{
				Name: "foo",
			},
			resp,
		)
		assert.True(t, moerr.IsMoErrCode(err, moerr.ErrNoDB))
	}

	// create database
	{
		resp := &memoryengine.CreateDatabaseResp{}
		err := testWrite(
			ctx, t, s, txnMeta,
			memoryengine.OpCreateDatabase,
			&memoryengine.CreateDatabaseReq{
				Name: "foo",
			},
			resp,
		)
		assert.Nil(t, err)
		assert.NotEmpty(t, resp.ID)
	}

	// get databases
	{
		resp := &memoryengine.GetDatabasesResp{}
		err := testRead(
			ctx, t, s, txnMeta,
			memoryengine.OpGetDatabases,
			&memoryengine.GetDatabasesReq{},
			resp,
		)
		assert.Nil(t, err)
		assert.Equal(t, 1, len(resp.Names))
		assert.Equal(t, "foo", resp.Names[0])
	}

	// open database
	var dbID ID
	{
		resp := &memoryengine.OpenDatabaseResp{}
		err := testRead(
			ctx, t, s, txnMeta,
			memoryengine.OpOpenDatabase,
			&memoryengine.OpenDatabaseReq{
				Name: "foo",
			},
			resp,
		)
		assert.Nil(t, err)
		assert.NotNil(t, resp.ID)
		dbID = resp.ID

		// delete database
		defer func() {
			{
				resp := &memoryengine.DeleteDatabaseResp{}
				err := testWrite(
					ctx, t, s, txnMeta,
					memoryengine.OpDeleteDatabase,
					&memoryengine.DeleteDatabaseReq{
						Name: "foo",
					},
					resp,
				)
				assert.Nil(t, err)
				assert.NotEmpty(t, resp.ID)
			}
			{
				resp := &memoryengine.GetDatabasesResp{}
				err := testRead(
					ctx, t, s, txnMeta,
					memoryengine.OpGetDatabases,
					&memoryengine.GetDatabasesReq{},
					resp,
				)
				assert.Nil(t, err)
				for _, name := range resp.Names {
					if name == "foo" {
						t.Fatal()
					}
				}
			}
		}()
	}

	// open relation
	{
		resp := &memoryengine.OpenRelationResp{}
		err := testRead(
			ctx, t, s, txnMeta,
			memoryengine.OpOpenRelation,
			&memoryengine.OpenRelationReq{
				DatabaseID: dbID,
				Name:       "table",
			},
			resp,
		)
		assert.True(t, moerr.IsMoErrCode(err, moerr.ErrNoSuchTable))
	}

	// create relation
	{
		defA := &engine.AttributeDef{
			Attr: engine.Attribute{
				Name:    "a",
				Type:    types.T_int64.ToType(),
				Primary: true,
			},
		}
		defB := &engine.AttributeDef{
			Attr: engine.Attribute{
				Name:    "b",
				Type:    types.T_int64.ToType(),
				Primary: false,
			},
		}
		defC := &engine.ConstraintDef{
			Cts: []engine.Constraint{
				&engine.PrimaryKeyDef{
					Pkey: &plan.PrimaryKeyDef{
						PkeyColName: "a",
						Names:       []string{"a"},
					},
				},
			},
		}

		resp := &memoryengine.CreateRelationResp{}
		err := testWrite(
			ctx, t, s, txnMeta,
			memoryengine.OpCreateRelation,
			&memoryengine.CreateRelationReq{
				DatabaseID: dbID,
				Name:       "table",
				Type:       memoryengine.RelationTable,
				Defs: []engine.TableDefPB{
					defA.ToPBVersion(),
					defB.ToPBVersion(),
					defC.ToPBVersion(),
				},
			},
			resp,
		)
		assert.Nil(t, err)
		assert.NotEmpty(t, resp.ID)
	}

	// get relations
	{
		resp := &memoryengine.GetRelationsResp{}
		err := testRead(
			ctx, t, s, txnMeta,
			memoryengine.OpGetRelations,
			&memoryengine.GetRelationsReq{
				DatabaseID: dbID,
			},
			resp,
		)
		assert.Nil(t, err)
		assert.Equal(t, 1, len(resp.Names))
		assert.Equal(t, "table", resp.Names[0])
	}

	// open relation
	var relID ID
	{
		resp := &memoryengine.OpenRelationResp{}
		err := testRead(
			ctx, t, s, txnMeta,
			memoryengine.OpOpenRelation,
			&memoryengine.OpenRelationReq{
				DatabaseID: dbID,
				Name:       "table",
			},
			resp,
		)
		assert.Nil(t, err)
		assert.NotNil(t, resp.ID)
		relID = resp.ID
		assert.Equal(t, memoryengine.RelationTable, resp.Type)
	}
	_ = relID

	// get relation defs
	{
		resp := &memoryengine.GetTableDefsResp{}
		err := testRead(
			ctx, t, s, txnMeta,
			memoryengine.OpGetTableDefs,
			&memoryengine.GetTableDefsReq{
				TableID: relID,
			},
			resp,
		)
		assert.Nil(t, err)
		assert.Equal(t, 3, len(resp.Defs))
	}

	// write
	{
		colA := testutil.NewVector(
			5,
			types.T_int64.ToType(),
			mp,
			false,
			[]int64{
				1, 2, 3, 4, 5,
			},
		)
		colB := testutil.NewVector(
			5,
			types.T_int64.ToType(),
			mp,
			false,
			[]int64{
				6, 7, 8, 9, 10,
			},
		)
		bat := batch.New(false, []string{"a", "b"})
		bat.Vecs[0] = colA
		bat.Vecs[1] = colB
		bat.InitZsOne(5)
		resp := &memoryengine.WriteResp{}
		err := testWrite(
			ctx, t, s, txnMeta,
			memoryengine.OpWrite,
			&memoryengine.WriteReq{
				TableID: relID,
				Batch:   bat,
			},
			resp,
		)
		assert.Nil(t, err)
	}

	// read
	var iterID ID
	{
		resp := &memoryengine.NewTableIterResp{}
		err := testRead(
			ctx, t, s, txnMeta,
			memoryengine.OpNewTableIter,
			&memoryengine.NewTableIterReq{
				TableID: relID,
			},
			resp,
		)
		assert.Nil(t, err)
		assert.NotEmpty(t, resp.IterID)
		iterID = resp.IterID
	}
	{
		resp := &memoryengine.ReadResp{}
		err := testRead(
			ctx, t, s, txnMeta,
			memoryengine.OpRead,
			&memoryengine.ReadReq{
				IterID:   iterID,
				ColNames: []string{"a", "b"},
			},
			resp,
		)
		assert.Nil(t, err)
		assert.NotNil(t, resp.Batch)
		assert.Equal(t, 5, resp.Batch.Length())
	}

	// delete by primary key
	{
		colA := testutil.NewVector(
			1,
			types.T_int64.ToType(),
			mp,
			false,
			[]int64{
				1,
			},
		)
		resp := &memoryengine.DeleteResp{}
		err := testWrite(
			ctx, t, s, txnMeta,
			memoryengine.OpDelete,
			&memoryengine.DeleteReq{
				TableID:    relID,
				ColumnName: "a",
				Vector:     colA,
			},
			resp,
		)
		assert.Nil(t, err)
	}

	// read after delete
	{
		resp := &memoryengine.NewTableIterResp{}
		err := testRead(
			ctx, t, s, txnMeta,
			memoryengine.OpNewTableIter,
			&memoryengine.NewTableIterReq{
				TableID: relID,
			},
			resp,
		)
		assert.Nil(t, err)
		assert.NotEmpty(t, resp.IterID)
		iterID = resp.IterID
	}
	{
		resp := &memoryengine.ReadResp{}
		err := testRead(
			ctx, t, s, txnMeta,
			memoryengine.OpRead,
			&memoryengine.ReadReq{
				IterID:   iterID,
				ColNames: []string{"a", "b"},
			},
			resp,
		)
		assert.Nil(t, err)
		assert.NotNil(t, resp.Batch)
		assert.Equal(t, 4, resp.Batch.Length())
	}

	// delete by non-primary key
	{
		colB := testutil.NewVector(
			1,
			types.T_int64.ToType(),
			mp,
			false,
			[]int64{
				8,
			},
		)
		resp := &memoryengine.DeleteResp{}
		err := testWrite(
			ctx, t, s, txnMeta,
			memoryengine.OpDelete,
			&memoryengine.DeleteReq{
				TableID:    relID,
				ColumnName: "b",
				Vector:     colB,
			},
			resp,
		)
		assert.Nil(t, err)
	}

	// read after delete
	{
		resp := &memoryengine.NewTableIterResp{}
		err := testRead(
			ctx, t, s, txnMeta,
			memoryengine.OpNewTableIter,
			&memoryengine.NewTableIterReq{
				TableID: relID,
			},
			resp,
		)
		assert.Nil(t, err)
		assert.NotEmpty(t, resp.IterID)
		iterID = resp.IterID
	}
	{
		resp := &memoryengine.ReadResp{}
		err := testRead(
			ctx, t, s, txnMeta,
			memoryengine.OpRead,
			&memoryengine.ReadReq{
				IterID:   iterID,
				ColNames: []string{"a", "b"},
			},
			resp,
		)
		assert.Nil(t, err)
		assert.NotNil(t, resp.Batch)
		assert.Equal(t, 3, resp.Batch.Length())
	}

	// write after delete
	{
		colA := testutil.NewVector(
			1,
			types.T_int64.ToType(),
			mp,
			false,
			[]int64{
				1,
			},
		)
		colB := testutil.NewVector(
			1,
			types.T_int64.ToType(),
			mp,
			false,
			[]int64{
				6,
			},
		)
		bat := batch.New(false, []string{"a", "b"})
		bat.Vecs[0] = colA
		bat.Vecs[1] = colB
		bat.InitZsOne(1)
		resp := &memoryengine.WriteResp{}
		err := testWrite(
			ctx, t, s, txnMeta,
			memoryengine.OpWrite,
			&memoryengine.WriteReq{
				TableID: relID,
				Batch:   bat,
			},
			resp,
		)
		assert.Nil(t, err)
	}

	// delete relation
	{
		resp := &memoryengine.DeleteRelationResp{}
		err := testWrite(
			ctx, t, s, txnMeta,
			memoryengine.OpDeleteRelation,
			&memoryengine.DeleteRelationReq{
				DatabaseID: dbID,
				Name:       "table",
			},
			resp,
		)
		assert.Nil(t, err)
		assert.NotEmpty(t, resp.ID)
	}
	{
		resp := &memoryengine.GetRelationsResp{}
		err := testRead(
			ctx, t, s, txnMeta,
			memoryengine.OpGetRelations,
			&memoryengine.GetRelationsReq{
				DatabaseID: dbID,
			},
			resp,
		)
		assert.Nil(t, err)
		assert.Equal(t, 0, len(resp.Names))
	}

	// new relation without primary key
	{
		defA := &engine.AttributeDef{
			Attr: engine.Attribute{
				Name: "a",
				Type: types.T_int64.ToType(),
			},
		}
		defB := &engine.AttributeDef{
			Attr: engine.Attribute{
				Name: "b",
				Type: types.T_int64.ToType(),
			},
		}

		resp := &memoryengine.CreateRelationResp{}
		err := testWrite(
			ctx, t, s, txnMeta,
			memoryengine.OpCreateRelation,
			&memoryengine.CreateRelationReq{
				DatabaseID: dbID,
				Name:       "table",
				Type:       memoryengine.RelationTable,
				Defs: []engine.TableDefPB{
					defA.ToPBVersion(),
					defB.ToPBVersion(),
				},
			},
			resp,
		)
		assert.Nil(t, err)
		assert.NotEmpty(t, resp.ID)
		relID = resp.ID
	}

	// write
	{
		colA := testutil.NewVector(
			5,
			types.T_int64.ToType(),
			mp,
			false,
			[]int64{
				1, 2, 3, 4, 5,
			},
		)
		colB := testutil.NewVector(
			5,
			types.T_int64.ToType(),
			mp,
			false,
			[]int64{
				6, 7, 8, 9, 10,
			},
		)
		bat := batch.New(false, []string{"a", "b"})
		bat.Vecs[0] = colA
		bat.Vecs[1] = colB
		bat.InitZsOne(5)
		resp := &memoryengine.WriteResp{}
		err := testWrite(
			ctx, t, s, txnMeta,
			memoryengine.OpWrite,
			&memoryengine.WriteReq{
				TableID: relID,
				Batch:   bat,
			},
			resp,
		)
		assert.Nil(t, err)
	}

	// delete by primary key
	{
		colA := testutil.NewVector(
			1,
			types.T_int64.ToType(),
			mp,
			false,
			[]int64{
				1,
			},
		)
		resp := &memoryengine.DeleteResp{}
		err := testWrite(
			ctx, t, s, txnMeta,
			memoryengine.OpDelete,
			&memoryengine.DeleteReq{
				TableID:    relID,
				ColumnName: "a",
				Vector:     colA,
			},
			resp,
		)
		assert.Nil(t, err)
	}

	// read after delete
	{
		resp := &memoryengine.NewTableIterResp{}
		err := testRead(
			ctx, t, s, txnMeta,
			memoryengine.OpNewTableIter,
			&memoryengine.NewTableIterReq{
				TableID: relID,
			},
			resp,
		)
		assert.Nil(t, err)
		assert.NotEmpty(t, resp.IterID)
		iterID = resp.IterID
	}
	{
		resp := &memoryengine.ReadResp{}
		err := testRead(
			ctx, t, s, txnMeta,
			memoryengine.OpRead,
			&memoryengine.ReadReq{
				IterID:   iterID,
				ColNames: []string{"a", "b"},
			},
			resp,
		)
		assert.Nil(t, err)
		assert.NotNil(t, resp.Batch)
		assert.Equal(t, 4, resp.Batch.Length())
	}

	// delete by non-primary key
	{
		colB := testutil.NewVector(
			1,
			types.T_int64.ToType(),
			mp,
			false,
			[]int64{
				8,
			},
		)
		resp := &memoryengine.DeleteResp{}
		err := testWrite(
			ctx, t, s, txnMeta,
			memoryengine.OpDelete,
			&memoryengine.DeleteReq{
				TableID:    relID,
				ColumnName: "b",
				Vector:     colB,
			},
			resp,
		)
		assert.Nil(t, err)
	}

	// read after delete
	{
		resp := &memoryengine.NewTableIterResp{}
		err := testRead(
			ctx, t, s, txnMeta,
			memoryengine.OpNewTableIter,
			&memoryengine.NewTableIterReq{
				TableID: relID,
			},
			resp,
		)
		assert.Nil(t, err)
		assert.NotEmpty(t, resp.IterID)
		iterID = resp.IterID
	}
	var rowIDs *vector.Vector
	{
		resp := &memoryengine.ReadResp{}
		err := testRead(
			ctx, t, s, txnMeta,
			memoryengine.OpRead,
			&memoryengine.ReadReq{
				IterID:   iterID,
				ColNames: []string{"a", "b", rowIDColumnName},
			},
			resp,
		)
		assert.Nil(t, err)
		assert.NotNil(t, resp.Batch)
		assert.Equal(t, 3, resp.Batch.Length())
		rowIDs = resp.Batch.Vecs[2]
	}

	// delete by row id
	{
		resp := &memoryengine.DeleteResp{}
		err := testWrite(
			ctx, t, s, txnMeta,
			memoryengine.OpDelete,
			&memoryengine.DeleteReq{
				TableID:    relID,
				ColumnName: rowIDColumnName,
				Vector:     rowIDs,
			},
			resp,
		)
		assert.Nil(t, err)
	}

	// read after delete
	{
		resp := &memoryengine.NewTableIterResp{}
		err := testRead(
			ctx, t, s, txnMeta,
			memoryengine.OpNewTableIter,
			&memoryengine.NewTableIterReq{
				TableID: relID,
			},
			resp,
		)
		assert.Nil(t, err)
		assert.NotEmpty(t, resp.IterID)
		iterID = resp.IterID
	}
	{
		resp := &memoryengine.ReadResp{}
		err := testRead(
			ctx, t, s, txnMeta,
			memoryengine.OpRead,
			&memoryengine.ReadReq{
				IterID:   iterID,
				ColNames: []string{"a", "b", rowIDColumnName},
			},
			resp,
		)
		assert.Nil(t, err)
		assert.Nil(t, resp.Batch)
	}

	t.Run("duplicated db", func(t *testing.T) {
		tx1 := txn.TxnMeta{
			ID:     []byte("1"),
			Status: txn.TxnStatus_Active,
			SnapshotTS: timestamp.Timestamp{
				PhysicalTime: 1,
				LogicalTime:  1,
			},
		}
		tx2 := txn.TxnMeta{
			ID:     []byte("1"),
			Status: txn.TxnStatus_Active,
			SnapshotTS: timestamp.Timestamp{
				PhysicalTime: 1,
				LogicalTime:  1,
			},
		}
		{
			resp := &memoryengine.CreateDatabaseResp{}
			err := testWrite(
				ctx, t, s, tx1,
				memoryengine.OpCreateDatabase,
				&memoryengine.CreateDatabaseReq{
					Name: "bar",
				},
				resp,
			)
			assert.Nil(t, err)
			assert.NotEmpty(t, resp.ID)
		}
		{
			resp := &memoryengine.CreateDatabaseResp{}
			err := testWrite(
				ctx, t, s, tx2,
				memoryengine.OpCreateDatabase,
				&memoryengine.CreateDatabaseReq{
					Name: "bar",
				},
				resp,
			)
			assert.NotNil(t, err)
		}
	})

}

func testRead[
	Resp encoding.BinaryUnmarshaler,
	Req encoding.BinaryMarshaler,
](
	ctx context.Context,
	t *testing.T,
	s *Storage,
	txnMeta txn.TxnMeta,
	op uint32,
	req Req,
	resp Resp,
) error {

	buf, err := req.MarshalBinary()
	if err != nil {
		return err
	}

	res, err := s.Read(ctx, txnMeta, op, buf)
	if err != nil {
		return err
	}

	data, err := res.Read()
	if err != nil {
		return err
	}

	return resp.UnmarshalBinary(data)
}

func testWrite[
	Resp encoding.BinaryUnmarshaler,
	Req encoding.BinaryMarshaler,
](
	ctx context.Context,
	t *testing.T,
	s *Storage,
	txnMeta txn.TxnMeta,
	op uint32,
	req Req,
	resp Resp,
) error {

	buf, err := req.MarshalBinary()
	if err != nil {
		return err
	}

	data, err := s.Write(ctx, txnMeta, op, buf)
	if err != nil {
		return err
	}

	return resp.UnmarshalBinary(data)
}
