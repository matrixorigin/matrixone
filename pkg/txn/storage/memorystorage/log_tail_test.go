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
	"math"
	"testing"
	"time"

	"github.com/matrixorigin/matrixone/pkg/common/mpool"
	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/container/vector"
	apipb "github.com/matrixorigin/matrixone/pkg/pb/api"
	"github.com/matrixorigin/matrixone/pkg/pb/timestamp"
	"github.com/matrixorigin/matrixone/pkg/testutil"
	"github.com/matrixorigin/matrixone/pkg/txn/clock"
	"github.com/matrixorigin/matrixone/pkg/txn/storage/memorystorage/memtable"
	"github.com/matrixorigin/matrixone/pkg/vm/engine"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/disttae"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/memoryengine"
	"github.com/stretchr/testify/assert"
)

func TestLogTail(t *testing.T) {
	memPool := mpool.MustNewZero()

	// storage
	ctx, cancel := context.WithTimeout(context.Background(), time.Hour)
	defer cancel()
	clock := clock.NewHLCClock(func() int64 {
		return time.Now().Unix()
	}, math.MaxInt)
	storage, err := NewMemoryStorage(
		memPool,
		SnapshotIsolation,
		clock,
		memoryengine.RandomIDGenerator,
	)
	assert.Nil(t, err)
	defer storage.Close(ctx)

	// get log tail
	{
		for _, tableID := range []uint64{1, 2, 3} {
			resp, err := testReadTx[apipb.SyncLogTailResp](
				ctx, t, storage, clock,
				memoryengine.OpGetLogTail,
				apipb.SyncLogTailReq{
					Table: &apipb.TableID{
						TbId: tableID,
					},
				},
			)
			assert.Nil(t, err)
			assert.Equal(t, 1, len(resp.Commands))
			cmd := resp.Commands[0]
			assert.Equal(t, apipb.Entry_Insert, cmd.EntryType)
			assert.Equal(t, tableID, cmd.TableId)
			assert.True(t, len(cmd.Bat.Attrs) > 0)
			assert.True(t, len(cmd.Bat.Vecs) > 0)
		}
	}

	// no log tail
	{
		for _, tableID := range []uint64{1, 2, 3} {
			resp, err := testReadTx[apipb.SyncLogTailResp](
				ctx, t, storage, clock,
				memoryengine.OpGetLogTail,
				apipb.SyncLogTailReq{
					CnHave: &timestamp.Timestamp{
						PhysicalTime: 999,
					},
					CnWant: &timestamp.Timestamp{
						PhysicalTime: 9999,
					},
					Table: &apipb.TableID{
						TbId: tableID,
					},
				},
			)
			assert.Nil(t, err)
			assert.Equal(t, 0, len(resp.Commands))
		}
	}

	// create then drop table, then get log tail
	// create database
	var dbID ID
	{
		resp, err := testWriteTx[memoryengine.CreateDatabaseResp](
			ctx, t, storage, clock,
			memoryengine.OpCreateDatabase,
			memoryengine.CreateDatabaseReq{
				Name: "foo",
			},
		)
		assert.Nil(t, err)
		assert.NotEmpty(t, resp.ID)
		dbID = resp.ID
	}

	// create relation
	{
		resp, err := testWriteTx[memoryengine.CreateRelationResp](
			ctx, t, storage, clock,
			memoryengine.OpCreateRelation,
			memoryengine.CreateRelationReq{
				DatabaseID: dbID,
				Name:       "table",
				Type:       memoryengine.RelationTable,
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
		assert.Nil(t, err)
		assert.NotEmpty(t, resp.ID)
	}

	// delete relation
	{
		resp, err := testWriteTx[memoryengine.DeleteRelationResp](
			ctx, t, storage, clock,
			memoryengine.OpDeleteRelation,
			memoryengine.DeleteRelationReq{
				DatabaseID: dbID,
				Name:       "table",
			},
		)
		assert.Nil(t, err)
		assert.NotEmpty(t, resp.ID)
	}

	// get log tail
	{
		for _, tableID := range []uint64{1, 2, 3} {
			resp, err := testReadTx[apipb.SyncLogTailResp](
				ctx, t, storage, clock,
				memoryengine.OpGetLogTail,
				apipb.SyncLogTailReq{
					Table: &apipb.TableID{
						TbId: tableID,
					},
				},
			)
			assert.Nil(t, err)
			_ = resp
		}
	}

}

func TestLogTailReplay(t *testing.T) {
	memPool := mpool.MustNewZero()

	// storage
	ctx, cancel := context.WithTimeout(context.Background(), time.Hour)
	defer cancel()
	clock := clock.NewHLCClock(func() int64 {
		return time.Now().Unix()
	}, math.MaxInt)
	storage, err := NewMemoryStorage(
		memPool,
		SnapshotIsolation,
		clock,
		memoryengine.RandomIDGenerator,
	)
	assert.Nil(t, err)
	defer storage.Close(ctx)

	// create database
	var dbID ID
	{
		resp, err := testWriteTx[memoryengine.CreateDatabaseResp](
			ctx, t, storage, clock,
			memoryengine.OpCreateDatabase,
			memoryengine.CreateDatabaseReq{
				Name: "foo",
			},
		)
		assert.Nil(t, err)
		assert.NotEmpty(t, resp.ID)
		dbID = resp.ID
	}

	// create relation
	var tableID ID
	{
		resp, err := testWriteTx[memoryengine.CreateRelationResp](
			ctx, t, storage, clock,
			memoryengine.OpCreateRelation,
			memoryengine.CreateRelationReq{
				DatabaseID: dbID,
				Name:       "table",
				Type:       memoryengine.RelationTable,
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
		assert.Nil(t, err)
		assert.NotEmpty(t, resp.ID)
		tableID = resp.ID
	}

	// insert
	{
		bat := testutil.NewBatchWithVectors(
			[]*vector.Vector{
				testutil.NewVector(
					1,
					types.T_int64.ToType(),
					memPool,
					false,
					[]int64{1},
				),
				testutil.NewVector(
					1,
					types.T_int64.ToType(),
					memPool,
					false,
					[]int64{2},
				),
			},
			[]int64{1},
		)
		bat.Attrs = []string{"a", "b"}
		resp, err := testWriteTx[memoryengine.WriteResp](
			ctx, t, storage, clock,
			memoryengine.OpWrite,
			memoryengine.WriteReq{
				TableID: tableID,
				Batch:   bat,
			},
		)
		assert.Nil(t, err)
		_ = resp
	}

	// partition
	partition := disttae.NewPartition()

	// get log tail and replay
	{
		resp, err := testReadTx[apipb.SyncLogTailResp](
			ctx, t, storage, clock,
			memoryengine.OpGetLogTail,
			apipb.SyncLogTailReq{
				Table: &apipb.TableID{
					TbId: uint64(tableID),
				},
			},
		)
		assert.Nil(t, err)
		assert.Equal(t, 1, len(resp.Commands))

		for _, cmd := range resp.Commands {
			switch cmd.EntryType {
			case apipb.Entry_Insert:
				err := partition.Insert(ctx, cmd.Bat)
				assert.Nil(t, err)
			case apipb.Entry_Delete:
				err := partition.Delete(ctx, cmd.Bat)
				assert.Nil(t, err)
			}
		}

		type Row struct {
			RowID disttae.RowID
			Value disttae.DataValue
		}
		var rows []Row

		tx := memtable.NewTransaction("d", memtable.Now(clock), memtable.SnapshotIsolation)
		iter := partition.Data.NewIter(tx)
		defer iter.Close()
		for ok := iter.First(); ok; ok = iter.Next() {
			key, value, err := iter.Read()
			assert.Nil(t, err)
			rows = append(rows, Row{
				RowID: key,
				Value: value,
			})
		}

		assert.Equal(t, 1, len(rows))
		assert.Equal(t, int64(1), rows[0].Value["a"].Value)
		assert.Equal(t, int64(2), rows[0].Value["b"].Value)

	}

	// delete and get replay tail
	{
		resp, err := testWriteTx[memoryengine.DeleteResp](
			ctx, t, storage, clock,
			memoryengine.OpDelete,
			memoryengine.DeleteReq{
				TableID:    tableID,
				ColumnName: "a",
				Vector:     testutil.NewVector(1, types.T_int64.ToType(), memPool, false, []int64{1}),
			},
		)
		assert.Nil(t, err)
		_ = resp
	}

	{
		resp, err := testReadTx[apipb.SyncLogTailResp](
			ctx, t, storage, clock,
			memoryengine.OpGetLogTail,
			apipb.SyncLogTailReq{
				Table: &apipb.TableID{
					TbId: uint64(tableID),
				},
			},
		)
		assert.Nil(t, err)
		assert.Equal(t, 2, len(resp.Commands))

		for _, cmd := range resp.Commands {
			switch cmd.EntryType {
			case apipb.Entry_Insert:
				err := partition.Insert(ctx, cmd.Bat)
				assert.Nil(t, err)
			case apipb.Entry_Delete:
				err := partition.Delete(ctx, cmd.Bat)
				assert.Nil(t, err)
			}
		}

		tx := memtable.NewTransaction("d", memtable.Now(clock), memtable.SnapshotIsolation)
		iter := partition.Data.NewIter(tx)
		defer iter.Close()
		n := 0
		for ok := iter.First(); ok; ok = iter.Next() {
			key, value, err := iter.Read()
			assert.Nil(t, err)
			n++
			_ = value
			_ = key
		}
		assert.Equal(t, 0, n)

	}

}
