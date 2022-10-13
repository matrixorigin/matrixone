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
	apipb "github.com/matrixorigin/matrixone/pkg/pb/api"
	"github.com/matrixorigin/matrixone/pkg/pb/timestamp"
	"github.com/matrixorigin/matrixone/pkg/pb/txn"
	"github.com/matrixorigin/matrixone/pkg/txn/clock"
	"github.com/matrixorigin/matrixone/pkg/vm/engine"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/memoryengine"
	"github.com/stretchr/testify/assert"
)

func TestLogTail(t *testing.T) {

	ctx, cancel := context.WithTimeout(context.Background(), time.Hour)
	defer cancel()

	clock := clock.NewHLCClock(func() int64 {
		return time.Now().Unix()
	}, math.MaxInt)
	storage, err := NewMemoryStorage(
		mpool.MustNewZero(),
		SnapshotIsolation,
		clock,
		memoryengine.RandomIDGenerator,
	)
	assert.Nil(t, err)
	defer storage.Close(ctx)

	// txn
	txnMeta := txn.TxnMeta{
		ID:     []byte("1"),
		Status: txn.TxnStatus_Active,
		SnapshotTS: timestamp.Timestamp{
			PhysicalTime: 1,
		},
	}

	// get log tail
	{
		for _, tableID := range []uint64{1, 2, 3} {
			resp, err := testRead[apipb.SyncLogTailResp](
				ctx, t, storage, txnMeta,
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
			resp, err := testRead[apipb.SyncLogTailResp](
				ctx, t, storage, txnMeta,
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
	{
		resp, err := testWrite[memoryengine.CreateDatabaseResp](
			ctx, t, storage, txnMeta,
			memoryengine.OpCreateDatabase,
			memoryengine.CreateDatabaseReq{
				Name: "foo",
			},
		)
		assert.Nil(t, err)
		assert.NotEmpty(t, resp.ID)
	}
	// open database
	var dbID ID
	{
		resp, err := testRead[memoryengine.OpenDatabaseResp](
			ctx, t, storage, txnMeta,
			memoryengine.OpOpenDatabase,
			memoryengine.OpenDatabaseReq{
				Name: "foo",
			},
		)
		assert.Nil(t, err)
		assert.NotNil(t, resp.ID)
		dbID = resp.ID
	}
	// create relation
	{
		resp, err := testWrite[memoryengine.CreateRelationResp](
			ctx, t, storage, txnMeta,
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
		resp, err := testWrite[memoryengine.DeleteRelationResp](
			ctx, t, storage, txnMeta,
			memoryengine.OpDeleteRelation,
			memoryengine.DeleteRelationReq{
				DatabaseID: dbID,
				Name:       "table",
			},
		)
		assert.Nil(t, err)
		assert.NotEmpty(t, resp.ID)
	}
	// commit
	err = storage.Commit(ctx, txnMeta)
	assert.Nil(t, err)
	// get log tail
	txnMeta = txn.TxnMeta{
		ID:     []byte("w"),
		Status: txn.TxnStatus_Active,
		SnapshotTS: timestamp.Timestamp{
			PhysicalTime: 2,
		},
	}
	{
		for _, tableID := range []uint64{1, 2, 3} {
			resp, err := testRead[apipb.SyncLogTailResp](
				ctx, t, storage, txnMeta,
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
