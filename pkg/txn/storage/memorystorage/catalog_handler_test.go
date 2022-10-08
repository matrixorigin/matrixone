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

	"github.com/google/uuid"
	"github.com/matrixorigin/matrixone/pkg/catalog"
	"github.com/matrixorigin/matrixone/pkg/pb/txn"
	"github.com/matrixorigin/matrixone/pkg/testutil"
	"github.com/matrixorigin/matrixone/pkg/txn/clock"
	"github.com/matrixorigin/matrixone/pkg/txn/storage/memorystorage/memtable"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/memoryengine"
	"github.com/stretchr/testify/assert"
)

func TestCatalogHandler(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	// new
	clock := clock.NewHLCClock(func() int64 {
		return time.Now().UnixNano()
	}, math.MaxInt64)
	catalogHandler, err := NewCatalogHandler(
		NewMemHandler(
			testutil.NewMheap(),
			memtable.Serializable,
			clock,
			memoryengine.RandomIDGenerator,
		),
	)
	assert.Nil(t, err)
	storage, err := New(catalogHandler)
	assert.Nil(t, err)

	// close
	defer func() {
		err := storage.Close(ctx)
		assert.Nil(t, err)
	}()

	handler := storage.handler
	meta := txn.TxnMeta{
		ID: []byte(uuid.NewString()),
	}

	// system db
	var dbID ID
	{
		var resp memoryengine.OpenDatabaseResp
		err = handler.HandleOpenDatabase(ctx, meta, memoryengine.OpenDatabaseReq{
			Name: catalog.MO_CATALOG,
		}, &resp)
		assert.Nil(t, err)
		dbID = resp.ID
		assert.NotEmpty(t, dbID)
	}

	// get tables
	{
		var resp memoryengine.GetRelationsResp
		err = handler.HandleGetRelations(ctx, meta, memoryengine.GetRelationsReq{
			DatabaseID: dbID,
		}, &resp)
		assert.Nil(t, err)
		assert.Equal(t, 3, len(resp.Names))
	}

	// mo_database
	var tableID ID
	{
		var resp memoryengine.OpenRelationResp
		err = handler.HandleOpenRelation(ctx, meta, memoryengine.OpenRelationReq{
			DatabaseID: dbID,
			Name:       catalog.MO_DATABASE,
		}, &resp)
		assert.Nil(t, err)
		tableID = resp.ID
		assert.NotEmpty(t, tableID)
	}
	var iterID ID
	{
		var resp memoryengine.NewTableIterResp
		err = handler.HandleNewTableIter(ctx, meta, memoryengine.NewTableIterReq{
			TableID: tableID,
		}, &resp)
		assert.Nil(t, err)
		iterID = resp.IterID
		assert.NotEmpty(t, iterID)
	}
	{
		var resp memoryengine.ReadResp
		err = handler.HandleRead(ctx, meta, memoryengine.ReadReq{
			IterID: iterID,
			ColNames: []string{
				catalog.SystemDBAttr_Name,
				catalog.SystemDBAttr_CatalogName,
			},
		}, &resp)
		assert.Nil(t, err)
		assert.Equal(t, 1, resp.Batch.Length())
		assert.Equal(t, 2, len(resp.Batch.Attrs))
	}
	{
		var resp memoryengine.CloseTableIterResp
		err = handler.HandleCloseTableIter(ctx, meta, memoryengine.CloseTableIterReq{
			IterID: iterID,
		}, &resp)
		assert.Nil(t, err)
	}

	// mo_tables
	{
		var resp memoryengine.OpenRelationResp
		err = handler.HandleOpenRelation(ctx, meta, memoryengine.OpenRelationReq{
			DatabaseID: dbID,
			Name:       catalog.MO_TABLES,
		}, &resp)
		assert.Nil(t, err)
		tableID = resp.ID
		assert.NotEmpty(t, tableID)
	}
	{
		var resp memoryengine.NewTableIterResp
		err = handler.HandleNewTableIter(ctx, meta, memoryengine.NewTableIterReq{
			TableID: tableID,
		}, &resp)
		assert.Nil(t, err)
		iterID = resp.IterID
		assert.NotEmpty(t, iterID)
	}
	{
		var resp memoryengine.ReadResp
		err = handler.HandleRead(ctx, meta, memoryengine.ReadReq{
			IterID: iterID,
			ColNames: []string{
				catalog.SystemRelAttr_Name,
				catalog.SystemRelAttr_DBName,
			},
		}, &resp)
		assert.Nil(t, err)
		assert.Equal(t, 3, resp.Batch.Length())
		assert.Equal(t, 2, len(resp.Batch.Attrs))
	}
	{
		var resp memoryengine.CloseTableIterResp
		err = handler.HandleCloseTableIter(ctx, meta, memoryengine.CloseTableIterReq{
			IterID: iterID,
		}, &resp)
		assert.Nil(t, err)
	}

	// mo_columns
	{
		var resp memoryengine.OpenRelationResp
		err = handler.HandleOpenRelation(ctx, meta, memoryengine.OpenRelationReq{
			DatabaseID: dbID,
			Name:       catalog.MO_COLUMNS,
		}, &resp)
		assert.Nil(t, err)
		tableID = resp.ID
		assert.NotEmpty(t, tableID)
	}
	{
		var resp memoryengine.NewTableIterResp
		err = handler.HandleNewTableIter(ctx, meta, memoryengine.NewTableIterReq{
			TableID: tableID,
		}, &resp)
		assert.Nil(t, err)
		iterID = resp.IterID
		assert.NotEmpty(t, iterID)
	}
	{
		var resp memoryengine.ReadResp
		err = handler.HandleRead(ctx, meta, memoryengine.ReadReq{
			IterID: iterID,
			ColNames: []string{
				catalog.SystemColAttr_DBName,
				catalog.SystemColAttr_RelName,
			},
		}, &resp)
		assert.Nil(t, err)
		totalColumns := len(catalog.MoDatabaseSchema) +
			len(catalog.MoTablesSchema) +
			len(catalog.MoColumnsSchema)
		assert.Equal(t, totalColumns, resp.Batch.Length())
		assert.Equal(t, 2, len(resp.Batch.Attrs))
	}
	{
		var resp memoryengine.CloseTableIterResp
		err = handler.HandleCloseTableIter(ctx, meta, memoryengine.CloseTableIterReq{
			IterID: iterID,
		}, &resp)
		assert.Nil(t, err)
	}

}
