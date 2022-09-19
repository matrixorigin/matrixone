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
	"context"
	"math"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/matrixorigin/matrixone/pkg/pb/txn"
	"github.com/matrixorigin/matrixone/pkg/testutil"
	"github.com/matrixorigin/matrixone/pkg/txn/clock"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/catalog"
	txnengine "github.com/matrixorigin/matrixone/pkg/vm/engine/txn"
	"github.com/stretchr/testify/assert"
)

func TestCatalogHandler(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	// new
	clock := clock.NewHLCClock(func() int64 {
		return time.Now().UnixNano()
	}, math.MaxInt64)
	storage, err := New(
		NewCatalogHandler(
			NewMemHandler(
				testutil.NewMheap(),
				Serializable,
				clock,
			),
		),
	)
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
	var dbID string
	{
		var resp txnengine.OpenDatabaseResp
		err = handler.HandleOpenDatabase(meta, txnengine.OpenDatabaseReq{
			Name: catalog.SystemDBName,
		}, &resp)
		assert.Nil(t, err)
		dbID = resp.ID
		assert.NotEmpty(t, dbID)
	}

	// get tables
	{
		var resp txnengine.GetRelationsResp
		err = handler.HandleGetRelations(meta, txnengine.GetRelationsReq{
			DatabaseID: dbID,
		}, &resp)
		assert.Nil(t, err)
		assert.Equal(t, 3, len(resp.Names))
	}

	// mo_database
	var tableID string
	{
		var resp txnengine.OpenRelationResp
		err = handler.HandleOpenRelation(meta, txnengine.OpenRelationReq{
			DatabaseID: dbID,
			Name:       catalog.SystemTable_DB_Name,
		}, &resp)
		assert.Nil(t, err)
		tableID = resp.ID
		assert.NotEmpty(t, tableID)
	}
	var iterID string
	{
		var resp txnengine.NewTableIterResp
		err = handler.HandleNewTableIter(meta, txnengine.NewTableIterReq{
			TableID: tableID,
		}, &resp)
		assert.Nil(t, err)
		iterID = resp.IterID
		assert.NotEmpty(t, iterID)
	}
	{
		var resp txnengine.ReadResp
		err = handler.HandleRead(meta, txnengine.ReadReq{
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
		var resp txnengine.CloseTableIterResp
		err = handler.HandleCloseTableIter(meta, txnengine.CloseTableIterReq{
			IterID: iterID,
		}, &resp)
		assert.Nil(t, err)
	}

	// mo_tables
	{
		var resp txnengine.OpenRelationResp
		err = handler.HandleOpenRelation(meta, txnengine.OpenRelationReq{
			DatabaseID: dbID,
			Name:       catalog.SystemTable_Table_Name,
		}, &resp)
		assert.Nil(t, err)
		tableID = resp.ID
		assert.NotEmpty(t, tableID)
	}
	{
		var resp txnengine.NewTableIterResp
		err = handler.HandleNewTableIter(meta, txnengine.NewTableIterReq{
			TableID: tableID,
		}, &resp)
		assert.Nil(t, err)
		iterID = resp.IterID
		assert.NotEmpty(t, iterID)
	}
	{
		var resp txnengine.ReadResp
		err = handler.HandleRead(meta, txnengine.ReadReq{
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
		var resp txnengine.CloseTableIterResp
		err = handler.HandleCloseTableIter(meta, txnengine.CloseTableIterReq{
			IterID: iterID,
		}, &resp)
		assert.Nil(t, err)
	}

	// mo_columns
	{
		var resp txnengine.OpenRelationResp
		err = handler.HandleOpenRelation(meta, txnengine.OpenRelationReq{
			DatabaseID: dbID,
			Name:       catalog.SystemTable_Columns_Name,
		}, &resp)
		assert.Nil(t, err)
		tableID = resp.ID
		assert.NotEmpty(t, tableID)
	}
	{
		var resp txnengine.NewTableIterResp
		err = handler.HandleNewTableIter(meta, txnengine.NewTableIterReq{
			TableID: tableID,
		}, &resp)
		assert.Nil(t, err)
		iterID = resp.IterID
		assert.NotEmpty(t, iterID)
	}
	{
		var resp txnengine.ReadResp
		err = handler.HandleRead(meta, txnengine.ReadReq{
			IterID: iterID,
			ColNames: []string{
				catalog.SystemColAttr_DBName,
				catalog.SystemColAttr_RelName,
			},
		}, &resp)
		assert.Nil(t, err)
		totalColumns := len(catalog.SystemDBSchema.ColDefs) +
			len(catalog.SystemTableSchema.ColDefs) +
			len(catalog.SystemColumnSchema.ColDefs)
		assert.Equal(t, totalColumns, resp.Batch.Length())
		assert.Equal(t, 2, len(resp.Batch.Attrs))
	}
	{
		var resp txnengine.CloseTableIterResp
		err = handler.HandleCloseTableIter(meta, txnengine.CloseTableIterReq{
			IterID: iterID,
		}, &resp)
		assert.Nil(t, err)
	}

}
