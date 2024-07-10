// Copyright 2024 Matrix Origin
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

package test

import (
	"context"
	"fmt"
	"github.com/matrixorigin/matrixone/pkg/container/types"
	catalog2 "github.com/matrixorigin/matrixone/pkg/vm/engine/tae/catalog"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/containers"
	"github.com/stretchr/testify/require"
	"testing"
	"time"

	"github.com/matrixorigin/matrixone/pkg/catalog"
	"github.com/matrixorigin/matrixone/pkg/defines"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/test/testutil"
)

func Test_InsertRows(t *testing.T) {
	var (
		accountId    = catalog.System_Account
		tableName    = "test1"
		databaseName = "db1"
	)

	ctx := context.WithValue(context.Background(), defines.TenantIDKey{}, accountId)
	ctx, cancel := context.WithTimeout(ctx, time.Minute*5)
	defer cancel()

	disttaeEngine, taeHandler, rpcAgent, _ := testutil.CreateEngines(ctx, testutil.TestOptions{}, t)
	defer func() {
		disttaeEngine.Close(ctx)
		taeHandler.Close(true)
		rpcAgent.Close()
	}()

	txn, err := disttaeEngine.NewTxnOperator(ctx, disttaeEngine.Now())
	require.Nil(t, err)

	err = disttaeEngine.Engine.Create(ctx, databaseName, txn)
	require.Nil(t, err)

	db, err := disttaeEngine.Engine.Database(ctx, databaseName, txn)
	require.Nil(t, err)

	schema := catalog2.MockSchemaAll(10, 0)
	schema.Name = tableName

	defs, err := testutil.TableDefBySchema(schema)
	require.Nil(t, err)

	err = db.Create(ctx, tableName, defs)
	require.Nil(t, err)

	rel, err := db.Relation(ctx, tableName, nil)
	require.Nil(t, err)
	require.Contains(t, rel.GetTableName(), tableName)

	bat := catalog2.MockBatch(schema, 10)
	err = rel.Write(ctx, containers.ToCNBatch(bat))
	require.Nil(t, err)

	err = txn.Commit(ctx)
	require.Nil(t, err)

	err = disttaeEngine.Engine.TryToSubscribeTable(ctx, rel.GetDBID(ctx), rel.GetTableID(ctx))
	require.Nil(t, err)

	// check partition state, before flush
	{
		stats, err := disttaeEngine.GetPartitionStateStats(ctx, rel.GetDBID(ctx), rel.GetTableID(ctx))
		require.Nil(t, err)

		fmt.Println(stats.String())
		expect := testutil.PartitionStateStats{
			DataObjectsVisible:   testutil.PObjectStats{},
			DataObjectsInvisible: testutil.PObjectStats{},
			InmemRows:            testutil.PInmemRowsStats{VisibleCnt: 10},
			CheckpointCnt:        0,
		}

		require.Equal(t, expect, stats)

		//require.Equal(t, int(0), stats.DataObjects.Visible.ObjCnt)
		//require.Equal(t, int(0), stats.DataObjects.Invisible.ObjCnt)
		//
		//require.Equal(t, int(0), stats.DataObjects.Visible.RowCnt)
		//require.Equal(t, int(0), stats.DataObjects.Invisible.RowCnt)
		//
		//require.Equal(t, int(10), stats.InmemRows.VisibleCnt)
		//require.Equal(t, int(0), stats.InmemRows.InvisibleCnt)
		//
		//require.Equal(t, int(0), stats.CheckpointCnt)
		//
		//require.Equal(t, int(0), stats.DataObjects.Visible.BlkCnt)
		//require.Equal(t, int(0), stats.DataObjects.Invisible.BlkCnt)
	}

	err = taeHandler.GetDB().FlushTable(ctx, accountId, rel.GetDBID(ctx), rel.GetTableID(ctx), types.TimestampToTS(disttaeEngine.Now()))
	require.Nil(t, err)
	// check partition state, after flush
	{

		stats, err := disttaeEngine.GetPartitionStateStats(ctx, rel.GetDBID(ctx), rel.GetTableID(ctx))
		require.Nil(t, err)

		fmt.Println(stats.String())

		expect := testutil.PartitionStateStats{
			DataObjectsVisible:   testutil.PObjectStats{ObjCnt: 1, BlkCnt: 1, RowCnt: 10},
			DataObjectsInvisible: testutil.PObjectStats{ObjCnt: 1, BlkCnt: 1, RowCnt: 10},
			InmemRows:            testutil.PInmemRowsStats{},
			CheckpointCnt:        0,
		}

		require.Equal(t, expect, stats)

	}

}
