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

package testutil

import (
	"context"
	"fmt"
	"os"
	"os/user"
	"path/filepath"
	"strconv"
	"testing"

	"github.com/matrixorigin/matrixone/pkg/catalog"
	"github.com/matrixorigin/matrixone/pkg/common/mpool"
	"github.com/matrixorigin/matrixone/pkg/container/batch"
	"github.com/matrixorigin/matrixone/pkg/defines"
	"github.com/matrixorigin/matrixone/pkg/pb/plan"
	"github.com/matrixorigin/matrixone/pkg/pb/timestamp"
	"github.com/matrixorigin/matrixone/pkg/sql/colexec"
	testutil2 "github.com/matrixorigin/matrixone/pkg/testutil"
	"github.com/matrixorigin/matrixone/pkg/txn/client"
	"github.com/matrixorigin/matrixone/pkg/vm/engine"
	catalog2 "github.com/matrixorigin/matrixone/pkg/vm/engine/tae/catalog"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/containers"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func GetDefaultTestPath(module string, t *testing.T) string {
	usr, _ := user.Current()
	dirName := fmt.Sprintf("%s-ut-workspace", usr.Username)
	return filepath.Join("/tmp", dirName, module, t.Name())
}

func MakeDefaultTestPath(module string, t *testing.T) string {
	path := GetDefaultTestPath(module, t)
	err := os.MkdirAll(path, os.FileMode(0755))
	assert.Nil(t, err)
	return path
}

func RemoveDefaultTestPath(module string, t *testing.T) {
	path := GetDefaultTestPath(module, t)
	os.RemoveAll(path)
}

func InitTestEnv(module string, t *testing.T) string {
	RemoveDefaultTestPath(module, t)
	return MakeDefaultTestPath(module, t)
}

func CreateEngines(ctx context.Context, t *testing.T) (
	disttaeEngine *TestDisttaeEngine, taeEngine *TestTxnStorage, rpcAgent *MockRPCAgent, mp *mpool.MPool) {

	if v := ctx.Value(defines.TenantIDKey{}); v == nil {
		panic("cannot find account id in ctx")
	}

	var err error

	mp, err = mpool.NewMPool("test", 0, mpool.NoFixed)
	require.Nil(t, err)

	rpcAgent = NewMockLogtailAgent()

	taeEngine, err = NewTestTAEEngine(ctx, "partition_state", t, rpcAgent, nil)
	require.Nil(t, err)

	disttaeEngine, err = NewTestDisttaeEngine(ctx, mp, taeEngine.GetDB().Runtime.Fs.Service, rpcAgent)
	require.Nil(t, err)

	return
}

func CreateDatabaseOnly(ctx context.Context, databaseName string,
	disttaeEngine *TestDisttaeEngine, taeEngine *TestTxnStorage, rpcAgent *MockRPCAgent, t *testing.T) (
	dbHandler engine.Database, dbEntry *catalog2.DBEntry, txnOp client.TxnOperator) {

	var err error

	txnOp, err = disttaeEngine.NewTxnOperator(ctx, disttaeEngine.Now())
	require.Nil(t, err)

	resp, databaseId := rpcAgent.CreateDatabase(ctx, databaseName, disttaeEngine.Engine, txnOp)
	require.Nil(t, resp.TxnError)

	dbHandler, err = disttaeEngine.Engine.Database(ctx, databaseName, txnOp)
	require.Nil(t, err)
	require.NotNil(t, dbHandler)

	require.Equal(t, strconv.Itoa(int(databaseId)), dbHandler.GetDatabaseId(ctx))

	dbEntry, err = taeEngine.GetDB().Catalog.GetDatabaseByID(databaseId)
	require.Nil(t, err)
	require.Equal(t, dbEntry.ID, databaseId)

	return
}

func CreateTableOnly(ctx context.Context, schema *catalog2.Schema,
	dbHandler engine.Database, dbEntry *catalog2.DBEntry, rpcAgent *MockRPCAgent,
	snapshot timestamp.Timestamp, t *testing.T) (tblHandler engine.Relation, tblEntry *catalog2.TableEntry) {

	resp, tableId := rpcAgent.CreateTable(ctx, dbHandler, schema, snapshot)
	require.Nil(t, resp.TxnError)

	var err error

	tblEntry, err = dbEntry.GetTableEntryByID(tableId)
	require.Nil(t, err)
	require.Equal(t, tblEntry.ID, tableId)
	require.Contains(t, tblEntry.GetFullName(), schema.Name)

	tblHandler, err = dbHandler.Relation(ctx, schema.Name, nil)
	require.Nil(t, err)
	require.Equal(t, tblHandler.GetTableID(ctx), tableId)
	require.Contains(t, tblHandler.GetTableName(), schema.Name)

	return tblHandler, tblEntry
}

func CreateDatabaseAndTable(ctx context.Context, schema *catalog2.Schema,
	databaseName string, disttaeEngine *TestDisttaeEngine,
	taeEngine *TestTxnStorage, rpcAgent *MockRPCAgent, t *testing.T) (
	dbHandler engine.Database, dbEntry *catalog2.DBEntry,
	tblHandler engine.Relation, tblEntry *catalog2.TableEntry, txnOp client.TxnOperator) {

	dbHandler, dbEntry, txnOp = CreateDatabaseOnly(ctx, databaseName, disttaeEngine, taeEngine, rpcAgent, t)
	tblHandler, tblEntry = CreateTableOnly(ctx, schema, dbHandler, dbEntry, rpcAgent, disttaeEngine.Now(), t)

	return
}

func MockObjectStatsBatBySchema(tblDef *plan.TableDef, schema *catalog2.Schema,
	eachRowCnt, batCnt int, mp *mpool.MPool, t *testing.T) (statsBat *batch.Batch) {

	offset := 0
	bats := make([]*batch.Batch, batCnt)
	for idx := range bats {
		ret := containers.MockBatchWithAttrsAndOffset(schema.Types(), schema.Attrs(), eachRowCnt, offset)

		bat := containers.ToCNBatch(ret)
		bats[idx] = bat

		offset += eachRowCnt
	}

	proc := testutil2.NewProcessWithMPool(mp)
	s3Writer, err := colexec.AllocS3Writer(proc, tblDef)
	require.Nil(t, err)

	s3Writer.InitBuffers(proc, bats[0])

	for idx := range bats {
		s3Writer.Put(bats[idx], proc)
	}

	err = s3Writer.SortAndFlush(proc)
	require.Nil(t, err)

	statsBat = s3Writer.GetBlockInfoBat()
	require.Equal(t, statsBat.Attrs, []string{catalog.BlockMeta_TableIdx_Insert, catalog.BlockMeta_BlockInfo, catalog.ObjectMeta_ObjectStats})
	require.Equal(t, statsBat.Vecs[0].Length(), 100)
	require.Equal(t, statsBat.Vecs[1].Length(), 100)
	require.Equal(t, statsBat.Vecs[2].Length(), 1)

	return
}
