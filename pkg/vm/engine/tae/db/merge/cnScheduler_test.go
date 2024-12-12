// Copyright 2024 Matrix Origin
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//	http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package merge

import (
	"context"
	"github.com/matrixorigin/matrixone/pkg/common/runtime"
	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/objectio"
	"github.com/matrixorigin/matrixone/pkg/pb/api"
	"github.com/matrixorigin/matrixone/pkg/taskservice"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/catalog"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/db/dbutils"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/txn/txnbase"
	"github.com/stretchr/testify/require"
	"slices"
	"testing"
	"time"
)

func TestScheduler_CNActiveObjectsString(t *testing.T) {
	memStorage := taskservice.NewMemTaskStorage()
	cnScheduler := NewTaskServiceGetter(func() (taskservice.TaskService, bool) {
		return taskservice.NewTaskService(runtime.DefaultRuntime(), memStorage), true
	})

	cata := catalog.MockCatalog()
	defer cata.Close()
	txnMgr := txnbase.NewTxnManager(catalog.MockTxnStoreFactory(cata), catalog.MockTxnFactory(cata), types.NewMockHLCClock(1))
	txnMgr.Start(context.Background())
	defer txnMgr.Stop()
	txn1, _ := txnMgr.StartTxn(nil)
	db, err := cata.CreateDBEntry("db", "", "", txn1)
	require.NoError(t, err)
	catalog.MockSchema(1, 0)
	tbl, err := db.CreateTableEntry(catalog.MockSchema(1, 0), txn1, nil)
	require.NoError(t, err)
	require.NoError(t, txn1.Commit(context.Background()))
	schema := tbl.GetLastestSchema(false)

	txn2, _ := txnMgr.StartTxn(nil)
	entry := newSortedDataEntryWithTableEntry(t, tbl, txn2, 0, 1, overlapSizeThreshold)
	stat := *entry.GetObjectStats()
	cnScheduler.addActiveObjects([]*catalog.ObjectEntry{entry})
	require.NotEmpty(t, cnScheduler.activeObjsString())

	cnScheduler.removeActiveObject([]objectio.ObjectId{*entry.ID()})
	require.Empty(t, cnScheduler.activeObjsString())

	taskEntry := &api.MergeTaskEntry{
		AccountId:   schema.AcInfo.TenantID,
		UserId:      schema.AcInfo.UserID,
		RoleId:      schema.AcInfo.RoleID,
		TblId:       tbl.ID,
		DbId:        tbl.GetDB().GetID(),
		TableName:   tbl.GetLastestSchema(false).Name,
		DbName:      tbl.GetDB().GetName(),
		ToMergeObjs: [][]byte{stat[:]},
	}

	require.NoError(t, cnScheduler.sendMergeTask(context.Background(), taskEntry))
	tasks, err := memStorage.QueryAsyncTask(context.Background())
	if err != nil {
		return
	}
	require.Equal(t, 1, len(tasks))
	meta := new(api.MergeTaskEntry)
	require.NoError(t, meta.Unmarshal(tasks[0].Metadata.Context))
	require.Equal(t, meta.DbName, tbl.GetDB().GetName())
	require.Error(t, cnScheduler.sendMergeTask(context.Background(), taskEntry))
}

func TestExecutorCNMerge(t *testing.T) {

	cata := catalog.MockCatalog()
	defer cata.Close()
	txnMgr := txnbase.NewTxnManager(catalog.MockTxnStoreFactory(cata), catalog.MockTxnFactory(cata), types.NewMockHLCClock(1))
	txnMgr.Start(context.Background())
	defer txnMgr.Stop()
	txn1, _ := txnMgr.StartTxn(nil)
	db, err := cata.CreateDBEntry("db", "", "", txn1)
	require.NoError(t, err)
	catalog.MockSchema(1, 0)
	tbl, err := db.CreateTableEntry(catalog.MockSchema(1, 0), txn1, nil)
	require.NoError(t, err)
	require.NoError(t, txn1.Commit(context.Background()))

	txn2, _ := txnMgr.StartTxn(nil)
	entry := newSortedDataEntryWithTableEntry(t, tbl, txn2, 0, 1, overlapSizeThreshold)

	memStorage := taskservice.NewMemTaskStorage()
	cnScheduler := NewTaskServiceGetter(func() (taskservice.TaskService, bool) {
		return taskservice.NewTaskService(runtime.DefaultRuntime(), memStorage), true
	})
	executor := newMergeExecutor(&dbutils.Runtime{}, cnScheduler)
	executor.executeFor(tbl, []*catalog.ObjectEntry{entry}, taskHostCN)
	require.NotEmpty(t, cnScheduler.activeObjsString())

	executor.executeFor(tbl, []*catalog.ObjectEntry{entry}, taskHostCN)
	entry2 := newSortedDataEntryWithTableEntry(t, tbl, txn2, 0, 1, overlapSizeThreshold)
	executor.executeFor(tbl, slices.Repeat([]*catalog.ObjectEntry{entry2}, 31), taskHostCN)

	executor.cnSched.prune(0, 0)
	executor.cnSched.prune(0, time.Hour)
}
