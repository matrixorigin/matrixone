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

package merge

import (
	"context"
	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/pb/api"
	"github.com/matrixorigin/matrixone/pkg/pb/plan"
	"github.com/matrixorigin/matrixone/pkg/vm/engine"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/catalog"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/db/dbutils"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/txn/txnbase"
	"github.com/stretchr/testify/require"
	"testing"
)

func TestStopStartMerge(t *testing.T) {
	scheduler := Scheduler{
		executor: newMergeExecutor(&dbutils.Runtime{
			LockMergeService: dbutils.NewLockMergeService(),
		}, nil),
	}

	lockService := scheduler.executor.rt.LockMergeService
	cata := catalog.MockCatalog()
	defer cata.Close()
	txnMgr := txnbase.NewTxnManager(catalog.MockTxnStoreFactory(cata), catalog.MockTxnFactory(cata), types.NewMockHLCClock(1))
	txnMgr.Start(context.Background())
	defer txnMgr.Stop()
	txn1, _ := txnMgr.StartTxn(nil)
	db, err := cata.CreateDBEntry("db", "", "", txn1)
	require.NoError(t, err)
	catalog.MockSchema(1, 0)

	tblEntry1, err := db.CreateTableEntry(&catalog.Schema{Name: "t1", Extra: &api.SchemaExtra{}}, txn1, nil)
	require.NoError(t, err)
	require.NoError(t, txn1.Commit(context.Background()))

	err = scheduler.StopMerge(tblEntry1, false)
	if err != nil {
		return
	}
	require.Equal(t, 1, len(lockService.LockedInfos()))
	require.Equal(t, 0, len(lockService.LockedInfos()[tblEntry1.GetID()].Indexes()))
	require.Equal(t, 0, len(lockService.Indexes()))

	constraintDef := engine.ConstraintDef{Cts: []engine.Constraint{&engine.IndexDef{Indexes: []*plan.IndexDef{{IndexTableName: "__mo_index_test"}}}}}
	marshal, err := constraintDef.MarshalBinary()
	require.NoError(t, err)

	txn2, _ := txnMgr.StartTxn(nil)
	mockSchema := &catalog.Schema{Name: "t2", Extra: &api.SchemaExtra{}, Constraint: marshal}
	tblEntry2, err := db.CreateTableEntry(mockSchema, txn2, nil)
	require.NoError(t, err)
	require.NoError(t, txn2.Commit(context.Background()))
	err = scheduler.StopMerge(tblEntry2, false)
	require.NoError(t, err)
	require.Equal(t, 2, len(lockService.LockedInfos()))
	require.Equal(t, "__mo_index_test", lockService.LockedInfos()[tblEntry2.GetID()].Indexes()[0])
	_, ok := lockService.Indexes()["__mo_index_test"]
	require.True(t, ok)

	require.Error(t, scheduler.onTable(tblEntry1))
	require.Error(t, scheduler.onTable(tblEntry2))

	scheduler.StartMerge(tblEntry1.GetID(), false)
	require.Equal(t, 1, len(lockService.LockedInfos()))
	scheduler.StartMerge(tblEntry2.GetID(), false)
	require.Equal(t, 0, len(lockService.LockedInfos()))
	require.Equal(t, 0, len(lockService.Indexes()))
}
