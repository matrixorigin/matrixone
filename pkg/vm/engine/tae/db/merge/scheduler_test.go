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
	"sync"
	"testing"
	"time"
)

func TestStopStartMerge(t *testing.T) {
	scheduler := Scheduler{
		executor: newMergeExecutor(&dbutils.Runtime{
			LockMergeService: dbutils.NewLockMergeService(),
		}, nil),
		stoppedTables: struct {
			sync.RWMutex
			m map[uint64]struct {
				time.Time
				indexes []string
			}
			indexes map[string]struct{}
		}{
			m: make(map[uint64]struct {
				time.Time
				indexes []string
			}),
			indexes: make(map[string]struct{}),
		},
	}

	cata := catalog.MockCatalog()
	defer cata.Close()
	txnMgr := txnbase.NewTxnManager(catalog.MockTxnStoreFactory(cata), catalog.MockTxnFactory(cata), types.NewMockHLCClock(1))
	txnMgr.Start(context.Background())
	defer txnMgr.Stop()
	txn1, _ := txnMgr.StartTxn(nil)
	db, err := cata.CreateDBEntry("db", "", "", txn1)
	require.NoError(t, err)
	catalog.MockSchema(1, 0)

	tblEntry1, err := db.CreateTableEntry(&catalog.Schema{Extra: &api.SchemaExtra{}}, txn1, nil)
	require.NoError(t, err)
	require.NoError(t, txn1.Commit(context.Background()))

	err = scheduler.StopMerge(tblEntry1)
	if err != nil {
		return
	}
	require.Equal(t, 1, len(scheduler.stoppedTables.m))
	require.Equal(t, 0, len(scheduler.stoppedTables.m[tblEntry1.GetID()].indexes))
	require.Equal(t, 0, len(scheduler.stoppedTables.indexes))

	constraintDef := engine.ConstraintDef{Cts: []engine.Constraint{&engine.IndexDef{Indexes: []*plan.IndexDef{{IndexTableName: "__mo_index_test"}}}}}
	marshal, err := constraintDef.MarshalBinary()
	require.NoError(t, err)

	txn2, _ := txnMgr.StartTxn(nil)
	mockSchema := &catalog.Schema{Extra: &api.SchemaExtra{}, Constraint: marshal}
	tblEntry2, err := db.CreateTableEntry(mockSchema, txn2, nil)
	require.NoError(t, txn2.Commit(context.Background()))
	err = scheduler.StopMerge(tblEntry2)
	require.NoError(t, err)
	require.Equal(t, 2, len(scheduler.stoppedTables.m))
	require.Equal(t, "__mo_index_test", scheduler.stoppedTables.m[tblEntry2.GetID()].indexes[0])
	_, ok := scheduler.stoppedTables.indexes["__mo_index_test"]
	require.True(t, ok)

	require.Error(t, scheduler.onTable(tblEntry1))
	require.Error(t, scheduler.onTable(tblEntry2))

	scheduler.StartMerge(tblEntry1.GetID())
	require.Equal(t, 1, len(scheduler.stoppedTables.m))
	scheduler.StartMerge(tblEntry2.GetID())
	require.Equal(t, 0, len(scheduler.stoppedTables.m))
	require.Equal(t, 0, len(scheduler.stoppedTables.indexes))
}
