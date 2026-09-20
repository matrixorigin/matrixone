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

package idxcron

import (
	"context"
	"testing"
	"time"

	"github.com/matrixorigin/matrixone/pkg/catalog"
	"github.com/matrixorigin/matrixone/pkg/common/moerr"
	"github.com/matrixorigin/matrixone/pkg/common/mpool"
	"github.com/matrixorigin/matrixone/pkg/container/batch"
	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/container/vector"
	"github.com/matrixorigin/matrixone/pkg/defines"
	indexplugin "github.com/matrixorigin/matrixone/pkg/indexplugin"
	catalogplugin "github.com/matrixorigin/matrixone/pkg/indexplugin/catalog"
	compileplugin "github.com/matrixorigin/matrixone/pkg/indexplugin/compile"
	idxcronplugin "github.com/matrixorigin/matrixone/pkg/indexplugin/idxcron"
	planplugin "github.com/matrixorigin/matrixone/pkg/indexplugin/plan"
	"github.com/matrixorigin/matrixone/pkg/pb/plan"
	"github.com/matrixorigin/matrixone/pkg/sql/parsers/tree"
	"github.com/matrixorigin/matrixone/pkg/txn/client"
	"github.com/matrixorigin/matrixone/pkg/util/executor"
	ivfflatidxcron "github.com/matrixorigin/matrixone/pkg/vectorindex/ivfflat/plugin/idxcron"
	"github.com/matrixorigin/matrixone/pkg/vectorindex/sqlexec"
	"github.com/matrixorigin/matrixone/pkg/vm/engine"
	"github.com/prashantv/gostub"
	"github.com/stretchr/testify/require"
)

type TestTask struct {
	jstr      string
	dsize     int64
	nlists    int64
	ts        types.Timestamp
	createdAt types.Timestamp
	hour      int
	skipped   bool
	expected  bool
}

func stubIdxcronTxnRunner() *gostub.Stubs {
	return gostub.Stub(&runTxnWithSqlContext, func(
		ctx context.Context,
		_ engine.Engine,
		_ client.TxnClient,
		cnUUID string,
		accountID uint32,
		duration time.Duration,
		resolveVariableFunc func(string, bool, bool) (interface{}, error),
		data any,
		fn func(*sqlexec.SqlProcess, any) error,
	) error {
		txnCtx := context.WithValue(ctx, defines.TenantIDKey{}, accountID)
		txnCtx, cancel := context.WithTimeout(txnCtx, duration)
		defer cancel()
		return fn(sqlexec.NewSqlProcessWithContext(sqlexec.NewSqlContext(
			txnCtx, cnUUID, nil, accountID, resolveVariableFunc,
		)), data)
	})
}

func getTestCases(t *testing.T) []TestTask {

	tasks := []TestTask{
		{
			// data size < nlist
			jstr: `{"cfg":{"kmeans_train_percent":{"t":"F", "v":1},
        "kmeans_max_iteration":{"t":"I", "v":4},
        "ivf_threads_build":{"t":"I", "v":8}
        }}`,
			dsize:     int64(100),
			nlists:    int64(1000),
			ts:        types.UnixToTimestamp(0),
			createdAt: types.UnixToTimestamp(time.Now().Unix()),
			hour:      3,
			skipped:   false,
			expected:  false,
		},

		{
			// just CreatedAt and skip update
			jstr: `{"cfg":{"kmeans_train_percent":{"t":"F", "v":1},
        "kmeans_max_iteration":{"t":"I", "v":4},
        "ivf_threads_build":{"t":"I", "v":8}
        }}`,
			dsize:     int64(1000000),
			nlists:    int64(1000),
			ts:        types.UnixToTimestamp(0),
			createdAt: types.UnixToTimestamp(time.Now().Unix()),
			hour:      3,
			skipped:   false,
			expected:  false,
		},

		{
			jstr: `{"cfg":{"kmeans_train_percent":{"t":"F", "v":1},
        "kmeans_max_iteration":{"t":"I", "v":4},
        "ivf_threads_build":{"t":"I", "v":8}
        }}`,
			dsize:     int64(1000000),
			nlists:    int64(1000),
			ts:        types.UnixToTimestamp(0),
			createdAt: types.UnixToTimestamp(time.Now().Add(-4 * OneWeek).Unix()),
			hour:      3,
			skipped:   false,
			expected:  true,
		},

		{
			jstr: `{"cfg":{"kmeans_train_percent":{"t":"F", "v":10},
	"kmeans_max_iteration":{"t":"I", "v":4},
	"ivf_threads_build":{"t":"I", "v":8}
	}}`,
			dsize:    int64(1000000),
			nlists:   int64(1000),
			ts:       types.UnixToTimestamp(0),
			hour:     3,
			skipped:  false,
			expected: true,
		},

		{
			jstr: `{"cfg":{"kmeans_train_percent":{"t":"F", "v":10},
        "kmeans_max_iteration":{"t":"I", "v":4},
        "ivf_threads_build":{"t":"I", "v":8}
        }}`,
			dsize:     int64(1000000),
			nlists:    int64(1000),
			createdAt: types.UnixToTimestamp(time.Now().Add(-4 * OneWeek).Unix()),
			ts: func() types.Timestamp {
				now := time.Now()
				unixts := now.Add(-2 * OneWeek).Unix()
				return types.UnixToTimestamp(unixts)
			}(),
			hour:     3,
			skipped:  false,
			expected: true,
		},

		{
			jstr: `{"cfg":{"kmeans_train_percent":{"t":"F", "v":10},
        "kmeans_max_iteration":{"t":"I", "v":4},
        "ivf_threads_build":{"t":"I", "v":8}
        }}`,
			dsize:     int64(1000000),
			nlists:    int64(1000),
			createdAt: types.UnixToTimestamp(time.Now().Add(-4 * OneWeek).Unix()),
			ts: func() types.Timestamp {
				now := time.Now()
				unixts := now.Add(-time.Hour).Unix()
				return types.UnixToTimestamp(unixts)
			}(),
			hour:     3,
			skipped:  false,
			expected: false,
		},

		{
			jstr: `{"cfg":{"kmeans_train_percent":{"t":"F", "v":10},
        "kmeans_max_iteration":{"t":"I", "v":4},
        "ivf_threads_build":{"t":"I", "v":8}
        }}`,
			dsize:     int64(10000000),
			nlists:    int64(1000),
			createdAt: types.UnixToTimestamp(time.Now().Add(-4 * OneWeek).Unix()),
			ts: func() types.Timestamp {
				now := time.Now()
				unixts := now.Add(-1 * time.Hour).Unix()
				return types.UnixToTimestamp(unixts)
			}(),
			hour:     3,
			skipped:  false,
			expected: false,
		},

		{
			jstr: `{"cfg":{"kmeans_train_percent":{"t":"F", "v":10},
        "kmeans_max_iteration":{"t":"I", "v":4},
        "ivf_threads_build":{"t":"I", "v":8}
        }}`,
			dsize:     int64(10000000),
			nlists:    int64(1000),
			createdAt: types.UnixToTimestamp(time.Now().Add(-4 * OneWeek).Unix()),
			ts: func() types.Timestamp {
				now := time.Now()
				unixts := now.Add(-2 * OneWeek).Unix()
				return types.UnixToTimestamp(unixts)
			}(),
			hour:     3,
			skipped:  false,
			expected: true,
		},
		{
			jstr:      "",
			dsize:     int64(10000000),
			nlists:    int64(1000),
			createdAt: types.UnixToTimestamp(time.Now().Add(-4 * OneWeek).Unix()),
			ts: func() types.Timestamp {
				now := time.Now()
				unixts := now.Add(-2 * OneWeek).Unix()
				return types.UnixToTimestamp(unixts)
			}(),
			hour:     3,
			skipped:  false,
			expected: true,
		},
		{
			jstr:      "",
			dsize:     int64(10000000),
			nlists:    int64(1000),
			createdAt: types.UnixToTimestamp(time.Now().Add(-4 * OneWeek).Unix()),
			ts: func() types.Timestamp {
				now := time.Now()
				unixts := now.Add(-2 * OneWeek).Unix()
				return types.UnixToTimestamp(unixts)
			}(),
			hour:     4, // wrong hour
			skipped:  true,
			expected: true,
		},
	}

	return tasks
}

/*
// return status as SQL to update mo_index_update
func runIvfflatReindex(ctx context.Context,
        txnEngine engine.Engine,
        txnClient client.TxnClient,
        cnUUID string,
        task IndexUpdateTaskInfo) (updated bool, err error) {

*/

// mockReindexAlgoPlugin is a minimal indexplugin.AlgoPlugin that
// exposes a caller-supplied SyncDescriptor + idxcron hook. The
// runReindex tests only consult Catalog() and Idxcron(); the rest
// can stay nil.
type mockReindexAlgoPlugin struct {
	algo    string
	desc    catalogplugin.SyncDescriptor
	idxcron idxcronplugin.Hooks
}

func (m *mockReindexAlgoPlugin) Algo() string                 { return m.algo }
func (m *mockReindexAlgoPlugin) Catalog() catalogplugin.Hooks { return mockCatalogHooks{d: m.desc} }
func (m *mockReindexAlgoPlugin) Compile() compileplugin.Hooks { return nil }
func (m *mockReindexAlgoPlugin) Plan() planplugin.Hooks       { return nil }
func (m *mockReindexAlgoPlugin) Idxcron() idxcronplugin.Hooks {
	if m.idxcron != nil {
		return m.idxcron
	}
	return alwaysUpdatable{}
}

var _ indexplugin.AlgoPlugin = (*mockReindexAlgoPlugin)(nil)

// mockCatalogHooks returns a constant SyncDescriptor; the other hook
// methods panic so tests catch unintended calls.
type mockCatalogHooks struct{ d catalogplugin.SyncDescriptor }

func (m mockCatalogHooks) HiddenTableTypes() []string                              { return nil }
func (m mockCatalogHooks) ParamsFromTree(_ *tree.Index) (map[string]string, error) { return nil, nil }
func (m mockCatalogHooks) DefaultOptions() map[string]string                       { return nil }
func (m mockCatalogHooks) SupportedOpTypes() map[string]string                     { return nil }
func (m mockCatalogHooks) SupportedVectorTypes() []types.T                         { return nil }
func (m mockCatalogHooks) IsVectorIndex() bool                                     { return true }
func (m mockCatalogHooks) SupportedPrimaryKeyTypes() []types.T                     { return nil }
func (m mockCatalogHooks) SupportedIncludeColumnTypes() []types.T                  { return nil }
func (m mockCatalogHooks) ValidQuantization(_, _ string) error                     { return nil }
func (m mockCatalogHooks) ExperimentalFlag() string                                { return "" }
func (m mockCatalogHooks) AlterTableCloneBehavior() catalogplugin.AlterTableCloneBehavior {
	return catalogplugin.AlterTableCloneBehavior{}
}
func (m mockCatalogHooks) RestoreBehavior() catalogplugin.RestoreBehavior {
	return catalogplugin.RestoreBehavior{}
}
func (m mockCatalogHooks) BuildSessionVars() []string                   { return nil }
func (m mockCatalogHooks) ShouldTruncateHiddenTable(_ string) bool      { return false }
func (m mockCatalogHooks) SyncDescriptor() catalogplugin.SyncDescriptor { return m.d }

// alwaysUpdatable is the trivial idxcron hook the mock uses — runReindex
// callers in tests don't exercise the CDC-delta gate.
type alwaysUpdatable struct{}

func (alwaysUpdatable) Updatable(_ idxcronplugin.UpdatableInput) (bool, string, error) {
	return true, "", nil
}

func newTestIvfTableDef(pkName string, pkType types.T, vecColName string, vecType types.T, vecWidth int32) *plan.TableDef {
	return &plan.TableDef{
		Name:  "test_orig_tbl",
		TblId: 1,
		Name2ColIndex: map[string]int32{
			pkName:     0,
			vecColName: 1,
			"dummy":    2, // Add another col to make sure pk/vec col indices are used
		},
		Cols: []*plan.ColDef{
			{Name: pkName, Typ: plan.Type{Id: int32(pkType)}},
			{Name: vecColName, Typ: plan.Type{Id: int32(vecType), Width: vecWidth}},
			{Name: "dummy", Typ: plan.Type{Id: int32(types.T_int32)}},
		},
		Pkey: &plan.PrimaryKeyDef{
			Names:       []string{pkName},
			PkeyColName: pkName,
		},
		Indexes: []*plan.IndexDef{
			{
				IndexName:          "ivf_idx",
				TableExist:         true,
				IndexAlgo:          catalog.MoIndexIvfFlatAlgo.ToString(),
				IndexAlgoTableType: catalog.SystemSI_IVFFLAT_TblType_Metadata,
				IndexTableName:     "meta_tbl",
				Parts:              []string{vecColName},
				IndexAlgoParams:    `{"lists":"1000","op_type":"vector_l2_ops", "auto_update":"true", "day":"7", "hour":"3"}`,
			},
			{
				IndexName:          "ivf_idx",
				TableExist:         true,
				IndexAlgo:          catalog.MoIndexIvfFlatAlgo.ToString(),
				IndexAlgoTableType: catalog.SystemSI_IVFFLAT_TblType_Centroids,
				IndexTableName:     "centriods",
				Parts:              []string{vecColName},
				IndexAlgoParams:    `{"lists":"1000","op_type":"vector_l2_ops", "auto_update":"true", "day":"7", "hour":"3"}`,
			},
			{
				IndexName:          "ivf_idx",
				TableExist:         true,
				IndexAlgo:          catalog.MoIndexIvfFlatAlgo.ToString(),
				IndexAlgoTableType: catalog.SystemSI_IVFFLAT_TblType_Entries,
				IndexTableName:     "entries",
				Parts:              []string{vecColName},
				IndexAlgoParams:    `{"lists":"1000","op_type":"vector_l2_ops", "auto_update":"true", "day":"7", "hour":"3"}`,
			},
		},
	}
}

func TestIvfflatReindex(t *testing.T) {

	ctx := context.WithValue(context.Background(), defines.TenantIDKey{}, catalog.System_Account)
	ctx, cancel := context.WithCancel(ctx)
	defer cancel()

	mp := mpool.MustNewZero()

	txnStub := stubIdxcronTxnRunner()
	defer txnStub.Reset()
	cnUUID := "a-b-c-d"
	tableid := uint64(1)
	dbname := "test"
	tablename := "test_orig_tbl"
	indexname := "ivf_idx"

	stub1 := gostub.Stub(&getTableDef, func(sqlproc *sqlexec.SqlProcess, txnEngine engine.Engine, dbname string, tablename string) (tableDef *plan.TableDef, err error) {
		return newTestIvfTableDef("a", types.T_int64, "b", types.T_array_float32, 3), nil
	})
	defer stub1.Reset()

	tasks := getTestCases(t)
	for _, ta := range tasks {

		func() {
			var err error

			m := (*sqlexec.Metadata)(nil)
			if len(ta.jstr) > 0 {
				m, err = sqlexec.NewMetadataFromJson(ta.jstr)
				require.Nil(t, err)
			}

			info := IndexUpdateTaskInfo{
				DbName:       dbname,
				TableName:    tablename,
				IndexName:    indexname,
				Action:       Action_Ivfflat_Reindex,
				AccountId:    catalog.System_Account,
				TableId:      tableid,
				Metadata:     m,
				LastUpdateAt: &ta.ts,
				CreatedAt:    ta.createdAt,
			}

			stub2 := gostub.Stub(&ivfflatidxcron.RunGetCountSql, func(sqlproc *sqlexec.SqlProcess, sql string) (executor.Result, error) {
				bat := batch.NewWithSize(1)
				bat.Vecs[0] = vector.NewVec(types.New(types.T_int64, 8, 0))
				vector.AppendFixed[int64](bat.Vecs[0], ta.dsize, false, mp)
				bat.SetRowCount(1)
				return executor.Result{Mp: mp, Batches: []*batch.Batch{bat}}, nil

			})
			defer stub2.Reset()

			stub3 := gostub.Stub(&runReindexSql, func(sqlproc *sqlexec.SqlProcess, sql string) (executor.Result, error) {
				return executor.Result{}, nil
			})
			defer stub3.Reset()

			updated, _, err := runReindex(ctx, nil, nil, cnUUID, &info, ta.hour,
				&mockReindexAlgoPlugin{
					algo:    "ivfflat",
					desc:    catalogplugin.SyncDescriptor{IdxcronAlgoToken: "IVFFLAT", IdxcronListsAware: true},
					idxcron: ivfflatidxcron.Hooks{},
				})
			require.NoError(t, err)
			require.Equal(t, ta.expected && !ta.skipped, updated)

		}()
	}
}

func TestIvfflatReindexRejectsStaleTableIdentity(t *testing.T) {
	txnStub := stubIdxcronTxnRunner()
	defer txnStub.Reset()

	// COPY can replace a table while a previously enumerated task still carries
	// its old physical ID. Reusing the table and index names must not let that
	// stale task rebuild the replacement index.
	info := IndexUpdateTaskInfo{
		DbName: "test", TableName: "test_orig_tbl", IndexName: "ivf_idx",
		Action: Action_Ivfflat_Reindex, AccountId: catalog.System_Account, TableId: 1,
	}
	lookupCalls := 0
	tableStub := gostub.Stub(&getTableDef, func(_ *sqlexec.SqlProcess, _ engine.Engine, dbname, tablename string) (*plan.TableDef, error) {
		lookupCalls++
		require.Equal(t, info.DbName, dbname)
		require.Equal(t, info.TableName, tablename)
		replacement := newTestIvfTableDef("a", types.T_int64, "b", types.T_array_float32, 3)
		replacement.TblId = 2
		return replacement, nil
	})
	defer tableStub.Reset()
	countCalls, rebuildCalls := 0, 0
	countStub := gostub.Stub(&ivfflatidxcron.RunGetCountSql, func(*sqlexec.SqlProcess, string) (executor.Result, error) {
		countCalls++
		return executor.Result{}, moerr.NewInternalErrorNoCtx("unexpected stale-task count")
	})
	defer countStub.Reset()
	rebuildStub := gostub.Stub(&runReindexSql, func(*sqlexec.SqlProcess, string) (executor.Result, error) {
		rebuildCalls++
		return executor.Result{}, nil
	})
	defer rebuildStub.Reset()

	updated, _, err := runReindex(context.Background(), nil, nil, "a-b-c-d", &info, 3,
		&mockReindexAlgoPlugin{
			algo:    "ivfflat",
			desc:    catalogplugin.SyncDescriptor{IdxcronAlgoToken: "IVFFLAT", IdxcronListsAware: true},
			idxcron: ivfflatidxcron.Hooks{},
		})
	require.ErrorContains(t, err, "table id mimstach")
	require.False(t, updated)
	require.Equal(t, 1, lookupCalls)
	require.Zero(t, countCalls)
	require.Zero(t, rebuildCalls)
}

func TestIvfflatReindexAutoUpdateOff(t *testing.T) {

	ctx := context.WithValue(context.Background(), defines.TenantIDKey{}, catalog.System_Account)
	ctx, cancel := context.WithCancel(ctx)
	defer cancel()

	mp := mpool.MustNewZero()

	txnStub := stubIdxcronTxnRunner()
	defer txnStub.Reset()
	cnUUID := "a-b-c-d"
	tableid := uint64(1)
	dbname := "test"
	tablename := "test_orig_tbl"
	indexname := "ivf_idx"

	stub1 := gostub.Stub(&getTableDef, func(sqlproc *sqlexec.SqlProcess, txnEngine engine.Engine, dbname string, tablename string) (tableDef *plan.TableDef, err error) {
		tbldef := newTestIvfTableDef("a", types.T_int64, "b", types.T_array_float32, 3)

		// reset auto_update = false
		for _, idxdef := range tbldef.Indexes {
			idxdef.IndexAlgoParams = `{"lists":"1000","op_type":"vector_l2_ops", "auto_update":"false"}`
		}
		return tbldef, nil
	})
	defer stub1.Reset()

	tasks := getTestCases(t)
	for _, ta := range tasks {

		func() {
			var err error

			m := (*sqlexec.Metadata)(nil)
			if len(ta.jstr) > 0 {
				m, err = sqlexec.NewMetadataFromJson(ta.jstr)
				require.Nil(t, err)
			}

			info := IndexUpdateTaskInfo{
				DbName:       dbname,
				TableName:    tablename,
				IndexName:    indexname,
				Action:       Action_Ivfflat_Reindex,
				AccountId:    catalog.System_Account,
				TableId:      tableid,
				Metadata:     m,
				LastUpdateAt: &ta.ts,
				CreatedAt:    ta.createdAt,
			}

			stub2 := gostub.Stub(&ivfflatidxcron.RunGetCountSql, func(sqlproc *sqlexec.SqlProcess, sql string) (executor.Result, error) {
				bat := batch.NewWithSize(1)
				bat.Vecs[0] = vector.NewVec(types.New(types.T_int64, 8, 0))
				vector.AppendFixed[int64](bat.Vecs[0], ta.dsize, false, mp)
				bat.SetRowCount(1)
				return executor.Result{Mp: mp, Batches: []*batch.Batch{bat}}, nil

			})
			defer stub2.Reset()

			stub3 := gostub.Stub(&runReindexSql, func(sqlproc *sqlexec.SqlProcess, sql string) (executor.Result, error) {
				return executor.Result{}, nil
			})
			defer stub3.Reset()

			updated, reason, err := runReindex(ctx, nil, nil, cnUUID, &info, ta.hour,
				&mockReindexAlgoPlugin{
					algo:    "ivfflat",
					desc:    catalogplugin.SyncDescriptor{IdxcronAlgoToken: "IVFFLAT", IdxcronListsAware: true},
					idxcron: ivfflatidxcron.Hooks{},
				})
			require.NoError(t, err)
			require.Equal(t, false, updated)
			require.Equal(t, Reason_Skipped, reason)

		}()
	}
}

func TestIndexUpdateTaskInfoSaveStatusError(t *testing.T) {

	tableid := uint64(1)
	dbname := "test"
	tablename := "test_orig_tbl"
	indexname := "ivf_idx"

	info := &IndexUpdateTaskInfo{
		DbName:       dbname,
		TableName:    tablename,
		IndexName:    indexname,
		Action:       Action_Ivfflat_Reindex,
		AccountId:    catalog.System_Account,
		TableId:      tableid,
		Metadata:     nil,
		LastUpdateAt: nil,
	}

	{
		// runSavestatusSql
		stub5 := gostub.Stub(&runSaveStatusSql, func(sqlproc *sqlexec.SqlProcess, sql string) (executor.Result, error) {
			return executor.Result{}, nil
		})
		defer stub5.Reset()

		err := info.saveStatus(nil, true, "reason", moerr.NewInternalErrorNoCtx("fake error"))
		require.NoError(t, err)

	}

	{
		// runSavestatusSql
		stub5 := gostub.Stub(&runSaveStatusSql, func(sqlproc *sqlexec.SqlProcess, sql string) (executor.Result, error) {
			return executor.Result{}, moerr.NewInternalErrorNoCtx("fake sql error")
		})
		defer stub5.Reset()

		err := info.saveStatus(nil, true, "reason", nil)
		require.Error(t, err)

	}

}

func TestCmdNoDefine(t *testing.T) {
	//ctx := context.WithValue(context.Background(), defines.TenantIDKey{}, catalog.System_Account)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	cnUUID := "a-b-c-d"

	{
		err := RenameSrcTable(ctx, cnUUID, nil, 0, 0, "old", "new")
		require.Error(t, err)
	}

	{
		err := UnregisterUpdateByTableId(ctx, cnUUID, nil, 0)
		require.Error(t, err)
	}
	{
		err := UnregisterUpdateByDbName(ctx, cnUUID, nil, "")
		require.Error(t, err)
	}
	{
		err := UnregisterUpdate(ctx, cnUUID, nil, 0, "idx", "action")
		require.Error(t, err)
	}
	{
		err := RegisterUpdate(ctx, cnUUID, nil, 0, "db", "tbl", "idx", "action", "meta")
		require.Error(t, err)
	}

}

func TestCmdSqlError(t *testing.T) {
	ctx := context.WithValue(context.Background(), defines.TenantIDKey{}, catalog.System_Account)
	ctx, cancel := context.WithCancel(ctx)
	defer cancel()

	// runCmdSql
	stub1 := gostub.Stub(&runCmdSql, func(sqlproc *sqlexec.SqlProcess, sql string) (executor.Result, error) {
		return executor.Result{}, moerr.NewInternalErrorNoCtx("fake sql error")
	})
	defer stub1.Reset()

	cnUUID := "a-b-c-d"

	{
		err := RenameSrcTable(ctx, cnUUID, nil, 0, 0, "old", "new")
		require.Error(t, err)
	}

	{
		err := UnregisterUpdateByTableId(ctx, cnUUID, nil, 0)
		require.Error(t, err)
	}
	{
		err := UnregisterUpdateByDbName(ctx, cnUUID, nil, "")
		require.Error(t, err)
	}
	{
		err := UnregisterUpdate(ctx, cnUUID, nil, 0, "idx", "action")
		require.Error(t, err)
	}
	{
		err := RegisterUpdate(ctx, cnUUID, nil, 0, "db", "tbl", "idx", "action", "meta")
		require.Error(t, err)
	}

}
