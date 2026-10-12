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
	"encoding/json"
	"errors"
	"fmt"
	"strings"
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
	"github.com/matrixorigin/matrixone/pkg/sql/parsers/dialect"
	"github.com/matrixorigin/matrixone/pkg/sql/parsers/dialect/mysql"
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
	indexname := "ivf'\\idx"

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

	info.Action = "action'\\quoted"
	for _, tc := range []struct {
		name      string
		indexName string
		updated   bool
		err       error
		status    string
		message   string
	}{
		{"error precedence", indexname, true, errors.New("failed '表'\\path\\tail\\"), Status_Error, ""},
		{"error", indexname, false, moerr.NewInternalErrorNoCtx("can't read C:\\index\nnext"), Status_Error, ""},
		{"skipped", indexname, false, nil, Status_Skipped, "not ready'\\yet"},
		{"updated", indexname, true, nil, Status_Ok, "reindex success"},
		{"empty index name", "", false, nil, Status_Skipped, "invalid index name"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			info.IndexName = tc.indexName
			mp := mpool.MustNewZero()
			defer mpool.DeleteMPool(mp)
			statusProc := &sqlexec.SqlProcess{}
			writeErr := errors.New("status write failed")
			calls := 0
			stub := gostub.Stub(&runSaveStatusSql, func(sqlproc *sqlexec.SqlProcess, sql string) (executor.Result, error) {
				require.Same(t, statusProc, sqlproc)
				stmt, err := mysql.ParseOne(context.Background(), sql, 0)
				require.NoError(t, err, sql)
				defer stmt.Free()
				update := stmt.(*tree.Update)
				var status IndexUpdateStatus
				literal := update.Exprs[len(update.Exprs)-1].Expr.(*tree.NumVal).String()
				require.NoError(t, json.Unmarshal([]byte(literal), &status))
				message := tc.message
				if tc.err != nil {
					message = tc.err.Error()
				}
				require.Equal(t, tc.status, status.Status)
				require.Equal(t, message, status.Msg)
				require.False(t, status.Time.IsZero())
				predicates := make(map[string]string)
				for expr := update.Where.Expr; ; {
					condition := expr
					if and, ok := expr.(*tree.AndExpr); ok {
						condition = and.Right
						expr = and.Left
					} else {
						expr = nil
					}
					comparison := condition.(*tree.ComparisonExpr)
					require.Equal(t, tree.EQUAL, comparison.Op)
					predicates[comparison.Left.(*tree.UnresolvedName).ColName()] = comparison.Right.(*tree.NumVal).String()
					if expr == nil {
						break
					}
				}
				require.Equal(t, map[string]string{
					"table_id": fmt.Sprint(info.TableId), "account_id": fmt.Sprint(info.AccountId),
					"action": info.Action, "index_name": info.IndexName,
				}, predicates)
				if tc.updated && tc.err == nil {
					require.Len(t, update.Exprs, 2)
					require.Len(t, update.Exprs[0].Names, 1)
					require.Equal(t, "last_update_at", update.Exprs[0].Names[0].ColName())
					now := update.Exprs[0].Expr.(*tree.FuncExpr)
					require.Equal(t, "now", now.Func.FunctionReference.(*tree.UnresolvedName).ColName())
					require.Empty(t, now.Exprs)
				} else {
					require.Len(t, update.Exprs, 1, "error/skipped must preserve last_update_at")
				}
				require.Len(t, update.Exprs[len(update.Exprs)-1].Names, 1)
				require.Equal(t, "status", update.Exprs[len(update.Exprs)-1].Names[0].ColName())
				calls++
				if calls == 2 {
					return executor.Result{}, writeErr
				}
				result := executor.NewMemResult([]types.Type{types.T_int64.ToType()}, mp)
				result.NewBatchWithRowCount(1)
				require.NoError(t, executor.AppendFixedRows(result, 0, []int64{1}))
				return result.GetResult(), nil
			})
			defer stub.Reset()
			require.NoError(t, info.saveStatus(statusProc, tc.updated, tc.message, tc.err))
			require.Zero(t, mp.CurrNB()+mp.OnHeapCurrNB())
			require.ErrorIs(t, info.saveStatus(statusProc, tc.updated, tc.message, tc.err), writeErr)
			require.Equal(t, 2, calls)
		})
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

func TestUnregisterUpdateByDbNameCompatibility(t *testing.T) {
	ctx := defines.AttachAccountId(t.Context(), 7)
	mp := mpool.MustNewZero()
	defer mpool.DeleteMPool(mp)
	for _, count := range []int64{-1, 0, 1} {
		t.Run(fmt.Sprint(count), func(t *testing.T) {
			calls := 0
			stub := gostub.Stub(&runCmdSql, func(_ *sqlexec.SqlProcess, sql string) (executor.Result, error) {
				calls++
				if calls == 1 {
					if count < 0 {
						return executor.Result{}, nil
					}
					bat := batch.NewWithSize(1)
					bat.Vecs[0] = vector.NewVec(types.T_int64.ToType())
					require.NoError(t, vector.AppendFixed(bat.Vecs[0], count, false, mp))
					bat.SetRowCount(1)
					return executor.Result{Mp: mp, Batches: []*batch.Batch{bat}}, nil
				}
				require.Zero(t, mp.CurrNB(), "count result must close before DELETE")
				stmt, err := mysql.ParseOne(ctx, sql, 0)
				require.NoError(t, err)
				defer stmt.Free()
				where := stmt.(*tree.Delete).Where.Expr.(*tree.AndExpr)
				account, ok := where.Left.(*tree.ComparisonExpr).Right.(*tree.NumVal).Uint64()
				require.True(t, ok)
				require.Equal(t, uint64(7), account)
				require.Equal(t, "db'\\name", where.Right.(*tree.ComparisonExpr).Right.(*tree.NumVal).String())
				return executor.Result{}, nil
			})
			defer stub.Reset()
			require.NoError(t, UnregisterUpdateByDbName(ctx, "cn", nil, "db'\\name"))
			want := 1
			if count > 0 {
				want = 2
			}
			require.Equal(t, want, calls)
			require.Zero(t, mp.CurrNB())
		})
	}
}

// Decode the actual executor input; the expected values never use the quoting helper.
func indexUpdateSQLStrings(sql string) []string {
	scanner := mysql.NewScanner(dialect.MYSQL, sql)
	defer mysql.PutScanner(scanner)
	var values []string
	for {
		token, value := scanner.Scan()
		if token == 0 {
			return values
		}
		if token == mysql.STRING {
			values = append(values, value)
		}
	}
}

func TestIndexUpdateCommandLiterals(t *testing.T) {
	ctx := defines.AttachAccountId(t.Context(), 17)
	for _, suffix := range []string{"plain", "表", "'quote", `\path`, `\tail`, `trailing\`, "'\\\x00\n"} {
		t.Run(suffix, func(t *testing.T) {
			dbName, tableName, indexName, action := "db"+suffix, "table"+suffix, "idx"+suffix, "action"+suffix
			metadata, err := json.Marshal(map[string]string{"path": suffix})
			require.NoError(t, err)
			mp := mpool.MustNewZero()
			defer mpool.DeleteMPool(mp)
			var expected [][]string
			calls := 0
			stub := gostub.Stub(&runCmdSql, func(proc *sqlexec.SqlProcess, query string) (executor.Result, error) {
				require.Zero(t, mp.CurrNB()+mp.OnHeapCurrNB(), "previous result must close before next command")
				require.Equal(t, "cn", proc.SqlCtx.CNUuid)
				require.Equal(t, uint32(0), proc.SqlCtx.AccountId)
				require.Nil(t, proc.SqlCtx.TxnOperator)
				_, hasDeadline := proc.SqlCtx.Ctx.Deadline()
				require.True(t, hasDeadline)
				require.NoError(t, proc.SqlCtx.Ctx.Err())
				values := indexUpdateSQLStrings(query)
				if strings.HasPrefix(query, "REPLACE") {
					require.Len(t, values, 6)
					var status IndexUpdateStatus
					require.NoError(t, json.Unmarshal([]byte(values[5]), &status))
					require.Equal(t, Status_Ok, status.Status)
					require.Equal(t, "index update registered", status.Msg)
					require.False(t, status.Time.IsZero())
					values = values[:5]
					require.Contains(t, query, "VALUES (17, 42,")
				} else if !strings.HasPrefix(query, "SELECT") {
					require.Contains(t, query, "account_id = 17")
					if !strings.Contains(query, "db_name =") {
						require.Contains(t, query, "table_id = 42")
					}
				}
				require.Less(t, calls, len(expected))
				require.Equal(t, expected[calls], values)
				calls++
				result := executor.NewMemResult([]types.Type{types.T_int64.ToType()}, mp)
				result.NewBatchWithRowCount(1)
				require.NoError(t, executor.AppendFixedRows(result, 0, []int64{1}))
				return result.GetResult(), nil
			})
			defer stub.Reset()
			check := func(want [][]string, fn func() error) {
				t.Helper()
				expected, calls = want, 0
				require.NoError(t, fn())
				require.Equal(t, len(want), calls)
				require.Zero(t, mp.CurrNB()+mp.OnHeapCurrNB())
			}
			check([][]string{{dbName, tableName, indexName, action, string(metadata)}}, func() error {
				return RegisterUpdate(ctx, "cn", nil, 42, dbName, tableName, indexName, action, string(metadata))
			})
			check([][]string{{indexName}}, func() error { return UnregisterUpdate(ctx, "cn", nil, 42, indexName, Action_Wildcard) })
			check([][]string{{indexName, action}}, func() error { return UnregisterUpdate(ctx, "cn", nil, 42, indexName, action) })
			check([][]string{{catalog.MO_INDEX_UPDATE, catalog.MO_CATALOG}, {dbName}}, func() error { return UnregisterUpdateByDbName(ctx, "cn", nil, dbName) })
			check([][]string{{"new" + suffix, tableName}}, func() error { return RenameSrcTable(ctx, "cn", nil, 7, 42, tableName, "new"+suffix) })
			check([][]string{nil}, func() error { return UnregisterUpdateByTableId(ctx, "cn", nil, 42) })
		})
	}
}

func TestIndexUpdateDatabaseDeleteError(t *testing.T) {
	mp := mpool.MustNewZero()
	defer mpool.DeleteMPool(mp)
	writeErr := errors.New("task deletion failed")
	calls := 0
	stub := gostub.Stub(&runCmdSql, func(_ *sqlexec.SqlProcess, _ string) (executor.Result, error) {
		calls++
		if calls == 2 {
			require.Zero(t, mp.CurrNB()+mp.OnHeapCurrNB(), "catalog probe must close before deletion fails")
			return executor.Result{}, writeErr
		}
		result := executor.NewMemResult([]types.Type{types.T_int64.ToType()}, mp)
		result.NewBatchWithRowCount(1)
		require.NoError(t, executor.AppendFixedRows(result, 0, []int64{1}))
		return result.GetResult(), nil
	})
	defer stub.Reset()
	require.ErrorIs(t, UnregisterUpdateByDbName(defines.AttachAccountId(t.Context(), 17), "cn", nil, `db\path`), writeErr)
	require.Equal(t, 2, calls)
	require.Zero(t, mp.CurrNB()+mp.OnHeapCurrNB())
}
