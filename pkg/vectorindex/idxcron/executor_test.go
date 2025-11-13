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
	"fmt"
	"testing"
	"time"

	"github.com/matrixorigin/matrixone/pkg/catalog"
	"github.com/matrixorigin/matrixone/pkg/common/mpool"
	"github.com/matrixorigin/matrixone/pkg/container/batch"
	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/container/vector"
	"github.com/matrixorigin/matrixone/pkg/defines"
	"github.com/matrixorigin/matrixone/pkg/pb/plan"
	"github.com/matrixorigin/matrixone/pkg/testutil/testengine"
	"github.com/matrixorigin/matrixone/pkg/util/executor"
	"github.com/matrixorigin/matrixone/pkg/vectorindex/sqlexec"
	"github.com/matrixorigin/matrixone/pkg/vm/engine"
	"github.com/prashantv/gostub"
	"github.com/stretchr/testify/require"
)

type TestTask struct {
	jstr      string
	dsize     uint64
	nlists    int64
	ts        types.Timestamp
	createdAt types.Timestamp
	expected  bool
}

func getTestCases(t *testing.T) []TestTask {

	tasks := []TestTask{
		{
			// data size < nlist
			jstr: `{"cfg":{"kmeans_train_percent":{"t":"F", "v":1},
        "kmeans_max_iteration":{"t":"I", "v":4},
        "ivf_threads_build":{"t":"I", "v":8}
        }}`,
			dsize:     uint64(100),
			nlists:    int64(1000),
			ts:        types.UnixToTimestamp(0),
			createdAt: types.UnixToTimestamp(time.Now().Unix()),
			expected:  false,
		},

		{
			// just CreatedAt and skip update
			jstr: `{"cfg":{"kmeans_train_percent":{"t":"F", "v":1},
        "kmeans_max_iteration":{"t":"I", "v":4},
        "ivf_threads_build":{"t":"I", "v":8}
        }}`,
			dsize:     uint64(1000000),
			nlists:    int64(1000),
			ts:        types.UnixToTimestamp(0),
			createdAt: types.UnixToTimestamp(time.Now().Unix()),
			expected:  false,
		},

		{
			jstr: `{"cfg":{"kmeans_train_percent":{"t":"F", "v":1},
        "kmeans_max_iteration":{"t":"I", "v":4},
        "ivf_threads_build":{"t":"I", "v":8}
        }}`,
			dsize:     uint64(1000000),
			nlists:    int64(1000),
			ts:        types.UnixToTimestamp(0),
			createdAt: types.UnixToTimestamp(time.Now().Add(-4 * OneWeek).Unix()),
			expected:  true,
		},

		{
			jstr: `{"cfg":{"kmeans_train_percent":{"t":"F", "v":10},
	"kmeans_max_iteration":{"t":"I", "v":4},
	"ivf_threads_build":{"t":"I", "v":8}
	}}`,
			dsize:    uint64(1000000),
			nlists:   int64(1000),
			ts:       types.UnixToTimestamp(0),
			expected: true,
		},

		{
			jstr: `{"cfg":{"kmeans_train_percent":{"t":"F", "v":10},
        "kmeans_max_iteration":{"t":"I", "v":4},
        "ivf_threads_build":{"t":"I", "v":8}
        }}`,
			dsize:     uint64(1000000),
			nlists:    int64(1000),
			createdAt: types.UnixToTimestamp(time.Now().Add(-4 * OneWeek).Unix()),
			ts: func() types.Timestamp {
				now := time.Now()
				unixts := now.Add(-2 * OneWeek).Unix()
				return types.UnixToTimestamp(unixts)
			}(),
			expected: true,
		},

		{
			jstr: `{"cfg":{"kmeans_train_percent":{"t":"F", "v":10},
        "kmeans_max_iteration":{"t":"I", "v":4},
        "ivf_threads_build":{"t":"I", "v":8}
        }}`,
			dsize:     uint64(1000000),
			nlists:    int64(1000),
			createdAt: types.UnixToTimestamp(time.Now().Add(-4 * OneWeek).Unix()),
			ts: func() types.Timestamp {
				now := time.Now()
				unixts := now.Add(-time.Hour).Unix()
				return types.UnixToTimestamp(unixts)
			}(),
			expected: false,
		},

		{
			jstr: `{"cfg":{"kmeans_train_percent":{"t":"F", "v":10},
        "kmeans_max_iteration":{"t":"I", "v":4},
        "ivf_threads_build":{"t":"I", "v":8}
        }}`,
			dsize:     uint64(10000000),
			nlists:    int64(1000),
			createdAt: types.UnixToTimestamp(time.Now().Add(-4 * OneWeek).Unix()),
			ts: func() types.Timestamp {
				now := time.Now()
				unixts := now.Add(-1 * time.Hour).Unix()
				return types.UnixToTimestamp(unixts)
			}(),
			expected: false,
		},

		{
			jstr: `{"cfg":{"kmeans_train_percent":{"t":"F", "v":10},
        "kmeans_max_iteration":{"t":"I", "v":4},
        "ivf_threads_build":{"t":"I", "v":8}
        }}`,
			dsize:     uint64(10000000),
			nlists:    int64(1000),
			createdAt: types.UnixToTimestamp(time.Now().Add(-4 * OneWeek).Unix()),
			ts: func() types.Timestamp {
				now := time.Now()
				unixts := now.Add(-2 * OneWeek).Unix()
				return types.UnixToTimestamp(unixts)
			}(),
			expected: true,
		},
	}

	return tasks
}

func TestCheckIndexUpdatable(t *testing.T) {

	tasks := getTestCases(t)
	for _, ta := range tasks {

		m, err := sqlexec.NewMetadataFromJson(ta.jstr)
		require.Nil(t, err)

		info := IndexUpdateTaskInfo{
			DbName:       "db",
			TableName:    "table",
			IndexName:    "index",
			Action:       Action_Ivfflat_Reindex,
			AccountId:    uint32(0),
			TableId:      uint64(100),
			Metadata:     m,
			LastUpdateAt: &ta.ts,
			CreatedAt:    ta.createdAt,
		}

		ok, err := info.checkIndexUpdatable(context.Background(), ta.dsize, ta.nlists)
		require.NoError(t, err)
		require.Equal(t, ta.expected, ok)

	}

}

/*
// return status as SQL to update mo_index_update
func runIvfflatReindex(ctx context.Context,
        txnEngine engine.Engine,
        txnClient client.TxnClient,
        cnUUID string,
        task IndexUpdateTaskInfo) (updated bool, err error) {

*/

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
				IndexAlgoParams:    `{"lists":"16","op_type":"vector_l2_ops"}`,
			},
			{
				IndexName:          "ivf_idx",
				TableExist:         true,
				IndexAlgo:          catalog.MoIndexIvfFlatAlgo.ToString(),
				IndexAlgoTableType: catalog.SystemSI_IVFFLAT_TblType_Centroids,
				IndexTableName:     "centriods",
				Parts:              []string{vecColName},
				IndexAlgoParams:    `{"lists":"16","op_type":"vector_l2_ops"}`,
			},
			{
				IndexName:          "ivf_idx",
				TableExist:         true,
				IndexAlgo:          catalog.MoIndexIvfFlatAlgo.ToString(),
				IndexAlgoTableType: catalog.SystemSI_IVFFLAT_TblType_Entries,
				IndexTableName:     "entries",
				Parts:              []string{vecColName},
				IndexAlgoParams:    `{"lists":"16","op_type":"vector_l2_ops"}`,
			},
		},
	}
}

func TestGetTableDef(t *testing.T) {
	ctx := context.WithValue(context.Background(), defines.TenantIDKey{}, catalog.System_Account)
	ctx, cancel := context.WithCancel(ctx)
	defer cancel()

	catalog.SetupDefines("")
	cnEngine, cnClient, _ := testengine.New(ctx)
	cnUUID := "a-b-c-d"
	dbname := "test"
	tablename := "ivfsrc"

	txnOp, err := sqlexec.GetTxn(ctx, cnEngine, cnClient, "idxcron")
	require.NoError(t, err)

	sqlproc := sqlexec.NewSqlProcessWithContext(sqlexec.NewSqlContext(ctx, cnUUID, txnOp, catalog.System_Account, nil))

	tabledef, err := getTableDef(sqlproc, cnEngine, dbname, tablename)
	require.NoError(t, err)

	fmt.Printf("tableDef %v\n", tabledef)
}

func TestIvfflatReindex(t *testing.T) {

	ctx := context.WithValue(context.Background(), defines.TenantIDKey{}, catalog.System_Account)
	ctx, cancel := context.WithCancel(ctx)
	defer cancel()

	mp := mpool.MustNewZero()

	catalog.SetupDefines("")
	cnEngine, cnClient, _ := testengine.New(ctx)
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

			m, err := sqlexec.NewMetadataFromJson(ta.jstr)
			require.Nil(t, err)

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

			stub2 := gostub.Stub(&runGetCountSql, func(sqlproc *sqlexec.SqlProcess, sql string) (executor.Result, error) {
				bat := batch.NewWithSize(1)
				bat.Vecs[0] = vector.NewVec(types.New(types.T_uint64, 8, 0))
				vector.AppendFixed[uint64](bat.Vecs[0], ta.dsize, false, mp)
				bat.SetRowCount(1)
				return executor.Result{Mp: mp, Batches: []*batch.Batch{bat}}, nil

			})
			defer stub2.Reset()

			stub3 := gostub.Stub(&runReindexSql, func(sqlproc *sqlexec.SqlProcess, sql string) (executor.Result, error) {
				return executor.Result{}, nil
			})
			defer stub3.Reset()

			updated, err := runIvfflatReindex(ctx, cnEngine, cnClient, cnUUID, info)
			fmt.Printf("updated = %v\n", updated)
			require.NoError(t, err)
			require.Equal(t, ta.expected, updated)

		}()
	}
}
