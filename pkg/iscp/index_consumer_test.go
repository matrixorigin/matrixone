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

package iscp

import (
	"context"
	"testing"

	"github.com/matrixorigin/matrixone/pkg/catalog"
	"github.com/matrixorigin/matrixone/pkg/container/batch"
	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/container/vector"
	"github.com/matrixorigin/matrixone/pkg/defines"
	"github.com/matrixorigin/matrixone/pkg/logutil"
	"github.com/matrixorigin/matrixone/pkg/pb/plan"
	"github.com/matrixorigin/matrixone/pkg/testutil"
	"github.com/matrixorigin/matrixone/pkg/testutil/testengine"
	"github.com/matrixorigin/matrixone/pkg/txn/client"
	"github.com/matrixorigin/matrixone/pkg/util/executor"
	"github.com/prashantv/gostub"
	"github.com/stretchr/testify/require"
	"github.com/tidwall/btree"
)

type MockRetriever struct {
	insertBatch *AtomicBatch
	deleteBatch *AtomicBatch
	noMoreData  bool
	dtype       int8
}

func (r *MockRetriever) Next() *ISCPData {
	logutil.Infof("TxRetriever Next()")
	if !r.noMoreData {
		r.noMoreData = true
		d := &ISCPData{insertBatch: r.insertBatch, deleteBatch: r.deleteBatch, noMoreData: false, err: nil}
		d.Set(0) // never close
		return d
	}
	d := &ISCPData{insertBatch: nil, deleteBatch: nil, noMoreData: r.noMoreData, err: nil}
	d.Set(0) // never close
	return d
}

func (r *MockRetriever) UpdateWatermark(ctx context.Context, cnUUID string, txn client.TxnOperator) error {
	logutil.Infof("TxnRetriever.UpdateWatermark()")
	return nil
}

func (r *MockRetriever) GetDataType() int8 {
	return r.dtype
}

func (r *MockRetriever) GetAccountID() uint32 {
	return uint32(0)
}

func (r *MockRetriever) GetTableID() uint64 {
	return uint64(0)
}

var _ DataRetriever = new(MockRetriever)

func newTestTableDef(pkName string, pkType types.T, vecColName string, vecType types.T, vecWidth int32) *plan.TableDef {
	return &plan.TableDef{
		Name: "test_orig_tbl",
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
				IndexName:          "hnsw_idx",
				TableExist:         true,
				IndexAlgo:          catalog.MoIndexHnswAlgo.ToString(),
				IndexAlgoTableType: catalog.Hnsw_TblType_Metadata,
				IndexTableName:     "meta_tbl",
				Parts:              []string{vecColName},
				IndexAlgoParams:    `{"m":"16","ef_construction":"200","ef_search":"100","op_type":"vector_l2_ops"}`,
			},
			{
				IndexName:          "hnsw_idx",
				TableExist:         true,
				IndexAlgo:          catalog.MoIndexHnswAlgo.ToString(),
				IndexAlgoTableType: catalog.Hnsw_TblType_Storage,
				IndexTableName:     "storage_tbl",
				Parts:              []string{vecColName},
				IndexAlgoParams:    `{"m":"16","ef_construction":"200","ef_search":"100","op_type":"vector_l2_ops"}`,
			},
		},
	}
}

func newTestIvfTableDef(pkName string, pkType types.T, vecColName string, vecType types.T, vecWidth int32) *plan.TableDef {
	return &plan.TableDef{
		Name: "test_orig_tbl",
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

func newTestIvfConsumerInfo() *ConsumerInfo {
	return &ConsumerInfo{
		ConsumerType: 0,
		DBName:       "test_db",
		TableName:    "test_tbl",
		IndexName:    "ivf_idx",
	}
}

func newTestConsumerInfo() *ConsumerInfo {
	return &ConsumerInfo{
		ConsumerType: 0,
		DBName:       "test_db",
		TableName:    "test_tbl",
		IndexName:    "hnsw_idx",
	}
}

func newTestJobID() JobID {
	return JobID{
		DBName:    "test_db",
		TableName: "test_tbl",
		JobName:   "index_hnsw_idx",
	}
}

func TestConsumer(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	sqls := make([]string, 0, 1)
	stub1 := gostub.Stub(&ExecWithResult, func(_ context.Context, sql string, _ string, _ client.TxnOperator) (executor.Result, error) {
		sqls = append(sqls, sql)
		return executor.Result{}, nil
	})
	defer stub1.Reset()

	r := &MockRetriever{
		insertBatch: nil,
		deleteBatch: nil,
		noMoreData:  true,
		dtype:       ISCPDataType_Snapshot,
	}

	tblDef := newTestIvfTableDef("pk", types.T_int64, "vec", types.T_array_float32, 4)
	cnUUID := "a-b-c-d"
	info := newTestIvfConsumerInfo()
	job := newTestJobID()

	catalog.SetupDefines("")
	cnEngine, cnClient, _ := testengine.New(ctx)

	consumer, err := NewConsumer(cnUUID, cnEngine, cnClient, tblDef, job, info)
	require.NoError(t, err)
	err = consumer.Consume(ctx, r)
	require.NoError(t, err)
}

func TestIvfSnapshot(t *testing.T) {

	proc := testutil.NewProcess(t)

	ctx := context.WithValue(context.Background(), defines.TenantIDKey{}, catalog.System_Account)
	ctx, cancel := context.WithCancel(ctx)
	defer cancel()

	sqls := make([]string, 0, 1)
	stub1 := gostub.Stub(&ExecWithResult, func(_ context.Context, sql string, _ string, _ client.TxnOperator) (executor.Result, error) {
		sqls = append(sqls, sql)
		return executor.Result{}, nil
	})
	defer stub1.Reset()

	/*
	   sqlexecstub := gostub.Stub(&sqlExecutorFactory, mockSqlExecutorFactory)
	   defer sqlexecstub.Reset()
	*/

	tblDef := newTestIvfTableDef("pk", types.T_int64, "vec", types.T_array_float32, 2)
	cnUUID := "a-b-c-d"
	info := newTestIvfConsumerInfo()
	job := newTestJobID()
	catalog.SetupDefines("")
	cnEngine, cnClient, _ := testengine.New(ctx)

	t.Run("snapshot", func(t *testing.T) {
		consumer, err := NewConsumer(cnUUID, cnEngine, cnClient, tblDef, job, info)
		require.NoError(t, err)

		bat := testutil.NewBatchWithVectors(
			[]*vector.Vector{
				testutil.NewVector(2, types.T_int64.ToType(), proc.Mp(), false, []int64{1, 2}),
				testutil.NewVector(2, types.T_array_float32.ToType(), proc.Mp(), false, [][]float32{{0.1, 0.2}, {0.3, 0.4}}),
				testutil.NewVector(2, types.T_int32.ToType(), proc.Mp(), false, []int32{1, 2}),
			}, nil)

		defer bat.Clean(testutil.TestUtilMp)

		insertAtomicBat := &AtomicBatch{
			Mp:      nil,
			Batches: []*batch.Batch{bat},
			Rows:    btree.NewBTreeGOptions(AtomicBatchRow.Less, btree.Options{Degree: 64}),
		}

		output := &MockRetriever{
			dtype:       ISCPDataType_Snapshot,
			insertBatch: insertAtomicBat,
			deleteBatch: nil,
			noMoreData:  false,
		}
		err = consumer.Consume(ctx, output)
		require.NoError(t, err)
		require.Equal(t, len(sqls), 1)
		sql := sqls[0]
		expected := "REPLACE INTO `test_db`.`entries` (`__mo_index_centroid_fk_version`, `__mo_index_centroid_fk_id`, `__mo_index_pri_col`, `__mo_index_centroid_fk_entry`) WITH centroid as (SELECT * FROM `test_db`.`centriods` WHERE `__mo_index_centroid_version` = (SELECT CAST(__mo_index_val as BIGINT) FROM `test_db`.`meta_tbl` WHERE `__mo_index_key` = 'version') ), src as (SELECT CAST(column_0 as BIGINT) as `src0`, CAST(column_1 as VECF32(2)) as `src1` FROM (VALUES ROW(1,'[0.1, 0.2]'),ROW(2,'[0.3, 0.4]'))) SELECT `__mo_index_centroid_version`, `__mo_index_centroid_id`, src0, src1 FROM src CENTROIDX('vector_l2_ops') JOIN centroid using (`__mo_index_centroid`, `src1`)"

		require.Equal(t, string(sql), expected)

		sqls = sqls[:0]
		//fmt.Printf("Consume %p %v\n", consumer.(*IndexConsumer).exec.(*MockSQLExecutor).sqls, consumer.(*IndexConsumer).exec.(*MockSQLExecutor).sqls)
	})

	t.Run("noMoreData", func(t *testing.T) {
		sqls = sqls[:0]
		consumer, err := NewConsumer(cnUUID, cnEngine, cnClient, tblDef, job, info)
		require.NoError(t, err)

		output := &MockRetriever{
			dtype:       ISCPDataType_Snapshot,
			insertBatch: nil,
			deleteBatch: nil,
			noMoreData:  true,
		}
		err = consumer.Consume(ctx, output)
		require.NoError(t, err)
		require.Equal(t, len(sqls), 0)
	})
}

func TestIvfTail(t *testing.T) {

	proc := testutil.NewProcess(t)

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	sqls := make([]string, 0, 1)
	stub1 := gostub.Stub(&ExecWithResult, func(_ context.Context, sql string, _ string, _ client.TxnOperator) (executor.Result, error) {
		sqls = append(sqls, sql)
		return executor.Result{}, nil
	})
	defer stub1.Reset()

	tblDef := newTestIvfTableDef("pk", types.T_int64, "vec", types.T_array_float32, 2)
	cnUUID := "a-b-c-d"
	info := newTestIvfConsumerInfo()
	job := newTestJobID()
	catalog.SetupDefines("")
	cnEngine, cnClient, _ := testengine.New(ctx)

	consumer, err := NewConsumer(cnUUID, cnEngine, cnClient, tblDef, job, info)
	require.NoError(t, err)

	bat := testutil.NewBatchWithVectors(
		[]*vector.Vector{
			testutil.NewVector(2, types.T_int64.ToType(), proc.Mp(), false, []int64{1, 2}),
			testutil.NewVector(2, types.T_array_float32.ToType(), proc.Mp(), false, [][]float32{{0.1, 0.2}, {0.3, 0.4}}),
			testutil.NewVector(2, types.T_int32.ToType(), proc.Mp(), false, []int64{1, 2}),
		}, nil)
	defer bat.Clean(testutil.TestUtilMp)

	fromTs := types.BuildTS(1, 0)
	insertAtomicBat := &AtomicBatch{
		Mp:      nil,
		Batches: []*batch.Batch{bat},
		Rows:    btree.NewBTreeGOptions(AtomicBatchRow.Less, btree.Options{Degree: 64}),
	}
	insertAtomicBat.Rows.Set(AtomicBatchRow{Ts: fromTs, Pk: []byte{1}, Offset: 0, Src: bat})
	insertAtomicBat.Rows.Set(AtomicBatchRow{Ts: fromTs, Pk: []byte{2}, Offset: 1, Src: bat})

	delbat := testutil.NewBatchWithVectors(
		[]*vector.Vector{
			testutil.NewVector(2, types.T_int64.ToType(), proc.Mp(), false, []int64{1, 2}),
			testutil.NewVector(2, types.T_array_float32.ToType(), proc.Mp(), false, [][]float32{{0.1, 0.2}, {0.3, 0.4}}),
			testutil.NewVector(2, types.T_int32.ToType(), proc.Mp(), false, []int64{1, 2}),
		}, nil)

	defer delbat.Clean(testutil.TestUtilMp)

	delfromTs := types.BuildTS(2, 0)
	delAtomicBat := &AtomicBatch{
		Mp:      nil,
		Batches: []*batch.Batch{delbat},
		Rows:    btree.NewBTreeGOptions(AtomicBatchRow.Less, btree.Options{Degree: 64}),
	}
	delAtomicBat.Rows.Set(AtomicBatchRow{Ts: delfromTs, Pk: []byte{1}, Offset: 0, Src: bat})
	delAtomicBat.Rows.Set(AtomicBatchRow{Ts: delfromTs, Pk: []byte{2}, Offset: 1, Src: bat})

	output := &MockRetriever{
		dtype:       ISCPDataType_Tail,
		insertBatch: insertAtomicBat,
		deleteBatch: delAtomicBat,
		noMoreData:  false,
	}

	err = consumer.Consume(ctx, output)
	require.NoError(t, err)

	//fmt.Printf("RES: %v\n", sqls)
	require.Equal(t, len(sqls), 2)

	row1 := []any{int64(1), []float32{0.1, 0.2}}
	row2 := []any{int64(2), []float32{0.3, 0.4}}

	writer, err := NewIvfflatSqlWriter("ivfflat", job, info, tblDef, tblDef.Indexes)
	require.NoError(t, err)
	writer.Insert(ctx, row1)
	writer.Insert(ctx, row2)
	writer.Delete(ctx, row1)
	writer.Delete(ctx, row2)
	expectedSqlBytes, _ := writer.ToSql()

	//fmt.Printf("EEEEEEEEEEEE %s %s\n", expectedSqlBytes, sqls[0])
	require.Equal(t, string(expectedSqlBytes), sqls[0])

}

func TestHnswConsumer(t *testing.T) {

	//proc := testutil.NewProcess(t)

	ctx := context.WithValue(context.Background(), defines.TenantIDKey{}, catalog.System_Account)
	ctx, cancel := context.WithCancel(ctx)
	defer cancel()

	sqls := make([]string, 0, 1)
	stub1 := gostub.Stub(&ExecWithResult, func(_ context.Context, sql string, _ string, _ client.TxnOperator) (executor.Result, error) {
		sqls = append(sqls, sql)
		return executor.Result{}, nil
	})
	defer stub1.Reset()

	tblDef := newTestTableDef("pk", types.T_int64, "vec", types.T_array_float32, 2)
	info := newTestConsumerInfo()
	job := newTestJobID()
	/*
		cnUUID := "a-b-c-d"
		catalog.SetupDefines("")
		cnEngine, cnClient, _ := testengine.New(ctx)
	*/

	t.Run("HnswSqlWriter", func(t *testing.T) {

		row1 := []any{int64(1), []float32{0.1, 0.2}}
		row2 := []any{int64(2), []float32{0.3, 0.4}}

		writer, err := NewHnswSqlWriter("hnsw", job, info, tblDef, tblDef.Indexes)
		require.NoError(t, err)
		writer.Insert(ctx, row1)
		writer.Insert(ctx, row2)
		writer.Delete(ctx, row1)
		writer.Delete(ctx, row2)
		json, _ := writer.ToSql()

		expectedSqlBytes := `{"cdc":[{"t":"I","pk":1,"v":[0.1,0.2]},{"t":"I","pk":2,"v":[0.3,0.4]},{"t":"D","pk":1},{"t":"D","pk":2}]}`
		require.Equal(t, expectedSqlBytes, string(json))

	})
}
