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
	"errors"
	"fmt"
	"strings"
	"testing"

	"github.com/matrixorigin/matrixone/pkg/catalog"
	"github.com/matrixorigin/matrixone/pkg/common/moerr"
	"github.com/matrixorigin/matrixone/pkg/container/batch"
	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/container/vector"
	"github.com/matrixorigin/matrixone/pkg/logutil"
	"github.com/matrixorigin/matrixone/pkg/pb/plan"
	"github.com/matrixorigin/matrixone/pkg/testutil"
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

func (r *MockRetriever) UpdateWatermark(executor.TxnExecutor, executor.StatementOption) error {
	logutil.Infof("TxnRetriever.UpdateWatermark()")
	return nil
}

func (r *MockRetriever) GetDataType() int8 {
	return r.dtype
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

func newTestConsumerInfo() *ConsumerInfo {
	return &ConsumerInfo{
		DbName:    "test_db",
		TableName: "test_tbl",
		IndexName: "hnsw_idx",
	}
}

type MockSQLExecutor struct {
	txnexec executor.TxnExecutor
	sqls    []string
}

// Exec exec a sql in a exists txn.
func (exec *MockSQLExecutor) Exec(ctx context.Context, sql string, opts executor.Options) (executor.Result, error) {
	exec.sqls = append(exec.sqls, sql)
	//fmt.Printf("Exec %p %v\n", exec.sqls, exec.sqls)
	return executor.Result{}, nil
}

var _ executor.SQLExecutor = new(MockSQLExecutor)

func mockSqlExecutorFactory(uuid string) (executor.SQLExecutor, error) {
	return &MockSQLExecutor{}, nil
}

type MockErrorTxnExecutor struct {
	database string
	ctx      context.Context
	sqls     []string
}

func (exec *MockErrorTxnExecutor) Use(db string) {
	exec.database = db
}

func (exec *MockErrorTxnExecutor) Exec(
	sql string,
	statementOption executor.StatementOption,
) (executor.Result, error) {
	if strings.Contains(sql, "FAILSQL") {
		return executor.Result{}, moerr.NewInternalErrorNoCtx("db error")
	} else if strings.Contains(sql, "MULTI_ERROR_NO_ROLLBACK") {
		var errs error
		errs = errors.Join(errs, moerr.NewInternalErrorNoCtx("db error"))
		errs = errors.Join(errs, moerr.NewInternalErrorNoCtx("db error 2"))
		return executor.Result{}, errs
	} else if strings.Contains(sql, "MULTI_ERROR_ROLLBACK") {
		var errs error
		errs = errors.Join(errs, moerr.NewInternalErrorNoCtx("db error"))
		errs = errors.Join(errs, moerr.NewQueryInterrupted(exec.ctx))
		return executor.Result{}, errs
	}

	exec.sqls = append(exec.sqls, sql)
	//fmt.Printf("APPEND %s", sql)
	return executor.Result{}, nil
}

func (exec *MockErrorTxnExecutor) LockTable(table string) error {
	return nil
}

func (exec *MockErrorTxnExecutor) Txn() client.TxnOperator {
	return nil
}

// ExecTxn executor sql in a txn. execFunc can use TxnExecutor to exec multiple sql
// in a transaction.
// NOTE: Pass SQL stmts one by one to TxnExecutor.Exec(). If you pass multiple SQL stmts to
// TxnExecutor.Exec() as `\n` seperated string, it will only execute the first SQL statement causing Bug.
func (exec *MockSQLExecutor) ExecTxn(ctx context.Context, execFunc func(txn executor.TxnExecutor) error, opts executor.Options) error {
	exec.txnexec = &MockErrorTxnExecutor{ctx: ctx}
	err := execFunc(exec.txnexec)
	exec.sqls = exec.txnexec.(*MockErrorTxnExecutor).sqls
	//fmt.Printf("ExecTxn %v\n", exec.sqls)
	return err
}

func TestConsumer(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	sqlexecstub := gostub.Stub(&sqlExecutorFactory, mockSqlExecutorFactory)
	defer sqlexecstub.Reset()

	r := &MockRetriever{
		insertBatch: nil,
		deleteBatch: nil,
		noMoreData:  true,
		dtype:       ISCPDataType_Snapshot,
	}

	tblDef := newTestTableDef("pk", types.T_int64, "vec", types.T_array_float32, 4)
	cnUUID := "a-b-c-d"
	info := newTestConsumerInfo()

	consumer, err := NewIndexConsumer(cnUUID, tblDef, info)
	require.NoError(t, err)
	err = consumer.Consume(ctx, r)
	require.NoError(t, err)
}

func TestHnswSnapshot(t *testing.T) {

	proc := testutil.NewProcess(t)

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	sqlexecstub := gostub.Stub(&sqlExecutorFactory, mockSqlExecutorFactory)
	defer sqlexecstub.Reset()

	tblDef := newTestTableDef("pk", types.T_int64, "vec", types.T_array_float32, 2)
	cnUUID := "a-b-c-d"
	info := newTestConsumerInfo()

	t.Run("snapshot", func(t *testing.T) {
		consumer, err := NewIndexConsumer(cnUUID, tblDef, info)
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
		sqls := consumer.(*IndexConsumer).exec.(*MockSQLExecutor).sqls
		require.Equal(t, len(sqls), 1)
		sql := sqls[0]
		require.Equal(t, string(sql), `SELECT hnsw_cdc_update('test_db', 'test_tbl', 2, '{"cdc":[{"t":"U","pk":1,"v":[0.1,0.2]},{"t":"U","pk":2,"v":[0.3,0.4]}]}');`)

		//fmt.Printf("Consume %p %v\n", consumer.(*IndexConsumer).exec.(*MockSQLExecutor).sqls, consumer.(*IndexConsumer).exec.(*MockSQLExecutor).sqls)
	})

	t.Run("noMoreData", func(t *testing.T) {
		consumer, err := NewIndexConsumer(cnUUID, tblDef, info)
		require.NoError(t, err)

		output := &MockRetriever{
			dtype:       ISCPDataType_Snapshot,
			insertBatch: nil,
			deleteBatch: nil,
			noMoreData:  true,
		}
		err = consumer.Consume(ctx, output)
		require.NoError(t, err)
		sqls := consumer.(*IndexConsumer).exec.(*MockSQLExecutor).sqls
		require.Equal(t, len(sqls), 0)
	})
}

func TestHnswTail(t *testing.T) {

	proc := testutil.NewProcess(t)

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	sqlexecstub := gostub.Stub(&sqlExecutorFactory, mockSqlExecutorFactory)
	defer sqlexecstub.Reset()

	tblDef := newTestTableDef("pk", types.T_int64, "vec", types.T_array_float32, 2)
	cnUUID := "a-b-c-d"
	info := newTestConsumerInfo()

	consumer, err := NewIndexConsumer(cnUUID, tblDef, info)
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
	sqls := consumer.(*IndexConsumer).exec.(*MockSQLExecutor).txnexec.(*MockErrorTxnExecutor).sqls

	require.Equal(t, len(sqls), 1)
	fmt.Printf("RES: %v\n", sqls)

	row1 := []any{int64(1), []float32{0.1, 0.2}}
	row2 := []any{int64(2), []float32{0.3, 0.4}}

	writer, _ := NewHnswSqlWriter("hnsw", info, tblDef, tblDef.Indexes)
	writer.Insert(ctx, row1)
	writer.Insert(ctx, row2)
	writer.Delete(ctx, row1)
	writer.Delete(ctx, row2)
	/*
		cdcForJson.Start = "1-0"
		cdcForJson.End = "2-0"
	*/
	expectedSqlBytes, _ := writer.ToSql()

	require.Equal(t, string(expectedSqlBytes), sqls[0])

}
