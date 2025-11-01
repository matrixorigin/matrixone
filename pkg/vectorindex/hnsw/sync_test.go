// Copyright 2022 Matrix Origin
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

package hnsw

import (
	"testing"
	"time"

	"github.com/matrixorigin/matrixone/pkg/catalog"
	"github.com/matrixorigin/matrixone/pkg/common/moerr"
	"github.com/matrixorigin/matrixone/pkg/common/mpool"
	"github.com/matrixorigin/matrixone/pkg/container/batch"
	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/pb/plan"
	"github.com/matrixorigin/matrixone/pkg/testutil"
	"github.com/matrixorigin/matrixone/pkg/txn/client"
	"github.com/matrixorigin/matrixone/pkg/util/executor"
	"github.com/matrixorigin/matrixone/pkg/vectorindex"
	"github.com/matrixorigin/matrixone/pkg/vectorindex/sqlexec"
	"github.com/stretchr/testify/require"
	"golang.org/x/exp/rand"
)

/*
// give metadata [index_id, checksum, timestamp]
func mock_runSql(proc *process.Process, sql string) (executor.Result, error) {
	return executor.Result{Mp: proc.Mp(), Batches: []*batch.Batch{makeMetaBatch(proc)}}, nil
}

// give blob
func mock_runSql_streaming(proc *process.Process, sql string, ch chan executor.Result, err_chan chan error) (executor.Result, error) {
	defer close(ch)
	res := executor.Result{Mp: proc.Mp(), Batches: []*batch.Batch{makeIndexBatch(proc)}}
	ch <- res
	return executor.Result{}, nil
}
*/

func mockEmptyMoIndexes() []*plan.IndexDef {
	return []*plan.IndexDef{}
}

func mockMoIndexes() []*plan.IndexDef {
	return []*plan.IndexDef{
		{
			IndexName:          "idx",
			Parts:              []string{"embed"},
			Unique:             false,
			IndexTableName:     "__meta",
			TableExist:         true,
			IndexAlgo:          "hnsw",
			IndexAlgoTableType: catalog.Hnsw_TblType_Metadata,
			IndexAlgoParams:    "{\"op_type\": \"vector_l2_ops\", \"m\":\"128\",\"ef_construction\":\"256\", \"ef_search\":\"100\"}",
		},
		{
			IndexName:          "idx",
			Parts:              []string{"embed"},
			Unique:             false,
			IndexTableName:     "__storage",
			TableExist:         true,
			IndexAlgo:          "hnsw",
			IndexAlgoTableType: catalog.Hnsw_TblType_Metadata,
			IndexAlgoParams:    "{\"op_type\": \"vector_l2_ops\", \"m\":\"128\",\"ef_construction\":\"256\", \"ef_search\":\"100\"}",
		},
	}

}

type MockTxnExecutor struct {
}

func (e *MockTxnExecutor) Use(db string) {
}
func (e *MockTxnExecutor) LockTable(table string) error {
	return nil
}

// NOTE: If you specify `AccoundId` in `StatementOption`, sql will be executed under that tenant.
// If not specified, it will be executed under the system tenant by default.
func (e *MockTxnExecutor) Exec(sql string, options executor.StatementOption) (executor.Result, error) {
	if sql == "fake" {
		return executor.Result{}, moerr.NewInternalErrorNoCtx("MockTxnExecutor error")
	}
	return executor.Result{}, nil
}

func (e *MockTxnExecutor) Txn() client.TxnOperator {
	return nil
}

// give metadata [index_id, checksum, timestamp]
func mock_runSql_empty(sqlproc *sqlexec.SqlProcess, sql string) (executor.Result, error) {
	proc := sqlproc.Proc
	return executor.Result{Mp: proc.Mp(), Batches: []*batch.Batch{}}, nil
}

func mock_runTxn(sqlproc *sqlexec.SqlProcess, fn func(exec executor.TxnExecutor) error) error {
	exec := &MockTxnExecutor{}
	err := fn(exec)
	return err
}

func TestSyncRunSqls(t *testing.T) {
	m := mpool.MustNewZero()
	proc := testutil.NewProcessWithMPool(t, "", m)
	sqlproc := sqlexec.NewSqlProcess(proc)

	runTxn = mock_runTxn

	sync := &HnswSync[float32]{}
	defer sync.Destroy()

	sqls := []string{"fake"}
	err := sync.runSqls(sqlproc, sqls)
	require.NotNil(t, err)

	sqls = []string{"sql"}
	err = sync.runSqls(sqlproc, sqls)
	require.Nil(t, err)
}

func TestSyncEmptyCatalogError(t *testing.T) {

	m := mpool.MustNewZero()
	proc := testutil.NewProcessWithMPool(t, "", m)
	sqlproc := sqlexec.NewSqlProcess(proc)

	runSql = mock_runSql_empty
	runSql_streaming = mock_runSql_streaming
	runTxn = mock_runTxn

	indexes := mockEmptyMoIndexes()

	cdc := vectorindex.VectorIndexCdc[float32]{Data: make([]vectorindex.VectorIndexCdcEntry[float32], 0, 1000)}

	key := int64(1000)
	v := []float32{0.1, 0.2, 0.3}

	for i := 0; i < 1000; i++ {
		e := vectorindex.VectorIndexCdcEntry[float32]{Type: vectorindex.CDC_UPSERT, PKey: key, Vec: v}
		cdc.Data = append(cdc.Data, e)
		key += 1
		for i := range len(v) {
			v[i] += 0.01
		}
	}

	_, err := NewHnswSync[float32](sqlproc, "db", "src", "idx", indexes, int32(types.T_array_float32), 3)
	require.NotNil(t, err)
}

func TestSyncUpsertWithEmpty(t *testing.T) {

	m := mpool.MustNewZero()
	proc := testutil.NewProcessWithMPool(t, "", m)
	sqlproc := sqlexec.NewSqlProcess(proc)

	runSql = mock_runSql_empty
	runSql_streaming = mock_runSql_streaming
	runTxn = mock_runTxn

	indexes := mockMoIndexes()
	cdc := vectorindex.VectorIndexCdc[float32]{Data: make([]vectorindex.VectorIndexCdcEntry[float32], 0, 1000)}

	key := int64(1000)
	v := []float32{0.1, 0.2, 0.3}

	for i := 0; i < 1000; i++ {
		e := vectorindex.VectorIndexCdcEntry[float32]{Type: vectorindex.CDC_UPSERT, PKey: key, Vec: v}
		cdc.Data = append(cdc.Data, e)
		key += 1
		for i := range len(v) {
			v[i] += 0.01
		}
	}

	sync, err := NewHnswSync[float32](sqlproc, "db", "src", "idx", indexes, int32(types.T_array_float32), 3)
	require.Nil(t, err)
	err = sync.RunOnce(sqlproc, &cdc)
	require.Nil(t, err)
}

func TestSyncVariableError(t *testing.T) {
	m := mpool.MustNewZero()
	proc := testutil.NewProcessWithMPool(t, "", m)
	sqlproc := sqlexec.NewSqlProcess(proc)

	runSql = mock_runSql_2files
	runSql_streaming = mock_runSql_streaming_2files
	runTxn = mock_runTxn
	indexes := mockMoIndexes()

	cdc := vectorindex.VectorIndexCdc[float32]{Data: make([]vectorindex.VectorIndexCdcEntry[float32], 0, 100)}

	key := int64(0)
	v := []float32{0.1, 0.2, 0.3}

	// 0 - 199 key exists, 200 - 399 new insert
	for i := 0; i < 400; i++ {
		e := vectorindex.VectorIndexCdcEntry[float32]{Type: vectorindex.CDC_UPSERT, PKey: key, Vec: v}
		cdc.Data = append(cdc.Data, e)
		key += 1
	}

	proc.SetResolveVariableFunc(func(key string, b1 bool, b2 bool) (any, error) {
		switch key {
		case "hnsw_max_index_capacity":
			return int64(10), moerr.NewInternalErrorNoCtx("hnsw_max_index_capacity error")
		case "hnsw_threads_build":
			return int64(8), moerr.NewInternalErrorNoCtx("hnsw_threads_build error")
		default:
			return int64(0), nil
		}
	})

	_, err := NewHnswSync[float32](sqlproc, "db", "src", "idx", indexes, int32(types.T_array_float32), 3)
	require.NotNil(t, err)

	//err = sync.RunOnce(sqlproc, &cdc)
	// require.Nil(t, err)

	proc.SetResolveVariableFunc(func(key string, b1 bool, b2 bool) (any, error) {
		switch key {
		case "hnsw_max_index_capacity":
			return int64(10), moerr.NewInternalErrorNoCtx("hnsw_max_index_capacity error")
		case "hnsw_threads_build":
			//return int64(8), moerr.NewInternalErrorNoCtx("hnsw_threads_build error")
			return int64(10), nil
		default:
			return int64(0), nil
		}
	})

	_, err = NewHnswSync[float32](sqlproc, "db", "src", "idx", indexes, int32(types.T_array_float32), 3)
	require.NotNil(t, err)
}

func TestSyncUpsert(t *testing.T) {

	m := mpool.MustNewZero()
	proc := testutil.NewProcessWithMPool(t, "", m)
	sqlproc := sqlexec.NewSqlProcess(proc)

	runSql = mock_runSql
	runSql_streaming = mock_runSql_streaming
	runTxn = mock_runTxn
	indexes := mockMoIndexes()

	cdc := vectorindex.VectorIndexCdc[float32]{Data: make([]vectorindex.VectorIndexCdcEntry[float32], 0, 1000)}

	key := int64(1000)
	v := []float32{0.1, 0.2, 0.3}

	for i := 0; i < 1000; i++ {
		e := vectorindex.VectorIndexCdcEntry[float32]{Type: vectorindex.CDC_UPSERT, PKey: key, Vec: v}
		cdc.Data = append(cdc.Data, e)
		key += 1
		for i := range len(v) {
			v[i] += 0.01
		}
	}

	sync, err := NewHnswSync[float32](sqlproc, "db", "src", "idx", indexes, int32(types.T_array_float32), 3)
	require.Nil(t, err)
	err = sync.RunOnce(sqlproc, &cdc)
	require.Nil(t, err)
}

// should delete all items
func TestSyncDelete(t *testing.T) {

	m := mpool.MustNewZero()
	proc := testutil.NewProcessWithMPool(t, "", m)
	sqlproc := sqlexec.NewSqlProcess(proc)

	runSql = mock_runSql
	runSql_streaming = mock_runSql_streaming
	runTxn = mock_runTxn
	indexes := mockMoIndexes()

	cdc := vectorindex.VectorIndexCdc[float32]{Data: make([]vectorindex.VectorIndexCdcEntry[float32], 0, 1000)}

	key := int64(0)

	for i := 0; i < 100; i++ {
		e := vectorindex.VectorIndexCdcEntry[float32]{Type: vectorindex.CDC_DELETE, PKey: key}
		cdc.Data = append(cdc.Data, e)
		key += 1
	}

	sync, err := NewHnswSync[float32](sqlproc, "db", "src", "idx", indexes, int32(types.T_array_float32), 3)
	require.Nil(t, err)
	err = sync.RunOnce(sqlproc, &cdc)
	require.Nil(t, err)
}

// should delete items and add the same keys back to the model
func TestSyncDeleteAndInsert(t *testing.T) {

	m := mpool.MustNewZero()
	proc := testutil.NewProcessWithMPool(t, "", m)
	sqlproc := sqlexec.NewSqlProcess(proc)

	runSql = mock_runSql
	runSql_streaming = mock_runSql_streaming
	runTxn = mock_runTxn
	indexes := mockMoIndexes()

	cdc := vectorindex.VectorIndexCdc[float32]{Data: make([]vectorindex.VectorIndexCdcEntry[float32], 0, 200)}

	key := int64(0)

	for i := 0; i < 100; i++ {
		e := vectorindex.VectorIndexCdcEntry[float32]{Type: vectorindex.CDC_DELETE, PKey: key}
		cdc.Data = append(cdc.Data, e)
		key += 1
	}

	key = 0
	v := []float32{0.1, 0.2, 0.3}
	for i := 0; i < 100; i++ {
		e := vectorindex.VectorIndexCdcEntry[float32]{Type: vectorindex.CDC_INSERT, PKey: key, Vec: v}
		cdc.Data = append(cdc.Data, e)
		key += 1

	}

	sync, err := NewHnswSync[float32](sqlproc, "db", "src", "idx", indexes, int32(types.T_array_float32), 3)
	require.Nil(t, err)
	err = sync.RunOnce(sqlproc, &cdc)
	require.Nil(t, err)
}

// should delete items and add the same keys back to the model
func TestSyncUpdate(t *testing.T) {

	m := mpool.MustNewZero()
	proc := testutil.NewProcessWithMPool(t, "", m)
	sqlproc := sqlexec.NewSqlProcess(proc)

	runSql = mock_runSql
	runSql_streaming = mock_runSql_streaming
	runTxn = mock_runTxn
	indexes := mockMoIndexes()

	cdc := vectorindex.VectorIndexCdc[float32]{Data: make([]vectorindex.VectorIndexCdcEntry[float32], 0, 100)}

	key := int64(0)
	v := []float32{0.1, 0.2, 0.3}

	for i := 0; i < 100; i++ {
		e := vectorindex.VectorIndexCdcEntry[float32]{Type: vectorindex.CDC_UPSERT, PKey: key, Vec: v}
		cdc.Data = append(cdc.Data, e)
		key += 1
	}

	sync, err := NewHnswSync[float32](sqlproc, "db", "src", "idx", indexes, int32(types.T_array_float32), 3)
	require.Nil(t, err)
	err = sync.RunOnce(sqlproc, &cdc)
	require.Nil(t, err)
}

// should delete items and add the same keys back to the model
func TestSyncDeleteAndUpsert(t *testing.T) {

	m := mpool.MustNewZero()
	proc := testutil.NewProcessWithMPool(t, "", m)
	sqlproc := sqlexec.NewSqlProcess(proc)

	runSql = mock_runSql
	runSql_streaming = mock_runSql_streaming
	runTxn = mock_runTxn
	indexes := mockMoIndexes()

	cdc := vectorindex.VectorIndexCdc[float32]{Data: make([]vectorindex.VectorIndexCdcEntry[float32], 0, 200)}

	key := int64(0)

	for i := 0; i < 100; i++ {
		e := vectorindex.VectorIndexCdcEntry[float32]{Type: vectorindex.CDC_DELETE, PKey: key}
		cdc.Data = append(cdc.Data, e)
		key += 1
	}

	key = 0
	v := []float32{0.1, 0.2, 0.3}
	for i := 0; i < 100; i++ {
		e := vectorindex.VectorIndexCdcEntry[float32]{Type: vectorindex.CDC_UPSERT, PKey: key, Vec: v}
		cdc.Data = append(cdc.Data, e)
		key += 1

	}

	sync, err := NewHnswSync[float32](sqlproc, "db", "src", "idx", indexes, int32(types.T_array_float32), 3)
	require.Nil(t, err)
	err = sync.RunOnce(sqlproc, &cdc)
	require.Nil(t, err)
}

// total 1000100 items and should have two models
func TestSyncAddOneModel(t *testing.T) {

	m := mpool.MustNewZero()
	proc := testutil.NewProcessWithMPool(t, "", m)
	sqlproc := sqlexec.NewSqlProcess(proc)

	runSql = mock_runSql
	runSql_streaming = mock_runSql_streaming
	runTxn = mock_runTxn
	indexes := mockMoIndexes()

	cdc := vectorindex.VectorIndexCdc[float32]{Data: make([]vectorindex.VectorIndexCdcEntry[float32], 0, 1000000)}

	key := int64(1000)
	v := []float32{0.1, 0.2, 0.3}

	for i := 0; i < 1000000; i++ {
		e := vectorindex.VectorIndexCdcEntry[float32]{Type: vectorindex.CDC_UPSERT, PKey: key, Vec: v}
		cdc.Data = append(cdc.Data, e)
		key += 1
		for i := range len(v) {
			v[i] += 0.001
		}
	}

	sync, err := NewHnswSync[float32](sqlproc, "db", "src", "idx", indexes, int32(types.T_array_float32), 3)
	require.Nil(t, err)
	err = sync.RunOnce(sqlproc, &cdc)
	require.Nil(t, err)
}

// should delete all items
func TestSyncDelete2Files(t *testing.T) {

	m := mpool.MustNewZero()
	proc := testutil.NewProcessWithMPool(t, "", m)
	sqlproc := sqlexec.NewSqlProcess(proc)

	runSql = mock_runSql_2files
	runSql_streaming = mock_runSql_streaming_2files
	runTxn = mock_runTxn
	indexes := mockMoIndexes()

	cdc := vectorindex.VectorIndexCdc[float32]{Data: make([]vectorindex.VectorIndexCdcEntry[float32], 0, 1000)}

	key := int64(0)

	for i := 0; i < 200; i++ {
		e := vectorindex.VectorIndexCdcEntry[float32]{Type: vectorindex.CDC_DELETE, PKey: key}
		cdc.Data = append(cdc.Data, e)
		key += 1
	}

	sync, err := NewHnswSync[float32](sqlproc, "db", "src", "idx", indexes, int32(types.T_array_float32), 3)
	require.Nil(t, err)
	err = sync.RunOnce(sqlproc, &cdc)
	require.Nil(t, err)
}

// should delete all items
func TestSyncDeleteShuffle2Files(t *testing.T) {

	m := mpool.MustNewZero()
	proc := testutil.NewProcessWithMPool(t, "", m)
	sqlproc := sqlexec.NewSqlProcess(proc)

	runSql = mock_runSql_2files
	runSql_streaming = mock_runSql_streaming_2files
	runTxn = mock_runTxn
	indexes := mockMoIndexes()

	cdc := vectorindex.VectorIndexCdc[float32]{Data: make([]vectorindex.VectorIndexCdcEntry[float32], 0, 1000)}

	key := int64(0)

	for i := 0; i < 200; i++ {
		e := vectorindex.VectorIndexCdcEntry[float32]{Type: vectorindex.CDC_DELETE, PKey: key}
		cdc.Data = append(cdc.Data, e)
		key += 1
	}

	rand.Seed(uint64(time.Now().UnixNano()))
	rand.Shuffle(len(cdc.Data), func(i, j int) { cdc.Data[i], cdc.Data[j] = cdc.Data[j], cdc.Data[i] })

	sync, err := NewHnswSync[float32](sqlproc, "db", "src", "idx", indexes, int32(types.T_array_float32), 3)
	require.Nil(t, err)
	err = sync.RunOnce(sqlproc, &cdc)
	require.Nil(t, err)
}

// should delete items and add the same keys back to the model
func TestSyncUpdateShuffle2Files(t *testing.T) {

	m := mpool.MustNewZero()
	proc := testutil.NewProcessWithMPool(t, "", m)
	sqlproc := sqlexec.NewSqlProcess(proc)

	runSql = mock_runSql_2files
	runSql_streaming = mock_runSql_streaming_2files
	runTxn = mock_runTxn
	indexes := mockMoIndexes()

	cdc := vectorindex.VectorIndexCdc[float32]{Data: make([]vectorindex.VectorIndexCdcEntry[float32], 0, 100)}

	key := int64(0)
	v := []float32{0.1, 0.2, 0.3}

	for i := 0; i < 200; i++ {
		e := vectorindex.VectorIndexCdcEntry[float32]{Type: vectorindex.CDC_UPSERT, PKey: key, Vec: v}
		cdc.Data = append(cdc.Data, e)
		key += 1
	}

	rand.Seed(uint64(time.Now().UnixNano()))
	rand.Shuffle(len(cdc.Data), func(i, j int) { cdc.Data[i], cdc.Data[j] = cdc.Data[j], cdc.Data[i] })

	sync, err := NewHnswSync[float32](sqlproc, "db", "src", "idx", indexes, int32(types.T_array_float32), 3)
	require.Nil(t, err)
	err = sync.RunOnce(sqlproc, &cdc)
	require.Nil(t, err)
}

// should update and insert items

func runSyncUpdateInsertShuffle2Files[T types.RealNumbers](t *testing.T) {
	m := mpool.MustNewZero()
	proc := testutil.NewProcessWithMPool(t, "", m)
	sqlproc := sqlexec.NewSqlProcess(proc)

	runSql = mock_runSql_2files
	runSql_streaming = mock_runSql_streaming_2files
	runTxn = mock_runTxn
	indexes := mockMoIndexes()

	cdc := vectorindex.VectorIndexCdc[T]{Data: make([]vectorindex.VectorIndexCdcEntry[T], 0, 100)}

	key := int64(0)
	v := []T{0.1, 0.2, 0.3}

	// 0 - 199 key exists, 200 - 399 new insert
	for i := 0; i < 400; i++ {
		e := vectorindex.VectorIndexCdcEntry[T]{Type: vectorindex.CDC_UPSERT, PKey: key, Vec: v}
		cdc.Data = append(cdc.Data, e)
		key += 1
	}

	rand.Seed(uint64(time.Now().UnixNano()))
	rand.Shuffle(len(cdc.Data), func(i, j int) { cdc.Data[i], cdc.Data[j] = cdc.Data[j], cdc.Data[i] })

	var ff T
	switch any(ff).(type) {
	case float32:
		sync, err := NewHnswSync[T](sqlproc, "db", "src", "idx", indexes, int32(types.T_array_float32), 3)
		require.Nil(t, err)
		err = sync.RunOnce(sqlproc, &cdc)
		require.Nil(t, err)
	case float64:
		sync, err := NewHnswSync[T](sqlproc, "db", "src", "idx", indexes, int32(types.T_array_float64), 3)
		require.Nil(t, err)
		err = sync.RunOnce(sqlproc, &cdc)
		require.Nil(t, err)
	}
}

func TestSyncUpdateInsertShuffle2FilesF32(t *testing.T) {
	runSyncUpdateInsertShuffle2Files[float32](t)
}

func TestSyncUpdateInsertShuffle2FilesF64(t *testing.T) {
	runSyncUpdateInsertShuffle2Files[float64](t)
}

func runSyncUpdateInsertShuffle2FilesWithSmallCap[T types.RealNumbers](t *testing.T) {
	m := mpool.MustNewZero()
	proc := testutil.NewProcessWithMPool(t, "", m)
	sqlproc := sqlexec.NewSqlProcess(proc)

	proc.SetResolveVariableFunc(func(key string, b1 bool, b2 bool) (any, error) {
		switch key {
		case "hnsw_max_index_capacity":
			return int64(10), nil
		case "hnsw_threads_build":
			return int64(8), nil
		default:
			return int64(0), nil
		}
	})

	runSql = mock_runSql_2files
	runSql_streaming = mock_runSql_streaming_2files
	runTxn = mock_runTxn
	indexes := mockMoIndexes()

	cdc := vectorindex.VectorIndexCdc[T]{Data: make([]vectorindex.VectorIndexCdcEntry[T], 0, 100)}

	key := int64(0)
	v := []T{0.1, 0.2, 0.3}

	// 0 - 199 key exists, 200 - 399 new insert
	for i := 0; i < 400; i++ {
		e := vectorindex.VectorIndexCdcEntry[T]{Type: vectorindex.CDC_UPSERT, PKey: key, Vec: v}
		cdc.Data = append(cdc.Data, e)
		key += 1
	}

	rand.Seed(uint64(time.Now().UnixNano()))
	rand.Shuffle(len(cdc.Data), func(i, j int) { cdc.Data[i], cdc.Data[j] = cdc.Data[j], cdc.Data[i] })

	var ff T
	switch any(ff).(type) {
	case float32:
		sync, err := NewHnswSync[T](sqlproc, "db", "src", "idx", indexes, int32(types.T_array_float32), 3)
		require.Nil(t, err)
		err = sync.RunOnce(sqlproc, &cdc)
		require.Nil(t, err)
	case float64:
		sync, err := NewHnswSync[T](sqlproc, "db", "src", "idx", indexes, int32(types.T_array_float64), 3)
		require.Nil(t, err)
		err = sync.RunOnce(sqlproc, &cdc)
		require.Nil(t, err)

	}
}

func TestSyncUpdateInsertShuffle2FilesF32WithSmallCap(t *testing.T) {
	runSyncUpdateInsertShuffle2FilesWithSmallCap[float32](t)
}

func runSyncContinuousUpdateInsertShuffle2FilesWithSmallCap[T types.RealNumbers](t *testing.T) {
	m := mpool.MustNewZero()
	proc := testutil.NewProcessWithMPool(t, "", m)
	sqlproc := sqlexec.NewSqlProcess(proc)

	proc.SetResolveVariableFunc(func(key string, b1 bool, b2 bool) (any, error) {
		switch key {
		case "hnsw_max_index_capacity":
			return int64(10), nil
		case "hnsw_threads_build":
			return int64(8), nil
		default:
			return int64(0), nil
		}
	})

	runSql = mock_runSql_2files
	runSql_streaming = mock_runSql_streaming_2files
	runTxn = mock_runTxn
	indexes := mockMoIndexes()

	cdc := vectorindex.VectorIndexCdc[T]{Data: make([]vectorindex.VectorIndexCdcEntry[T], 0, 100)}

	key := int64(0)
	v := []T{0.1, 0.2, 0.3}

	// 0 - 199 key exists, 200 - 399 new insert
	for i := 0; i < 400; i++ {
		e := vectorindex.VectorIndexCdcEntry[T]{Type: vectorindex.CDC_UPSERT, PKey: key, Vec: v}
		cdc.Data = append(cdc.Data, e)
		key += 1
	}

	rand.Seed(uint64(time.Now().UnixNano()))
	rand.Shuffle(len(cdc.Data), func(i, j int) { cdc.Data[i], cdc.Data[j] = cdc.Data[j], cdc.Data[i] })

	var err error
	var sync *HnswSync[T]

	var ff T
	switch any(ff).(type) {
	case float32:
		sync, err = NewHnswSync[T](sqlproc, "db", "src", "idx", indexes, int32(types.T_array_float32), 3)
		require.Nil(t, err)

	case float64:
		sync, err = NewHnswSync[T](sqlproc, "db", "src", "idx", indexes, int32(types.T_array_float64), 3)
		require.Nil(t, err)
	}

	defer sync.Destroy()
	for i := 0; i < 10; i++ {
		err = sync.Update(sqlproc, &cdc)
		require.Nil(t, err)
	}

	err = sync.Save(sqlproc)
	require.NoError(t, err)
}

func TestSyncContinuousUpdateInsertShuffle2FilesF32WithSmallCap(t *testing.T) {
	runSyncContinuousUpdateInsertShuffle2FilesWithSmallCap[float32](t)
}
