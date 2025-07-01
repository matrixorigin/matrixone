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

package idxcdc

import (
	"context"
	"testing"

	"github.com/matrixorigin/matrixone/pkg/catalog"
	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/pb/plan"
	"github.com/matrixorigin/matrixone/pkg/util/executor"
	"github.com/prashantv/gostub"
	"github.com/stretchr/testify/require"
)

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
}

// Exec exec a sql in a exists txn.
func (exec MockSQLExecutor) Exec(ctx context.Context, sql string, opts executor.Options) (executor.Result, error) {

	return executor.Result{}, nil
}

var _ executor.SQLExecutor = new(MockSQLExecutor)

func mockSqlExecutorFactory(uuid string) (executor.SQLExecutor, error) {
	return MockSQLExecutor{}, nil
}

// ExecTxn executor sql in a txn. execFunc can use TxnExecutor to exec multiple sql
// in a transaction.
// NOTE: Pass SQL stmts one by one to TxnExecutor.Exec(). If you pass multiple SQL stmts to
// TxnExecutor.Exec() as `\n` seperated string, it will only execute the first SQL statement causing Bug.
func (exec MockSQLExecutor) ExecTxn(ctx context.Context, execFunc func(txn executor.TxnExecutor) error, opts executor.Options) error {
	return nil
}

func TestConsumer(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	sqlexecstub := gostub.Stub(&sqlExecutorFactory, mockSqlExecutorFactory)
	defer sqlexecstub.Reset()

	r := NewTxnRetriever()

	tblDef := newTestTableDef("pk", types.T_int64, "vec", types.T_array_float32, 4)
	cnUUID := "a-b-c-d"
	info := &ConsumerInfo{DbName: "db", TableName: "tbl", IndexName: "hnsw_idx"}

	consumer, err := NewIndexConsumer(cnUUID, tblDef, info)
	require.NoError(t, err)
	err = consumer.Consume(ctx, r)
	require.NoError(t, err)
}
