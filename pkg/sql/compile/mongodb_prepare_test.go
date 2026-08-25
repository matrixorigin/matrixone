// Copyright 2026 Matrix Origin
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package compile

import (
	"encoding/json"
	"testing"

	"github.com/golang/mock/gomock"
	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/pb/plan"
	"github.com/matrixorigin/matrixone/pkg/sql/colexec/mongoscan"
	sqlmongodb "github.com/matrixorigin/matrixone/pkg/sql/mongodb"
	"github.com/matrixorigin/matrixone/pkg/util/executor"
	"github.com/matrixorigin/matrixone/pkg/vm/process"
	"github.com/stretchr/testify/require"
)

func TestCompilePreparedMongoScanDoesNotMutateCachedPlan(t *testing.T) {
	columns := []sqlmongodb.ColumnMapping{
		{Name: "value", Path: "reading.value", TypeID: int32(types.T_int64), Conversion: sqlmongodb.ConversionStrict},
		{Name: "quality", Path: "quality", TypeID: int32(types.T_varchar), Conversion: sqlmongodb.ConversionStrict},
	}
	exec := &mongoDBMappingTestExecutor{results: make(map[string]executor.Result)}
	c, _, _ := newMongoDBMappingTestCompile(t, gomock.NewController(t), exec)
	c.addr = "cn-local:6001"
	c.anal = &AnalyzeModule{qry: &plan.Query{}, isFirst: true}
	exec.results[sqlmongodb.GetMappingByTableIDSQL(7, 9)] = mongoDBMappingLookupResult(t, c.proc, columns)

	node := &plan.Node{
		NodeType: plan.Node_EXTERNAL_SCAN,
		TableDef: &plan.TableDef{Cols: []*plan.ColDef{{Name: "value"}}},
		ExternScan: &plan.ExternScan{
			Type: int32(plan.ExternType_MONGODB_TB),
			MongodbScan: &plan.MongoScan{
				TableId: 9, Database: "telemetry", Collection: "events",
				Columns: sqlmongodb.ColumnsToPlan(columns), MaxParallelism: 1,
			},
		},
	}

	scopes, err := c.compileExternScan(node)
	require.NoError(t, err)
	require.Len(t, scopes, 1)
	defer ReleaseScopes(scopes)

	op, ok := scopes[0].RootOp.(*mongoscan.MongoScan)
	require.True(t, ok)
	require.NotSame(t, node.ExternScan.MongodbScan, op.Scan)
	require.Equal(t, uint64(11), op.Scan.MappingId)
	require.Equal(t, uint64(4), op.Scan.MappingVersion)
	require.Equal(t, uint64(22), op.Scan.ConnectionId)
	require.Equal(t, uint64(3), op.Scan.ConnectionVersion)
	require.Len(t, op.Scan.Columns, 1)
	require.Equal(t, "value", op.Scan.Columns[0].Name)

	// A prepared plan is cached and reused across executions. Hydration must
	// therefore remain private to the execution operator built above.
	require.Zero(t, node.ExternScan.MongodbScan.MappingId)
	require.Zero(t, node.ExternScan.MongodbScan.ConnectionId)
	require.Len(t, node.ExternScan.MongodbScan.Columns, 2)
	require.Equal(t, []string{sqlmongodb.GetMappingByTableIDSQL(7, 9)}, exec.sqls)
}

func mongoDBMappingLookupResult(
	t *testing.T,
	proc *process.Process,
	columns []sqlmongodb.ColumnMapping,
) executor.Result {
	t.Helper()
	encodedColumns, err := json.Marshal(columns)
	require.NoError(t, err)
	result := executor.NewMemResult([]types.Type{
		types.T_uint64.ToType(),
		types.T_uint64.ToType(),
		types.T_uint64.ToType(),
		types.T_varchar.ToType(),
		types.T_varchar.ToType(),
		types.T_varchar.ToType(),
		types.T_int32.ToType(),
		types.T_uint64.ToType(),
		types.T_uint64.ToType(),
	}, proc.Mp())
	result.NewBatchWithRowCount(1)
	require.NoError(t, executor.AppendFixedRows(result, 0, []uint64{11}))
	require.NoError(t, executor.AppendFixedRows(result, 1, []uint64{22}))
	require.NoError(t, executor.AppendFixedRows(result, 2, []uint64{3}))
	require.NoError(t, executor.AppendStringRows(result, 3, []string{"telemetry"}))
	require.NoError(t, executor.AppendStringRows(result, 4, []string{"events"}))
	require.NoError(t, executor.AppendBytesRows(result, 5, [][]byte{encodedColumns}))
	require.NoError(t, executor.AppendFixedRows(result, 6, []int32{1}))
	require.NoError(t, executor.AppendFixedRows(result, 7, []uint64{0}))
	require.NoError(t, executor.AppendFixedRows(result, 8, []uint64{4}))
	return result.GetResult()
}
