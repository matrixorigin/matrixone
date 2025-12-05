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

package test

import (
	"context"
	"fmt"
	"testing"

	"github.com/lni/goutils/leaktest"
	"github.com/stretchr/testify/require"

	"github.com/matrixorigin/matrixone/pkg/catalog"
	"github.com/matrixorigin/matrixone/pkg/common/runtime"
	"github.com/matrixorigin/matrixone/pkg/container/types"
	planpb "github.com/matrixorigin/matrixone/pkg/pb/plan"
	"github.com/matrixorigin/matrixone/pkg/sql/plan"
	"github.com/matrixorigin/matrixone/pkg/sql/plan/explain"
	"github.com/matrixorigin/matrixone/pkg/util/executor"
	"github.com/matrixorigin/matrixone/pkg/vm/engine"
	catalog2 "github.com/matrixorigin/matrixone/pkg/vm/engine/tae/catalog"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/testutils/config"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/test/testutil"
)

// TestAssociativeLawRemapping tests that associative law transformations
// correctly migrate OnList conditions to avoid remapping errors
func TestAssociativeLawRemapping(t *testing.T) {
	defer leaktest.AfterTest(t)()

	catalog.SetupDefines("")

	opts := testutil.TestOptions{}
	opts.TaeEngineOptions = config.WithLongScanAndCKPOpts(nil)
	p := testutil.InitEnginePack(opts, t)
	defer p.Close()

	dbName := "test_db"
	ctx := p.Ctx

	// Create database and tables using enginepack style
	txnop := p.StartCNTxn()

	// Helper function to create schema with primary key constraint
	createSchemaWithPK := func(name string, pkColName string, pkColType types.Type, otherCols map[string]types.Type) *catalog2.Schema {
		schema := catalog2.NewEmptySchema(name)
		schema.AppendPKCol(pkColName, pkColType, 0)
		for colName, colType := range otherCols {
			schema.AppendCol(colName, colType)
		}

		// Create primary key constraint
		pkDef := &planpb.PrimaryKeyDef{
			Names:       []string{pkColName},
			PkeyColName: pkColName,
		}
		pkConstraint := &engine.PrimaryKeyDef{
			Pkey: pkDef,
		}
		constraintDef := &engine.ConstraintDef{
			Cts: []engine.Constraint{pkConstraint},
		}
		schema.Constraint, _ = constraintDef.MarshalBinary()
		schema.Finalize(false)
		return schema
	}

	// Create connector_job table schema
	jobSchema := createSchemaWithPK("connector_job", "id", types.T_int64.ToType(), map[string]types.Type{
		"file_id": types.T_int64.ToType(),
		"task_id": types.T_int64.ToType(),
		"status":  types.T_int8.ToType(),
	})

	// Create task table schema
	taskSchema := createSchemaWithPK("task", "id", types.T_int64.ToType(), map[string]types.Type{
		"uid": types.New(types.T_varchar, 255, 0),
	})

	// Create file table schema
	fileSchema := createSchemaWithPK("file", "id", types.T_int64.ToType(), map[string]types.Type{
		"job_id":   types.T_int64.ToType(),
		"task_id":  types.T_int64.ToType(),
		"table_id": types.T_int64.ToType(),
	})

	// Create tables
	_, rels := p.CreateDBAndTables(txnop, dbName, jobSchema, taskSchema, fileSchema)
	jobRel := rels[0]

	// Get table IDs
	jobTableID := jobRel.GetTableID(p.Ctx)

	require.NoError(t, txnop.Commit(ctx))

	// Set stats directly by creating a shared stats cache and injecting it
	// through context. The compiler context will pick it up during query execution.

	// Create stats for each table to trigger associative law rule 1:
	// Rule 1: A*(B*C) -> (A*B)*C when C.selectivity >= 0.9 and B.outcnt < C.outcnt
	// - file (A): large table, 1000 rows
	// - connector_job (B): small after filter, 9 rows -> outcnt = 7.29 after filter
	// - task (C): large table, 1000 rows, selectivity = 1.0

	// Create a shared stats cache that we can populate
	sharedStatsCache := plan.NewStatsCache()

	// Connector_job table (B): small after filter
	jobStats := plan.NewStatsInfo()
	jobStats.TableCnt = 9
	sharedStatsCache.SetStatsInfo(jobTableID, jobStats)

	// Store stats cache in context so compiler context can access it
	ctx = context.WithValue(ctx, "test_stats_cache", sharedStatsCache)

	// Get SQL executor
	v, ok := runtime.ServiceRuntime("").GetGlobalVariables(runtime.InternalSQLExecutor)
	require.True(t, ok)
	exec := v.(executor.SQLExecutor)

	// Test the problematic query
	// This should not cause remapping error after the fix
	txnop = p.StartCNTxn()
	res, err := exec.Exec(ctx, fmt.Sprintf(`
		SELECT DISTINCT t.id 
		FROM %s.connector_job job, %s.task t, %s.file f 
		WHERE job.task_id = t.id 
		  AND f.job_id = job.id  
		  AND f.task_id = t.id 
		  AND job.status != 4 
		  AND job.status != 5 
		  AND t.uid = '019ac448-45b7-7545-9762-2a73f9ab129' 
		  AND f.table_id in (1)
	`, dbName, dbName, dbName), executor.Options{}.WithTxn(txnop))

	// Print logical plan in EXPLAIN format
	if res.LogicalPlan != nil {
		planObj := &plan.Plan{
			Plan: &plan.Plan_Query{
				Query: res.LogicalPlan,
			},
		}
		explainOutput := explain.DebugPlan(planObj)
		t.Logf("Logical Plan (EXPLAIN format):\n%s", explainOutput)
	}
	// The query should succeed without remapping error
	require.NoError(t, err, "Query should not cause remapping error")
	require.NoError(t, txnop.Commit(ctx))

}
