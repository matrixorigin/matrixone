// Copyright 2026 Matrix Origin
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
// http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package compile

import (
	"context"
	"fmt"
	"strings"
	"testing"

	"github.com/google/uuid"
	"github.com/matrixorigin/matrixone/pkg/catalog"
	"github.com/matrixorigin/matrixone/pkg/common/mpool"
	moruntime "github.com/matrixorigin/matrixone/pkg/common/runtime"
	"github.com/matrixorigin/matrixone/pkg/container/batch"
	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/container/vector"
	"github.com/matrixorigin/matrixone/pkg/defines"
	icebergapi "github.com/matrixorigin/matrixone/pkg/iceberg/api"
	icebergio "github.com/matrixorigin/matrixone/pkg/iceberg/io"
	"github.com/matrixorigin/matrixone/pkg/pb/plan"
	"github.com/matrixorigin/matrixone/pkg/sql/colexec/dependent"
	"github.com/matrixorigin/matrixone/pkg/sql/colexec/icebergwrite"
	"github.com/matrixorigin/matrixone/pkg/sql/parsers/dialect/mysql"
	plan2 "github.com/matrixorigin/matrixone/pkg/sql/plan"
	"github.com/matrixorigin/matrixone/pkg/testutil"
	"github.com/matrixorigin/matrixone/pkg/util/executor"
	"github.com/matrixorigin/matrixone/pkg/vm"
	"github.com/matrixorigin/matrixone/pkg/vm/process"
	"github.com/stretchr/testify/require"
)

// Exercise the real continuation with a parsed nested correlated SELECT feeding
// an Iceberg INSERT sink. No ordinary engine is supplied: the sink must retain
// the Iceberg commit boundary, including overwrite planning, across executions.
func TestDependentIcebergInsert(t *testing.T) {
	for _, operation := range []string{"append", "overwrite"} {
		t.Run(operation, func(t *testing.T) {
			proc := testutil.NewProcessWithOwnedMPool(t, "", mpool.MustNewZero())
			defer proc.Free()
			proc.Ctx = defines.AttachAccountId(proc.Ctx, 42)
			proc.SetStmtProfile(process.NewStmtProfile(uuid.Nil, uuid.New()))
			restoreRuntimeVariableForTest(t, proc.GetService(), moruntime.MOProtocolVersion, defines.MORPCVersion119)
			sqlExec := &dependentIcebergCatalog{t: t, proc: proc}
			restoreRuntimeVariableForTest(t, proc.GetService(), moruntime.InternalSQLExecutor, sqlExec)
			var coordinators []*dependentIcebergCoordinator
			restoreRuntimeVariableForTest(t, proc.GetService(), IcebergAppendCoordinatorFactoryRuntimeKey,
				icebergwrite.CoordinatorFactoryFunc(func(ctx context.Context, request icebergwrite.AppendRequest) (icebergwrite.Coordinator, error) {
					require.NoError(t, ctx.Err())
					require.Equal(t, uint32(42), request.AccountID)
					require.Equal(t, operation, request.Operation)
					if operation == "overwrite" {
						require.Equal(t, int64(30), request.DMLScan.BaseSnapshotID)
						require.Equal(t, 9, request.DMLScan.BaseSchemaID)
						require.Equal(t, "main", request.DMLScan.Ref)
						require.Empty(t, request.DMLScan.OverwriteScope)
						require.NotEmpty(t, request.DMLScan.ObjectIORef)
					}
					coordinator := &dependentIcebergCoordinator{statement: request.StatementID}
					coordinators = append(coordinators, coordinator)
					return coordinator, nil
				}))
			stmts, err := mysql.Parse(proc.Ctx, "SELECT (SELECT (SELECT n.N_NATIONKEY)) FROM NATION n", 1)
			require.NoError(t, err)
			defer stmts[0].Free()
			optimizer := plan2.NewMockOptimizer(true, proc)
			bound, err := plan2.BuildPlan(optimizer.CurrentContext(), stmts[0], false)
			require.NoError(t, err)
			q := bound.GetQuery()
			td := &plan.TableDef{Name: "gold_orders", DbId: 1, TblId: 2, TableType: catalog.SystemExternalRel, Createsql: icebergInsertCreatesql(), Cols: []*plan.ColDef{{Name: "id", Typ: plan.Type{Id: int32(types.T_int32)}}}}
			sink := &plan.Node{NodeId: int32(len(q.Nodes)), NodeType: plan.Node_INSERT, Children: []int32{q.Steps[q.MainOutputStep()]}, TableDef: td, InsertCtx: &plan.InsertCtx{TableDef: td, Ref: &plan.ObjectRef{Db: 1, Obj: 2, ObjName: "gold_orders"}}}
			if operation == "overwrite" {
				sink.ExtraOptions = icebergapi.DMLOverwritePlanExtraOptions
			}
			q.Nodes = append(q.Nodes, sink)
			q.Steps[q.MainOutputStep()] = sink.NodeId
			require.NoError(t, q.ValidateCorrelation())
			planner := &fakeCompileIcebergPlanner{plan: &icebergapi.IcebergScanPlan{Snapshot: icebergapi.SnapshotPlan{SnapshotID: 30, SchemaID: 9, RefName: "main"}, ObjectIORef: registerCompileTestObjectIO(t)}}
			// The injected planner reuses one provider; retain its own fixture
			// lease while independently constructed writer generations come/go.
			ref := planner.plan.ObjectIORef
			require.True(t, icebergio.RetainObjectIORef(ref))
			defer icebergio.ReleaseObjectIORef(ref)
			compileCtx, cancelCompile := context.WithCancel(proc.Ctx)
			cancelCompile()
			c := &Compile{proc: &process.Process{Ctx: compileCtx, Base: proc.Base}, db: "db", anal: &AnalyzeModule{qry: q}, icebergScanPlanner: planner}
			// Prove the actual adapter selection, not only the standalone constructor.
			factory, _, err := c.compileDependentKernel(sink, []*plan.Node{q.Nodes[sink.Children[0]]})
			require.NoError(t, err)
			func() {
				native, err := factory(proc, []*dependent.DemandInput{{}})
				require.NoError(t, err)
				defer func() {
					native.Free(proc, false, nil)
					native.Release()
				}()
				require.IsType(t, &icebergwrite.IcebergWrite{}, native)
			}()
			require.Empty(t, coordinators, "construction must not create a runtime coordinator")
			budget, err := proc.GetExecutionResourceBudget()
			require.NoError(t, err)
			registry, err := mpool.NewAllocationAccountRegistry(1, 1024)
			require.NoError(t, err)
			account, err := registry.OpenWithController(1<<26, budget)
			require.NoError(t, err)
			defer func() { account.Seal(); _, err := registry.Finalize(account); require.NoError(t, err) }()
			opened, closed := 0, 0
			template, err := dependent.Compile(proc.Ctx, q, func(node *plan.Node) (dependent.SourceFactory, error) {
				return func(child *process.Process, allocation *vector.AllocationAccountSelection, _ *mpool.AllocationAccount) (dependent.Source, error) {
					opened++
					if node.NodeType == plan.Node_VALUE_SCAN {
						return &dependentFixtureSource{batch: batch.EmptyForConstFoldBatch, rows: 1, close: func() { closed++ }}, nil
					}
					b := batch.NewWithSize(len(node.TableDef.Cols))
					for i := range b.Vecs {
						b.Vecs[i], err = vector.NewOffHeapVecWithTypeAndAllocation(types.T_int32.ToType(), allocation)
						if err != nil {
							b.Clean(child.Mp())
							return nil, err
						}
						for _, v := range []int32{11, 22} {
							if err := vector.AppendFixed(b.Vecs[i], v, false, child.Mp()); err != nil {
								b.Clean(child.Mp())
								return nil, err
							}
						}
					}
					b.SetRowCount(2)
					return &dependentFixtureSource{batch: b, rows: 1, close: func() { closed++; b.Clean(child.Mp()) }}, nil
				}, nil
			}, c.compileDependentKernel)
			require.NoError(t, err)
			for generation := range 2 {
				proc.GetStmtProfile().SetStmtId(uuid.New())
				func() {
					op := &dependent.Operator{Template: template}
					require.NoError(t, op.SetAllocationAccount(account))
					defer func() {
						op.Reset(proc, false, nil)
						op.Free(proc, false, nil)
						require.NoError(t, op.ClearAllocationAccount(account))
						op.Release()
					}()
					require.NoError(t, op.Prepare(proc))
					for {
						result, err := vm.Exec(op, proc)
						require.NoError(t, err)
						if result.Batch == nil {
							break
						}
					}
				}()
				require.Len(t, coordinators, generation+1)
				coord := coordinators[generation]
				require.Equal(t, []int32{11, 22}, coord.values)
				require.Equal(t, 1, coord.begins)
				require.Equal(t, 1, coord.commits)
				require.Zero(t, coord.aborts)
				require.Equal(t, opened, closed)
				require.Zero(t, account.Snapshot().Used)
				require.Zero(t, budget.Used())
			}
			icebergio.ReleaseObjectIORef(ref)
			require.False(t, icebergio.RetainObjectIORef(ref), "no writer lease may outlive its execution")
			require.NotEqual(t, coordinators[0].statement, coordinators[1].statement)
			if operation == "overwrite" {
				require.Positive(t, sqlExec.calls)
			} else {
				require.Zero(t, sqlExec.calls)
			}
		})
	}
}

func TestDependentIcebergInsertRejectsBeforeNativeWrite(t *testing.T) {
	for _, mode := range []string{"malformed_mapping", "invalid_factory", "overwrite_access_denied", "overwrite_planner_error"} {
		t.Run(mode, func(t *testing.T) {
			proc := testutil.NewProc(t)
			proc.Ctx = defines.AttachAccountId(proc.Ctx, 42)
			coordinators := 0
			restoreRuntimeVariableForTest(t, proc.GetService(), IcebergAppendCoordinatorFactoryRuntimeKey,
				icebergwrite.CoordinatorFactoryFunc(func(context.Context, icebergwrite.AppendRequest) (icebergwrite.Coordinator, error) {
					coordinators++
					return &dependentIcebergCoordinator{}, nil
				}))
			td := &plan.TableDef{Name: "gold_orders", DbId: 1, TblId: 2, TableType: catalog.SystemExternalRel, Createsql: icebergInsertCreatesql()}
			node := &plan.Node{NodeType: plan.Node_INSERT, TableDef: td, InsertCtx: &plan.InsertCtx{TableDef: td}}
			catalog := &dependentIcebergCatalog{t: t, proc: proc, denyPrincipal: mode == "overwrite_access_denied"}
			restoreRuntimeVariableForTest(t, proc.GetService(), moruntime.InternalSQLExecutor, catalog)
			c := &Compile{proc: proc, icebergScanPlanner: &fakeCompileIcebergPlanner{err: fmt.Errorf("injected overwrite planning failure")}}
			switch mode {
			case "malformed_mapping":
				td.Createsql = "MO_ICEBERG:{"
			case "invalid_factory":
				restoreRuntimeVariableForTest(t, proc.GetService(), IcebergAppendCoordinatorFactoryRuntimeKey, 123)
			default:
				node.ExtraOptions = icebergapi.DMLOverwritePlanExtraOptions
			}
			factory, _, err := c.compileDependentKernel(node, []*plan.Node{{}})
			require.NoError(t, err)
			op, err := factory(proc, []*dependent.DemandInput{{}})
			require.Error(t, err)
			require.Nil(t, op, "failed Iceberg admission must not fall back to an MO writer")
			require.Zero(t, coordinators)
		})
	}
}

type dependentIcebergCoordinator struct {
	statement               string
	values                  []int32
	begins, commits, aborts int
}

func (c *dependentIcebergCoordinator) Begin(context.Context, icebergwrite.AppendRequest) error {
	c.begins++
	return nil
}
func (c *dependentIcebergCoordinator) Append(_ context.Context, b *batch.Batch) error {
	for row := 0; row < b.RowCount(); row++ {
		c.values = append(c.values, vector.GetFixedAtNoTypeCheck[int32](b.Vecs[0], row))
	}
	return nil
}
func (c *dependentIcebergCoordinator) Commit(context.Context) error       { c.commits++; return nil }
func (c *dependentIcebergCoordinator) Abort(context.Context, error) error { c.aborts++; return nil }

// Only the catalog data boundary is injected; the normal access checks and
// overwrite planner execute unchanged. Unexpected SQL is an error, not a write.
type dependentIcebergCatalog struct {
	executor.SQLExecutor
	t             *testing.T
	proc          *process.Process
	calls         int
	denyPrincipal bool
}

func (e *dependentIcebergCatalog) Exec(ctx context.Context, sql string, _ executor.Options) (executor.Result, error) {
	e.calls++
	if err := ctx.Err(); err != nil {
		return executor.Result{}, err
	}
	var values []any
	switch {
	case strings.Contains(sql, "join mo_catalog.mo_iceberg_catalogs"):
		values = []any{uint32(42), uint64(1), uint64(2), uint64(7), "sales", "orders", "main", "append_only", "append_only", uint32(42), "", "30", "hash", uint64(1), "ksa_gold", "rest", "https://catalog.example.com/rest", "s3://warehouse", "none", "", "", uint64(1)}
	case strings.Contains(sql, "mo_iceberg_principal_map"):
		if e.denyPrincipal {
			return executor.Result{}, nil
		}
		values = []any{uint64(0), uint64(0), "test-principal", "", uint64(1)}
	case strings.Contains(sql, "mo_iceberg_residency_policy"):
		values = []any{"cluster", uint32(0), uint64(0), "https://catalog.example.com/rest", "catalog.example.com", "*", "*", "enabled", uint64(1)}
	default:
		return executor.Result{}, fmt.Errorf("unexpected Iceberg catalog SQL: %s", sql)
	}
	b := batch.NewWithSize(len(values))
	for i, value := range values {
		switch value := value.(type) {
		case uint32:
			b.Vecs[i] = vector.NewVec(types.T_uint32.ToType())
			require.NoError(e.t, vector.AppendFixed(b.Vecs[i], value, false, e.proc.Mp()))
		case uint64:
			b.Vecs[i] = vector.NewVec(types.T_uint64.ToType())
			require.NoError(e.t, vector.AppendFixed(b.Vecs[i], value, false, e.proc.Mp()))
		case string:
			b.Vecs[i] = vector.NewVec(types.T_varchar.ToType())
			require.NoError(e.t, vector.AppendBytes(b.Vecs[i], []byte(value), false, e.proc.Mp()))
		}
	}
	b.SetRowCount(1)
	return executor.Result{Mp: e.proc.Mp(), Batches: []*batch.Batch{b}}, nil
}
