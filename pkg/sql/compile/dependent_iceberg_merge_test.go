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

	"github.com/gogo/protobuf/proto"
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
	"github.com/matrixorigin/matrixone/pkg/iceberg/model"
	"github.com/matrixorigin/matrixone/pkg/pb/plan"
	"github.com/matrixorigin/matrixone/pkg/sql/colexec/dependent"
	"github.com/matrixorigin/matrixone/pkg/sql/colexec/icebergwrite"
	sqliceberg "github.com/matrixorigin/matrixone/pkg/sql/iceberg"
	"github.com/matrixorigin/matrixone/pkg/sql/parsers/dialect/mysql"
	plan2 "github.com/matrixorigin/matrixone/pkg/sql/plan"
	"github.com/matrixorigin/matrixone/pkg/testutil"
	"github.com/matrixorigin/matrixone/pkg/vm"
	"github.com/matrixorigin/matrixone/pkg/vm/process"
	"github.com/stretchr/testify/require"
)

type dependentMergeContext struct{ plan2.CompilerContext }

func (c dependentMergeContext) Resolve(db, name string, snapshot *plan.Snapshot) (*plan.ObjectRef, *plan.TableDef, error) {
	if name != "gold_orders" && name != "dim_orders" {
		return c.CompilerContext.Resolve(db, name, snapshot)
	}
	td := &plan.TableDef{Name: name, DbName: db, DbId: 1, TblId: 2,
		Cols: []*plan.ColDef{{Name: "id", Typ: types.T_int64.ToType().PlanType(), Default: &plan.Default{NullAbility: true}}}, Pkey: &plan.PrimaryKeyDef{}}
	if name == "gold_orders" {
		td.TableType = catalog.SystemExternalRel
		td.Createsql = sqliceberg.BuildCreateSQLEnvelope(model.TableMapping{Namespace: "sales", TableName: "orders", DefaultRef: "main", ReadMode: model.ReadModeMergeOnRead, WriteMode: model.WriteModeMergeOnRead}, "ksa_gold")
	} else {
		td.TableType = catalog.SystemOrdinaryRel
		td.TblId = 3
	}
	return &plan.ObjectRef{Db: 1, Obj: int64(td.TblId), DbName: db, ObjName: name}, td, nil
}

type dependentMergeCoordinator struct {
	request                 icebergwrite.AppendRequest
	rows                    []string
	begins, commits, aborts int
}

func (c *dependentMergeCoordinator) Begin(_ context.Context, request icebergwrite.AppendRequest) error {
	c.request = request
	c.begins++
	return nil
}
func (c *dependentMergeCoordinator) Append(_ context.Context, b *batch.Batch) error {
	for row := 0; row < b.RowCount(); row++ {
		path, ordinal := "NULL", "NULL"
		if v := b.Vecs[c.request.DataFilePathColumnIndex]; !v.IsNull(uint64(row)) {
			path = v.GetStringAt(row)
		}
		if v := b.Vecs[c.request.RowOrdinalColumnIndex]; !v.IsNull(uint64(row)) {
			ordinal = fmt.Sprint(vector.GetFixedAtNoTypeCheck[int64](v, row))
		}
		action := b.Vecs[c.request.MergeActionColumnIndex].GetStringAt(row)
		c.rows = append(c.rows, fmt.Sprintf("%d/%s/%s/%s", vector.GetFixedAtNoTypeCheck[int64](b.Vecs[0], row), path, ordinal, action))
	}
	return nil
}
func (c *dependentMergeCoordinator) Commit(context.Context) error       { c.commits++; return nil }
func (c *dependentMergeCoordinator) Abort(context.Context, error) error { c.aborts++; return nil }

func TestDependentIcebergMergeProjection(t *testing.T) {
	for _, mode := range []string{"update", "delete"} {
		t.Run(mode, func(t *testing.T) {
			proc := testutil.NewProcessWithOwnedMPool(t, "", mpool.MustNewZero())
			defer proc.Free()
			proc.Ctx = defines.AttachAccountId(proc.Ctx, 42)
			proc.SetStmtProfile(process.NewStmtProfile(uuid.Nil, uuid.New()))
			restoreRuntimeVariableForTest(t, proc.GetService(), moruntime.MOProtocolVersion, defines.MORPCVersion119)
			clause := "when matched then update set id=(select (select s.id+10))"
			want := []string{"12/target.parquet/1/update", "3/NULL/NULL/insert"}
			if mode == "delete" {
				clause = "when matched and t.id=(select (select s.id)) then delete"
				want[0] = "2/target.parquet/1/delete"
			}
			stmt, err := mysql.ParseOne(proc.Ctx, "merge into gold_orders t using dim_orders s on t.id=s.id "+clause+" when not matched then insert (id) values (s.id)", 1)
			require.NoError(t, err)
			defer stmt.Free()
			bound, err := plan2.BuildPlan(dependentMergeContext{plan2.NewMockCompilerContext(true, proc)}, stmt, false)
			require.NoError(t, err)
			q := bound.GetQuery()
			require.NotNil(t, q.Correlation)
			require.NoError(t, q.ValidateCorrelation())
			original := proto.Clone(q).(*plan.Query)
			ref := registerCompileTestObjectIO(t)
			// The injected scan plan needs its own fixture lease across writer
			// generations, just as the real target scan owns an independent lease.
			require.True(t, icebergio.RetainObjectIORef(ref))
			defer icebergio.ReleaseObjectIORef(ref)
			c := &Compile{proc: proc, anal: &AnalyzeModule{qry: q}, icebergScanPlans: make(map[int32]*icebergapi.IcebergScanPlan)}
			for _, node := range q.Nodes {
				if node.GetExternScan().GetIcebergScan() != nil {
					c.icebergScanPlans[node.NodeId] = &icebergapi.IcebergScanPlan{Snapshot: icebergapi.SnapshotPlan{SnapshotID: 30, SchemaID: 9, RefName: "main"}, ObjectIORef: ref}
				}
			}
			var coordinators []*dependentMergeCoordinator
			restoreRuntimeVariableForTest(t, proc.GetService(), IcebergAppendCoordinatorFactoryRuntimeKey, icebergwrite.CoordinatorFactoryFunc(func(_ context.Context, request icebergwrite.AppendRequest) (icebergwrite.Coordinator, error) {
				require.Equal(t, icebergwrite.OperationMerge, request.Operation)
				require.Equal(t, int64(30), request.DMLScan.BaseSnapshotID)
				coord := &dependentMergeCoordinator{}
				coordinators = append(coordinators, coord)
				return coord, nil
			}))
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
					ok := false
					defer func() {
						if !ok {
							b.Clean(child.Mp())
						}
					}()
					for i, col := range node.TableDef.Cols {
						typ, err := types.TypeFromPlan(col.Typ)
						if err != nil {
							return nil, err
						}
						b.Vecs[i], err = vector.NewOffHeapVecWithTypeAndAllocation(typ, allocation)
						if err != nil {
							return nil, err
						}
						for row := 0; row < 2; row++ {
							switch strings.ToLower(col.Name) {
							case "id":
								value := int64(row + 1)
								if node.TableDef.Name == "dim_orders" {
									value++
								}
								err = vector.AppendFixed(b.Vecs[i], value, false, child.Mp())
							case icebergapi.DMLDataFilePathColumnName:
								err = vector.AppendBytes(b.Vecs[i], []byte("target.parquet"), false, child.Mp())
							case icebergapi.DMLRowOrdinalColumnName:
								err = vector.AppendFixed(b.Vecs[i], int64(row), false, child.Mp())
							default:
								return nil, fmt.Errorf("unexpected MERGE source column %s/%v", col.Name, typ)
							}
							if err != nil {
								return nil, err
							}
						}
					}
					b.SetRowCount(2)
					ok = true
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
				require.ElementsMatch(t, want, coord.rows)
				require.Equal(t, 1, coord.begins)
				require.Equal(t, 1, coord.commits)
				require.Zero(t, coord.aborts)
				require.Equal(t, opened, closed)
				require.Zero(t, account.Snapshot().Used)
				require.Zero(t, budget.Used())
				require.True(t, proto.Equal(original, q), "the cached physical query must not be normalized in place")
			}
			require.NotEqual(t, coordinators[0].request.StatementID, coordinators[1].request.StatementID)
			icebergio.ReleaseObjectIORef(ref)
			require.False(t, icebergio.RetainObjectIORef(ref), "no writer may retain the target provider after both executions")
		})
	}
}
