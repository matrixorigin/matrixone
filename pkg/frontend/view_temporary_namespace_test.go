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

package frontend

import (
	"context"
	"encoding/json"
	"errors"
	"testing"

	"github.com/golang/mock/gomock"
	"github.com/matrixorigin/matrixone/pkg/catalog"
	"github.com/matrixorigin/matrixone/pkg/common/moerr"
	"github.com/matrixorigin/matrixone/pkg/container/types"
	mock_frontend "github.com/matrixorigin/matrixone/pkg/frontend/test"
	pb "github.com/matrixorigin/matrixone/pkg/pb/plan"
	"github.com/matrixorigin/matrixone/pkg/pb/txn"
	"github.com/matrixorigin/matrixone/pkg/sql/parsers/dialect/mysql"
	"github.com/matrixorigin/matrixone/pkg/sql/parsers/tree"
	"github.com/matrixorigin/matrixone/pkg/sql/plan"
	"github.com/matrixorigin/matrixone/pkg/testutil"
	"github.com/matrixorigin/matrixone/pkg/vm/engine"
	"github.com/matrixorigin/matrixone/pkg/vm/process"
	"github.com/stretchr/testify/require"
)

// Use the real session namespace resolver under the real planner; only the
// catalog and unrelated compiler services are fixtures.
type temporaryViewCompiler struct {
	*plan.MockCompilerContext
	relations *TxnCompilerContext
}

func (c *temporaryViewCompiler) DefaultDatabase() string { return "mo_catalog" }
func (c *temporaryViewCompiler) GetContext() context.Context {
	return c.relations.GetContext()
}
func (c *temporaryViewCompiler) SetContext(ctx context.Context) { c.relations.SetContext(ctx) }
func (c *temporaryViewCompiler) Resolve(db, name string, snapshot *plan.Snapshot, output *plan.TableResolution) (*plan.ObjectRef, *plan.TableDef, error) {
	return c.relations.Resolve(db, name, snapshot, output)
}

func TestPersistentViewTemporaryNamespace(t *testing.T) {
	for _, surface := range []string{"query", "describe", "regenerate", "request describe", "request plan"} {
		for _, shadow := range []string{"source", "middle_v"} {
			t.Run(surface+"/"+shadow, func(t *testing.T) {
				proc := testutil.NewProcess(t)
				ctrl := gomock.NewController(t)
				txnOp := mock_frontend.NewMockTxnOperator(ctrl)
				txnOp.EXPECT().Txn().Return(txn.TxnMeta{}).AnyTimes()
				storage := mock_frontend.NewMockEngine(ctrl)
				database := mock_frontend.NewMockDatabase(ctrl)
				storage.EXPECT().Database(gomock.Any(), "mo_catalog", txnOp).Return(database, nil).AnyTimes()
				ses, _ := newObservedProtocolSession()
				ses.tempTables, ses.tempTablesRev = make(map[string]string), make(map[string]string)
				ses.txnHandler = InitTxnHandler("", storage, proc.Ctx, txnOp)
				ses.AddTempTable("mo_catalog", shadow, "physical_temp")
				relations := &TxnCompilerContext{execCtx: &ExecCtx{reqCtx: proc.Ctx, ses: ses, proc: proc}}
				mock := plan.NewMockCompilerContext(false, proc)
				mock.DatabaseExistsFunc = func(name string, _ *plan.Snapshot) bool { return name == "mo_catalog" }
				mock.GetDatabaseIdFunc = func(string, *plan.Snapshot) (uint64, error) { return 7, nil }
				compiler := &temporaryViewCompiler{MockCompilerContext: mock, relations: relations}
				definitions := map[string]*plan.TableDef{
					"source": {Name: "source", DbName: "mo_catalog", DbId: 7, TblId: 11, TableType: catalog.SystemOrdinaryRel,
						Cols: []*plan.ColDef{{Name: "value", Typ: plan.Type{Id: int32(types.T_int32)}}}},
					"physical_temp": {Name: "physical_temp", DbName: "mo_catalog", DbId: 7, TblId: 12, TableType: catalog.SystemTemporaryTable,
						Cols: []*plan.ColDef{{Name: "value", Typ: plan.Type{Id: int32(types.T_int64)}}}},
				}
				for i, v := range []struct{ name, source string }{{"middle_v", "mo_catalog.source"}, {"root_v", "middle_v"}} {
					lower := int64(1)
					data, err := json.Marshal(plan.ViewData{Stmt: "create view " + v.name + " as select value from " + v.source,
						DefaultDatabase: "mo_catalog", LowerCaseTableNames: &lower})
					require.NoError(t, err)
					definitions[v.name] = &plan.TableDef{Name: v.name, DbName: "mo_catalog", DbId: 7, TblId: uint64(21 + i),
						TableType: catalog.SystemViewRel, ViewSql: &pb.ViewDef{View: string(data)}}
				}
				var sourceErr error
				database.EXPECT().Relation(gomock.Any(), gomock.Any(), nil).DoAndReturn(
					func(_ context.Context, name string, _ any) (engine.Relation, error) {
						if name == "source" && sourceErr != nil {
							return nil, sourceErr
						}
						def := definitions[name]
						if def == nil {
							return nil, moerr.NewNoSuchTableNoCtx("mo_catalog", name)
						}
						relation := mock_frontend.NewMockRelation(ctrl)
						relation.EXPECT().GetTableDef(gomock.Any()).Return(def).AnyTimes()
						relation.EXPECT().GetTableID(gomock.Any()).Return(def.TblId).AnyTimes()
						return relation, nil
					}).AnyTimes()
				parse := func(sql string) tree.Statement {
					statements, err := mysql.Parse(t.Context(), sql, 1)
					require.NoError(t, err)
					require.Len(t, statements, 1)
					t.Cleanup(statements[0].Free)
					return statements[0]
				}
				budget := process.MustNewExecutionResourceBudget(64<<20, 64<<20)
				t.Cleanup(budget.Close)
				generation, err := budget.OpenGeneration(1)
				require.NoError(t, err)
				t.Cleanup(generation.Close)
				provider := viewSchemaPlanProvider(func(ctx context.Context) (*plan.ViewSchemaBinding, error) {
					previous := compiler.GetContext()
					compiler.SetContext(ctx)
					return &plan.ViewSchemaBinding{Compiler: compiler, Generation: generation, Check: func() error { return nil },
						Authorize: func(context.Context, string, string, *plan.Snapshot) error { return nil },
						Close:     func() { compiler.SetContext(previous) }}, nil
				})
				derive := func() ([]*plan.ColDef, error) {
					switch surface {
					case "describe":
						return plan.DescribeViewColumns(compiler, definitions["root_v"].ViewSql.View)
					case "regenerate":
						value, err := plan.RegenerateViewDefinition(compiler, definitions["root_v"].ViewSql.View)
						if err != nil {
							return nil, err
						}
						return value.TableDef.Cols, nil
					case "request describe":
						request := plan.NewViewSchemaRequest(compiler.GetContext(), provider)
						defer request.Close()
						var lastColumns []*plan.ColDef
						for range 2 { // A warm request must agree with the first derivation.
							result, err := request.Describe("mo_catalog", "root_v", nil)
							if err != nil {
								return nil, err
							}
							columns, err := result.Columns()
							result.Release()
							if err != nil {
								return nil, err
							}
							require.Equal(t, definitions["source"].Cols[0].Typ.Id, columns[0].Typ.Id)
							lastColumns = columns
						}
						return lastColumns, nil
					default:
						stmt := parse("select v.value, t.value from root_v v cross join " + shadow + " t")
						var p *plan.Plan
						var err error
						if surface == "request plan" {
							request := plan.NewViewSchemaRequest(compiler.GetContext(), provider)
							defer request.Close()
							p, err = request.BuildPlan(stmt, false)
						} else {
							p, err = plan.BuildPlan(compiler, stmt, false, nil)
						}
						if err != nil {
							return nil, err
						}
						var sources []uint64
						for _, node := range p.GetQuery().Nodes {
							if node.NodeType == pb.Node_TABLE_SCAN {
								sources = append(sources, node.TableDef.TblId)
							}
						}
						require.ElementsMatch(t, []uint64{11, 12}, sources)
						columns := plan.GetResultColumnsFromPlan(p)
						require.EqualValues(t, types.T_int64, columns[1].Typ.Id)
						return columns[:1], nil
					}
				}
				for _, typ := range []types.T{types.T_int32, types.T_int16} {
					definitions["source"].Cols[0].Typ.Id = int32(typ)
					columns, err := derive()
					require.NoError(t, err)
					require.EqualValues(t, typ, columns[0].Typ.Id)
					_, direct, err := compiler.Resolve("mo_catalog", shadow, nil, nil)
					require.NoError(t, err)
					require.Equal(t, uint64(12), direct.TblId)
				}
				for _, failure := range []error{errors.New("catalog read failed"), moerr.NewNoSuchTableNoCtx("mo_catalog", "source")} {
					sourceErr = failure
					_, err := derive()
					if moerr.IsMoErrCode(failure, moerr.ErrNoSuchTable) {
						require.Error(t, err)
					} else {
						require.ErrorIs(t, err, failure)
					}
					_, direct, err := compiler.Resolve("mo_catalog", shadow, nil, nil)
					require.NoError(t, err)
					require.Equal(t, uint64(12), direct.TblId)
				}
			})
		}
	}
}
