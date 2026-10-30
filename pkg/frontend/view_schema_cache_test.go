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
	"testing"

	"github.com/matrixorigin/matrixone/pkg/catalog"
	"github.com/matrixorigin/matrixone/pkg/container/types"
	pb "github.com/matrixorigin/matrixone/pkg/pb/plan"
	"github.com/matrixorigin/matrixone/pkg/sql/parsers/dialect/mysql"
	plan "github.com/matrixorigin/matrixone/pkg/sql/plan"
	"github.com/matrixorigin/matrixone/pkg/vm/process"
	"github.com/stretchr/testify/require"
)

type viewSchemaPlanProvider func(context.Context) (*plan.ViewSchemaBinding, error)

func (p viewSchemaPlanProvider) OpenViewSchemaBinding(ctx context.Context) (*plan.ViewSchemaBinding, error) {
	return p(ctx)
}

type viewSchemaCacheCompiler struct {
	*plan.MockCompilerContext
	definitions map[string]*plan.TableDef
}

func (c *viewSchemaCacheCompiler) Resolve(_ string, name string, _ *plan.Snapshot) (*plan.ObjectRef, *plan.TableDef, error) {
	def := c.definitions[name]
	if def == nil {
		return nil, nil, nil
	}
	return &plan.ObjectRef{SchemaName: "tpch", ObjName: name, Db: 7, Obj: int64(def.TblId)}, def, nil
}

func TestViewSchemaPlansInvalidateOnTransitiveCatalogChanges(t *testing.T) {
	for _, mutation := range []string{"metadata only", "COPY physical replacement", "same name recreation", "intermediate view replacement"} {
		t.Run(mutation, func(t *testing.T) {
			proc := newPlanTestProcess(t)
			mock := plan.NewMockCompilerContext(false, proc)
			mock.GetAccountIdFunc = func() (uint32, error) { return 0, nil }
			compiler := &viewSchemaCacheCompiler{MockCompilerContext: mock, definitions: map[string]*plan.TableDef{
				"source": {Name: "source", DbName: "tpch", DbId: 7, TblId: 10, LogicalId: 100, Version: 1,
					Cols: []*plan.ColDef{{Name: "value", Typ: plan.Type{Id: int32(types.T_int32)}}}},
				"middle_v": {Name: "middle_v", DbName: "tpch", DbId: 7, TblId: 20, Version: 1, TableType: catalog.SystemViewRel,
					ViewSql: &pb.ViewDef{View: `{"Stmt":"create view middle_v as select value from source","DefaultDatabase":"tpch","lower_case_table_names":1}`}},
				"root_v": {Name: "root_v", DbName: "tpch", DbId: 7, TblId: 30, Version: 1, TableType: catalog.SystemViewRel,
					ViewSql: &pb.ViewDef{View: `{"Stmt":"create view root_v as select value from middle_v","DefaultDatabase":"tpch","lower_case_table_names":1}`}},
			}}
			budget := process.MustNewExecutionResourceBudget(64<<20, 64<<20)
			defer budget.Close()
			generation, err := budget.OpenGeneration(1)
			require.NoError(t, err)
			defer generation.Close()
			provider := viewSchemaPlanProvider(func(ctx context.Context) (*plan.ViewSchemaBinding, error) {
				compiler.SetContext(ctx)
				return &plan.ViewSchemaBinding{Compiler: compiler, Generation: generation, Check: func() error { return nil },
					Authorize: func(context.Context, string, string, *plan.Snapshot) error { return nil }, Close: func() {}}, nil
			})
			request := plan.NewViewSchemaRequest(t.Context(), provider)
			defer request.Close()
			statements, err := mysql.Parse(t.Context(), "select value from root_v limit 0", 1)
			require.NoError(t, err)
			defer statements[0].Free()
			p, err := request.BuildPlan(statements[0], true)
			require.NoError(t, err)
			request.Close()
			require.Len(t, p.GetQuery().CatalogDependencies, 3)
			schemas, _, err := plan.ResetPreparePlan(compiler, p)
			require.NoError(t, err)
			require.Len(t, schemas, 3)
			changed, err := checkModify(p, compiler.Resolve)
			require.NoError(t, err)
			require.False(t, changed)
			source, intermediate := compiler.definitions["source"], compiler.definitions["middle_v"]
			switch mutation {
			case "metadata only":
				source.Version++
				source.Cols[0].Typ.Id = int32(types.T_int64)
			case "COPY physical replacement":
				source.TblId++ // LogicalId intentionally remains unchanged.
			case "same name recreation":
				source.TblId++
				source.LogicalId++
			case "intermediate view replacement":
				intermediate.Version++
			}
			require.EqualValues(t, 1, compiler.definitions["root_v"].Version)
			changed, err = checkModify(p, compiler.Resolve)
			require.NoError(t, err)
			require.True(t, changed, "an unchanged root View version cannot validate its changed transitive source")
			require.EqualValues(t, types.T_int32, plan.GetResultColumnsFromPlan(p)[0].Typ.Id,
				"the independently owned old plan remains a snapshot, not mutable catalog state")
			require.Zero(t, generation.Used())
		})
	}
}
