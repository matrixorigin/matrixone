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

package plan

import (
	"testing"

	"github.com/matrixorigin/matrixone/pkg/catalog"
	"github.com/matrixorigin/matrixone/pkg/container/types"
	planpb "github.com/matrixorigin/matrixone/pkg/pb/plan"
	"github.com/matrixorigin/matrixone/pkg/sql/parsers/dialect/mysql"
	"github.com/stretchr/testify/require"
)

// Model both compiler contexts: the catalog-only frontend and the internal
// executor. The fast path must not treat either one's persisted View columns
// as the current result schema.
type viewMetadataConsumerContext struct {
	*MockCompilerContext
	fastCalls int
}

func (c *viewMetadataConsumerContext) BuildTableDefByMoColumns(db, name string) (*TableDef, error) {
	c.fastCalls++
	_, def, err := c.MockCompilerContext.Resolve(db, name, nil)
	return def, err
}

func newViewMetadataConsumerContext(t *testing.T) *viewMetadataConsumerContext {
	t.Helper()
	ctx := NewMockCompilerContext(true)
	ctx.GetAccountIdFunc = func() (uint32, error) { return 0, nil }
	ctx.tables["nation"].DbId = 10
	ctx.tables["nation"].Cols[1].Typ.Width = 60
	ctx.tables["v1"] = &TableDef{
		Name: "v1", DbName: "tpch", DbId: 10, TblId: 100, Version: 1,
		TableType: catalog.SystemViewRel,
		Cols:      []*ColDef{{Name: "label", OriginName: "label", Typ: Type{Id: int32(types.T_varchar), Width: 5}}},
		ViewSql:   &planpb.ViewDef{View: `{"Stmt":"create view v1 as select n_name as label from nation","DefaultDatabase":"tpch"}`},
	}
	ctx.objects["v1"] = &ObjectRef{SchemaName: "tpch", ObjName: "v1", Db: 10, Obj: 100}
	return &viewMetadataConsumerContext{MockCompilerContext: ctx}
}

func buildViewMetadataConsumerPlan(t *testing.T, ctx CompilerContext, sql string, prepared bool) (*Plan, error) {
	t.Helper()
	stmts, err := mysql.Parse(ctx.GetContext(), sql, 1)
	require.NoError(t, err)
	require.Len(t, stmts, 1)
	defer stmts[0].Free()
	opt := NewBaseOptimizer(ctx)
	if prepared {
		opt = NewPrepareOptimizer(ctx)
	}
	query, err := opt.Optimize(stmts[0], prepared)
	if err != nil {
		return nil, err
	}
	return &Plan{Plan: &planpb.Plan_Query{Query: query}}, nil
}

func TestViewMetadataLimitZeroBindsCurrentColumns(t *testing.T) {
	for _, sql := range []string{
		"select * from v1 limit 0",
		"select * from (select * from v1 limit 0) d",
		"with c as (select * from v1) select * from c limit 0",
	} {
		t.Run(sql, func(t *testing.T) {
			ctx := newViewMetadataConsumerContext(t)
			p, err := buildViewMetadataConsumerPlan(t, ctx, sql, false)
			require.NoError(t, err)
			cols := GetResultColumnsFromPlan(p)
			require.Len(t, cols, 1)
			require.Equal(t, "label", cols[0].Name)
			require.Equal(t, int32(60), cols[0].Typ.Width)
			require.Equal(t, int32(5), ctx.tables["v1"].Cols[0].Typ.Width, "description must not rewrite stored columns")
		})
	}
	t.Run("LIMIT 0 flag does not leak into UNION sibling", func(t *testing.T) {
		ctx := newViewMetadataConsumerContext(t)
		p, err := buildViewMetadataConsumerPlan(t, ctx,
			"select * from (select * from v1 limit 0) d union all select * from v1", false)
		require.NoError(t, err)
		require.Equal(t, int32(60), GetResultColumnsFromPlan(p)[0].Typ.Width)
		require.Equal(t, 1, ctx.fastCalls, "only the inner LIMIT 0 may probe the catalog-only path")
	})
	t.Run("ordinary table keeps catalog fast path", func(t *testing.T) {
		ctx := newViewMetadataConsumerContext(t)
		p, err := buildViewMetadataConsumerPlan(t, ctx, "select * from nation limit 0", false)
		require.NoError(t, err)
		require.Equal(t, 1, ctx.fastCalls)
		require.Len(t, p.GetQuery().CatalogDependencies, 1, "the table may later be replaced by a View")
		require.Equal(t, "nation", p.GetQuery().CatalogDependencies[0].ObjName)
	})
}

func TestViewMetadataEmptyPlanRetainsSourceDependencies(t *testing.T) {
	for _, prepared := range []bool{false, true} {
		for _, sql := range []string{"select label from v1 limit 0", "select label from v1 where false"} {
			t.Run(sql+map[bool]string{false: "/ordinary", true: "/prepared"}[prepared], func(t *testing.T) {
				ctx := newViewMetadataConsumerContext(t)
				p, err := buildViewMetadataConsumerPlan(t, ctx, sql, prepared)
				require.NoError(t, err)
				var names []string
				for _, ref := range p.GetQuery().GetCatalogDependencies() {
					names = append(names, ref.ObjName)
				}
				require.Contains(t, names, "v1")
				require.Contains(t, names, "nation", "result types depend on the source even if no rows can be read")
				schemas, _, err := ResetPreparePlan(ctx, p)
				require.NoError(t, err)
				var schemaNames []string
				for _, ref := range schemas {
					schemaNames = append(schemaNames, ref.ObjName)
				}
				require.Contains(t, schemaNames, "nation")
			})
		}
	}
}

func TestViewMetadataShowValuesUsesCurrentTypes(t *testing.T) {
	ctx := newViewMetadataConsumerContext(t)
	ctx.tables["v1"].Cols[0].Typ = Type{Id: int32(types.T_json)}
	stmts, err := mysql.Parse(ctx.GetContext(), "show table_values from tpch.v1", 1)
	require.NoError(t, err)
	defer stmts[0].Free()
	p, err := BuildPlan(ctx, stmts[0], false)
	require.NoError(t, err)
	cols := GetResultColumnsFromPlan(p)
	require.Len(t, cols, 2)
	for _, col := range cols {
		require.Equal(t, int32(types.T_varchar), col.Typ.Id, "stored JSON must not replace current MIN/MAX with NULL")
		require.Equal(t, int32(60), col.Typ.Width)
	}
}

func TestViewMetadataShowConsumersRejectInvalidView(t *testing.T) {
	for _, sql := range []string{"show column_number from v1", "show table_values from v1", "show columns from v1"} {
		t.Run(sql, func(t *testing.T) {
			ctx := newViewMetadataConsumerContext(t)
			ctx.tables["v1"].ViewSql.View = `{"Stmt":"create view v1 as select missing_column as label from nation","DefaultDatabase":"tpch"}`
			stmts, err := mysql.Parse(ctx.GetContext(), sql, 1)
			require.NoError(t, err)
			defer stmts[0].Free()
			_, err = BuildPlan(ctx, stmts[0], false)
			require.Error(t, err, "an invalid View must not be described using its stored columns")
		})
	}
}

func TestViewMetadataShowConsumersTrackSourceAndTarget(t *testing.T) {
	for _, sql := range []string{"show column_number from v1", "show table_values from v1", "show columns from v1"} {
		t.Run(sql, func(t *testing.T) {
			ctx := newViewMetadataConsumerContext(t)
			stmts, err := mysql.Parse(ctx.GetContext(), sql, 1)
			require.NoError(t, err)
			defer stmts[0].Free()
			p, err := BuildPlan(ctx, stmts[0], false)
			require.NoError(t, err)
			var names []string
			for _, ref := range p.GetQuery().GetCatalogDependencies() {
				names = append(names, ref.ObjName)
			}
			require.Contains(t, names, "v1")
			require.Contains(t, names, "nation")
		})
	}
}
