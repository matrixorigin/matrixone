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
	"context"
	"testing"

	"github.com/golang/mock/gomock"
	"github.com/matrixorigin/matrixone/pkg/catalog"
	moruntime "github.com/matrixorigin/matrixone/pkg/common/runtime"
	"github.com/matrixorigin/matrixone/pkg/defines"
	pb "github.com/matrixorigin/matrixone/pkg/pb/plan"
	"github.com/matrixorigin/matrixone/pkg/sql/parsers/dialect/mysql"
	"github.com/matrixorigin/matrixone/pkg/sql/parsers/tree"
	"github.com/stretchr/testify/require"
)

func TestMaterializedViewRefreshDeleteRetainsRelation(t *testing.T) {
	for _, kind := range []string{catalog.SystemOrdinaryRel, catalog.SystemMaterializedViewRel} {
		t.Run(kind, func(t *testing.T) {
			proc := newPlanTestProcess(t)
			ctx := NewMockCompilerContext(true, proc)
			table := ctx.tables["nation"]
			table.TableType = kind
			ctx.SetContext(defines.WithMaterializedViewWrite(
				defines.AttachAccountId(proc.Ctx, 0), 0, table.TblId, []byte("refresh")))
			stmt, err := mysql.ParseOne(ctx.GetContext(), "delete from tpch.nation", 1)
			require.NoError(t, err)
			defer stmt.Free()
			// Unrestricted DELETE reaches the legacy planner, which must keep
			// the MV's physical identity for the refresh publication transaction.
			p, err := buildDelete(stmt.(*tree.Delete), ctx, false)
			require.NoError(t, err)
			found := false
			for _, node := range p.GetQuery().Nodes {
				if node.NodeType == pb.Node_DELETE && node.DeleteCtx.TableDef.TblId == table.TblId {
					found = true
					require.Equal(t, kind == catalog.SystemOrdinaryRel, node.DeleteCtx.CanTruncate)
				}
			}
			require.True(t, found, "expected an origin-table DELETE node")
		})
	}
}

func TestMaterializedViewSelectContract(t *testing.T) {
	ctx := NewMockCompilerContext2(gomock.NewController(t))
	ctx.EXPECT().GetContext().Return(context.Background()).AnyTimes()
	for _, tc := range []struct {
		sql     string
		allowed bool
	}{
		{"select k,sum(v),count(*),min(v),max(v),avg(v) from t where v>0 group by k", true},
		{"select a.k,b.v from a inner join b on a.k=b.k", true},
		{"select a.k,b.v from a left join b on a.k=b.k", false},
		{"select rand() from t", false}, {"select now() from t", false},
		{"select distinct k from t", false}, {"select count(distinct k) from t", false},
		{"select k from t limit 1", false}, {"select k from t order by k", false},
		{"select @secret from t", false}, {"select k from t where k in (select k from s)", false},
		{"with x as (select * from t) select * from x", false},
		{"select row_number() over (order by k) from t", false},
		{"select k from t union all select k from s", false},
		{"select sample(k, 1 rows) from t", false},
		{"select ? from t", false},
		{"select uuid() from t", false},
		{"select sum (v) from t", false},
		{"select current_timestamp() from t", false},
		{"select k from t for update", false},
		{"select a.k from a cross join b", false},
		{"select a.k from a right join b on a.k=b.k", false},
		{"select k,sum(v) from t group by k with rollup", false},
		{"select k from t {MO_TS = 123}", false},
		{"select k from t {SNAPSHOT = 'snapshot'}", false},
		{"select cast(v as decimal(20,2)),abs(v),coalesce(v,0) from t where k is not null", true},
		{"select k,sum(v) from t group by k having sum(v)>10", true},
		{"select concat(k,'x') from t", true},
	} {
		t.Run(tc.sql, func(t *testing.T) {
			stmts, err := mysql.Parse(context.Background(), tc.sql, 1)
			require.NoError(t, err)
			defer stmts[0].Free()
			err = ValidateMaterializedViewSelect(ctx, stmts[0].(*tree.Select))
			if tc.allowed {
				require.NoError(t, err)
			} else {
				require.Error(t, err)
			}
		})
	}
}

type materializedViewPlanContext struct {
	*MockCompilerContext
	enabled bool
}

func (c *materializedViewPlanContext) ResolveVariable(name string, system, global bool) (interface{}, error) {
	if name == "experimental_materialized_view" {
		if c.enabled {
			return int8(1), nil
		}
		return int8(0), nil
	}
	return c.MockCompilerContext.ResolveVariable(name, system, global)
}
func TestMaterializedViewCreatePlan(t *testing.T) {
	for _, scenario := range []string{"aggregate", "star", "gate disabled", "old protocol", "old authoring floor", "temporary source", "view source", "cross database", "short interval", "duplicate headings"} {
		t.Run(scenario, func(t *testing.T) {
			proc := newPlanTestProcess(t)
			base := NewMockCompilerContext(false, proc)
			base.SetContext(proc.Ctx)
			ctx := &materializedViewPlanContext{base, true}
			setPlanTestGlobalVariable(t, proc.GetService(), moruntime.MOProtocolVersion, int64(defines.MORPCVersion117))
			setPlanTestGlobalVariable(t, proc.GetService(), moruntime.PersistedExpressionProtocolAuthoringFloor, int64(defines.MORPCVersion117))
			query := "create materialized view stored refresh manual as select n_regionkey,sum(n_nationkey) as s,count(*) as n from nation group by n_regionkey"
			switch scenario {
			case "star":
				query = "create materialized view stored refresh manual as select * from nation"
			case "gate disabled":
				ctx.enabled = false
			case "old protocol":
				setPlanTestGlobalVariable(t, proc.GetService(), moruntime.MOProtocolVersion, int64(defines.MORPCVersion116))
			case "old authoring floor":
				setPlanTestGlobalVariable(t, proc.GetService(), moruntime.PersistedExpressionProtocolAuthoringFloor, int64(defines.MORPCVersion116))
			case "temporary source":
				base.tables["nation"].IsTemporary = true
			case "view source":
				base.tables["nation"].TableType = catalog.SystemViewRel
			case "cross database":
				query = "create materialized view bvt_test1.stored refresh manual as select * from tpch.nation"
			case "short interval":
				query = "create materialized view stored refresh every interval 1 second as select * from nation"
			case "duplicate headings":
				query = "create materialized view stored refresh manual as select n_nationkey as x,n_regionkey as x from nation"
			}
			stmt, err := mysql.ParseOne(proc.Ctx, query, 1)
			require.NoError(t, err)
			defer stmt.Free()
			p, err := BuildPlan(ctx, stmt, false)
			if scenario != "aggregate" && scenario != "star" {
				require.Error(t, err)
				return
			}
			require.NoError(t, err)
			create := p.GetDdl().GetCreateTable()
			require.NotNil(t, create)
			require.Empty(t, create.CreateAsSelectSql)
			require.Nil(t, p.GetDdl().Query)
			require.Equal(t, catalog.SystemMaterializedViewRel, create.TableDef.TableType)
			definition, err := catalog.MaterializedViewDefinition(create.TableDef)
			require.NoError(t, err)
			require.Len(t, definition.Sources, 1)
			require.Equal(t, "nation", definition.Sources[0].Table)
			require.Equal(t, "tpch", definition.Database)
			require.NotContains(t, definition.Query, "select *")
			if scenario == "aggregate" {
				require.Equal(t, "n_regionkey", create.TableDef.Cols[0].Name)
				require.Equal(t, "s", create.TableDef.Cols[1].Name)
				require.Equal(t, "n", create.TableDef.Cols[2].Name)
			}
		})
	}
}

func TestMaterializedViewCatalogPlans(t *testing.T) {
	ctx := NewMockCompilerContext(false, newPlanTestProcess(t))
	for _, ddl := range catalog.MaterializedViewCatalogDDLs {
		stmt, err := mysql.ParseOne(context.Background(), ddl, 1)
		require.NoError(t, err)
		func() {
			defer stmt.Free()
			p, err := BuildPlan(ctx, stmt, false)
			require.NoError(t, err)
			kind := ""
			for _, def := range p.GetDdl().GetCreateTable().TableDef.Defs {
				if props := def.GetProperties(); props != nil {
					for _, property := range props.Properties {
						if property.Key == catalog.SystemRelAttr_Kind {
							kind = property.Value
						}
					}
				}
			}
			require.Equal(t, catalog.SystemClusterRel, kind)
			require.NotNil(t, FindColumn(p.GetDdl().GetCreateTable().TableDef.Cols, "account_id"))
			require.NotNil(t, p.GetDdl().GetCreateTable().TableDef.Pkey)
		}()
	}
}
