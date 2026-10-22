// Copyright 2026 Matrix Origin
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at http://www.apache.org/licenses/LICENSE-2.0
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.
package plan

import (
	"context"
	"encoding/json"
	"testing"

	moruntime "github.com/matrixorigin/matrixone/pkg/common/runtime"
	"github.com/matrixorigin/matrixone/pkg/defines"
	planpb "github.com/matrixorigin/matrixone/pkg/pb/plan"
	"github.com/matrixorigin/matrixone/pkg/sql/parsers"
	"github.com/matrixorigin/matrixone/pkg/sql/parsers/dialect"
	"github.com/stretchr/testify/require"
)

func TestNativePADViewBindingPersistence(t *testing.T) {
	ctx := NewMockCompilerContext(false, newPlanTestProcess(t))
	ctx.GetAccountIdFunc = func() (uint32, error) { return 42, nil }
	outer := context.WithValue(t.Context(), collationBindingRevisionKey{}, uint32(1))
	ctx.SetContext(outer)
	rt := moruntime.ServiceRuntime(ctx.GetProcess().GetService())
	for _, key := range []string{moruntime.MOProtocolVersion, moruntime.PersistedExpressionProtocolFloor, moruntime.PersistedExpressionProtocolAuthoringFloor} {
		previous, exists := rt.GetGlobalVariables(key)
		t.Cleanup(func() {
			if exists {
				rt.SetGlobalVariables(key, previous)
			} else if current, ok := rt.GetGlobalVariables(key); ok {
				rt.CompareAndDeleteGlobalVariables(key, current)
			}
		})
	}
	set := func(local, read, author int64) {
		rt.SetGlobalVariables(moruntime.MOProtocolVersion, local)
		rt.SetGlobalVariables(moruntime.PersistedExpressionProtocolFloor, read)
		rt.SetGlobalVariables(moruntime.PersistedExpressionProtocolAuthoringFloor, author)
	}
	build := func(sql string) (*Plan, error) {
		stmt, err := parsers.ParseOne(ctx.GetContext(), dialect.MYSQL, sql, 1)
		if err != nil {
			return nil, err
		}
		defer stmt.Free()
		return BuildPlan(&rootSQLCompilerContext{MockCompilerContext: ctx, rootSQL: sql}, stmt, false)
	}
	for _, sql := range []string{
		"create view native_v as select 'A' collate utf8mb4_general_ci = 'a' as b",
		"create view native_v as select n_name collate utf8mb4_general_ci as s from nation",
		"create view native_v as select n_name collate utf8mb3_bin as s from nation",
	} {
		t.Run(sql, func(t *testing.T) {
			for _, floors := range [][3]int64{{123, 129, 129}, {124, 129, 129}, {125, 129, 129}, {128, 129, 129}, {129, 129, 123}, {129, 129, 124}, {129, 129, 128}} {
				set(floors[0], floors[1], floors[2])
				p, err := build(sql)
				require.ErrorContains(t, err, "protocol version 129")
				require.Nil(t, p)
			}
			set(129, 129, 129)
			p, err := build(sql)
			require.NoError(t, err)
			table := p.GetDdl().GetCreateView().GetTableDef()
			var data ViewData
			require.NoError(t, json.Unmarshal([]byte(table.ViewSql.View), &data))
			require.Equal(t, uint32(1), data.CollationBindingRevision)
			require.NotNil(t, data.RequiredProtocolVersion)
			require.Equal(t, defines.MORPCVersion129, *data.RequiredProtocolVersion)
			require.NoError(t, RequireCatalogCollationsForAuthoring(ctx.GetContext(), ctx.GetProcess(), table))
			for _, floors := range [][2]int64{{123, 129}, {124, 129}, {125, 129}, {128, 129}, {129, 123}, {129, 124}, {129, 128}, {129, 129}} {
				set(floors[0], floors[1], 124)
				for _, reader := range []string{"catalog", "describe", "regenerate", "expand"} {
					var err error
					switch reader {
					case "catalog":
						err = RequireCatalogCollations(ctx.GetContext(), ctx.GetProcess(), table)
					case "describe":
						_, err = DescribeViewColumns(ctx, table.ViewSql.View)
					case "regenerate":
						var regenerated *RegeneratedViewDefinition
						regenerated, err = RegenerateViewDefinition(ctx, table.ViewSql.View)
						if err == nil {
							var got ViewData
							require.NoError(t, json.Unmarshal([]byte(regenerated.TableDef.ViewSql.View), &got))
							require.Equal(t, data.CollationBindingRevision, got.CollationBindingRevision)
						}
					case "expand":
						b := NewQueryBuilder(planpb.Query_SELECT, ctx, false, false)
						_, err = b.bindView(NewBindContext(b, nil), table, nil, &ObjectRef{}, "tpch", "native_v", nil)
					}
					if floors[0] < 129 || floors[1] < 129 {
						require.ErrorContains(t, err, "protocol version 129", reader)
					} else {
						require.NoError(t, err, reader)
					}
					require.Same(t, outer, ctx.GetContext())
				}
			}
			set(129, 129, 124)
			require.ErrorContains(t, RequireCatalogCollationsForAuthoring(ctx.GetContext(), ctx.GetProcess(), table), "protocol version 129")
			data.RequiredProtocolVersion = nil
			malformed, err := json.Marshal(data)
			require.NoError(t, err)
			_, err = DescribeViewColumns(ctx, string(malformed))
			require.ErrorContains(t, err, "collation binding revision 1")
		})
	}
	// 不使用native显式COLLATE的新View不应无故提升最低版本或marker。
	set(118, 118, 118)
	p, err := build("create view plain_v as select 1 as n")
	require.NoError(t, err)
	var plain ViewData
	require.NoError(t, json.Unmarshal([]byte(p.GetDdl().GetCreateView().GetTableDef().ViewSql.View), &plain))
	require.Zero(t, plain.CollationBindingRevision)
	require.Nil(t, plain.RequiredProtocolVersion)
}
